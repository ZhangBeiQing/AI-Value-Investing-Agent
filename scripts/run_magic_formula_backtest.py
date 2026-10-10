#!/usr/bin/env python3
"""运行格林布拉特神奇公式 A 股研究回测（独立研究，不触碰交易账本）。

支持两种实现：
- magic    神奇公式：EBIT/EV 收益率 + EBIT/有形资本 投资回报率 综合排名
- channel2 书本第 14 章"渠道二"：ROA≥25% 后取最低 P/E，排除公用事业/金融/P/E≤5/上周披露
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import bootstrap_run_logging_from_argv

bootstrap_run_logging_from_argv("magic_formula")

from services.research.magic_formula import (
    MagicFormulaConfig,
    run_backtest,
    write_comparison,
    write_outputs,
)
from services.research.hs300_universe import (
    load_or_fetch_membership,
    membership_stocks_for_year,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="格林布拉特神奇公式 A 股回测")
    parser.add_argument("--base-dir", default="data", help="数据根目录 (default: data)")
    parser.add_argument("--output-dir", default="data/magic_formula", help="输出目录")
    parser.add_argument("--start", default="2018-01-01", help="回测起始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-09-30", help="回测结束日 (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, default=20, help="组合持仓数量 (default: 20)")
    parser.add_argument("--keep-financials", action="store_true", help="保留银行/券商/保险")
    parser.add_argument("--method", choices=["magic", "channel2", "both"], default="magic", help="选股实现（默认 magic）")
    parser.add_argument("--dynamic-hs300", action="store_true", help="按沪深300历史成分逐期重建股票池（动态 universe）")
    parser.add_argument("--rebalance", choices=["A", "S", "Q", "M"], default="A", help="调仓频率: A年度(默认)/S半年/Q季度/M月度")
    parser.add_argument("--no-write", action="store_true", help="只打印，不写文件")
    args = parser.parse_args()

    config = MagicFormulaConfig(
        base_dir=args.base_dir,
        output_dir=args.output_dir,
        start_date=args.start,
        end_date=args.end,
        top_n=args.top_n,
        exclude_financials=not args.keep_financials,
        rebalance_freq=args.rebalance,
    )

    membership = None
    if args.dynamic_hs300:
        years = list(range(int(args.start[:4]), int(args.end[:4]) + 1))
        snapshots = load_or_fetch_membership(args.base_dir, years)
        membership = {year: membership_stocks_for_year(snapshots, year) for year in years}
        print(f"动态沪深300成分: { {y: len(v) for y, v in membership.items()} }")

    methods = ["magic", "channel2"] if args.method == "both" else [args.method]
    results = {}
    for method in methods:
        result = run_backtest(config, method=method, membership=membership)
        results[method] = result
        print(json.dumps(result.summary, ensure_ascii=False, indent=2))
        if not args.no_write:
            paths = write_outputs(result, config, method)
            print(f"[{method}] 已写出:")
            for name, path in paths.items():
                print(f"  {name}: {path}")

    if len(results) == 2:
        print("\n==== 神奇公式 vs 渠道二 ====")
        for method, result in results.items():
            p = result.summary.get("portfolio", {})
            print(
                f"{method:9s} CAGR={p.get('cagr')}  累计={p.get('total_return')}  "
                f"夏普={p.get('sharpe')}  回撤={p.get('max_drawdown')}"
            )
        if not args.no_write:
            print("对照报告:", write_comparison(results, config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
