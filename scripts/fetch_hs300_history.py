#!/usr/bin/env python3
"""抓取沪深300历史成分名单，并批量拉取长期价格+财报缓存。

用于"严格动态成分"回测：先缓存 2008-2026 每年的沪深300成分，再抓取并集股票的长历史。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import bootstrap_run_logging_from_argv

bootstrap_run_logging_from_argv("magic_formula")

from services.research.hs300_universe import (
    ensure_symbols_data,
    load_or_fetch_membership,
    union_stocks,
    union_symbols,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="抓取沪深300历史成分与长历史数据")
    parser.add_argument("--base-dir", default="data", help="数据根目录 (default: data)")
    parser.add_argument("--start-year", type=int, default=2008)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--lookback-price-days", type=int, default=7600, help="价格回溯天数（覆盖 2008 起）")
    parser.add_argument("--membership-only", action="store_true", help="只抓成分名单，不拉行情")
    parser.add_argument("--include-delisted", action="store_true", help="也尝试抓取已退市股票（会较慢）")
    parser.add_argument("--force", action="store_true", help="强制重抓成分名单")
    args = parser.parse_args()

    years = list(range(args.start_year, args.end_year + 1))
    snapshots = load_or_fetch_membership(args.base_dir, years, force=args.force)
    union = union_symbols(snapshots)
    print(f"年度快照: {[y for y in years if str(y) in snapshots]}")
    print(f"各年成分数: { {y: len(snapshots.get(str(y), {})) for y in years} }")
    print(f"并集股票数: {len(union)}")

    if args.membership_only:
        print("仅抓成分名单，结束。")
        return 0

    stocks = union_stocks(snapshots)
    if not args.include_delisted:
        stocks = _filter_listed(base_dir=args.base_dir, stocks=stocks)
        print(f"过滤退市股后待抓取: {len(stocks)}")
    result = ensure_symbols_data(args.base_dir, stocks, lookback_price_days=args.lookback_price_days)
    print(f"抓取完成: 成功 {result['ok']}，失败 {result['failed']}")
    return 0


def _has_local_cache(base_dir: str, symbol: str) -> bool:
    import glob
    import os

    for d in glob.glob(os.path.join(base_dir, "stock_info", "*")):
        if d.endswith("_" + symbol):
            return os.path.exists(os.path.join(d, "prices", "price.csv")) and os.path.exists(
                os.path.join(d, "financials_cache", "profit_sheet.csv")
            )
    return False


def _filter_listed(*, base_dir: str, stocks: list) -> list:
    """跳过已退市（当前 A 股名单里没有）且本地无缓存的股票，避免无谓重试。"""
    import akshare as ak

    listing = set(ak.stock_info_a_code_name()["code"].astype(str).str.zfill(6))
    kept = []
    for stock in stocks:
        code = stock.symbol.split(".")[0]
        if code in listing or _has_local_cache(base_dir, stock.symbol):
            kept.append(stock)
    return kept


if __name__ == "__main__":
    raise SystemExit(main())
