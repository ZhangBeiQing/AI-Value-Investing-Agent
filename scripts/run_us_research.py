#!/usr/bin/env python3
"""Generate a US stock research pack (``us_tracked`` book).

默认只处理 configs/stock_pool.py 的 US_TRACKED_STOCKS（当前为台积电 TSM.US），
产物写到 data/skill_runs/<最近已收盘美东交易日>/us_tracked/，不触碰 fixed_tracked。

用法：
    python scripts/run_us_research.py --symbol TSM.US --date 2026-11-05
    python scripts/run_us_research.py                      # 默认池 + 最近已收盘交易日
    python scripts/run_us_research.py --symbol TSM.US --skip-refresh
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import bootstrap_run_logging_from_argv

bootstrap_run_logging_from_argv("us_research")

from core.network import install_network_timeouts  # noqa: E402
from services.pipeline.us_research import run_us_research_pack  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="生成美股研究包（01-04，us_tracked 账本）。",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "要分析的美股交易日 YYYY-MM-DD；默认取最近一个已收盘的美东交易日，"
            "若传入日期尚未收盘会自动回退。"
        ),
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=None,
        help="美股代码，可重复，例如 --symbol TSM.US；默认使用 US_TRACKED_STOCKS。",
    )
    parser.add_argument(
        "--base-dir",
        default="data",
        help="数据根目录（默认 data）。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="快照/研究包并发数（默认 2）。",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="跳过数据刷新，只用已有缓存生成研究包（排障用）。",
    )
    return parser


def main() -> int:
    install_network_timeouts()
    args = build_parser().parse_args()
    output_dir = run_us_research_pack(
        args.date,
        symbols=args.symbols,
        base_dir=args.base_dir,
        max_workers=args.max_workers,
        skip_refresh=args.skip_refresh,
    )
    print(f"美股研究包已生成: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
