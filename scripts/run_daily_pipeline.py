#!/usr/bin/env python3
"""Run the daily skill pipeline (steps 1-4)."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.pipeline.daily_pipeline import SKILL_FLOW_CONFIG, run_daily_pipeline
from shared_data_access.market_calendar import NonTradingDayError


def main() -> int:
    parser = argparse.ArgumentParser(description="运行 daily pipeline；默认仅生成综合 fixed_tracked，显式指定时加跑 short_book。")
    parser.add_argument(
        "--date",
        dest="run_date",
        default=date.today().strftime("%Y-%m-%d"),
        help="Run date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--base-dir",
        default="data",
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--prompt-config",
        default=str(SKILL_FLOW_CONFIG),
        help="Prompt source path (fixed_tracked 默认: main_policy.md).",
    )
    parser.add_argument(
        "--signature",
        default="",
        help="Agent signature used for historical context and trade summary files.",
    )
    parser.add_argument(
        "--manifest",
        default="auto",
        help="Run manifest path or 'auto' to build from selection outputs.",
    )
    parser.add_argument(
        "--all-books",
        action="store_true",
        help="启用全部交易账本：生成综合 fixed_tracked + short_book。long_book 只作为 fixed_tracked 的候选来源，不单独生成。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Max concurrent workers for snapshot/research generation.",
    )
    parser.add_argument(
        "--skip-disclosures",
        action="store_true",
        help="跳过公告（disclosures）刷新阶段，加速 pipeline。",
    )
    parser.add_argument(
        "--allow-non-trading-date",
        action="store_true",
        help=(
            "显式允许在周末或节假日按该自然日生成分析产物；"
            "仅用于盘外补充宏观/新闻分析，不表示该日可以交易。"
        ),
    )
    args = parser.parse_args()

    prompt_config = args.prompt_config
    if args.all_books or args.manifest != "auto":
        prompt_config = None

    try:
        run_daily_pipeline(
            args.run_date,
            base_dir=args.base_dir,
            prompt_config=prompt_config,
            signature=args.signature,
            manifest_path=args.manifest,
            max_workers=args.max_workers,
            skip_disclosures=args.skip_disclosures,
            all_books=args.all_books,
            allow_non_trading_date=args.allow_non_trading_date,
        )
    except NonTradingDayError as exc:
        print(f"SKIPPED: {exc}")
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
