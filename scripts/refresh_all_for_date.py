#!/usr/bin/env python3
"""一键刷新某个交易日的固定股票池 Python 链路数据，并打印后续 skill 清单。

典型用法（当天晚上 9 点，分析当天收盘，为下一交易日出预案）：

    python scripts/refresh_all_for_date.py                # --date 默认今天
    python scripts/refresh_all_for_date.py --date 2026-04-22
    python scripts/refresh_all_for_date.py --fresh-heavy  # 连财报结构化数据一起强刷
    python scripts/refresh_all_for_date.py --include-selection-universe
    python scripts/refresh_all_for_date.py --date 2026-08-09 --allow-non-trading-date
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger  # noqa: E402
from services.data_refresh.refresh_orchestrator import (  # noqa: E402
    format_followup_checklist,
    run_refresh_pipeline,
    summarize_result,
)


LOGGER = init_component_logger(
    "RefreshAllForDate",
    group="main_scripts",
    filename_prefix="refresh_all_for_date",
)


def _default_date() -> str:
    """默认分析日 = 今天。

    日常节奏是当天 21:00（A 股 15:00 收盘后）分析当天收盘、为下一交易日出预案，
    所以默认值就是 `today`，不做任何减一。
    """
    return date.today().strftime("%Y-%m-%d")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="一键刷新某个交易日固定股票池所需的宏观/行情/财报/新闻缓存等 Python 链路数据。",
    )
    parser.add_argument(
        "--date",
        default=_default_date(),
        help=(
            "要分析的日期 YYYY-MM-DD（默认为今天）。默认要求交易日；"
            "周末或节假日补充分析需同时传 --allow-non-trading-date。"
        ),
    )
    parser.add_argument(
        "--fresh-heavy",
        action="store_true",
        help="额外强刷财报结构化数据等重缓存（默认只强刷轻量数据）。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="basic_stock_info / 共享数据刷新并发度，默认 8。",
    )
    parser.add_argument(
        "--look-back-days",
        type=int,
        default=0,
        help="basic_stock_info 回看天数，默认 0。",
    )
    parser.add_argument(
        "--signature",
        default="",
        help="可选的下游脚本 signature。",
    )
    parser.add_argument(
        "--base-dir",
        default="data",
        help="选股系统 base_dir，默认 data。",
    )
    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="单步失败后继续执行后续步骤（默认失败即停）。",
    )
    parser.add_argument(
        "--include-selection-universe",
        action="store_true",
        help="额外恢复旧口径：连同 master_universe 与选股系统链路一起刷新；默认仅处理固定股票池。",
    )
    parser.add_argument(
        "--no-generate-prefilter",
        action="store_true",
        help="跳过量化初筛生成；默认会自动生成 factor_store 和 12_quant_prefilter。",
    )
    parser.add_argument(
        "--skip-news-boards",
        action="store_true",
        help="跳过新闻采集和板块热度分析步骤。",
    )
    parser.add_argument(
        "--allow-non-trading-date",
        action="store_true",
        help=(
            "显式允许在周末或节假日按该自然日刷新并生成分析输入；"
            "仅用于盘外补充宏观/新闻分析，不表示该日可以交易。"
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    run_date: str = args.date

    LOGGER.info(
        "开始一键刷新：date=%s, fresh_heavy=%s, max_workers=%d, include_selection_universe=%s, generate_prefilter=%s, allow_non_trading_date=%s",
        run_date,
        args.fresh_heavy,
        args.max_workers,
        args.include_selection_universe,
        not args.no_generate_prefilter,
        args.allow_non_trading_date,
    )

    result = run_refresh_pipeline(
        run_date,
        fresh_heavy=args.fresh_heavy,
        max_workers=args.max_workers,
        look_back_days=args.look_back_days,
        signature=args.signature,
        base_dir=args.base_dir,
        stop_on_failure=not args.continue_on_failure,
        include_selection_universe=args.include_selection_universe,
        generate_prefilter=not args.no_generate_prefilter,
        skip_news_boards=args.skip_news_boards,
        allow_non_trading_date=args.allow_non_trading_date,
    )

    print(summarize_result(result))

    if result.skipped_non_trading_date:
        LOGGER.info("目标日期不是交易日，已正常跳过且未生成后续 skill 清单。")
        return 0

    if result.succeeded:
        print(
            format_followup_checklist(
                run_date,
                include_selection_universe=args.include_selection_universe,
                allow_non_trading_date=args.allow_non_trading_date,
            )
        )
        LOGGER.info("一键刷新整体成功")
        return 0

    LOGGER.error("一键刷新存在失败步骤，请查看上方汇总并处理后重跑。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
