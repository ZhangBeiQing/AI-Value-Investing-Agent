#!/usr/bin/env python3
"""Manage the first-stage selection system foundation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger
from services.selection_system import (
    SelectionSystemPaths,
    initialize_selection_system,
    load_master_universe,
)
from services.selection_system.master_universe import BootstrapMode


LOGGER = init_component_logger(
    "SelectionSystem",
    group="selection_system",
    filename_prefix="manage_selection_system",
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the stage-1 stock selection system foundation.")
    parser.add_argument(
        "--base-dir",
        default="data",
        help="Selection system base data directory. Default: data",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize selection-system directories and state files.")
    init_parser.add_argument(
        "--bootstrap",
        choices=("stock_pool", "empty"),
        default="stock_pool",
        help="How to initialize master_universe. Default: stock_pool",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing selection-system bootstrap files.",
    )

    subparsers.add_parser("validate-universe", help="Validate the master_universe file.")

    summary_parser = subparsers.add_parser("show-universe", help="Show a compact universe summary.")
    summary_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of leading stocks to show. Default: 10",
    )

    news_parser = subparsers.add_parser("run-news", help="Run the standalone news acquisition/dedup/enrichment pipeline.")
    news_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    news_parser.add_argument("--model", default="deepseek-v3.2-exp", help="Dedup model name.")
    news_parser.add_argument("--batch-size", type=int, default=20, help="Batch size for LLM dedup.")

    market_parser = subparsers.add_parser(
        "run-signals",
        aliases=["run-market-signals"],
        help="Run standalone board-change and stock-heat ingestion.",
    )
    market_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    market_parser.add_argument("--board-limit", type=int, default=60, help="Max board items to keep.")
    market_parser.add_argument(
        "--stock-limit",
        "--stock-heat-limit",
        dest="stock_limit",
        type=int,
        default=100,
        help="Max stock heat items to keep.",
    )

    board_heat_parser = subparsers.add_parser(
        "build-board-heat-state",
        help="Build board heat candidates and DeepSeek board research output.",
    )
    board_heat_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    board_heat_parser.add_argument("--top-n", type=int, default=3, help="Top rising/falling boards to keep.")
    board_heat_parser.add_argument(
        "--stocks-per-board",
        type=int,
        default=3,
        help="How many stock hints to attach per board.",
    )
    board_heat_parser.add_argument(
        "--model",
        default="deepseek-v3.2-exp",
        help="Deep research model name.",
    )
    board_heat_parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force refresh the board metrics cache snapshot.",
    )

    announcements_parser = subparsers.add_parser(
        "build-announcements",
        help="Build recent company-announcement summaries for the master universe.",
    )
    announcements_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    announcements_parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Lookback window for announcements. Default: 30",
    )
    announcements_parser.add_argument(
        "--max-items-per-symbol",
        type=int,
        default=6,
        help="Max announcement titles kept per symbol. Default: 6",
    )
    announcements_parser.add_argument(
        "--refresh-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Deprecated flag. build-announcements now always updates all universe symbols before aggregation.",
    )
    announcements_parser.add_argument(
        "--force-refresh-disclosures",
        action="store_true",
        help="Deprecated compatibility flag. build-announcements already refreshes all universe symbols before aggregation.",
    )

    shared_context_parser = subparsers.add_parser(
        "build-shared-context",
        help="Build the shared selection-context markdown from current upstream artifacts.",
    )
    shared_context_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    shared_context_parser.add_argument(
        "--announcement-limit",
        type=int,
        default=24,
        help="How many announcement rows to include in the markdown. Default: 24",
    )

    candidate_parser = subparsers.add_parser(
        "build-candidate-pools",
        help="Prepare local-agent inputs for short/long candidate selection.",
    )
    candidate_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    candidate_parser.add_argument("--short-count", type=int, default=15, help="Short-book candidate count.")
    candidate_parser.add_argument("--long-count", type=int, default=15, help="Long-book candidate count.")

    merge_parser = subparsers.add_parser(
        "merge-candidates",
        help="Merge short/long pools and build the deep-research queue.",
    )
    merge_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")

    factor_parser = subparsers.add_parser(
        "build-factor-store",
        help="Build by-symbol and by-date factor-store snapshots from local caches.",
    )
    factor_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    factor_parser.add_argument(
        "--max-staleness-days",
        type=int,
        default=10,
        help="Max days between requested date and cached basic snapshot date. Default: 10.",
    )
    factor_parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Only write CSV/JSON outputs, skip parquet.",
    )

    return parser


def _selection_paths(base_dir: str) -> SelectionSystemPaths:
    return SelectionSystemPaths.from_base_dir(base_dir)


def _handle_init(base_dir: str, bootstrap: str, force: bool) -> int:
    bootstrap_mode: BootstrapMode = "empty" if bootstrap == "empty" else "stock_pool"
    paths = initialize_selection_system(
        _selection_paths(base_dir),
        bootstrap_mode=bootstrap_mode,
        force=force,
    )
    LOGGER.info("selection system 初始化完成")
    LOGGER.info("master_universe: %s", paths.master_universe_path)
    LOGGER.info("market_state: %s", paths.market_state_dir)
    LOGGER.info("symbol_memory: %s", paths.symbol_memory_dir)
    LOGGER.info("selection_runs: %s", paths.selection_runs_dir)
    return 0


def _handle_validate_universe(base_dir: str) -> int:
    paths = _selection_paths(base_dir)
    document = load_master_universe(paths)
    LOGGER.info("master_universe 校验通过: %s", paths.master_universe_path)
    LOGGER.info("summary: %s", json.dumps(document.summary(), ensure_ascii=False))
    return 0


def _handle_show_universe(base_dir: str, limit: int) -> int:
    paths = _selection_paths(base_dir)
    document = load_master_universe(paths)
    summary = document.summary()
    LOGGER.info("master_universe summary: %s", json.dumps(summary, ensure_ascii=False))
    preview = [stock.to_dict() for stock in document.stocks[: max(limit, 0)]]
    LOGGER.info("master_universe preview: %s", json.dumps(preview, ensure_ascii=False, indent=2))
    return 0


def _handle_run_news(
    base_dir: str,
    run_date: str,
    model: str,
    batch_size: int,
) -> int:
    from services.selection_system.news_curation import run_news_curation_pipeline

    outputs = run_news_curation_pipeline(
        run_date,
        base_dir=base_dir,
        model=model,
        batch_size=batch_size,
    )
    LOGGER.info("news curation 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_run_market_signals(
    base_dir: str,
    run_date: str,
    board_limit: int,
    stock_limit: int,
) -> int:
    from services.selection_system.market_signals import run_market_signals_pipeline

    outputs = run_market_signals_pipeline(
        run_date,
        base_dir=base_dir,
        board_limit=board_limit,
        stock_limit=stock_limit,
    )
    LOGGER.info("market signals 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_build_board_heat_state(
    base_dir: str,
    run_date: str,
    top_n: int,
    stocks_per_board: int,
    model: str,
    force_refresh: bool,
) -> int:
    from services.selection_system.board_heat import build_board_heat_state

    outputs = build_board_heat_state(
        run_date,
        base_dir=base_dir,
        top_n=top_n,
        stocks_per_board=stocks_per_board,
        model=model,
        force_refresh=force_refresh,
    )
    LOGGER.info("board heat state 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_build_announcements(
    base_dir: str,
    run_date: str,
    lookback_days: int,
    max_items_per_symbol: int,
    refresh_missing: bool,
    force_refresh_disclosures: bool,
) -> int:
    from services.selection_system.announcement_summary import build_recent_company_announcements

    outputs = build_recent_company_announcements(
        run_date,
        base_dir=base_dir,
        lookback_days=lookback_days,
        max_items_per_symbol=max_items_per_symbol,
        refresh_missing=refresh_missing,
        force_refresh_disclosures=force_refresh_disclosures,
    )
    LOGGER.info("recent announcements 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_build_shared_context(
    base_dir: str,
    run_date: str,
    announcement_limit: int,
) -> int:
    from services.selection_system.shared_context import build_shared_selection_context

    outputs = build_shared_selection_context(
        run_date,
        base_dir=base_dir,
        announcement_limit=announcement_limit,
    )
    LOGGER.info("shared selection context 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_build_candidate_pools(
    base_dir: str,
    run_date: str,
    short_count: int,
    long_count: int,
) -> int:
    from services.selection_system.candidate_selection import build_candidate_pools

    outputs = build_candidate_pools(
        run_date,
        base_dir=base_dir,
        short_count=short_count,
        long_count=long_count,
    )
    LOGGER.info("candidate pools 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_merge_candidates(
    base_dir: str,
    run_date: str,
) -> int:
    from services.selection_system.candidate_selection import merge_candidate_pools

    outputs = merge_candidate_pools(
        run_date,
        base_dir=base_dir,
    )
    LOGGER.info("candidate merge 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_build_factor_store(
    base_dir: str,
    run_date: str,
    max_staleness_days: int,
    write_parquet: bool,
) -> int:
    from services.selection_system.factor_store import FactorStoreConfig, build_factor_store_for_date

    outputs = build_factor_store_for_date(
        run_date,
        base_dir=base_dir,
        config=FactorStoreConfig(
            max_staleness_days=max_staleness_days,
            write_parquet=write_parquet,
            write_csv=True,
        ),
    )
    LOGGER.info("factor store 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "init":
        return _handle_init(args.base_dir, args.bootstrap, args.force)
    if args.command == "validate-universe":
        return _handle_validate_universe(args.base_dir)
    if args.command == "show-universe":
        return _handle_show_universe(args.base_dir, args.limit)
    if args.command == "run-news":
        return _handle_run_news(
            args.base_dir,
            args.date,
            args.model,
            args.batch_size,
        )
    if args.command in {"run-signals", "run-market-signals"}:
        return _handle_run_market_signals(
            args.base_dir,
            args.date,
            args.board_limit,
            args.stock_limit,
        )
    if args.command == "build-board-heat-state":
        return _handle_build_board_heat_state(
            args.base_dir,
            args.date,
            args.top_n,
            args.stocks_per_board,
            args.model,
            args.force_refresh,
        )
    if args.command == "build-announcements":
        return _handle_build_announcements(
            args.base_dir,
            args.date,
            args.lookback_days,
            args.max_items_per_symbol,
            args.refresh_missing,
            args.force_refresh_disclosures,
        )
    if args.command == "build-shared-context":
        return _handle_build_shared_context(
            args.base_dir,
            args.date,
            args.announcement_limit,
        )
    if args.command == "build-candidate-pools":
        return _handle_build_candidate_pools(
            args.base_dir,
            args.date,
            args.short_count,
            args.long_count,
        )
    if args.command == "merge-candidates":
        return _handle_merge_candidates(
            args.base_dir,
            args.date,
        )
    if args.command == "build-factor-store":
        return _handle_build_factor_store(
            args.base_dir,
            args.date,
            args.max_staleness_days,
            not args.no_parquet,
        )
    parser.error(f"未知命令: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
