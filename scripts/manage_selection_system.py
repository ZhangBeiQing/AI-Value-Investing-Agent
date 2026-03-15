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

    run_parser = subparsers.add_parser("run-daily", help="Run the stage-1 selection system daily pipeline.")
    run_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    run_parser.add_argument("--include-live-feeds", action="store_true", help="Pull AkShare live market feeds.")
    run_parser.add_argument("--top-hot", type=int, default=12, help="Number of hot candidates.")
    run_parser.add_argument("--top-core", type=int, default=12, help="Number of core candidates.")
    run_parser.add_argument("--max-workers", type=int, default=6, help="Max workers for snapshot building.")
    run_parser.add_argument("--signature", default="", help="Optional trade signature for holdings guardrail.")
    run_parser.add_argument("--cache-only", action="store_true", help="Use local caches only and skip missing snapshot refresh.")
    return parser


def _selection_paths(base_dir: str) -> SelectionSystemPaths:
    return SelectionSystemPaths.from_base_dir(base_dir)


def _handle_init(base_dir: str, bootstrap: str, force: bool) -> int:
    paths = initialize_selection_system(
        _selection_paths(base_dir),
        bootstrap_mode=bootstrap,
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


def _handle_run_daily(
    base_dir: str,
    run_date: str,
    include_live_feeds: bool,
    top_hot: int,
    top_core: int,
    max_workers: int,
    signature: str,
    cache_only: bool,
) -> int:
    from services.selection_system.daily_pipeline import run_selection_pipeline

    run_dir = run_selection_pipeline(
        run_date,
        base_dir=base_dir,
        include_live_feeds=include_live_feeds,
        top_hot=top_hot,
        top_core=top_core,
        max_workers=max_workers,
        signature=signature,
        cache_only=cache_only,
    )
    LOGGER.info("selection run 完成: %s", run_dir)
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
    if args.command == "run-daily":
        return _handle_run_daily(
            args.base_dir,
            args.date,
            args.include_live_feeds,
            args.top_hot,
            args.top_core,
            args.max_workers,
            args.signature,
            args.cache_only,
        )

    parser.error(f"未知命令: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
