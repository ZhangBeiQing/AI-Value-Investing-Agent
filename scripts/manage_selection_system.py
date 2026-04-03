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

    hot_news_parser = subparsers.add_parser(
        "update-gradual-hot-news-summary",
        help="Update today's gradual hot-news summary from daily news and recent state.",
    )
    hot_news_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    hot_news_parser.add_argument(
        "--model",
        default="deepseek-v3.2-exp",
        help="Theme extraction and theme-op planning model name.",
    )
    hot_news_parser.add_argument(
        "--embedding-model",
        default="text-embedding-v4",
        help="Embedding model used for theme retrieval.",
    )
    hot_news_parser.add_argument(
        "--candidate-limit",
        type=int,
        default=8,
        help="Max theme candidates extracted from today's news.",
    )
    hot_news_parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Rollback the same run_date from SQLite state before rebuilding.",
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
) -> int:
    from services.selection_system.board_heat import build_board_heat_state

    outputs = build_board_heat_state(
        run_date,
        base_dir=base_dir,
        top_n=top_n,
        stocks_per_board=stocks_per_board,
        model=model,
    )
    LOGGER.info("board heat state 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_build_hot_news_state(
    base_dir: str,
    run_date: str,
    model: str,
    embedding_model: str,
    candidate_limit: int,
    force_rebuild: bool,
) -> int:
    from services.selection_system.hot_news_state import build_hot_news_state

    outputs = build_hot_news_state(
        run_date,
        base_dir=base_dir,
        model=model,
        embedding_model=embedding_model,
        candidate_limit=candidate_limit,
        force_rebuild=force_rebuild,
    )
    LOGGER.info("hot news state 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
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
        )
    if args.command == "update-gradual-hot-news-summary":
        return _handle_build_hot_news_state(
            args.base_dir,
            args.date,
            args.model,
            args.embedding_model,
            args.candidate_limit,
            args.force_rebuild,
        )

    parser.error(f"未知命令: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
