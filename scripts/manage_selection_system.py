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

    chip_parser = subparsers.add_parser(
        "refresh-chip-distribution",
        help="Refresh cached chip-distribution raw data for master_universe or specified symbols.",
    )
    chip_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format, used for logging consistency.")
    chip_parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols to refresh. Default: all master_universe symbols.",
    )
    chip_parser.add_argument(
        "--adjust",
        default="qfq",
        choices=("", "qfq", "hfq"),
        help="AkShare adjustment mode for stock_cyq_em. Default: qfq.",
    )
    chip_parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore TTL and refresh chip-distribution cache.",
    )
    chip_parser.add_argument(
        "--prefer-local",
        action="store_true",
        help="Skip AkShare and compute chip distribution from local price.csv.",
    )
    chip_parser.add_argument(
        "--full-history",
        action="store_true",
        help="When using local calculation, compute all available price history instead of latest 90 rows.",
    )
    chip_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of symbols to refresh, useful for smoke tests.",
    )

    factor_scores_parser = subparsers.add_parser(
        "build-factor-scores",
        help="Build configured factor score outputs from 12_factor_snapshot.",
    )
    factor_scores_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    factor_scores_parser.add_argument(
        "--config",
        default="configs/selection_system/factor_scoring.yaml",
        help="Factor scoring YAML config path.",
    )
    factor_scores_parser.add_argument(
        "--max-staleness-days",
        type=int,
        default=10,
        help="Max days between requested date and cached basic snapshot date. Default: 10.",
    )
    factor_scores_parser.add_argument(
        "--no-build-factor-store",
        action="store_true",
        help="Do not auto-build factor store when 12_factor_snapshot is missing.",
    )

    quant_parser = subparsers.add_parser(
        "build-quant-prefilter",
        help="Build TopN quantitative prefilter outputs from factor snapshots.",
    )
    quant_parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    quant_parser.add_argument("--top-n", type=int, default=20, help="Top N per score bucket. Default: 20.")
    quant_parser.add_argument("--min-amount", type=float, default=0.5, help="Minimum amount/latest_volume filter. Default: 0.5.")
    quant_parser.add_argument("--min-liquidity-score", type=float, default=0.05, help="Minimum liquidity_score. Default: 0.05.")
    quant_parser.add_argument(
        "--max-staleness-days",
        type=int,
        default=10,
        help="Max days between requested date and cached basic snapshot date. Default: 10.",
    )
    quant_parser.add_argument(
        "--no-build-factor-store",
        action="store_true",
        help="Do not auto-build factor store when 12_factor_snapshot is missing.",
    )

    quant_backtest_parser = subparsers.add_parser(
        "backtest-quant-prefilter",
        help="Backtest TopN quantitative prefilter forward returns from factor-store by_date snapshots.",
    )
    quant_backtest_parser.add_argument("--start-date", help="Backtest start date YYYY-MM-DD. Default: first available factor date.")
    quant_backtest_parser.add_argument("--end-date", help="Backtest end date YYYY-MM-DD. Default: latest available factor date.")
    quant_backtest_parser.add_argument("--output-date", help="Selection run date used for output files. Default: end date.")
    quant_backtest_parser.add_argument("--top-n", type=int, default=20, help="Top N portfolio size. Default: 20.")
    quant_backtest_parser.add_argument(
        "--score-column",
        default="combined_score",
        choices=("combined_score", "short_score", "long_score"),
        help="Score column to rank by. Default: combined_score.",
    )
    quant_backtest_parser.add_argument(
        "--hold-days",
        default="1,3,5,10,20",
        help="Comma-separated holding periods in trading days. Default: 1,3,5,10,20.",
    )
    quant_backtest_parser.add_argument("--min-amount", type=float, default=0.5, help="Minimum amount/latest_volume filter. Default: 0.5.")
    quant_backtest_parser.add_argument("--min-liquidity-score", type=float, default=0.05, help="Minimum liquidity_score. Default: 0.05.")

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


def _handle_refresh_chip_distribution(
    base_dir: str,
    run_date: str,
    symbols: str,
    adjust: str,
    force_refresh: bool,
    prefer_local: bool,
    full_history: bool,
    limit: int,
) -> int:
    from shared_data_access.cache_registry import update_chip_distribution_cached
    from utlity.stock_utils import SymbolFormatError, parse_symbol

    paths = _selection_paths(base_dir)
    requested = [item.strip().upper() for item in symbols.split(",") if item.strip()]
    if requested:
        symbol_values = requested
    else:
        document = load_master_universe(paths)
        symbol_values = [stock.symbol for stock in document.stocks]
    if limit > 0:
        symbol_values = symbol_values[:limit]

    success_count = 0
    for symbol in symbol_values:
        try:
            symbol_info = parse_symbol(symbol)
        except SymbolFormatError as exc:
            LOGGER.warning("跳过非法 symbol: %s error=%s", symbol, exc)
            continue
        frame = update_chip_distribution_cached(
            symbol_info,
            adjust=adjust,
            base_data_dir=base_dir,
            logger=LOGGER,
            force_refresh=force_refresh,
            prefer_local=prefer_local,
            local_full_history=full_history,
        )
        if frame is not None and not frame.empty:
            success_count += 1

    LOGGER.info(
        "筹码分布刷新完成: run_date=%s requested=%d available=%d",
        run_date,
        len(symbol_values),
        success_count,
    )
    return 0


def _handle_build_factor_scores(
    base_dir: str,
    run_date: str,
    config_path: str,
    max_staleness_days: int,
    ensure_factor_store: bool,
) -> int:
    from services.selection_system.factor_scoring import FactorScoringConfig, build_factor_scores_for_date

    outputs = build_factor_scores_for_date(
        run_date,
        base_dir=base_dir,
        config=FactorScoringConfig(
            config_path=config_path,
            max_staleness_days=max_staleness_days,
        ),
        ensure_factor_store=ensure_factor_store,
    )
    LOGGER.info("factor scores 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_build_quant_prefilter(
    base_dir: str,
    run_date: str,
    top_n: int,
    min_amount: float,
    min_liquidity_score: float,
    max_staleness_days: int,
    ensure_factor_store: bool,
) -> int:
    from services.selection_system.quant_prefilter import QuantPrefilterConfig, build_quant_prefilter_for_date

    outputs = build_quant_prefilter_for_date(
        run_date,
        base_dir=base_dir,
        config=QuantPrefilterConfig(
            top_n=top_n,
            min_amount=min_amount,
            min_liquidity_score=min_liquidity_score,
            max_staleness_days=max_staleness_days,
        ),
        ensure_factor_store=ensure_factor_store,
    )
    LOGGER.info("quant prefilter 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
    return 0


def _handle_backtest_quant_prefilter(
    base_dir: str,
    start_date: str | None,
    end_date: str | None,
    output_date: str | None,
    top_n: int,
    score_column: str,
    hold_days: str,
    min_amount: float,
    min_liquidity_score: float,
) -> int:
    from services.selection_system.quant_prefilter import backtest_quant_prefilter, parse_hold_days

    outputs = backtest_quant_prefilter(
        base_dir=base_dir,
        start_date=start_date,
        end_date=end_date,
        output_date=output_date,
        top_n=top_n,
        score_column=score_column,
        hold_days=parse_hold_days(hold_days),
        min_amount=min_amount,
        min_liquidity_score=min_liquidity_score,
    )
    LOGGER.info("quant prefilter backtest 完成: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
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
    if args.command == "refresh-chip-distribution":
        return _handle_refresh_chip_distribution(
            args.base_dir,
            args.date,
            args.symbols,
            args.adjust,
            args.force_refresh,
            args.prefer_local,
            args.full_history,
            args.limit,
        )
    if args.command == "build-factor-scores":
        return _handle_build_factor_scores(
            args.base_dir,
            args.date,
            args.config,
            args.max_staleness_days,
            not args.no_build_factor_store,
        )
    if args.command == "build-quant-prefilter":
        return _handle_build_quant_prefilter(
            args.base_dir,
            args.date,
            args.top_n,
            args.min_amount,
            args.min_liquidity_score,
            args.max_staleness_days,
            not args.no_build_factor_store,
        )
    if args.command == "backtest-quant-prefilter":
        return _handle_backtest_quant_prefilter(
            args.base_dir,
            args.start_date,
            args.end_date,
            args.output_date,
            args.top_n,
            args.score_column,
            args.hold_days,
            args.min_amount,
            args.min_liquidity_score,
        )
    parser.error(f"未知命令: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
