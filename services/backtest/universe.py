"""Build the daily research universe without giving quant candidates trade authority."""

from __future__ import annotations

import csv
import shutil
from pathlib import Path

from core.logging import get_logger
from services.backtest.experiment import BacktestExperiment
from services.backtest.ledger import BacktestLedger
from services.selection_system.quant_prefilter import build_quant_prefilter_for_date
from shared_data_access.historical_prices import known_not_listed_as_of
from utlity import parse_symbol


LOGGER = get_logger("BacktestUniverse")
LONG_PREFILTER_FILENAME = "12_quant_prefilter_long.csv"


def _read_symbols(path: Path) -> list[str]:
    if not path.exists():
        return []
    symbols: list[str] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            symbol = str(row.get("symbol") or "").strip()
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    return symbols


def materialize_long_prefilter(
    experiment: BacktestExperiment,
    run_date: str,
) -> Path | None:
    target_dir = experiment.context.selection_runs_root / run_date
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / LONG_PREFILTER_FILENAME
    if target.exists():
        return target
    source = (
        experiment.context.source_data_root
        / "selection_runs"
        / run_date
        / LONG_PREFILTER_FILENAME
    )
    if not source.exists():
        try:
            outputs = build_quant_prefilter_for_date(
                run_date,
                base_dir=experiment.root,
                source_base_dir=experiment.context.source_data_root,
                ensure_factor_store=False,
            )
        except FileNotFoundError:
            LOGGER.warning(
                "缺少可用于 %s 的历史因子快照，无法生成长期量化候选。",
                run_date,
            )
            return None
        generated = outputs.get("quant_prefilter_long_csv")
        return generated if generated and generated.exists() else None
    shutil.copy2(source, target)
    return target


def build_daily_universe(
    experiment: BacktestExperiment,
    run_date: str,
    ledger: BacktestLedger,
) -> tuple[list[str], dict[str, object]]:
    prefilter_path = materialize_long_prefilter(experiment, run_date)
    long_symbols = _read_symbols(prefilter_path) if prefilter_path else []
    held_symbols = ledger.held_symbols(on_or_before=run_date)
    ordered: list[str] = []
    not_listed: list[dict[str, str]] = []
    held_set = set(held_symbols)
    for group in (experiment.base_universe, long_symbols, held_symbols):
        for symbol in group:
            if not symbol or symbol in ordered:
                continue
            cached_first_date = known_not_listed_as_of(
                parse_symbol(symbol),
                run_date,
                base_dir=experiment.context.source_data_root,
            )
            if (
                symbol not in held_set
                and cached_first_date is not None
            ):
                not_listed.append(
                    {
                        "symbol": symbol,
                        "first_trading_date": cached_first_date,
                    }
                )
                continue
            ordered.append(symbol)
    metadata: dict[str, object] = {
        "run_date": run_date,
        "base_universe_count": len(experiment.base_universe),
        "long_prefilter_path": str(prefilter_path) if prefilter_path else None,
        "long_candidate_count": len(long_symbols),
        "held_symbol_count": len(held_symbols),
        "symbol_count": len(ordered),
        "symbols": ordered,
        "long_candidates_missing": prefilter_path is None,
        "not_listed_as_of_date": not_listed,
        "not_listed_as_of_date_count": len(not_listed),
        "note": "12_quant_prefilter_long.csv 只扩展研究范围，不直接产生交易动作。",
    }
    LOGGER.info(
        "回测当日股票集合: date=%s base=%d long=%d held=%d not_listed=%d total=%d",
        run_date,
        len(experiment.base_universe),
        len(long_symbols),
        len(held_symbols),
        len(not_listed),
        len(ordered),
    )
    return ordered, metadata


__all__ = [
    "LONG_PREFILTER_FILENAME",
    "build_daily_universe",
    "materialize_long_prefilter",
]
