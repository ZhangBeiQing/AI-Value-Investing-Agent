"""Frozen current fixed-book scope, plus existing isolated holdings."""

from __future__ import annotations

from core.logging import get_logger
from services.backtest.experiment import BacktestExperiment
from services.backtest.ledger import BacktestLedger
from shared_data_access.historical_prices import known_not_listed_as_of
from commons import parse_symbol


LOGGER = get_logger("BacktestUniverse")


def build_daily_universe(
    experiment: BacktestExperiment,
    run_date: str,
    ledger: BacktestLedger,
) -> tuple[list[str], dict[str, object]]:
    held_symbols = ledger.held_symbols(on_or_before=run_date)
    excluded = set(experiment.excluded_symbols)
    ordered: list[str] = []
    not_listed: list[dict[str, str]] = []
    held_set = set(held_symbols)
    for group in (experiment.base_universe, held_symbols):
        for symbol in group:
            if not symbol or symbol in ordered or symbol in excluded:
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
        "held_symbol_count": len(held_symbols),
        "symbol_count": len(ordered),
        "symbols": ordered,
        "not_listed_as_of_date": not_listed,
        "not_listed_as_of_date_count": len(not_listed),
        "note": "仅使用创建实验时冻结的当前 fixed_tracked 股池和隔离持仓，不并入选股候选。",
    }
    LOGGER.info(
        "回测当日股票集合: date=%s base=%d held=%d not_listed=%d total=%d",
        run_date,
        len(experiment.base_universe),
        len(held_symbols),
        len(not_listed),
        len(ordered),
    )
    return ordered, metadata


__all__ = ["build_daily_universe"]
