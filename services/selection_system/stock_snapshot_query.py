"""Snapshot query helpers for the selection system."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import pandas as pd

from core.logging import get_logger
from services.snapshot.basic_snapshot import load_basic_snapshot_from_cache
from utlity.stock_utils import normalize_symbol

from .master_universe import load_master_universe
from .paths import SelectionSystemPaths


LOGGER = get_logger("SelectionStockSnapshot")

LOWER_IS_BETTER_FIELDS = {
    "pe_ttm",
    "pb",
    "ps",
    "pe_2y_percentile",
    "pe_current_vs_median",
    "volatility_3m",
    "volatility_6m",
    "volatility_1y",
}


def query_stock_snapshot(
    run_date: str,
    *,
    symbols: Sequence[str] | None = None,
    stock_names: Sequence[str] | None = None,
    base_dir: str | Path = "data",
) -> Dict[str, Any]:
    universe = load_master_universe(SelectionSystemPaths.from_base_dir(base_dir))
    requested_symbols, unmatched = _resolve_requested_symbols(
        universe,
        symbols=symbols or [],
        stock_names=stock_names or [],
    )
    payload = _build_snapshot_payload(run_date, requested_symbols, base_dir=base_dir)
    items: list[dict[str, Any]] = []
    stocks = payload.get("stocks") if isinstance(payload, Mapping) else {}
    field_notes = (payload.get("field_notes") or {}).get("stock_fields", {}) if isinstance(payload, Mapping) else {}
    for symbol in requested_symbols:
        stock_payload = dict((stocks or {}).get(symbol) or {})
        if not stock_payload:
            unmatched.append({"requested": symbol, "reason": "snapshot_missing"})
            continue
        items.append(
            {
                "symbol": symbol,
                "stock_name": stock_payload.get("stock_name") or _name_by_symbol(universe).get(symbol, symbol),
                "snapshot": stock_payload,
            }
        )
    return {
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "item_count": len(items),
        "items": items,
        "unmatched": unmatched,
        "field_notes": field_notes,
    }


def rank_stock_snapshot(
    run_date: str,
    *,
    field: str,
    top: int = 20,
    ascending: bool | None = None,
    base_dir: str | Path = "data",
) -> Dict[str, Any]:
    universe = load_master_universe(SelectionSystemPaths.from_base_dir(base_dir))
    df, field_notes = _build_snapshot_frame(run_date, [stock.symbol for stock in universe.stocks], base_dir=base_dir)
    if field not in df.columns:
        available = sorted(col for col in df.columns if col not in {"symbol", "stock_name"})
        raise ValueError(f"snapshot 字段不存在: {field}. 可用字段示例: {available[:20]}")

    effective_ascending = ascending if ascending is not None else field in LOWER_IS_BETTER_FIELDS
    ranked = df.dropna(subset=[field]).sort_values(field, ascending=effective_ascending).head(max(int(top), 0))
    items = ranked.to_dict(orient="records")
    return {
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "field": field,
        "ascending": effective_ascending,
        "top": top,
        "item_count": len(items),
        "items": items,
        "field_note": field_notes.get(field, ""),
    }


def filter_stock_snapshot(
    run_date: str,
    *,
    expr: str,
    base_dir: str | Path = "data",
) -> Dict[str, Any]:
    universe = load_master_universe(SelectionSystemPaths.from_base_dir(base_dir))
    df, field_notes = _build_snapshot_frame(run_date, [stock.symbol for stock in universe.stocks], base_dir=base_dir)
    if not expr.strip():
        raise ValueError("expr 不能为空")
    filtered = df.query(expr, engine="python")
    items = filtered.sort_values(["symbol"]).to_dict(orient="records")
    return {
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "expr": expr,
        "item_count": len(items),
        "items": items,
        "field_notes": field_notes,
    }


def _build_snapshot_frame(
    run_date: str,
    symbols: Sequence[str],
    *,
    base_dir: str | Path,
) -> tuple[pd.DataFrame, Dict[str, str]]:
    payload = _build_snapshot_payload(run_date, symbols, base_dir=base_dir)
    stocks = payload.get("stocks") if isinstance(payload, Mapping) else {}
    field_notes = (payload.get("field_notes") or {}).get("stock_fields", {}) if isinstance(payload, Mapping) else {}
    rows: list[dict[str, Any]] = []
    for symbol, stock_payload in (stocks or {}).items():
        if not isinstance(stock_payload, Mapping):
            continue
        row = {"symbol": symbol}
        row.update(dict(stock_payload))
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["symbol", "stock_name"]), field_notes
    ordered_columns = ["symbol", "stock_name"] + [col for col in df.columns if col not in {"symbol", "stock_name"}]
    return df[ordered_columns], field_notes


def _build_snapshot_payload(
    run_date: str,
    symbols: Sequence[str],
    *,
    base_dir: str | Path,
) -> Dict[str, Any]:
    if not symbols:
        return {"stocks": {}, "field_notes": {}}
    LOGGER.info("生成 snapshot 查询载荷: run_date=%s symbol_count=%d", run_date, len(symbols))
    return load_basic_snapshot_from_cache(
        symbols,
        run_date,
    )


def _resolve_requested_symbols(
    universe: Any,
    *,
    symbols: Sequence[str],
    stock_names: Sequence[str],
) -> tuple[list[str], list[dict[str, str]]]:
    universe_name_by_symbol = _name_by_symbol(universe)
    name_to_symbol = {
        stock.name.strip(): stock.symbol
        for stock in universe.stocks
        if stock.name.strip()
    }
    requested: list[str] = []
    unmatched: list[dict[str, str]] = []
    seen: set[str] = set()
    for symbol in symbols:
        raw_symbol = str(symbol or "").strip()
        if not raw_symbol:
            continue
        try:
            normalized = normalize_symbol(raw_symbol)
        except Exception:
            unmatched.append({"requested": raw_symbol, "reason": "invalid_symbol"})
            continue
        if normalized not in universe_name_by_symbol:
            unmatched.append({"requested": normalized, "reason": "symbol_not_in_master_universe"})
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        requested.append(normalized)
    for stock_name in stock_names:
        normalized_name = str(stock_name or "").strip()
        if not normalized_name:
            continue
        symbol = name_to_symbol.get(normalized_name)
        if symbol is None:
            unmatched.append({"requested": normalized_name, "reason": "stock_name_not_in_master_universe"})
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        requested.append(symbol)
    return requested, unmatched


def _name_by_symbol(universe: Any) -> Dict[str, str]:
    return {
        stock.symbol: stock.name
        for stock in universe.stocks
    }


__all__ = [
    "filter_stock_snapshot",
    "query_stock_snapshot",
    "rank_stock_snapshot",
]
