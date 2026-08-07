"""Read-only, as-of historical price access for replay and backtests."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

from shared_data_access.paths import price_cache_dir
from utlity import SymbolInfo


DATE_COLUMN = "日期"
NUMERIC_COLUMNS = ("开盘", "最高", "最低", "收盘", "成交量", "成交额", "换手率")


def load_price_history(
    symbol_info: SymbolInfo,
    *,
    base_dir: str | Path,
    as_of_date: str | date | None = None,
) -> pd.DataFrame:
    """Load the shared price cache without refreshing it, then apply a read cutoff."""

    price_file = price_cache_dir(symbol_info, base_dir=base_dir) / "price.csv"
    if not price_file.exists():
        return pd.DataFrame()
    frame = pd.read_csv(price_file)
    if DATE_COLUMN not in frame.columns:
        return pd.DataFrame()
    frame[DATE_COLUMN] = pd.to_datetime(frame[DATE_COLUMN], errors="coerce")
    frame = frame.dropna(subset=[DATE_COLUMN]).sort_values(DATE_COLUMN)
    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if as_of_date is not None:
        cutoff = pd.Timestamp(as_of_date).normalize()
        frame = frame.loc[frame[DATE_COLUMN] <= cutoff]
    return frame.reset_index(drop=True)


def exact_price(
    symbol_info: SymbolInfo,
    target_date: str | date,
    field: str,
    *,
    base_dir: str | Path,
) -> float | None:
    """Return an exact-date cached price; never fall back to an earlier row."""

    if field not in NUMERIC_COLUMNS:
        raise ValueError(f"不支持的行情字段: {field}")
    frame = load_price_history(symbol_info, base_dir=base_dir, as_of_date=target_date)
    if frame.empty or field not in frame.columns:
        return None
    target = pd.Timestamp(target_date).normalize()
    rows = frame.loc[frame[DATE_COLUMN].dt.normalize() == target]
    if rows.empty:
        return None
    value = rows.iloc[-1][field]
    return float(value) if pd.notna(value) else None


def first_trading_date(
    symbol_info: SymbolInfo,
    *,
    base_dir: str | Path,
) -> str | None:
    """Return the first date present in the canonical price cache."""

    frame = load_price_history(symbol_info, base_dir=base_dir)
    if frame.empty:
        return None
    return frame.iloc[0][DATE_COLUMN].strftime("%Y-%m-%d")


def price_cache_coverage_start(
    symbol_info: SymbolInfo,
    *,
    base_dir: str | Path,
) -> str | None:
    """Return the requested history start recorded by the price cache."""

    meta_path = (
        price_cache_dir(symbol_info, base_dir=base_dir)
        / ".cache_registry_meta.json"
    )
    if not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    raw = payload.get("requested_start_date")
    parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).strftime("%Y-%m-%d")


def known_not_listed_as_of(
    symbol_info: SymbolInfo,
    target_date: str | date,
    *,
    base_dir: str | Path,
) -> str | None:
    """Return first trading date only when cache coverage proves pre-listing."""

    target = pd.Timestamp(target_date).strftime("%Y-%m-%d")
    coverage_start = price_cache_coverage_start(
        symbol_info,
        base_dir=base_dir,
    )
    first_date = first_trading_date(symbol_info, base_dir=base_dir)
    if (
        coverage_start is not None
        and first_date is not None
        and coverage_start <= target < first_date
    ):
        return first_date
    return None


def close_on_or_before(
    symbol_info: SymbolInfo,
    target_date: str | date,
    *,
    base_dir: str | Path,
) -> tuple[str, float] | None:
    """Return the latest close visible on or before target_date."""

    frame = load_price_history(symbol_info, base_dir=base_dir, as_of_date=target_date)
    if frame.empty or "收盘" not in frame.columns:
        return None
    valid = frame.dropna(subset=["收盘"])
    if valid.empty:
        return None
    row = valid.iloc[-1]
    return row[DATE_COLUMN].strftime("%Y-%m-%d"), float(row["收盘"])


def next_trading_date(
    symbol_info: SymbolInfo,
    after_date: str | date,
    *,
    base_dir: str | Path,
    end_date: str | date | None = None,
) -> str | None:
    """Return the first cached session strictly after after_date."""

    frame = load_price_history(symbol_info, base_dir=base_dir)
    if frame.empty:
        return None
    after = pd.Timestamp(after_date).normalize()
    rows = frame.loc[frame[DATE_COLUMN].dt.normalize() > after]
    if end_date is not None:
        rows = rows.loc[rows[DATE_COLUMN].dt.normalize() <= pd.Timestamp(end_date).normalize()]
    if rows.empty:
        return None
    return rows.iloc[0][DATE_COLUMN].strftime("%Y-%m-%d")


def trading_dates_between(
    symbol_info: SymbolInfo,
    start_date: str | date,
    end_date: str | date,
    *,
    base_dir: str | Path,
) -> list[str]:
    """Return cached sessions in an inclusive date range."""

    frame = load_price_history(symbol_info, base_dir=base_dir, as_of_date=end_date)
    if frame.empty:
        return []
    start = pd.Timestamp(start_date).normalize()
    rows = frame.loc[frame[DATE_COLUMN].dt.normalize() >= start]
    return rows[DATE_COLUMN].dt.strftime("%Y-%m-%d").drop_duplicates().tolist()


def union_trading_dates(
    symbol_infos: Iterable[SymbolInfo],
    start_date: str | date,
    end_date: str | date,
    *,
    base_dir: str | Path,
) -> list[str]:
    dates: set[str] = set()
    for symbol_info in symbol_infos:
        dates.update(
            trading_dates_between(
                symbol_info,
                start_date,
                end_date,
                base_dir=base_dir,
            )
        )
    return sorted(dates)


__all__ = [
    "close_on_or_before",
    "exact_price",
    "first_trading_date",
    "known_not_listed_as_of",
    "load_price_history",
    "next_trading_date",
    "price_cache_coverage_start",
    "trading_dates_between",
    "union_trading_dates",
]
