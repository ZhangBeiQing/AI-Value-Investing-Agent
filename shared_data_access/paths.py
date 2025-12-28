"""Helpers for deriving canonical cache paths."""

from __future__ import annotations

from pathlib import Path

from shared_data_access.cache_registry import CacheKind, build_cache_dir, SymbolInfo
from utlity import get_stock_data_dir, resolve_base_dir


def base_data_dir(base_dir: Path | str | None = None) -> Path:
    return resolve_base_dir(base_dir)


def stock_root(stock_name: str, symbol: str, base_dir: Path | str | None = None) -> Path:
    return get_stock_data_dir(stock_name, symbol, base_dir=base_dir)


def financial_cache_dir(symbolInfo: SymbolInfo, base_dir: Path | str | None = None) -> Path:
    return build_cache_dir(symbolInfo, CacheKind.FINANCIALS, base_dir=base_dir, ensure=False)


def price_cache_dir(symbolInfo: SymbolInfo, base_dir: Path | str | None = None) -> Path:
    return build_cache_dir(symbolInfo, CacheKind.PRICE_SERIES, base_dir=base_dir, ensure=False)


def global_cache_dir(base_dir: Path | str | None = None) -> Path:
    return base_data_dir(base_dir) / "global_cache"


__all__ = [
    "base_data_dir",
    "stock_root",
    "financial_cache_dir",
    "price_cache_dir",
    "global_cache_dir",
]
