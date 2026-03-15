"""Selection-system helpers for constructing SymbolInfo without stock_pool coupling."""

from __future__ import annotations

from typing import Dict

from utlity.stock_utils import SYMBOL_SUFFIX_INFO, SymbolInfo, normalize_symbol

from .models import MasterUniverseDocument, MasterUniverseStock


def build_symbol_info(symbol: str, name: str, *, description: str = "") -> SymbolInfo:
    normalized = normalize_symbol(symbol)
    code, suffix = normalized.split(".", 1)
    metadata = SYMBOL_SUFFIX_INFO.get(suffix)
    if metadata is None:
        raise ValueError(f"不支持的 symbol 后缀: {suffix}")
    return SymbolInfo(
        symbol=normalized,
        code=code,
        suffix=suffix,
        market=metadata["market"],
        calendar=metadata["calendar"],
        stock_name=name.strip() or normalized,
        description=description,
    )


def stock_to_symbol_info(stock: MasterUniverseStock) -> SymbolInfo:
    return build_symbol_info(stock.symbol, stock.name)


def build_symbol_info_map(document: MasterUniverseDocument) -> Dict[str, SymbolInfo]:
    return {stock.symbol: stock_to_symbol_info(stock) for stock in document.stocks}
