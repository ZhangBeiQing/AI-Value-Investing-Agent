"""Input validation utilities shared across data access components."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from .exceptions import DataValidationError

SYMBOL_PATTERN = re.compile(r"^[0-9A-Z]+\.[A-Z]{2,4}$")


def normalize_symbol(symbol: str) -> str:
    """Return upper-cased CODE.SUFFIX strings; raise on mismatches.

    Args:
        symbol: Stock code with explicit suffix (e.g. ``002415.SZ``).
    """

    if not isinstance(symbol, str):
        raise DataValidationError("股票代码必须是字符串")
    candidate = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(candidate):
        raise DataValidationError(
            f"股票代码格式非法: {symbol!r}，需要 CODE.SUFFIX 形式"
        )
    return candidate


def normalize_stock_name(name: str) -> str:
    if not isinstance(name, str):
        raise DataValidationError("股票名称必须是字符串")
    candidate = name.strip()
    if not candidate:
        raise DataValidationError("股票名称不能为空")
    return candidate


def normalize_date(date_str: str) -> datetime:
    """Force ISO date strings (YYYY-MM-DD)."""

    if not isinstance(date_str, str):
        raise DataValidationError("日期必须是字符串")
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    except ValueError as exc:
        raise DataValidationError(
            f"日期格式非法: {date_str!r}，仅支持 YYYY-MM-DD"
        ) from exc


def normalize_optional_date(date_str: Optional[str]) -> Optional[datetime]:
    if date_str is None:
        return None
    return normalize_date(date_str)


__all__ = ["normalize_symbol", "normalize_stock_name", "normalize_date", "normalize_optional_date"]
