"""Pydantic schemas describing indicator batch inputs and outputs."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict

from utlity import SymbolInfo


class IndicatorSpec(BaseModel):
    """Describe a single indicator calculation."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: Literal[
        "price_snapshot",
        "pct_change",
        "macd",
        "rsi",
        "return_metrics",
        "correlation_matrix",
        "liquidity_profile",
        "fundamental_ttm",
    ]
    params: Dict[str, Any] = Field(default_factory=dict)
    alias: Optional[str] = None


class IndicatorBatchRequest(BaseModel):
    """Batch request for one symbol across multiple indicators."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    symbolInfo: SymbolInfo
    start_date: date
    end_date: date
    benchmarks: List[str] = Field(default_factory=list)
    frequency: Literal["daily", "weekly", "monthly"] = "daily"
    specs: List[IndicatorSpec]
    price_fields: List[str] = Field(
        default_factory=lambda: ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
    )
    context: Dict[str, Any] = Field(default_factory=dict)


class IndicatorBatchResult(BaseModel):
    """Container for indicator results."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    symbolInfo: SymbolInfo
    prices: Any
    tabular: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
