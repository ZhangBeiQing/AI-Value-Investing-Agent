"""Typed containers for shared data access results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from utlity import SymbolInfo

@dataclass(frozen=True)
class FinancialDataBundle:
    profit_sheet: pd.DataFrame
    balance_sheet: pd.DataFrame
    cash_flow_sheet: pd.DataFrame
    stock_price: pd.DataFrame
    financial_abstract: pd.DataFrame


@dataclass(frozen=True)
class PriceDataBundle:
    """Container for price data with a specified time range.
    
    Attributes:
        frame: DataFrame containing price data with 'date' as index and 'close' as column
        start: Start date of the price data range (inclusive)
        end: End date of the price data range (inclusive)
        source_path: Optional path to the source data file
    """
    frame: pd.DataFrame
    start: datetime
    end: datetime
    source_path: Optional[Path] = None


@dataclass(frozen=True)
class ShareInfo:
    total_shares: float
    float_shares: float
    source: str


@dataclass(frozen=True)
class DisclosureBundle:
    frame: pd.DataFrame
    start: datetime
    end: datetime
    source_path: Optional[Path] = None


@dataclass(frozen=True)
class PreparedData:
    symbolInfo: SymbolInfo
    as_of: datetime
    financials: FinancialDataBundle
    prices: PriceDataBundle
    share_info: ShareInfo
    disclosures: Optional[DisclosureBundle] = None


__all__ = [
    "FinancialDataBundle",
    "PriceDataBundle",
    "ShareInfo",
    "DisclosureBundle",
    "PreparedData",
]
