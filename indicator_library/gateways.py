"""Gateway layer that supplies normalized price/financial frames to the indicator library."""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, Iterable, Protocol, Sequence

import pandas as pd


class PriceSeriesGateway(Protocol):
    """Abstract source of normalized OHLCV frames."""

    def get_price_series(
        self,
        stock_name: str,
        start_date: date,
        end_date: date,
        fields: Sequence[str],
    ) -> pd.DataFrame:
        ...

    def get_metadata(self, symbol: str) -> Dict[str, str]:
        """Optional metadata hook."""
        return {}


class DataFrameGateway(PriceSeriesGateway):
    """Use an in-memory dataframe whose columns follow {name}_{field}."""

    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame.sort_index()

    def get_price_series(
        self,
        stock_name: str,
        start_date: date,
        end_date: date,
        fields: Sequence[str],
    ) -> pd.DataFrame:
        if self._frame.empty:
            return pd.DataFrame()
        start_ts = _ensure_timestamp(start_date)
        end_ts = _ensure_timestamp(end_date)
        sliced = self._frame.loc[(self._frame.index >= start_ts) & (self._frame.index <= end_ts)]
        result = pd.DataFrame(index=sliced.index)
        for field in fields:
            column = f"{stock_name}_{field}"
            if column in sliced.columns:
                result[field] = pd.to_numeric(sliced[column], errors="coerce")
        return result


def _ensure_timestamp(value: date | datetime) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value
    return pd.Timestamp(value)

