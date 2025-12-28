"""Momentum and oscillator style indicators."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

try:
    import talib as ta
except ImportError:  # pragma: no cover - talib is part of runtime deps
    ta = None  # type: ignore


def macd_indicator(
    price_df: pd.DataFrame,
    *,
    fastperiod: int = 12,
    slowperiod: int = 26,
    signalperiod: int = 9,
    column_name: str = "MACD",
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    if price_df.empty or "收盘" not in price_df.columns:
        return pd.DataFrame()
    if ta is None:
        _log_warning(logger, "talib 未安装，MACD 无法计算")
        return pd.DataFrame()
    close = price_df["收盘"].ffill().to_numpy()
    macd, _, _ = ta.MACD(close, fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
    return pd.DataFrame({column_name: macd}, index=price_df.index)


def rsi_indicator(
    price_df: pd.DataFrame,
    *,
    period: int = 14,
    column_name: str = "RSI(14)",
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    if price_df.empty or "收盘" not in price_df.columns:
        return pd.DataFrame()
    if ta is None:
        _log_warning(logger, "talib 未安装，RSI 无法计算")
        return pd.DataFrame()
    close = price_df["收盘"].ffill().to_numpy()
    if len(close) < period:
        return pd.DataFrame()
    rsi = ta.RSI(close, timeperiod=period)
    return pd.DataFrame({column_name: rsi}, index=price_df.index)


def pct_change_indicator(
    price_df: pd.DataFrame,
    *,
    column_name: str = "涨跌幅(%)",
) -> pd.DataFrame:
    if price_df.empty or "收盘" not in price_df.columns:
        return pd.DataFrame()
    pct = price_df["收盘"].pct_change(fill_method=None) * 100
    return pd.DataFrame({column_name: pct}, index=price_df.index)


def _log_warning(logger: logging.Logger | None, message: str) -> None:
    if logger is not None:
        logger.warning(message)

