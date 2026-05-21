"""Local CYQ chip-distribution calculation from cached price data."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


OUTPUT_COLUMNS = [
    "日期",
    "获利比例",
    "平均成本",
    "90成本-低",
    "90成本-高",
    "90集中度",
    "70成本-低",
    "70成本-高",
    "70集中度",
]


def build_chip_distribution_from_price_frame(
    price_frame: pd.DataFrame,
    *,
    max_output_rows: int | None = 90,
    source_lookback_rows: int | None = 210,
    cyq_window: int = 120,
    factor: int = 150,
) -> pd.DataFrame:
    """Compute CYQ summary rows using the same core algorithm as AkShare.

    The input frame must contain daily OHLC and turnover data. The returned
    columns intentionally match ``akshare.stock_cyq_em`` so downstream factor
    code can read one unified cache.
    """

    frame = _normalize_price_frame(price_frame)
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    if source_lookback_rows is not None and source_lookback_rows > 0:
        min_rows = cyq_window + (max_output_rows or 0)
        effective_rows = max(source_lookback_rows, min_rows)
        frame = frame.tail(effective_rows).reset_index(drop=True)

    records: list[dict[str, Any]] = []
    for index in range(len(frame)):
        row = _compute_cyq_row(frame, index, cyq_window=cyq_window, factor=factor)
        if row:
            records.append(row)

    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    if max_output_rows is not None and max_output_rows > 0:
        result = result.tail(max_output_rows)
    return result.reset_index(drop=True)


def build_chip_distribution_from_price_csv(
    price_csv_path: str,
    *,
    max_output_rows: int | None = 90,
    source_lookback_rows: int | None = 210,
    cyq_window: int = 120,
    factor: int = 150,
) -> pd.DataFrame:
    frame = pd.read_csv(price_csv_path)
    return build_chip_distribution_from_price_frame(
        frame,
        max_output_rows=max_output_rows,
        source_lookback_rows=source_lookback_rows,
        cyq_window=cyq_window,
        factor=factor,
    )


def _normalize_price_frame(price_frame: pd.DataFrame) -> pd.DataFrame:
    if price_frame is None or price_frame.empty:
        return pd.DataFrame()

    required = ["日期", "开盘", "收盘", "最高", "最低"]
    if not all(col in price_frame.columns for col in required):
        return pd.DataFrame()

    frame = price_frame.copy()
    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    for col in ("开盘", "收盘", "最高", "最低", "换手率"):
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=required)
    frame = frame.sort_values("日期").reset_index(drop=True)
    if "换手率" not in frame.columns:
        frame["换手率"] = 0.0
    frame["换手率"] = frame["换手率"].fillna(0.0).map(_normalize_turnover)
    return frame


def _normalize_turnover(value: Any) -> float:
    try:
        turnover = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(turnover) or turnover <= 0:
        return 0.0
    if turnover > 1:
        turnover = turnover / 100.0
    return min(turnover, 1.0)


def _compute_cyq_row(
    frame: pd.DataFrame,
    index: int,
    *,
    cyq_window: int,
    factor: int,
) -> dict[str, Any]:
    start = max(0, index - cyq_window + 1)
    window = frame.iloc[start : index + 1]
    if window.empty:
        return {}

    max_price = float(window["最高"].max())
    min_price = float(window["最低"].min())
    if not np.isfinite(max_price) or not np.isfinite(min_price):
        return {}

    accuracy = max(0.01, (max_price - min_price) / (factor - 1))
    xdata = np.zeros(factor, dtype=float)

    for _, row in window.iterrows():
        open_price = float(row["开盘"])
        close_price = float(row["收盘"])
        high_price = float(row["最高"])
        low_price = float(row["最低"])
        turnover = _normalize_turnover(row.get("换手率"))
        if not all(np.isfinite(value) for value in (open_price, close_price, high_price, low_price)):
            continue

        average_price = (open_price + close_price + high_price + low_price) / 4.0
        xdata *= 1.0 - turnover

        if abs(high_price - low_price) < 1e-12:
            idx = _clip_index(np.floor((average_price - min_price) / accuracy), factor)
            xdata[idx] += (factor - 1) * turnover / 2.0
            continue

        high_idx = _clip_index(np.floor((high_price - min_price) / accuracy), factor)
        low_idx = _clip_index(np.ceil((low_price - min_price) / accuracy), factor)
        slope = 2.0 / (high_price - low_price)

        for price_idx in range(low_idx, high_idx + 1):
            current_price = min_price + accuracy * price_idx
            if current_price <= average_price:
                if abs(average_price - low_price) < 1e-12:
                    increment = slope * turnover
                else:
                    increment = (current_price - low_price) / (average_price - low_price) * slope * turnover
            else:
                if abs(high_price - average_price) < 1e-12:
                    increment = slope * turnover
                else:
                    increment = (high_price - current_price) / (high_price - average_price) * slope * turnover
            xdata[price_idx] += max(increment, 0.0)

    total_chips = float(xdata.sum())
    current_close = float(frame.iloc[index]["收盘"])
    current_date = pd.Timestamp(frame.iloc[index]["日期"]).date()
    if total_chips <= 0:
        benefit_part = 0.0
        avg_cost = 0.0
        p70_low = p70_high = p90_low = p90_high = 0.0
        p70_con = p90_con = 0.0
    else:
        benefit_part = float(xdata[_price_grid(min_price, accuracy, factor) <= current_close].sum() / total_chips)
        avg_cost = _cost_by_chip(xdata, min_price, accuracy, total_chips * 0.5)
        p70_low, p70_high, p70_con = _percent_chip_range(xdata, min_price, accuracy, total_chips, 0.7)
        p90_low, p90_high, p90_con = _percent_chip_range(xdata, min_price, accuracy, total_chips, 0.9)

    return {
        "日期": current_date,
        "获利比例": benefit_part,
        "平均成本": round(avg_cost, 2),
        "90成本-低": round(p90_low, 2),
        "90成本-高": round(p90_high, 2),
        "90集中度": p90_con,
        "70成本-低": round(p70_low, 2),
        "70成本-高": round(p70_high, 2),
        "70集中度": p70_con,
    }


def _clip_index(value: Any, factor: int) -> int:
    try:
        idx = int(value)
    except (TypeError, ValueError):
        idx = 0
    return max(0, min(factor - 1, idx))


def _price_grid(min_price: float, accuracy: float, factor: int) -> np.ndarray:
    return min_price + accuracy * np.arange(factor, dtype=float)


def _cost_by_chip(xdata: np.ndarray, min_price: float, accuracy: float, chip: float) -> float:
    cumulative = 0.0
    last_price = min_price + accuracy * (len(xdata) - 1)
    for index, value in enumerate(xdata):
        if cumulative + float(value) > chip:
            return min_price + index * accuracy
        cumulative += float(value)
    return last_price


def _percent_chip_range(
    xdata: np.ndarray,
    min_price: float,
    accuracy: float,
    total_chips: float,
    percent: float,
) -> tuple[float, float, float]:
    low_quantile = (1.0 - percent) / 2.0
    high_quantile = (1.0 + percent) / 2.0
    low_price = _cost_by_chip(xdata, min_price, accuracy, total_chips * low_quantile)
    high_price = _cost_by_chip(xdata, min_price, accuracy, total_chips * high_quantile)
    concentration = 0.0 if low_price + high_price == 0 else (high_price - low_price) / (low_price + high_price)
    return low_price, high_price, concentration


__all__ = [
    "OUTPUT_COLUMNS",
    "build_chip_distribution_from_price_csv",
    "build_chip_distribution_from_price_frame",
]
