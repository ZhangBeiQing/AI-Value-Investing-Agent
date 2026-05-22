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


def build_latest_chip_distribution_from_price_frame(
    price_frame: pd.DataFrame,
    *,
    source_lookback_rows: int | None = 210,
    cyq_window: int = 120,
    factor: int = 150,
) -> pd.DataFrame:
    """Compute only the latest CYQ summary row from cached price data."""

    frame = _normalize_price_frame(price_frame)
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    if source_lookback_rows is not None and source_lookback_rows > 0:
        effective_rows = max(source_lookback_rows, cyq_window)
        frame = frame.tail(effective_rows).reset_index(drop=True)

    row = _compute_cyq_row(frame, len(frame) - 1, cyq_window=cyq_window, factor=factor)
    if not row:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return pd.DataFrame([row], columns=OUTPUT_COLUMNS)


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
    xdata = _cyq_chip_distribution(window, min_price=min_price, accuracy=accuracy, factor=factor)

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


def _cyq_chip_distribution(
    window: pd.DataFrame,
    *,
    min_price: float,
    accuracy: float,
    factor: int,
) -> np.ndarray:
    """Vectorized equivalent of the AkShare CYQ rolling-window calculation."""

    opens = window["开盘"].to_numpy(dtype=float)
    closes = window["收盘"].to_numpy(dtype=float)
    highs = window["最高"].to_numpy(dtype=float)
    lows = window["最低"].to_numpy(dtype=float)
    turnovers = window["换手率"].map(_normalize_turnover).to_numpy(dtype=float)
    valid = np.isfinite(opens) & np.isfinite(closes) & np.isfinite(highs) & np.isfinite(lows) & np.isfinite(turnovers)
    if not valid.any():
        return np.zeros(factor, dtype=float)

    opens = opens[valid]
    closes = closes[valid]
    highs = highs[valid]
    lows = lows[valid]
    turnovers = turnovers[valid]
    averages = (opens + closes + highs + lows) / 4.0
    grid = _price_grid(min_price, accuracy, factor)

    day_count = len(opens)
    price_grid = grid.reshape(1, -1)
    low_grid = lows.reshape(-1, 1)
    high_grid = highs.reshape(-1, 1)
    avg_grid = averages.reshape(-1, 1)
    turnover_grid = turnovers.reshape(-1, 1)
    price_in_range = (price_grid >= low_grid) & (price_grid <= high_grid)
    height = high_grid - low_grid

    contribution = np.zeros((day_count, factor), dtype=float)
    flat_mask = np.abs(height[:, 0]) < 1e-12
    if flat_mask.any():
        indices = np.floor((averages[flat_mask] - min_price) / accuracy).astype(int)
        indices = np.clip(indices, 0, factor - 1)
        contribution[np.where(flat_mask)[0], indices] = (factor - 1) * turnovers[flat_mask] / 2.0

    normal_mask = ~flat_mask
    if normal_mask.any():
        slope = 2.0 / np.where(np.abs(height) < 1e-12, np.nan, height)
        left_denominator = avg_grid - low_grid
        right_denominator = high_grid - avg_grid

        with np.errstate(divide="ignore", invalid="ignore"):
            left_value = np.where(
                np.abs(left_denominator) < 1e-12,
                slope * turnover_grid,
                (price_grid - low_grid) / left_denominator * slope * turnover_grid,
            )
            right_value = np.where(
                np.abs(right_denominator) < 1e-12,
                slope * turnover_grid,
                (high_grid - price_grid) / right_denominator * slope * turnover_grid,
            )
        normal_value = np.where(price_grid <= avg_grid, left_value, right_value)
        normal_value = np.where(price_in_range & normal_mask.reshape(-1, 1), normal_value, 0.0)
        contribution += np.nan_to_num(np.maximum(normal_value, 0.0), nan=0.0, posinf=0.0, neginf=0.0)

    retention = np.clip(1.0 - turnovers, 0.0, 1.0)
    reverse_cumprod = np.cumprod(retention[::-1])[::-1]
    decay_after = np.ones(day_count, dtype=float)
    if day_count > 1:
        decay_after[:-1] = reverse_cumprod[1:]
    return (contribution * decay_after.reshape(-1, 1)).sum(axis=0)


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
    "build_latest_chip_distribution_from_price_frame",
]
