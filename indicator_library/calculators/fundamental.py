"""Fundamental indicator helpers (TTM, rolling profits, etc.)."""

from __future__ import annotations

from collections import deque
from typing import Any, Dict, Optional, Tuple, List
import logging
import re

import numpy as np
import pandas as pd


def fundamental_ttm_indicator(
    price_df: pd.DataFrame,
    *,
    frame: pd.DataFrame | None,
    value_column: str,
    date_column: str = "REPORT_DATE",
    window: int = 4,
) -> Dict[str, Any]:
    """Generic rolling TTM calculator driven by an external financial frame."""

    if frame is None or frame.empty or value_column not in frame.columns:
        return {}

    work_df = frame.copy()
    work_df[date_column] = pd.to_datetime(work_df[date_column], errors="coerce")
    work_df = work_df.dropna(subset=[date_column])
    work_df = work_df.sort_values(date_column)
    work_df[value_column] = pd.to_numeric(work_df[value_column], errors="coerce")
    work_df = work_df.dropna(subset=[value_column])
    if work_df.empty:
        return {}

    rolling = work_df[value_column].rolling(window=window).sum()
    if rolling.empty or np.isnan(rolling.iloc[-1]):
        return {}

    return {
        "ttm_value": float(rolling.iloc[-1]),
        "ttm_window": window,
        "ttm_end_date": work_df[date_column].iloc[-1],
    }


def calculate_rolling_ttm_profit(
    profit_sheet: pd.DataFrame,
    *,
    profit_column: str | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Calculate rolling TTM net profit/EPS for A/H shared logic."""
    if profit_sheet.empty:
        return pd.DataFrame()

    try:
        profit_sheet_sorted = profit_sheet.copy()
        if "REPORT_DATE" in profit_sheet_sorted.columns:
            profit_sheet_sorted["REPORT_DATE"] = pd.to_datetime(
                profit_sheet_sorted["REPORT_DATE"]
            )
        profit_sheet_sorted = profit_sheet_sorted.sort_values("REPORT_DATE", ascending=True)

        if profit_column:
            if profit_column not in profit_sheet_sorted.columns:
                _log(
                    logger,
                    logging.WARNING,
                    f"未找到指定的利润列 {profit_column}，无法计算TTM",
                )
                return pd.DataFrame()
            profit_col = profit_column
        elif "PARENT_NETPROFIT" in profit_sheet_sorted.columns:
            profit_col = "PARENT_NETPROFIT"
        elif "HOLDER_PROFIT" in profit_sheet_sorted.columns:
            profit_col = "HOLDER_PROFIT"
        elif "NETPROFIT" in profit_sheet_sorted.columns:
            profit_col = "NETPROFIT"
        else:
            _log(logger, logging.WARNING, "未找到净利润列，无法计算TTM")
            return pd.DataFrame()

        eps_col = "BASIC_EPS" if "BASIC_EPS" in profit_sheet_sorted.columns else None

        if profit_col == "HOLDER_PROFIT" and "DATE_TYPE_CODE" in profit_sheet_sorted.columns:
            return calculate_hk_indicator_ttm(
                profit_sheet_sorted,
                eps_col,
                logger=logger,
            )

        date_strings = profit_sheet_sorted["REPORT_DATE"].astype(str).str[:10]
        is_annual_data = all(date_str.endswith("12-31") for date_str in date_strings)

        if is_annual_data:
            ttm_data = []
            for _, row in profit_sheet_sorted.iterrows():
                report_date = row["REPORT_DATE"]
                annual_profit = row[profit_col]
                ttm_data.append(
                    {
                        "REPORT_DATE": report_date,
                        "TTM_NET_PROFIT": annual_profit / 100000000,
                        "TTM_NET_PROFIT_RAW": annual_profit,
                        "TTM_EPS": 0,
                    }
                )
            return pd.DataFrame(ttm_data)

        quarterly_data = []
        sheet = profit_sheet_sorted.copy()
        column = sheet[profit_col]

        for _, row in sheet.iterrows():
            report_date = row["REPORT_DATE"]
            cumulative_profit = row[profit_col]
            date_str = (
                report_date.strftime("%Y-%m-%d")
                if hasattr(report_date, "strftime")
                else str(report_date)[:10]
            )
            quarterly_profit = _derive_quarterly_profit(sheet, profit_col, date_str, cumulative_profit)
            quarterly_data.append(
                {
                    "REPORT_DATE": report_date,
                    "QUARTERLY_PROFIT": quarterly_profit,
                }
            )

        quarterly_df = pd.DataFrame(quarterly_data)
        if quarterly_df.empty or len(quarterly_df) < 4:
            return pd.DataFrame()

        ttm_records = []
        for idx in range(3, len(quarterly_df)):
            current_date = quarterly_df.iloc[idx]["REPORT_DATE"]
            ttm_profit = quarterly_df.iloc[idx - 3 : idx + 1]["QUARTERLY_PROFIT"].sum()
            quarter_profit = quarterly_df.iloc[idx]["QUARTERLY_PROFIT"]
            ttm_records.append(
                {
                    "REPORT_DATE": current_date,
                    "TTM_NET_PROFIT": ttm_profit / 100000000,
                    "TTM_NET_PROFIT_RAW": ttm_profit,
                    "TTM_EPS": 0,
                    "QUARTERLY_NET_PROFIT": quarter_profit / 100000000,
                    "QUARTERLY_NET_PROFIT_RAW": quarter_profit,
                }
            )

        return pd.DataFrame(ttm_records)
    except Exception as exc:  # pragma: no cover - defensive log
        _log(logger, logging.ERROR, f"计算滚动TTM净利润时出错: {exc}")
        return pd.DataFrame()


def calculate_hk_indicator_ttm(
    profit_sheet: pd.DataFrame,
    eps_col: Optional[str],
    *,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Specialized TTM calculator for HK indicator frames."""
    if profit_sheet.empty or "HOLDER_PROFIT" not in profit_sheet.columns:
        return pd.DataFrame()

    work_df = profit_sheet.copy()
    work_df["REPORT_DATE"] = pd.to_datetime(work_df["REPORT_DATE"], errors="coerce")
    work_df = work_df.dropna(subset=["REPORT_DATE"]).sort_values("REPORT_DATE")

    work_df["HOLDER_PROFIT"] = pd.to_numeric(work_df["HOLDER_PROFIT"], errors="coerce")
    if eps_col:
        work_df[eps_col] = pd.to_numeric(work_df[eps_col], errors="coerce")

    revenue_col = None
    if "OPERATE_INCOME" in work_df.columns:
        revenue_col = "OPERATE_INCOME"
        work_df[revenue_col] = pd.to_numeric(work_df[revenue_col], errors="coerce")

    fiscal_end = _parse_fiscal_year_end(work_df)
    profit_prev: Dict[int, float] = {}
    revenue_prev: Dict[int, float] = {}
    quarterly_rows: List[Dict[str, Any]] = []
    share_samples: List[float] = []

    for _, row in work_df.iterrows():
        report_date = row["REPORT_DATE"]
        cum_profit = row["HOLDER_PROFIT"]
        if pd.isna(cum_profit):
            continue

        fiscal_year_key = _infer_fiscal_year_end_year(report_date, fiscal_end)
        prev_cum = profit_prev.get(fiscal_year_key)
        quarter_profit = float(cum_profit) if prev_cum is None else float(cum_profit - prev_cum)
        if quarter_profit <= 0 < float(cum_profit):
            quarter_profit = float(cum_profit)
        profit_prev[fiscal_year_key] = float(cum_profit)

        quarter_revenue = np.nan
        if revenue_col:
            cum_revenue = row[revenue_col]
            if not pd.isna(cum_revenue):
                prev_rev = revenue_prev.get(fiscal_year_key)
                if prev_rev is None:
                    quarter_revenue = float(cum_revenue)
                else:
                    quarter_revenue = float(cum_revenue - prev_rev)
                    if quarter_revenue <= 0 < float(cum_revenue):
                        quarter_revenue = float(cum_revenue)
                revenue_prev[fiscal_year_key] = float(cum_revenue)

        shares_estimate = np.nan
        if eps_col and eps_col in row.index:
            eps_value = row[eps_col]
            if not pd.isna(eps_value) and float(eps_value) != 0:
                shares_estimate = float(cum_profit) / float(eps_value)
                if shares_estimate > 0:
                    share_samples.append(shares_estimate)

        quarterly_rows.append(
            {
                "REPORT_DATE": report_date,
                "QUARTERLY_NET_PROFIT": quarter_profit,
                "QUARTERLY_REVENUE": quarter_revenue,
                "SHARES_ESTIMATE": shares_estimate,
            }
        )

    quarterly_df = pd.DataFrame(quarterly_rows)
    if quarterly_df.empty:
        return pd.DataFrame()

    quarterly_df = quarterly_df.sort_values("REPORT_DATE")

    share_baseline = float(np.nanmedian(share_samples)) if share_samples else 0.0
    latest_share = share_baseline

    profit_window: deque = deque()
    revenue_window: deque = deque()
    ttm_rows: List[Dict[str, Any]] = []

    for _, row in quarterly_df.iterrows():
        quarter_profit = float(row["QUARTERLY_NET_PROFIT"])
        if np.isnan(quarter_profit):
            continue
        profit_window.append(quarter_profit)
        if len(profit_window) > 4:
            profit_window.popleft()

        quarter_revenue = row.get("QUARTERLY_REVENUE", np.nan)
        if not np.isnan(quarter_revenue):
            revenue_window.append(float(quarter_revenue))
            if len(revenue_window) > 4:
                revenue_window.popleft()

        shares_estimate = row.get("SHARES_ESTIMATE")
        if shares_estimate and not np.isnan(shares_estimate):
            latest_share = shares_estimate

        if len(profit_window) == 4:
            ttm_profit = sum(profit_window)
            ttm_revenue = sum(revenue_window) if revenue_window else np.nan
            ttm_eps = ttm_profit / latest_share if latest_share else np.nan
            ttm_rows.append(
                {
                    "REPORT_DATE": row["REPORT_DATE"],
                    "TTM_NET_PROFIT": ttm_profit / 100000000,
                    "TTM_NET_PROFIT_RAW": ttm_profit,
                    "TTM_REVENUE": ttm_revenue / 100000000 if not np.isnan(ttm_revenue) else np.nan,
                    "TTM_EPS": ttm_eps,
                }
            )

    return pd.DataFrame(ttm_rows)


def _derive_quarterly_profit(sheet: pd.DataFrame, column: str, date_str: str, cumulative_profit: float) -> float:
    if date_str.endswith("12-31"):
        year = date_str[:4]
        q3_target = year + "-09-30"
        q3_data = sheet[sheet["REPORT_DATE"].astype(str).str[:10] == q3_target]
        if not q3_data.empty:
            prev = q3_data.iloc[0][column]
            return cumulative_profit - prev
        return cumulative_profit

    prev_cumulative = 0
    if date_str.endswith("06-30"):
        year = date_str[:4]
        q1_target = year + "-03-31"
        q1_data = sheet[sheet["REPORT_DATE"].astype(str).str[:10] == q1_target]
        if not q1_data.empty:
            prev_cumulative = q1_data.iloc[0][column]
    elif date_str.endswith("09-30"):
        year = date_str[:4]
        h1_target = year + "-06-30"
        h1_data = sheet[sheet["REPORT_DATE"].astype(str).str[:10] == h1_target]
        if not h1_data.empty:
            prev_cumulative = h1_data.iloc[0][column]
    elif date_str.endswith("03-31"):
        prev_cumulative = 0

    return cumulative_profit - prev_cumulative


def _parse_fiscal_year_end(profit_sheet: pd.DataFrame) -> Tuple[int, int]:
    default_end = (12, 31)
    if profit_sheet.empty or "FISCAL_YEAR" not in profit_sheet.columns:
        return default_end

    for raw_value in profit_sheet["FISCAL_YEAR"].dropna():
        value_str = str(raw_value).strip()
        if not value_str or value_str.lower() == "nan":
            continue
        normalized = (
            value_str.replace("月", "-")
            .replace("日", "")
            .replace(".", "-")
            .replace("年", "-")
        )
        parts = [segment for segment in re.split(r"[^0-9]", normalized) if segment]
        if len(parts) >= 2:
            try:
                month = int(parts[0])
                day = int(parts[1])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return (month, day)
            except ValueError:
                continue

    return default_end


def _infer_fiscal_year_end_year(report_date: pd.Timestamp, fiscal_end: Tuple[int, int]) -> int:
    if pd.isna(report_date):
        return int(pd.Timestamp.today().year)
    year = report_date.year
    month, day = fiscal_end
    fiscal_end_date = pd.Timestamp(year=year, month=month, day=day)
    if report_date <= fiscal_end_date:
        return year
    return year + 1


def _log(logger: logging.Logger | None, level: int, message: str) -> None:
    if logger is not None:
        logger.log(level, message)


__all__ = [
    "fundamental_ttm_indicator",
    "calculate_rolling_ttm_profit",
    "calculate_hk_indicator_ttm",
]
