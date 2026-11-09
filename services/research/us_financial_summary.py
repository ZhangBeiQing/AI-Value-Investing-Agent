"""Deterministic US financial summary for the research pack.

美股没有 A 股那种 LLM 深研稿。本模块直接从 ``financials_cache`` 的规范化
宽表生成结构化的财报摘要，保证第 3 章在离线、无 LLM 的情况下也有可用内容。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from commons import SymbolInfo
from shared_data_access.cache_registry import CacheKind, build_cache_dir

US_META_FILENAME = "us_financial_meta.json"
LOGGER = logging.getLogger("UsFinancialSummary")


def _load_meta(symbol_info: SymbolInfo) -> Dict[str, Any]:
    cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.FINANCIALS,
        base_dir="data",
        ensure=False,
    )
    path = cache_dir / US_META_FILENAME
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_frame(symbol_info: SymbolInfo, filename: str) -> pd.DataFrame:
    cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.FINANCIALS,
        base_dir="data",
        ensure=False,
    )
    path = cache_dir / filename
    if not path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if frame.empty or "REPORT_DATE" not in frame.columns:
        return frame
    frame["REPORT_DATE"] = pd.to_datetime(frame["REPORT_DATE"], errors="coerce")
    return frame.dropna(subset=["REPORT_DATE"]).sort_values("REPORT_DATE")


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype="float64", index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _to_yi(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric / 1e8


def _fmt(value: Any, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    return f"{float(value):,.{digits}f}"


def _pct(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    return f"{float(value):.1f}%"


def _table(rows: List[Dict[str, Any]], columns: List[str]) -> str:
    if not rows:
        return "> 无可用数据。"
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, separator]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(column, "—")) for column in columns) + " |")
    return "\n".join(lines)


def _quarterly_series(profit: pd.DataFrame, column: str) -> pd.DataFrame:
    """Convert year-to-date cumulative values into single-quarter values."""

    if profit.empty or column not in profit.columns:
        return pd.DataFrame()
    work = profit[["REPORT_DATE", column]].copy()
    work[column] = pd.to_numeric(work[column], errors="coerce")
    work["YEAR"] = work["REPORT_DATE"].dt.year
    work["MONTH"] = work["REPORT_DATE"].dt.month
    quarterly: List[Dict[str, Any]] = []
    for year, group in work.groupby("YEAR"):
        group = group.sort_values("REPORT_DATE")
        previous = 0.0
        for _, row in group.iterrows():
            value = row[column]
            if pd.isna(value):
                previous = 0.0
                continue
            if row["MONTH"] <= 3:
                single = float(value)
            else:
                single = float(value) - previous
            previous = float(value)
            quarterly.append({"REPORT_DATE": row["REPORT_DATE"], "value": single})
    return pd.DataFrame(quarterly).sort_values("REPORT_DATE").reset_index(drop=True)


def build_us_financial_summary(
    symbol_info: SymbolInfo,
    today_dt: datetime,
) -> Optional[str]:
    profit = _load_frame(symbol_info, "profit_sheet.csv")
    if profit.empty:
        return None
    balance = _load_frame(symbol_info, "balance_sheet.csv")
    cashflow = _load_frame(symbol_info, "cash_flow_sheet.csv")
    meta = _load_meta(symbol_info)

    as_of = pd.Timestamp(today_dt)
    for frame in (profit, balance, cashflow):
        if frame.empty:
            continue
    profit = profit[profit["REPORT_DATE"] <= as_of]
    balance = balance[balance["REPORT_DATE"] <= as_of]
    cashflow = cashflow[cashflow["REPORT_DATE"] <= as_of]
    # 公告时点截断：只有申报日在分析日（含）之前的报告期才可用
    for name, frame in (("profit", profit), ("balance", balance), ("cashflow", cashflow)):
        if frame.empty or "NOTICE_DATE" not in frame.columns:
            continue
        notice = pd.to_datetime(frame["NOTICE_DATE"], errors="coerce")
        filtered = frame[notice.isna() | (notice <= as_of)]
        if name == "profit":
            profit = filtered
        elif name == "balance":
            balance = filtered
        else:
            cashflow = filtered
    if profit.empty:
        return None

    latest = profit.iloc[-1]
    latest_date = pd.Timestamp(latest["REPORT_DATE"])
    notice_date = latest.get("NOTICE_DATE")
    currency = str(meta.get("currency") or latest.get("CURRENCY") or "USD")
    fx = meta.get("fx_local_per_usd")
    fx_text = (
        "金额单位：美元"
        if currency.upper() == "USD" or not fx
        else f"金额单位：美元（原始币种 {currency}，按 1 USD = {float(fx):.2f} {currency} 折算）"
    )

    lines: List[str] = []
    lines.append("## 美股结构化财报摘要（SEC 申报口径）")
    lines.append("")
    lines.append(
        f"- 最新报告期：{latest_date.strftime('%Y-%m-%d')}"
        + (f"（申报日 {pd.Timestamp(notice_date).strftime('%Y-%m-%d')}）" if pd.notna(notice_date) else "")
    )
    lines.append(f"- {fx_text}")
    lines.append(
        "- 口径：东财美股三大报表按「单季报(Q1) + 累计季报(Q2/Q3) + 年报(Q4)」拼接为年内累计；"
        "利润表/现金流量表为期间累计，资产负债表为期末时点。"
    )
    if meta.get("deduct_proxy"):
        lines.append("- 美股没有「扣非归母净利润」口径，扣非列等同报表归母净利润，不应据此判断非经常性损益。")
    lines.append("")

    # ---- 年度/累计利润表 ----
    lines.append("### 1. 利润表（年内累计，亿美元）")
    lines.append("")
    rows: List[Dict[str, Any]] = []
    for _, row in profit.tail(8).iterrows():
        revenue = _to_yi(row.get("TOTAL_OPERATE_INCOME"))
        net_profit = _to_yi(row.get("PARENT_NETPROFIT"))
        gross_margin = None
        if pd.notna(row.get("GROSS_PROFIT")) and pd.notna(row.get("TOTAL_OPERATE_INCOME")) and row["TOTAL_OPERATE_INCOME"]:
            gross_margin = float(row["GROSS_PROFIT"]) / float(row["TOTAL_OPERATE_INCOME"]) * 100
        net_margin = None
        if net_profit is not None and revenue:
            net_margin = net_profit / revenue * 100
        rows.append(
            {
                "报告期": pd.Timestamp(row["REPORT_DATE"]).strftime("%Y-%m-%d"),
                "营业收入": _fmt(revenue),
                "归母净利润": _fmt(net_profit),
                "毛利率": _pct(gross_margin),
                "净利率": _pct(net_margin),
            }
        )
    lines.append(_table(rows, ["报告期", "营业收入", "归母净利润", "毛利率", "净利率"]))
    lines.append("")

    # ---- 单季趋势 ----
    revenue_single = _quarterly_series(profit, "TOTAL_OPERATE_INCOME")
    profit_single = _quarterly_series(profit, "PARENT_NETPROFIT")
    if not revenue_single.empty and not profit_single.empty:
        merged_full = revenue_single.merge(
            profit_single,
            on="REPORT_DATE",
            suffixes=("_revenue", "_profit"),
        )
        yoy_map: Dict[str, float] = {}
        for _, row in merged_full.iterrows():
            prior = pd.Timestamp(row["REPORT_DATE"]) - pd.DateOffset(years=1)
            match = merged_full[merged_full["REPORT_DATE"] == prior]
            if match.empty:
                continue
            prior_revenue = float(match.iloc[0]["value_revenue"])
            if prior_revenue:
                yoy_map[pd.Timestamp(row["REPORT_DATE"]).strftime("%Y-%m-%d")] = (
                    float(row["value_revenue"]) / prior_revenue - 1
                ) * 100
        merged = merged_full.tail(8)
        quarter_rows: List[Dict[str, Any]] = []
        for _, row in merged.iterrows():
            key = pd.Timestamp(row["REPORT_DATE"]).strftime("%Y-%m-%d")
            quarter_rows.append(
                {
                    "单季截止": key,
                    "单季营收": _fmt(_to_yi(row["value_revenue"])),
                    "单季归母净利润": _fmt(_to_yi(row["value_profit"])),
                    "营收同比": _pct(yoy_map.get(key)),
                }
            )
        lines.append("### 2. 近 8 个单季趋势（亿美元）")
        lines.append("")
        lines.append(_table(quarter_rows, ["单季截止", "单季营收", "单季归母净利润", "营收同比"]))
        lines.append("")

    # ---- 资产负债表 ----
    if not balance.empty:
        latest_balance = balance.iloc[-1]
        total_assets = _to_yi(latest_balance.get("TOTAL_ASSETS"))
        total_liab = _to_yi(latest_balance.get("TOTAL_LIABILITIES"))
        equity = _to_yi(latest_balance.get("TOTAL_PARENT_EQUITY"))
        debt_ratio = None
        if total_assets and total_liab is not None:
            debt_ratio = total_liab / total_assets * 100
        cash = _to_yi(latest_balance.get("MONETARYFUNDS"))
        lines.append("### 3. 资产负债表（最新期末，亿美元）")
        lines.append("")
        lines.append(
            _table(
                [
                    {
                        "项目": "总资产",
                        "数值": _fmt(total_assets),
                    },
                    {"项目": "总负债", "数值": _fmt(total_liab)},
                    {"项目": "归母权益", "数值": _fmt(equity)},
                    {"项目": "资产负债率", "数值": _pct(debt_ratio)},
                    {"项目": "现金及等价物", "数值": _fmt(cash)},
                ],
                ["项目", "数值"],
            )
        )
        lines.append("")

    # ---- 现金流 ----
    if not cashflow.empty:
        latest_cash = cashflow.iloc[-1]
        lines.append(
            f"### 4. 现金流量表（{pd.Timestamp(latest_cash['REPORT_DATE']).strftime('%Y-%m-%d')} 累计，亿美元）"
        )
        lines.append("")
        lines.append(
            _table(
                [
                    {"项目": "经营活动净现金流", "数值": _fmt(_to_yi(latest_cash.get("NETCASH_OPERATE")))},
                    {"项目": "投资活动净现金流", "数值": _fmt(_to_yi(latest_cash.get("NETCASH_INVEST")))},
                    {"项目": "筹资活动净现金流", "数值": _fmt(_to_yi(latest_cash.get("NETCASH_FINANCE")))},
                    {"项目": "资本开支（购建长期资产）", "数值": _fmt(_to_yi(latest_cash.get("CONSTRUCT_LONG_ASSET")))},
                ],
                ["项目", "数值"],
            )
        )
        lines.append("")

    lines.append("### 5. 口径与限制")
    lines.append("")
    lines.append("- 数据来自东方财富美股财报接口，报告期为公司披露口径，可能与美国财年不一致。")
    lines.append("- 美股没有同花顺/经济通口径的机构一致预期，本文件不提供一致预期或目标价。")
    lines.append("- 汇率折算使用「东财市值/PE 反推的隐含汇率」，用于口径统一，不构成精确换汇依据。")
    lines.append("")
    return "\n".join(lines)


__all__ = ["build_us_financial_summary"]
