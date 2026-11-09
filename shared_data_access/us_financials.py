"""US financial statements: fetch, normalize, and cache.

东财美股三大报表是「长表」（一行一个科目），而本仓库下游（TTM 计算、估值
分析、basic_snapshot）都按 A 股宽表列名读取。本模块负责：

1. 按 ``单季报 / 累计季报 / 年报`` 拼出年内累计季度序列（A 股口径）；
2. 把科目名映射成 A 股 canonical 列名，并补 ``NOTICE_DATE``；
3. 统一币种：ADR 用「隐含汇率」把本币报表折算成美元，使
   ``美元市值 / 美元净利润`` 与东财展示的 PE 同口径；
4. 生成同结构的 ``financial_abstract``（宽表，列为 YYYYMMDD）。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import akshare as ak  # type: ignore
import pandas as pd

from commons import SymbolInfo, api_call_with_delay, resolve_base_dir
from shared_data_access.cache_registry import (
    CacheKind,
    build_cache_dir,
    record_cache_refresh,
    should_refresh,
)
from shared_data_access.us_market import derive_total_shares, fetch_us_quote

US_STATEMENT_INDICATORS = ("单季报", "累计季报", "年报")
US_STATEMENT_KEYS = ("综合损益表", "资产负债表", "现金流量表")
US_STATEMENT_FILES = {
    "综合损益表": "profit_sheet.csv",
    "资产负债表": "balance_sheet.csv",
    "现金流量表": "cash_flow_sheet.csv",
}
US_META_FILENAME = "us_financial_meta.json"

PROFIT_ITEM_MAP = {
    "主营收入": "TOTAL_OPERATE_INCOME",
    "营业收入": "TOTAL_OPERATE_INCOME",
    "主营成本": "OPERATE_COST",
    "营业成本": "OPERATE_COST",
    "毛利": "GROSS_PROFIT",
    "研发费用": "RESEARCH_EXPENSE",
    "营销费用": "SALE_EXPENSE",
    "一般及行政费用": "MANAGE_EXPENSE",
    "营业费用": "OPERATE_EXPENSE",
    "营业利润": "OPERATE_PROFIT",
    "利息收入": "FE_INTEREST_INCOME",
    "利息支出": "FE_INTEREST_EXPENSE",
    "持续经营税前利润": "TOTAL_PROFIT",
    "所得税": "INCOME_TAX",
    "净利润": "NETPROFIT",
    "归属于母公司股东净利润": "PARENT_NETPROFIT",
    "少数股东损益": "MINORITY_INTEREST",
    "基本每股收益-普通股": "BASIC_EPS",
    "摊薄每股收益-普通股": "DILUTED_EPS",
    "基本加权平均股数-普通股": "WEIGHTED_AVG_SHARES",
    "摊薄加权平均股数-普通股": "DILUTED_WEIGHTED_AVG_SHARES",
    "全面收益总额": "TOTAL_COMPREHENSIVE_INCOME",
}

BALANCE_ITEM_MAP = {
    "现金及现金等价物": "MONETARYFUNDS",
    "短期投资": "SHORT_INVEST",
    "应收账款": "ACCOUNTS_RECE",
    "存货": "INVENTORY",
    "流动资产合计": "TOTAL_CURRENT_ASSETS",
    "物业、厂房及设备": "FIXED_ASSET",
    "固定资产": "FIXED_ASSET",
    "无形资产": "INTANGIBLE_ASSET",
    "商誉": "GOODWILL",
    "长期投资": "LONG_EQUITY_INVEST",
    "非流动资产合计": "TOTAL_NONCURRENT_ASSETS",
    "总资产": "TOTAL_ASSETS",
    "应付账款": "ACCOUNTS_PAYABLE",
    "短期债务": "SHORT_LOAN",
    "流动负债合计": "TOTAL_CURRENT_LIAB",
    "长期负债": "LONG_LOAN",
    "非流动负债合计": "TOTAL_NONCURRENT_LIAB",
    "总负债": "TOTAL_LIABILITIES",
    "普通股": "SHARE_CAPITAL",
    "股本溢价": "CAPITAL_RESERVE",
    "留存收益": "UNASSIGN_RPOFIT",
    "归属于母公司股东权益": "TOTAL_PARENT_EQUITY",
    "少数股东权益": "MINORITY_EQUITY",
    "股东权益合计": "TOTAL_EQUITY",
    "负债及股东权益合计": "TOTAL_LIAB_EQUITY",
}

CASHFLOW_ITEM_MAP = {
    "净利润": "NETPROFIT",
    "折旧及摊销": "FA_IR_DEPR",
    "经营活动产生的现金流量净额": "NETCASH_OPERATE",
    "购买固定资产": "CONSTRUCT_LONG_ASSET",
    "投资活动产生的现金流量净额": "NETCASH_INVEST",
    "筹资活动产生的现金流量净额": "NETCASH_FINANCE",
    "股息支付": "ASSIGN_DIVIDEND_PORFIT",
    "回购股份": "REPURCHASE",
    "现金及现金等价物增加(减少)额": "CCE_ADD",
    "现金及现金等价物期末余额": "END_CASH",
    "现金及现金等价物期初余额": "BEGIN_CASH",
}

STATEMENT_ITEM_MAPS = {
    "综合损益表": PROFIT_ITEM_MAP,
    "资产负债表": BALANCE_ITEM_MAP,
    "现金流量表": CASHFLOW_ITEM_MAP,
}

ABSTRACT_LABELS = {
    "OPERATE_INCOME": ("常用指标", "营业总收入"),
    "PARENT_HOLDER_NETPROFIT": ("常用指标", "归母净利润"),
    "NETPROFIT": ("常用指标", "净利润"),
    "GROSS_PROFIT_RATIO": ("常用指标", "毛利率"),
    "NET_PROFIT_RATIO": ("常用指标", "销售净利率"),
    "ROE_AVG": ("常用指标", "ROE"),
    "DEBT_ASSET_RATIO": ("常用指标", "资产负债率"),
    "CURRENT_RATIO": ("常用指标", "流动比率"),
    "SPEED_RATIO": ("常用指标", "速动比率"),
    "OPERATE_INCOME_YOY": ("成长能力", "营业总收入同比增长"),
    "PARENT_HOLDER_NETPROFIT_YOY": ("成长能力", "归母净利润同比增长"),
}

PERCENT_RATIO_COLUMNS = (
    "GROSS_PROFIT_RATIO",
    "NET_PROFIT_RATIO",
    "ROE_AVG",
    "DEBT_ASSET_RATIO",
    "OPERATE_INCOME_YOY",
    "PARENT_HOLDER_NETPROFIT_YOY",
)

# 需要按「本币 → 美元」换算的金额列（比率列与股数不换算）
MONEY_COLUMNS = {
    "TOTAL_OPERATE_INCOME",
    "OPERATE_INCOME",
    "OPERATE_COST",
    "GROSS_PROFIT",
    "RESEARCH_EXPENSE",
    "SALE_EXPENSE",
    "MANAGE_EXPENSE",
    "OPERATE_EXPENSE",
    "OPERATE_PROFIT",
    "TOTAL_PROFIT",
    "INCOME_TAX",
    "NETPROFIT",
    "PARENT_NETPROFIT",
    "DEDUCT_PARENT_NETPROFIT",
    "MINORITY_INTEREST",
    "FE_INTEREST_INCOME",
    "FE_INTEREST_EXPENSE",
    "TOTAL_COMPREHENSIVE_INCOME",
    "MONETARYFUNDS",
    "SHORT_INVEST",
    "ACCOUNTS_RECE",
    "INVENTORY",
    "TOTAL_CURRENT_ASSETS",
    "FIXED_ASSET",
    "INTANGIBLE_ASSET",
    "GOODWILL",
    "LONG_EQUITY_INVEST",
    "TOTAL_NONCURRENT_ASSETS",
    "TOTAL_ASSETS",
    "ACCOUNTS_PAYABLE",
    "SHORT_LOAN",
    "TOTAL_CURRENT_LIAB",
    "LONG_LOAN",
    "TOTAL_NONCURRENT_LIAB",
    "TOTAL_LIABILITIES",
    "SHARE_CAPITAL",
    "CAPITAL_RESERVE",
    "UNASSIGN_RPOFIT",
    "TOTAL_PARENT_EQUITY",
    "MINORITY_EQUITY",
    "TOTAL_EQUITY",
    "TOTAL_LIAB_EQUITY",
    "FA_IR_DEPR",
    "NETCASH_OPERATE",
    "CONSTRUCT_LONG_ASSET",
    "NETCASH_INVEST",
    "NETCASH_FINANCE",
    "ASSIGN_DIVIDEND_PORFIT",
    "REPURCHASE",
    "CCE_ADD",
    "END_CASH",
    "BEGIN_CASH",
}


def _resolve_logger(logger: logging.Logger | None) -> logging.Logger:
    return logger if logger is not None else logging.getLogger(__name__)


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "", "-", "nan"):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if pd.notna(numeric) else None


def _fetch_statement_raw(
    ticker: str,
    statement: str,
    indicator: str,
    *,
    logger: logging.Logger,
) -> pd.DataFrame:
    try:
        frame = api_call_with_delay(
            ak.stock_financial_us_report_em,
            stock=ticker,
            symbol=statement,
            indicator=indicator,
            logger=logger,
        )
    except Exception as exc:
        logger.warning("获取美股报表失败 %s/%s/%s: %s", ticker, statement, indicator, exc)
        return pd.DataFrame()
    if frame is None or frame.empty:
        return pd.DataFrame()
    return frame


def _pick_indicator_row(month: int, available: Iterable[str]) -> str:
    """按报告月份选择口径：Q1=单季报、Q2/Q3=累计季报、Q4=年报。"""

    available = set(available)
    if month == 3 and "单季报" in available:
        return "单季报"
    if month in (6, 9) and "累计季报" in available:
        return "累计季报"
    if month == 12 and "年报" in available:
        return "年报"
    for candidate in ("累计季报", "年报", "单季报"):
        if candidate in available:
            return candidate
    return ""


def _pick_indicator_for_date(
    report_date: Any,
    dates_by_indicator: Dict[str, set],
) -> str:
    """兜底选口径：按该报告期实际出现在哪张表里取。

    上面的月份规则假设财年 12 月结束。微软（6 月结账）、英伟达（1 月结账）这类
    非自然财年会把月份整体错位，导致整张表拼空；此时以「该日期真实存在于哪个
    口径」为准，并优先取更完整的那张表。
    """

    candidates = [
        indicator
        for indicator, dates in dates_by_indicator.items()
        if report_date in dates
    ]
    for preferred in ("年报", "累计季报", "单季报"):
        if preferred in candidates:
            return preferred
    return candidates[0] if candidates else ""


def build_quarterly_wide_frame(
    frames: Dict[str, pd.DataFrame],
    item_map: Dict[str, str],
) -> pd.DataFrame:
    """把三个口径的长表合成「一行一个报告期」的宽表。"""

    records: Dict[pd.Timestamp, Dict[str, Any]] = {}
    source_by_date: Dict[pd.Timestamp, str] = {}

    prepared: Dict[str, pd.DataFrame] = {}
    for indicator, frame in frames.items():
        if frame is None or frame.empty:
            continue
        work = frame.copy()
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE"])
        work["ITEM_NAME"] = work["ITEM_NAME"].astype(str).str.strip()
        work["AMOUNT"] = pd.to_numeric(work["AMOUNT"], errors="coerce")
        prepared[indicator] = work

    all_dates = sorted(
        {
            date
            for frame in prepared.values()
            for date in frame["REPORT_DATE"].unique()
        }
    )
    dates_by_indicator = {
        indicator: {pd.Timestamp(value) for value in frame["REPORT_DATE"].unique()}
        for indicator, frame in prepared.items()
    }
    for report_date in all_dates:
        month = pd.Timestamp(report_date).month
        indicator = _pick_indicator_row(month, prepared.keys())
        # 月份规则落空（非自然财年）时，按该报告期真实所在口径兜底
        if not indicator or pd.Timestamp(report_date) not in dates_by_indicator.get(indicator, set()):
            indicator = _pick_indicator_for_date(pd.Timestamp(report_date), dates_by_indicator)
        if not indicator:
            continue
        frame = prepared[indicator]
        rows = frame[frame["REPORT_DATE"] == report_date]
        if rows.empty:
            continue
        values: Dict[str, Any] = {}
        for _, row in rows.iterrows():
            column = item_map.get(row["ITEM_NAME"])
            if not column:
                continue
            amount = _to_float(row["AMOUNT"])
            if amount is None:
                continue
            # 同一报告期重复科目：保留首次出现的值
            values.setdefault(column, amount)
        if not values:
            continue
        records[pd.Timestamp(report_date)] = values
        source_by_date[pd.Timestamp(report_date)] = indicator

    if not records:
        return pd.DataFrame()

    wide = pd.DataFrame.from_dict(records, orient="index")
    wide.index.name = "REPORT_DATE"
    wide = wide.sort_index().reset_index()
    wide["REPORT_TYPE"] = wide["REPORT_DATE"].map(source_by_date)
    return wide


def _notice_date_map(indicator_frame: pd.DataFrame) -> Dict[pd.Timestamp, pd.Timestamp]:
    mapping: Dict[pd.Timestamp, pd.Timestamp] = {}
    if indicator_frame is None or indicator_frame.empty:
        return mapping
    frame = indicator_frame.copy()
    frame["REPORT_DATE"] = pd.to_datetime(frame["REPORT_DATE"], errors="coerce")
    frame["NOTICE_DATE"] = pd.to_datetime(frame.get("NOTICE_DATE"), errors="coerce")
    frame = frame.dropna(subset=["REPORT_DATE", "NOTICE_DATE"])
    for _, row in frame.iterrows():
        key = pd.Timestamp(row["REPORT_DATE"])
        notice = pd.Timestamp(row["NOTICE_DATE"])
        # 东财该字段偶有错位（把后一年年报的申报日挂到更早的报告期上），
        # 只接受落在「报告期 ~ 报告期+200 天」内的值，其余交给法定滞后兜底。
        delta = notice - key
        if delta < timedelta(days=0) or delta > timedelta(days=200):
            continue
        if key not in mapping or notice < mapping[key]:
            mapping[key] = notice
    return mapping


def _fallback_notice_date(report_date: pd.Timestamp) -> pd.Timestamp:
    # 美股 10-Q 法定 40/45 天、10-K 60/90 天；无 NOTICE_DATE 时保守取 60 天
    return pd.Timestamp(report_date) + timedelta(days=60)


def _attach_notice_dates(
    wide: pd.DataFrame,
    notice_map: Dict[pd.Timestamp, pd.Timestamp],
) -> pd.DataFrame:
    """Attach the filing date (SEC) per report period, falling back to a lag."""

    if wide.empty:
        return wide
    work = wide.copy()
    work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
    work["NOTICE_DATE"] = [
        notice_map.get(pd.Timestamp(date)) or _fallback_notice_date(pd.Timestamp(date))
        for date in work["REPORT_DATE"]
    ]
    return work.sort_values("REPORT_DATE").reset_index(drop=True)


def _infer_currency(
    indicator_frame: pd.DataFrame,
    frames: Iterable[pd.DataFrame],
) -> str:
    """Prefer the ISO currency code from the analysis-indicator endpoint."""

    if indicator_frame is not None and not indicator_frame.empty:
        for column in ("CURRENCY_ABBR", "CURRENCY"):
            if column in indicator_frame.columns:
                values = (
                    indicator_frame[column]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                    .tolist()
                )
                if values:
                    return values[0].upper()
    for frame in frames:
        if frame is None or frame.empty:
            continue
        if "CURRENCY" in frame.columns:
            values = (
                frame["CURRENCY"].dropna().astype(str).str.strip().unique().tolist()
            )
            if values:
                return values[0]
    return "USD"


def _combine_indicator_frames(
    variants: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """按报告月份把指标接口的三种口径合成一条「年内累计」序列。"""

    prepared: Dict[str, pd.DataFrame] = {}
    for indicator, frame in variants.items():
        if frame is None or frame.empty:
            continue
        work = frame.copy()
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE"])
        if not work.empty:
            prepared[indicator] = work
    if not prepared:
        return pd.DataFrame()

    dates = sorted(
        {date for frame in prepared.values() for date in frame["REPORT_DATE"].unique()}
    )
    rows: List[pd.Series] = []
    for report_date in dates:
        month = pd.Timestamp(report_date).month
        indicator = _pick_indicator_row(month, prepared.keys())
        frame = prepared.get(indicator)
        if frame is None:
            continue
        subset = frame[frame["REPORT_DATE"] == report_date]
        if subset.empty:
            continue
        rows.append(subset.iloc[0])
    if not rows:
        return pd.DataFrame()
    combined = pd.DataFrame(rows)
    return combined.sort_values("REPORT_DATE").reset_index(drop=True)


def _ttm_profit_from_wide(wide: pd.DataFrame) -> Optional[float]:
    """从年内累计利润表反推最近四个单季并求和（本币）。"""

    if wide is None or wide.empty or "PARENT_NETPROFIT" not in wide.columns:
        return None
    work = wide[["REPORT_DATE", "PARENT_NETPROFIT"]].copy()
    work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
    work = work.dropna(subset=["REPORT_DATE"]).sort_values("REPORT_DATE")
    if work.empty:
        return None

    singles: List[float] = []
    for _, group in work.groupby(work["REPORT_DATE"].dt.year):
        previous = 0.0
        for _, row in group.iterrows():
            value = _to_float(row["PARENT_NETPROFIT"])
            if value is None:
                previous = 0.0
                continue
            month = pd.Timestamp(row["REPORT_DATE"]).month
            single = float(value) if month <= 3 else float(value) - previous
            previous = float(value)
            singles.append(single)
    if len(singles) < 4:
        return None
    return float(sum(singles[-4:]))


def compute_implied_fx(
    *,
    currency: str,
    quote: Optional[Dict[str, Any]],
    ttm_profit_local: Optional[float],
) -> Tuple[Optional[float], str]:
    """Return (fx_local_per_usd, source) so that local_amount / fx == USD."""

    normalized = (currency or "").strip().upper()
    if normalized in ("", "USD", "US$", "美元"):
        return 1.0, "currency_is_usd"

    if not quote:
        return None, "missing_quote"
    market_cap = _to_float(quote.get("market_cap"))
    pe_ttm = _to_float(quote.get("pe_ttm"))
    if not market_cap or not pe_ttm or pe_ttm <= 0:
        return None, "missing_pe_for_implied_fx"
    if not ttm_profit_local or ttm_profit_local <= 0:
        return None, "missing_local_ttm_profit"

    implied_profit_usd = market_cap / pe_ttm
    if implied_profit_usd <= 0:
        return None, "invalid_implied_profit"
    fx = ttm_profit_local / implied_profit_usd
    if fx <= 0 or not (0.01 <= fx <= 100000):
        return None, "implied_fx_out_of_range"
    return float(fx), "implied_from_eastmoney_pe"


def _convert_money_columns(wide: pd.DataFrame, fx: Optional[float]) -> pd.DataFrame:
    if wide.empty or fx in (None, 0):
        return wide
    work = wide.copy()
    for column in MONEY_COLUMNS.intersection(work.columns):
        work[column] = pd.to_numeric(work[column], errors="coerce") / float(fx)
    return work


def _build_financial_abstract(
    *,
    profit_wide: pd.DataFrame,
    balance_wide: pd.DataFrame,
    indicator_frame: pd.DataFrame,
    total_shares: Optional[float],
    fx: Optional[float],
) -> pd.DataFrame:
    if profit_wide.empty:
        return pd.DataFrame()

    dates = (
        pd.to_datetime(profit_wide["REPORT_DATE"], errors="coerce")
        .dropna()
        .sort_values()
        .tolist()
    )
    if not dates:
        return pd.DataFrame()
    date_keys = [pd.Timestamp(date).strftime("%Y%m%d") for date in dates]

    def by_date(frame: pd.DataFrame, column: str) -> Dict[str, float]:
        if frame.empty or column not in frame.columns:
            return {}
        work = frame[["REPORT_DATE", column]].copy()
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE"])
        return {
            pd.Timestamp(row["REPORT_DATE"]).strftime("%Y%m%d"): _to_float(row[column])
            for _, row in work.iterrows()
        }

    revenue = by_date(profit_wide, "TOTAL_OPERATE_INCOME")
    net_profit = by_date(profit_wide, "PARENT_NETPROFIT")
    net_profit_total = by_date(profit_wide, "NETPROFIT")
    equity = by_date(balance_wide, "TOTAL_PARENT_EQUITY")

    indicator_ratios: Dict[str, Dict[str, float]] = {}
    if indicator_frame is not None and not indicator_frame.empty:
        frame = indicator_frame.copy()
        frame["REPORT_DATE"] = pd.to_datetime(frame["REPORT_DATE"], errors="coerce")
        frame = frame.dropna(subset=["REPORT_DATE"])
        for column in PERCENT_RATIO_COLUMNS:
            if column not in frame.columns:
                continue
            series = pd.to_numeric(frame[column], errors="coerce")
            indicator_ratios[column] = {
                pd.Timestamp(row["REPORT_DATE"]).strftime("%Y%m%d"): _to_float(value)
                for (_, row), value in zip(frame.iterrows(), series)
            }
        for column in ("CURRENT_RATIO", "SPEED_RATIO"):
            if column not in frame.columns:
                continue
            series = pd.to_numeric(frame[column], errors="coerce")
            indicator_ratios[column] = {
                pd.Timestamp(row["REPORT_DATE"]).strftime("%Y%m%d"): _to_float(value)
                for (_, row), value in zip(frame.iterrows(), series)
            }

    rows: List[Dict[str, Any]] = []

    def add_row(section: str, label: str, values: Dict[str, float]) -> None:
        if not values:
            return
        record: Dict[str, Any] = {"选项": section, "指标": label}
        for key in date_keys:
            record[key] = values.get(key)
        rows.append(record)

    add_row("常用指标", "营业总收入", revenue)
    add_row("常用指标", "归母净利润", net_profit)
    add_row("常用指标", "净利润", net_profit_total)

    for column, (section, label) in ABSTRACT_LABELS.items():
        if column in ("OPERATE_INCOME", "PARENT_HOLDER_NETPROFIT", "NETPROFIT"):
            continue
        values = indicator_ratios.get(column, {})
        add_row(section, label, values)

    if total_shares:
        eps_source = net_profit or net_profit_total
        eps = {
            key: (value / total_shares if value is not None else None)
            for key, value in eps_source.items()
        }
        add_row("常用指标", "基本每股收益", eps)
        bps = {
            key: (value / total_shares if value is not None else None)
            for key, value in equity.items()
        }
        add_row("常用指标", "每股净资产", bps)

    return pd.DataFrame(rows)


def fetch_us_financial_frames(
    ticker: str,
    *,
    logger: logging.Logger,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """Fetch all statement variants and the analysis indicator frame."""

    frames: Dict[str, Dict[str, pd.DataFrame]] = {key: {} for key in US_STATEMENT_KEYS}
    for statement in US_STATEMENT_KEYS:
        for indicator in US_STATEMENT_INDICATORS:
            raw = _fetch_statement_raw(ticker, statement, indicator, logger=logger)
            if not raw.empty:
                frames[statement][indicator] = raw

    indicator_variants: Dict[str, pd.DataFrame] = {}
    for indicator in US_STATEMENT_INDICATORS:
        try:
            frame = api_call_with_delay(
                ak.stock_financial_us_analysis_indicator_em,
                symbol=ticker,
                indicator=indicator,
                logger=logger,
            )
        except Exception as exc:
            logger.warning("获取美股财务指标失败 %s/%s: %s", ticker, indicator, exc)
            continue
        if frame is not None and not frame.empty:
            indicator_variants[indicator] = frame

    indicator_frame = _combine_indicator_frames(indicator_variants)
    return frames, indicator_frame


def normalize_us_financials(
    *,
    frames: Dict[str, Dict[str, pd.DataFrame]],
    indicator_frame: pd.DataFrame,
    quote: Optional[Dict[str, Any]],
    filing_notice_map: Optional[Dict[pd.Timestamp, pd.Timestamp]] = None,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Pure normalization entrypoint (also used by tests).

    ``filing_notice_map`` 是「报告期 → SEC 申报日」映射；它比东财财务指标里的
    NOTICE_DATE 可靠，缺省时才退回指标接口的映射。
    """

    resolved_logger = _resolve_logger(logger)
    notice_map = dict(filing_notice_map or {})
    for key, value in _notice_date_map(indicator_frame).items():
        notice_map.setdefault(key, value)
    currency = _infer_currency(
        indicator_frame,
        (
            frame
            for statement_frames in frames.values()
            for frame in statement_frames.values()
        ),
    )

    wide_frames: Dict[str, pd.DataFrame] = {}
    for statement, item_map in STATEMENT_ITEM_MAPS.items():
        wide = build_quarterly_wide_frame(frames.get(statement, {}), item_map)
        if wide.empty:
            wide_frames[statement] = wide
            continue
        wide = _attach_notice_dates(wide, notice_map)
        if statement == "综合损益表":
            if "TOTAL_OPERATE_INCOME" in wide.columns:
                wide["OPERATE_INCOME"] = wide["TOTAL_OPERATE_INCOME"]
            if "PARENT_NETPROFIT" in wide.columns:
                wide["DEDUCT_PARENT_NETPROFIT"] = wide["PARENT_NETPROFIT"]
        wide_frames[statement] = wide

    ttm_profit_local = _ttm_profit_from_wide(wide_frames.get("综合损益表"))
    fx, fx_source = compute_implied_fx(
        currency=currency,
        quote=quote,
        ttm_profit_local=ttm_profit_local,
    )
    total_shares = derive_total_shares(quote)

    converted: Dict[str, pd.DataFrame] = {}
    for statement, wide in wide_frames.items():
        if wide.empty:
            converted[statement] = wide
            continue
        work = wide.copy()
        work["CURRENCY"] = currency
        work = _convert_money_columns(work, fx)
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"]).dt.strftime("%Y-%m-%d")
        work["NOTICE_DATE"] = pd.to_datetime(work["NOTICE_DATE"]).dt.strftime("%Y-%m-%d")
        converted[statement] = work

    profit_wide = converted.get("综合损益表", pd.DataFrame())
    balance_wide = converted.get("资产负债表", pd.DataFrame())

    abstract = _build_financial_abstract(
        profit_wide=profit_wide,
        balance_wide=balance_wide,
        indicator_frame=indicator_frame,
        total_shares=total_shares,
        fx=fx,
    )

    meta = {
        "ticker": (quote or {}).get("ticker"),
        "currency": currency,
        "fx_local_per_usd": fx,
        "fx_source": fx_source,
        "ttm_profit_local": ttm_profit_local,
        "total_shares": total_shares,
        "quote_market_cap": (quote or {}).get("market_cap"),
        "quote_pe_ttm": (quote or {}).get("pe_ttm"),
        "quote_pb": (quote or {}).get("pb"),
        "quote_price": (quote or {}).get("price"),
        "deduct_proxy": True,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if fx is None:
        resolved_logger.warning(
            "美股 %s 无法确定本币→美元汇率（%s），估值章节将标记为不可用",
            (quote or {}).get("ticker") or "?",
            fx_source,
        )
    return {
        "profit_sheet": converted.get("综合损益表", pd.DataFrame()),
        "balance_sheet": converted.get("资产负债表", pd.DataFrame()),
        "cash_flow_sheet": converted.get("现金流量表", pd.DataFrame()),
        "financial_abstract": abstract,
        "meta": meta,
    }


def update_us_financial_data_cached(
    symbol_info: SymbolInfo,
    base_data_dir: str | Path = "data",
    force_refresh: bool = False,
    force_refresh_financials: bool | None = None,
    logger: logging.Logger | None = None,
) -> Dict[str, pd.DataFrame]:
    resolved_logger = _resolve_logger(logger)
    cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.FINANCIALS,
        base_dir=base_data_dir,
        ensure=True,
    )
    force = bool(force_refresh or force_refresh_financials)
    if not force and not should_refresh(cache_dir, CacheKind.FINANCIALS):
        return {}

    resolved_logger.info("正在获取美股 %s %s 三大报表...", symbol_info.stock_name, symbol_info.symbol)
    frames, indicator_frame = fetch_us_financial_frames(symbol_info.code, logger=resolved_logger)
    if not any(frames[statement] for statement in US_STATEMENT_KEYS):
        resolved_logger.error("美股 %s 三大报表全部为空，放弃写入缓存", symbol_info.symbol)
        return {}

    quote = fetch_us_quote(symbol_info.code, logger=resolved_logger)

    filing_notice_map: Dict[pd.Timestamp, pd.Timestamp] = {}
    try:
        from shared_data_access.us_disclosures import (
            filing_notice_date_map,
            load_sec_submissions,
            normalize_filings,
        )

        payload = load_sec_submissions(
            symbol_info.code,
            base_dir=base_data_dir,
            force_refresh=False,
            logger=resolved_logger,
        )
        filings = normalize_filings(payload, lookback_days=3650)
        filing_notice_map = filing_notice_date_map(filings)
    except Exception as exc:
        resolved_logger.warning(
            "读取 SEC 申报日失败 %s，公告日退回指标接口/法定滞后: %s",
            symbol_info.symbol,
            exc,
        )

    normalized = normalize_us_financials(
        frames=frames,
        indicator_frame=indicator_frame,
        quote=quote,
        filing_notice_map=filing_notice_map,
        logger=resolved_logger,
    )

    result: Dict[str, pd.DataFrame] = {}
    for statement, filename in US_STATEMENT_FILES.items():
        frame = normalized.get(
            {
                "综合损益表": "profit_sheet",
                "资产负债表": "balance_sheet",
                "现金流量表": "cash_flow_sheet",
            }[statement]
        )
        if frame is None or frame.empty:
            resolved_logger.warning("美股 %s %s 为空，跳过写入", symbol_info.symbol, statement)
            continue
        path = cache_dir / filename
        frame.to_csv(path, index=False, encoding="utf-8")
        resolved_logger.info("已缓存美股 %s 到 %s", statement, path)
        result[filename] = frame

    abstract = normalized.get("financial_abstract")
    if abstract is not None and not abstract.empty:
        abstract_path = cache_dir / "financial_abstract.csv"
        abstract.to_csv(abstract_path, index=False, encoding="utf-8")
        resolved_logger.info("已缓存美股财务摘要到 %s", abstract_path)
        result["financial_abstract.csv"] = abstract

    meta_path = cache_dir / US_META_FILENAME
    meta_path.write_text(
        json.dumps(normalized.get("meta") or {}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    record_cache_refresh(cache_dir)
    return result


__all__ = [
    "US_META_FILENAME",
    "build_quarterly_wide_frame",
    "compute_implied_fx",
    "fetch_us_financial_frames",
    "normalize_us_financials",
    "update_us_financial_data_cached",
]
