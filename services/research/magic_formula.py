"""Greenblatt 神奇公式研究回测（《股市稳赚》The Little Book That Beats the Market）。

核心口径（见书附录）：
- 投资回报率 Return on Capital = EBIT_TTM / (净营运资本 + 净固定资产)
  - 净营运资本 = 经营性流动资产 - 经营性流动负债
    - 经营性流动资产 = 应收票据及应收账款 + 存货 + 预付款项 + 其他流动资产（剔除多余现金）
    - 经营性流动负债 = 流动负债合计 - 短期有息债务
  - 净固定资产 = 固定资产净值 + 在建工程 + 使用权资产 - 商誉 - 无形资产
- 收益率 Earnings Yield = EBIT_TTM / EV，EV = 总市值 + 净有息债务
  - 净有息债务 = 有息负债合计 - 货币资金
- EBIT_TTM 由利润表累计口径滚动为 TTM：EBIT = 利润总额 + 利息费用
  （利息费用缺失时回退 营业利润 + 财务费用；不依赖披露稀疏的折旧摊销）

选股：对全宇宙按收益率、投资回报率各自排名，两排名相加，取综合最小者买入，
等权持有至下一调仓日再轮换。

本模块只读取本地缓存，并按公告日 (NOTICE_DATE) 做 PIT 截断，避免前视偏差。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from commons.stock_utils import get_stock_data_dir, parse_symbol
from core.logging import get_logger
from indicator_library.calculators.fundamental import calculate_rolling_ttm_profit
from services.selection_system.factor_history import (
    _align_event_series,
    _load_share_history,
    _read_financial_csv,
)
from services.selection_system.master_universe import load_master_universe


LOGGER = get_logger("MagicFormula")

FINANCIAL_KEYWORDS = ("银行", "保险", "证券", "信托", "金融", "租赁")

DEFAULT_TOP_N = 20
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_END_DATE = "2026-09-30"


@dataclass(frozen=True)
class MagicFormulaConfig:
    """神奇公式回测配置。"""

    base_dir: str = "data"
    output_dir: str = "data/magic_formula"
    start_date: str = DEFAULT_START_DATE
    end_date: str = DEFAULT_END_DATE
    top_n: int = DEFAULT_TOP_N
    exclude_financials: bool = True
    min_period_price_observations: int = 20
    rebalance_freq: str = "A"  # A=年度, S=半年, Q=季度, M=月度


def _today_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _normalize_num(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(series, errors="coerce")


def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name in frame.columns:
        return _normalize_num(frame[name])
    return pd.Series(np.nan, index=frame.index, dtype=float)


def _sum_columns(frame: pd.DataFrame, names: Sequence[str], *, require_any: bool = False) -> pd.Series:
    """按列名求和；缺失列按 0 处理。require_any=True 时全缺失返回 NaN。"""
    present = [name for name in names if name in frame.columns]
    if not present:
        return pd.Series(np.nan if require_any else 0.0, index=frame.index, dtype=float)
    total = pd.Series(0.0, index=frame.index, dtype=float)
    for name in present:
        total = total.add(_normalize_num(frame[name]).fillna(0.0), fill_value=0.0)
    return total


def _is_financial(stock: Any) -> bool:
    text = f"{getattr(stock, 'sector', '')}{getattr(stock, 'industry', '')}{getattr(stock, 'name', '')}"
    return any(keyword in text for keyword in FINANCIAL_KEYWORDS)


UTILITY_SECTOR_KEYWORD = "公用事业"
UTILITY_INDUSTRY_KEYWORDS = ("电力", "水务", "燃气", "热力", "供水", "供气", "水电", "核电运营", "风电光伏运营")


def _is_utility(sector: str, industry: str) -> bool:
    """严格按书本"渠道二"排除公用事业（电力/水务/燃气等）。"""
    if UTILITY_SECTOR_KEYWORD in (sector or ""):
        return True
    return any(keyword in (industry or "") for keyword in UTILITY_INDUSTRY_KEYWORDS)


def load_a_share_universe(base_dir: str = "data", *, exclude_financials: bool = True) -> list[Any]:
    """加载 master_universe 中的 A 股标的（剔除 ETF，可选剔除金融）。"""
    document = load_master_universe()
    stocks = []
    for stock in document.stocks:
        symbol = stock.symbol
        if not symbol.endswith((".SH", ".SZ")):
            continue
        if "ETF" in stock.name.upper() or "ETF" in symbol.upper():
            continue
        if exclude_financials and _is_financial(stock):
            continue
        stocks.append(stock)
    LOGGER.info("神奇公式宇宙: %d 只 A 股(剔除 ETF%s)", len(stocks), " 与金融" if exclude_financials else "")
    return stocks


# --------------------------------------------------------------------------- #
# 财报口径
# --------------------------------------------------------------------------- #
def _notice_date_series(frame: pd.DataFrame, report_col: str = "REPORT_DATE") -> pd.Series:
    notice_col = None
    for candidate in ("NOTICE_DATE", "公告日期"):
        if candidate in frame.columns:
            notice_col = candidate
            break
    if report_col in frame.columns:
        report = pd.to_datetime(frame[report_col], errors="coerce")
    else:
        report = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    if notice_col is None:
        return report
    notice = pd.to_datetime(frame[notice_col], errors="coerce")
    inferred = report + pd.to_timedelta(np.where(report.dt.month == 12, 120, 45), unit="D")
    return notice.fillna(inferred)


def _ebit_series(profit: pd.DataFrame) -> pd.Series:
    """EBIT = 利润总额 + 利息费用；缺失时回退 营业利润 + 财务费用。"""
    total_profit = _column(profit, "TOTAL_PROFIT")
    interest_expense = _column(profit, "FE_INTEREST_EXPENSE")
    operate_profit = _column(profit, "OPERATE_PROFIT")
    finance_expense = _column(profit, "FINANCE_EXPENSE")

    primary = total_profit + interest_expense.fillna(0.0)
    fallback = operate_profit + finance_expense.fillna(0.0)
    ebit = primary.where(total_profit.notna() & interest_expense.notna(), fallback)
    return ebit


def ttm_metric_history(profit: pd.DataFrame, value_series: pd.Series) -> pd.DataFrame:
    """把累计口径的任意利润表指标滚动为 TTM，并按公告日索引（单位：元）。"""
    if profit is None or profit.empty or "REPORT_DATE" not in profit.columns:
        return pd.DataFrame(columns=["notice_date", "TTM_RAW"])
    work = profit.copy()
    work["_TTM_VALUE_"] = pd.to_numeric(value_series, errors="coerce")
    ttm = calculate_rolling_ttm_profit(work, profit_column="_TTM_VALUE_", logger=LOGGER)
    if ttm.empty or "REPORT_DATE" not in ttm.columns:
        return pd.DataFrame(columns=["notice_date", "TTM_RAW"])
    ttm = ttm.copy()
    ttm["REPORT_DATE"] = pd.to_datetime(ttm["REPORT_DATE"], errors="coerce")
    ttm = ttm.dropna(subset=["REPORT_DATE"]).sort_values("REPORT_DATE")
    release_map = _release_date_map(profit)
    ttm["notice_date"] = ttm["REPORT_DATE"].map(
        lambda dt: release_map.get(pd.Timestamp(dt).normalize(), pd.Timestamp(dt).normalize() + pd.Timedelta(days=45))
    )
    ttm = ttm.dropna(subset=["notice_date"]).sort_values("notice_date")
    ttm["TTM_RAW"] = pd.to_numeric(ttm["TTM_NET_PROFIT_RAW"], errors="coerce")
    return ttm[["notice_date", "TTM_RAW"]].dropna().reset_index(drop=True)


def ebit_ttm_history(profit: pd.DataFrame) -> pd.DataFrame:
    """返回以公告日为索引的 EBIT TTM 序列（单位：元）。"""
    if profit is None or profit.empty:
        return pd.DataFrame(columns=["notice_date", "EBIT_TTM_RAW"])
    frame = ttm_metric_history(profit, _ebit_series(profit))
    return frame.rename(columns={"TTM_RAW": "EBIT_TTM_RAW"})


def _release_date_map(frame: pd.DataFrame) -> dict[pd.Timestamp, pd.Timestamp]:
    if "REPORT_DATE" not in frame.columns:
        return {}
    work = frame[["REPORT_DATE"]].copy()
    work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
    work["notice_date"] = _notice_date_series(frame)
    work = work.dropna(subset=["REPORT_DATE", "notice_date"])
    return {
        pd.Timestamp(row["REPORT_DATE"]).normalize(): pd.Timestamp(row["notice_date"]).normalize()
        for _, row in work.iterrows()
    }


def balance_metrics_frame(balance: pd.DataFrame) -> pd.DataFrame:
    """按公告日输出每期资产负债表衍生指标（单位：亿元）。"""
    if balance is None or balance.empty:
        return pd.DataFrame()
    work = balance.copy()
    notice = _notice_date_series(work)
    frame = pd.DataFrame(index=work.index)

    receivables_combined = _column(work, "NOTE_ACCOUNTS_RECE")
    receivables_split = _column(work, "NOTE_RECE").fillna(0.0) + _column(work, "ACCOUNTS_RECE").fillna(0.0)
    receivables = receivables_combined.where(receivables_combined.notna(), receivables_split)

    operating_assets = (
        receivables.fillna(0.0)
        + _column(work, "INVENTORY").fillna(0.0)
        + _column(work, "PREPAYMENT").fillna(0.0)
        + _column(work, "OTHER_CURRENT_ASSET").fillna(0.0)
    )
    short_interest_debt = _sum_columns(
        work, ("SHORT_LOAN", "NONCURRENT_LIAB_1YEAR", "SHORT_BOND_PAYABLE", "SHORT_FIN_PAYABLE")
    )
    operating_liabilities = _column(work, "TOTAL_CURRENT_LIAB") - short_interest_debt
    net_working_capital = operating_assets - operating_liabilities

    net_fixed_assets = (
        _column(work, "FIXED_ASSET").fillna(0.0)
        + _column(work, "CIP").fillna(0.0)
        + _column(work, "USERIGHT_ASSET").fillna(0.0)
        - _column(work, "GOODWILL").fillna(0.0)
        - _column(work, "INTANGIBLE_ASSET").fillna(0.0)
    )

    interest_debt = _sum_columns(
        work,
        (
            "SHORT_LOAN",
            "LONG_LOAN",
            "BOND_PAYABLE",
            "SHORT_BOND_PAYABLE",
            "NONCURRENT_LIAB_1YEAR",
            "LEASE_LIAB",
            "SHORT_FIN_PAYABLE",
            "LONG_PAYABLE",
        ),
    )
    net_debt = interest_debt - _column(work, "MONETARYFUNDS").fillna(0.0)

    frame["notice_date"] = notice.values
    frame["net_working_capital_yi"] = net_working_capital.values / 1e8
    frame["net_fixed_assets_yi"] = net_fixed_assets.values / 1e8
    frame["net_debt_yi"] = net_debt.values / 1e8
    frame["total_assets_yi"] = _column(work, "TOTAL_ASSETS").values / 1e8
    frame = frame.dropna(subset=["notice_date"]).sort_values("notice_date")
    return frame


def _aligned_latest(frame: pd.DataFrame, column: str, dates: pd.DatetimeIndex, index_name: str = "notice_date") -> pd.Series:
    if frame is None or frame.empty or column not in frame.columns:
        return pd.Series(np.nan, index=dates, dtype=float)
    series = pd.Series(pd.to_numeric(frame[column], errors="coerce").values, index=pd.to_datetime(frame[index_name]))
    series = series.dropna().sort_index()
    series = series[~series.index.duplicated(keep="last")]
    aligned = series.reindex(series.index.union(dates)).sort_index().ffill().reindex(dates)
    return pd.to_numeric(aligned, errors="coerce")


def _latest_notice_before(frame: pd.DataFrame, as_of_date: pd.Timestamp) -> pd.Timestamp:
    """返回不晚于 as_of_date 的最近一次财报公告日（用于"上周公布收益"过滤）。"""
    if frame is None or frame.empty or "REPORT_DATE" not in frame.columns:
        return pd.NaT
    notice = pd.to_datetime(_notice_date_series(frame), errors="coerce").dropna()
    notice = notice[notice <= pd.Timestamp(as_of_date)]
    return notice.max() if len(notice) else pd.NaT


# --------------------------------------------------------------------------- #
# 横截面评分
# --------------------------------------------------------------------------- #
def _stock_data_dir(base_dir: str, symbol: str) -> Path:
    """按 symbol 后缀定位缓存目录，避免改名/ST 导致目录名与当前名称不一致。"""
    root = Path(base_dir) / "stock_info"
    hits = sorted(root.glob(f"*_{symbol}"))
    if hits:
        return hits[0]
    return get_stock_data_dir(parse_symbol(symbol), base_dir=base_dir)


def _stock_frames(stock: Any, base_dir: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data_dir = _stock_data_dir(base_dir, stock.symbol)
    fin_dir = data_dir / "financials_cache"
    profit = _read_financial_csv(fin_dir / "profit_sheet.csv")
    balance = _read_financial_csv(fin_dir / "balance_sheet.csv")
    price = _read_financial_csv(data_dir / "prices" / "price.csv")
    if not price.empty and "日期" in price.columns:
        price["日期"] = pd.to_datetime(price["日期"], errors="coerce")
        price = price.dropna(subset=["日期"]).sort_values("日期").set_index("日期")
    share_path = data_dir / "share_info" / "stock_share_change_cninfo.csv"
    share_history = _load_share_history(share_path)
    return profit, balance, price, share_history


def build_cross_section(
    stocks: Sequence[Any],
    as_of_date: pd.Timestamp,
    *,
    base_dir: str = "data",
) -> pd.DataFrame:
    """在给定调仓日构造宇宙横截面（PIT）。"""
    target = pd.DatetimeIndex([pd.Timestamp(as_of_date).normalize()])
    records: list[dict[str, Any]] = []
    for stock in stocks:
        try:
            profit, balance, price, share_history = _stock_frames(stock, base_dir)
        except Exception as exc:  # pragma: no cover - 防御
            LOGGER.warning("跳过 %s: 读取缓存失败 %s", stock.symbol, exc)
            continue
        if profit.empty or balance.empty or price.empty:
            continue

        ebit_ttm = ebit_ttm_history(profit)
        ebit_value = _align_event_series(
            pd.Series(ebit_ttm["EBIT_TTM_RAW"].values, index=pd.to_datetime(ebit_ttm["notice_date"])),
            target,
        )
        parent_ttm = ttm_metric_history(profit, _column(profit, "PARENT_NETPROFIT"))
        parent_value = _align_event_series(
            pd.Series(parent_ttm["TTM_RAW"].values, index=pd.to_datetime(parent_ttm["notice_date"])),
            target,
        )
        balance_frame = balance_metrics_frame(balance)
        nwc = _aligned_latest(balance_frame, "net_working_capital_yi", target)
        nfa = _aligned_latest(balance_frame, "net_fixed_assets_yi", target)
        net_debt = _aligned_latest(balance_frame, "net_debt_yi", target)
        total_assets = _aligned_latest(balance_frame, "total_assets_yi", target)

        close = _column(price, "收盘").dropna()
        if close.empty:
            continue
        close_price = float(close.reindex(close.index.union(target)).sort_index().ffill().reindex(target).iloc[-1])
        total_shares = _share_series_for_dates(share_history, target)
        shares = float(total_shares.iloc[-1]) if not total_shares.empty else np.nan
        if not np.isfinite(close_price) or not np.isfinite(shares) or shares <= 0:
            continue

        market_cap_yi = close_price * shares / 1e8
        ebit_yi = float(ebit_value.iloc[-1]) / 1e8 if np.isfinite(ebit_value.iloc[-1]) else np.nan
        parent_netprofit_yi = float(parent_value.iloc[-1]) / 1e8 if np.isfinite(parent_value.iloc[-1]) else np.nan
        nwc_yi = float(nwc.iloc[-1])
        nfa_yi = float(nfa.iloc[-1])
        net_debt_yi = float(net_debt.iloc[-1])
        total_assets_yi = float(total_assets.iloc[-1])
        ev_yi = market_cap_yi + net_debt_yi if np.isfinite(net_debt_yi) else np.nan
        invested_capital_yi = nwc_yi + nfa_yi
        earnings_yield = ebit_yi / ev_yi if np.isfinite(ev_yi) and ev_yi > 0 else np.nan
        return_on_capital = ebit_yi / invested_capital_yi if np.isfinite(invested_capital_yi) and invested_capital_yi > 0 else np.nan
        roa = ebit_yi / total_assets_yi if np.isfinite(total_assets_yi) and total_assets_yi > 0 else np.nan
        pe_ttm = market_cap_yi / parent_netprofit_yi if np.isfinite(parent_netprofit_yi) and parent_netprofit_yi > 0 else np.nan

        records.append(
            {
                "symbol": stock.symbol,
                "name": stock.name,
                "sector": getattr(stock, "sector", ""),
                "industry": getattr(stock, "industry", ""),
                "close": close_price,
                "market_cap_yi": market_cap_yi,
                "ebit_ttm_yi": ebit_yi,
                "parent_netprofit_ttm_yi": parent_netprofit_yi,
                "net_debt_yi": net_debt_yi,
                "ev_yi": ev_yi,
                "total_assets_yi": total_assets_yi,
                "net_working_capital_yi": nwc_yi,
                "net_fixed_assets_yi": nfa_yi,
                "invested_capital_yi": invested_capital_yi,
                "earnings_yield": earnings_yield,
                "return_on_capital": return_on_capital,
                "roa": roa,
                "pe_ttm": pe_ttm,
                "last_notice_date": _latest_notice_before(profit, as_of_date),
            }
        )
    return pd.DataFrame(records)


def _share_series_for_dates(share_history: pd.DataFrame, dates: pd.DatetimeIndex) -> pd.Series:
    if share_history is None or share_history.empty:
        return pd.Series(np.nan, index=dates, dtype=float)
    series = share_history.set_index("effective_date")["total_shares"].sort_index()
    result = series.reindex(series.index.union(dates)).sort_index().ffill().reindex(dates)
    return pd.to_numeric(result, errors="coerce")


def rank_cross_section(cross_section: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """按收益率与投资回报率排名相加，返回综合排名最靠前的 top_n。"""
    if cross_section.empty:
        return cross_section
    work = cross_section.copy()
    valid = work["earnings_yield"].notna() & work["return_on_capital"].notna()
    work = work[valid].copy()
    if work.empty:
        return work
    work["earnings_yield_rank"] = work["earnings_yield"].rank(ascending=False, method="first")
    work["return_on_capital_rank"] = work["return_on_capital"].rank(ascending=False, method="first")
    work["combined_rank"] = work["earnings_yield_rank"] + work["return_on_capital_rank"]
    work = work.sort_values("combined_rank").reset_index(drop=True)
    work["rank"] = np.arange(1, len(work) + 1)
    return work.head(top_n).reset_index(drop=True)


def select_holdings(cross_section: pd.DataFrame, top_n: int) -> list[str]:
    ranked = rank_cross_section(cross_section, top_n)
    return ranked["symbol"].tolist()


CHANNEL2_ROA_THRESHOLD = 0.25
CHANNEL2_MIN_PE = 5.0
CHANNEL2_EXCLUDE_RECENT_EARNINGS_DAYS = 7


def rank_channel2(
    cross_section: pd.DataFrame,
    top_n: int,
    as_of_date: pd.Timestamp,
    *,
    roa_threshold: float = CHANNEL2_ROA_THRESHOLD,
    min_pe: float = CHANNEL2_MIN_PE,
    exclude_recent_days: int = CHANNEL2_EXCLUDE_RECENT_EARNINGS_DAYS,
) -> pd.DataFrame:
    """书本第 14 章"渠道二"：ROA≥25% + 最低 P/E，排除公用事业/金融/低 P/E/上周披露。

    注：A 股无 ADR，"排除外国公司" 不适用；数据源不含"共同基金"。
    """
    if cross_section.empty:
        return cross_section
    work = cross_section.copy()
    work = work[work["roa"].notna() & (work["roa"] >= roa_threshold)]
    work = work[work["pe_ttm"].notna() & (work["pe_ttm"] > min_pe)]
    work = work[~work.apply(lambda row: _is_utility(str(row.get("sector", "")), str(row.get("industry", ""))), axis=1)]
    if exclude_recent_days > 0:
        cutoff = pd.Timestamp(as_of_date).normalize() - pd.Timedelta(days=exclude_recent_days)
        last_notice = pd.to_datetime(work["last_notice_date"], errors="coerce")
        work = work[~(last_notice >= cutoff)]
    work = work.sort_values("pe_ttm").reset_index(drop=True)
    work["rank"] = np.arange(1, len(work) + 1)
    return work.head(top_n).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 回测
# --------------------------------------------------------------------------- #
@dataclass
class BacktestResult:
    """回测结果容器。"""

    nav: pd.DataFrame
    holdings: pd.DataFrame
    summary: dict[str, Any]
    rebalance_dates: list[pd.Timestamp]


def _load_close_matrix(stocks: Sequence[Any], base_dir: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    frames: dict[str, pd.Series] = {}
    for stock in stocks:
        data_dir = _stock_data_dir(base_dir, stock.symbol)
        price = _read_financial_csv(data_dir / "prices" / "price.csv")
        if price.empty or "日期" not in price.columns or "收盘" not in price.columns:
            continue
        dates = pd.to_datetime(price["日期"], errors="coerce")
        series = pd.Series(pd.to_numeric(price["收盘"], errors="coerce").values, index=dates).dropna()
        frames[stock.symbol] = series[~series.index.duplicated(keep="last")]
    if not frames:
        return pd.DataFrame()
    matrix = pd.DataFrame(frames).sort_index()
    matrix = matrix.loc[(matrix.index >= start) & (matrix.index <= end)]
    return matrix


_REBALANCE_MONTHS: dict[str, list[int]] = {
    "A": [1],
    "S": [1, 7],
    "Q": [1, 4, 7, 10],
    "M": list(range(1, 13)),
}
_FREQ_LABEL = {"A": "每年一次", "S": "每半年一次", "Q": "每季度一次", "M": "每月一次"}


def _resolve_rebalance_dates(
    all_dates: pd.DatetimeIndex,
    start: pd.Timestamp,
    end: pd.Timestamp,
    freq: str = "A",
) -> list[pd.Timestamp]:
    freq = (freq or "A").upper()
    if freq not in _REBALANCE_MONTHS:
        raise ValueError(f"未知调仓频率: {freq}（可选 A/S/Q/M）")
    anchors = [start]
    for year in range(start.year, end.year + 1):
        for month in _REBALANCE_MONTHS[freq]:
            anchors.append(pd.Timestamp(year=year, month=month, day=1))
    dates: list[pd.Timestamp] = []
    for target in sorted(set(anchors)):
        if target > end:
            continue
        after = all_dates[all_dates >= target]
        if len(after) == 0:
            continue
        candidate = after[0]
        if start <= candidate <= end:
            dates.append(candidate)
    return sorted(set(dates))


def _period_series(close: pd.DataFrame, symbols: Sequence[str], d0: pd.Timestamp, d1: pd.Timestamp) -> pd.Series:
    cols = [s for s in symbols if s in close.columns]
    if not cols:
        return pd.Series(dtype=float)
    history = close.loc[:d0, cols].ffill()
    if history.empty:
        return pd.Series(dtype=float)
    base = history.iloc[-1]
    window = close.loc[(close.index > d0) & (close.index <= d1), cols]
    if window.empty:
        return pd.Series({d0: 1.0})
    relative = window.div(base, axis=1)
    value = relative.mean(axis=1, skipna=True)
    return pd.concat([pd.Series({d0: 1.0}), value])


def _chain_periods(close: pd.DataFrame, scheduled: list[tuple[pd.Timestamp, pd.Timestamp, Sequence[str]]]) -> pd.Series:
    nav_parts: list[pd.Series] = []
    cumulative = 1.0
    for d0, d1, symbols in scheduled:
        period = _period_series(close, symbols, d0, d1)
        if period.empty:
            continue
        nav_parts.append(period * cumulative)
        cumulative = float((period * cumulative).iloc[-1])
    if not nav_parts:
        return pd.Series(dtype=float)
    nav = pd.concat(nav_parts)
    nav = nav[~nav.index.duplicated(keep="first")].sort_index()
    return nav / nav.iloc[0]


def _performance_metrics(nav: pd.Series) -> dict[str, Any]:
    if nav.empty or len(nav) < 2:
        return {}
    returns = nav.pct_change().dropna()
    total_return = float(nav.iloc[-1] / nav.iloc[0] - 1.0)
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = float((nav.iloc[-1] / nav.iloc[0]) ** (1.0 / years) - 1.0) if years > 0 else float("nan")
    volatility = float(returns.std() * np.sqrt(252)) if len(returns) > 1 else float("nan")
    sharpe = float(cagr / volatility) if volatility and np.isfinite(volatility) and volatility > 0 else float("nan")
    max_drawdown = float((nav / nav.cummax() - 1.0).min())
    return {
        "total_return": round(total_return, 4),
        "cagr": round(cagr, 4),
        "annual_volatility": round(volatility, 4),
        "sharpe": round(sharpe, 3),
        "max_drawdown": round(max_drawdown, 4),
        "years": round(years, 2),
        "start": nav.index[0].strftime("%Y-%m-%d"),
        "end": nav.index[-1].strftime("%Y-%m-%d"),
    }


def _load_index_series(base_dir: str, *name_fragments: str) -> pd.Series | None:
    """按目录名片段匹配本地指数缓存，返回收盘价序列。"""
    root = Path(base_dir) / "stock_info"
    seen: set[Path] = set()
    for fragment in name_fragments:
        for candidate in sorted(root.glob(f"*{fragment}*")):
            if candidate in seen:
                continue
            seen.add(candidate)
            price = _read_financial_csv(candidate / "prices" / "price.csv")
            if price.empty or "日期" not in price.columns or "收盘" not in price.columns:
                continue
            dates = pd.to_datetime(price["日期"], errors="coerce")
            series = pd.Series(pd.to_numeric(price["收盘"], errors="coerce").values, index=dates).dropna()
            if not series.empty:
                return series[~series.index.duplicated(keep="last")].sort_index()
    return None


def run_backtest(
    config: MagicFormulaConfig | None = None,
    *,
    stocks: Sequence[Any] | None = None,
    method: str = "magic",
    membership: dict[int, list[Any]] | None = None,
) -> BacktestResult:
    """对宇宙做等权、年度调仓回测。

    method="magic": 神奇公式（收益率 + 投资回报率综合排名）。
    method="channel2": 书本第 14 章"渠道二"（ROA≥25% 后取最低 P/E）。
    membership: {年份: 该年沪深300成分列表}；给定时按调仓日动态重建股票池（消除幸存者偏差）。
    """
    if method not in {"magic", "channel2"}:
        raise ValueError(f"未知 method: {method}")
    config = config or MagicFormulaConfig()
    dynamic = bool(membership)
    if dynamic:
        merged: dict[str, Any] = {}
        for member_list in membership.values():
            for stock in member_list:
                merged[stock.symbol] = stock
        universe = list(merged.values())
    elif stocks is not None:
        universe = list(stocks)
    else:
        universe = load_a_share_universe(config.base_dir, exclude_financials=config.exclude_financials)
    start = pd.Timestamp(config.start_date)
    end = pd.Timestamp(config.end_date)
    close = _load_close_matrix(universe, config.base_dir, start - pd.Timedelta(days=15), end)
    if close.empty:
        raise RuntimeError("价格矩阵为空，无法回测")

    rebalances = _resolve_rebalance_dates(close.index, start, end, config.rebalance_freq)
    if not rebalances:
        raise RuntimeError("没有可用的调仓日")

    scheduled: list[tuple[pd.Timestamp, pd.Timestamp, Sequence[str]]] = []
    benchmark_scheduled: list[tuple[pd.Timestamp, pd.Timestamp, Sequence[str]]] = []
    all_symbols = [s for s in close.columns]
    holding_rows: list[dict[str, Any]] = []
    for idx, d0 in enumerate(rebalances):
        d1 = rebalances[idx + 1] if idx + 1 < len(rebalances) else end
        period_universe = membership.get(d0.year, universe) if dynamic else universe
        cross_section = build_cross_section(period_universe, d0, base_dir=config.base_dir)
        if method == "channel2":
            ranked = rank_channel2(cross_section, config.top_n, d0)
        else:
            ranked = rank_cross_section(cross_section, config.top_n)
        holdings = ranked["symbol"].tolist()
        if not holdings:
            LOGGER.warning("调仓日 %s 无有效持仓，跳过", d0.strftime("%Y-%m-%d"))
            continue
        scheduled.append((d0, d1, holdings))
        benchmark_symbols = [s.symbol for s in period_universe] if dynamic else all_symbols
        benchmark_scheduled.append((d0, d1, benchmark_symbols))
        period = f"{d0.strftime('%Y-%m-%d')}~{d1.strftime('%Y-%m-%d')}"
        for _, row in ranked.iterrows():
            holding_rows.append(
                {
                    "period": period,
                    "rebalance_date": d0.strftime("%Y-%m-%d"),
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "rank": int(row["rank"]),
                    "earnings_yield": round(float(row["earnings_yield"]), 5) if np.isfinite(row.get("earnings_yield", np.nan)) else None,
                    "return_on_capital": round(float(row["return_on_capital"]), 5) if np.isfinite(row.get("return_on_capital", np.nan)) else None,
                    "roa": round(float(row["roa"]), 5) if np.isfinite(row.get("roa", np.nan)) else None,
                    "pe_ttm": round(float(row["pe_ttm"]), 3) if np.isfinite(row.get("pe_ttm", np.nan)) else None,
                    "market_cap_yi": round(float(row["market_cap_yi"]), 2),
                    "ev_yi": round(float(row["ev_yi"]), 2) if np.isfinite(row.get("ev_yi", np.nan)) else None,
                }
            )
        LOGGER.info("[%s] 调仓 %s: 选中 %d 只", method, d0.strftime("%Y-%m-%d"), len(holdings))

    portfolio_nav = _chain_periods(close, scheduled)
    benchmark_nav = _chain_periods(close, benchmark_scheduled)
    nav = pd.DataFrame({"portfolio": portfolio_nav, "benchmark": benchmark_nav})

    def attach_index(series: pd.Series | None, column: str) -> dict[str, Any]:
        if series is None or portfolio_nav.empty:
            return {}
        aligned = series.reindex(series.index.union(portfolio_nav.index)).sort_index().ffill().reindex(portfolio_nav.index).dropna()
        if aligned.empty:
            return {}
        nav[column] = aligned / aligned.iloc[0]
        return _performance_metrics(nav[column].dropna())

    hs300_metrics = attach_index(_load_index_series(config.base_dir, "000300.SH", "沪深300"), "hs300")
    dividend_metrics = attach_index(_load_index_series(config.base_dir, "000015.IDX", "上证红利"), "dividend")
    nav = nav.dropna(how="all")
    nav.index.name = "date"

    summary = {
        "method": method,
        "dynamic_membership": dynamic,
        "config": asdict(config),
        "universe_size": len(universe),
        "portfolio": _performance_metrics(portfolio_nav),
        "benchmark": _performance_metrics(benchmark_nav),
        "hs300": hs300_metrics,
        "dividend": dividend_metrics,
    }
    if portfolio_nav.index.isin(benchmark_nav.index).any():
        aligned = pd.DataFrame({"p": portfolio_nav, "b": benchmark_nav}).dropna()
        if len(aligned) > 1:
            summary["annual_excess_return"] = round(
                float((aligned["p"].iloc[-1] / aligned["p"].iloc[0]) ** (1 / max((aligned.index[-1] - aligned.index[0]).days / 365.25, 1e-9))
                      - (aligned["b"].iloc[-1] / aligned["b"].iloc[0]) ** (1 / max((aligned.index[-1] - aligned.index[0]).days / 365.25, 1e-9))),
                4,
            )
    return BacktestResult(
        nav=nav,
        holdings=pd.DataFrame(holding_rows),
        summary=summary,
        rebalance_dates=rebalances,
    )


# --------------------------------------------------------------------------- #
# 输出
# --------------------------------------------------------------------------- #
def _format_pct(value: Any) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "n/a"


def _method_title(method: str) -> str:
    return "神奇公式·渠道二 A 股回测报告" if method == "channel2" else "神奇公式 A 股回测报告"


def _holding_line(method: str, group: pd.DataFrame) -> str:
    parts = []
    for _, row in group.iterrows():
        if method == "channel2":
            pe = f"{row['pe_ttm']:.1f}" if pd.notna(row.get("pe_ttm")) else "n/a"
            roa = f"{row['roa'] * 100:.1f}%" if pd.notna(row.get("roa")) else "n/a"
            parts.append(f"{row['name']}({row['symbol']}, P/E{pe}, ROA{roa})")
        else:
            parts.append(f"{row['name']}({row['symbol']}, 综合#{int(row['rank'])})")
    return "、".join(parts)


def write_outputs(result: BacktestResult, config: MagicFormulaConfig, method: str | None = None) -> dict[str, str]:
    method = method or str(result.summary.get("method", "magic"))
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _today_stamp()

    nav_path = output_dir / f"magic_formula_{method}_nav.csv"
    holdings_path = output_dir / f"magic_formula_{method}_holdings.csv"
    summary_path = output_dir / f"magic_formula_{method}_summary.json"
    report_path = output_dir / f"magic_formula_{method}_report_{stamp}.md"

    result.nav.to_csv(nav_path, encoding="utf-8")
    result.holdings.to_csv(holdings_path, index=False, encoding="utf-8")
    summary_path.write_text(json.dumps(result.summary, ensure_ascii=False, indent=2), encoding="utf-8")

    portfolio = result.summary.get("portfolio", {})
    benchmark = result.summary.get("benchmark", {})
    hs300 = result.summary.get("hs300", {}) or {}
    dividend = result.summary.get("dividend", {}) or {}
    has_index = bool(hs300)
    has_dividend = bool(dividend)
    if method == "channel2":
        rule = "渠道二: ROA≥25% 后取最低 P/E，排除公用事业/金融、P/E≤5、上周披露财报者"
    else:
        rule = "神奇公式: 收益率(EBIT/EV) 与投资回报率(EBIT/有形资本) 各自排名相加"
    if result.summary.get("dynamic_membership"):
        universe_desc = f"沪深300 历史成分（逐期动态重建，去重 {result.summary.get('universe_size')} 只，消除幸存者偏差）"
        bench_label = "当期沪深300等权"
    else:
        universe_desc = f"master_universe A 股（{result.summary.get('universe_size')} 只，剔除 ETF" + ("与金融" if config.exclude_financials else "") + "）"
        bench_label = "宇宙等权"
    lines = [
        f"# {_method_title(method)}",
        "",
        f"- 宇宙: {universe_desc}",
        f"- 选股规则: {rule}",
        f"- 区间: {portfolio.get('start', config.start_date)} ~ {portfolio.get('end', config.end_date)}"
        "（起始日以各标的实际可用价格数据为准）",
        f"- 调仓: {_FREQ_LABEL.get((config.rebalance_freq or 'A').upper(), config.rebalance_freq)}，组合规模前 {config.top_n}，等权、买入持有至下个调仓日",
        "",
        "## 业绩对比",
        "",
        f"| 指标 | 组合 | 沪深300 | 上证红利 | {bench_label} |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| 累计收益 | {_format_pct(portfolio.get('total_return'))} | {_format_pct(hs300.get('total_return')) if has_index else 'n/a'} | {_format_pct(dividend.get('total_return')) if has_dividend else 'n/a'} | {_format_pct(benchmark.get('total_return'))} |",
        f"| 年化收益(CAGR) | {_format_pct(portfolio.get('cagr'))} | {_format_pct(hs300.get('cagr')) if has_index else 'n/a'} | {_format_pct(dividend.get('cagr')) if has_dividend else 'n/a'} | {_format_pct(benchmark.get('cagr'))} |",
        f"| 年化波动 | {_format_pct(portfolio.get('annual_volatility'))} | {_format_pct(hs300.get('annual_volatility')) if has_index else 'n/a'} | {_format_pct(dividend.get('annual_volatility')) if has_dividend else 'n/a'} | {_format_pct(benchmark.get('annual_volatility'))} |",
        f"| 夏普 | {portfolio.get('sharpe')} | {hs300.get('sharpe') if has_index else 'n/a'} | {dividend.get('sharpe') if has_dividend else 'n/a'} | {benchmark.get('sharpe')} |",
        f"| 最大回撤 | {_format_pct(portfolio.get('max_drawdown'))} | {_format_pct(hs300.get('max_drawdown')) if has_index else 'n/a'} | {_format_pct(dividend.get('max_drawdown')) if has_dividend else 'n/a'} | {_format_pct(benchmark.get('max_drawdown'))} |",
        "",
        "## 每期持仓",
        "",
    ]
    if not result.holdings.empty:
        for period, group in result.holdings.groupby("period", sort=False):
            lines.append(f"- {period}: {_holding_line(method, group)}")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    paths = {
        "nav": str(nav_path),
        "holdings": str(holdings_path),
        "summary": str(summary_path),
        "report": str(report_path),
    }
    LOGGER.info("神奇公式输出: %s", paths)
    return paths


def write_comparison(results: dict[str, BacktestResult], config: MagicFormulaConfig) -> str:
    """输出神奇公式 vs 渠道二 的对照报告。"""
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"magic_formula_comparison_{_today_stamp()}.md"

    lines = [
        "# 神奇公式 vs 渠道二 对照",
        "",
        f"- 区间: {results['magic'].summary.get('portfolio', {}).get('start')} ~ {results['magic'].summary.get('portfolio', {}).get('end')}",
        f"- 宇宙: master_universe A 股 {results['magic'].summary.get('universe_size')} 只（剔 ETF/金融）",
        "- 渠道二规则: ROA≥25% 后取最低 P/E，排除公用事业/金融、P/E≤5、上周披露财报者",
        "",
        "| 指标 | 神奇公式 | 渠道二(ROA≥25%+低PE) | 宇宙等权 | 沪深300 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    metrics = [
        ("累计收益", "total_return", True),
        ("年化(CAGR)", "cagr", True),
        ("年化波动", "annual_volatility", True),
        ("夏普", "sharpe", False),
        ("最大回撤", "max_drawdown", True),
    ]
    for label, key, as_pct in metrics:
        magic = results["magic"].summary.get("portfolio", {}).get(key)
        ch2 = results["channel2"].summary.get("portfolio", {}).get(key)
        bench = results["magic"].summary.get("benchmark", {}).get(key)
        hs = results["magic"].summary.get("hs300", {}).get(key)
        fmt = (lambda v: f"{float(v)*100:.2f}%" if isinstance(v, (int, float)) else "n/a") if as_pct else (lambda v: f"{v}" if v is not None else "n/a")
        lines.append(f"| {label} | {fmt(magic)} | {fmt(ch2)} | {fmt(bench)} | {fmt(hs)} |")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    LOGGER.info("对照报告: %s", report_path)
    return str(report_path)


__all__ = [
    "MagicFormulaConfig",
    "BacktestResult",
    "FINANCIAL_KEYWORDS",
    "UTILITY_INDUSTRY_KEYWORDS",
    "CHANNEL2_ROA_THRESHOLD",
    "CHANNEL2_MIN_PE",
    "balance_metrics_frame",
    "build_cross_section",
    "ebit_ttm_history",
    "load_a_share_universe",
    "rank_channel2",
    "rank_cross_section",
    "run_backtest",
    "select_holdings",
    "ttm_metric_history",
    "write_comparison",
    "write_outputs",
]
