"""统一的日线行情抓取与规范化（A 股 / 港股 / ETF / 指数）。

所有来源在 :func:`_finalize_price_frame` 统一到 canonical 列与单位：
成交量=股，成交额=元，换手率=小数比例，流通股本=股。东财接口的「手 / 百分数」
由调用方通过 ``volume_in_lots`` / ``turnover_in_percent`` 声明后换算。
"""

from __future__ import annotations

import logging
import time

import akshare as ak  # type: ignore
import numpy as np
import pandas as pd
from requests import exceptions as requests_exceptions

from commons import SymbolInfo, api_call_with_delay

HK_HIST_MAX_RETRIES = 3


CANONICAL_PRICE_COLUMNS = (
    "日期",
    "开盘",
    "最高",
    "最低",
    "收盘",
    "成交量",
    "成交额",
    "换手率",
    "流通股本",
)

_PRICE_COLUMN_ALIASES = {
    "date": "日期",
    "open": "开盘",
    "high": "最高",
    "low": "最低",
    "close": "收盘",
    "volume": "成交量",
    "amount": "成交额",
    "turnover": "换手率",
    "outstanding_share": "流通股本",
}


def _finalize_price_frame(
    df: pd.DataFrame,
    *,
    volume_in_lots: bool = False,
    turnover_in_percent: bool = False,
) -> pd.DataFrame:
    """Return a canonical daily-price frame shared by every data source.

    Canonical units: 成交量=股, 成交额=元, 换手率=小数比例, 流通股本=股。
    东财接口（``stock_zh_a_hist`` / ``stock_hk_hist``）的成交量以「手」、换手率以百分数
    返回，调用方需显式声明 ``volume_in_lots`` / ``turnover_in_percent`` 完成换算；新浪
    接口已是目标口径，无需换算。所有来源统一裁剪为固定列集合，避免同一 ``price.csv``
    因回退切换而出现列结构或单位不一致。
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=list(CANONICAL_PRICE_COLUMNS))

    frame = df.rename(columns=_PRICE_COLUMN_ALIASES).copy()
    if "日期" not in frame.columns:
        return pd.DataFrame(columns=list(CANONICAL_PRICE_COLUMNS))

    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame = frame.dropna(subset=["日期"])
    if frame.empty:
        return pd.DataFrame(columns=list(CANONICAL_PRICE_COLUMNS))

    for column in CANONICAL_PRICE_COLUMNS[1:]:
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if volume_in_lots:
        frame["成交量"] = frame["成交量"] * 100
    if turnover_in_percent:
        frame["换手率"] = frame["换手率"] / 100

    frame = frame.sort_values("日期").reset_index(drop=True)
    return frame[list(CANONICAL_PRICE_COLUMNS)]


def _fetch_via_stock_zh_a_daily(ak_symbol: str, start_date: str, end_date: str, adjust: str, logger) -> pd.DataFrame:
    import time as _time
    max_attempts = 3
    base_delay = 2.0
    last_error = None
    for attempt in range(max_attempts):
        try:
            df = api_call_with_delay(
                ak.stock_zh_a_daily,
                symbol=ak_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                logger=logger,
            )
            if df is None or df.empty:
                raise ValueError("stock_zh_a_daily 返回空数据")
            return df
        except Exception as exc:
            last_error = exc
            if attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                if logger:
                    logger.info(
                        "stock_zh_a_daily 抓取失败(第 %d/%d 次)，%ss 后重试: %s",
                        attempt + 1,
                        max_attempts,
                        delay,
                        exc,
                    )
                _time.sleep(delay)
    raise last_error


def fetch_cn_a_daily_with_fallback(symbol_info: SymbolInfo, start_date: str, end_date: str, adjust: str = "qfq", logger: logging.Logger = None) -> pd.DataFrame:
    """优先使用 stock_zh_a_daily（新浪），带重试；最终回退到 stock_zh_a_hist。"""

    adjust = adjust or ""
    ak_symbol = symbol_info.to_akshare_equity()
    hist_symbol = symbol_info.code

    try:
        df = _fetch_via_stock_zh_a_daily(ak_symbol, start_date, end_date, adjust, logger)
        return _finalize_price_frame(df)
    except Exception as exc:
        logger.warning(
            "stock_zh_a_daily 获取 %s 失败(已重试)，改用 stock_zh_a_hist: %s",
            symbol_info.symbol,
            exc,
        )

    df_hist = api_call_with_delay(
        ak.stock_zh_a_hist,
        symbol=hist_symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        logger=logger
    )
    if df_hist is not None and not df_hist.empty:
        # 东财成交量单位为「手」、换手率为百分数，换算到新浪口径
        return _finalize_price_frame(
            df_hist, volume_in_lots=True, turnover_in_percent=True
        )
    raise ValueError("stock_zh_a_hist 返回空数据")


def fetch_cn_etf_daily(symbol_info: SymbolInfo, start_date: str, end_date: str, logger: logging.Logger = None) -> pd.DataFrame:
    """使用 fund_etf_hist_sina 获取A股ETF行情数据。"""
    
    try:
        ak_symbol = symbol_info.to_akshare_etf()
        df = api_call_with_delay(
            ak.fund_etf_hist_sina,
            symbol=ak_symbol,
            logger=logger,
        )
        
        if df is None or df.empty:
            raise ValueError(f"fund_etf_hist_sina 未返回 {symbol_info.symbol} 数据")

        return _finalize_price_frame(df)
    except Exception as exc:
        logger.warning(
            "fund_etf_hist_sina 获取 %s ETF数据失败: %s",
            symbol_info.symbol,
            exc,
        )
        raise


def fetch_cn_index_daily(symbol_info: SymbolInfo, logger: logging.Logger = None) -> pd.DataFrame:
    """使用 stock_zh_index_daily 获取A股指数行情数据。"""
    
    try:
        ak_symbol = symbol_info.to_akshare_index()
        df = api_call_with_delay(
            ak.stock_zh_index_daily,
            symbol=ak_symbol,
            logger=logger,
        )
        
        if df is None or df.empty:
            raise ValueError(f"stock_zh_index_daily 未返回 {symbol_info.symbol} 数据")

        return _finalize_price_frame(df)
    except Exception as exc:
        logger.warning(
            "stock_zh_index_daily 获取 %s 指数数据失败: %s",
            symbol_info.symbol,
            exc,
        )
        raise


def _resolve_logger(logger: logging.Logger | None) -> logging.Logger:
    return logger if logger is not None else logging.getLogger(__name__)


def _normalize_hk_daily_frame(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    frame = df.copy()
    frame = frame.rename(
        columns={
            "date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
            "amount": "成交额",
        }
    )

    if "日期" not in frame.columns:
        return pd.DataFrame()

    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame = frame.dropna(subset=["日期"])
    if frame.empty:
        return pd.DataFrame()

    start_dt = pd.to_datetime(start_date, errors="coerce")
    end_dt = pd.to_datetime(end_date, errors="coerce")
    if pd.notna(start_dt):
        frame = frame[frame["日期"] >= start_dt]
    if pd.notna(end_dt):
        frame = frame[frame["日期"] <= end_dt]
    if frame.empty:
        return pd.DataFrame()

    ordered_columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
        "流通股本",
    ]
    for col in ordered_columns[1:]:
        if col not in frame.columns:
            frame[col] = np.nan
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    frame = frame.sort_values("日期").reset_index(drop=True)
    extra_columns = [col for col in frame.columns if col not in ordered_columns]
    frame = frame[ordered_columns + extra_columns]
    return frame


def _fetch_hk_hist_from_eastmoney(
    symbol_info: SymbolInfo,
    start_date: str,
    end_date: str,
    adjust: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    last_error: Exception | None = None
    ak_symbol = symbol_info.to_hk_symbol()
    for attempt in range(1, HK_HIST_MAX_RETRIES + 1):
        try:
            df = api_call_with_delay(
                ak.stock_hk_hist,
                symbol=ak_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                logger=logger,
            )
            normalized = _normalize_hk_daily_frame(df, start_date, end_date)
            if normalized.empty:
                raise ValueError(f"stock_hk_hist 未返回 {symbol_info.symbol} 数据")
            return _finalize_price_frame(
                normalized, volume_in_lots=True, turnover_in_percent=True
            )
        except Exception as exc:
            last_error = exc
            retryable = isinstance(
                exc,
                (
                    requests_exceptions.RequestException,
                    ConnectionError,
                    TimeoutError,
                ),
            )
            if attempt >= HK_HIST_MAX_RETRIES or not retryable:
                break
            backoff_seconds = 1.5 * attempt
            logger.warning(
                "stock_hk_hist 获取 %s %s 失败，第 %d/%d 次重试前等待 %.1f 秒: %s",
                symbol_info.stock_name,
                symbol_info.symbol,
                attempt,
                HK_HIST_MAX_RETRIES,
                backoff_seconds,
                exc,
            )
            time.sleep(backoff_seconds)

    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def _fetch_hk_daily_from_sina(
    symbol_info: SymbolInfo,
    start_date: str,
    end_date: str,
    adjust: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    ak_symbol = symbol_info.to_hk_symbol()
    df = api_call_with_delay(
        ak.stock_hk_daily,
        symbol=ak_symbol,
        adjust=adjust,
        logger=logger,
    )
    normalized = _normalize_hk_daily_frame(df, start_date, end_date)
    if normalized.empty:
        raise ValueError(f"stock_hk_daily 未返回 {symbol_info.symbol} 数据")
    return _finalize_price_frame(normalized)


def fetch_hk_a_daily_with_fallback(symbol_info: SymbolInfo, start_date: str, end_date: str, adjust: str = "qfq", logger: logging.Logger = None) -> pd.DataFrame:
    """优先使用新浪港股日线接口，失败时回退到东财港股历史接口。"""
    adjust = adjust or ""
    resolved_logger = _resolve_logger(logger)
    try:
        df = _fetch_hk_daily_from_sina(
            symbol_info=symbol_info,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            logger=resolved_logger,
        )
        resolved_logger.info(
            "港股新浪日线获取成功: %s %s, 区间 %s-%s, %d 条",
            symbol_info.stock_name,
            symbol_info.symbol,
            start_date,
            end_date,
            len(df),
        )
        return df
    except Exception as exc:
        resolved_logger.warning(
            "stock_hk_daily 获取 %s %s 失败，改用 stock_hk_hist: %s",
            symbol_info.stock_name,
            symbol_info.symbol,
            exc,
        )
        try:
            df = _fetch_hk_hist_from_eastmoney(
                symbol_info=symbol_info,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                logger=resolved_logger,
            )
            resolved_logger.info(
                "港股东财历史回退成功: %s %s, 区间 %s-%s, %d 条",
                symbol_info.stock_name,
                symbol_info.symbol,
                start_date,
                end_date,
                len(df),
            )
            return df
        except Exception as fallback_exc:
            resolved_logger.error(
                "stock_hk_hist 获取 %s %s 也失败: %s",
                symbol_info.stock_name,
                symbol_info.symbol,
                fallback_exc,
            )
            raise ValueError(
                f"港股历史行情获取失败: {symbol_info.stock_name} {symbol_info.symbol}; "
                f"stock_hk_daily={exc}; stock_hk_hist={fallback_exc}"
            ) from fallback_exc
