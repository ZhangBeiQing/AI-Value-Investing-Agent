"""统一的日线行情抓取与规范化（A 股 / 港股 / ETF / 指数）。

所有来源在 :func:`_finalize_price_frame` 统一到 canonical 列与单位：
成交量=股，成交额=元，换手率=小数比例，流通股本=股。东财接口的「手 / 百分数」
由调用方通过 ``volume_in_lots`` / ``turnover_in_percent`` 声明后换算。
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime

import akshare as ak  # type: ignore
import numpy as np
import pandas as pd
import requests
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


TENCENT_HK_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/hkfqkline/get"
_TENCENT_HK_TIMEOUT_SECONDS = 20.0

# 交易日判定的小缓存，避免同一进程内反复查日历
_TRADING_DAY_CACHE: dict[tuple[str, str], bool] = {}


def _coerce_request_end_date(end_date: str) -> date | None:
    """把 ``YYYYMMDD`` / ``YYYY-MM-DD`` 解析成 date；失败返回 None。"""

    if not end_date:
        return None
    text = str(end_date).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _is_trading_day(target: date, *, market: str = "CN", logger: logging.Logger | None = None) -> bool | None:
    """判断是否为交易日；取不到日历时返回 None（调用方按"不强制"处理）。"""

    key = (target.isoformat(), market.upper())
    if key in _TRADING_DAY_CACHE:
        return _TRADING_DAY_CACHE[key]
    try:
        from shared_data_access.market_calendar import inspect_market_session

        session = inspect_market_session(target.isoformat(), market=market)
        result: bool | None = bool(session.is_trading_day)
    except Exception as exc:  # pragma: no cover - 日历不可用时保守放行
        if logger:
            logger.debug("交易日历不可用，跳过收盘覆盖校验: %s", exc)
        result = None
    if result is not None:
        _TRADING_DAY_CACHE[key] = result
    return result


def _frame_covers_trading_day(
    frame: pd.DataFrame,
    end_date: str,
    *,
    market: str = "CN",
    logger: logging.Logger | None = None,
) -> bool:
    """数据源"返回了数据"不等于"返回了目标交易日"。

    2026-11-02 真实事故：新浪港股接口返回的是到 10-30 为止的非空数据，原逻辑只判
    ``empty``，非空即成功，于是港股价格悄悄停在上一交易日。

    规则：请求区间终点是交易日时，帧的最后一根 K 线必须 ≥ 该日；周末/节假日不强制；
    日期列解析失败时保守放行，避免误伤正常数据。
    """

    if frame is None or frame.empty:
        return False
    end_dt = _coerce_request_end_date(end_date)
    if end_dt is None:
        return True
    is_trading = _is_trading_day(end_dt, market=market, logger=logger)
    if not is_trading:
        return True

    if "日期" not in frame.columns:
        return True
    last_dates = pd.to_datetime(frame["日期"], errors="coerce").dropna()
    if last_dates.empty:
        return True
    return bool(last_dates.max().date() >= end_dt)


def _frame_last_date(frame: pd.DataFrame) -> str:
    if frame is None or frame.empty or "日期" not in frame.columns:
        return "空"
    last_dates = pd.to_datetime(frame["日期"], errors="coerce").dropna()
    if last_dates.empty:
        return "空"
    return last_dates.max().date().isoformat()


def _fetch_tencent_daily_frame(
    symbol_info: SymbolInfo,
    start_date: str,
    end_date: str,
    adjust: str,
    logger: logging.Logger | None,
) -> pd.DataFrame:
    """回退链第三条腿：腾讯日线（A 股 / ETF 走 akshare，港股走腾讯 K 线接口）。

    2026-11-02 那天新浪报 SSL EOF、东财经代理不可达，两条主链同时失效，
    这条链是当时唯一还能拿到当日收盘的路径。
    """

    if symbol_info.is_hk_market():
        return _fetch_tencent_hk_daily(symbol_info, start_date, end_date, adjust, logger)
    return _fetch_tencent_cn_daily(symbol_info, start_date, end_date, adjust, logger)


def _fetch_tencent_cn_daily(
    symbol_info: SymbolInfo,
    start_date: str,
    end_date: str,
    adjust: str,
    logger: logging.Logger | None,
) -> pd.DataFrame:
    if _symbol_looks_like_etf(symbol_info):
        ak_symbol = symbol_info.to_akshare_etf()
    else:
        ak_symbol = symbol_info.to_akshare_equity()

    df = api_call_with_delay(
        ak.stock_zh_a_hist_tx,
        symbol=ak_symbol,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust or "",
        logger=logger,
    )
    if df is None or df.empty:
        raise ValueError("stock_zh_a_hist_tx 返回空数据")

    # 腾讯返回 date/open/close/high/low/amount，其中 amount 是成交量（手）
    frame = df.rename(columns={"amount": "成交量"}).copy()
    finalized = _finalize_price_frame(frame, volume_in_lots=True)
    if finalized.empty:
        raise ValueError("stock_zh_a_hist_tx 数据无法归一化")
    return finalized


def _symbol_looks_like_etf(symbol_info: SymbolInfo) -> bool:
    try:
        from commons import is_etf_symbol

        return bool(is_etf_symbol(symbol_info))
    except Exception:
        return False


def _fetch_tencent_hk_daily(
    symbol_info: SymbolInfo,
    start_date: str,
    end_date: str,
    adjust: str,
    logger: logging.Logger | None,
) -> pd.DataFrame:
    code = symbol_info.to_hk_symbol()
    ak_symbol = code if code.lower().startswith("hk") else f"hk{code}"
    start_text = _coerce_request_end_date(start_date)
    end_text = _coerce_request_end_date(end_date)
    if start_text is None or end_text is None:
        raise ValueError("腾讯港股日线需要合法的起止日期")

    response = requests.get(
        TENCENT_HK_KLINE_URL,
        params={
            "param": (
                f"{ak_symbol},day,{start_text.isoformat()},"
                f"{end_text.isoformat()},640,{adjust or 'qfq'}"
            )
        },
        timeout=_TENCENT_HK_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = json.loads(response.text or "{}")
    node = (payload.get("data") or {}).get(ak_symbol) or {}
    rows = node.get("qfqday") or node.get("day") or []
    if not rows:
        raise ValueError(f"腾讯港股日线未返回 {symbol_info.symbol} 数据")

    records = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        records.append(
            {
                "日期": row[0],
                "开盘": row[1],
                "收盘": row[2],
                "最高": row[3],
                "最低": row[4],
                "成交量": row[5],
            }
        )
    if not records:
        raise ValueError(f"腾讯港股日线返回结构无法解析: {symbol_info.symbol}")

    frame = pd.DataFrame(records)
    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame = frame.dropna(subset=["日期"])
    if frame.empty:
        raise ValueError(f"腾讯港股日线日期解析失败: {symbol_info.symbol}")
    frame = frame[frame["日期"] >= pd.Timestamp(start_text)]
    frame = frame[frame["日期"] <= pd.Timestamp(end_text)]
    if frame.empty:
        raise ValueError(f"腾讯港股日线在请求区间内无数据: {symbol_info.symbol}")
    return _finalize_price_frame(frame)


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
    """新浪 → 东财 → 腾讯 三条腿，且每条都必须覆盖到目标交易日。

    2026-11-02 事故：前两条同时失效时无路可走，因此补腾讯日线作末位回退。
    """

    logger = _resolve_logger(logger)
    adjust = adjust or ""
    ak_symbol = symbol_info.to_akshare_equity()
    hist_symbol = symbol_info.code

    errors: list[str] = []
    try:
        df = _fetch_via_stock_zh_a_daily(ak_symbol, start_date, end_date, adjust, logger)
        finalized = _finalize_price_frame(df)
        if _frame_covers_trading_day(finalized, end_date, market="CN", logger=logger):
            return finalized
        message = f"stock_zh_a_daily 只返回到 {_frame_last_date(finalized)}，未覆盖 {end_date}"
        errors.append(message)
        logger.warning("%s %s，继续回退", symbol_info.symbol, message)
    except Exception as exc:
        errors.append(f"stock_zh_a_daily={exc}")
        logger.warning(
            "stock_zh_a_daily 获取 %s 失败(已重试)，改用 stock_zh_a_hist: %s",
            symbol_info.symbol,
            exc,
        )

    try:
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
            finalized = _finalize_price_frame(
                df_hist, volume_in_lots=True, turnover_in_percent=True
            )
            if _frame_covers_trading_day(finalized, end_date, market="CN", logger=logger):
                return finalized
            message = f"stock_zh_a_hist 只返回到 {_frame_last_date(finalized)}，未覆盖 {end_date}"
            errors.append(message)
            logger.warning("%s %s，改用腾讯日线", symbol_info.symbol, message)
        else:
            errors.append("stock_zh_a_hist 返回空数据")
    except Exception as exc:
        errors.append(f"stock_zh_a_hist={exc}")
        logger.warning(
            "stock_zh_a_hist 获取 %s 失败，改用腾讯日线: %s",
            symbol_info.symbol,
            exc,
        )

    try:
        finalized = _fetch_tencent_daily_frame(
            symbol_info, start_date, end_date, adjust, logger
        )
        if _frame_covers_trading_day(finalized, end_date, market="CN", logger=logger):
            logger.info(
                "腾讯日线回退成功: %s %s, 至 %s",
                symbol_info.stock_name,
                symbol_info.symbol,
                _frame_last_date(finalized),
            )
            return finalized
        errors.append(f"腾讯日线只返回到 {_frame_last_date(finalized)}")
    except Exception as exc:
        errors.append(f"tencent={exc}")

    raise ValueError(
        f"A 股历史行情获取失败且未覆盖 {end_date}: {symbol_info.stock_name} {symbol_info.symbol}; "
        + "; ".join(errors)
    )


def fetch_cn_etf_daily(symbol_info: SymbolInfo, start_date: str, end_date: str, logger: logging.Logger = None) -> pd.DataFrame:
    """新浪 ETF 日线 → 腾讯日线回退，两者都必须覆盖到目标交易日。"""

    logger = _resolve_logger(logger)
    errors: list[str] = []
    try:
        ak_symbol = symbol_info.to_akshare_etf()
        df = api_call_with_delay(
            ak.fund_etf_hist_sina,
            symbol=ak_symbol,
            logger=logger,
        )

        if df is None or df.empty:
            raise ValueError(f"fund_etf_hist_sina 未返回 {symbol_info.symbol} 数据")

        finalized = _finalize_price_frame(df)
        if _frame_covers_trading_day(finalized, end_date, market="CN", logger=logger):
            return finalized
        message = f"fund_etf_hist_sina 只返回到 {_frame_last_date(finalized)}，未覆盖 {end_date}"
        errors.append(message)
        logger.warning("%s %s，改用腾讯日线", symbol_info.symbol, message)
    except Exception as exc:
        errors.append(f"fund_etf_hist_sina={exc}")
        logger.warning(
            "fund_etf_hist_sina 获取 %s ETF数据失败，改用腾讯日线: %s",
            symbol_info.symbol,
            exc,
        )

    try:
        finalized = _fetch_tencent_daily_frame(
            symbol_info, start_date, end_date, "qfq", logger
        )
        if _frame_covers_trading_day(finalized, end_date, market="CN", logger=logger):
            logger.info(
                "腾讯日线回退成功(ETF): %s %s, 至 %s",
                symbol_info.stock_name,
                symbol_info.symbol,
                _frame_last_date(finalized),
            )
            return finalized
        errors.append(f"腾讯日线只返回到 {_frame_last_date(finalized)}")
    except Exception as exc:
        errors.append(f"tencent={exc}")

    raise ValueError(
        f"ETF 历史行情获取失败且未覆盖 {end_date}: {symbol_info.stock_name} {symbol_info.symbol}; "
        + "; ".join(errors)
    )


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
            finalized = _finalize_price_frame(
                normalized, volume_in_lots=True, turnover_in_percent=True
            )
            if not _frame_covers_trading_day(finalized, end_date, market="HK", logger=logger):
                raise ValueError(
                    f"stock_hk_hist 只返回到 {_frame_last_date(finalized)}，未覆盖 {end_date}"
                )
            return finalized
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
    finalized = _finalize_price_frame(normalized)
    if not _frame_covers_trading_day(finalized, end_date, market="HK", logger=logger):
        raise ValueError(
            f"stock_hk_daily 只返回到 {_frame_last_date(finalized)}，未覆盖 {end_date}"
        )
    return finalized


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
                "stock_hk_hist 获取 %s %s 也失败: %s，改用腾讯港股日线",
                symbol_info.stock_name,
                symbol_info.symbol,
                fallback_exc,
            )
            try:
                df = _fetch_tencent_hk_daily(
                    symbol_info, start_date, end_date, adjust, resolved_logger
                )
                if not _frame_covers_trading_day(
                    df, end_date, market="HK", logger=resolved_logger
                ):
                    raise ValueError(
                        f"腾讯港股日线只返回到 {_frame_last_date(df)}，未覆盖 {end_date}"
                    )
                resolved_logger.info(
                    "腾讯港股日线回退成功: %s %s, 至 %s",
                    symbol_info.stock_name,
                    symbol_info.symbol,
                    _frame_last_date(df),
                )
                return df
            except Exception as tencent_exc:
                resolved_logger.error(
                    "腾讯港股日线获取 %s %s 也失败: %s",
                    symbol_info.stock_name,
                    symbol_info.symbol,
                    tencent_exc,
                )
                raise ValueError(
                    f"港股历史行情获取失败: {symbol_info.stock_name} {symbol_info.symbol}; "
                    f"stock_hk_daily={exc}; stock_hk_hist={fallback_exc}; tencent={tencent_exc}"
                ) from tencent_exc
