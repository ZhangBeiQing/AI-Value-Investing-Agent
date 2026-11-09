"""US market primitives: quote snapshot, share count, and daily prices.

美股没有 A 股那样的「交易所前缀」符号，东财行情需要 ``市场前缀.TICKER``
（105=NASDAQ / 106=NYSE / 107=AMEX）。本模块负责：

1. 探测并缓存 ticker 对应的东财 secid；
2. 抓取单标的行情快照（最新价、总市值、PE(TTM)、PB、换手率），
   作为美股股本与「市场口径估值」的权威来源；
3. 提供日线行情（东财优先、新浪兜底）。

所有网络访问都收敛在这里，上层只消费规范化后的字典 / DataFrame。
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

import akshare as ak  # type: ignore
import pandas as pd
import requests

from commons import SymbolInfo, api_call_with_delay, resolve_base_dir

US_QUOTE_TTL_SECONDS = 6 * 3600
US_MARKET_PREFIXES = ("105", "106", "107")
US_QUOTE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
US_QUOTE_FIELDS = (
    "f43,f44,f45,f46,f47,f48,f57,f58,f59,f60,"
    "f116,f117,f162,f164,f167,f168,f170,f171"
)
US_QUOTE_UT = "fa5fd1943c7b386f172d6893dbfba10b"
US_QUOTE_TIMEOUT_SECONDS = 15.0
US_CACHE_DIRNAME = "us_quotes"


def _resolve_logger(logger: logging.Logger | None) -> logging.Logger:
    return logger if logger is not None else logging.getLogger(__name__)


def us_quote_cache_dir(base_dir: str | Path | None = None) -> Path:
    target = resolve_base_dir(base_dir) / "global_cache" / US_CACHE_DIRNAME
    target.mkdir(parents=True, exist_ok=True)
    return target


def _quote_cache_path(ticker: str, base_dir: str | Path | None = None) -> Path:
    return us_quote_cache_dir(base_dir) / f"{ticker.upper()}.json"


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_scaled(value: Any, digits: int) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric == 0:
        return None
    return numeric / (10 ** max(int(digits or 0), 0))


def _parse_ratio(value: Any) -> Optional[float]:
    """东财的 PE/PB/换手率/涨跌幅按 1/100 存储。"""

    if value in (None, "", "-"):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric == 0:
        return None
    return numeric / 100.0


def _decode_quote(data: Dict[str, Any], ticker: str, secid: str) -> Dict[str, Any]:
    digits = int(data.get("f59") or 0)
    price = _parse_scaled(data.get("f43"), digits)
    prev_close = _parse_scaled(data.get("f60"), digits)
    market_cap = None
    raw_cap = data.get("f116")
    if raw_cap not in (None, "", "-"):
        try:
            market_cap = float(raw_cap) or None
        except (TypeError, ValueError):
            market_cap = None

    total_shares = None
    if price and market_cap:
        total_shares = market_cap / price

    return {
        "ticker": ticker.upper(),
        "secid": secid,
        "name": str(data.get("f58") or "").strip(),
        "price": price,
        "prev_close": prev_close,
        "open": _parse_scaled(data.get("f46"), digits),
        "high": _parse_scaled(data.get("f44"), digits),
        "low": _parse_scaled(data.get("f45"), digits),
        "volume": _parse_scaled(data.get("f47"), 0),
        "amount": _parse_scaled(data.get("f48"), 0),
        "change_pct": _parse_ratio(data.get("f170")),
        "turnover_rate": _parse_ratio(data.get("f168")),
        "pe_ttm": _parse_ratio(data.get("f164")) or _parse_ratio(data.get("f162")),
        "pb": _parse_ratio(data.get("f167")),
        "market_cap": market_cap,
        "total_shares": total_shares,
        "currency": "USD",
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }


def _request_quote(secid: str, timeout: float = US_QUOTE_TIMEOUT_SECONDS) -> Optional[Dict[str, Any]]:
    response = requests.get(
        US_QUOTE_URL,
        params={"secid": secid, "fields": US_QUOTE_FIELDS, "ut": US_QUOTE_UT},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, dict) and data else None


def resolve_us_secid(
    ticker: str,
    *,
    base_dir: str | Path | None = None,
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> Optional[str]:
    """Return eastmoney ``prefix.TICKER`` for a US symbol, cached globally."""

    resolved_logger = _resolve_logger(logger)
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return None

    cache_path = _quote_cache_path(ticker, base_dir)
    if not force_refresh:
        cached = _read_json(cache_path)
        if cached and cached.get("secid"):
            return str(cached["secid"])

    for prefix in US_MARKET_PREFIXES:
        secid = f"{prefix}.{ticker}"
        try:
            data = _request_quote(secid)
        except Exception as exc:  # pragma: no cover - network defensive
            resolved_logger.debug("探测美股 secid 失败 %s: %s", secid, exc)
            continue
        if not data:
            continue
        if str(data.get("f57") or "").upper() != ticker:
            continue
        resolved_logger.info("美股 secid 命中: %s -> %s", ticker, secid)
        return secid
    resolved_logger.warning("未能为 %s 解析东财 secid（可能为 OTC/粉单）", ticker)
    return None


def fetch_us_quote(
    ticker: str,
    *,
    base_dir: str | Path | None = None,
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> Optional[Dict[str, Any]]:
    """Fetch (or reuse) a US single-symbol quote snapshot."""

    resolved_logger = _resolve_logger(logger)
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return None

    cache_path = _quote_cache_path(ticker, base_dir)
    cached = _read_json(cache_path)
    if cached and not force_refresh:
        fetched_at = _parse_timestamp(cached.get("fetched_at"))
        if fetched_at is not None and (
            datetime.now() - fetched_at
        ).total_seconds() < US_QUOTE_TTL_SECONDS:
            return cached

    secid = resolve_us_secid(
        ticker,
        base_dir=base_dir,
        force_refresh=force_refresh,
        logger=resolved_logger,
    )
    if not secid:
        return cached

    try:
        data = _request_quote(secid)
    except Exception as exc:
        resolved_logger.warning("获取美股行情快照失败 %s: %s", ticker, exc)
        return cached
    if not data:
        resolved_logger.warning("美股行情快照为空: %s", ticker)
        return cached

    quote = _decode_quote(data, ticker, secid)
    if quote.get("name"):
        _write_json(cache_path, quote)
    return quote


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def get_us_quote(
    symbol_info: SymbolInfo,
    *,
    base_dir: str | Path | None = None,
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> Optional[Dict[str, Any]]:
    return fetch_us_quote(
        symbol_info.code,
        base_dir=base_dir,
        force_refresh=force_refresh,
        logger=logger,
    )


def get_us_stock_name(
    ticker: str,
    *,
    base_dir: str | Path | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Resolve a US ticker to its Chinese short name via eastmoney quote."""

    resolved_logger = _resolve_logger(logger)
    quote = fetch_us_quote(ticker, base_dir=base_dir, logger=resolved_logger)
    if quote and quote.get("name"):
        return str(quote["name"]).strip()
    return ""


def derive_total_shares(quote: Optional[Dict[str, Any]]) -> Optional[float]:
    if not quote:
        return None
    total = quote.get("total_shares")
    if total:
        return float(total)
    price = quote.get("price")
    market_cap = quote.get("market_cap")
    if price and market_cap:
        return float(market_cap) / float(price)
    return None


def _normalize_sina_us_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    work = frame.rename(
        columns={
            "date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
        }
    ).copy()
    if "日期" not in work.columns:
        return pd.DataFrame()
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce")
    work = work.dropna(subset=["日期"]).sort_values("日期")
    return work.reset_index(drop=True)


def _slice_by_date(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    start_dt = pd.to_datetime(str(start_date), errors="coerce")
    end_dt = pd.to_datetime(str(end_date), errors="coerce")
    work = frame.copy()
    if pd.notna(start_dt):
        work = work[work["日期"] >= start_dt]
    if pd.notna(end_dt):
        work = work[work["日期"] <= end_dt]
    return work.reset_index(drop=True)


def fetch_us_daily_from_eastmoney(
    symbol_info: SymbolInfo,
    start_date: str,
    end_date: str,
    *,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from shared_data_access.price_fetch import _finalize_price_frame

    secid = resolve_us_secid(
        symbol_info.code,
        base_dir=None,
        logger=logger,
    )
    if not secid:
        raise ValueError(f"未解析到 {symbol_info.symbol} 的东财 secid")

    df = api_call_with_delay(
        ak.stock_us_hist,
        symbol=secid,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",
        logger=logger,
    )
    if df is None or df.empty:
        raise ValueError(f"stock_us_hist 未返回 {symbol_info.symbol} 数据")
    finalized = _finalize_price_frame(df, turnover_in_percent=True)
    if finalized.empty:
        raise ValueError(f"stock_us_hist 数据无法归一化: {symbol_info.symbol}")
    return finalized


def fetch_us_daily_from_sina(
    symbol_info: SymbolInfo,
    start_date: str,
    end_date: str,
    *,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    from shared_data_access.price_fetch import _finalize_price_frame

    df = api_call_with_delay(
        ak.stock_us_daily,
        symbol=symbol_info.code,
        adjust="qfq",
        logger=logger,
    )
    normalized = _normalize_sina_us_daily(df)
    sliced = _slice_by_date(normalized, start_date, end_date)
    if sliced.empty:
        raise ValueError(f"stock_us_daily 未返回 {symbol_info.symbol} 区间数据")
    finalized = _finalize_price_frame(sliced)
    if finalized.empty:
        raise ValueError(f"stock_us_daily 数据无法归一化: {symbol_info.symbol}")
    return finalized


def latest_completed_us_session(now: Optional[datetime] = None) -> date:
    """Return the latest US session whose close is already available.

    美股收盘 16:00 ET，对应北京时间次日凌晨。北京时间晚上运行研究时，
    当天美东还没开盘/未收盘，因此「已完成收盘」的最近交易日通常要往前推一天。
    """

    from zoneinfo import ZoneInfo

    from shared_data_access.market_calendar import inspect_market_session

    moment = now or datetime.now(ZoneInfo("Asia/Shanghai"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    et = moment.astimezone(ZoneInfo("America/New_York"))
    target = et.date()
    session = inspect_market_session(target.isoformat(), market="US")
    if session.is_trading_day and et.hour < 16:
        target = date.fromisoformat(str(session.previous_trading_day))
    elif not session.is_trading_day:
        target = date.fromisoformat(str(session.previous_trading_day))
    return target


def us_session_close_utc(session_date: str | date) -> datetime:
    """Convert a US session date to its 16:00 ET close in UTC (naive UTC)."""

    from zoneinfo import ZoneInfo

    if isinstance(session_date, str):
        session_date = date.fromisoformat(session_date)
    close_et = datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        16,
        0,
        tzinfo=ZoneInfo("America/New_York"),
    )
    return close_et.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


__all__ = [
    "US_QUOTE_TTL_SECONDS",
    "derive_total_shares",
    "fetch_us_daily_from_eastmoney",
    "fetch_us_daily_from_sina",
    "fetch_us_quote",
    "get_us_quote",
    "get_us_stock_name",
    "latest_completed_us_session",
    "resolve_us_secid",
    "us_quote_cache_dir",
    "us_session_close_utc",
]
