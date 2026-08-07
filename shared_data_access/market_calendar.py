"""Deterministic market-session lookup for daily pipelines and backtests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas_market_calendars as mcal

from shared_data_access.historical_prices import load_price_history
from utlity import parse_symbol


MARKET_CALENDAR_NAMES = {
    "CN": "SSE",
    "HK": "XHKG",
    "US": "NYSE",
}
MARKET_REFERENCE_SYMBOLS = {
    "CN": "000001.IDX",
}


@dataclass(frozen=True)
class MarketSessionRange:
    market: str
    start_date: str
    end_date: str
    dates: tuple[str, ...]
    source: str


@dataclass(frozen=True)
class MarketSessionCheck:
    market: str
    target_date: str
    is_trading_day: bool
    previous_trading_day: str | None
    next_trading_day: str | None
    source: str


class NonTradingDayError(ValueError):
    def __init__(self, check: MarketSessionCheck) -> None:
        self.check = check
        super().__init__(
            f"{check.target_date} 不是 {check.market} 交易日；"
            f"previous={check.previous_trading_day}, "
            f"next={check.next_trading_day}, source={check.source}"
        )


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def market_sessions_between(
    start_date: str | date,
    end_date: str | date,
    *,
    market: str = "CN",
    base_dir: str | Path = "data",
) -> MarketSessionRange:
    """Return real sessions, preferring a fully covering reference price cache."""

    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start > end:
        raise ValueError("start_date 不能晚于 end_date")
    normalized_market = market.upper()
    reference_symbol = MARKET_REFERENCE_SYMBOLS.get(normalized_market)
    if reference_symbol:
        frame = load_price_history(
            parse_symbol(reference_symbol),
            base_dir=base_dir,
        )
        if not frame.empty:
            cache_start = frame.iloc[0]["日期"].date()
            cache_end = frame.iloc[-1]["日期"].date()
            if cache_start <= start and end <= cache_end:
                dates = tuple(
                    frame.loc[
                        (frame["日期"].dt.date >= start)
                        & (frame["日期"].dt.date <= end),
                        "日期",
                    ]
                    .dt.strftime("%Y-%m-%d")
                    .drop_duplicates()
                    .tolist()
                )
                return MarketSessionRange(
                    market=normalized_market,
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                    dates=dates,
                    source=f"reference_price_cache:{reference_symbol}",
                )

    calendar_name = MARKET_CALENDAR_NAMES.get(normalized_market)
    if not calendar_name:
        raise ValueError(f"不支持的交易日历市场: {normalized_market}")
    try:
        calendar = mcal.get_calendar(calendar_name)
        schedule = calendar.schedule(start_date=start, end_date=end)
    except Exception as exc:
        raise RuntimeError(
            f"无法加载 {normalized_market} 交易日历，拒绝退化为工作日判断"
        ) from exc
    dates = tuple(schedule.index.strftime("%Y-%m-%d").tolist())
    return MarketSessionRange(
        market=normalized_market,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        dates=dates,
        source=f"pandas_market_calendars:{calendar_name}",
    )


def inspect_market_session(
    target_date: str | date,
    *,
    market: str = "CN",
    base_dir: str | Path = "data",
) -> MarketSessionCheck:
    target = _parse_date(target_date)
    window = market_sessions_between(
        target - timedelta(days=16),
        target + timedelta(days=16),
        market=market,
        base_dir=base_dir,
    )
    target_text = target.isoformat()
    earlier = [item for item in window.dates if item < target_text]
    later = [item for item in window.dates if item > target_text]
    return MarketSessionCheck(
        market=window.market,
        target_date=target_text,
        is_trading_day=target_text in window.dates,
        previous_trading_day=earlier[-1] if earlier else None,
        next_trading_day=later[0] if later else None,
        source=window.source,
    )


def ensure_market_session(
    target_date: str | date,
    *,
    market: str = "CN",
    base_dir: str | Path = "data",
) -> MarketSessionCheck:
    check = inspect_market_session(
        target_date,
        market=market,
        base_dir=base_dir,
    )
    if not check.is_trading_day:
        raise NonTradingDayError(check)
    return check


__all__ = [
    "MarketSessionCheck",
    "MarketSessionRange",
    "NonTradingDayError",
    "ensure_market_session",
    "inspect_market_session",
    "market_sessions_between",
]
