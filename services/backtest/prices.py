"""Historical quotes available at the decision day's Beijing night cutoff."""

from datetime import datetime
from zoneinfo import ZoneInfo

from commons import parse_symbol
from shared_data_access.historical_prices import close_on_or_before
from shared_data_access.us_market import latest_completed_us_session
from shared_data_access.market_calendar import inspect_market_session


def quote_date_as_of(symbol: str, run_date: str) -> str:
    market = parse_symbol(symbol).market
    if market == "HK":
        session = inspect_market_session(run_date, market="HK")
        if not session.is_trading_day:
            if not session.previous_trading_day:
                raise ValueError(f"无法确认 {run_date} 之前的港股交易日")
            return session.previous_trading_day
    if market != "US":
        return run_date
    cutoff = datetime.fromisoformat(f"{run_date}T23:59:59").replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return latest_completed_us_session(cutoff).isoformat()


def historical_close(symbol: str, run_date: str, source_data_root):
    return close_on_or_before(
        parse_symbol(symbol), quote_date_as_of(symbol, run_date), base_dir=source_data_root,
    )
