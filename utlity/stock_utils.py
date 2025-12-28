"""
Common helpers for stock symbol normalization, trading-day evaluation,
and filesystem layout used by the stock analysis toolkit.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import shutil
from typing import Any, Callable, Dict, Optional, Set, Tuple, TypeVar
import pandas as pd
import pandas_market_calendars as mcal  # type: ignore
import akshare as ak  # type: ignore
from configs.stock_pool import TRACKED_A_STOCKS, StockEntry
import numpy as np

# 计算仓库根路径：stock_utils.py 位于 <repo>/utlity/，因而上移 1 级即可
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = REPO_ROOT / "data"

SYMBOL_SUFFIX_INFO: Dict[str, Dict[str, str]] = {
    "SH": {"market": "CN_A", "calendar": "CN"},
    "SZ": {"market": "CN_A", "calendar": "CN"},
    "HK": {"market": "HK", "calendar": "HK"},
    "US": {"market": "US", "calendar": "US"},
    "IDX": {"market": "CN_INDEX", "calendar": "CN"},
}

DEFAULT_API_CALL_DELAY = 0.5
T = TypeVar("T")
ETF_CODE_PREFIXES = ("51", "58", "15", "16", "50", "53")


def _sanitize_stock_name_value(value: Any) -> str:
    """Trim空白并确保始终返回字符串，避免名称前后残留空格。"""

    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def api_call_with_delay(
    api_func: Callable[..., T],
    *args: Any,
    logger: Optional[logging.Logger] = None,
    delay: Optional[float] = None,
    **kwargs: Any,
) -> T:
    """统一的 AkShare 调用包装器，提供简单的节流与日志支持。"""

    wait_seconds = DEFAULT_API_CALL_DELAY if delay is None else max(delay, 0)
    func_name = getattr(api_func, "__name__", str(api_func))

    if logger:
        logger.info("开始调用API: %s", func_name)

    try:
        result = api_func(*args, **kwargs)
        if logger:
            logger.info("API调用成功: %s", func_name)
        return result
    except Exception as exc:
        if logger:
            logger.error("API调用失败: %s, 错误: %s", func_name, exc)
        raise
    finally:
        if wait_seconds > 0:
            if logger:
                logger.debug("API调用后等待 %.2f 秒...", wait_seconds)
            time.sleep(wait_seconds)

CALENDAR_DIR = DEFAULT_DATA_DIR / "calendars"
CALENDAR_DIR.mkdir(parents=True, exist_ok=True)

_TRADING_DAY_CACHE: Dict[str, Set[str]] = {}

MARKET_CALENDAR_NAMES: Dict[str, Tuple[str, ...]] = {
    "CN": ("SSE", "XSHG", "SZSE"),
    "HK": ("HKEX",),
    "US": ("NYSE", "XNYS"),
}

_SYMBOL_METADATA_MAP: Dict[str, StockEntry] = {
    entry.symbol.upper(): entry for entry in TRACKED_A_STOCKS
}


class SymbolFormatError(ValueError):
    """Raised when a symbol does not satisfy the required format."""


@dataclass(frozen=True)
class SymbolInfo:
    """Normalized stock symbol metadata."""

    symbol: str
    code: str
    suffix: str
    market: str
    calendar: str
    stock_name: str
    description: str

    def ensure_market(self, allowed: Tuple[str, ...]) -> None:
        if self.market not in allowed:
            raise SymbolFormatError(
                f"Symbol {self.symbol} is not supported in the requested context; "
                f"allowed markets: {allowed}, got {self.market}"
            )

    def to_akshare_equity(self) -> str:
        """Return AkShare equity symbol with exchange prefix."""
        self.ensure_market(("CN_A",))
        prefix = "sh" if self.suffix == "SH" else "sz"
        return f"{prefix}{self.code}"

    def to_akshare_etf(self) -> str:
        """Return AkShare ETF symbol (same as equity format)."""
        return self.to_akshare_equity()

    def to_akshare_index(self) -> str:
        """Return AkShare index symbol with inferred exchange prefix."""
        self.ensure_market(("CN_INDEX",))
        prefix = _infer_index_prefix(self.code)
        return f"{prefix}{self.code}"

    def to_hk_symbol(self) -> str:
        """Return zero-padded HK symbol for AkShare."""
        self.ensure_market(("HK",))
        return self.code.zfill(5)

    def to_us_symbol(self) -> str:
        """Return US ticker symbol for AkShare."""
        self.ensure_market(("US",))
        return self.code

    def to_xueqiu_symbol(self) -> str:
        """Return Xueqiu formatted symbol (e.g., SH600000)."""
        if self.market == "CN_A":
            prefix = "SH" if self.suffix == "SH" else "SZ"
            return f"{prefix}{self.code}"
        if self.market == "HK":
            return f"HK{self.code.zfill(5)}"
        if self.market == "US":
            return self.code
        raise SymbolFormatError(f"Xueqiu format not supported for market {self.market}")
    
    def is_cn_market(self) -> bool:
        return self.market == "CN_A"
    
    def is_hk_market(self) -> bool:
        return self.market == "HK"
    
    def is_cn_index_market(self) -> bool:
        return self.market == "CN_INDEX"



def normalize_symbol(symbol: str) -> str:
    """Normalize user supplied symbol to CODE.SUFFIX format."""
    if not symbol or not isinstance(symbol, str):
        raise SymbolFormatError("Symbol must be a non-empty string like 600000.SH")

    cleaned = symbol.strip().upper()
    if "." not in cleaned:
        raise SymbolFormatError(
            f"Symbol {symbol} is invalid. Expected format like 601877.SH or 09988.HK"
        )

    code, suffix = cleaned.split(".", 1)
    if not code or not suffix:
        raise SymbolFormatError(f"Symbol {symbol} is invalid. Missing code or suffix.")

    if suffix not in SYMBOL_SUFFIX_INFO:
        raise SymbolFormatError(
            f"Unsupported suffix {suffix} in symbol {symbol}. "
            f"Allowed suffixes: {', '.join(sorted(SYMBOL_SUFFIX_INFO.keys()))}"
        )

    if not re.fullmatch(r"[A-Z0-9]+", code):
        raise SymbolFormatError(
            f"Symbol code {code} contains unsupported characters; "
            "use alphanumerics only."
        )

    # 港股代码统一补零到 5 位，方便后续 akshare/东财接口复用
    if suffix == "HK" and code.isdigit():
        code = code.zfill(5)

    return f"{code}.{suffix}"


def parse_symbol(symbol: str) -> SymbolInfo:
    """Parse the normalized symbol into structured metadata."""
    normalized = normalize_symbol(symbol)
    stock_name = get_stock_name(normalized)
    code, suffix = normalized.split(".", 1)
    metadata = SYMBOL_SUFFIX_INFO[suffix]
    stock_entry = _SYMBOL_METADATA_MAP.get(normalized)
    resolved_name = _sanitize_stock_name_value(stock_entry.name if stock_entry else stock_name)
    if not resolved_name:
        resolved_name = normalized
    return SymbolInfo(
        symbol=normalized,
        code=code,
        suffix=suffix,
        market=metadata["market"],
        calendar=metadata["calendar"],
        stock_name=resolved_name,
        description=stock_entry.description if stock_entry else "",
    )

def is_hk_stock(stock_code: str) -> bool:
    """判断是否为港股代码 (CODE.SUFFIX 格式)"""
    normalized_code = normalize_symbol(stock_code)
    _, suffix = normalized_code.split(".", 1)
    if(suffix == "HK"):
        return True
    else:
        return False   


def is_cn_etf_symbol(symbol: str) -> bool:
    """判断裸字符串是否为常见前缀的 A 股 ETF/基金标的。"""
    try:
        normalized = normalize_symbol(symbol)
    except SymbolFormatError:
        return False
    code, suffix = normalized.split(".", 1)
    if suffix not in ("SH", "SZ"):
        return False
    return code.startswith(ETF_CODE_PREFIXES)


def is_cn_etf(symbolInfo: SymbolInfo) -> bool:
    """判断是否为常见前缀的 A 股 ETF/基金标的。"""
    return is_cn_etf_symbol(symbolInfo.symbol)

def resolve_base_dir(base_dir: Path | str | None = None) -> Path:
    """Resolve a base directory relative to the repository root."""

    if base_dir is None:
        return DEFAULT_DATA_DIR

    candidate = Path(base_dir)
    if not candidate.is_absolute():
        return (REPO_ROOT / candidate).resolve()
    return candidate


def sanitize_stock_name(stock_name: str) -> str:
    """Sanitize stock name for filesystem usage, supporting Chinese characters."""
    if not stock_name or not isinstance(stock_name, str):
        raise ValueError("stock_name must be a non-empty string")
    
    # Replace whitespace with underscores
    sanitized = re.sub(r"\s+", "_", stock_name.strip())
    
    # Remove only problematic filesystem characters, keep Chinese characters
    # Remove: / \ : * ? " < > | and other control characters
    sanitized = re.sub(r'[/\\:*?"<>|\x00-\x1f\x7f]', "", sanitized)
    
    # Remove leading/trailing dots and spaces that might cause issues
    sanitized = sanitized.strip('. ')
    
    return sanitized or "UNKNOWN"


def get_stock_data_dir(
    symbolInfo: SymbolInfo,
    base_dir: Path | str | None = None,
) -> Path:
    """返回股票缓存的根目录，统一迁移到 data/stock_info 下。"""
    base = resolve_base_dir(base_dir)
    # 新目录位于 data/stock_info/<name_symbol>
    new_root = base / "stock_info" / f"{symbolInfo.stock_name}_{symbolInfo.symbol}"
    return new_root


def ensure_stock_subdir(
    symbolInfo: SymbolInfo,
    subdir: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Ensure a sub-directory under the stock root exists and return its path."""
    root = get_stock_data_dir(symbolInfo, base_dir=base_dir)
    target = root / subdir
    target.mkdir(parents=True, exist_ok=True)
    return target


def is_trading_day(
    check_date: date, 
    calendar_market: str, 
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Return True if the date is a trading day for the specified market.
    
    Uses pandas_market_calendars for accurate trading day determination.
    Falls back to weekday heuristic if pandas_market_calendars is unavailable.
    
    Args:
        check_date: The date to check
        calendar_market: Market identifier (e.g., 'US', 'CN', 'HK')
        logger: Optional logger for error reporting
        
    Returns:
        bool: True if the date is a trading day, False otherwise
    """
    try:
        # Map calendar_market to pandas_market_calendars calendar names
        calendar_mapping = {
            'US': 'NYSE',
            'CN': 'SSE', 
            'HK': 'XHKG'
        }
        
        calendar_name = calendar_mapping.get(calendar_market)
        if not calendar_name:
            if logger:
                logger.warning(f"Unknown calendar market: {calendar_market}, falling back to weekday check")
            return check_date.weekday() < 5
        
        # Get the market calendar
        calendar = mcal.get_calendar(calendar_name)
        
        # Convert date to datetime for pandas_market_calendars
        check_datetime = datetime.combine(check_date, datetime.min.time())
        
        # Use valid_days to check if the date is a trading day
        trading_days = calendar.valid_days(check_datetime, check_datetime)
        
        return len(trading_days) > 0
        
    except Exception as e:
        if logger:
            logger.warning(f"Error checking trading day with pandas_market_calendars: {e}, falling back to weekday check")
        # Fallback to weekday heuristic when pandas_market_calendars unavailable
        return check_date.weekday() < 5


def get_latest_trading_day(
    reference_date: date,
    calendar_market: str,
    logger: Optional[logging.Logger] = None,
) -> date:
    """Return the most recent trading day on or before reference_date."""
    current = reference_date
    for _ in range(366):  # Cap to avoid infinite loops
        if is_trading_day(current, calendar_market, logger=logger):
            return current
        current -= timedelta(days=1)
    raise RuntimeError(
        f"Could not determine trading day within one year prior to {reference_date}"
    )

def get_last_trading_day(
    reference_date: date,
    calendar_market: str = 'CN',
    logger: Optional[logging.Logger] = None,
) -> date:
    """Return the most recent trading day on or before reference_date."""
    current = reference_date - timedelta(days=1)
    for _ in range(366):  # Cap to avoid infinite loops
        if is_trading_day(current, calendar_market, logger=logger):
            return current
        current -= timedelta(days=1)
    raise RuntimeError(
        f"Could not determine trading day within one year prior to {reference_date}"
    )


def get_next_trading_day(
    reference_date: date,
    calendar_market: str,
    logger: Optional[logging.Logger] = None,
) -> date:
    """Return the next trading day strictly after reference_date."""
    current = reference_date + timedelta(days=1)
    for _ in range(366):
        if is_trading_day(current, calendar_market, logger=logger):
            return current
        current += timedelta(days=1)
    raise RuntimeError(
        f"Could not determine next trading day within one year after {reference_date}"
    )


def is_cache_expired(file_path: Path, ttl_days: int) -> bool:
    """Check whether a cache file is older than ttl_days."""
    if not file_path.exists():
        return True
    modified = datetime.fromtimestamp(file_path.stat().st_mtime).date()
    return (date.today() - modified).days >= ttl_days


def get_trading_calendar(
    calendar_market: str,
    *,
    start_year: int = 2020,  # 新增参数：起始年份
    force_refresh: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Set[str]:
    """Fetch trading calendar dates for the specified market.
    
    Args:
        calendar_market: Market code (e.g., 'CN', 'US', 'HK')
        start_year: Starting year for calendar data (default: 2020)
        force_refresh: Force refresh from remote sources
        logger: Optional logger for debugging
    
    Returns:
        Set of trading dates in 'YYYY-MM-DD' format
    """
    normalized_market = calendar_market.upper()
    
    # 缓存键包含起始年份，避免不同年份范围的数据冲突
    cache_key = f"{normalized_market}_{start_year}"
    
    if not force_refresh and cache_key in _TRADING_DAY_CACHE:
        return _TRADING_DAY_CACHE[cache_key]

    calendar_file = CALENDAR_DIR / f"{normalized_market.lower()}_trading_days_{start_year}.csv"

    if not force_refresh and calendar_file.exists():
        try:
            df = pd.read_csv(calendar_file)
            dates = _extract_dates(df)
            _TRADING_DAY_CACHE[cache_key] = dates
            return dates
        except Exception as exc:  # pragma: no cover - defensive
            if logger:
                logger.warning(
                    "Failed to load cached trading calendar %s: %s",
                    calendar_file,
                    exc,
                )

    dates: Set[str] = set()
    df = None
    try:
        df = _download_calendar_from_market_calendars(
            normalized_market, 
            start_year=start_year,
            logger=logger
        )
    except Exception as exc:  # pragma: no cover - defensive
        if logger:
            logger.warning(
                "Failed to download trading calendar for %s via pandas_market_calendars: %s",
                normalized_market,
                exc,
            )

    if df is None and ak is not None:
        try:
            df = _download_calendar_from_akshare(normalized_market, logger=logger)
        except Exception as exc:  # pragma: no cover - defensive
            if logger:
                logger.warning(
                    "Failed to download trading calendar for %s via akshare: %s",
                    normalized_market,
                    exc,
                )
    elif df is None and logger:
        logger.warning(
            "No trading calendar source available for %s; falling back to weekday heuristic.",
            normalized_market,
        )

    if df is not None:
        dates = _extract_dates(df)
        if dates:
            df_to_save = pd.DataFrame(sorted(dates), columns=["trade_date"])
            calendar_file.parent.mkdir(parents=True, exist_ok=True)
            df_to_save.to_csv(calendar_file, index=False)

    _TRADING_DAY_CACHE[cache_key] = dates
    return dates


def _download_calendar_from_market_calendars(
    market: str,
    *,
    start_year: int = 2020,  # 默认从2020年开始，可配置
    logger: Optional[logging.Logger] = None,
) -> Optional[pd.DataFrame]:
    """Attempt to download trading calendars via pandas_market_calendars.
    
    Args:
        market: Market code (e.g., 'CN', 'US', 'HK')
        start_year: Starting year for calendar data (default: 2020)
        logger: Optional logger for debugging
    
    Returns:
        DataFrame with trading dates or None if failed
    """

    if mcal is None:
        return None

    calendar_names = MARKET_CALENDAR_NAMES.get(market, ())
    for calendar_name in calendar_names:
        try:
            calendar = mcal.get_calendar(calendar_name)
        except Exception as exc:  # pragma: no cover - defensive
            if logger:
                logger.debug(
                    "Calendar %s unavailable for market %s: %s",
                    calendar_name,
                    market,
                    exc,
                )
            continue

        # 使用可配置的起始年份，减少数据量
        start = pd.Timestamp(f"{start_year}-01-01")
        end = pd.Timestamp(date.today() + timedelta(days=365))
        
        if logger:
            logger.info(f"Downloading {market} calendar from {start.date()} to {end.date()}")
        
        schedule = calendar.schedule(start_date=start, end_date=end)
        if schedule.empty:
            continue

        return pd.DataFrame({"trade_date": schedule.index.strftime("%Y-%m-%d")})

    return None


def _download_calendar_from_akshare(
    market: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> Optional[pd.DataFrame]:
    """Download trading calendar DataFrame from AkShare."""

    if ak is None:
        return None

    if market == "CN" and hasattr(ak, "tool_trade_date_hist_sina"):
        return api_call_with_delay(
            ak.tool_trade_date_hist_sina,
            logger=logger,
        )

    if market == "HK":
        for candidate in (
            "stock_hk_trade_calendar",
            "stock_hk_trade_date",
            "stock_hk_trade_date_hist",
        ):
            if hasattr(ak, candidate):
                return api_call_with_delay(
                    getattr(ak, candidate),
                    logger=logger,
                )
        if logger:
            logger.debug("AkShare has no HK trading calendar endpoint available.")

    if market == "US":
        for candidate in ("stock_us_trade_date_hist", "stock_us_trade_date"):
            if hasattr(ak, candidate):
                return api_call_with_delay(
                    getattr(ak, candidate),
                    logger=logger,
                )
        if logger:
            logger.debug("AkShare has no US trading calendar endpoint available.")

    return None


def _extract_dates(df: pd.DataFrame) -> Set[str]:
    """Extract date strings from a calendar DataFrame."""
    for candidate in ("trade_date", "calendarDate", "cal_date", "date"):
        if candidate in df.columns:
            series = pd.to_datetime(df[candidate], errors="coerce")
            return {
                value.strftime("%Y-%m-%d")
                for value in series
                if not pd.isna(value)
            }
    if df.index.name and "date" in df.index.name.lower():
        series = pd.to_datetime(df.index, errors="coerce")
        return {
            value.strftime("%Y-%m-%d")
            for value in series
            if not pd.isna(value)
        }
    raise RuntimeError("Unable to extract dates from calendar DataFrame")


def _infer_index_prefix(code: str) -> str:
    """Infer whether an index should use sh or sz prefix."""
    if code.startswith(("000", "880", "885", "901", "58", "68")):
        return "sh"
    if code.startswith(("399", "159", "00")):
        return "sz"
    # Default to sh when uncertain; caller can override if necessary.
    return "sh"


def get_stock_name(symbol: str, logger: Optional[logging.Logger] = None) -> str:
    """
    获取股票的标准名称。
    
    首先从本地映射表查询，如果不存在则使用 akshare API 获取。
    对于港股使用 ak.stock_hk_financial_indicator_em，对于 A 股使用 ak.stock_financial_analysis_indicator_em。
    获取到的名称会保存到本地映射表中。
    
    Args:
        symbol: 股票代码，格式为 CODE.SUFFIX（如 301389.SZ 或 09988.HK）
        logger: 可选的日志记录器
        
    Returns:
        str: 股票的标准名称
        
    Raises:
        ValueError: 当 symbol 格式无效或无法获取股票名称时
    """
    # 标准化 symbol 格式
    normalized_symbol = normalize_symbol(symbol)
    
    # 本地映射表文件路径
    mapping_file = REPO_ROOT / "data/global_cache/symbol_stock_name_mapping.csv"
    
    # 确保目录存在
    mapping_file.parent.mkdir(parents=True, exist_ok=True)
    
    # 尝试从本地映射表读取
    if mapping_file.exists():
        try:
            mapping_df = pd.read_csv(mapping_file)
            # 查找匹配的 symbol
            match = mapping_df[mapping_df['symbol'] == normalized_symbol]
            if not match.empty:
                stock_name = _sanitize_stock_name_value(match.iloc[0]['stock_name'])
                if stock_name:
                    if logger:
                        logger.info(f"从本地映射表获取股票名称: {normalized_symbol} -> {stock_name}")
                    return stock_name
        except Exception as e:
            if logger:
                logger.warning(f"读取本地映射表失败: {e}")
    
    # 如果本地映射表中没有，则使用 akshare API 获取
    try:
        if is_hk_stock(normalized_symbol):
            # 港股使用 ak.stock_hk_financial_indicator_em
            if logger:
                logger.info(f"使用 akshare API 获取港股名称: {normalized_symbol}")
            
            # 提取港股代码（去掉 .HK 后缀）
            code = normalized_symbol.split('.')[0]
            
            # 调用 akshare API
            hk_data = api_call_with_delay(
                ak.stock_financial_hk_analysis_indicator_em,
                symbol=code,
                logger=logger
            )
            
            if hk_data is not None and not hk_data.empty:
                # 从返回数据中获取股票名称
                if 'SECURITY_NAME_ABBR' in hk_data.columns:
                    stock_name = hk_data['SECURITY_NAME_ABBR'].iloc[0]
                else:
                    raise ValueError(f"无法从港股数据中找到名称列: {hk_data.columns}")
            else:
                raise ValueError(f"stock_financial_hk_analysis_indicator_em 获取港股数据为空: {normalized_symbol}")
        else:
            # A 股使用 ak.stock_financial_analysis_indicator_em
            if logger:
                logger.info(f"使用 akshare API 获取 A 股名称: {normalized_symbol}")
            
            # 调用 akshare API
            a_stock_data = api_call_with_delay(
                ak.stock_financial_analysis_indicator_em,
                symbol=normalized_symbol,
                indicator="按报告期",
                logger=logger
            )
            
            if a_stock_data is not None and not a_stock_data.empty:
                # 从返回数据中获取股票名称
                if 'SECURITY_NAME_ABBR' in a_stock_data.columns:
                    stock_name = a_stock_data['SECURITY_NAME_ABBR'].iloc[0]
                else:
                    raise ValueError(f"无法从 ak.stock_financial_analysis_indicator_em 返回的A股数据中找到名称列: {a_stock_data.columns}")
            else:
                raise ValueError(f"使用 ak.stock_financial_analysis_indicator_em 获取 A 股数据为空: {normalized_symbol}")
        
        stock_name = _sanitize_stock_name_value(stock_name)
        if not stock_name:
            raise ValueError(f"获取到的股票名称为空: {normalized_symbol}")

        # 更新本地映射表
        try:
            # 读取现有映射表
            if mapping_file.exists():
                mapping_df = pd.read_csv(mapping_file)
            else:
                mapping_df = pd.DataFrame(columns=['symbol', 'stock_name'])
            
            # 检查是否已存在该 symbol
            existing_index = mapping_df[mapping_df['symbol'] == normalized_symbol].index
            if len(existing_index) > 0:
                # 更新现有记录
                mapping_df.loc[existing_index[0], 'stock_name'] = stock_name
            else:
                # 添加新记录
                new_row = pd.DataFrame({'symbol': [normalized_symbol], 'stock_name': [stock_name]})
                mapping_df = pd.concat([mapping_df, new_row], ignore_index=True)
            
            # 保存映射表
            mapping_df.to_csv(mapping_file, index=False)
            
            if logger:
                logger.info(f"更新本地股票代码-名称映射表: {normalized_symbol} -> {stock_name}")
        except Exception as e:
            if logger:
                logger.warning(f"更新股票代码-名称本地映射表失败: {e}")
        
        return stock_name
        
    except Exception as e:
        error_msg = f"获取股票名称失败: {normalized_symbol}, 错误: {e}"
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg)


def fetch_cn_a_daily_with_fallback(symbol_info: SymbolInfo, start_date: str, end_date: str, adjust: str = "qfq", logger: logging.Logger = None) -> pd.DataFrame:
    """优先使用 stock_zh_a_daily 获取A股行情，失败时回退到 stock_zh_a_hist。"""

    adjust = adjust or ""
    hist_symbol = symbol_info.code

    try:
        ak_symbol = symbol_info.to_akshare_equity()
        df = api_call_with_delay(
            ak.stock_zh_a_daily,
            symbol=ak_symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            logger=logger,
        )
        if df is None or df.empty:
            raise ValueError(f"stock_zh_a_daily 也未返回 {symbol_info.symbol} 数据")
        df = df.rename(
            columns={
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
        )
        return df
    except Exception as exc:
        logger.warning(
            "stock_zh_a_daily 获取 %s 失败，改用 stock_zh_a_hist: %s",
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
        if "流通股本" not in df.columns:
            df["流通股本"] = np.nan
        return df_hist
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
        
        # 重命名列名以保持一致性
        df = df.rename(
            columns={
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
        )
        
        # 添加缺失的列
        if "换手率" not in df.columns:
            df["换手率"] = np.nan
        if "流通股本" not in df.columns:
            df["流通股本"] = np.nan
        
        return df
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
        
        # 重命名列名以保持一致性
        df = df.rename(
            columns={
                "date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "volume": "成交量",
                "amount": "成交额",
                "outstanding_share": "流通股本",
            }
        )
        
        # 添加缺失的列
        if "换手率" not in df.columns:
            df["换手率"] = np.nan
        if "流通股本" not in df.columns:
            df["流通股本"] = np.nan
        
        return df
    except Exception as exc:
        logger.warning(
            "stock_zh_index_daily 获取 %s 指数数据失败: %s",
            symbol_info.symbol,
            exc,
        )
        raise


def fetch_hk_a_daily_with_fallback(symbol_info: SymbolInfo, start_date: str, end_date: str, adjust: str = "qfq", logger: logging.Logger = None) -> pd.DataFrame:
    """优先使用 stock_hk_daily 获取港股行情"""
    adjust = adjust or ""
    try:
        ak_symbol = symbol_info.to_hk_symbol()
        df = api_call_with_delay(
            ak.stock_hk_hist,
            symbol=ak_symbol,
            adjust=adjust,
            logger=logger,
        )
        if df is None or df.empty:
            raise ValueError(f"stock_hk_daily 未返回 {symbol_info.symbol} 数据")
        df = df.rename(
            columns={
                "date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "volume": "成交量",
            }
        )
        if "成交额" not in df.columns:
            df["成交额"] = np.nan
        if "换手率" not in df.columns:
            df["换手率"] = np.nan
        if "流通股本" not in df.columns:
            df["流通股本"] = np.nan
        
        # 使用start_date和end_date进行日期过滤
        if "日期" in df.columns and not df.empty:
            # 确保日期列是datetime类型
            df["日期"] = pd.to_datetime(df["日期"])
            
            # 转换start_date和end_date为datetime类型
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            # 过滤日期范围
            df = df[(df["日期"] >= start_dt) & (df["日期"] <= end_dt)]
            
            if logger:
                logger.info(f"港股数据已过滤日期范围 {start_date} 到 {end_date}, 剩余 {len(df)} 条记录")
        
        return df
    except Exception as exc:
        logger.warning(
            "stock_hk_daily 获取 %s %s 失败: %s",
            symbol_info.stock_name,
            symbol_info.symbol,
            exc,
        )
        raise ValueError(f"stock_hk_daily 获取 {symbol_info.stock_name} {symbol_info.symbol} 失败: {exc}")
