"""commons 包 - 提供股票符号、交易日历与缓存目录等通用工具。

主要模块:
- stock_utils: 符号规范化、交易日历、文件路径布局与行情抓取/归一化
- similar_stocks: 相似股票检索
"""

from .stock_utils import (
    SymbolInfo,
    SymbolFormatError,
    api_call_with_delay,
    normalize_symbol,
    parse_symbol,
    sanitize_stock_name,
    get_stock_data_dir,
    ensure_stock_subdir,
    is_trading_day,
    get_trading_calendar,
    get_latest_trading_day,
    get_next_trading_day,
    is_cache_expired,
    resolve_base_dir,
    get_last_trading_day,
    fetch_cn_a_daily_with_fallback,
    fetch_hk_a_daily_with_fallback,
    is_cn_etf,
    is_cn_etf_symbol,
    is_etf_symbol,
    CANONICAL_PRICE_COLUMNS,
)

__all__ = [
    "SymbolInfo",
    "SymbolFormatError",
    "api_call_with_delay",
    "normalize_symbol",
    "parse_symbol",
    "sanitize_stock_name",
    "get_stock_data_dir",
    "ensure_stock_subdir",
    "is_trading_day",
    "get_last_trading_day",
    "get_trading_calendar",
    "get_latest_trading_day",
    "get_next_trading_day",
    "is_cache_expired",
    "resolve_base_dir",
    "is_cn_etf",
    "is_cn_etf_symbol",
    "is_etf_symbol",
    "fetch_cn_a_daily_with_fallback",
    "fetch_hk_a_daily_with_fallback",
    "CANONICAL_PRICE_COLUMNS",
]

__version__ = "1.0.0"
__author__ = "LLM Stock Analysis Team"
