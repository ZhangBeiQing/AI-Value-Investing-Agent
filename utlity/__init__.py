"""
utlity包 - 提供通用的工具函数和缓存管理功能

主要模块:
- utility: 缓存管理和通用工具函数
- utils: 重试机制、数据过滤和格式化工具
- pdf_marker: PDF处理工具
"""

# 从utility模块导入常用的缓存管理函数
from .utility import (
    read_cache_file,
    manage_cache_with_cleanup,
    show_json,
    show_parts
)

# 从utils模块导入常用的工具函数
from .utils import (
    retry,
    filter_reports_by_type,
    abbreviate_number,
    save_to_file
)

# 从stock_utils模块导入股票相关工具
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
)

# 定义包的公开接口
__all__ = [
    # utility模块的函数
    'read_cache_file',
    'manage_cache_with_cleanup',
    'show_json',
    'show_parts',
    # utils模块的函数
    'retry',
    'filter_reports_by_type',
    'abbreviate_number',
    'save_to_file',
    # stock_utils模块的函数和类
    'SymbolInfo',
    'SymbolFormatError',
    'api_call_with_delay',
    'normalize_symbol',
    'parse_symbol',
    'sanitize_stock_name',
    'get_stock_data_dir',
    'ensure_stock_subdir',
    'is_trading_day',
    'get_last_trading_day',
    'get_trading_calendar',
    'get_latest_trading_day',
    'get_next_trading_day',
    'is_cache_expired',
    'resolve_base_dir',
    'is_cn_etf',
    'is_cn_etf_symbol',
    # pdf_marker模块的函数
    'fetch_cn_a_daily_with_fallback',
    'fetch_hk_a_daily_with_fallback'
]

# 包版本信息
__version__ = '1.0.0'
__author__ = 'LLM Stock Analysis Team'
