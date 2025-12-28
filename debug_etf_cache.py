
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import os
import sys
import json
import logging

# Setup path to import modules
sys.path.append("/home/zhangbeiqing/programer/AI-Trader_my")

from shared_data_access.cache_registry import (
    build_cache_dir, 
    CacheKind, 
    parse_symbol, 
    update_price_data_cached,
    _meta_path
)
from utlity import SymbolInfo

# Setup basic logging to see output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("debug_etf")

def debug_etf_cache():
    symbol = "513300.SH"
    symbol_info = parse_symbol(symbol)
    base_dir = Path("data")
    
    # Locate cache file
    price_cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.PRICE_SERIES,
        base_dir=base_dir,
        ensure=True,
    )
    
    # 1. Simulate a scenario where we ALREADY requested data from 2000 days ago
    # Required start for 2000 days lookback is roughly 2020-06-XX
    # Let's say we requested from 2020-01-01
    requested_start = "20200101" 
    
    # Write metadata simulating a previous successful fetch attempt (even if data is short)
    meta_path = _meta_path(price_cache_dir)
    meta_content = {
        "last_updated": datetime.now().isoformat(),
        "requested_start_date": requested_start
    }
    meta_path.write_text(json.dumps(meta_content), encoding="utf-8")
    print(f"Written metadata to {meta_path}: {meta_content}")

    # 2. Call update_price_data_cached with lookback_days=2000
    # This should internally trigger the check:
    #   existing data (from 2020-11) is LATER than required (2020-06)
    #   BUT meta['requested_start_date'] (2020-01) is EARLIER than required (2020-06)
    #   So it should SKIP refresh.
    
    print("\nCalling update_price_data_cached...")
    df = update_price_data_cached(
        symbolInfo=symbol_info,
        lookback_days=2000,
        force_refresh=False,
        base_data_dir=base_dir,
        logger=logger
    )
    
    if df.empty:
        print("Returned empty DataFrame (unexpected if file exists)")
    else:
        print(f"Returned DataFrame with {len(df)} rows. Range: {df['日期'].min()} to {df['日期'].max()}")
        print("Success! If API call logs are missing above, then cache bypass worked.")

if __name__ == "__main__":
    debug_etf_cache()
