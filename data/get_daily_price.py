"""Fetch daily行情并转存为 Alpha Vantage 风格 JSON 文件。

本脚本现在完全依赖 SharedDataAccess 作为统一数据入口，不再直接调用
akshare。这样可以复用缓存目录、日志与港/美股扩展逻辑，同时保证
manage_daily_data 等上层流程只需维护一套数据抓取策略。生成的数据
格式仍与原始 Alpha Vantage 接口保持兼容，便于 `merge_jsonl.py` 与
`price_tools.py` 等下游模块复用。

使用说明：
1. 基本用法：python get_daily_price.py
   - 默认获取股票池中所有股票的日线数据
   - 使用1天缓存（当天内不会重复抓取）

2. 指定股票：python get_daily_price.py --symbols 000001.SZ 600036.SH
   - 只获取指定股票的数据

3. 强制刷新：python get_daily_price.py --force-refresh
   - 忽略缓存，强制重新抓取所有数据

4. 调整参数：python get_daily_price.py --limit 30 --delay 1.0 --cache-ttl-days 0
   - --limit: 限制交易日数量（默认240天）
   - --delay: API调用间隔（默认0.5秒）
   - --cache-ttl-days: 缓存有效期（默认1天，0表示禁用缓存）
   - --adjust: 复权类型（默认qfq前复权，可选qfq前复权/hfq后复权/不复权）

5. 输出文件：
   - 生成的JSON文件保存在当前目录，命名格式：daily_prices_{symbol}.json
   - 文件格式与Alpha Vantage API返回格式保持一致

6. 下游使用：
   - 生成的JSON文件会被 merge_jsonl.py 合并为 merged.jsonl
   - price_tools.py 从 merged.jsonl 读取价格数据供交易系统使用
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import logging

# Import project tools
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from utlity import parse_symbol
from shared_data_access.data_access import SharedDataAccess
from shared_data_access.models import PriceDataBundle
from configs.stock_pool import TRACKED_SYMBOLS

DATA_DIR = Path(__file__).resolve().parent
DEFAULT_LIMIT = 900  # roughly one trading year
LOGGER = logging.getLogger("get_daily_price")
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def should_use_cache(file_path: Path, cache_ttl_days: int) -> bool:
    if cache_ttl_days <= 0 or not file_path.exists():
        return False
    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=cache_ttl_days)


def format_decimal(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return f"{float(value):.4f}"


def format_volume(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return f"{int(round(float(value)))}"


def normalize_dataframe(df, limit: int):
    df = df.copy()
    if limit > 0:
        df = df.tail(limit)
    df = df.sort_values("date", ascending=False)
    return df


def extract_price_dataframe(bundle: PriceDataBundle) -> pd.DataFrame:
    if bundle.frame.empty:
        raise ValueError("价格数据为空，无法构建日线payload")
    frame = bundle.frame.copy()
    index_name = frame.index.name or "date"
    frame.index.name = index_name
    frame = frame.reset_index().rename(columns={index_name: "date"})
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    sanitized = sanitize_price_dataframe(frame)
    sanitized["date"] = sanitized["date"].dt.strftime("%Y-%m-%d")
    return sanitized


def sanitize_price_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """统一列名并确保包含 date/open/high/low/close/volume。"""

    rename_map = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "开": "open",
        "高": "high",
        "低": "low",
        "收": "close",
        "量": "volume",
    }
    cleaned = df.rename(columns=rename_map)
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if col in cleaned.columns:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
    return cleaned


def dataframe_to_payload(symbol: str, df, limit: int, adjust: str) -> Dict:
    df = normalize_dataframe(df, limit)
    series: Dict[str, Dict[str, Optional[str]]] = {}

    for _, row in df.iterrows():
        date_value = row["date"]
        if isinstance(date_value, (datetime,)):
            date_str = date_value.strftime("%Y-%m-%d")
        else:
            date_str = str(date_value)

        series[date_str] = {
            "1. open": format_decimal(row.get("open")),
            "2. high": format_decimal(row.get("high")),
            "3. low": format_decimal(row.get("low")),
            "4. close": format_decimal(row.get("close")),
            "5. volume": format_volume(row.get("volume")),
        }

    last_refreshed = next(iter(series.keys()), "")
    output_size = "Compact" if limit > 0 else "Full"

    return {
        "Meta Data": {
            "1. Information": "Daily Prices (open, high, low, close) and Volumes",
            "2. Symbol": symbol,
            "3. Last Refreshed": last_refreshed,
            "4. Output Size": output_size,
            "5. Time Zone": "Asia/Shanghai",
            "6. Adjust Type": adjust or "none",
        },
        "Time Series (Daily)": series,
    }


def fetch_symbol_daily(
    symbol: str,
    *,
    adjust: str,
    limit: int,
    shared_access: SharedDataAccess,
    as_of_date: str,
    force_refresh_data: bool = False,
) -> Dict:
    info = parse_symbol(symbol)
    dataset = shared_access.prepare_dataset(
        symbolInfo=info,
        as_of_date=as_of_date,
        force_refresh=force_refresh_data,
        force_refresh_price=force_refresh_data,
        force_refresh_financials=False,
        skip_financial_refresh=True,
    )
    price_df = extract_price_dataframe(dataset.prices)
    return dataframe_to_payload(symbol, price_df, limit=limit, adjust=adjust)


def save_payload(symbol: str, payload: Dict):
    file_path = DATA_DIR / f"daily_prices_{symbol}.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=4)
    return file_path


def parse_args():
    parser = argparse.ArgumentParser(description="拉取A股日线行情并保存为JSON")
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=TRACKED_SYMBOLS,
        help="目标股票列表，使用 000001.SZ 格式，默认取股票池",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="每只股票保留的交易日数量，默认240个交易日",
    )
    parser.add_argument(
        "--adjust",
        choices=["", "qfq", "hfq"],
        default="qfq",
        help="akshare 复权参数，默认为前复权",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="每次 API 调用后的休眠秒数，默认0.5秒",
    )
    parser.add_argument(
        "--cache-ttl-days",
        type=int,
        default=1,
        help="缓存有效期（天）。在此时间内不会重复抓取，0 表示每次都刷新",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="忽略缓存，强制重新拉取",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    shared_access = SharedDataAccess(base_dir=None, logger=LOGGER)
    as_of_date = datetime.now().strftime("%Y-%m-%d")
    if args.delay != 0.5:
        LOGGER.info("delay 参数在 SharedDataAccess 模式下已不再使用，忽略自定义值 %s", args.delay)

    for symbol in args.symbols:
        output_path = DATA_DIR / f"daily_prices_{symbol}.json"
        if not args.force_refresh and should_use_cache(output_path, args.cache_ttl_days):
            print(f"✅ {symbol} 使用缓存 {output_path}")
            continue

        try:
            payload = fetch_symbol_daily(
                symbol,
                adjust=args.adjust,
                limit=args.limit,
                shared_access=shared_access,
                as_of_date=as_of_date,
                force_refresh_data=args.force_refresh,
            )
            save_payload(symbol, payload)
            print(f"✅ {symbol} 数据已更新 -> {output_path}")
        except Exception as exc:
            print(f"❌ 获取 {symbol} 失败: {exc}")


if __name__ == "__main__":
    main()
