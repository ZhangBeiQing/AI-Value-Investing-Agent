"""CLI for inspecting cache registry health.

Usage examples:

    python tools/cache_registry_cli.py --symbol 002415.SZ --stock-name 海康威视

    python tools/cache_registry_cli.py --symbol 600519.SH --stock-name 贵州茅台 --kinds financials_cache prices
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared_data_access.cache_registry import (
    CacheKind,
    build_cache_dir,
    check_cache,
)
from stock_data_provider import StockDataProvider


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect cache health for stocks")
    parser.add_argument("--symbol", help="股票代码 (CODE.SUFFIX)")
    parser.add_argument("--stock-name", help="股票中文名称")
    parser.add_argument("--batch-file", help="批量文件（每行: symbol,stock_name）")
    parser.add_argument("--base-dir", default=None, help="数据根目录，默认读取 .env 配置")
    parser.add_argument(
        "--kinds",
        nargs="*",
        default=[kind.value for kind in CacheKind],
        help="要检查的缓存类别，默认全部",
    )
    parser.add_argument("--json", action="store_true", help="以 JSON 输出结果")
    parser.add_argument("--ensure", action="store_true", help="缺失时调用 StockDataProvider 刷新缓存")
    parser.add_argument("--price-lookback", type=int, default=420, help="ensure 模式下的价格 lookback 天数")
    return parser


def parse_args() -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args()
    if not args.batch_file and (not args.symbol or not args.stock_name):
        parser.error("必须提供 --symbol/--stock-name 或 --batch-file")
    return args


def run_checks(
    targets: list[tuple[str, str]],
    kinds: list[CacheKind],
    *,
    base_dir: str | None,
    ensure: bool,
    price_lookback: int,
) -> list[dict]:
    provider: StockDataProvider | None = StockDataProvider(base_dir=base_dir) if ensure else None
    rows: list[dict] = []
    for symbol, stock_name in targets:
        if provider and ensure:
            try:
                ensure_symbol_data(
                    base_dir,
                    symbol,
                    stock_name=stock_name,
                    price_lookback_days=price_lookback,
                    force_refresh=False,
                )
            except Exception as exc:
                print(f"⚠️ ensure {symbol} 失败: {exc}")
        for kind in kinds:
            cache_dir = build_cache_dir(stock_name, symbol, kind, base_dir=base_dir, ensure=False)
            status = check_cache(cache_dir, kind)
            rows.append(
                {
                    "symbol": symbol,
                    "stock_name": stock_name,
                    "kind": kind.value,
                    "path": str(cache_dir),
                    "exists": cache_dir.exists(),
                    "missing_files": status.missing_files,
                    "last_updated": status.last_updated.isoformat() if status.last_updated else None,
                    "stale": status.stale,
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    targets: list[tuple[str, str]] = []
    if args.batch_file:
        batch_path = Path(args.batch_file)
        if not batch_path.exists():
            raise SystemExit(f"批量文件不存在: {batch_path}")
        for raw in batch_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                symbol, name = [part.strip() for part in line.split(",", 1)]
            else:
                parts = line.split()
                if len(parts) < 2:
                    raise SystemExit(f"批量文件格式错误: {line}")
                symbol, name = parts[0], parts[1]
            targets.append((symbol, name))
    else:
        targets.append((args.symbol, args.stock_name))

    kinds = []
    for raw in args.kinds:
        try:
            kinds.append(CacheKind(raw))
        except ValueError:
            raise SystemExit(f"未知缓存类型: {raw}")

    rows = run_checks(
        targets,
        kinds,
        base_dir=args.base_dir,
        ensure=args.ensure,
        price_lookback=args.price_lookback,
    )

    if args.json:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        for row in rows:
            print(f"{row['symbol']} [{row['kind']}] {row['path']}")
            print(f"  exists      : {row['exists']}")
            print(f"  last_updated: {row['last_updated']}")
            print(f"  stale       : {row['stale']}")
            if row["missing_files"]:
                print(f"  missing     : {', '.join(row['missing_files'])}")
            print()


if __name__ == "__main__":
    main()
