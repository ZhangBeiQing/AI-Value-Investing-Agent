"""Migrate legacy cache layout to the shared cache registry structure.

Usage:

    python tools/cache_migration.py --base-dir data --dry-run

    python tools/cache_migration.py --stocks-file configs/stock_pool.txt

The script currently handles:
1. ``analysis/prices_*.csv`` -> ``prices/{yyyymmdd}.csv``
2. ``analysis/*enhanced_pe*`` -> ``pe_pb_analysis/``
3. Legacy ``financial_cache`` (no "s") -> ``financials_cache``

It prints a summary of operations and records cache metadata via
``record_cache_refresh`` when files change.
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path
from typing import Iterable, List, Tuple

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared_data_access.cache_registry import CacheKind, build_cache_dir, record_cache_refresh


PRICE_PATTERN = re.compile(r"prices_(\d{8})\.csv$", re.IGNORECASE)
PE_FILE_PATTERN = re.compile(r"enhanced_pe_analysis", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate legacy cache files")
    parser.add_argument("--base-dir", default="data", help="数据根目录，默认 data/")
    parser.add_argument("--stocks-file", help="可选：自定义股票列表文件 (symbol,stock_name)")
    parser.add_argument("--dry-run", action="store_true", help="仅打印计划，不执行移动")
    return parser.parse_args()


def iter_stock_dirs(base_dir: Path, stock_file: str | None) -> Iterable[Tuple[str, str, Path]]:
    if stock_file:
        path = Path(stock_file)
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                symbol, name = [part.strip() for part in line.split(",", 1)]
            else:
                parts = line.split()
                if len(parts) < 2:
                    raise ValueError(f"非法行: {line}")
                symbol, name = parts[0], parts[1]
            stock_dir = base_dir / f"{name}_{symbol}"
            if stock_dir.is_dir():
                yield symbol, name, stock_dir
    else:
        for child in base_dir.iterdir():
            if "_" not in child.name:
                continue
            try:
                name, symbol = child.name.rsplit("_", 1)
            except ValueError:
                continue
            if child.is_dir():
                yield symbol, name, child


def migrate_financial_cache(stock_dir: Path, dry_run: bool) -> bool:
    legacy = stock_dir / "financial_cache"
    target = stock_dir / CacheKind.FINANCIALS.value
    if not legacy.exists() or legacy.resolve() == target.resolve():
        return False
    print(f"[financials] rename {legacy} -> {target}")
    if dry_run:
        return True
    target.mkdir(parents=True, exist_ok=True)
    for item in legacy.iterdir():
        shutil.move(str(item), target / item.name)
    legacy.rmdir()
    record_cache_refresh(target)
    return True


def migrate_prices(stock_dir: Path, dry_run: bool) -> bool:
    analysis_dir = stock_dir / "analysis"
    if not analysis_dir.is_dir():
        return False
    prices_dir = stock_dir / CacheKind.PRICE_SERIES.value
    changed = False
    for file in list(analysis_dir.glob("prices_*.csv")):
        match = PRICE_PATTERN.match(file.name)
        if not match:
            continue
        target_name = f"{match.group(1)}.csv"
        target_path = prices_dir / target_name
        prices_dir.mkdir(parents=True, exist_ok=True)
        print(f"[prices] move {file} -> {target_path}")
        changed = True
        if not dry_run:
            shutil.move(str(file), target_path)
    if changed and not dry_run:
        record_cache_refresh(prices_dir)
    return changed


def migrate_pe_analysis(stock_dir: Path, dry_run: bool) -> bool:
    analysis_dir = stock_dir / "analysis"
    if not analysis_dir.is_dir():
        return False
    target_dir = stock_dir / CacheKind.PE_ANALYSIS.value
    changed = False
    for file in list(analysis_dir.glob("*")):
        if file.is_dir():
            continue
        if PE_FILE_PATTERN.search(file.name) or file.suffix.lower() in {".json", ".md", ".csv"} and "pe_" in file.stem:
            target_dir.mkdir(parents=True, exist_ok=True)
            print(f"[pe_pb_analysis] move {file} -> {target_dir / file.name}")
            changed = True
            if not dry_run:
                shutil.move(str(file), target_dir / file.name)
    if changed and not dry_run:
        record_cache_refresh(target_dir)
    return changed


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    if not base_dir.exists():
        raise SystemExit(f"base_dir 不存在: {base_dir}")

    summary: List[str] = []
    for symbol, name, stock_dir in iter_stock_dirs(base_dir, args.stocks_file):
        print(f"\n==> {name} ({symbol})")
        touched = False
        touched |= migrate_financial_cache(stock_dir, args.dry_run)
        touched |= migrate_prices(stock_dir, args.dry_run)
        touched |= migrate_pe_analysis(stock_dir, args.dry_run)
        if touched:
            summary.append(f"{name}({symbol})")
        else:
            print("  无需迁移")

    print("\n迁移完成" + (" (dry-run)" if args.dry_run else ""))
    if summary:
        print("已处理股票:", ", ".join(summary))
    else:
        print("没有发现需要迁移的缓存")


if __name__ == "__main__":
    main()
