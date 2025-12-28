"""CI-friendly cache health check.

Example:

    python tools/cache_health_check.py --batch-file configs/stock_targets.txt --ensure
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from cache_registry_cli import build_parser as build_cache_parser, run_checks
from cache_registry_cli import CacheKind


def parse_args() -> argparse.Namespace:
    parser = build_cache_parser()
    parser.add_argument("--fail-on-missing", action="store_true", help="缺少关键文件即失败")
    parser.add_argument("--fail-on-stale", action="store_true", help="缓存过期即失败")
    parser.add_argument("--output", help="可选：将检查结果写入 JSON 文件")
    args = parser.parse_args()
    if not args.batch_file and (not args.symbol or not args.stock_name):
        parser.error("必须提供 --symbol/--stock-name 或 --batch-file")
    return args


def run() -> None:
    args = parse_args()
    targets: list[tuple[str, str]] = []
    if args.batch_file:
        batch_path = Path(args.batch_file)
        for raw in batch_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                symbol, name = [part.strip() for part in line.split(",", 1)]
            else:
                symbol, name = line.split()
            targets.append((symbol, name))
    else:
        targets.append((args.symbol, args.stock_name))

    kinds = [CacheKind(raw) for raw in args.kinds]
    rows = run_checks(
        targets,
        kinds,
        base_dir=args.base_dir,
        ensure=args.ensure,
        price_lookback=args.price_lookback,
    )

    if args.output:
        Path(args.output).write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    failing = []
    for row in rows:
        if args.fail_on_missing and row["missing_files"]:
            failing.append(row)
        elif args.fail_on_stale and row["stale"]:
            failing.append(row)

    if failing:
        print("⚠️ 缓存检查失败: 以下条目异常")
        for row in failing:
            print(
                f"- {row['symbol']} [{row['kind']}] missing={row['missing_files']} stale={row['stale']}"
            )
        raise SystemExit(1)
    else:
        print("✅ 缓存检查通过，总计", len(rows), "项")


if __name__ == "__main__":
    run()
