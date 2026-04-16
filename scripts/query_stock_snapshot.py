#!/usr/bin/env python3
"""Query compact stock snapshots by symbol or stock name."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.selection_system.stock_snapshot_query import query_stock_snapshot


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query stock snapshots by symbol or stock name.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=[],
        help="Symbol to query. Repeat this flag to query multiple symbols.",
    )
    parser.add_argument(
        "--stock-name",
        dest="stock_names",
        action="append",
        default=[],
        help="Stock name to query. Repeat this flag to query multiple names.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if not args.symbols and not args.stock_names:
        parser.error("至少需要提供 --symbol 或 --stock-name")
    payload = query_stock_snapshot(
        args.date,
        symbols=args.symbols,
        stock_names=args.stock_names,
    )
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
