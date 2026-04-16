#!/usr/bin/env python3
"""Rank stock snapshots by a selected field."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.selection_system.stock_snapshot_query import rank_stock_snapshot


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rank stock snapshots by field.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    parser.add_argument("--field", required=True, help="Snapshot field to rank by.")
    parser.add_argument("--top", type=int, default=20, help="How many rows to return. Default: 20")
    parser.add_argument(
        "--ascending",
        action="store_true",
        help="Sort ascending. If omitted, direction is inferred from the field.",
    )
    parser.add_argument(
        "--descending",
        action="store_true",
        help="Sort descending. Overrides inferred direction.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    ascending = None
    if args.ascending and args.descending:
        parser.error("--ascending 与 --descending 不能同时使用")
    if args.ascending:
        ascending = True
    elif args.descending:
        ascending = False
    payload = rank_stock_snapshot(
        args.date,
        field=args.field,
        top=args.top,
        ascending=ascending,
    )
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
