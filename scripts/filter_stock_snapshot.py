#!/usr/bin/env python3
"""Filter stock snapshots by a pandas-query style expression."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.selection_system.stock_snapshot_query import filter_stock_snapshot


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Filter stock snapshots with a query expression.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    parser.add_argument(
        "--expr",
        required=True,
        help='Filter expression, e.g. \'liquidity_score >= 0.6 and pe_ttm <= 25\'',
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    payload = filter_stock_snapshot(
        args.date,
        expr=args.expr,
    )
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
