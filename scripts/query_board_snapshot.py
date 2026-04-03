#!/usr/bin/env python3
"""Query compact board snapshots by board name."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.selection_system.board_snapshot_query import query_board_snapshot


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query compact board snapshot summaries by board name.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format.")
    parser.add_argument(
        "--board-name",
        dest="board_names",
        action="append",
        required=True,
        help="Board name to query. Repeat this flag to query multiple boards.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    payload = query_board_snapshot(
        args.date,
        args.board_names,
    )
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
