#!/usr/bin/env python3
"""Merge the daily skill decision into persistent trade summary files."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.post_trade_pipeline import merge_trade_summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge skill outputs into trade summary files")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--decision-file", default="")
    parser.add_argument("--signature", default="")
    args = parser.parse_args()

    merge_trade_summary(
        args.run_date,
        base_dir=args.base_dir,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        decision_file=Path(args.decision_file) if args.decision_file else None,
        signature=args.signature,
    )


if __name__ == "__main__":
    main()
