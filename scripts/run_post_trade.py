#!/usr/bin/env python3
"""Run trade execution and summary merge for a skill output directory."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.post_trade_pipeline import run_post_trade


def main() -> None:
    parser = argparse.ArgumentParser(description="Run post-trade steps for the skill pipeline")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--output-dir", default="", help="Override skill output directory.")
    parser.add_argument(
        "--book-type",
        default="",
        choices=["", "fixed_tracked", "short_book", "long_book"],
        help="Run post-trade for a specific book output directory.",
    )
    parser.add_argument("--signature", default="")
    parser.add_argument("--confirm", action="store_true", help="Compatibility flag; execution still proceeds directly.")
    args = parser.parse_args()

    run_post_trade(
        args.run_date,
        base_dir=args.base_dir,
        signature=args.signature,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        book_type=args.book_type,
    )


if __name__ == "__main__":
    main()
