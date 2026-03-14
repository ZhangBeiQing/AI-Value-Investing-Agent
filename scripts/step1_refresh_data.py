#!/usr/bin/env python3
"""Step 1 for the skill pipeline: refresh caches and daily market data."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.pipeline.steps.refresh_data import run_refresh_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh daily data for the skill pipeline.")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", required=True, help="Unused but kept for pipeline compatibility.")
    parser.add_argument("--signature", default="", help="Agent signature.")
    args = parser.parse_args()

    run_refresh_data(args.run_date, signature=args.signature)


if __name__ == "__main__":
    main()
