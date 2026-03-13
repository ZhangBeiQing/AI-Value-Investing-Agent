#!/usr/bin/env python3
"""Step 1 for the skill pipeline: refresh caches and daily market data."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh daily data for the skill pipeline.")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", required=True, help="Unused but kept for pipeline compatibility.")
    parser.add_argument("--signature", default="", help="Agent signature.")
    args = parser.parse_args()

    command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "manage_daily_data.py"),
        "--date",
        args.run_date,
    ]
    if args.signature:
        command.extend(["--signature", args.signature])

    subprocess.run(command, check=True, cwd=str(PROJECT_ROOT))


if __name__ == "__main__":
    main()
