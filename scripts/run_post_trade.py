#!/usr/bin/env python3
"""Run trade execution and summary merge for a skill output directory."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run post-trade steps for the skill pipeline")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--signature", default="")
    parser.add_argument("--confirm", action="store_true", help="Compatibility flag; execution still proceeds directly.")
    args = parser.parse_args()

    execute_command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "execute_trade_from_decision.py"),
        "--date",
        args.run_date,
        "--base-dir",
        args.base_dir,
    ]
    if args.signature:
        execute_command.extend(["--signature", args.signature])

    merge_command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "merge_trade_summary.py"),
        "--date",
        args.run_date,
        "--base-dir",
        args.base_dir,
    ]
    if args.signature:
        merge_command.extend(["--signature", args.signature])

    subprocess.run(execute_command, check=True, cwd=str(PROJECT_ROOT))
    subprocess.run(merge_command, check=True, cwd=str(PROJECT_ROOT))


if __name__ == "__main__":
    main()
