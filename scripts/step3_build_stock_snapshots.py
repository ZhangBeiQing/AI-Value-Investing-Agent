#!/usr/bin/env python3
"""Step 3 for the skill pipeline: generate 04_stock_research/*.md."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.pipeline.steps.build_stock_research import write_stock_research_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Build stock research markdown files.")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--signature", default="")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    write_stock_research_bundle(args.run_date, output_dir)


if __name__ == "__main__":
    main()
