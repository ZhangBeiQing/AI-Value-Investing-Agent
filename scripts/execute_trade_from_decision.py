#!/usr/bin/env python3
"""Execute trades based on 05_decision.json using local Python functions."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.post_trade_pipeline import execute_trade_from_decision


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute trades based on 05_decision.json")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--base-dir", default="data", help="Base data directory.")
    parser.add_argument("--output-dir", default="", help="Skill output directory. Default: data/skill_runs/YYYY-MM-DD")
    parser.add_argument("--decision-file", default="", help="Override decision JSON path (default: output_dir/05_decision.json).")
    parser.add_argument("--skip-validate", action="store_true", help="Skip decision JSON validation.")
    parser.add_argument("--signature", default="", help="Trading signature (fallback to env SIGNATURE).")
    args = parser.parse_args()

    execute_trade_from_decision(
        args.run_date,
        base_dir=args.base_dir,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        decision_file=Path(args.decision_file) if args.decision_file else None,
        skip_validate=args.skip_validate,
        signature=args.signature,
    )


if __name__ == "__main__":
    main()
