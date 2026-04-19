#!/usr/bin/env python3
"""Bootstrap agent_data directories for fixed/short/long books."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.book_data_bootstrap import bootstrap_book_agent_data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bootstrap data/agent_data/book-* directories for multi-book trading."
    )
    parser.add_argument("--date", dest="run_date", default="", help="Run date (YYYY-MM-DD). Default: latest skill_runs date.")
    parser.add_argument("--base-dir", default="data", help="Base data directory (default: data).")
    parser.add_argument(
        "--legacy-signature",
        default="deepseek-reasoner",
        help="Legacy agent_data signature to copy into book-fixed_tracked.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing book directories/files in place when possible.",
    )
    args = parser.parse_args()

    result = bootstrap_book_agent_data(
        run_date=args.run_date or None,
        base_dir=args.base_dir,
        legacy_signature=args.legacy_signature,
        force=args.force,
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
