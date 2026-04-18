#!/usr/bin/env python3
"""Run the daily skill pipeline (steps 1-4)."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.pipeline.daily_pipeline import SKILL_FLOW_CONFIG, run_daily_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily pipeline steps 1-4.")
    parser.add_argument(
        "--date",
        dest="run_date",
        default=date.today().strftime("%Y-%m-%d"),
        help="Run date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--base-dir",
        default="data",
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--prompt-config",
        default=str(SKILL_FLOW_CONFIG),
        help="Prompt flow config path (default: skill_flow.json).",
    )
    parser.add_argument(
        "--signature",
        default="",
        help="Agent signature used for historical context and trade summary files.",
    )
    parser.add_argument(
        "--manifest",
        default="auto",
        help="Run manifest path or 'auto' to build from selection outputs.",
    )
    args = parser.parse_args()

    prompt_config = args.prompt_config
    if args.manifest == "auto" and not args.signature and prompt_config == str(SKILL_FLOW_CONFIG):
        prompt_config = None

    run_daily_pipeline(
        args.run_date,
        base_dir=args.base_dir,
        prompt_config=prompt_config,
        signature=args.signature,
        manifest_path=args.manifest,
    )


if __name__ == "__main__":
    main()
