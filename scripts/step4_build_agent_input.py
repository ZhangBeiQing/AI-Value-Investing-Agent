#!/usr/bin/env python3
"""Step 4 for the skill pipeline: generate 02_basic_snapshot_payload.json and 03_agent_input.md."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.pipeline.steps.build_agent_input import DEFAULT_PROMPT_CONFIG, write_agent_input_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Build skill agent input files.")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--signature", default="")
    parser.add_argument("--prompt-config", default=os.environ.get("PROMPT_FLOW_CONFIG", str(DEFAULT_PROMPT_CONFIG)))
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    write_agent_input_bundle(
        args.run_date,
        output_dir,
        signature=args.signature,
        prompt_config=Path(args.prompt_config),
    )


if __name__ == "__main__":
    main()
