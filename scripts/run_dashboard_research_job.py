#!/usr/bin/env python3
"""Detached worker for one dashboard research job."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.recommendation_dashboard.jobs import run_job  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="运行网页单股研究后台任务")
    parser.add_argument("job_id")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_ROOT / "data")
    args = parser.parse_args()
    run_job(args.data_dir.resolve(), args.job_id)


if __name__ == "__main__":
    main()
