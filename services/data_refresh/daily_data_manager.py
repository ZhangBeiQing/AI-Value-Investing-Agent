"""Service wrapper for daily data refresh."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_manage_daily_data(run_date: str, signature: str = "") -> None:
    command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "manage_daily_data.py"),
        "--date",
        run_date,
    ]
    if signature:
        command.extend(["--signature", signature])
    subprocess.run(command, check=True, cwd=str(PROJECT_ROOT))

