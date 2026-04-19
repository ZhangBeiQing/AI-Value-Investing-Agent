"""Service wrapper for daily data refresh."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_manage_daily_data(
    run_date: str,
    signature: str = "",
    *,
    symbols: Iterable[str] | None = None,
    max_workers: int = 4,
) -> None:
    command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "manage_daily_data.py"),
        "--date",
        run_date,
    ]
    if signature:
        command.extend(["--signature", signature])
    command.extend(["--max-workers", str(max(1, int(max_workers or 1)))])
    target_symbols = [symbol for symbol in (symbols or []) if symbol]
    if target_symbols:
        command.extend(["--symbols", *target_symbols])
    subprocess.run(command, check=True, cwd=str(PROJECT_ROOT))
