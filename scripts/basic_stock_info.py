#!/usr/bin/env python3
"""CLI entry for the basic stock snapshot step (invoked as a subprocess by manage_daily_data)."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.network import install_network_timeouts
from services.snapshot.basic_snapshot import *  # noqa: F401,F403

# 本脚本会直接跑在子进程里抓取外部数据；未设超时的 socket 读一旦卡住会永久挂起。
install_network_timeouts()


if __name__ == "__main__":
    from services.snapshot.basic_snapshot import _main

    _main()

