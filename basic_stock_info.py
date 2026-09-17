"""Compatibility wrapper for historical basic stock snapshot imports."""

from core.network import install_network_timeouts
from services.snapshot.basic_snapshot import *  # noqa: F401,F403

# 本脚本会直接跑在子进程里抓取外部数据；未设超时的 socket 读一旦卡住会永久挂起。
install_network_timeouts()


if __name__ == "__main__":
    from services.snapshot.basic_snapshot import _main

    _main()

