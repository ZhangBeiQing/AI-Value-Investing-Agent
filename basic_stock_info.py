"""Compatibility wrapper for historical basic stock snapshot imports."""

from services.snapshot.basic_snapshot import *  # noqa: F401,F403


if __name__ == "__main__":
    from services.snapshot.basic_snapshot import _main

    _main()

