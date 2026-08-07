"""Per-experiment process lock for mutable backtest stages."""

from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar


F = TypeVar("F", bound=Callable[..., Any])


@contextmanager
def experiment_lock(experiment_root: str | Path) -> Iterator[None]:
    root = Path(experiment_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".backtest.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"回测实验正由另一个进程执行: {root}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(
            f"pid={os.getpid()} acquired_at={datetime.now().isoformat()}\n"
        )
        handle.flush()
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def locked_experiment_stage(func: F) -> F:
    @wraps(func)
    def wrapper(experiment, *args, **kwargs):
        with experiment_lock(experiment.root):
            return func(experiment, *args, **kwargs)

    return wrapper  # type: ignore[return-value]


__all__ = ["experiment_lock", "locked_experiment_stage"]
