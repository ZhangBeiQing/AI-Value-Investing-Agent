"""Scoped cache-only reads for nested historical research calls."""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


_CACHE_ONLY: ContextVar[bool] = ContextVar("shared_data_cache_only", default=False)


def is_cached_data_only() -> bool:
    return _CACHE_ONLY.get()


@contextmanager
def cached_data_only() -> Iterator[None]:
    """Disable implicit fetching within this worker, restoring policy on exit."""
    token = _CACHE_ONLY.set(True)
    try:
        yield
    finally:
        _CACHE_ONLY.reset(token)
