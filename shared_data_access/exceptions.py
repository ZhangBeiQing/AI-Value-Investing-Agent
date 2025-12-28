"""Custom exceptions for the shared data access layer."""

from __future__ import annotations


class DataValidationError(ValueError):
    """Raised when inbound parameters fail strict validation."""


class DataUnavailableError(RuntimeError):
    """Raised when upstream sources cannot provide reliable data."""


class CacheIntegrityError(RuntimeError):
    """Raised when cached artifacts are corrupt or incomplete."""


__all__ = [
    "DataValidationError",
    "DataUnavailableError",
    "CacheIntegrityError",
]
