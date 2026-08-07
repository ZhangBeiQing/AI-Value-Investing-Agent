"""Custom exceptions for the shared data access layer."""

from __future__ import annotations


class DataValidationError(ValueError):
    """Raised when inbound parameters fail strict validation."""


class DataUnavailableError(RuntimeError):
    """Raised when upstream sources cannot provide reliable data."""


class SymbolNotListedAsOfDateError(DataUnavailableError):
    """Raised when cached trading history starts after the requested date."""

    def __init__(
        self,
        *,
        symbol: str,
        as_of_date: str,
        first_trading_date: str,
    ) -> None:
        self.symbol = symbol
        self.as_of_date = as_of_date
        self.first_trading_date = first_trading_date
        super().__init__(
            f"{symbol} 在 {as_of_date} 尚无上市交易记录；"
            f"缓存首个交易日为 {first_trading_date}"
        )


class CacheIntegrityError(RuntimeError):
    """Raised when cached artifacts are corrupt or incomplete."""


__all__ = [
    "DataValidationError",
    "DataUnavailableError",
    "SymbolNotListedAsOfDateError",
    "CacheIntegrityError",
]
