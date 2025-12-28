"""Compatibility shim that re-exports the shared indicator library package."""

from indicator_library import (
    IndicatorLibrary,
    IndicatorBatchRequest,
    IndicatorBatchResult,
    IndicatorSpec,
    IndicatorCalculationError,
)
from indicator_library.library import ReturnMetrics

__all__ = [
    "IndicatorLibrary",
    "IndicatorBatchRequest",
    "IndicatorBatchResult",
    "IndicatorSpec",
    "IndicatorCalculationError",
    "ReturnMetrics",
]
