"""Unified data access and indicator interfaces for the AI-Trader project.

This package exposes two primary entrypoints:

* :class:`SharedDataAccess` – canonical service layer for loading/caching
  price, financial, and share-structure data with consistent validation.
* :class:`IndicatorLibrary` – reusable collection of performance/valuation
  computations shared by the analysis modules.

Both abstractions enforce the formatting and data-quality guarantees outlined
in ``PROJECT_SYSTEM_SUMMARY.md`` and should be preferred over ad-hoc helpers
inside individual tools.
"""

from .data_access import SharedDataAccess
from .indicator_library import IndicatorLibrary

__all__ = ["SharedDataAccess", "IndicatorLibrary"]
