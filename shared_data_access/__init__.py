"""Unified data access and indicator interfaces for the AI-Trader project.

This package exposes two primary entrypoints:

* :class:`SharedDataAccess` – canonical service layer for loading/caching
  price, financial, and share-structure data with consistent validation.
* :class:`IndicatorLibrary` – reusable collection of performance/valuation
  computations shared by the analysis modules.

Both abstractions enforce the formatting and data-quality guarantees outlined
in ``docs/PROJECT_SYSTEM_SUMMARY.md`` and should be preferred over ad-hoc helpers
inside individual tools.
"""

from .data_access import SharedDataAccess
from .indicator_library import IndicatorLibrary
from .cache_registry import (
    update_chip_distribution_cached,
    update_cn_profit_forecast_cached,
    update_hk_profit_forecast_cached,
)
from .board_metrics import build_board_quant_snapshot, update_board_history_ths_cached
from .chip_distribution import (
    build_chip_distribution_from_price_csv,
    build_chip_distribution_from_price_frame,
)
from .macro_objective_panel import (
    load_macro_objective_panel,
    load_or_build_macro_objective_panel,
    render_macro_objective_panel_markdown,
)
from .industry_financial_panel import (
    build_industry_financial_snapshot,
    completed_report_periods,
    load_industry_financial_snapshot_cached,
    update_industry_financial_panel_cached,
)
from .industry_catalog import (
    load_industry_catalog_cached,
    update_industry_catalog_cached,
)

__all__ = [
    "SharedDataAccess",
    "IndicatorLibrary",
    "build_board_quant_snapshot",
    "build_chip_distribution_from_price_csv",
    "build_chip_distribution_from_price_frame",
    "build_industry_financial_snapshot",
    "completed_report_periods",
    "load_industry_financial_snapshot_cached",
    "load_industry_catalog_cached",
    "load_macro_objective_panel",
    "load_or_build_macro_objective_panel",
    "render_macro_objective_panel_markdown",
    "update_board_history_ths_cached",
    "update_chip_distribution_cached",
    "update_cn_profit_forecast_cached",
    "update_hk_profit_forecast_cached",
    "update_industry_financial_panel_cached",
    "update_industry_catalog_cached",
]
