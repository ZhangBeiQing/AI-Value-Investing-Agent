"""Unified data access interface for the AI-Trader project.

This package exposes the shared data access entrypoint:

* :class:`SharedDataAccess` – canonical service layer for loading/caching
  price, financial, and share-structure data with consistent validation.

It enforces the formatting and data-quality guarantees outlined in
``docs/PROJECT_SYSTEM_SUMMARY.md`` and should be preferred over ad-hoc helpers
inside individual tools. Indicator computations live in the top-level
``indicator_library`` package.
"""

from .data_access import SharedDataAccess
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
from .market_calendar import (
    NonTradingDayError,
    ensure_market_session,
    inspect_market_session,
    market_sessions_between,
)

__all__ = [
    "SharedDataAccess",
    "build_board_quant_snapshot",
    "build_chip_distribution_from_price_csv",
    "build_chip_distribution_from_price_frame",
    "build_industry_financial_snapshot",
    "completed_report_periods",
    "load_industry_financial_snapshot_cached",
    "load_industry_catalog_cached",
    "load_macro_objective_panel",
    "NonTradingDayError",
    "ensure_market_session",
    "inspect_market_session",
    "market_sessions_between",
    "load_or_build_macro_objective_panel",
    "render_macro_objective_panel_markdown",
    "update_board_history_ths_cached",
    "update_chip_distribution_cached",
    "update_cn_profit_forecast_cached",
    "update_hk_profit_forecast_cached",
    "update_industry_financial_panel_cached",
    "update_industry_catalog_cached",
]
