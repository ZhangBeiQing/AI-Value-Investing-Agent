"""Selection system foundation services."""

from .announcement_summary import build_recent_company_announcements
from .bootstrap import initialize_selection_system
from .board_heat import build_board_heat_state
from .candidate_selection import build_candidate_pools, merge_candidate_pools
from .master_universe import (
    build_master_universe_from_stock_pool,
    initialize_master_universe,
    load_master_universe,
    save_master_universe,
)
from .models import MasterUniverseDocument, MasterUniverseStock
from .paths import SelectionSystemPaths, resolve_selection_base_dir
from .factor_history import build_factor_history
from .factor_scoring import build_factor_scores_for_date
from .quant_prefilter import backtest_quant_prefilter, build_quant_prefilter_for_date
from .shared_context import build_shared_selection_context

__all__ = [
    "MasterUniverseDocument",
    "MasterUniverseStock",
    "SelectionSystemPaths",
    "build_recent_company_announcements",
    "build_board_heat_state",
    "build_candidate_pools",
    "build_master_universe_from_stock_pool",
    "build_factor_scores_for_date",
    "build_factor_history",
    "build_quant_prefilter_for_date",
    "build_shared_selection_context",
    "backtest_quant_prefilter",
    "merge_candidate_pools",
    "initialize_master_universe",
    "initialize_selection_system",
    "load_master_universe",
    "resolve_selection_base_dir",
    "save_master_universe",
]
