"""Filesystem layout helpers for the selection system."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SELECTION_BASE_DIR = PROJECT_ROOT / "data"


def resolve_selection_base_dir(base_dir: str | Path | None = None) -> Path:
    """Resolve the selection-system base directory relative to the repo root."""

    if base_dir is None:
        return DEFAULT_SELECTION_BASE_DIR

    candidate = Path(base_dir)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate


@dataclass(frozen=True)
class SelectionSystemPaths:
    """Canonical directories used by the selection system foundation."""

    base_dir: Path

    @classmethod
    def from_base_dir(cls, base_dir: str | Path | None = None) -> "SelectionSystemPaths":
        return cls(base_dir=resolve_selection_base_dir(base_dir))

    @property
    def universe_dir(self) -> Path:
        return self.base_dir / "universe"

    @property
    def master_universe_path(self) -> Path:
        return self.universe_dir / "master_universe.json"

    @property
    def market_state_dir(self) -> Path:
        return self.base_dir / "market_state"

    @property
    def raw_news_dir(self) -> Path:
        return self.market_state_dir / "raw_news"

    @property
    def raw_news_manifest_path(self) -> Path:
        return self.raw_news_dir / "raw_news_manifest.json"

    @property
    def theme_state_path(self) -> Path:
        return self.market_state_dir / "theme_state.json"

    @property
    def symbol_hot_state_path(self) -> Path:
        return self.market_state_dir / "symbol_hot_state.json"

    @property
    def runtime_hot_pool_path(self) -> Path:
        return self.market_state_dir / "runtime_hot_pool.json"

    @property
    def runtime_core_pool_path(self) -> Path:
        return self.market_state_dir / "runtime_core_pool.json"

    @property
    def runtime_holdings_guardrail_path(self) -> Path:
        return self.market_state_dir / "runtime_holdings_guardrail.json"

    @property
    def symbol_memory_dir(self) -> Path:
        return self.base_dir / "symbol_memory"

    @property
    def symbol_memory_index_path(self) -> Path:
        return self.symbol_memory_dir / "index.json"

    @property
    def selection_runs_dir(self) -> Path:
        return self.base_dir / "selection_runs"

    @property
    def selection_runs_manifest_path(self) -> Path:
        return self.selection_runs_dir / "manifest.json"

    def run_dir(self, run_date: str) -> Path:
        return self.selection_runs_dir / run_date

    def run_manifest_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "run_manifest.json"

    def run_raw_news_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "01_raw_news_items.json"

    def raw_news_daily_path(self, run_date: str) -> Path:
        return self.raw_news_dir / f"{run_date}.json"

    def run_news_items_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "02_news_items.json"

    def run_snapshot_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "03_simplified_snapshot.json"

    def run_theme_state_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "04_theme_state.json"

    def run_symbol_hot_state_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "05_symbol_hot_state.json"

    def run_hot_candidates_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "06_hot_candidates.json"

    def run_core_candidates_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "07_core_candidates.json"

    def run_symbol_memory_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "08_symbol_memory.json"

    def ensure_directories(self) -> None:
        for path in (
            self.universe_dir,
            self.market_state_dir,
            self.raw_news_dir,
            self.symbol_memory_dir,
            self.selection_runs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def ensure_run_dir(self, run_date: str) -> Path:
        run_dir = self.run_dir(run_date)
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def iter_run_dirs(self) -> Iterable[Path]:
        if not self.selection_runs_dir.exists():
            return []
        return sorted(
            (path for path in self.selection_runs_dir.iterdir() if path.is_dir()),
            key=lambda path: path.name,
        )
