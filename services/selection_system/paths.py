"""Filesystem layout helpers for the selection system."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


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

    def ensure_directories(self) -> None:
        for path in (
            self.universe_dir,
            self.market_state_dir,
            self.raw_news_dir,
            self.symbol_memory_dir,
            self.selection_runs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
