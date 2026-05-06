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
    def board_signals_dir(self) -> Path:
        return self.market_state_dir / "board_signals"

    @property
    def board_signals_manifest_path(self) -> Path:
        return self.board_signals_dir / "manifest.json"

    @property
    def board_heat_state_dir(self) -> Path:
        return self.market_state_dir / "board_heat_state"

    @property
    def board_heat_state_manifest_path(self) -> Path:
        return self.board_heat_state_dir / "manifest.json"

    @property
    def board_heat_state_latest_path(self) -> Path:
        return self.board_heat_state_dir / "latest.json"

    @property
    def symbol_memory_dir(self) -> Path:
        return self.base_dir / "symbol_memory"

    @property
    def selection_runs_dir(self) -> Path:
        return self.base_dir / "selection_runs"

    def run_dir(self, run_date: str) -> Path:
        return self.selection_runs_dir / run_date

    def run_manifest_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "run_manifest.json"

    def run_news_candidates_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "01_news_candidates.json"

    def run_news_dedup_decisions_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "02_news_dedup_decisions.json"

    def run_news_deduped_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "02_news_deduped.json"

    def run_news_enriched_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "03_news_enriched.json"

    def run_news_prompt_input_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "03_news_prompt_input.json"

    def run_recent_company_announcements_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "04_recent_company_announcements.json"

    def run_board_signals_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "04_board_signals.json"

    def board_signals_daily_path(self, run_date: str) -> Path:
        return self.board_signals_dir / f"{run_date}.json"

    def run_board_candidates_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "04_board_candidates.json"

    def run_board_heat_state_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "05_board_heat_state.json"

    def run_board_heat_digest_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "05_board_heat_digest.json"

    def run_hot_news_state_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "06_hot_news_state.json"

    def run_shared_selection_context_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "07_shared_selection_context.md"

    def run_short_book_candidates_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "08_short_book_candidates.json"

    def run_short_book_input_markdown_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "08_short_book_input.md"

    def run_short_book_input_payload_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "08_short_book_input.json"

    def run_long_book_candidates_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "09_long_book_candidates.json"

    def run_long_book_input_markdown_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "09_long_book_input.md"

    def run_long_book_input_payload_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "09_long_book_input.json"

    def run_candidate_merge_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "10_candidate_merge.json"

    def run_deep_research_queue_path(self, run_date: str) -> Path:
        return self.run_dir(run_date) / "11_deep_research_queue.json"

    def board_heat_state_daily_path(self, run_date: str) -> Path:
        return self.board_heat_state_dir / f"{run_date}.json"

    def ensure_directories(self) -> None:
        for path in (
            self.universe_dir,
            self.market_state_dir,
            self.board_signals_dir,
            self.board_heat_state_dir,
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
