"""Filesystem layout for the independent industry-research layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


def resolve_data_dir(base_dir: str | Path | None = None) -> Path:
    if base_dir is None:
        return DEFAULT_DATA_DIR
    candidate = Path(base_dir)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate


@dataclass(frozen=True)
class IndustryResearchPaths:
    """Canonical paths owned by the industry-research layer."""

    base_dir: Path

    @classmethod
    def from_base_dir(cls, base_dir: str | Path | None = None) -> "IndustryResearchPaths":
        return cls(base_dir=resolve_data_dir(base_dir))

    @property
    def root_dir(self) -> Path:
        return self.base_dir / "industry_research"

    @property
    def radar_root_dir(self) -> Path:
        return self.root_dir / "radar"

    @property
    def structural_scans_root_dir(self) -> Path:
        return self.root_dir / "structural_scans"

    def structural_scan_dir(self, run_date: str) -> Path:
        return self.structural_scans_root_dir / run_date

    def structural_scan_input_path(self, run_date: str) -> Path:
        return self.structural_scan_dir(run_date) / "scan_input.json"

    def structural_scan_markdown_path(self, run_date: str) -> Path:
        return self.structural_scan_dir(run_date) / "scan_input.md"

    def structural_pool_template_path(self, run_date: str) -> Path:
        return self.structural_scan_dir(run_date) / "structural_opportunity_pool_template.json"

    def structural_pool_path(self, run_date: str) -> Path:
        return self.structural_scan_dir(run_date) / "structural_opportunity_pool.json"

    def radar_dir(self, run_date: str) -> Path:
        return self.radar_root_dir / run_date

    def radar_json_path(self, run_date: str) -> Path:
        return self.radar_dir(run_date) / "industry_radar.json"

    def agent_review_path(self, run_date: str) -> Path:
        return self.radar_dir(run_date) / "agent_review_input.md"

    @property
    def latest_radar_path(self) -> Path:
        return self.radar_root_dir / "latest.json"

    @property
    def workdirs_root_dir(self) -> Path:
        return self.root_dir / "workdirs"

    def theme_workdir(self, run_date: str, theme_id: str) -> Path:
        return self.workdirs_root_dir / run_date / theme_id

    @property
    def cards_root_dir(self) -> Path:
        return self.root_dir / "cards"

    def theme_card_dir(self, theme_id: str) -> Path:
        return self.cards_root_dir / theme_id

    def theme_indicator_history_path(self, theme_id: str) -> Path:
        return self.theme_card_dir(theme_id) / "indicator_history.jsonl"

    def ensure_radar_dir(self, run_date: str) -> Path:
        target = self.radar_dir(run_date)
        target.mkdir(parents=True, exist_ok=True)
        return target

    def ensure_structural_scan_dir(self, run_date: str) -> Path:
        target = self.structural_scan_dir(run_date)
        target.mkdir(parents=True, exist_ok=True)
        return target
