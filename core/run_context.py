"""Explicit path context for live and isolated backtest runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
DEFAULT_BACKTESTS_ROOT = DEFAULT_DATA_ROOT / "backtests" / "fixed_tracked"


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


@dataclass(frozen=True)
class RunContext:
    """Separate shared source caches from mutable run outputs."""

    mode: Literal["live", "backtest"]
    source_data_root: Path
    run_output_root: Path
    skill_runs_root: Path
    selection_runs_root: Path
    agent_data_root: Path
    research_cache_root: Path
    macro_output_root: Path
    experiment_id: str = ""
    knowledge_cutoff: str = ""

    @classmethod
    def live(cls, data_root: str | Path = DEFAULT_DATA_ROOT) -> "RunContext":
        root = Path(data_root).resolve()
        return cls(
            mode="live",
            source_data_root=root,
            run_output_root=root,
            skill_runs_root=root / "skill_runs",
            selection_runs_root=root / "selection_runs",
            agent_data_root=root / "agent_data",
            research_cache_root=root / "research_artifact_cache",
            macro_output_root=root / "macro_economy",
        )

    @classmethod
    def backtest(
        cls,
        experiment_root: str | Path,
        *,
        source_data_root: str | Path = DEFAULT_DATA_ROOT,
        allowed_backtests_root: str | Path = DEFAULT_BACKTESTS_ROOT,
        knowledge_cutoff: str = "",
    ) -> "RunContext":
        root = Path(experiment_root).resolve()
        allowed_root = Path(allowed_backtests_root).resolve()
        source_root = Path(source_data_root).resolve()
        if root == allowed_root or not _is_relative_to(root, allowed_root):
            raise ValueError(
                f"回测目录必须是 {allowed_root} 下的独立实验子目录: {root}"
            )
        agent_root = root / "agent_data"
        live_agent_root = source_root / "agent_data"
        if agent_root.resolve() == live_agent_root.resolve():
            raise ValueError("回测 agent_data 不得指向正式 data/agent_data")
        return cls(
            mode="backtest",
            source_data_root=source_root,
            run_output_root=root,
            skill_runs_root=root / "skill_runs",
            selection_runs_root=root / "selection_runs",
            agent_data_root=agent_root,
            research_cache_root=root / "research_artifact_cache",
            macro_output_root=root / "macro_economy",
            experiment_id=root.name,
            knowledge_cutoff=knowledge_cutoff,
        )

    def with_cutoff(self, knowledge_cutoff: str) -> "RunContext":
        return RunContext(
            mode=self.mode,
            source_data_root=self.source_data_root,
            run_output_root=self.run_output_root,
            skill_runs_root=self.skill_runs_root,
            selection_runs_root=self.selection_runs_root,
            agent_data_root=self.agent_data_root,
            research_cache_root=self.research_cache_root,
            macro_output_root=self.macro_output_root,
            experiment_id=self.experiment_id,
            knowledge_cutoff=knowledge_cutoff,
        )

    def ensure_output_dirs(self) -> None:
        for path in (
            self.run_output_root,
            self.skill_runs_root,
            self.selection_runs_root,
            self.agent_data_root,
            self.research_cache_root,
            self.macro_output_root,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def as_dict(self) -> dict[str, str]:
        return {
            "mode": self.mode,
            "source_data_root": str(self.source_data_root),
            "run_output_root": str(self.run_output_root),
            "skill_runs_root": str(self.skill_runs_root),
            "selection_runs_root": str(self.selection_runs_root),
            "agent_data_root": str(self.agent_data_root),
            "research_cache_root": str(self.research_cache_root),
            "macro_output_root": str(self.macro_output_root),
            "experiment_id": self.experiment_id,
            "knowledge_cutoff": self.knowledge_cutoff,
        }


__all__ = [
    "DEFAULT_BACKTESTS_ROOT",
    "DEFAULT_DATA_ROOT",
    "RunContext",
]
