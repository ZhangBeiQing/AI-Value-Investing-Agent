"""Daily skill pipeline orchestration service."""

from __future__ import annotations

import shutil
from pathlib import Path

from services.pipeline.steps.build_agent_input import write_agent_input_bundle
from services.pipeline.steps.build_global_context import write_global_context
from services.pipeline.steps.build_stock_research import write_stock_research_bundle
from services.pipeline.steps.refresh_data import run_refresh_data


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SKILL_FLOW_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "skill_flow.json"


def resolve_output_dir(base_dir: str, run_date: str) -> Path:
    return Path(base_dir) / "skill_runs" / run_date


def safe_clean_dir(target_dir: Path) -> None:
    if target_dir.exists():
        if target_dir.is_dir() and target_dir.parent.name == "skill_runs":
            shutil.rmtree(target_dir)
        else:
            raise ValueError(f"Refuse to clean unexpected path: {target_dir}")
    target_dir.mkdir(parents=True, exist_ok=True)


def run_daily_pipeline(
    run_date: str,
    *,
    base_dir: str = "data",
    prompt_config: str | Path | None = None,
    signature: str = "",
) -> Path:
    output_dir = resolve_output_dir(base_dir, run_date)
    safe_clean_dir(output_dir)

    resolved_prompt_config = Path(prompt_config) if prompt_config else SKILL_FLOW_CONFIG
    run_refresh_data(run_date, signature=signature)
    write_global_context(run_date, output_dir)
    write_stock_research_bundle(run_date, output_dir)
    write_agent_input_bundle(
        run_date,
        output_dir,
        signature=signature,
        prompt_config=resolved_prompt_config,
    )
    return output_dir
