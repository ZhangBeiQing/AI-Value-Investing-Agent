"""Daily skill pipeline orchestration service."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

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


def write_manifest(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _ordered_timestamp(reference: str | None = None) -> str:
    current = datetime.now()
    if not reference:
        return current.isoformat()

    try:
        ref_dt = datetime.fromisoformat(reference)
    except ValueError:
        return current.isoformat()

    if current <= ref_dt:
        current = ref_dt + timedelta(microseconds=1)
    return current.isoformat()


def _latest_manifest_timestamp(manifest: Dict[str, object]) -> str | None:
    latest: str | None = None
    for info in manifest.get("steps", {}).values():
        if not isinstance(info, dict):
            continue
        for key in ("ended_at", "started_at"):
            value = info.get(key)
            if isinstance(value, str) and value and (latest is None or value > latest):
                latest = value
    return latest


def _mark_step_running(manifest: Dict[str, object], name: str, output_dir: Path) -> Dict[str, object]:
    step_info = {
        "status": "running",
        "started_at": _ordered_timestamp(_latest_manifest_timestamp(manifest)),
    }
    manifest["steps"][name] = step_info
    write_manifest(output_dir / "run_manifest.json", manifest)
    return step_info


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
    manifest: Dict[str, object] = {
        "run_date": run_date,
        "output_dir": str(output_dir),
        "status": "running",
        "steps": {},
        "prompt_config": str(resolved_prompt_config),
    }
    write_manifest(output_dir / "run_manifest.json", manifest)

    try:
        step_info = _mark_step_running(manifest, "step1_refresh_data", output_dir)
        run_refresh_data(run_date, signature=signature)
        step_info["status"] = "done"
        step_info["ended_at"] = _ordered_timestamp(step_info["started_at"])
        write_manifest(output_dir / "run_manifest.json", manifest)

        step_info = _mark_step_running(manifest, "step2_build_global_context", output_dir)
        write_global_context(run_date, output_dir)
        step_info["status"] = "done"
        step_info["ended_at"] = _ordered_timestamp(step_info["started_at"])
        write_manifest(output_dir / "run_manifest.json", manifest)

        step_info = _mark_step_running(manifest, "step3_build_stock_snapshots", output_dir)
        write_stock_research_bundle(run_date, output_dir)
        step_info["status"] = "done"
        step_info["ended_at"] = _ordered_timestamp(step_info["started_at"])
        write_manifest(output_dir / "run_manifest.json", manifest)

        step_info = _mark_step_running(manifest, "step4_build_agent_input", output_dir)
        write_agent_input_bundle(
            run_date,
            output_dir,
            signature=signature,
            prompt_config=resolved_prompt_config,
        )
        step_info["status"] = "done"
        step_info["ended_at"] = _ordered_timestamp(step_info["started_at"])
        manifest["status"] = "done"
        write_manifest(output_dir / "run_manifest.json", manifest)
        return output_dir
    except Exception as exc:
        manifest["status"] = "failed"
        failed_steps = [name for name, info in manifest["steps"].items() if info.get("status") == "running"]
        if failed_steps:
            current = manifest["steps"][failed_steps[-1]]
            current["status"] = "failed"
            current["error"] = str(exc)
            current["ended_at"] = _ordered_timestamp(current.get("started_at"))
        write_manifest(output_dir / "run_manifest.json", manifest)
        raise
