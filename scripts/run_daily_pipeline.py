#!/usr/bin/env python3
"""Run the daily skill pipeline (steps 1-4)."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SKILL_FLOW_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "skill_flow.json"


def _resolve_output_dir(base_dir: str, run_date: str) -> Path:
    return Path(base_dir) / "skill_runs" / run_date


def _safe_clean_dir(target_dir: Path) -> None:
    if target_dir.exists():
        if target_dir.is_dir() and target_dir.parent.name == "skill_runs":
            shutil.rmtree(target_dir)
        else:
            raise ValueError(f"Refuse to clean unexpected path: {target_dir}")
    target_dir.mkdir(parents=True, exist_ok=True)


def _write_manifest(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_step(
    name: str,
    script: Path,
    run_date: str,
    output_dir: Path,
    extra_args: list[str] | None,
    env: dict[str, str],
    manifest: Dict[str, object],
) -> None:
    step_info = {
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "command": [sys.executable, str(script), "--date", run_date, "--output-dir", str(output_dir)],
    }
    if extra_args:
        step_info["command"].extend(extra_args)
    manifest["steps"][name] = step_info
    _write_manifest(output_dir / "run_manifest.json", manifest)

    try:
        subprocess.run(
            step_info["command"],
            check=True,
            cwd=str(PROJECT_ROOT),
            env=env,
        )
        step_info["status"] = "done"
    except subprocess.CalledProcessError as exc:
        step_info["status"] = "failed"
        step_info["error"] = f"exit_code={exc.returncode}"
        manifest["status"] = "failed"
        step_info["ended_at"] = datetime.now().isoformat()
        _write_manifest(output_dir / "run_manifest.json", manifest)
        raise SystemExit(exc.returncode) from exc

    step_info["ended_at"] = datetime.now().isoformat()
    _write_manifest(output_dir / "run_manifest.json", manifest)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily pipeline steps 1-4.")
    parser.add_argument(
        "--date",
        dest="run_date",
        default=date.today().strftime("%Y-%m-%d"),
        help="Run date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--base-dir",
        default="data",
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--prompt-config",
        default=str(SKILL_FLOW_CONFIG),
        help="Prompt flow config path (default: skill_flow.json).",
    )
    parser.add_argument(
        "--signature",
        default="",
        help="Agent signature used for historical context and trade summary files.",
    )
    args = parser.parse_args()

    output_dir = _resolve_output_dir(args.base_dir, args.run_date)
    _safe_clean_dir(output_dir)

    manifest: Dict[str, object] = {
        "run_date": args.run_date,
        "output_dir": str(output_dir),
        "status": "running",
        "steps": {},
        "prompt_config": str(args.prompt_config),
    }
    _write_manifest(output_dir / "run_manifest.json", manifest)

    env = os.environ.copy()
    env["PROMPT_FLOW_CONFIG"] = str(Path(args.prompt_config).resolve())
    if args.signature:
        env["SIGNATURE"] = args.signature

    _run_step(
        "step1_refresh_data",
        PROJECT_ROOT / "scripts" / "step1_refresh_data.py",
        args.run_date,
        output_dir,
        ["--signature", args.signature] if args.signature else [],
        env,
        manifest,
    )
    _run_step(
        "step2_build_global_context",
        PROJECT_ROOT / "scripts" / "step2_build_global_context.py",
        args.run_date,
        output_dir,
        ["--base-dir", args.base_dir, *(["--signature", args.signature] if args.signature else [])],
        env,
        manifest,
    )
    _run_step(
        "step3_build_stock_snapshots",
        PROJECT_ROOT / "scripts" / "step3_build_stock_snapshots.py",
        args.run_date,
        output_dir,
        ["--base-dir", args.base_dir, *(["--signature", args.signature] if args.signature else [])],
        env,
        manifest,
    )
    _run_step(
        "step4_build_agent_input",
        PROJECT_ROOT / "scripts" / "step4_build_agent_input.py",
        args.run_date,
        output_dir,
        ["--base-dir", args.base_dir, *(["--signature", args.signature] if args.signature else [])],
        env,
        manifest,
    )

    manifest["status"] = "done"
    _write_manifest(output_dir / "run_manifest.json", manifest)


if __name__ == "__main__":
    main()
