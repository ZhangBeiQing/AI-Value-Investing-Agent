#!/usr/bin/env python3
"""Merge the daily skill decision into persistent trade summary files."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path

import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.general_tools import get_config_value
from trade_summary import (
    get_portfolio_historical_context,
    initialize_data_files,
    process_and_merge_operations,
    save_daily_operations,
)


def _resolve_output_dir(base_dir: str, run_date: str) -> Path:
    return Path(base_dir) / "skill_runs" / run_date


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _update_manifest(output_dir: Path, step_name: str, status: str, extra: dict | None = None) -> None:
    manifest_path = output_dir / "run_manifest.json"
    if not manifest_path.exists():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return
    steps = manifest.setdefault("steps", {})
    step_info = steps.get(step_name, {})
    step_info.update({"status": status, "ended_at": datetime.now().isoformat()})
    if extra:
        step_info.update(extra)
    steps[step_name] = step_info
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge skill outputs into trade summary files")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--decision-file", default="")
    parser.add_argument("--signature", default="")
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else _resolve_output_dir(args.base_dir, args.run_date)
    decision_path = Path(args.decision_file) if args.decision_file else output_dir / "05_decision.json"
    if not decision_path.exists():
        raise SystemExit(f"Decision file not found: {decision_path}")

    decision = _load_json(decision_path)
    summary_date = decision.get("summary_date") or args.run_date
    signature = args.signature or os.environ.get("SIGNATURE") or get_config_value("SIGNATURE") or "deepseek-reasoner"

    initialize_data_files(signature)
    saved_operations = save_daily_operations(signature, decision)
    process_and_merge_operations(signature, saved_operations)

    daily_summary = {
        "summary_date": summary_date,
        "signature": signature,
        "decision_file": str(decision_path),
        "system_risk_notes": decision.get("system_risk_notes", []),
        "system_focus_items": decision.get("system_focus_items", []),
        "saved_operations_count": len(saved_operations),
    }
    (output_dir / "07_daily_summary.json").write_text(
        json.dumps(daily_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    stock_codes = [op.get("stock_code") for op in saved_operations if op.get("stock_code")]
    history_merge = {
        "summary_date": summary_date,
        "signature": signature,
        "saved_operations": saved_operations,
        "latest_portfolio_context": get_portfolio_historical_context(signature, stock_codes, n=2),
    }
    history_path = output_dir / "08_history_merge.json"
    history_path.write_text(json.dumps(history_merge, ensure_ascii=False, indent=2), encoding="utf-8")
    _update_manifest(output_dir, "merge_trade_summary", "done", {"history_file": str(history_path)})


if __name__ == "__main__":
    main()
