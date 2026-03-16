"""Orchestration for the stage-1 selection-system daily run."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from core.logging import get_logger

from .bootstrap import initialize_selection_system
from .candidates import build_candidate_bundles
from .master_universe import load_master_universe
from .news_pipeline import (
    build_news_items,
    collect_raw_news_items,
    load_recent_news_items,
    persist_news_items,
    persist_raw_news,
    rebuild_symbol_hot_state,
    rebuild_theme_state,
)
from .paths import SelectionSystemPaths
from .snapshots import build_universe_snapshot, persist_snapshot, snapshot_map
from .store import save_json_file
from .symbol_memory import build_symbol_memory_payload, persist_symbol_memories


LOGGER = get_logger("SelectionPipeline")


def run_selection_pipeline(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    include_live_feeds: bool = True,
    top_hot: int = 12,
    top_core: int = 12,
    max_workers: int = 6,
    news_lookback_days: int = 45,
    signature: str = "",
    cache_only: bool = False,
) -> Path:
    paths = initialize_selection_system(SelectionSystemPaths.from_base_dir(base_dir))
    run_dir = paths.ensure_run_dir(run_date)
    manifest: Dict[str, Any] = {
        "schema_version": 1,
        "run_date": run_date,
        "status": "running",
        "updated_at": datetime.now().isoformat(),
        "steps": {},
    }
    save_json_file(paths.run_manifest_path(run_date), manifest)

    def start_step(name: str) -> Dict[str, Any]:
        step = {"status": "running", "started_at": datetime.now().isoformat()}
        manifest["steps"][name] = step
        save_json_file(paths.run_manifest_path(run_date), manifest)
        return step

    def finish_step(step: Dict[str, Any], *, extra: Dict[str, Any] | None = None) -> None:
        step["status"] = "done"
        step["ended_at"] = datetime.now().isoformat()
        if extra:
            step.update(extra)
        manifest["updated_at"] = datetime.now().isoformat()
        save_json_file(paths.run_manifest_path(run_date), manifest)

    try:
        universe = load_master_universe(paths)

        step = start_step("step1_collect_raw_news")
        raw_items = collect_raw_news_items(
            universe,
            run_date,
            include_live_feeds=include_live_feeds,
            lookback_days=news_lookback_days,
        )
        persist_raw_news(paths.run_raw_news_path(run_date), run_date, raw_items)
        persist_raw_news(paths.raw_news_daily_path(run_date), run_date, raw_items)
        _append_raw_news_manifest(paths, run_date, raw_items_count=len(raw_items))
        finish_step(step, extra={"items": len(raw_items)})

        step = start_step("step2_build_news_items")
        news_items = build_news_items(raw_items, universe)
        persist_news_items(paths.run_news_items_path(run_date), run_date, news_items)
        finish_step(step, extra={"items": len(news_items)})

        step = start_step("step3_build_snapshot")
        snapshot_payload = build_universe_snapshot(
            universe,
            run_date,
            base_dir=base_dir,
            max_workers=max_workers,
            cache_only=cache_only,
        )
        persist_snapshot(paths.run_snapshot_path(run_date), snapshot_payload)
        finish_step(step, extra={"symbols": len(snapshot_payload.get("symbols", []))})

        step = start_step("step4_rebuild_theme_state")
        recent_news = load_recent_news_items(paths, run_date, lookback_days=news_lookback_days)
        theme_state = rebuild_theme_state(recent_news, run_date)
        save_json_file(paths.run_theme_state_path(run_date), theme_state)
        save_json_file(paths.theme_state_path, theme_state)
        finish_step(step, extra={"themes": len(theme_state.get("themes", []))})

        step = start_step("step5_rebuild_symbol_hot_state")
        symbol_hot_state = rebuild_symbol_hot_state(
            universe,
            recent_news,
            theme_state,
            snapshot_map(snapshot_payload),
            run_date,
        )
        save_json_file(paths.run_symbol_hot_state_path(run_date), symbol_hot_state)
        save_json_file(paths.symbol_hot_state_path, symbol_hot_state)
        finish_step(step, extra={"symbols": len(symbol_hot_state.get("symbols", []))})

        step = start_step("step6_select_candidates")
        bundles = build_candidate_bundles(
            universe,
            snapshot_map(snapshot_payload),
            symbol_hot_state,
            run_date,
            top_hot=top_hot,
            top_core=top_core,
            signature=signature,
        )
        save_json_file(paths.run_hot_candidates_path(run_date), bundles["hot"])
        save_json_file(paths.run_core_candidates_path(run_date), bundles["core"])
        save_json_file(paths.runtime_hot_pool_path, bundles["hot"])
        save_json_file(paths.runtime_core_pool_path, bundles["core"])
        save_json_file(
            paths.runtime_holdings_guardrail_path,
            {
                "schema_version": 1,
                "updated_at": datetime.now().isoformat(),
                "symbols": bundles["hot"].get("holding_guardrail", []),
            },
        )
        finish_step(
            step,
            extra={
                "hot_candidates": len(bundles["hot"].get("candidates", [])),
                "core_candidates": len(bundles["core"].get("candidates", [])),
            },
        )

        step = start_step("step7_build_symbol_memory")
        symbol_memory = build_symbol_memory_payload(bundles["hot"], bundles["core"], run_date)
        save_json_file(paths.run_symbol_memory_path(run_date), symbol_memory)
        persist_symbol_memories(paths.symbol_memory_dir, symbol_memory)
        finish_step(step, extra={"symbols": len(symbol_memory.get("symbols", []))})

        manifest["status"] = "done"
        manifest["updated_at"] = datetime.now().isoformat()
        save_json_file(paths.run_manifest_path(run_date), manifest)
        _append_run_manifest(paths, run_date, status="done")
        LOGGER.info("selection pipeline 完成: run_dir=%s", run_dir)
        return run_dir
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["updated_at"] = datetime.now().isoformat()
        running = [name for name, step in manifest["steps"].items() if step.get("status") == "running"]
        if running:
            current = manifest["steps"][running[-1]]
            current["status"] = "failed"
            current["ended_at"] = datetime.now().isoformat()
            current["error"] = str(exc)
        save_json_file(paths.run_manifest_path(run_date), manifest)
        _append_run_manifest(paths, run_date, status="failed")
        raise


def _append_run_manifest(paths: SelectionSystemPaths, run_date: str, *, status: str) -> None:
    from .store import load_json_file

    current = load_json_file(
        paths.selection_runs_manifest_path,
        default={
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "runs": [],
        },
    )
    runs = current.get("runs", []) if isinstance(current, dict) else []
    runs = [run for run in runs if run.get("run_date") != run_date]
    runs.append(
        {
            "run_date": run_date,
            "status": status,
            "run_dir": str(paths.run_dir(run_date)),
            "updated_at": datetime.now().isoformat(),
        }
    )
    runs.sort(key=lambda item: item.get("run_date", ""), reverse=True)
    save_json_file(
        paths.selection_runs_manifest_path,
        {
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "runs": runs,
        },
    )


def _append_raw_news_manifest(paths: SelectionSystemPaths, run_date: str, *, raw_items_count: int) -> None:
    from .store import load_json_file

    current = load_json_file(
        paths.raw_news_manifest_path,
        default={
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "items": [],
        },
    )
    items = current.get("items", []) if isinstance(current, dict) else []
    items = [entry for entry in items if entry.get("run_date") != run_date]
    items.append(
        {
            "run_date": run_date,
            "path": str(paths.raw_news_daily_path(run_date)),
            "items_count": raw_items_count,
            "updated_at": datetime.now().isoformat(),
        }
    )
    items.sort(key=lambda item: item.get("run_date", ""), reverse=True)
    save_json_file(
        paths.raw_news_manifest_path,
        {
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "items": items,
        },
    )
