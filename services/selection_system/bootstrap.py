"""Bootstrap helpers for the selection system foundation."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .master_universe import BootstrapMode, initialize_master_universe
from .paths import SelectionSystemPaths


def _write_json_if_needed(path: Path, payload: Dict[str, Any], *, force: bool) -> None:
    if path.exists() and not force:
        return
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def initialize_selection_system(
    paths: SelectionSystemPaths | None = None,
    *,
    bootstrap_mode: BootstrapMode = "stock_pool",
    force: bool = False,
) -> SelectionSystemPaths:
    resolved_paths = paths or SelectionSystemPaths.from_base_dir()
    resolved_paths.ensure_directories()

    initialize_master_universe(
        resolved_paths,
        bootstrap_mode=bootstrap_mode,
        force=force,
    )

    now = datetime.now().isoformat()
    _write_json_if_needed(
        resolved_paths.raw_news_manifest_path,
        {
            "schema_version": 1,
            "updated_at": now,
            "items": [],
        },
        force=force,
    )
    _write_json_if_needed(
        resolved_paths.theme_state_path,
        {
            "schema_version": 1,
            "updated_at": now,
            "themes": [],
        },
        force=force,
    )
    _write_json_if_needed(
        resolved_paths.symbol_hot_state_path,
        {
            "schema_version": 1,
            "updated_at": now,
            "symbols": [],
        },
        force=force,
    )
    _write_json_if_needed(
        resolved_paths.symbol_memory_index_path,
        {
            "schema_version": 1,
            "updated_at": now,
            "symbols": [],
        },
        force=force,
    )
    _write_json_if_needed(
        resolved_paths.selection_runs_manifest_path,
        {
            "schema_version": 1,
            "updated_at": now,
            "runs": [],
        },
        force=force,
    )
    return resolved_paths
