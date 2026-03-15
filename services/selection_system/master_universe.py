"""Master universe repository helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from configs.stock_pool import TRACKED_A_STOCKS

from .models import MasterUniverseDocument, MasterUniverseStock
from .paths import SelectionSystemPaths

BootstrapMode = Literal["empty", "stock_pool"]


def build_master_universe_from_stock_pool() -> MasterUniverseDocument:
    stocks = [
        MasterUniverseStock(
            symbol=entry.symbol,
            name=entry.name,
            sector="",
            industry="",
        )
        for entry in TRACKED_A_STOCKS
    ]
    return MasterUniverseDocument(
        stocks=stocks,
        description="由 configs.stock_pool.TRACKED_A_STOCKS 初始化的一期主股票宇宙",
    )


def load_master_universe(paths: SelectionSystemPaths | None = None) -> MasterUniverseDocument:
    resolved_paths = paths or SelectionSystemPaths.from_base_dir()
    target = resolved_paths.master_universe_path
    if not target.exists():
        raise FileNotFoundError(f"master_universe 文件不存在: {target}")
    payload = json.loads(target.read_text(encoding="utf-8"))
    return MasterUniverseDocument.from_dict(payload)


def save_master_universe(
    document: MasterUniverseDocument,
    paths: SelectionSystemPaths | None = None,
) -> Path:
    resolved_paths = paths or SelectionSystemPaths.from_base_dir()
    resolved_paths.ensure_directories()
    target = resolved_paths.master_universe_path
    normalized_document = document.with_updated_timestamp()
    target.write_text(
        json.dumps(normalized_document.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return target


def initialize_master_universe(
    paths: SelectionSystemPaths | None = None,
    *,
    bootstrap_mode: BootstrapMode = "stock_pool",
    force: bool = False,
) -> Path:
    resolved_paths = paths or SelectionSystemPaths.from_base_dir()
    target = resolved_paths.master_universe_path
    if target.exists() and not force:
        return target

    if bootstrap_mode == "stock_pool":
        document = build_master_universe_from_stock_pool()
    elif bootstrap_mode == "empty":
        document = MasterUniverseDocument.empty()
    else:
        raise ValueError(f"不支持的 bootstrap_mode: {bootstrap_mode}")

    return save_master_universe(document, resolved_paths)
