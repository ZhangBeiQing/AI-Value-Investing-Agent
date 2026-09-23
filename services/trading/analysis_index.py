"""Persistent deep-analysis index shared by CLI and dashboard publishers."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from core.logging import get_logger


LOGGER = get_logger("AnalysisIndex")


def _snapshot_price_map(skill_runs_root: Path, book_type: str, run_date: str) -> dict[str, float]:
    snapshot_path = skill_runs_root / run_date / book_type / "02_basic_snapshot_payload.json"
    if not snapshot_path.is_file():
        return {}
    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    stocks = payload.get("stocks", {}) or {}
    return {
        symbol: float(fields["latest_price"])
        for symbol, fields in stocks.items()
        if isinstance(fields, dict) and isinstance(fields.get("latest_price"), (int, float))
    }


def update_analysis_index(
    decision: dict[str, Any],
    book_type: str,
    run_date: str,
    skill_runs_root: Path,
    *,
    snapshot_root: Path | None = None,
) -> None:
    """Update the latest deep-analysis metadata for every decision entry.

    ``skill_runs_root`` owns ``_analysis_index.json``. Dashboard jobs may pass
    their isolated ``skill_runs`` directory as ``snapshot_root`` so the index
    uses the exact single-stock analysis snapshot without publishing that
    temporary snapshot into the official daily directory.
    """
    index_path = skill_runs_root / "_analysis_index.json"
    if index_path.is_file():
        index = json.loads(index_path.read_text(encoding="utf-8"))
    else:
        index = {}
    book_index = index.setdefault(book_type, {})
    current_prices = _snapshot_price_map(snapshot_root or skill_runs_root, book_type, run_date)

    for entry in decision.get("stock_decisions", []):
        symbol = entry.get("symbol", "")
        if not symbol:
            continue
        existing_date = str((book_index.get(symbol) or {}).get("deep_analysis_date") or "")
        if existing_date > run_date:
            LOGGER.info(
                "跳过较旧分析索引更新: %s %s < %s", symbol, run_date, existing_date
            )
            continue
        book_index[symbol] = {
            "deep_analysis_date": run_date,
            "last_deep_analysis_price": current_prices.get(symbol),
            "price_impression": entry.get("price_impression", ""),
            "confidence_score": entry.get("confidence_score", 0),
            "sizing_reason": entry.get("sizing_reason", ""),
        }

    for symbol, entry in book_index.items():
        if entry.get("last_deep_analysis_price") is not None:
            continue
        historical_date = entry.get("deep_analysis_date")
        if historical_date:
            historical_price = _snapshot_price_map(
                skill_runs_root, book_type, historical_date
            ).get(symbol)
            if historical_price is not None:
                entry["last_deep_analysis_price"] = historical_price

    skill_runs_root.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=skill_runs_root, prefix=".analysis-index-", delete=False
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(index, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(temp_path, index_path)
        temp_path = None
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()
    LOGGER.info("分析索引已更新: %s (%s -> %s 只)", index_path, book_type, len(book_index))


__all__ = ["update_analysis_index"]
