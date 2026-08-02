"""Cached Shenwan industry catalog used by semiannual structural scans."""

from __future__ import annotations

from datetime import datetime
import json
import math
from numbers import Real
from pathlib import Path
from typing import Any, Dict, Mapping

import akshare as ak
import pandas as pd

from core.logging import get_logger

from .cache_registry import (
    CacheKind,
    build_global_cache_dir,
    record_cache_refresh,
    should_refresh,
)


LOGGER = get_logger("IndustryCatalog")
SOURCE_NAMES = {
    "level1": "akshare.sw_index_first_info",
    "level2": "akshare.sw_index_second_info",
    "level3": "akshare.sw_index_third_info",
}
COMMON_COLUMN_MAP = {
    "行业代码": "industry_code",
    "行业名称": "industry_name",
    "上级行业": "parent_industry",
    "成份个数": "constituent_count",
    "静态市盈率": "pe_static",
    "TTM(滚动)市盈率": "pe_ttm",
    "市净率": "pb",
    "静态股息率": "dividend_yield_pct",
}


def update_industry_catalog_cached(
    *,
    base_dir: str | Path = "data",
    force_refresh: bool = False,
) -> Dict[str, Path]:
    """Refresh the complete SW catalog without applying historical cutoffs."""

    cache_dir = build_global_cache_dir(
        CacheKind.INDUSTRY_CATALOG_SW,
        base_dir=base_dir,
        ensure=True,
    )
    latest_path = cache_dir / "latest.json"
    if not should_refresh(cache_dir, CacheKind.INDUSTRY_CATALOG_SW, force=force_refresh):
        payload = _load_json(latest_path)
        snapshot_path = Path(str(payload.get("snapshot_path") or ""))
        if snapshot_path.exists():
            LOGGER.info("复用申万行业目录缓存: %s", snapshot_path)
            return {"industry_catalog": snapshot_path, "latest": latest_path}

    fetched_at = datetime.now().isoformat()
    levels = {
        "level1": _normalize_catalog_frame(ak.sw_index_first_info(), level="level1"),
        "level2": _normalize_catalog_frame(ak.sw_index_second_info(), level="level2"),
        "level3": _normalize_catalog_frame(ak.sw_index_third_info(), level="level3"),
    }
    snapshot_date = datetime.now().date().isoformat()
    payload = {
        "schema_version": 1,
        "snapshot_date": snapshot_date,
        "fetched_at": fetched_at,
        "sources": SOURCE_NAMES,
        "summary": {f"{level}_count": len(frame.index) for level, frame in levels.items()},
        "levels": {
            level: _records(frame)
            for level, frame in levels.items()
        },
    }
    snapshot_path = cache_dir / "snapshots" / f"{snapshot_date}.json"
    payload["snapshot_path"] = str(snapshot_path)
    _save_json(snapshot_path, payload)
    _save_json(latest_path, payload)
    record_cache_refresh(
        cache_dir,
        latest_snapshot_date=snapshot_date,
        level1_count=len(levels["level1"].index),
        level2_count=len(levels["level2"].index),
        level3_count=len(levels["level3"].index),
    )
    LOGGER.info(
        "申万行业目录已刷新: level1=%d level2=%d level3=%d output=%s",
        len(levels["level1"].index),
        len(levels["level2"].index),
        len(levels["level3"].index),
        snapshot_path,
    )
    return {"industry_catalog": snapshot_path, "latest": latest_path}


def load_industry_catalog_cached(
    as_of_date: str,
    *,
    base_dir: str | Path = "data",
    allow_previous: bool = True,
) -> tuple[Dict[str, Any], Path | None]:
    """Load an existing SW catalog snapshot without external refresh."""

    cutoff = pd.Timestamp(as_of_date)
    cache_dir = build_global_cache_dir(
        CacheKind.INDUSTRY_CATALOG_SW,
        base_dir=base_dir,
        ensure=False,
    )
    snapshot_dir = cache_dir / "snapshots"
    exact_path = snapshot_dir / f"{as_of_date}.json"
    if exact_path.exists():
        return _load_json(exact_path), exact_path
    if not allow_previous or not snapshot_dir.exists():
        return {}, None

    candidates: list[tuple[pd.Timestamp, Path]] = []
    for path in snapshot_dir.iterdir():
        if not path.is_file() or path.suffix != ".json":
            continue
        try:
            snapshot_date = pd.Timestamp(path.stem)
        except (TypeError, ValueError):
            continue
        if snapshot_date <= cutoff:
            candidates.append((snapshot_date, path))
    if not candidates:
        return {}, None
    candidates.sort(key=lambda item: item[0])
    selected_path = candidates[-1][1]
    return _load_json(selected_path), selected_path


def _normalize_catalog_frame(frame: pd.DataFrame, *, level: str) -> pd.DataFrame:
    missing = [column for column in COMMON_COLUMN_MAP if column not in frame.columns and column != "上级行业"]
    if missing:
        raise ValueError(f"申万行业目录缺少预期字段: {', '.join(missing)}")
    work = frame.rename(columns=COMMON_COLUMN_MAP).copy()
    if "parent_industry" not in work.columns:
        work["parent_industry"] = None
    work["level"] = level
    work["industry_code"] = work["industry_code"].astype(str).str.strip()
    work["industry_name"] = work["industry_name"].astype(str).str.strip()
    for column in ("constituent_count", "pe_static", "pe_ttm", "pb", "dividend_yield_pct"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    columns = [
        "level",
        "industry_code",
        "industry_name",
        "parent_industry",
        "constituent_count",
        "pe_static",
        "pe_ttm",
        "pb",
        "dividend_yield_pct",
    ]
    return work[columns].reset_index(drop=True)


def _records(frame: pd.DataFrame) -> list[Dict[str, Any]]:
    return frame.where(pd.notna(frame), None).to_dict(orient="records")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _save_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_json_numbers(payload)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_json_numbers(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: _normalize_json_numbers(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_normalize_json_numbers(item) for item in payload]
    if isinstance(payload, tuple):
        return [_normalize_json_numbers(item) for item in payload]
    if isinstance(payload, bool) or payload is None or isinstance(payload, int):
        return payload
    if isinstance(payload, Real):
        numeric = float(payload)
        if not math.isfinite(numeric):
            return None
        return round(numeric, 4)
    return payload


__all__ = [
    "load_industry_catalog_cached",
    "update_industry_catalog_cached",
]
