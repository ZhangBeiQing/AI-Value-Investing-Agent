"""Independent market-signal ingestion for board rotation."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Any, Dict, List, Sequence

import akshare as ak
import pandas as pd

from core.logging import get_logger
from utlity.stock_utils import SymbolFormatError, api_call_with_delay, normalize_symbol
from .paths import SelectionSystemPaths
from .store import load_json_file, save_json_file


LOGGER = get_logger("SelectionMarketSignals")
DEFAULT_BOARD_LIMIT = 60


def run_market_signals_pipeline(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    board_limit: int = DEFAULT_BOARD_LIMIT,
    stock_limit: int | None = None,
) -> Dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    today = datetime.now().date().isoformat()
    if str(run_date).strip()[:10] != today:
        LOGGER.warning(
            "板块/热度接口主要提供当前快照，run_date=%s 仅用于归档；实际抓取日期=%s",
            run_date,
            today,
        )

    LOGGER.info(
        "开始独立市场信号模块: run_date=%s board_limit=%d",
        run_date,
        board_limit,
    )

    board_payload = collect_board_signals(run_date, limit=board_limit)
    save_json_file(paths.run_board_signals_path(run_date), board_payload)
    save_json_file(paths.board_signals_daily_path(run_date), board_payload)
    _append_manifest(
        paths.board_signals_manifest_path,
        run_date,
        path=paths.board_signals_daily_path(run_date),
        item_count=len(board_payload.get("items", [])),
        source_errors=board_payload.get("source_status", []),
    )
    LOGGER.info("板块异动结果已写入: %s", paths.run_board_signals_path(run_date))

    return {
        "board_signals": paths.run_board_signals_path(run_date),
    }


def collect_board_signals(run_date: str, *, limit: int = DEFAULT_BOARD_LIMIT) -> Dict[str, Any]:
    collected_at = datetime.now().isoformat()
    source_status = []
    items: List[Dict[str, Any]] = []

    try:
        frame = api_call_with_delay(ak.stock_board_change_em, logger=LOGGER, delay=0)
    except Exception as exc:
        LOGGER.warning("板块异动拉取失败: %s", exc)
        source_status.append(
            {
                "source": "stock_board_change_em",
                "status": "error",
                "error": str(exc),
            }
        )
    else:
        source_status.append(
            {
                "source": "stock_board_change_em",
                "status": "ok",
                "rows": int(len(frame.index)),
            }
        )
        prepared = frame.copy()
        for column in ("涨跌幅", "主力净流入", "板块异动总次数"):
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
        prepared = prepared.sort_values(
            by=["板块异动总次数", "主力净流入", "涨跌幅"],
            ascending=[False, False, False],
        )
        for index, (_, row) in enumerate(prepared.head(max(limit, 0)).iterrows(), start=1):
            items.append(
                {
                    "board_id": f"BD{index:02d}",
                    "board_name": str(row.get("板块名称") or "").strip(),
                    "change_pct": _round_float(row.get("涨跌幅")),
                    "main_net_inflow_wan": _round_float(row.get("主力净流入")),
                    "change_count": _round_int(row.get("板块异动总次数")),
                    "leading_stock_code": _normalize_external_symbol(
                        row.get("板块异动最频繁个股及所属类型-股票代码")
                    ),
                    "leading_stock_name": str(row.get("板块异动最频繁个股及所属类型-股票名称") or "").strip(),
                    "leading_action": str(row.get("板块异动最频繁个股及所属类型-买卖方向") or "").strip(),
                    "change_types": _normalize_change_types(row.get("板块具体异动类型列表及出现次数")),
                }
            )

    return {
        "schema_version": 1,
        "run_date": run_date,
        "collected_at": collected_at,
        "source_status": source_status,
        "items": items,
    }

def _normalize_change_types(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, dict):
        items = value.items()
        return [{"type": str(key), "count": _round_int(raw_value)} for key, raw_value in items]
    if isinstance(value, list):
        normalized = []
        for item in value:
            if isinstance(item, dict):
                normalized.append(
                    {
                        "type": str(item.get("t") or item.get("type") or item.get("name") or "").strip(),
                        "count": _round_int(item.get("ct") or item.get("count") or item.get("value")),
                    }
                )
            else:
                normalized.append({"type": str(item).strip(), "count": None})
        return normalized
    text = str(value or "").strip()
    if not text:
        return []
    return [{"type": text, "count": None}]


def _normalize_external_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    try:
        if "." in text:
            return normalize_symbol(text)
        if re.fullmatch(r"(SH|SZ)\d{6}", text):
            return normalize_symbol(f"{text[2:]}.{text[:2]}")
        if re.fullmatch(r"HK\d{5}", text):
            return normalize_symbol(f"{text[2:]}.HK")
        if text.isdigit() and len(text) == 6:
            suffix = "SH" if text.startswith(("5", "6", "9")) else "SZ"
            return normalize_symbol(f"{text}.{suffix}")
        if text.isdigit() and len(text) == 5:
            return normalize_symbol(f"{text}.HK")
    except SymbolFormatError:
        return ""
    return ""


def _round_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _round_int(value: Any) -> int | None:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _append_manifest(
    manifest_path: Path,
    run_date: str,
    *,
    path: Path,
    item_count: int,
    source_errors: Sequence[Mapping[str, Any]],
) -> None:
    payload = load_json_file(
        manifest_path,
        default={
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "items": [],
        },
    )
    items = payload.get("items", []) if isinstance(payload, dict) else []
    items = [item for item in items if item.get("run_date") != run_date]
    items.append(
        {
            "run_date": run_date,
            "path": str(path),
            "item_count": int(item_count),
            "source_error_count": sum(
                1 for item in source_errors if str(item.get("status") or "").lower() == "error"
            ),
            "updated_at": datetime.now().isoformat(),
        }
    )
    items.sort(key=lambda item: item.get("run_date", ""), reverse=True)
    save_json_file(
        manifest_path,
        {
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "items": items,
        },
    )
