"""Query compact board snapshot summaries by board name."""

from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from core.logging import get_logger
from shared_data_access.cache_registry import CacheKind, build_global_cache_dir

from .store import load_json_file


LOGGER = get_logger("BoardSnapshotQuery")


def query_board_snapshot(
    run_date: str,
    board_names: Sequence[str],
    *,
    base_dir: str | Path = "data",
) -> Dict[str, Any]:
    cache_dir = build_global_cache_dir(CacheKind.BOARD_METRICS_THS, base_dir=base_dir, ensure=False)
    daily_snapshot = load_json_file(cache_dir / "daily_snapshots" / f"{run_date}.json", default={}) or {}
    market_snapshot = load_json_file(cache_dir / "market_snapshots" / f"{run_date}.json", default={}) or {}

    daily_boards = daily_snapshot.get("boards") if isinstance(daily_snapshot, Mapping) else []
    market_boards = market_snapshot.get("boards") if isinstance(market_snapshot, Mapping) else []
    if not isinstance(daily_boards, list):
        daily_boards = []
    if not isinstance(market_boards, list):
        market_boards = []

    standard_names = [
        str(item.get("board_name") or "").strip()
        for item in daily_boards
        if isinstance(item, Mapping) and str(item.get("board_name") or "").strip()
    ]
    daily_map = {
        str(item.get("board_name") or "").strip(): item
        for item in daily_boards
        if isinstance(item, Mapping) and str(item.get("board_name") or "").strip()
    }
    market_map = {
        str(item.get("board_name") or "").strip(): item
        for item in market_boards
        if isinstance(item, Mapping) and str(item.get("board_name") or "").strip()
    }

    items: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []
    for raw_name in board_names:
        requested = str(raw_name or "").strip()
        if not requested:
            continue
        resolved = _resolve_board_name(requested, standard_names)
        if resolved is None:
            unmatched.append(
                {
                    "requested_board_name": requested,
                    "candidates": _top_name_candidates(requested, standard_names),
                }
            )
            continue

        daily_item = daily_map.get(resolved, {})
        market_item = market_map.get(resolved, {})
        items.append(
            {
                "requested_board_name": requested,
                "board_name": resolved,
                "today": {
                    "change_pct": market_item.get("change_pct"),
                    "up_count": market_item.get("up_count"),
                    "down_count": market_item.get("down_count"),
                    "total_turnover": market_item.get("total_turnover"),
                    "net_inflow": market_item.get("net_inflow"),
                    "leading_stock_name": market_item.get("leading_stock_name"),
                    "leading_stock_change_pct": market_item.get("leading_stock_change_pct"),
                },
                "trend": {
                    "interval_returns": _nested(daily_item, "quant_metrics", "interval_returns"),
                    "interval_rankings": _nested(daily_item, "quant_metrics", "interval_rankings"),
                    "continuity": _nested(daily_item, "quant_metrics", "continuity"),
                },
                "structure": {
                    "breadth": _nested(daily_item, "quant_metrics", "breadth"),
                    "phase": _nested(daily_item, "quant_metrics", "phase"),
                },
            }
        )

    return {
        "run_date": run_date,
        "item_count": len(items),
        "items": items,
        "unmatched": unmatched,
    }


def _resolve_board_name(requested: str, standard_names: Sequence[str]) -> str | None:
    normalized = requested.strip()
    if not normalized:
        return None

    exact = [name for name in standard_names if name == normalized]
    if exact:
        return exact[0]

    contains = [name for name in standard_names if normalized in name or name in normalized]
    if len(contains) == 1:
        return contains[0]

    scored = sorted(
        (
            (SequenceMatcher(None, normalized, name).ratio(), name)
            for name in standard_names
        ),
        key=lambda item: item[0],
        reverse=True,
    )
    if not scored:
        return None
    best_score, best_name = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    if best_score >= 0.72 and (best_score - second_score) >= 0.08:
        return best_name
    return None


def _top_name_candidates(requested: str, standard_names: Sequence[str], limit: int = 5) -> List[str]:
    scored = sorted(
        (
            (SequenceMatcher(None, requested, name).ratio(), name)
            for name in standard_names
        ),
        key=lambda item: item[0],
        reverse=True,
    )
    return [name for score, name in scored[: max(limit, 0)] if score >= 0.4]


def _nested(item: Mapping[str, Any], *keys: str) -> Any:
    current: Any = item
    for key in keys:
        if not isinstance(current, Mapping):
            return {}
        current = current.get(key)
    return current if current is not None else {}
