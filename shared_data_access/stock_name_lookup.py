"""Resolve exact stock names from already refreshed shared market caches."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from .cache_registry import CacheKind, build_global_cache_dir


def lookup_cached_stock_names(name: str, *, base_dir: str | Path = "data") -> list[dict[str, str]]:
    """Return verified local name/code pairs without fetching or changing caches."""
    name = name.strip()
    if not name:
        return []
    matches: dict[str, dict[str, str]] = {}
    board_dir = build_global_cache_dir(CacheKind.BOARD_METRICS_THS, base_dir=base_dir, ensure=False)
    board_path = board_dir / "latest_market_snapshot.json"
    if board_path.is_file():
        payload = json.loads(board_path.read_text(encoding="utf-8"))
        for board in payload.get("boards", []):
            for stock in board.get("related_stock_hints", []):
                symbol = stock.get("symbol")
                if stock.get("name") == name and isinstance(symbol, str) and symbol.endswith((".SH", ".SZ", ".HK")):
                    matches[symbol] = {"symbol": symbol, "name": name}

    # The financial breadth cache holds thousands of A-share name/code pairs,
    # including securities outside the currently selected board-market hints.
    panel_dir = build_global_cache_dir(CacheKind.INDUSTRY_FINANCIAL_PANEL, base_dir=base_dir, ensure=False)
    raw_dir = panel_dir / "raw"
    if raw_dir.is_dir():
        paths = sorted(raw_dir.glob("*/*.csv"), reverse=True)
        if paths:
            with paths[0].open("r", encoding="utf-8-sig", newline="") as stream:
                for row in csv.DictReader(stream):
                    if (row.get("stock_name") or "").strip() != name:
                        continue
                    code = (row.get("stock_code") or "").strip()
                    suffix = "SZ" if code.startswith(("0", "3")) else "SH" if code.startswith("6") else ""
                    if len(code) == 6 and code.isdigit() and suffix:
                        symbol = f"{code}.{suffix}"
                        matches.setdefault(symbol, {"symbol": symbol, "name": name})
    return list(matches.values())
