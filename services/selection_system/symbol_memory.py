"""Lightweight symbol memory outputs for shortlisted candidates."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from core.logging import get_logger

from .store import save_json_file


LOGGER = get_logger("SymbolMemory")


def build_symbol_memory_payload(
    hot_bundle: Dict[str, Any],
    core_bundle: Dict[str, Any],
    run_date: str,
) -> Dict[str, Any]:
    records: Dict[str, Dict[str, Any]] = {}
    for strategy, bundle in (("hot", hot_bundle), ("core", core_bundle)):
        for candidate in bundle.get("candidates", []):
            symbol = candidate["symbol"]
            entry = records.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "name": candidate.get("name"),
                    "updated_at": datetime.now().isoformat(),
                    "strategies": [],
                    "snapshot": candidate.get("snapshot") or {},
                    "hot_state": candidate.get("hot_state") or {},
                    "reasons": [],
                    "recent_news_titles": [],
                },
            )
            if strategy not in entry["strategies"]:
                entry["strategies"].append(strategy)
            entry["reasons"].extend(candidate.get("reasons") or [])
            entry["recent_news_titles"].extend((candidate.get("hot_state") or {}).get("today_news_delta") or [])

    symbols = []
    for record in records.values():
        record["reasons"] = _unique(record["reasons"])[:6]
        record["recent_news_titles"] = _unique(record["recent_news_titles"])[:8]
        record["memory_summary"] = _build_memory_summary(record)
        symbols.append(record)

    symbols.sort(key=lambda item: (len(item.get("strategies", [])), item.get("symbol", "")), reverse=True)
    payload = {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "symbols": symbols,
    }
    LOGGER.info("symbol_memory 构建完成: total=%d", len(symbols))
    return payload


def persist_symbol_memories(base_dir: Path, payload: Dict[str, Any]) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    index_payload = {
        "schema_version": payload.get("schema_version", 1),
        "updated_at": payload.get("updated_at"),
        "symbols": [
            {
                "symbol": record.get("symbol"),
                "name": record.get("name"),
                "strategies": record.get("strategies"),
                "summary": record.get("memory_summary"),
            }
            for record in payload.get("symbols", [])
        ],
    }
    save_json_file(base_dir / "index.json", index_payload)
    for record in payload.get("symbols", []):
        symbol = str(record.get("symbol") or "").replace("/", "_")
        save_json_file(base_dir / f"{symbol}.json", record)


def _build_memory_summary(record: Dict[str, Any]) -> str:
    themes = [theme.get("theme_name") for theme in record.get("hot_state", {}).get("hot_themes", []) if isinstance(theme, dict)]
    snapshot = record.get("snapshot") or {}
    parts = [
        f"策略归属: {', '.join(record.get('strategies', [])) or '未归类'}",
        f"主题: {', '.join(themes[:3]) or '暂无显著主题'}",
    ]
    if snapshot.get("roe") is not None:
        parts.append(f"ROE {snapshot.get('roe')}")
    if snapshot.get("return_3m") is not None:
        parts.append(f"近3个月收益 {snapshot.get('return_3m')}")
    return " | ".join(parts)


def _unique(values: Iterable[Any]) -> List[str]:
    result = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result
