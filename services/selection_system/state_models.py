"""State payload builders for the selection system."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now().isoformat()


@dataclass(frozen=True)
class RawNewsItem:
    raw_id: str
    source: str
    source_type: str
    collected_at: str
    published_at: str
    title: str
    content: str
    url: str = ""
    author: str = ""
    channel: str = ""
    raw_tags: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "raw_id": self.raw_id,
            "source": self.source,
            "source_type": self.source_type,
            "collected_at": self.collected_at,
            "published_at": self.published_at,
            "title": self.title,
            "content": self.content,
            "url": self.url,
            "author": self.author,
            "channel": self.channel,
            "raw_tags": list(self.raw_tags),
            "extra": dict(self.extra),
        }


@dataclass(frozen=True)
class NewsItem:
    item_id: str
    raw_id: str
    source: str
    source_type: str
    published_at: str
    title: str
    url: str
    event_type: str
    scope: str
    raw_facts: str
    key_points: List[str]
    quantitative_data: Dict[str, Any]
    entities: Dict[str, List[str]]
    bull_points: List[str]
    bear_points: List[str]
    time_sensitivity: str
    importance_hint: float
    novelty_hint: float
    sentiment_hint: str
    dedupe_hash: str
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "raw_id": self.raw_id,
            "source": self.source,
            "source_type": self.source_type,
            "published_at": self.published_at,
            "title": self.title,
            "url": self.url,
            "event_type": self.event_type,
            "scope": self.scope,
            "raw_facts": self.raw_facts,
            "key_points": list(self.key_points),
            "quantitative_data": dict(self.quantitative_data),
            "entities": {key: list(value) for key, value in self.entities.items()},
            "bull_points": list(self.bull_points),
            "bear_points": list(self.bear_points),
            "time_sensitivity": self.time_sensitivity,
            "importance_hint": self.importance_hint,
            "novelty_hint": self.novelty_hint,
            "sentiment_hint": self.sentiment_hint,
            "dedupe_hash": self.dedupe_hash,
            "extra": dict(self.extra),
        }


def empty_theme_state_payload() -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at": _now_iso(),
        "themes": [],
    }


def empty_symbol_hot_state_payload() -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at": _now_iso(),
        "symbols": [],
    }


def empty_symbol_memory_payload() -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at": _now_iso(),
        "symbols": [],
    }
