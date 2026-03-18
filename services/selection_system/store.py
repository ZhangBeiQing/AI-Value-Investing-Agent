"""Small JSON persistence helpers for the selection system."""

from __future__ import annotations

import json
import math
from numbers import Real
from pathlib import Path
from typing import Any


def load_json_file(path: Path, *, default: Any | None = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


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


def save_json_file(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_json_numbers(payload)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
