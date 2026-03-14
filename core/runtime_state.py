"""Runtime environment state helpers for the skill-only architecture."""

from __future__ import annotations

import json
import os
from pathlib import Path


def _load_runtime_env() -> dict:
    runtime_path = os.environ.get("RUNTIME_ENV_PATH", "")
    if not runtime_path:
        return {}
    path = Path(runtime_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_config_value(key: str, default=None):
    runtime = _load_runtime_env()
    if key in runtime:
        return runtime.get(key)
    return os.environ.get(key, default)


def write_config_value(key: str, value):
    runtime_path = os.environ.get("RUNTIME_ENV_PATH", "")
    if runtime_path:
        path = Path(runtime_path)
        payload = _load_runtime_env()
        payload[key] = value
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.environ[key] = str(value)


__all__ = ["get_config_value", "write_config_value"]

