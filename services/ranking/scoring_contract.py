"""Validate weekly Agent scores against the local JSON schema without dependencies."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WEEKLY_SCORE_SCHEMA = PROJECT_ROOT / "configs/prompt_flow/fixed_tracked/weekly_score.schema.json"


@lru_cache(maxsize=1)
def load_weekly_score_schema() -> dict[str, Any]:
    return json.loads(WEEKLY_SCORE_SCHEMA.read_text(encoding="utf-8"))


def _validate(value: Any, schema: dict[str, Any], field: str, errors: list[str]) -> None:
    kind = schema["type"]
    if kind == "object":
        if not isinstance(value, dict):
            errors.append(f"{field} 必须是对象")
            return
        properties = schema["properties"]
        for key in schema.get("required", []):
            if key not in value:
                errors.append(f"{field} 缺少字段: {key}")
        if schema.get("additionalProperties") is False:
            for key in sorted(value.keys() - properties.keys()):
                errors.append(f"{field} 不允许额外字段: {key}")
        for key, child in properties.items():
            if key in value:
                _validate(value[key], child, f"{field}.{key}", errors)
    elif kind == "integer":
        if type(value) is not int:
            errors.append(f"{field} 必须是整数（不接受布尔值或小数）")
        elif not schema["minimum"] <= value <= schema["maximum"]:
            errors.append(f"{field} 必须在 {schema['minimum']} 到 {schema['maximum']} 之间")
    elif kind == "string":
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field} 必须是非空字符串")
        elif schema.get("pattern") and not re.fullmatch(schema["pattern"], value):
            errors.append(f"{field} 格式非法: {value}")
        if isinstance(value, str) and (field.endswith("_reason")):
            if re.search(r"https?://|www\.|\[[^\]]+\]\([^\)]+\)|\[\^[^\]]+\]", value):
                errors.append(f"{field} 不得包含 URL、Markdown 链接或脚注引用")


def validate_weekly_score_entry(entry: Any, *, expected_symbol: str = "") -> list[str]:
    """Check structure, exact integer scores, reasons and stock identity."""
    errors: list[str] = []
    _validate(entry, load_weekly_score_schema(), "score", errors)
    if isinstance(entry, dict) and expected_symbol and entry.get("symbol") != expected_symbol:
        errors.append(f"symbol 不匹配: expected={expected_symbol}, actual={entry.get('symbol')}")
    return errors


def load_score_file(path: Path, *, expected_symbol: str = "") -> tuple[Any, list[str]]:
    """Reject malformed JSON and duplicate keys instead of silently overwriting values."""
    def unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"JSON 重复字段: {key}")
            result[key] = value
        return result

    try:
        entry = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_keys)
    except (OSError, ValueError) as exc:
        return None, [f"无法读取评分 JSON: {exc}"]
    return entry, validate_weekly_score_entry(entry, expected_symbol=expected_symbol)
