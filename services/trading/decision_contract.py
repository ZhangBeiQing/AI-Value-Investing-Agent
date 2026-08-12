"""Shared stock decision contract helpers."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXED_STOCK_DECISION_SCHEMA = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "stock_decision.schema.json"
)
@lru_cache(maxsize=1)
def load_fixed_stock_decision_schema() -> dict[str, Any]:
    return json.loads(
        FIXED_STOCK_DECISION_SCHEMA.read_text(encoding="utf-8")
    )


def required_stock_decision_fields() -> list[str]:
    """Return required fields from the canonical fixed_tracked schema."""
    return list(load_fixed_stock_decision_schema().get("required") or [])


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_string_list(value: Any, field: str, errors: list[str]) -> None:
    if not isinstance(value, list):
        errors.append(f"{field} 必须是数组")
        return
    for idx, item in enumerate(value):
        if not isinstance(item, str):
            errors.append(f"{field}[{idx}] 必须是字符串")


def validate_stock_decision_entry(
    entry: Any,
    *,
    expected_symbol: str = "",
    reject_extra_fields: bool = True,
) -> list[str]:
    """Validate one fixed_tracked stock verdict without a third-party validator."""
    if not isinstance(entry, dict):
        return ["单股 decision 必须是 JSON 对象"]

    schema = load_fixed_stock_decision_schema()
    errors: list[str] = []
    required = required_stock_decision_fields()
    for field in required:
        if field not in entry:
            errors.append(f"缺少字段: {field}")

    allowed_fields = set((schema.get("properties") or {}).keys())
    if reject_extra_fields:
        for field in sorted(set(entry) - allowed_fields):
            errors.append(f"不允许额外字段: {field}")

    string_fields = [
        "symbol",
        "stock_name",
        "scan",
        "delta_summary",
        "price_impression",
        "recommended_action",
        "action_type",
        "sizing_reason",
    ]
    for field in string_fields:
        if field in entry and not isinstance(entry[field], str):
            errors.append(f"{field} 必须是字符串")

    symbol = entry.get("symbol")
    if expected_symbol and symbol != expected_symbol:
        errors.append(
            f"symbol 不匹配: expected={expected_symbol}, actual={symbol}"
        )

    properties = schema.get("properties") or {}
    enum_fields = ["price_impression", "action_type"]
    for field in enum_fields:
        value = entry.get(field)
        allowed = ((properties.get(field) or {}).get("enum") or [])
        if value is not None and value not in allowed:
            errors.append(f"{field} 非法: {value}")

    for field in [
        "key_facts",
        "inferences",
        "key_risks",
        "next_day_watchlist",
    ]:
        if field in entry:
            _validate_string_list(entry[field], field, errors)

    court = entry.get("court")
    if court is not None:
        if not isinstance(court, dict):
            errors.append("court 必须是对象")
        else:
            court_allowed = {"pro", "con", "verdict"}
            for field in ["pro", "con", "verdict"]:
                if field not in court:
                    errors.append(f"court 缺少字段: {field}")
            if reject_extra_fields:
                for field in sorted(set(court) - court_allowed):
                    errors.append(f"court 不允许额外字段: {field}")
            if "pro" in court:
                _validate_string_list(court["pro"], "court.pro", errors)
            if "con" in court:
                _validate_string_list(court["con"], "court.con", errors)
            if "verdict" in court and not isinstance(court["verdict"], str):
                errors.append("court.verdict 必须是字符串")

    action_num = entry.get("action_num")
    if action_num is not None:
        if not isinstance(action_num, int) or isinstance(action_num, bool):
            errors.append("action_num 必须是整数")
        else:
            action_type = entry.get("action_type")
            if action_type in {"BUY", "SELL"} and action_num <= 0:
                errors.append(f"{action_type} 时 action_num 必须大于 0")
            if action_type in {"HOLD", "FLAT"} and action_num != 0:
                errors.append(f"{action_type} 时 action_num 必须等于 0")

    confidence = entry.get("confidence_score")
    if confidence is not None:
        if not _is_number(confidence):
            errors.append("confidence_score 必须是数字")
        elif not 0 <= float(confidence) <= 1:
            errors.append("confidence_score 必须在 0 到 1 之间")

    current_position_pct = entry.get("current_position_pct")
    if current_position_pct is not None:
        if not isinstance(current_position_pct, str) or not re.fullmatch(
            r"^(100(?:\.0+)?|(?:\d|[1-9]\d)(?:\.\d+)?)%$",
            current_position_pct,
        ):
            errors.append(
                'current_position_pct 必须是 0% 到 100% 的百分比字符串，例如 "3.5%"'
            )

    return errors


__all__ = [
    "FIXED_STOCK_DECISION_SCHEMA",
    "load_fixed_stock_decision_schema",
    "required_stock_decision_fields",
    "validate_stock_decision_entry",
]
