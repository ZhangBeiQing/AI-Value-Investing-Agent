"""Deterministic filesystem helpers for fixed_tracked stock debates."""

from __future__ import annotations

import json
import math
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.trading.decision_contract import validate_stock_decision_entry
from commons.stock_utils import parse_symbol, sanitize_stock_name


LOGGER = get_logger("DebatePipeline")
_SAFE_SYMBOL = re.compile(r"^[A-Za-z0-9._-]+$")


def normalize_debate_symbol(symbol: str) -> str:
    normalized = (symbol or "").strip().upper()
    if not normalized or not _SAFE_SYMBOL.fullmatch(normalized):
        raise ValueError(f"非法 symbol 目录名: {symbol!r}")
    if normalized in {".", ".."}:
        raise ValueError(f"非法 symbol 目录名: {symbol!r}")
    return normalized


def debate_directory_name(symbol: str) -> str:
    """Return the human-readable, filesystem-safe directory name for a debate."""
    normalized_symbol = normalize_debate_symbol(symbol)
    stock_name = sanitize_stock_name(parse_symbol(normalized_symbol).stock_name)
    return f"{stock_name}_{normalized_symbol}"


def debate_symbol_dir(book_dir: str | Path, symbol: str) -> Path:
    return Path(book_dir) / "debate" / debate_directory_name(symbol)


def prepare_debate_directories(book_dir: str | Path, symbol: str) -> Path:
    """Create the current three roles' directories without producing decisions."""
    symbol_dir = debate_symbol_dir(book_dir, symbol)
    for relative in ("advocates/bull", "advocates/bear", "adjudicator", "final"):
        (symbol_dir / relative).mkdir(parents=True, exist_ok=True)
    LOGGER.info("单裁决目录已准备: %s", symbol_dir)
    return symbol_dir


def write_review_context(book_dir: str | Path, symbol: str, *, position_shares: float) -> Path:
    """Record the actual book position used to check decision semantics."""
    if not isinstance(position_shares, (int, float)) or isinstance(position_shares, bool) or not math.isfinite(position_shares) or position_shares < 0:
        raise ValueError("position_shares 必须为有限非负数")
    root = debate_symbol_dir(book_dir, symbol)
    return atomic_write_json(root / "review_context.json", {
        "symbol": normalize_debate_symbol(symbol),
        "analysis_date": Path(book_dir).parent.name,
        "position_shares": position_shares,
    })


def atomic_write_json(path: str | Path, payload: dict[str, Any]) -> Path:
    """Write one JSON object atomically in its final directory."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.replace(target)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return target


def _read_json_object(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    if not path.exists():
        return None, [f"文件不存在: {path}"]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, [f"JSON 读取失败: {path} ({exc})"]
    if not isinstance(payload, dict):
        return None, [f"JSON 顶层必须是对象: {path}"]
    return payload, []


def _reference_errors(path: Path, payload: Any) -> list[str]:
    rendered = json.dumps(payload, ensure_ascii=False)
    errors: list[str] = []
    if re.search(r"https?://|www\.", rendered, re.IGNORECASE):
        errors.append(f"{path}: 最终辩论产物禁止包含原始 URL")
    if re.search(r"\[[^\]]+\]\([^\)]+\)", rendered):
        errors.append(f"{path}: 最终辩论产物禁止包含 Markdown 链接")
    return errors


def validate_opening(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return ["opening 必须是 JSON 对象"]
    errors: list[str] = []
    if set(payload) != {"arguments"}:
        errors.append("opening 只能包含 arguments")
    arguments = payload.get("arguments")
    if not isinstance(arguments, list):
        errors.append("arguments 必须是数组")
    else:
        for idx, argument in enumerate(arguments):
            if not isinstance(argument, str) or not argument.strip():
                errors.append(f"arguments[{idx}] 必须是非空字符串")
    return errors


def validate_rebuttal(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return ["rebuttal 必须是 JSON 对象"]
    errors: list[str] = []
    if set(payload) != {"rebuttals"}:
        errors.append("rebuttal 只能包含 rebuttals")
    rebuttals = payload.get("rebuttals")
    if not isinstance(rebuttals, list):
        errors.append("rebuttals 必须是数组")
        return errors
    for idx, item in enumerate(rebuttals):
        if not isinstance(item, dict):
            errors.append(f"rebuttals[{idx}] 必须是对象")
            continue
        if set(item) != {"original_argument", "rebuttal"}:
            errors.append(
                f"rebuttals[{idx}] 只能包含 original_argument 和 rebuttal"
            )
        for field in ["original_argument", "rebuttal"]:
            value = item.get(field)
            if not isinstance(value, str) or not value.strip():
                errors.append(
                    f"rebuttals[{idx}].{field} 必须是非空字符串"
                )
    return errors


def _role_payload_errors(
    path: Path,
    validator,
) -> tuple[dict[str, Any] | None, list[str]]:
    payload, errors = _read_json_object(path)
    if payload is None:
        return None, errors
    return payload, [f"{path}: {error}" for error in validator(payload)]


def _question_errors(symbol_dir: Path) -> list[str]:
    request = symbol_dir / "adjudicator" / "review_request.json"
    resolution = symbol_dir / "adjudicator" / "review_resolution.json"
    if not request.exists():
        return ["存在无对应质询的 resolution"] if resolution.exists() else []
    questions, errors = _read_json_object(request)
    answers, answer_errors = _read_json_object(resolution)
    errors.extend(answer_errors)
    if questions is None or answers is None:
        return errors
    items = questions.get("questions")
    resolved = answers.get("resolutions")
    if not isinstance(items, list) or not items or not isinstance(resolved, list):
        return errors + ["质询及其解决记录必须为非空问题清单"]
    ids = []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"].strip() or not isinstance(item.get("question"), str) or not item["question"].strip():
            errors.append("质询缺少有效 id/question")
        else:
            ids.append(item["id"])
    resolved_ids = []
    for item in resolved:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not isinstance(item.get("conclusion"), str) or not item["conclusion"].strip():
            errors.append("解决记录缺少有效 id/conclusion")
        else:
            resolved_ids.append(item["id"])
    if len(ids) != len(set(ids)) or len(resolved_ids) != len(set(resolved_ids)) or set(ids) != set(resolved_ids):
        errors.append("质询尚未逐项解决或存在重复编号")
    return errors


def validate_debate_artifacts(
    book_dir: str | Path, symbol: str, *, require_verdict: bool = False,
) -> list[str]:
    """Check current role outputs, question closure and executable quantity semantics."""
    symbol = normalize_debate_symbol(symbol)
    root = debate_symbol_dir(book_dir, symbol)
    errors: list[str] = []
    for side in ("bull", "bear"):
        path = root / "advocates" / side / "opening.json"
        payload, role_errors = _role_payload_errors(path, validate_opening)
        errors.extend(role_errors)
        if payload is not None:
            errors.extend(_reference_errors(path, payload))
            if not payload.get("arguments"):
                errors.append(f"{side} opening 不得为空")
        rebuttal = path.with_name("rebuttal.json")
        if rebuttal.exists():
            payload, role_errors = _role_payload_errors(rebuttal, validate_rebuttal)
            errors.extend(role_errors)
            if payload is not None:
                errors.extend(_reference_errors(rebuttal, payload))
    errors.extend(_question_errors(root))
    verdict_path = root / "final" / "stock_verdict.json"
    if require_verdict or verdict_path.exists():
        context, context_errors = _read_json_object(root / "review_context.json")
        errors.extend(context_errors)
        shares = context.get("position_shares") if context else None
        valid_shares = isinstance(shares, (int, float)) and not isinstance(shares, bool) and math.isfinite(shares) and shares >= 0
        if not valid_shares:
            errors.append("缺少合法的当日实际持仓数量")
        if context and (context.get("symbol") != symbol or context.get("analysis_date") != Path(book_dir).parent.name):
            errors.append("持仓上下文的股票或日期不匹配")
        verdict, verdict_errors = _read_json_object(verdict_path)
        errors.extend(verdict_errors)
        if verdict is not None:
            errors.extend(validate_stock_decision_entry(verdict, expected_symbol=symbol))
            errors.extend(_reference_errors(verdict_path, verdict))
            if not verdict.get("sizing_reason") or not verdict.get("current_position_pct"):
                errors.append("裁决必须包含 sizing_reason 和 current_position_pct")
            action, quantity = verdict.get("action_type"), verdict.get("action_num")
            if valid_shares:
                allowed = {"BUY", "SELL", "HOLD"} if shares > 0 else {"BUY", "FLAT"}
                if action not in allowed:
                    errors.append("裁决动作与当前持仓不兼容")
                if action == "SELL" and isinstance(quantity, int) and quantity > shares:
                    errors.append("卖出数量超过当前持仓")
            if action in {"BUY", "SELL"} and isinstance(quantity, int) and not isinstance(quantity, bool) and quantity > 0:
                from services.trading.trade_executor import _validate_lot_size

                lot_error = _validate_lot_size(action.lower(), {symbol: quantity}, Path(book_dir).parent.name)
                if lot_error:
                    errors.append(lot_error["error"])
    return errors
