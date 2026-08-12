"""Deterministic filesystem helpers for fixed_tracked stock debates."""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.trading.decision_contract import validate_stock_decision_entry
from utlity.stock_utils import parse_symbol, sanitize_stock_name


LOGGER = get_logger("DebatePipeline")
VALID_ACTIONS = ("BUY", "SELL", "HOLD", "FLAT")
VALID_PRICE_IMPRESSIONS = (
    "明显低估",
    "偏低估",
    "合理偏低估",
    "合理",
    "合理偏贵",
    "偏贵",
    "明显高估",
    "泡沫",
)
JUROR_IDS = ("juror_01", "juror_02", "juror_03")
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
    try:
        stock_name = sanitize_stock_name(
            parse_symbol(normalized_symbol).stock_name
        )
    except Exception as exc:
        LOGGER.warning(
            "无法解析辩论标的名称，目录退回仅代码: symbol=%s error=%s",
            normalized_symbol,
            exc,
        )
        return normalized_symbol
    return f"{stock_name}_{normalized_symbol}"


def debate_symbol_dir(
    book_dir: str | Path,
    symbol: str,
    *,
    allow_legacy: bool = True,
) -> Path:
    """Resolve a debate directory, reading legacy code-only directories if needed."""
    normalized_symbol = normalize_debate_symbol(symbol)
    debate_root = Path(book_dir) / "debate"
    named_dir = debate_root / debate_directory_name(normalized_symbol)
    legacy_dir = debate_root / normalized_symbol
    if allow_legacy and not named_dir.exists() and legacy_dir.exists():
        return legacy_dir
    return named_dir


def prepare_debate_directories(book_dir: str | Path, symbol: str) -> Path:
    """Create role-owned directories without creating or overwriting results."""
    # 新运行统一使用“名称_代码”；旧代码目录只为 aggregate/validate 兼容保留。
    symbol_dir = debate_symbol_dir(book_dir, symbol, allow_legacy=False)
    directories = [
        symbol_dir / "advocates" / "bull",
        symbol_dir / "advocates" / "bear",
        *(symbol_dir / "jury" / juror_id for juror_id in JUROR_IDS),
        symbol_dir / "final",
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    LOGGER.info("辩论目录已准备: %s", symbol_dir)
    return symbol_dir


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


def validate_ballot(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return ["ballot 必须是 JSON 对象"]
    errors: list[str] = []
    allowed_fields = {"action_type", "action_num", "price_impression", "reason"}
    required_fields = {"action_type", "price_impression", "reason"}
    if not required_fields.issubset(payload) or not set(payload).issubset(
        allowed_fields
    ):
        errors.append(
            "ballot 必须包含 action_type、price_impression、reason，"
            "并且只能额外包含 action_num"
        )
    action_type = payload.get("action_type")
    if action_type not in VALID_ACTIONS:
        errors.append(f"action_type 非法: {action_type}")
    price_impression = payload.get("price_impression")
    if price_impression not in VALID_PRICE_IMPRESSIONS:
        errors.append(f"price_impression 非法: {price_impression}")
    reason = payload.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        errors.append("reason 必须是非空字符串")
    action_num = payload.get("action_num")
    if action_num is not None:
        if not isinstance(action_num, int) or isinstance(action_num, bool):
            errors.append("action_num 必须是整数")
        elif action_type in {"BUY", "SELL"} and action_num <= 0:
            errors.append(f"{action_type} 时 action_num 必须大于 0")
        elif action_type in {"HOLD", "FLAT"} and action_num != 0:
            errors.append(f"{action_type} 时 action_num 必须等于 0")
    return errors


def _role_payload_errors(
    path: Path,
    validator,
) -> tuple[dict[str, Any] | None, list[str]]:
    payload, errors = _read_json_object(path)
    if payload is None:
        return None, errors
    return payload, [f"{path}: {error}" for error in validator(payload)]


def aggregate_jury_votes(
    book_dir: str | Path,
    symbol: str,
    *,
    position_shares: float,
) -> tuple[Path, dict[str, Any]]:
    """Count three independent ballots and atomically write vote_summary.json."""
    normalized_symbol = normalize_debate_symbol(symbol)
    if position_shares < 0:
        raise ValueError("position_shares 不能小于 0")
    symbol_dir = debate_symbol_dir(book_dir, normalized_symbol)
    votes: list[dict[str, Any]] = []
    price_impression_votes: list[dict[str, str]] = []
    errors: list[str] = []
    for juror_id in JUROR_IDS:
        ballot_path = symbol_dir / "jury" / juror_id / "ballot.json"
        payload, ballot_errors = _role_payload_errors(
            ballot_path,
            validate_ballot,
        )
        errors.extend(ballot_errors)
        if payload is not None and not ballot_errors:
            vote: dict[str, Any] = {
                "juror": juror_id,
                "action_type": payload["action_type"],
            }
            if "action_num" in payload:
                vote["action_num"] = payload["action_num"]
            votes.append(vote)
            price_impression_votes.append(
                {
                    "juror": juror_id,
                    "price_impression": payload["price_impression"],
                }
            )
    if errors:
        raise ValueError("无法聚合投票:\n- " + "\n- ".join(errors))

    allowed_actions = (
        {"BUY", "SELL", "HOLD"}
        if position_shares > 0
        else {"BUY", "FLAT"}
    )
    invalid_position_votes = [
        vote
        for vote in votes
        if vote["action_type"] not in allowed_actions
    ]
    if invalid_position_votes:
        details = ", ".join(
            f"{vote['juror']}={vote['action_type']}"
            for vote in invalid_position_votes
        )
        raise ValueError(
            "ballot 动作与当前持仓状态不兼容: "
            f"position_shares={position_shares}, {details}"
        )

    invalid_sell_quantities = [
        vote
        for vote in votes
        if vote["action_type"] == "SELL"
        and vote.get("action_num") is not None
        and vote["action_num"] > position_shares
    ]
    if invalid_sell_quantities:
        details = ", ".join(
            f"{vote['juror']}={vote['action_num']}"
            for vote in invalid_sell_quantities
        )
        raise ValueError(
            "ballot 卖出数量超过当前持仓: "
            f"position_shares={position_shares}, {details}"
        )

    counts = Counter(vote["action_type"] for vote in votes)
    majority_action = next(
        (action for action in VALID_ACTIONS if counts[action] >= 2),
        None,
    )
    resolved_action = majority_action or (
        "HOLD" if position_shares > 0 else "FLAT"
    )
    summary: dict[str, Any] = {
        "symbol": normalized_symbol,
        "position_shares": position_shares,
        "status": "majority" if majority_action else "no_majority",
        "majority_action": majority_action,
        "resolved_action": resolved_action,
        "counts": {
            action: counts.get(action, 0)
            for action in VALID_ACTIONS
        },
        "votes": votes,
        "price_impression_votes": price_impression_votes,
    }
    summary_path = symbol_dir / "final" / "vote_summary.json"
    atomic_write_json(summary_path, summary)
    LOGGER.info(
        "投票聚合完成: symbol=%s status=%s majority=%s",
        normalized_symbol,
        summary["status"],
        resolved_action,
    )
    return summary_path, summary


def validate_debate_artifacts(
    book_dir: str | Path,
    symbol: str,
    *,
    require_verdict: bool = False,
) -> list[str]:
    """Validate all phase artifacts for one symbol."""
    normalized_symbol = normalize_debate_symbol(symbol)
    symbol_dir = debate_symbol_dir(book_dir, normalized_symbol)
    validators = {
        symbol_dir / "advocates" / "bull" / "opening.json": validate_opening,
        symbol_dir / "advocates" / "bear" / "opening.json": validate_opening,
        symbol_dir / "advocates" / "bull" / "rebuttal.json": validate_rebuttal,
        symbol_dir / "advocates" / "bear" / "rebuttal.json": validate_rebuttal,
        **{
            symbol_dir / "jury" / juror_id / "ballot.json": validate_ballot
            for juror_id in JUROR_IDS
        },
    }
    errors: list[str] = []
    for path, validator in validators.items():
        _, artifact_errors = _role_payload_errors(path, validator)
        errors.extend(artifact_errors)

    vote_summary_path = symbol_dir / "final" / "vote_summary.json"
    vote_summary, vote_errors = _read_json_object(vote_summary_path)
    errors.extend(vote_errors)
    if vote_summary is not None:
        if vote_summary.get("symbol") != normalized_symbol:
            errors.append(f"{vote_summary_path}: symbol 不匹配")
        if vote_summary.get("status") not in {"majority", "no_majority"}:
            errors.append(f"{vote_summary_path}: status 非法")
        majority_action = vote_summary.get("majority_action")
        if majority_action is not None and majority_action not in VALID_ACTIONS:
            errors.append(f"{vote_summary_path}: majority_action 非法")
        resolved_action = vote_summary.get("resolved_action")
        if resolved_action not in VALID_ACTIONS:
            errors.append(f"{vote_summary_path}: resolved_action 非法")
        if majority_action and resolved_action != majority_action:
            errors.append(
                f"{vote_summary_path}: resolved_action 必须等于多数票"
            )
        position_shares = vote_summary.get("position_shares")
        if (
            not isinstance(position_shares, (int, float))
            or isinstance(position_shares, bool)
            or position_shares < 0
        ):
            errors.append(f"{vote_summary_path}: position_shares 非法")
        elif vote_summary.get("status") == "no_majority":
            fallback_action = "HOLD" if position_shares > 0 else "FLAT"
            if resolved_action != fallback_action:
                errors.append(
                    f"{vote_summary_path}: 无多数时 resolved_action "
                    f"必须为 {fallback_action}"
                )

        ballot_votes: list[dict[str, Any]] = []
        ballot_price_impressions: list[dict[str, str]] = []
        for juror_id in JUROR_IDS:
            ballot_path = symbol_dir / "jury" / juror_id / "ballot.json"
            ballot, ballot_read_errors = _read_json_object(ballot_path)
            if (
                ballot is not None
                and not ballot_read_errors
                and not validate_ballot(ballot)
            ):
                ballot_vote: dict[str, Any] = {
                    "juror": juror_id,
                    "action_type": ballot["action_type"],
                }
                if "action_num" in ballot:
                    ballot_vote["action_num"] = ballot["action_num"]
                ballot_votes.append(ballot_vote)
                ballot_price_impressions.append(
                    {
                        "juror": juror_id,
                        "price_impression": ballot["price_impression"],
                    }
                )
        if len(ballot_votes) == len(JUROR_IDS):
            expected_counts = Counter(
                vote["action_type"] for vote in ballot_votes
            )
            expected_counts_payload = {
                action: expected_counts.get(action, 0)
                for action in VALID_ACTIONS
            }
            if vote_summary.get("votes") != ballot_votes:
                errors.append(
                    f"{vote_summary_path}: votes 与实际 ballot 不一致"
                )
            if (
                vote_summary.get("price_impression_votes")
                != ballot_price_impressions
            ):
                errors.append(
                    f"{vote_summary_path}: price_impression_votes "
                    "与实际 ballot 不一致"
                )
            if vote_summary.get("counts") != expected_counts_payload:
                errors.append(
                    f"{vote_summary_path}: counts 与实际 ballot 不一致"
                )
            expected_majority = next(
                (
                    action
                    for action in VALID_ACTIONS
                    if expected_counts[action] >= 2
                ),
                None,
            )
            expected_status = (
                "majority" if expected_majority else "no_majority"
            )
            if vote_summary.get("majority_action") != expected_majority:
                errors.append(
                    f"{vote_summary_path}: majority_action 与实际 ballot 不一致"
                )
            if vote_summary.get("status") != expected_status:
                errors.append(
                    f"{vote_summary_path}: status 与实际 ballot 不一致"
                )
            if isinstance(position_shares, (int, float)) and not isinstance(
                position_shares,
                bool,
            ):
                expected_resolved = expected_majority or (
                    "HOLD" if position_shares > 0 else "FLAT"
                )
                if resolved_action != expected_resolved:
                    errors.append(
                        f"{vote_summary_path}: resolved_action "
                        f"与实际 ballot/持仓不一致"
                    )

    verdict_path = symbol_dir / "final" / "stock_verdict.json"
    if require_verdict or verdict_path.exists():
        verdict, verdict_read_errors = _read_json_object(verdict_path)
        errors.extend(verdict_read_errors)
        if verdict is not None:
            errors.extend(
                f"{verdict_path}: {error}"
                for error in validate_stock_decision_entry(
                    verdict,
                    expected_symbol=normalized_symbol,
                )
            )
            if vote_summary is not None:
                resolved_action = vote_summary.get("resolved_action")
                if (
                    resolved_action
                    and verdict.get("action_type") != resolved_action
                ):
                    errors.append(
                        f"{verdict_path}: action_type 必须服从多数票或回退结果 "
                        f"{resolved_action}"
                    )

    return errors


__all__ = [
    "JUROR_IDS",
    "VALID_ACTIONS",
    "VALID_PRICE_IMPRESSIONS",
    "aggregate_jury_votes",
    "atomic_write_json",
    "debate_directory_name",
    "debate_symbol_dir",
    "normalize_debate_symbol",
    "prepare_debate_directories",
    "validate_ballot",
    "validate_debate_artifacts",
    "validate_opening",
    "validate_rebuttal",
]
