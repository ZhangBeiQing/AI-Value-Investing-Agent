"""Publish one validated dashboard verdict into the fixed-tracked paper book."""

from __future__ import annotations

import fcntl
import json
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.trading.analysis_index import update_analysis_index
from services.trading.decision_contract import validate_stock_decision_entry
from services.trading.post_trade_pipeline import (
    execute_trade_from_decision,
    merge_trade_summary,
    validate_decision_json,
)


LOGGER = get_logger("DashboardDecisionPublisher")
SIGNATURE = "book-fixed_tracked"
PROJECT_DATA = Path(__file__).resolve().parents[2] / "data"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _already_published(data_dir: Path, symbol: str, run_date: str, verdict: dict[str, Any]) -> bool:
    operations = data_dir / "agent_data" / SIGNATURE / "stock_decisions.json"
    if not operations.is_file():
        return False
    existing = [item for item in _read_json(operations)
                if item.get("symbol") == symbol and item.get("operation_date") == run_date]
    if not existing:
        return False
    if len(existing) != 1 or any(
        {key: value for key, value in item.items() if key != "operation_date"} != verdict
        for item in existing
    ):
        raise RuntimeError(f"{symbol} 在 {run_date} 已有不同的正式决策；拒绝覆盖或重复记账")
    return True


def _check_official_daily_decision(data_dir: Path, symbol: str, run_date: str) -> dict[str, Any]:
    official = data_dir / "skill_runs" / run_date / "fixed_tracked" / "05_decision.json"
    if not official.is_file():
        return {}
    decision = _read_json(official)
    if any(item.get("symbol") == symbol for item in decision.get("stock_decisions", [])):
        raise RuntimeError(f"正式 05_decision.json 已包含 {symbol}，网页任务不能再次入账")
    return decision


def _position_trade_present(data_dir: Path, symbol: str, run_date: str, action: str, quantity: int) -> bool:
    """Fail closed when a prior process may have appended a trade but no 06 log."""
    path = data_dir / "agent_data" / SIGNATURE / "position" / "position.jsonl"
    if not path.is_file():
        return False
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            record = json.loads(line)
            this_action = record.get("this_action") or {}
            if (record.get("date") == run_date and this_action.get("action") == action.lower()
                    and (this_action.get("trades") or {}).get(symbol) == quantity):
                return True
    return False


def publish_dashboard_verdict(
    data_dir: Path, workspace: Path, *, symbol: str, run_date: str,
    verdict: dict[str, Any],
) -> None:
    """Validate and merge a single-stock verdict; never modify the manual holdings file."""
    if data_dir.resolve() != PROJECT_DATA.resolve():
        raise ValueError("正式发布只能使用项目 data 目录")
    errors = validate_stock_decision_entry(verdict, expected_symbol=symbol)
    if errors:
        raise ValueError("单股裁决校验失败：" + "; ".join(errors[:5]))
    lock_path = data_dir / "web_research_runs" / "jobs" / ".publish.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        prior = _already_published(data_dir, symbol, run_date, verdict)
        official = _check_official_daily_decision(data_dir, symbol, run_date)
        decision = {
            "summary_date": run_date,
            "stock_decisions": [verdict],
            "system_risk_notes": official.get("system_risk_notes", []),
            "system_focus_items": official.get("system_focus_items", []),
        }
        errors = validate_decision_json(decision, expected_symbols=[symbol], book_type="fixed_tracked")
        if errors:
            raise ValueError("05 决策校验失败：" + "; ".join(errors[:5]))
        decision_path = workspace / "05_decision.json"
        if decision_path.is_file() and _read_json(decision_path) != decision:
            raise RuntimeError("任务目录中的 05 决策与本次裁决不一致")
        decision_path.write_text(json.dumps(decision, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        log_path = workspace / "06_execution_log.json"
        action = verdict["action_type"]
        if action in {"BUY", "SELL"}:
            if not log_path.is_file() and _position_trade_present(
                data_dir, symbol, run_date, action, verdict["action_num"]
            ):
                raise RuntimeError("发现同日同数量的虚拟交易但没有本任务 06 日志；为防止重复记账，请人工核查")
            execute_trade_from_decision(
                run_date, output_dir=workspace, signature=SIGNATURE, book_type="fixed_tracked"
            )
        elif not log_path.is_file():
            log_path.write_text(json.dumps({
                "summary_date": run_date, "signature": SIGNATURE,
                "decision_file": str(decision_path), "actions": [],
                "no_trade": True, "position_record_written": False,
                "execution_mode": "dashboard_signal_only",
            }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        merge_trade_summary(
            run_date, output_dir=workspace, signature=SIGNATURE, book_type="fixed_tracked"
        )
        update_analysis_index(
            decision,
            "fixed_tracked",
            run_date,
            data_dir / "skill_runs",
            snapshot_root=workspace.parent.parent,
        )
        LOGGER.info("网页单股裁决已并入正式虚拟账本: %s %s action=%s previous=%s",
                    run_date, symbol, action, prior)
