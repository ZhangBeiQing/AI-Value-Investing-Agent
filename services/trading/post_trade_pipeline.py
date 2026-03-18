"""Post-trade service pipeline for skill-only workflow."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from core.runtime_state import get_config_value, write_config_value
from services.trading.price_reference import add_no_trade_record
from services.trading.trade_executor import execute_buy_orders, execute_sell_orders
from services.trading.trade_summary import (
    get_portfolio_historical_context,
    initialize_data_files,
    process_and_merge_operations,
    save_daily_operations,
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def resolve_output_dir(base_dir: str, run_date: str) -> Path:
    return Path(base_dir) / "skill_runs" / run_date


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_runtime_env(output_dir: Path, signature: str, today_date: str) -> Path:
    runtime_path_str = os.environ.get("RUNTIME_ENV_PATH", "").strip()
    runtime_path = Path(runtime_path_str) if runtime_path_str else output_dir / "runtime_env.json"

    payload: dict = {}
    if runtime_path.exists():
        try:
            payload = json.loads(runtime_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}

    payload.update({"SIGNATURE": signature, "TODAY_DATE": today_date})
    payload.setdefault("IF_TRADE", False)

    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.environ["RUNTIME_ENV_PATH"] = str(runtime_path)
    os.environ["SIGNATURE"] = signature
    os.environ["TODAY_DATE"] = today_date
    return runtime_path


def load_initial_cash(default_value: float = 500000.0) -> float:
    try:
        initial_cash = get_config_value("INITIAL_CASH")
        if initial_cash is None:
            initial_cash = get_config_value("INIT_CASH")
        if initial_cash is None:
            initial_cash = os.environ.get("INITIAL_CASH") or os.environ.get("INIT_CASH")
        if initial_cash is None:
            return default_value
        return float(initial_cash)
    except Exception:
        return default_value


def ensure_position_file(signature: str, today_date: str) -> Path:
    from configs.stock_pool import TRACKED_A_STOCKS

    position_file = PROJECT_ROOT / "data" / "agent_data" / signature / "position" / "position.jsonl"
    if position_file.exists():
        return position_file

    position_file.parent.mkdir(parents=True, exist_ok=True)
    initial_cash = load_initial_cash()
    positions = {entry.symbol: 0 for entry in TRACKED_A_STOCKS}
    positions["CASH"] = initial_cash
    record = {
        "date": today_date,
        "id": 0,
        "positions": positions,
        "this_action": {"action": "init"},
        "total_value": initial_cash,
    }
    position_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
    return position_file


def extract_trades(decision: dict) -> Tuple[Dict[str, int], Dict[str, int]]:
    buys: Dict[str, int] = {}
    sells: Dict[str, int] = {}
    for op in decision.get("stock_operations", []) or []:
        action = (op.get("action_type") or "").upper()
        symbol = op.get("stock_code")
        qty = op.get("action_num")
        if not symbol or not isinstance(qty, int) or qty <= 0:
            continue
        if action == "BUY":
            buys[symbol] = qty
        elif action == "SELL":
            sells[symbol] = qty
    return buys, sells


def validate_decision_json(decision: dict) -> List[str]:
    errors: List[str] = []
    if not isinstance(decision, dict):
        return ["decision 不是有效的 JSON 对象"]

    required_top = ["summary_date", "stock_operations", "system_risk_notes", "system_focus_items"]
    for key in required_top:
        if key not in decision:
            errors.append(f"缺少顶层字段: {key}")

    ops = decision.get("stock_operations")
    if not isinstance(ops, list) or not ops:
        errors.append("stock_operations 必须是非空数组")
        return errors

    required_fields = [
        "stock_code",
        "stock_name",
        "action_type",
        "action_num",
        "action_price",
        "reason",
        "confidence_score",
        "position_size",
        "price_target",
        "stop_loss",
        "last_analysis_date",
        "key_observations",
        "individual_risk_notes",
        "individual_focus",
    ]
    valid_actions = {"BUY", "SELL", "HOLD", "FLAT"}
    for idx, op in enumerate(ops):
        if not isinstance(op, dict):
            errors.append(f"stock_operations[{idx}] 不是对象")
            continue
        for field in required_fields:
            if field not in op:
                errors.append(f"stock_operations[{idx}] 缺少字段: {field}")
        action = (op.get("action_type") or "").upper()
        if action and action not in valid_actions:
            errors.append(f"stock_operations[{idx}] action_type 非法: {action}")

    try:
        from configs.stock_pool import TRACKED_A_STOCKS

        tracked = {entry.symbol for entry in TRACKED_A_STOCKS}
        present = {op.get("stock_code") for op in ops if isinstance(op, dict)}
        missing = sorted(sym for sym in tracked if sym not in present)
        if missing:
            errors.append(f"缺少股票池标的: {', '.join(missing)}")
    except Exception:
        pass

    return errors


def _tool_result_to_payload(result: dict) -> dict:
    return {
        "is_error": bool(result.get("error")),
        "data": result,
        "structured_content": None,
        "content": None,
    }


def _extract_error_from_tool_payload(payload: dict) -> str | None:
    data = payload.get("data")
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, str) and err.strip():
            return err.strip()
    return None


def call_trade_functions(buys: Dict[str, int], sells: Dict[str, int]) -> List[dict]:
    results: List[dict] = []
    if sells:
        result = execute_sell_orders(sells)
        results.append({"tool": "sell", "trades": sells, "result": _tool_result_to_payload(result)})
    if buys:
        result = execute_buy_orders(buys)
        results.append({"tool": "buy", "trades": buys, "result": _tool_result_to_payload(result)})
    return results


def execute_trade_from_decision(
    run_date: str,
    *,
    base_dir: str = "data",
    output_dir: str | Path | None = None,
    decision_file: str | Path | None = None,
    skip_validate: bool = False,
    signature: str = "",
) -> Path:
    resolved_output_dir = Path(output_dir) if output_dir else resolve_output_dir(base_dir, run_date)
    decision_path = Path(decision_file) if decision_file else resolved_output_dir / "05_decision.json"
    if not decision_path.exists():
        raise SystemExit(f"Decision file not found: {decision_path}")

    decision = load_json(decision_path)
    if not skip_validate:
        validation_errors = validate_decision_json(decision)
        if validation_errors:
            msg = "决策 JSON 校验失败：\n- " + "\n- ".join(validation_errors)
            raise SystemExit(msg)
    summary_date = decision.get("summary_date") or run_date

    resolved_signature = signature or (get_config_value("SIGNATURE") or "deepseek-reasoner")
    ensure_runtime_env(resolved_output_dir, resolved_signature, summary_date)
    ensure_position_file(resolved_signature, summary_date)

    buys, sells = extract_trades(decision)

    execution_log = {
        "summary_date": summary_date,
        "signature": resolved_signature,
        "decision_file": str(decision_path),
        "execution_mode": "local_python_functions",
        "executed_at": datetime.now().isoformat(),
        "actions": [],
        "no_trade": False,
    }

    try:
        if not buys and not sells:
            add_no_trade_record(summary_date, resolved_signature)
            execution_log["no_trade"] = True
        else:
            results = call_trade_functions(buys, sells)
            execution_log["actions"] = results
            tool_errors: List[str] = []
            for action in results:
                err = _extract_error_from_tool_payload((action or {}).get("result") or {})
                if err:
                    tool_errors.append(f"{action.get('tool')}: {err}")
            if tool_errors:
                raise RuntimeError("交易执行失败（工具返回 error）：\n- " + "\n- ".join(tool_errors))
        if get_config_value("IF_TRADE"):
            write_config_value("IF_TRADE", False)
    except Exception as exc:
        execution_log["error"] = str(exc)
        raise

    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    log_path = resolved_output_dir / "06_execution_log.json"
    log_path.write_text(json.dumps(execution_log, ensure_ascii=False, indent=2), encoding="utf-8")
    return log_path


def merge_trade_summary(
    run_date: str,
    *,
    base_dir: str = "data",
    output_dir: str | Path | None = None,
    decision_file: str | Path | None = None,
    signature: str = "",
) -> tuple[Path, Path]:
    resolved_output_dir = Path(output_dir) if output_dir else resolve_output_dir(base_dir, run_date)
    decision_path = Path(decision_file) if decision_file else resolved_output_dir / "05_decision.json"
    if not decision_path.exists():
        raise SystemExit(f"Decision file not found: {decision_path}")

    decision = load_json(decision_path)
    summary_date = decision.get("summary_date") or run_date
    resolved_signature = signature or os.environ.get("SIGNATURE") or get_config_value("SIGNATURE") or "deepseek-reasoner"

    initialize_data_files(resolved_signature)
    saved_operations = save_daily_operations(resolved_signature, decision)
    process_and_merge_operations(resolved_signature, saved_operations)

    daily_summary = {
        "summary_date": summary_date,
        "signature": resolved_signature,
        "decision_file": str(decision_path),
        "system_risk_notes": decision.get("system_risk_notes", []),
        "system_focus_items": decision.get("system_focus_items", []),
        "saved_operations_count": len(saved_operations),
    }
    daily_summary_path = resolved_output_dir / "07_daily_summary.json"
    daily_summary_path.write_text(json.dumps(daily_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    stock_codes = [op.get("stock_code") for op in saved_operations if op.get("stock_code")]
    history_merge = {
        "summary_date": summary_date,
        "signature": resolved_signature,
        "saved_operations": saved_operations,
        "latest_portfolio_context": get_portfolio_historical_context(resolved_signature, stock_codes, n=2),
    }
    history_path = resolved_output_dir / "08_history_merge.json"
    history_path.write_text(json.dumps(history_merge, ensure_ascii=False, indent=2), encoding="utf-8")
    return daily_summary_path, history_path


def run_post_trade(run_date: str, *, base_dir: str = "data", signature: str = "") -> tuple[Path, Path, Path]:
    log_path = execute_trade_from_decision(run_date, base_dir=base_dir, signature=signature)
    daily_summary_path, history_path = merge_trade_summary(run_date, base_dir=base_dir, signature=signature)
    return log_path, daily_summary_path, history_path
