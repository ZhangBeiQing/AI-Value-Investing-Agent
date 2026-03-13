#!/usr/bin/env python3
"""Execute trades based on 05_decision.json using local Python functions."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agent_tools.tool_trade import buy as _buy_tool
from agent_tools.tool_trade import sell as _sell_tool
from tools.general_tools import get_config_value, write_config_value
from tools.price_tools import add_no_trade_record


buy = getattr(_buy_tool, "fn", _buy_tool)
sell = getattr(_sell_tool, "fn", _sell_tool)


def _resolve_output_dir(base_dir: str, run_date: str) -> Path:
    return Path(base_dir) / "skill_runs" / run_date


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_runtime_env(output_dir: Path, signature: str, today_date: str) -> Path:
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


def _load_initial_cash(default_value: float = 500000.0) -> float:
    cfg_path = PROJECT_ROOT / "configs" / "default_config.json"
    if not cfg_path.exists():
        return default_value
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
        return float((data.get("agent_config") or {}).get("initial_cash", default_value))
    except Exception:
        return default_value


def _ensure_position_file(signature: str, today_date: str) -> Path:
    from configs.stock_pool import TRACKED_A_STOCKS

    position_file = PROJECT_ROOT / "data" / "agent_data" / signature / "position" / "position.jsonl"
    if position_file.exists():
        return position_file

    position_file.parent.mkdir(parents=True, exist_ok=True)
    initial_cash = _load_initial_cash()
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


def _extract_trades(decision: dict) -> Tuple[Dict[str, int], Dict[str, int]]:
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


def _validate_decision_json(decision: dict) -> List[str]:
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


def _call_trade_functions(buys: Dict[str, int], sells: Dict[str, int]) -> List[dict]:
    results: List[dict] = []
    if sells:
        result = sell(sells)
        results.append({"tool": "sell", "trades": sells, "result": _tool_result_to_payload(result)})
    if buys:
        result = buy(buys)
        results.append({"tool": "buy", "trades": buys, "result": _tool_result_to_payload(result)})
    return results


def _update_manifest(output_dir: Path, step_name: str, status: str, extra: dict | None = None) -> None:
    manifest_path = output_dir / "run_manifest.json"
    if not manifest_path.exists():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return
    steps = manifest.setdefault("steps", {})
    step_info = steps.get(step_name, {})
    step_info.update({"status": status, "ended_at": datetime.now().isoformat()})
    if extra:
        step_info.update(extra)
    steps[step_name] = step_info
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute trades based on 05_decision.json")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--base-dir", default="data", help="Base data directory.")
    parser.add_argument("--output-dir", default="", help="Skill output directory. Default: data/skill_runs/YYYY-MM-DD")
    parser.add_argument("--decision-file", default="", help="Override decision JSON path (default: output_dir/05_decision.json).")
    parser.add_argument("--skip-validate", action="store_true", help="Skip decision JSON validation.")
    parser.add_argument("--signature", default="", help="Trading signature (fallback to env SIGNATURE).")
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else _resolve_output_dir(args.base_dir, args.run_date)
    decision_path = Path(args.decision_file) if args.decision_file else output_dir / "05_decision.json"
    if not decision_path.exists():
        raise SystemExit(f"Decision file not found: {decision_path}")

    decision = _load_json(decision_path)
    if not args.skip_validate:
        validation_errors = _validate_decision_json(decision)
        if validation_errors:
            msg = "决策 JSON 校验失败：\n- " + "\n- ".join(validation_errors)
            _update_manifest(output_dir, "execute_trade_from_decision", "failed", {"error": msg})
            raise SystemExit(msg)
    summary_date = decision.get("summary_date") or args.run_date

    signature = args.signature or (get_config_value("SIGNATURE") or "deepseek-reasoner")
    _ensure_runtime_env(output_dir, signature, summary_date)
    _ensure_position_file(signature, summary_date)

    buys, sells = _extract_trades(decision)

    execution_log = {
        "summary_date": summary_date,
        "signature": signature,
        "decision_file": str(decision_path),
        "execution_mode": "local_python_functions",
        "executed_at": datetime.now().isoformat(),
        "actions": [],
        "no_trade": False,
    }

    try:
        if not buys and not sells:
            add_no_trade_record(summary_date, signature)
            execution_log["no_trade"] = True
        else:
            results = _call_trade_functions(buys, sells)
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
        _update_manifest(output_dir, "execute_trade_from_decision", "failed", {"error": str(exc)})
        raise

    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "06_execution_log.json"
    log_path.write_text(json.dumps(execution_log, ensure_ascii=False, indent=2), encoding="utf-8")
    _update_manifest(output_dir, "execute_trade_from_decision", "done", {"log": str(log_path)})


if __name__ == "__main__":
    main()
