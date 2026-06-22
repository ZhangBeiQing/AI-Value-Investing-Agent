"""Post-trade service pipeline for skill-only workflow."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from core.runtime_state import get_config_value, write_config_value
from core.logging import init_component_logger

logger = init_component_logger("PostTradePipeline")
from services.trading.price_reference import add_no_trade_record
from services.trading.trade_executor import execute_buy_orders, execute_sell_orders
from services.trading.trade_summary import (
    get_portfolio_historical_context,
    initialize_data_files,
    process_and_merge_operations,
    save_daily_operations,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VALID_BOOK_TYPES = {"fixed_tracked", "short_book", "long_book"}


def _normalize_book_type(book_type: str | None) -> str:
    raw = (book_type or "").strip()
    if not raw:
        return ""
    if raw not in VALID_BOOK_TYPES:
        raise ValueError(f"Unsupported book_type: {raw}")
    return raw


def _default_signature(book_type: str | None) -> str:
    normalized = _normalize_book_type(book_type)
    return f"book-{normalized}" if normalized else "book-fixed_tracked"


def resolve_output_dir(base_dir: str, run_date: str, book_type: str = "") -> Path:
    base_output_dir = Path(base_dir) / "skill_runs" / run_date
    normalized_book_type = _normalize_book_type(book_type)
    return base_output_dir / normalized_book_type if normalized_book_type else base_output_dir


def _infer_book_type_from_output_dir(output_dir: Path) -> str:
    return output_dir.name if output_dir.name in VALID_BOOK_TYPES else ""


def _resolve_output_dir(
    run_date: str,
    *,
    base_dir: str,
    output_dir: str | Path | None = None,
    book_type: str = "",
) -> Path:
    if output_dir:
        return Path(output_dir)
    return resolve_output_dir(base_dir, run_date, book_type=book_type)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _decision_entries(decision: dict) -> List[dict]:
    entries = decision.get("stock_decisions")
    if isinstance(entries, list) and entries:
        return entries
    legacy_entries = decision.get("stock_operations")
    if isinstance(legacy_entries, list):
        return legacy_entries
    return []


def _dated_decision_entries(decision: dict, summary_date: str) -> List[dict]:
    dated_entries: List[dict] = []
    for entry in _decision_entries(decision):
        dated_entry = dict(entry)
        dated_entry["operation_date"] = summary_date
        symbol = _entry_symbol(dated_entry)
        if symbol and "symbol" not in dated_entry:
            dated_entry["symbol"] = symbol
        dated_entries.append(dated_entry)
    return dated_entries


def _entry_symbol(entry: dict) -> str | None:
    symbol = entry.get("symbol") or entry.get("stock_code")
    return symbol if isinstance(symbol, str) and symbol.strip() else None


def _collect_decision_symbols(decision: dict) -> List[str]:
    symbols: List[str] = []
    for entry in _decision_entries(decision):
        symbol = _entry_symbol(entry)
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _load_expected_symbols_from_snapshot(output_dir: Path) -> List[str]:
    snapshot_path = output_dir / "02_basic_snapshot_payload.json"
    if not snapshot_path.exists():
        return []
    try:
        payload = load_json(snapshot_path)
    except Exception:
        return []

    stocks = payload.get("stocks")
    if isinstance(stocks, dict):
        return [symbol for symbol in stocks.keys() if isinstance(symbol, str) and symbol.strip()]
    return []


def _resolve_expected_symbols(output_dir: Path, decision: dict) -> List[str]:
    snapshot_symbols = _load_expected_symbols_from_snapshot(output_dir)
    if snapshot_symbols:
        return snapshot_symbols
    decision_symbols = _collect_decision_symbols(decision)
    if decision_symbols:
        return decision_symbols
    try:
        from configs.stock_pool import TRACKED_A_STOCKS

        return [entry.symbol for entry in TRACKED_A_STOCKS]
    except Exception:
        return []


def ensure_runtime_env(output_dir: Path, signature: str, today_date: str) -> Path:
    runtime_path_str = os.environ.get("RUNTIME_ENV_PATH", "").strip()
    runtime_path = (
        Path(runtime_path_str) if runtime_path_str else output_dir / "runtime_env.json"
    )

    payload: dict = {}
    if runtime_path.exists():
        try:
            payload = json.loads(runtime_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}

    payload.update({"SIGNATURE": signature, "TODAY_DATE": today_date})
    payload.setdefault("IF_TRADE", False)

    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
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


def ensure_position_file(signature: str, today_date: str, *, symbols: List[str] | None = None) -> Path:
    position_file = (
        PROJECT_ROOT / "data" / "agent_data" / signature / "position" / "position.jsonl"
    )
    if position_file.exists():
        return position_file

    position_file.parent.mkdir(parents=True, exist_ok=True)
    initial_cash = load_initial_cash()
    target_symbols = [symbol for symbol in (symbols or []) if symbol]
    if not target_symbols:
        from configs.stock_pool import TRACKED_A_STOCKS

        target_symbols = [entry.symbol for entry in TRACKED_A_STOCKS]
    positions = {symbol: 0 for symbol in target_symbols}
    positions["CASH"] = initial_cash
    record = {
        "date": today_date,
        "id": 0,
        "positions": positions,
        "this_action": {"action": "init"},
        "total_value": initial_cash,
    }
    position_file.write_text(
        json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return position_file


def extract_trades(decision: dict) -> Tuple[Dict[str, int], Dict[str, int]]:
    buys: Dict[str, int] = {}
    sells: Dict[str, int] = {}
    for op in _decision_entries(decision):
        action = (op.get("action_type") or "").upper()
        symbol = _entry_symbol(op)
        qty = op.get("action_num")
        if not symbol or not isinstance(qty, int) or qty <= 0:
            continue
        if action == "BUY":
            buys[symbol] = qty
        elif action == "SELL":
            sells[symbol] = qty
    return buys, sells


def _analysis_profile_fields(book_type: str, entry: dict | None = None) -> List[str]:
    normalized = (book_type or "").strip().lower()
    short_book_fields = [
        "catalyst_and_momentum",
        "trading_mode",
        "risk_reward_setup",
        "max_holding_days",
    ]
    if normalized == "short_book":
        return short_book_fields
    if normalized:
        return []
    probe = entry or {}
    if any(field in probe for field in short_book_fields):
        return short_book_fields
    return []


def validate_decision_json(
    decision: dict,
    *,
    expected_symbols: List[str] | None = None,
    book_type: str = "",
) -> List[str]:
    errors: List[str] = []
    if not isinstance(decision, dict):
        return ["decision 不是有效的 JSON 对象"]

    required_top = ["summary_date", "system_risk_notes", "system_focus_items"]
    for key in required_top:
        if key not in decision:
            errors.append(f"缺少顶层字段: {key}")

    ops = _decision_entries(decision)
    if not isinstance(ops, list) or not ops:
        errors.append("stock_decisions 必须是非空数组")
        return errors

    common_required_fields = [
        "symbol",
        "stock_name",
        "scan",
        "deep_analysis_date",
        "history_anchor",
        "delta_summary",
        "price_impression",
        "key_facts",
        "inferences",
        "motion",
        "court",
        "recommended_action",
        "action_type",
        "action_num",
        "price_target",
        "key_risks",
        "next_day_watchlist",
        "confidence_score",
    ]
    valid_actions = {"BUY", "SELL", "HOLD", "FLAT"}
    for idx, op in enumerate(ops):
        if not isinstance(op, dict):
            errors.append(f"stock_decisions[{idx}] 不是对象")
            continue
        required_fields = common_required_fields.copy()
        insert_at = required_fields.index("key_facts")
        required_fields[insert_at:insert_at] = _analysis_profile_fields(book_type, op)
        for field in required_fields:
            if field == "symbol":
                if not _entry_symbol(op):
                    errors.append(f"stock_decisions[{idx}] 缺少字段: symbol")
                continue
            if field not in op:
                errors.append(f"stock_decisions[{idx}] 缺少字段: {field}")
        action = (op.get("action_type") or "").upper()
        if action and action not in valid_actions:
            errors.append(f"stock_decisions[{idx}] action_type 非法: {action}")
        qty = op.get("action_num")
        if not isinstance(qty, int):
            errors.append(f"stock_decisions[{idx}] action_num 必须是整数")
        elif action in {"BUY", "SELL"} and qty <= 0:
            errors.append(f"stock_decisions[{idx}] {action} 时 action_num 必须大于 0")

    target_symbols = [symbol for symbol in (expected_symbols or []) if symbol]
    if target_symbols:
        present = {_entry_symbol(op) for op in ops if isinstance(op, dict)}
        missing = sorted(sym for sym in target_symbols if sym not in present)
        if missing:
            logger.info(
                "未深度分析的全池标的(不影响校验): %s",
                ", ".join(missing),
            )

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
        results.append(
            {"tool": "sell", "trades": sells, "result": _tool_result_to_payload(result)}
        )
    if buys:
        result = execute_buy_orders(buys)
        results.append(
            {"tool": "buy", "trades": buys, "result": _tool_result_to_payload(result)}
        )
    return results


def _load_matching_execution_log(
    log_path: Path,
    *,
    summary_date: str,
    signature: str,
    buys: Dict[str, int],
    sells: Dict[str, int],
) -> dict | None:
    """复用同一决策的成功执行日志，避免汇总失败后重复真实交易。"""
    if not log_path.exists():
        return None

    try:
        payload = json.loads(log_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"已有执行日志无法读取: {log_path}") from exc

    if payload.get("error"):
        return None

    expected_actions: list[dict] = []
    if sells:
        expected_actions.append({"tool": "sell", "trades": sells})
    if buys:
        expected_actions.append({"tool": "buy", "trades": buys})
    actual_actions = [
        {"tool": action.get("tool"), "trades": action.get("trades") or {}}
        for action in payload.get("actions", [])
    ]
    actions_match = (
        actual_actions == expected_actions
        and bool(payload.get("no_trade")) == (not buys and not sells)
    )
    if (
        payload.get("summary_date") == summary_date
        and payload.get("signature") == signature
        and actions_match
    ):
        return payload

    raise RuntimeError(
        "检测到已有成功执行记录，但它与当前决策不一致。"
        f"为防止重复交易，已停止执行: {log_path}"
    )


def execute_trade_from_decision(
    run_date: str,
    *,
    base_dir: str = "data",
    output_dir: str | Path | None = None,
    decision_file: str | Path | None = None,
    skip_validate: bool = False,
    signature: str = "",
    book_type: str = "",
) -> Path:
    resolved_output_dir = _resolve_output_dir(
        run_date,
        base_dir=base_dir,
        output_dir=output_dir,
        book_type=book_type,
    )
    decision_path = (
        Path(decision_file)
        if decision_file
        else resolved_output_dir / "05_decision.json"
    )
    if not decision_path.exists():
        raise SystemExit(f"Decision file not found: {decision_path}")

    decision = load_json(decision_path)
    inferred_book_type = _normalize_book_type(
        book_type or _infer_book_type_from_output_dir(resolved_output_dir)
    )
    expected_symbols = _resolve_expected_symbols(resolved_output_dir, decision)
    if not skip_validate:
        validation_errors = validate_decision_json(
            decision,
            expected_symbols=expected_symbols,
            book_type=inferred_book_type,
        )
        if validation_errors:
            msg = "决策 JSON 校验失败：\n- " + "\n- ".join(validation_errors)
            raise SystemExit(msg)
    summary_date = decision.get("summary_date") or run_date

    if signature:
        resolved_signature = signature
    elif inferred_book_type:
        resolved_signature = _default_signature(inferred_book_type)
    else:
        resolved_signature = get_config_value("SIGNATURE") or "book-fixed_tracked"
    ensure_runtime_env(resolved_output_dir, resolved_signature, summary_date)
    ensure_position_file(resolved_signature, summary_date, symbols=expected_symbols)

    buys, sells = extract_trades(decision)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    log_path = resolved_output_dir / "06_execution_log.json"
    if _load_matching_execution_log(
        log_path,
        summary_date=summary_date,
        signature=resolved_signature,
        buys=buys,
        sells=sells,
    ):
        logger.info(
            "检测到同一决策已成功执行，跳过重复交易: date=%s, signature=%s",
            summary_date,
            resolved_signature,
        )
        return log_path

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
                err = _extract_error_from_tool_payload(
                    (action or {}).get("result") or {}
                )
                if err:
                    tool_errors.append(f"{action.get('tool')}: {err}")
            if tool_errors:
                raise RuntimeError(
                    "交易执行失败（工具返回 error）：\n- " + "\n- ".join(tool_errors)
                )
        if get_config_value("IF_TRADE"):
            write_config_value("IF_TRADE", False)
    except Exception as exc:
        execution_log["error"] = str(exc)
        raise

    log_path.write_text(
        json.dumps(execution_log, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return log_path


def merge_trade_summary(
    run_date: str,
    *,
    base_dir: str = "data",
    output_dir: str | Path | None = None,
    decision_file: str | Path | None = None,
    signature: str = "",
    book_type: str = "",
) -> tuple[Path, Path]:
    resolved_output_dir = _resolve_output_dir(
        run_date,
        base_dir=base_dir,
        output_dir=output_dir,
        book_type=book_type,
    )
    decision_path = (
        Path(decision_file)
        if decision_file
        else resolved_output_dir / "05_decision.json"
    )
    if not decision_path.exists():
        raise SystemExit(f"Decision file not found: {decision_path}")

    decision = load_json(decision_path)
    summary_date = decision.get("summary_date") or run_date
    inferred_book_type = _normalize_book_type(
        book_type or _infer_book_type_from_output_dir(resolved_output_dir)
    )
    if signature:
        resolved_signature = signature
    elif inferred_book_type:
        resolved_signature = _default_signature(inferred_book_type)
    else:
        resolved_signature = (
            os.environ.get("SIGNATURE")
            or get_config_value("SIGNATURE")
            or "book-fixed_tracked"
        )

    initialize_data_files(resolved_signature)
    normalized_decision = dict(decision)
    normalized_decision["summary_date"] = summary_date
    saved_operations = save_daily_operations(resolved_signature, normalized_decision)
    process_and_merge_operations(resolved_signature, saved_operations)
    decision_operations = _dated_decision_entries(normalized_decision, summary_date)

    daily_summary = {
        "summary_date": summary_date,
        "signature": resolved_signature,
        "decision_file": str(decision_path),
        "system_risk_notes": decision.get("system_risk_notes", []),
        "system_focus_items": decision.get("system_focus_items", []),
        "saved_operations_count": len(decision_operations),
    }
    daily_summary_path = resolved_output_dir / "07_daily_summary.json"
    daily_summary_path.write_text(
        json.dumps(daily_summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    stock_codes = [
        _entry_symbol(op) for op in decision_operations if _entry_symbol(op)
    ]
    history_merge = {
        "summary_date": summary_date,
        "signature": resolved_signature,
        "saved_operations": decision_operations,
        "latest_portfolio_context": get_portfolio_historical_context(
            resolved_signature, stock_codes, n=2
        ),
    }
    history_path = resolved_output_dir / "08_history_merge.json"
    history_path.write_text(
        json.dumps(history_merge, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return daily_summary_path, history_path


def run_post_trade(
    run_date: str,
    *,
    base_dir: str = "data",
    signature: str = "",
    output_dir: str | Path | None = None,
    book_type: str = "",
) -> tuple[Path, Path, Path]:
    log_path = execute_trade_from_decision(
        run_date,
        base_dir=base_dir,
        output_dir=output_dir,
        signature=signature,
        book_type=book_type,
    )
    daily_summary_path, history_path = merge_trade_summary(
        run_date,
        base_dir=base_dir,
        output_dir=output_dir,
        signature=signature,
        book_type=book_type,
    )
    return log_path, daily_summary_path, history_path
