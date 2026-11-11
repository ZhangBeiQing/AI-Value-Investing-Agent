"""D+1 open-price simulation and isolated 06-08 generation."""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.backtest.coverage import next_experiment_trading_date, ensure_previous_days_completed
from services.backtest.experiment import BacktestExperiment
from services.backtest.ledger import BacktestLedger
from services.backtest.locking import locked_experiment_stage
from services.trading.post_trade_pipeline import validate_decision_json
from services.trading.trade_executor import _validate_lot_size
from services.trading.price_tools import _symbol_fx_rate
from services.trading.trade_summary import (
    get_portfolio_historical_context,
    initialize_data_files,
    process_and_merge_operations,
    save_daily_operations,
    use_agent_data_root,
)
from shared_data_access.historical_prices import exact_price
from shared_data_access.market_calendar import inspect_market_session
from commons import parse_symbol


LOGGER = get_logger("BacktestExecution")


def _entry_symbol(entry: dict[str, Any]) -> str:
    value = entry.get("symbol") or entry.get("stock_code")
    return str(value or "").strip()


def _stable_order_id(
    experiment_id: str,
    decision_date: str,
    symbol: str,
    action: str,
) -> str:
    return f"{experiment_id}:{decision_date}:{symbol}:{action}"


def _load_decision(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    errors = validate_decision_json(
        payload,
        book_type="fixed_tracked",
        allow_empty_stock_decisions=True,
    )
    if errors:
        raise ValueError("回测 05_decision.json 校验失败:\n- " + "\n- ".join(errors))
    return payload


def _execution_price(
    experiment: BacktestExperiment,
    symbol: str,
    execution_date: str,
) -> float | None:
    info = parse_symbol(symbol)
    if info.market in {"US", "HK"} and not inspect_market_session(
        execution_date, market=info.market, base_dir=experiment.context.source_data_root,
    ).is_trading_day:
        return None
    price = exact_price(
        info,
        execution_date,
        "开盘",
        base_dir=experiment.context.source_data_root,
    )
    return price if price is not None and math.isfinite(price) and price > 0 else None


@locked_experiment_stage
def simulate_post_trade(
    experiment: BacktestExperiment,
    *,
    decision_date: str,
    execution_date: str,
    decision_file: str | Path | None = None,
) -> tuple[Path, Path, Path]:
    """Execute one decision once, then update isolated trade memory."""

    expected_execution_date = next_experiment_trading_date(
        experiment,
        decision_date,
    )
    ensure_previous_days_completed(experiment, decision_date)
    if execution_date != expected_execution_date:
        raise ValueError(
            f"execution_date 必须是下一交易日 {expected_execution_date}，"
            f"实际传入 {execution_date}"
        )
    book_dir = (
        experiment.context.skill_runs_root
        / decision_date
        / "fixed_tracked"
    )
    decision_path = Path(decision_file) if decision_file else book_dir / "05_decision.json"
    if not decision_path.exists():
        raise FileNotFoundError(f"回测决策文件不存在: {decision_path}")
    log_path = book_dir / "06_execution_log.json"
    daily_summary_path = book_dir / "07_daily_summary.json"
    history_path = book_dir / "08_history_merge.json"
    if log_path.exists():
        existing = json.loads(log_path.read_text(encoding="utf-8"))
        if (
            existing.get("status") == "success"
            and existing.get("decision_date") == decision_date
            and existing.get("execution_date") == execution_date
        ):
            if not daily_summary_path.exists() or not history_path.exists():
                raise RuntimeError("06 已成功但 07/08 缺失，需修复归档；禁止再次成交")
            return log_path, daily_summary_path, history_path
        raise RuntimeError(f"已有不匹配的回测执行日志: {log_path}")

    decision = _load_decision(decision_path)
    ledger = BacktestLedger(
        agent_data_root=experiment.context.agent_data_root,
        signature=experiment.signature,
        source_data_root=experiment.context.source_data_root,
    )
    ledger.initialize(
        start_date=experiment.start_date,
        initial_cash=experiment.initial_cash,
        symbols=list(experiment.base_universe),
    )
    if any(record.get("decision_date") == decision_date for record in ledger.records()):
        raise RuntimeError("隔离账本已有当日执行但缺少成功 06；拒绝重复成交，请核对中断现场")
    state = ledger.latest(on_or_before=execution_date)
    positions = dict(state.positions)
    entries = list(decision.get("stock_decisions") or [])
    if decision.get("summary_date") != decision_date:
        raise ValueError("05 的 summary_date 与决策日不一致")
    symbols = [_entry_symbol(entry) for entry in entries]
    if len(symbols) != len(set(symbols)):
        raise ValueError("同一天每只股票只能有一个最终决策")
    allowed = set(experiment.base_universe) | set(positions)
    if set(symbols) - allowed:
        raise ValueError("决策包含固定股池与隔离持仓以外的股票")
    orders: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    order_ids: list[str] = []

    # Same sell-then-buy batches and quantity rules as the daily executor.
    # A batch fails as a whole: the simulator never invents quantities or ranks research.
    for action in ("SELL", "BUY"):
        batch = [entry for entry in entries if entry.get("action_type") == action]
        if not batch:
            continue
        trades = {_entry_symbol(entry): entry["action_num"] for entry in batch}
        lot_error = _validate_lot_size(action.lower(), trades, decision_date)
        batch_orders = []
        for symbol, quantity in trades.items():
            local_price = _execution_price(experiment, symbol, execution_date)
            rate = _symbol_fx_rate(symbol)
            batch_orders.append({
                "order_id": _stable_order_id(experiment.experiment_id, decision_date, symbol, action),
                "decision_date": decision_date, "execution_date": execution_date,
                "symbol": symbol, "action": action, "requested_shares": quantity,
                "filled_shares": 0, "execution_price": None, "price_local": local_price,
                "fx_rate": rate, "price_cny": local_price * rate if local_price is not None else None,
                "status": "rejected", "reason": "", "adjustments": [],
            })
        reason = "invalid_trade_unit" if lot_error else ""
        if not reason and any(order["price_cny"] is None for order in batch_orders):
            for order in batch_orders:
                order.update(status="pending", reason="exact_open_price_missing")
            orders.extend(batch_orders)
            continue
        if not reason and action == "SELL" and any(
            quantity > float(positions.get(symbol, 0)) for symbol, quantity in trades.items()
        ):
            reason = "insufficient_position"
        total = sum((order["price_cny"] or 0) * order["requested_shares"] for order in batch_orders)
        if not reason and action == "BUY" and total > float(positions.get("CASH", 0)):
            reason = "insufficient_cash"
        if reason:
            for order in batch_orders:
                order["reason"] = reason
            orders.extend(batch_orders)
            continue
        for order in batch_orders:
            symbol = order["symbol"]
            quantity = order["requested_shares"]
            positions[symbol] = float(positions.get(symbol, 0)) + (quantity if action == "BUY" else -quantity)
            if positions[symbol] == 0:
                positions.pop(symbol)
            order.update(status="filled", filled_shares=quantity, execution_price=order["price_cny"])
            actions.append({"order_id": order["order_id"], "action": action.lower(),
                            "symbol": symbol, "shares": quantity, "price": order["price_cny"],
                            "price_local": order["price_local"], "fx_rate": order["fx_rate"]})
            order_ids.append(order["order_id"])
        positions["CASH"] = round(float(positions.get("CASH", 0)) + (total if action == "SELL" else -total), 2)
        orders.extend(batch_orders)

    new_state = ledger.append_execution(
        decision_date=decision_date,
        execution_date=execution_date,
        positions=positions,
        actions=actions,
        order_ids=order_ids,
    )
    ledger.append_order_records(orders)

    book_dir.mkdir(parents=True, exist_ok=True)
    execution_log = {
        "status": "success",
        "execution_mode": "backtest_next_open",
        "experiment_id": experiment.experiment_id,
        "signature": experiment.signature,
        "decision_date": decision_date,
        "execution_date": execution_date,
        "decision_file": str(decision_path),
        "executed_at": datetime.now().isoformat(),
        "orders": orders,
        "portfolio_adjustments": [
            {
                "order_id": order["order_id"],
                "status": order["status"],
                "reason": order["reason"],
                "adjustments": order.get("adjustments") or [],
            }
            for order in orders
            if order.get("reason") or order.get("adjustments")
        ],
        "actions": actions,
        "ending_positions": new_state.positions,
        "ending_total_value": new_state.total_value,
        "no_trade": not actions,
    }
    log_path.write_text(
        json.dumps(execution_log, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    with use_agent_data_root(experiment.context.agent_data_root):
        initialize_data_files(experiment.signature)
        normalized_decision = dict(decision)
        normalized_decision["summary_date"] = decision_date
        saved_operations = save_daily_operations(
            experiment.signature,
            normalized_decision,
        )
        snapshot_path = book_dir / "02_basic_snapshot_payload.json"
        research_symbols = (
            json.loads(snapshot_path.read_text(encoding="utf-8"))["stocks"]
            if snapshot_path.exists() else experiment.base_universe
        )
        snapshot_symbols = set(research_symbols) | {
            symbol for symbol, shares in new_state.positions.items()
            if symbol != "CASH" and shares > 0
        }
        process_and_merge_operations(
            experiment.signature, saved_operations, snapshot_symbols=snapshot_symbols,
        )
        stock_codes = [
            _entry_symbol(entry)
            for entry in normalized_decision.get("stock_decisions") or []
            if _entry_symbol(entry)
        ]
        portfolio_context = get_portfolio_historical_context(
            experiment.signature,
            stock_codes,
            n=2,
        )

    daily_summary = {
        "summary_date": decision_date,
        "execution_date": execution_date,
        "signature": experiment.signature,
        "decision_file": str(decision_path),
        "system_risk_notes": decision.get("system_risk_notes", []),
        "system_focus_items": decision.get("system_focus_items", []),
        "saved_operations_count": len(decision.get("stock_decisions") or []),
        "filled_order_count": len(actions),
        "ending_total_value": new_state.total_value,
    }
    daily_summary_path.write_text(
        json.dumps(daily_summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    history_payload = {
        "summary_date": decision_date,
        "execution_date": execution_date,
        "signature": experiment.signature,
        "saved_operations": decision.get("stock_decisions") or [],
        "latest_portfolio_context": portfolio_context,
        "ending_positions": new_state.positions,
    }
    history_path.write_text(
        json.dumps(history_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    LOGGER.info(
        "回测后处理完成: experiment=%s decision=%s execution=%s filled=%d",
        experiment.experiment_id,
        decision_date,
        execution_date,
        len(actions),
    )
    return log_path, daily_summary_path, history_path


__all__ = ["simulate_post_trade"]
