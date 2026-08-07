"""D+1 open-price simulation and isolated 06-08 generation."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.backtest.coverage import next_experiment_trading_date
from services.backtest.experiment import BacktestExperiment
from services.backtest.ledger import BacktestLedger
from services.backtest.locking import locked_experiment_stage
from services.trading.post_trade_pipeline import validate_decision_json
from services.trading.trade_summary import (
    get_portfolio_historical_context,
    initialize_data_files,
    process_and_merge_operations,
    save_daily_operations,
    use_agent_data_root,
)
from shared_data_access.historical_prices import exact_price
from utlity import parse_symbol


LOGGER = get_logger("BacktestExecution")


def _entry_symbol(entry: dict[str, Any]) -> str:
    value = entry.get("symbol") or entry.get("stock_code")
    return str(value or "").strip()


def _lot_size(symbol: str) -> int:
    info = parse_symbol(symbol)
    return 100 if info.is_cn_market() else 1


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
    return exact_price(
        parse_symbol(symbol),
        execution_date,
        "开盘",
        base_dir=experiment.context.source_data_root,
    )


def _rank_buy(entry: dict[str, Any], held_symbols: set[str]) -> tuple[float, int, str]:
    symbol = _entry_symbol(entry)
    try:
        confidence = float(entry.get("confidence_score") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return (-confidence, 0 if symbol in held_symbols else 1, symbol)


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
    state = ledger.latest(on_or_before=execution_date)
    positions = dict(state.positions)
    held_symbols = {
        symbol
        for symbol, shares in positions.items()
        if symbol != "CASH" and float(shares or 0) > 0
    }
    entries = list(decision.get("stock_decisions") or [])
    sell_entries = [
        entry for entry in entries if str(entry.get("action_type") or "").upper() == "SELL"
    ]
    buy_entries = sorted(
        (
            entry
            for entry in entries
            if str(entry.get("action_type") or "").upper() == "BUY"
        ),
        key=lambda entry: _rank_buy(entry, held_symbols),
    )

    orders: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    order_ids: list[str] = []

    for entry in sell_entries + buy_entries:
        symbol = _entry_symbol(entry)
        action = str(entry.get("action_type") or "").upper()
        requested = int(entry.get("action_num") or 0)
        order_id = _stable_order_id(
            experiment.experiment_id,
            decision_date,
            symbol,
            action,
        )
        order: dict[str, Any] = {
            "order_id": order_id,
            "decision_date": decision_date,
            "execution_date": execution_date,
            "symbol": symbol,
            "action": action,
            "requested_shares": requested,
            "filled_shares": 0,
            "execution_price": None,
            "status": "rejected",
            "reason": "",
            "adjustments": [],
        }
        if not symbol or requested <= 0:
            order["reason"] = "invalid_order"
            orders.append(order)
            continue
        lot_size = _lot_size(symbol)
        normalized = requested - requested % lot_size
        if normalized != requested:
            order["adjustments"].append(
                {
                    "type": "lot_size_round_down",
                    "from_shares": requested,
                    "to_shares": normalized,
                    "lot_size": lot_size,
                }
            )
        if normalized <= 0:
            order["reason"] = "below_minimum_lot"
            orders.append(order)
            continue
        price = _execution_price(experiment, symbol, execution_date)
        if price is None:
            order["status"] = "pending"
            order["reason"] = "exact_open_price_missing"
            orders.append(order)
            continue

        filled = normalized
        if action == "SELL":
            available = int(float(positions.get(symbol, 0) or 0))
            filled = min(filled, available)
            filled -= filled % lot_size
            if filled != normalized:
                order["adjustments"].append(
                    {
                        "type": "position_cap",
                        "from_shares": normalized,
                        "to_shares": filled,
                    }
                )
            if filled <= 0:
                order["reason"] = "insufficient_position"
                orders.append(order)
                continue
            positions[symbol] = available - filled
            if positions[symbol] == 0:
                positions.pop(symbol, None)
            positions["CASH"] = round(
                float(positions.get("CASH", 0.0)) + price * filled,
                4,
            )
        else:
            cash = float(positions.get("CASH", 0.0) or 0.0)
            affordable = int(cash // (price * lot_size)) * lot_size
            filled = min(filled, affordable)
            if filled != normalized:
                order["adjustments"].append(
                    {
                        "type": "cash_cap",
                        "from_shares": normalized,
                        "to_shares": filled,
                    }
                )
            if filled <= 0:
                order["reason"] = "insufficient_cash"
                orders.append(order)
                continue
            positions["CASH"] = round(cash - price * filled, 4)
            positions[symbol] = int(float(positions.get(symbol, 0) or 0)) + filled

        order.update(
            {
                "filled_shares": filled,
                "execution_price": price,
                "status": "filled",
                "reason": "",
            }
        )
        orders.append(order)
        order_ids.append(order_id)
        actions.append(
            {
                "order_id": order_id,
                "action": action.lower(),
                "symbol": symbol,
                "shares": filled,
                "price": price,
            }
        )

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
        process_and_merge_operations(experiment.signature, saved_operations)
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
