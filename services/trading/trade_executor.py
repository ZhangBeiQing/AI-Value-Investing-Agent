"""Local trade execution service implementation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from core.logging import init_tool_logger
from core.runtime_state import get_config_value, write_config_value
from tools.price_tools import compute_total_value, get_latest_position, get_prev_close_prices


PROJECT_ROOT = Path(__file__).resolve().parents[2]
logger = init_tool_logger("TradeTools")


def _require_signature() -> str:
    signature = get_config_value("SIGNATURE")
    if signature is None:
        raise ValueError("SIGNATURE environment variable is not set")
    return signature


def _require_today_date() -> str:
    today_date = get_config_value("TODAY_DATE")
    if not today_date:
        raise ValueError("TODAY_DATE environment variable is not set")
    return today_date


def _validate_lot_size(action: str, trades: Dict[str, int], today_date: str) -> dict | None:
    for symbol, amount in trades.items():
        if not isinstance(amount, int) or amount <= 0:
            logger.warning("%s 数量非法: %s -> %s", action, symbol, amount)
            return {"error": f"Invalid amount for {symbol}. Must be a positive integer.", "symbol": symbol, "date": today_date}
        if amount % 100 != 0:
            logger.warning("%s 数量非100整数倍: %s -> %s", action, symbol, amount)
            action_cn = "买入" if action == "buy" else "卖出"
            return {
                "error": f"股票最小交易单位为1手(100股)，{action_cn}数量必须是100的整数倍。你输入的股票列表中，{symbol}{action_cn}数量不是100的整数倍！！ 本次所有股票{action_cn}操作全部无效，请重新{action_cn}",
                "symbol": symbol,
                "date": today_date,
            }
    return None


def _resolve_stock_prices(today_date: str, trades: Dict[str, int], action: str) -> dict | None:
    symbols = list(trades.keys())
    price_data = get_prev_close_prices(today_date, symbols)
    stock_prices: Dict[str, float] = {}
    for symbol in symbols:
        price = price_data.get(f"{symbol}_price")
        if price is None:
            logger.error("%s 缺少价格: %s", action, symbol)
            return {"error": f"Symbol {symbol} not found! This action will not be allowed.", "symbol": symbol, "date": today_date}
        stock_prices[symbol] = float(price)
    return stock_prices


def _position_file(signature: str) -> Path:
    return PROJECT_ROOT / "data" / "agent_data" / signature / "position" / "position.jsonl"


def _append_position_record(position_file_path: Path, record: dict) -> None:
    position_file_path.parent.mkdir(parents=True, exist_ok=True)
    needs_newline = False
    if position_file_path.exists() and position_file_path.stat().st_size > 0:
        with open(position_file_path, "rb") as handle:
            handle.seek(-1, 2)
            if handle.read(1) != b"\n":
                needs_newline = True

    with open(position_file_path, "a", encoding="utf-8") as handle:
        if needs_newline:
            handle.write("\n")
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def execute_buy_orders(trades: Dict[str, int]) -> Dict[str, Any]:
    signature = _require_signature()
    today_date = _require_today_date()
    logger.info("%s buy 请求: trades=%s", today_date, trades)

    if not trades or not isinstance(trades, dict):
        logger.warning("%s buy 参数非法: %s", today_date, trades)
        return {"error": "Invalid trades parameter. Must be a non-empty dictionary.", "date": today_date}

    validation_error = _validate_lot_size("buy", trades, today_date)
    if validation_error:
        return validation_error

    try:
        current_position, current_action_id = get_latest_position(today_date, signature)
    except Exception as exc:
        logger.exception("buy 获取仓位失败")
        return {"error": f"Failed to get latest position: {exc}", "date": today_date}

    try:
        portfolio_value = compute_total_value(today_date, current_position)
    except Exception as exc:
        logger.warning("buy 计算总资产失败，跳过10%%限制: %s", exc)
        portfolio_value = None

    stock_prices = _resolve_stock_prices(today_date, trades, "buy")
    if not isinstance(stock_prices, dict) or "error" in stock_prices:
        return stock_prices  # type: ignore[return-value]

    violations = []
    if portfolio_value is not None and portfolio_value > 0:
        per_stock_limit = portfolio_value * 0.10
        for symbol, amount in trades.items():
            order_value = stock_prices[symbol] * amount
            if order_value > per_stock_limit + 1e-6:
                violations.append(
                    {"symbol": symbol, "order_value": round(order_value, 2), "limit": round(per_stock_limit, 2)}
                )
        if violations:
            logger.warning("buy 超过单日10%%限制: %s", violations)
            return {
                "error": "为了控制风险，用户强制要求使用金字塔分批买入法，单只股票单日买入金额不得超过总资产的10%。请调整仓位，具体规则参考decision_rules的【决策与风控要求】中的【加仓节奏：金字塔分批建仓】",
                "violations": violations,
                "date": today_date,
            }

    total_cost = sum(stock_prices[symbol] * amount for symbol, amount in trades.items())
    try:
        cash_left = float(current_position["CASH"]) - float(total_cost)
    except Exception as exc:
        logger.exception("buy 计算现金失败")
        return {"error": f"Failed to calculate cash balance: {exc}", "date": today_date}

    if cash_left < 0:
        logger.warning("buy 现金不足: need=%s, cash=%s", total_cost, current_position.get("CASH", 0))
        return {
            "error": "Insufficient cash for total purchase! This action will not be allowed.",
            "required_cash": total_cost,
            "cash_available": current_position.get("CASH", 0),
            "trades": trades,
            "date": today_date,
        }

    new_position = current_position.copy()
    new_position["CASH"] = round(float(cash_left), 2)
    for symbol, amount in trades.items():
        new_position[symbol] = new_position.get(symbol, 0) + amount

    try:
        total_value = compute_total_value(today_date, new_position)
    except Exception:
        total_value = None

    record = {
        "date": today_date,
        "id": current_action_id + 1,
        "this_action": {"action": "buy", "trades": trades},
        "positions": new_position,
        "total_value": total_value,
    }
    logger.info("buy 写入 position.jsonl: %s", record)
    _append_position_record(_position_file(signature), record)

    write_config_value("IF_TRADE", True)
    logger.info("buy 成功: 新仓位=%s", new_position)
    return new_position


def execute_sell_orders(trades: Dict[str, int]) -> Dict[str, Any]:
    logger.info("sell 请求: trades=%s", trades)
    signature = _require_signature()
    today_date = _require_today_date()

    if not trades or not isinstance(trades, dict):
        logger.warning("sell 参数非法: %s", trades)
        return {"error": "Invalid trades parameter. Must be a non-empty dictionary.", "date": today_date}

    validation_error = _validate_lot_size("sell", trades, today_date)
    if validation_error:
        return validation_error

    try:
        current_position, current_action_id = get_latest_position(today_date, signature)
    except Exception as exc:
        logger.exception("sell 获取仓位失败")
        return {"error": f"Failed to get latest position: {exc}", "date": today_date}

    stock_prices = _resolve_stock_prices(today_date, trades, "sell")
    if not isinstance(stock_prices, dict) or "error" in stock_prices:
        return stock_prices  # type: ignore[return-value]

    for symbol, amount in trades.items():
        if symbol not in current_position:
            logger.warning("sell 无持仓: %s", symbol)
            return {"error": f"No position for {symbol}! This action will not be allowed.", "symbol": symbol, "date": today_date}
        if current_position[symbol] < amount:
            logger.warning("sell 持仓不足: %s 请求=%s 当前=%s", symbol, amount, current_position.get(symbol))
            return {
                "error": "Insufficient shares! This action will not be allowed.",
                "have": current_position.get(symbol, 0),
                "want_to_sell": amount,
                "symbol": symbol,
                "date": today_date,
            }

    new_position = current_position.copy()
    total_proceeds = 0.0
    for symbol, amount in trades.items():
        new_position[symbol] -= amount
        total_proceeds += stock_prices[symbol] * amount
        if new_position[symbol] == 0:
            new_position.pop(symbol, None)

    new_position["CASH"] = round(float(new_position.get("CASH", 0.0)) + float(total_proceeds), 2)

    record = {
        "date": today_date,
        "id": current_action_id + 1,
        "this_action": {"action": "sell", "trades": trades},
        "positions": new_position,
        "total_value": None,
    }
    try:
        record["total_value"] = compute_total_value(today_date, new_position)
    except Exception:
        pass

    logger.info("sell 写入 position.jsonl: %s", record)
    _append_position_record(_position_file(signature), record)

    write_config_value("IF_TRADE", True)
    logger.info("sell 成功: 新仓位=%s", new_position)
    return new_position


__all__ = ["execute_buy_orders", "execute_sell_orders"]
