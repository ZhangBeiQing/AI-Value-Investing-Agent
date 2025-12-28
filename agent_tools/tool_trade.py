from fastmcp import FastMCP
import sys
import os
from typing import Dict, List, Optional, Any
# Add project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from tools.price_tools import (
    get_prev_close_prices,
    get_latest_position,
    compute_total_value,
)
import json
from tools.general_tools import get_config_value,write_config_value
from logging_utils import init_tool_logger

mcp = FastMCP("TradeTools")
logger = init_tool_logger(mcp.name)



@mcp.tool()
def buy(trades: Dict[str, int]) -> Dict[str, Any]:
    """
    Buy stocks function (supports multiple stocks)
    
    This function simulates stock buying operations for multiple stocks, including the following steps:
    1. Get current position and operation ID
    2. Get stock opening prices for all requested stocks
    3. Validate buy conditions (sufficient cash for total purchase)
    4. Update position (increase stock quantities, decrease cash)
    5. Record transaction to position.jsonl file
    6."为了控制风险，用户强制要求使用金字塔分批买入法，单只股票单日买入金额不得超过总资产的10%，否则buy会返回报错
    
    Args:
        trades: Dictionary where key is stock symbol and value is buy quantity
                Example: {"600276.SH": 10, "002371.SZ": 5, "002352.SZ": 2}
        
    Returns:
        Dict[str, Any]:
          - Success: Returns new position dictionary (containing stock quantities and cash balance)
          - Failure: Returns {"error": error message, ...} dictionary
        
    Raises:
        ValueError: Raised when SIGNATURE environment variable is not set
        
    Example:
        >>> result = buy({"600276.SH": 10, "002371.SZ": 5, "002352.SZ": 2})
        >>> print(result)  # {"600276.SH": 110, "002371.SZ": 10, "002352.SZ": 2, ..., "CASH": 4500.0}
    """
    # Step 1: Get environment variables and basic information
    # Get signature (model name) from environment variable, used to determine data storage path
    signature = get_config_value("SIGNATURE")
    if signature is None:
        raise ValueError("SIGNATURE environment variable is not set")
    
    # Get current trading date from environment variable
    today_date = get_config_value("TODAY_DATE")
    logger.info(f"{today_date} buy 请求: trades={trades}")
    # Validate input
    if not trades or not isinstance(trades, dict):
        logger.warning(f"{today_date} buy 参数非法: %s", trades)
        return {"error": "Invalid trades parameter. Must be a non-empty dictionary.", "date": today_date}
    
    # Check for invalid amounts
    for symbol, amount in trades.items():
        if not isinstance(amount, int) or amount <= 0:
            logger.warning("buy 金额非法: %s -> %s", symbol, amount)
            return {"error": f"Invalid amount for {symbol}. Must be a positive integer.", "symbol": symbol, "date": today_date}
        if amount % 100 != 0:
            logger.warning("buy 数量非100整数倍: %s -> %s", symbol, amount)
            return {
                "error": f"股票最小交易单位为1手(100股)，买入数量必须是100的整数倍。你输入的股票列表中，{symbol}买入数量不是100的整数倍！！ 本次所有股票买入操作全部无效，请重新买入",
                "symbol": symbol,
                "date": today_date,
            }
    
    # Step 2: Get current latest position and operation ID
    # get_latest_position returns two values: position dictionary and current maximum operation ID
    # This ID is used to ensure each operation has a unique identifier
    try:
        current_position, current_action_id = get_latest_position(today_date, signature)
    except Exception as e:
        logger.exception("buy 获取仓位失败")
        return {"error": f"Failed to get latest position: {str(e)}", "date": today_date}

    try:
        portfolio_value = compute_total_value(today_date, current_position)
    except Exception as exc:
        logger.warning("buy 计算总资产失败，跳过10%%限制: %s", exc)
        portfolio_value = None
    
    # Step 3: Get stock opening prices for all requested stocks
    # Use get_open_prices function to get the opening prices of all specified stocks for the day
    # If any stock symbol does not exist or price data is missing, KeyError exception will be raised
    symbols = list(trades.keys())
    # Step 3: Get current prices for symbols
    symbols = list(trades.keys())
    price_data = get_prev_close_prices(today_date, symbols)
    stock_prices = {}
    for symbol in symbols:
        price = price_data.get(f'{symbol}_price')
        if price is None:
            logger.error(f"{today_date} buy 缺少价格: {symbol}")
            return {"error": f"Symbol {symbol} not found! This action will not be allowed.", "symbol": symbol, "date": today_date}
        
        stock_prices[symbol] = price

    # enforce per-stock 10% rule if portfolio value known
    violations = []
    if portfolio_value is not None and portfolio_value > 0:
        per_stock_limit = portfolio_value * 0.10
        for symbol, amount in trades.items():
            order_value = stock_prices[symbol] * amount
            if order_value > per_stock_limit + 1e-6:
                violations.append(
                    {
                        "symbol": symbol,
                        "order_value": round(order_value, 2),
                        "limit": round(per_stock_limit, 2),
                    }
                )
        if violations:
            logger.warning("buy 超过单日10%%限制: %s", violations)
            return {
                "error": "为了控制风险，用户强制要求使用金字塔分批买入法，单只股票单日买入金额不得超过总资产的10%。请调整仓位，具体规则参考decision_rules的【决策与风控要求】中的【加仓节奏：金字塔分批建仓】",
                "violations": violations,
                "date": today_date,
            }

    # Step 4: Calculate total cost and validate buy conditions
    total_cost = 0
    for symbol, amount in trades.items():
        total_cost += stock_prices[symbol] * amount
    
    # Check if cash balance is sufficient for total purchase
    try:
        cash_left = current_position["CASH"] - total_cost
    except Exception as e:
        logger.exception("buy 计算现金失败")
        return {"error": f"Failed to calculate cash balance: {str(e)}", "date": today_date}

    if cash_left < 0:
        # Insufficient cash, return error message
        logger.warning("buy 现金不足: need=%s, cash=%s", total_cost, current_position.get("CASH", 0))
        return {"error": "Insufficient cash for total purchase! This action will not be allowed.", 
                "required_cash": total_cost, 
                "cash_available": current_position.get("CASH", 0), 
                "trades": trades, 
                "date": today_date}
    
    # Step 5: Execute buy operations, update position
    # Create a copy of current position to avoid directly modifying original data
    new_position = current_position.copy()
    
    # Decrease cash balance（统一保留两位小数，避免 numpy.float/float round 属性差异）
    new_position["CASH"] = round(float(cash_left), 2)
    
    # Increase stock position quantities for all trades
    for symbol, amount in trades.items():
        new_position[symbol] = new_position.get(symbol, 0) + amount
    
    # Step 6: Record transaction to position.jsonl file
    # Build file path: {project_root}/data/agent_data/{signature}/position/position.jsonl
    # Use append mode ("a") to write new transaction record
    # Each operation ID increments by 1, ensuring uniqueness of operation sequence
    position_file_path = os.path.join(project_root, "data", "agent_data", signature, "position", "position.jsonl")
    
    # Check if the file ends with a newline
    needs_newline = False
    if os.path.exists(position_file_path) and os.path.getsize(position_file_path) > 0:
        with open(position_file_path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                needs_newline = True

    with open(position_file_path, "a", encoding="utf-8") as f:
        if needs_newline:
            f.write("\n")
        # 计算当日总资产价值 total_value
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
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    
    # Step 7: Return updated position
    write_config_value("IF_TRADE", True)
    logger.info("buy 成功: 新仓位=%s", new_position)
    return new_position

@mcp.tool()
def sell(trades: Dict[str, int]) -> Dict[str, Any]:
    """
    Sell stocks function (supports multiple stocks)

    Follows同buy逻辑: 校验输入 -> 校验持仓 -> 计算成交 -> 写入 position.jsonl。
    """
    logger.info("sell 请求: trades=%s", trades)
    signature = get_config_value("SIGNATURE")
    if signature is None:
        raise ValueError("SIGNATURE environment variable is not set")

    today_date = get_config_value("TODAY_DATE")

    if not trades or not isinstance(trades, dict):
        logger.warning("sell 参数非法: %s", trades)
        return {"error": "Invalid trades parameter. Must be a non-empty dictionary.", "date": today_date}

    for symbol, amount in trades.items():
        if not isinstance(amount, int) or amount <= 0:
            logger.warning("sell 数量非法: %s -> %s", symbol, amount)
            return {"error": f"Invalid amount for {symbol}. Must be a positive integer.", "symbol": symbol, "date": today_date}
        if amount % 100 != 0:
            logger.warning("sell 数量非100整数倍: %s -> %s", symbol, amount)
            return {
                "error": f"股票最小交易单位为1手(100股)，卖出数量必须是100的整数倍。你输入的股票列表中，{symbol}卖出数量不是100的整数倍！！ 本次所有股票卖出操作全部无效，请重新卖出",
                "symbol": symbol,
                "date": today_date,
            }

    try:
        current_position, current_action_id = get_latest_position(today_date, signature)
    except Exception as exc:
        logger.exception("sell 获取仓位失败")
        return {"error": f"Failed to get latest position: {exc}", "date": today_date}

    symbols = list(trades.keys())
    symbols = list(trades.keys())
    price_data = get_prev_close_prices(today_date, symbols)
    stock_prices = {}
    
    for symbol in symbols:
        price = price_data.get(f"{symbol}_price")
        if price is None:
             logger.error("sell 缺少价格: %s", symbol)
             return {"error": f"Symbol {symbol} not found! This action will not be allowed.", "symbol": symbol, "date": today_date}
        
        stock_prices[symbol] = price

    for symbol, amount in trades.items():
        if symbol not in current_position:
            logger.warning("sell 无持仓: %s", symbol)
            return {"error": f"No position for {symbol}! This action will not be allowed.", "symbol": symbol, "date": today_date}
        if current_position[symbol] < amount:
            logger.warning(
                "sell 持仓不足: %s 请求=%s 当前=%s",
                symbol,
                amount,
                current_position.get(symbol),
            )
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
        proceeds = stock_prices[symbol] * amount
        total_proceeds += proceeds
        if new_position[symbol] == 0:
            new_position.pop(symbol, None)

    new_cash = float(new_position.get("CASH", 0.0)) + float(total_proceeds)
    new_position["CASH"] = round(new_cash, 2)

    position_file_path = os.path.join(
        project_root,
        "data",
        "agent_data",
        signature,
        "position",
        "position.jsonl",
    )
    os.makedirs(os.path.dirname(position_file_path), exist_ok=True)
    record = {
        "date": today_date,
        "id": current_action_id + 1,
        "this_action": {"action": "sell", "trades": trades},
        "positions": new_position,
        "total_value": None,
    }
    # 计算当日总资产价值 total_value
    try:
        from tools.price_tools import compute_total_value
        record["total_value"] = compute_total_value(today_date, new_position)
    except Exception:
        pass
    # Check if the file ends with a newline
    needs_newline = False
    if os.path.exists(position_file_path) and os.path.getsize(position_file_path) > 0:
        with open(position_file_path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                needs_newline = True

    with open(position_file_path, "a", encoding="utf-8") as f:
        if needs_newline:
            f.write("\n")
        logger.info("sell 写入 position.jsonl: %s", record)
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    write_config_value("IF_TRADE", True)
    logger.info("sell 成功: 新仓位=%s", new_position)
    return new_position


if __name__ == "__main__":
    port = int(os.getenv("TRADE_HTTP_PORT", "8002"))
    mcp.run(transport="streamable-http", port=port)
