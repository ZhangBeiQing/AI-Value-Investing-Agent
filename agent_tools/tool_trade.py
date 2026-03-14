#!/usr/bin/env python
from __future__ import annotations

import os
from typing import Any, Dict

from fastmcp import FastMCP

from services.trading.trade_executor import (
    execute_buy_orders as _execute_buy_orders,
    execute_sell_orders as _execute_sell_orders,
)


mcp = FastMCP("TradeTools")


@mcp.tool()
def buy(trades: Dict[str, int]) -> Dict[str, Any]:
    return _execute_buy_orders(trades)


@mcp.tool()
def sell(trades: Dict[str, int]) -> Dict[str, Any]:
    return _execute_sell_orders(trades)


if __name__ == "__main__":
    port = int(os.getenv("TRADE_HTTP_PORT", "8002"))
    mcp.run(transport="streamable-http", port=port)
