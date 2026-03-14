#!/usr/bin/env python
from __future__ import annotations

import os
import warnings

from fastmcp import FastMCP

from services.research.stock_analysis import (
    analyze_stock_dynamics_and_valuation as _analyze_stock_dynamics_and_valuation,
    run_enhanced_pe_pb_analysis,
    summarize_stock_price_dynamics,
)


warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"pkg_resources")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"py_mini_racer")

mcp = FastMCP("StockAnalysis")


@mcp.tool()
def analyze_stock_dynamics_and_valuation(symbol: str, today_time: str):
    return _analyze_stock_dynamics_and_valuation(symbol=symbol, today_time=today_time)


if __name__ == "__main__":
    port = int(os.getenv("ANALYSIS_HTTP_PORT", "8004"))
    mcp.run(transport="streamable-http", port=port)

