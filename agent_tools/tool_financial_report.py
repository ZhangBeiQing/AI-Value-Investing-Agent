#!/usr/bin/env python
from __future__ import annotations

import os

from dotenv import load_dotenv
from fastmcp import FastMCP

from services.research.financial_report import get_financial_report_summary as _get_financial_report_summary


load_dotenv()

mcp = FastMCP("FinancialReportSummary")


@mcp.tool()
def get_financial_report_summary(symbol: str, today_time: str) -> dict:
    return _get_financial_report_summary(symbol=symbol, today_time=today_time)


if __name__ == "__main__":
    port = int(os.getenv("FIN_REPORT_HTTP_PORT", "8008"))
    mcp.run(transport="streamable-http", port=port)
