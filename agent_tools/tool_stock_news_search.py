#!/usr/bin/env python
from __future__ import annotations

import os

from dotenv import load_dotenv
from fastmcp import FastMCP

from services.research.news_summary import search_stock_news as _search_stock_news


load_dotenv()

mcp = FastMCP("StockNewsSearch")


@mcp.tool()
def search_stock_news(symbol: str, today_time: str) -> str:
    return _search_stock_news(symbol=symbol, today_time=today_time)


if __name__ == "__main__":
    port = int(os.getenv("NEWS_HTTP_PORT", "8006"))
    mcp.run(transport="streamable-http", port=port)

