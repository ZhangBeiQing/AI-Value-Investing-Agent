#!/usr/bin/env python
from __future__ import annotations

import os

from dotenv import load_dotenv
from fastmcp import FastMCP

from services.research.macro_summary import get_macro_summary as _get_macro_summary


load_dotenv()

mcp = FastMCP("MacroSummary")


@mcp.tool()
def get_macro_summary(today_time: str | None = None) -> str:
    return _get_macro_summary(today_time=today_time)


if __name__ == "__main__":
    port = int(os.getenv("MACRO_HTTP_PORT", "8007"))
    mcp.run(transport="streamable-http", port=port)

