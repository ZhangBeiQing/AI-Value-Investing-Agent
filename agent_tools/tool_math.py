from fastmcp import FastMCP
import os
from dotenv import load_dotenv

from logging_utils import init_tool_logger

load_dotenv()

mcp = FastMCP("Math")
logger = init_tool_logger(mcp.name)

@mcp.tool()
def add(a: float, b: float) -> float:
    """Add two numbers (supports int and float)"""
    result = float(a) + float(b)
    logger.info("math.add: %s + %s = %s", a, b, result)
    return result

@mcp.tool()
def multiply(a: float, b: float) -> float:
    """Multiply two numbers (supports int and float)"""
    result = float(a) * float(b)
    logger.info("math.multiply: %s * %s = %s", a, b, result)
    return result

if __name__ == "__main__":
    port = int(os.getenv("MATH_HTTP_PORT", "8000"))
    mcp.run(transport="streamable-http", port=port)
