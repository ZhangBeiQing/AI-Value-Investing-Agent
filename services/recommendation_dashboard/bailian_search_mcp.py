"""Local stdio bridge for the Bailian WebSearch Streamable HTTP MCP."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.server.fastmcp import FastMCP


SERVER_NAME = "WebSearch"
GLOBAL_CONFIG = Path.home() / ".config" / "opencode" / "opencode.json"
MCP = FastMCP(SERVER_NAME)


def _remote_config() -> tuple[str, dict[str, str]]:
    config = json.loads(GLOBAL_CONFIG.read_text(encoding="utf-8"))
    remote = config.get("mcp", {}).get(SERVER_NAME, {})
    url = remote.get("url")
    headers = remote.get("headers")
    if not isinstance(url, str) or not url.startswith("https://"):
        raise RuntimeError("OpenCode 全局配置中缺少有效的百炼 WebSearch MCP 地址")
    if not isinstance(headers, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in headers.items()):
        raise RuntimeError("OpenCode 全局配置中缺少百炼 WebSearch MCP 鉴权头")
    return url, headers


def _content_text(content: list[Any]) -> str:
    chunks: list[str] = []
    for block in content:
        text = getattr(block, "text", None)
        if isinstance(text, str):
            chunks.append(text)
        else:
            chunks.append(str(block))
    return "\n".join(chunks)


@MCP.tool()
async def bailian_web_search(query: str, count: int = 5) -> str:
    """Search the public web through Alibaba Cloud Bailian WebSearch."""
    if not query.strip():
        raise ValueError("搜索关键词不能为空")
    url, headers = _remote_config()
    arguments = {"query": query.strip(), "count": max(1, min(int(count), 10))}
    async with streamablehttp_client(url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("bailian_web_search", arguments)
    if getattr(result, "isError", False):
        raise RuntimeError(_content_text(result.content) or "百炼 WebSearch 调用失败")
    return _content_text(result.content)


if __name__ == "__main__":
    MCP.run(transport="stdio")
