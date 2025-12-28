#!/usr/bin/env python
"""宏观经济总结读取工具
=========================

设计背景
--------
研究团队会周期性地将“深度研究”结果整理为 Markdown，并固定保存在
``data/macro_economy`` 目录下。相比实时联网检索，本地文件具有更高的可控性
与可追溯性，因此我们提供一个 MCP 工具让 Agent 直接读取这些总结。

设计原则
--------
1. **只读访问**：工具仅返回 Markdown 原文，避免被 Agent 意外修改。
2. **智能选稿**：优先根据用户提供的 `range_hint`（如“20250501-20251006”）
   精确匹配文件；若未提供或找不到，则按文件名排序后选择最新文件。
3. **稳健反馈**：若目录或文件不存在，返回结构化的错误提示，方便 Agent
   进行 fallback 策略。
4. **易扩展**：所有关键路径、端口通过环境变量可配置，默认仍指向项目内的
   `data/macro_economy`。

工具作用
--------
- 在投研 Agent 工作流中快速注入宏观背景，辅助基本面或量化分析。
- 为策略回测提供“时点”宏观快照，可结合 `range_hint` 精准获取某一份总结。
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from fastmcp import FastMCP
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from agent_tools.logging_utils import init_tool_logger

load_dotenv()

mcp = FastMCP("MacroSummary")
logger = init_tool_logger("macro_summary")

MACRO_BASE_DIR = Path(os.getenv("MACRO_DIR", "data/macro_economy")).resolve()
SUPPORTED_EXTS = {".md", ".markdown", ".txt"}


def list_macro_files() -> List[Path]:
    """列出目录下所有支持的宏观经济总结文件。"""
    if not MACRO_BASE_DIR.is_dir():
        logger.warning("宏观目录不存在: %s", MACRO_BASE_DIR)
        return []
    files = [p for p in MACRO_BASE_DIR.iterdir() if p.suffix.lower() in SUPPORTED_EXTS]
    files.sort(key=lambda p: p.name)
    return files


def _extract_date_from_name(path: Path) -> Optional[datetime]:
    stem = path.stem
    for idx in range(len(stem) - 7):
        chunk = stem[idx : idx + 8]
        if not chunk.isdigit():
            continue
        try:
            return datetime.strptime(chunk, "%Y%m%d")
        except ValueError:
            continue
    return None


def pick_macro_file(range_hint: Optional[str], today_dt: Optional[datetime]) -> Optional[Path]:
    """根据 hint 或 today_time 挑选文件。"""
    files = list_macro_files()
    if not files:
        return None

    if range_hint:
        hint_lower = range_hint.lower()
        for candidate in files:
            if hint_lower in candidate.stem.lower():
                return candidate

    if today_dt:
        dated_candidates: List[Tuple[datetime, Path]] = []
        for candidate in files:
            file_dt = _extract_date_from_name(candidate)
            if file_dt is None:
                continue
            if file_dt.date() < today_dt.date():
                dated_candidates.append((file_dt, candidate))
        if dated_candidates:
            dated_candidates.sort(key=lambda pair: pair[0])
            return dated_candidates[-1][1]
        logger.warning(
            "未找到早于 %s 的宏观总结文件，将返回最新一篇。",
            today_dt.date().isoformat(),
        )

    return files[-1]  # fallback


def read_file(path: Path) -> str:
    """以 UTF-8 读取文件内容。"""
    return path.read_text(encoding="utf-8")


@mcp.tool()
def get_macro_summary(today_time: Optional[str] = None) -> str:
    """返回today_time的最新宏观经济总结，用于当前宏观形势分析

    Args:
        today_time: 当前日期，格式要求（YYYY-MM-DD），例如：”2025-09-01”

    Returns:
        Markdown 内容；若失败则返回错误说明。
    """

    # TODO: 根据 today_time 自动筛选更接近的摘要文件
    logger.info("get_macro_summary 请求: today_time=%s", today_time)
    today_dt = None
    if today_time:
        try:
            today_dt = datetime.strptime(today_time, "%Y-%m-%d")
        except ValueError:
            logger.warning("today_time 无法解析，回退到最新文件: %s", today_time)

    target = pick_macro_file(range_hint=None, today_dt=today_dt)
    if not target:
        message = (
            "[MacroSummary] 未找到任何宏观经济文件。"
            f" 请确认目录 {MACRO_BASE_DIR} 是否存在。"
        )
        logger.error("get_macro_summary 失败: %s", message)
        return message

    try:
        content = read_file(target)
    except OSError as exc:
        message = f"[MacroSummary] 读取文件失败 ({target.name}): {exc}"
        logger.exception("get_macro_summary 读取失败: %s", target)
        return message

    logger.info("get_macro_summary 返回: file=%s, size=%d", target.name, len(content))
    return content


if __name__ == "__main__":
    port = int(os.getenv("MACRO_HTTP_PORT", "8007"))
    mcp.run(transport="streamable-http", port=port)
