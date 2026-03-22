"""Macro summary service implementation for the skill-only architecture."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from dotenv import load_dotenv

from core.logging import init_tool_logger
from shared_data_access import (
    load_or_build_macro_objective_panel,
    render_macro_objective_panel_markdown,
)


load_dotenv()

logger = init_tool_logger("macro_summary")

MACRO_BASE_DIR = Path(os.getenv("MACRO_DIR", "data/macro_economy")).resolve()
SUPPORTED_EXTS = {".md", ".markdown", ".txt"}


def list_macro_files() -> List[Path]:
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
            if file_dt.date() <= today_dt.date():
                dated_candidates.append((file_dt, candidate))
        if dated_candidates:
            dated_candidates.sort(key=lambda pair: pair[0])
            return dated_candidates[-1][1]
        logger.warning("未找到早于 %s 的宏观总结文件，将返回最新一篇。", today_dt.date().isoformat())

    return files[-1]


def read_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def get_macro_summary(today_time: Optional[str] = None) -> str:
    logger.info("get_macro_summary 请求: today_time=%s", today_time)
    today_dt = None
    if today_time:
        try:
            today_dt = datetime.strptime(today_time, "%Y-%m-%d")
        except ValueError:
            logger.warning("today_time 格式异常，将忽略该筛选条件: %s", today_time)

    target = pick_macro_file(range_hint=None, today_dt=today_dt)
    if target is None:
        message = f"未找到任何宏观经济总结文件，请检查目录：{MACRO_BASE_DIR}"
        logger.error(message)
        return message

    try:
        content = read_file(target)
    except OSError as exc:
        message = f"读取宏观总结文件失败：{target} ({exc})"
        logger.exception(message)
        return message

    panel_markdown = ""
    run_date = today_dt.strftime("%Y-%m-%d") if today_dt else datetime.now().strftime("%Y-%m-%d")
    try:
        panel_payload = load_or_build_macro_objective_panel(run_date)
        panel_markdown = render_macro_objective_panel_markdown(panel_payload)
    except Exception as exc:
        logger.warning("加载宏观客观数据面板失败: %s", exc)

    logger.info("get_macro_summary 命中文件: %s", target)
    if panel_markdown:
        return f"{content.rstrip()}\n\n{panel_markdown}\n"
    return content


__all__ = [
    "get_macro_summary",
    "list_macro_files",
    "pick_macro_file",
    "read_file",
]
