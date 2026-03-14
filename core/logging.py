"""Structured logging helpers for the skill-only architecture."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _sanitize(value: str, fallback: str) -> str:
    candidate = value.strip() if value else ""
    if not candidate:
        return fallback
    safe = [ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in candidate]
    result = "".join(safe).strip("_")
    return result or fallback


def _detect_model_name() -> str:
    env_signature = os.environ.get("SIGNATURE", "").strip()
    if env_signature:
        return _sanitize(env_signature, "unknown_model")
    default_signature = os.environ.get("DEFAULT_SIGNATURE", "").strip()
    if default_signature:
        return _sanitize(default_signature, "unknown_model")
    return "unknown_model"


def init_tool_logger(
    tool_name: str,
    *,
    model_name: Optional[str] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    safe_tool = _sanitize(tool_name, "unknown_tool")
    safe_model = _sanitize(model_name or _detect_model_name(), "unknown_model")
    logger = logging.getLogger(f"mcp.{safe_model}.{safe_tool}")
    if logger.handlers:
        return logger

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = PROJECT_ROOT / "logs" / safe_model / f"{safe_tool}_tool"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{timestamp}.log"

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    logger.info("日志初始化: %s", log_path)
    return logger


__all__ = ["init_tool_logger"]
