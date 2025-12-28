"""Shared logging helper for MCP tools.

Each MCP tool should create exactly one logger instance per process so that
logs are routed to ``logs/{model}/{tool}/{timestamp}.log`` as required by the
project guidelines. The helper below normalizes model / tool names, creates
the directory hierarchy, and wires both file + console handlers with the
same formatter.
"""

from __future__ import annotations

import logging
import json
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
    config_path = PROJECT_ROOT / "configs" / "default_config.json"
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        enabled_models = [
            model for model in config.get("models", [])
            if model.get("enabled")
        ]
        
        if enabled_models:
            signature = enabled_models[0].get("signature")
            if signature:
                return _sanitize(signature, "unknown_model")

    except (FileNotFoundError, json.JSONDecodeError, IndexError):
        pass

    return "unknown_model"


def init_tool_logger(
    tool_name: str,
    *,
    model_name: Optional[str] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Create (or reuse) a structured logger for an MCP tool."""

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
