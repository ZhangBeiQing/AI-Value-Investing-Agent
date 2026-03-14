"""Unified logging helpers inspired by AReaL, adapted for the skill-only architecture."""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_HEADER = "(AI-Stock)"
DATE_FORMAT = "%Y%m%d-%H:%M:%S"
LOG_PREFIX_WIDTH = 18
DEFAULT_LEVEL = logging.INFO

ANSI_RESET = "\033[0m"
ANSI_COLORS = {
    "blue": "\033[34m",
    "white": "\033[37m",
    "purple": "\033[35m",
    "green": "\033[32m",
    "cyan": "\033[36m",
    "yellow": "\033[33m",
    "red": "\033[31m",
    "bold_red": "\033[1;31m",
    "header": "\033[1;38;2;54;116;181m",
}

# Exact component-name to color mapping.
LOGGER_COLORS_EXACT: dict[str, str] = {
    "ManageDailyData": "blue",
    "SharedDataAccess": "blue",
    "CacheRegistry": "blue",
    "DataRefresh": "blue",
    "DailyPipeline": "white",
    "BuildAgentInput": "white",
    "PostTradePipeline": "white",
    "AgentPrompt": "purple",
    "LLMOutput": "purple",
    "MacroSummary": "purple",
    "StockAnalysis": "cyan",
    "NewsSummary": "purple",
    "StockNewsSearch": "purple",
    "FinancialReport": "green",
    "BasicStockInfo": "green",
    "BasicSnapshot": "green",
    "TradeTools": "cyan",
    "TradeSummary": "green",
    "PriceReference": "cyan",
    "RuntimeState": "white",
}

# Prefix patterns checked in order when exact match is not found.
LOGGER_PATTERNS: list[tuple[str, str]] = [
    ("Trade", "cyan"),
    ("Stock", "cyan"),
    ("Macro", "purple"),
    ("News", "purple"),
    ("Prompt", "purple"),
    ("Cache", "blue"),
    ("Shared", "blue"),
    ("Data", "blue"),
    ("Basic", "green"),
    ("Pipeline", "white"),
]

DEFAULT_LOGGER_COLOR = "white"

_HANDLER_LOCK = threading.Lock()


def register_logger_color(name: str, color: str) -> None:
    LOGGER_COLORS_EXACT[name] = color


def register_logger_pattern(pattern: str, color: str) -> None:
    LOGGER_PATTERNS.append((pattern, color))


def _sanitize(value: str, fallback: str) -> str:
    candidate = value.strip() if value else ""
    if not candidate:
        return fallback
    safe = [ch if ch.isalnum() or ch in ("-", "_", "/") else "_" for ch in candidate]
    result = "".join(safe).strip("_")
    return result or fallback


def _to_component_name(value: str, fallback: str = "UnknownComponent") -> str:
    raw = value.strip() if value else ""
    if not raw:
        return fallback
    if raw.startswith("[") and raw.endswith("]"):
        return raw
    if "." in raw:
        raw = raw.split(".")[-1]
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", raw) if part]
    if not parts:
        return fallback
    if len(parts) == 1 and any(ch.isupper() for ch in parts[0][1:]):
        return parts[0]
    return "".join(part[:1].upper() + part[1:] for part in parts)


def _default_filename_prefix(component_name: str) -> str:
    words = re.findall(r"[A-Z][a-z0-9]*|[a-z0-9]+", component_name)
    if not words:
        return _sanitize(component_name.lower(), "component")
    return "_".join(word.lower() for word in words)


def _detect_model_name() -> str:
    env_signature = os.environ.get("SIGNATURE", "").strip()
    if env_signature:
        return _sanitize(env_signature, "unknown_model")
    default_signature = os.environ.get("DEFAULT_SIGNATURE", "").strip()
    if default_signature:
        return _sanitize(default_signature, "unknown_model")
    return "unknown_model"


def _supports_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    stream = getattr(sys.stdout, "isatty", None)
    return bool(stream and stream())


def _resolve_logger_color(name: str) -> str:
    if name in LOGGER_COLORS_EXACT:
        return LOGGER_COLORS_EXACT[name]
    for pattern, color in LOGGER_PATTERNS:
        if name.startswith(pattern) or pattern in name:
            return color
    return DEFAULT_LOGGER_COLOR


class ComponentColorFormatter(logging.Formatter):
    """Console formatter with per-component colors and level overrides."""

    def __init__(self, *, use_color: bool) -> None:
        super().__init__("%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s", datefmt=DATE_FORMAT)
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        if not self.use_color:
            return f"{LOG_HEADER} {message}"

        if record.levelno >= logging.CRITICAL:
            body_color = ANSI_COLORS["bold_red"]
        elif record.levelno >= logging.ERROR:
            body_color = ANSI_COLORS["red"]
        elif record.levelno >= logging.WARNING:
            body_color = ANSI_COLORS["yellow"]
        else:
            body_color = ANSI_COLORS[_resolve_logger_color(record.name)]
        return f"{ANSI_COLORS['header']}{LOG_HEADER}{ANSI_RESET} {body_color}{message}{ANSI_RESET}"


class PlainFormatter(logging.Formatter):
    """Plain formatter used for dedicated log files."""

    def __init__(self, *, prefix: str | None = None) -> None:
        base = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s"
        if prefix:
            padded = prefix.ljust(LOG_PREFIX_WIDTH)
            base = f"{padded}{base}"
        super().__init__(f"{LOG_HEADER} {base}", datefmt=DATE_FORMAT)


def _add_handler_once(logger: logging.Logger, handler: logging.Handler, handler_name: str) -> None:
    with _HANDLER_LOCK:
        for existing in logger.handlers:
            if existing.get_name() == handler_name:
                handler.close()
                return
        handler.set_name(handler_name)
        logger.addHandler(handler)


def setup_file_logging(
    logger: logging.Logger,
    *,
    log_dir: str | Path,
    filename: str,
    merged_filename: str = "merged.log",
    merged_prefix: str | None = None,
    level: int = DEFAULT_LEVEL,
) -> logging.Logger:
    target_dir = Path(log_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    dedicated_path = target_dir / filename
    dedicated_handler = logging.FileHandler(dedicated_path, encoding="utf-8")
    dedicated_handler.setLevel(level)
    dedicated_handler.setFormatter(PlainFormatter())
    _add_handler_once(logger, dedicated_handler, f"file:{dedicated_path}")

    merged_path = target_dir / merged_filename
    merged_handler = logging.FileHandler(merged_path, encoding="utf-8")
    merged_handler.setLevel(level)
    merged_handler.setFormatter(PlainFormatter(prefix=merged_prefix))
    _add_handler_once(logger, merged_handler, f"merged:{merged_path}:{merged_prefix or ''}")
    return logger


def get_logger(
    name: str,
    *,
    level: int = DEFAULT_LEVEL,
    log_dir: str | Path | None = None,
    filename: str | None = None,
    merged_filename: str = "merged.log",
    merged_prefix: str | None = None,
    console: bool = True,
) -> logging.Logger:
    component_name = _to_component_name(name)
    logger = logging.getLogger(component_name)
    logger.setLevel(level)
    logger.propagate = False

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(ComponentColorFormatter(use_color=_supports_color()))
        _add_handler_once(logger, console_handler, f"console:{component_name}")

    if log_dir and filename:
        setup_file_logging(
            logger,
            log_dir=log_dir,
            filename=filename,
            merged_filename=merged_filename,
            merged_prefix=merged_prefix or f"[{_default_filename_prefix(component_name)}]",
            level=level,
        )
    return logger


def init_component_logger(
    component_name: str,
    *,
    group: str = "main_scripts",
    filename_prefix: str | None = None,
    level: int = DEFAULT_LEVEL,
) -> logging.Logger:
    component_label = _to_component_name(component_name)
    safe_group = _sanitize(group, "main_scripts")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_prefix = filename_prefix or _default_filename_prefix(component_label)
    log_dir = PROJECT_ROOT / "logs" / Path(safe_group) / component_label
    log_filename = f"{file_prefix}_{timestamp}.log"
    logger = get_logger(
        component_label,
        level=level,
        log_dir=log_dir,
        filename=log_filename,
        merged_prefix=f"[{file_prefix}]",
    )
    logger.info("日志初始化: %s", log_dir / log_filename)
    return logger


def init_tool_logger(
    tool_name: str,
    *,
    model_name: Optional[str] = None,
    level: int = DEFAULT_LEVEL,
) -> logging.Logger:
    safe_tool = _sanitize(tool_name, "unknown_tool")
    safe_model = _sanitize(model_name or _detect_model_name(), "unknown_model")
    component_name = _to_component_name(tool_name, "UnknownTool")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = PROJECT_ROOT / "logs" / safe_model / f"{safe_tool}_tool"
    logger = get_logger(
        component_name,
        level=level,
        log_dir=log_dir,
        filename=f"{timestamp}.log",
        merged_prefix=f"[{safe_tool}]",
    )
    logger.info("日志初始化: %s", log_dir / f"{timestamp}.log")
    return logger


__all__ = [
    "DEFAULT_LEVEL",
    "LOGGER_COLORS_EXACT",
    "LOGGER_PATTERNS",
    "get_logger",
    "init_component_logger",
    "init_tool_logger",
    "register_logger_color",
    "register_logger_pattern",
    "setup_file_logging",
]
