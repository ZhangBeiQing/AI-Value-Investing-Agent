"""Unified logging helpers inspired by AReaL, adapted for the skill-only architecture."""

from __future__ import annotations

import logging
import os
import re
import shutil
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_HEADER = "(AI-Stock)"
DATE_FORMAT = "%Y%m%d-%H:%M:%S"
LOG_PREFIX_WIDTH = 18
DEFAULT_LEVEL = logging.INFO

# 运行日志目录环境变量：设置后，同一流程的所有组件日志都落到该目录，
# 便于按「日期 + 流程」一次定位。子进程会继承该变量。
RUN_LOG_DIR_ENV = "AI_STOCK_LOG_DIR"
RUN_LOG_KEEP_DAYS_ENV = "AI_STOCK_LOG_KEEP_DAYS"
DEFAULT_RUN_LOG_KEEP_DAYS = 14
DEFAULT_RUN_LOG_BASE = "logs/runs"

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


def _resolve_run_log_base(base_dir: str | Path | None) -> Path:
    if base_dir is None:
        return PROJECT_ROOT / DEFAULT_RUN_LOG_BASE
    candidate = Path(base_dir)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _run_log_dir() -> Optional[Path]:
    """Return the active run-scoped log directory, if one has been configured."""

    raw = os.environ.get(RUN_LOG_DIR_ENV, "").strip()
    if not raw:
        return None
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    try:
        candidate.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    return candidate


def prune_run_logs(
    *,
    keep_days: int | None = None,
    base_dir: str | Path | None = None,
) -> int:
    """Delete run-log date directories older than ``keep_days`` (default 14)."""

    if keep_days is None:
        raw = os.environ.get(RUN_LOG_KEEP_DAYS_ENV, "").strip()
        try:
            keep_days = int(raw) if raw else DEFAULT_RUN_LOG_KEEP_DAYS
        except ValueError:
            keep_days = DEFAULT_RUN_LOG_KEEP_DAYS
    if keep_days <= 0:
        return 0
    base = _resolve_run_log_base(base_dir)
    if not base.exists():
        return 0
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    removed = 0
    for day_dir in base.iterdir():
        if not day_dir.is_dir():
            continue
        name = day_dir.name
        if len(name) == 10 and name[:4].isdigit() and name <= cutoff:
            shutil.rmtree(day_dir, ignore_errors=True)
            removed += 1
    return removed


def configure_run_logging(
    flow: str,
    run_date: str | None = None,
    *,
    base_dir: str | Path | None = None,
    keep_days: int | None = None,
) -> Path:
    """Point all subsequent loggers at ``logs/runs/<date>/<flow>``.

    Entry scripts call this once (directly or via
    :func:`bootstrap_run_logging_from_argv`) before importing services, so that
    every component of one daily flow shares a single directory and a single
    ``merged.log``. Subprocesses inherit ``AI_STOCK_LOG_DIR`` automatically.
    """

    date_str = str(run_date or datetime.now().strftime("%Y-%m-%d")).strip()[:10]
    base = _resolve_run_log_base(base_dir)
    target = base / date_str / _sanitize(flow, "run")
    target.mkdir(parents=True, exist_ok=True)
    os.environ[RUN_LOG_DIR_ENV] = str(target)
    prune_run_logs(keep_days=keep_days, base_dir=base_dir)
    return target


def bootstrap_run_logging_from_argv(
    flow: str,
    *,
    date_argv: str = "--date",
    base_dir: str | Path | None = None,
) -> Path:
    """Configure run logging using ``--date`` from ``sys.argv`` when present."""

    run_date = None
    argv = sys.argv[1:]
    for index, token in enumerate(argv):
        if token == date_argv and index + 1 < len(argv):
            run_date = argv[index + 1]
            break
        if token.startswith(f"{date_argv}="):
            run_date = token.split("=", 1)[1]
            break
    return configure_run_logging(flow, run_date, base_dir=base_dir)


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
    file_prefix = filename_prefix or _default_filename_prefix(component_label)
    run_dir = _run_log_dir()
    if run_dir is not None:
        # 运行流水目录：同一流程的所有组件共享一个目录与一个 merged.log
        log_dir = run_dir
        log_filename = f"{component_label}.log"
    else:
        # 独立调试：扁平到 logs/debug/<group>/<Component>/，不再按模型签名分层
        safe_group = _sanitize(group, "debug")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = PROJECT_ROOT / "logs" / "debug" / Path(safe_group) / component_label
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
    # model_name 保留仅为兼容旧调用签名，不再参与目录分段
    safe_tool = _sanitize(tool_name, "unknown_tool")
    component_name = _to_component_name(tool_name, "UnknownTool")
    run_dir = _run_log_dir()
    if run_dir is not None:
        log_dir = run_dir
        log_filename = f"{component_name}.log"
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = PROJECT_ROOT / "logs" / "debug" / "tools" / safe_tool
        log_filename = f"{timestamp}.log"
    logger = get_logger(
        component_name,
        level=level,
        log_dir=log_dir,
        filename=log_filename,
        merged_prefix=f"[{safe_tool}]",
    )
    logger.info("日志初始化: %s", log_dir / log_filename)
    return logger


__all__ = [
    "DEFAULT_LEVEL",
    "RUN_LOG_DIR_ENV",
    "LOGGER_COLORS_EXACT",
    "LOGGER_PATTERNS",
    "bootstrap_run_logging_from_argv",
    "configure_run_logging",
    "get_logger",
    "init_component_logger",
    "init_tool_logger",
    "prune_run_logs",
    "register_logger_color",
    "register_logger_pattern",
    "setup_file_logging",
]
