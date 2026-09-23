from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from core import logging as clog


def _clear_handlers(logger) -> None:
    for handler in list(logger.handlers):
        handler.flush()
        handler.close()
        logger.removeHandler(handler)


def test_configure_run_logging_creates_dir_and_env(tmp_path: Path) -> None:
    previous = os.environ.pop(clog.RUN_LOG_DIR_ENV, None)
    try:
        run_dir = clog.configure_run_logging("data_prep", "2026-09-24", base_dir=tmp_path)
        assert run_dir == tmp_path / "2026-09-24" / "data_prep"
        assert run_dir.is_dir()
        assert os.environ[clog.RUN_LOG_DIR_ENV] == str(run_dir)
    finally:
        if previous is None:
            os.environ.pop(clog.RUN_LOG_DIR_ENV, None)
        else:
            os.environ[clog.RUN_LOG_DIR_ENV] = previous


def test_loggers_share_run_dir_and_merged(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    monkeypatch.setenv(clog.RUN_LOG_DIR_ENV, str(run_dir))

    component = clog.init_component_logger("TestCompRunDir", filename_prefix="test_comp")
    tool = clog.init_tool_logger("test_tool")
    try:
        component.info("hello component")
        tool.info("hello tool")
        for logger in (component, tool):
            for handler in logger.handlers:
                handler.flush()

        names = sorted(path.name for path in run_dir.iterdir())
        assert "TestCompRunDir.log" in names
        assert "TestTool.log" in names
        assert "merged.log" in names
        merged = (run_dir / "merged.log").read_text(encoding="utf-8")
        assert "hello component" in merged
        assert "hello tool" in merged
    finally:
        _clear_handlers(component)
        _clear_handlers(tool)


def test_fallback_layout_has_no_model_layer(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv(clog.RUN_LOG_DIR_ENV, raising=False)
    monkeypatch.setattr(clog, "PROJECT_ROOT", tmp_path)

    component = clog.init_component_logger("FallbackComp", group="main_scripts")
    tool = clog.init_tool_logger("weird_tool")
    try:
        component.info("x")
        tool.info("y")
        for logger in (component, tool):
            for handler in logger.handlers:
                handler.flush()

        comp_dir = tmp_path / "logs" / "debug" / "main_scripts" / "FallbackComp"
        assert list(comp_dir.glob("*.log"))
        tool_dir = tmp_path / "logs" / "debug" / "tools" / "weird_tool"
        assert list(tool_dir.glob("*.log"))
        assert not (tmp_path / "logs" / "unknown_model").exists()
    finally:
        _clear_handlers(component)
        _clear_handlers(tool)


def test_prune_run_logs_removes_old_dates(tmp_path: Path) -> None:
    old = tmp_path / "2020-01-01" / "data_prep"
    old.mkdir(parents=True)
    today = datetime.now().strftime("%Y-%m-%d")
    recent = tmp_path / today / "data_prep"
    recent.mkdir(parents=True)

    removed = clog.prune_run_logs(keep_days=7, base_dir=tmp_path)

    assert removed == 1
    assert not old.exists()
    assert recent.exists()
