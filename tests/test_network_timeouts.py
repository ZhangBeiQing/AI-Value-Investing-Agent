"""Small regression checks for the data-refresh timeout guards."""

from __future__ import annotations

import subprocess
import sys

import pytest

from core.network import install_requests_default_timeout
from scripts.manage_daily_data import run_subprocess


def test_requests_default_timeout_can_be_updated(monkeypatch: pytest.MonkeyPatch) -> None:
    from requests.adapters import HTTPAdapter

    original_send = HTTPAdapter.send
    original_patched = getattr(HTTPAdapter, "_default_timeout_patched", False)
    original_seconds = getattr(HTTPAdapter, "_default_timeout_seconds", None)
    try:
        monkeypatch.setattr(HTTPAdapter, "send", lambda self, request, **kwargs: kwargs["timeout"])
        monkeypatch.setattr(HTTPAdapter, "_default_timeout_patched", False, raising=False)
        install_requests_default_timeout(1)
        adapter = HTTPAdapter()
        assert adapter.send(None, timeout=None) == 1
        install_requests_default_timeout(2)
        assert adapter.send(None, timeout=None) == 2
        assert adapter.send(None, timeout=3) == 3
    finally:
        HTTPAdapter.send = original_send
        HTTPAdapter._default_timeout_patched = original_patched
        HTTPAdapter._default_timeout_seconds = original_seconds


def test_subprocess_timeout_is_opt_in(tmp_path) -> None:
    log_path = tmp_path / "step.log"
    result = run_subprocess("short", [sys.executable, "-c", "print('ok')"], log_path)
    assert result["status"] == "success"

    with pytest.raises(subprocess.TimeoutExpired):
        run_subprocess(
            "stalled",
            [sys.executable, "-c", "import time; time.sleep(10)"],
            log_path,
            timeout_seconds=0.1,
        )
    assert "timeout after" in log_path.read_text(encoding="utf-8")


def test_board_heat_client_sets_explicit_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """2026-10-28 事故：板块研究 client 不设 timeout，dashscope 不返回时整条 refresh 无限挂住。"""
    from services.selection_system import board_heat

    captured: dict = {}

    class _FakeOpenAI:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setenv("AUDIT_MODEL_API_KEY", "test-key")
    monkeypatch.setenv("AUDIT_MODEL_BASE_URL", "https://example.invalid/v1")
    monkeypatch.delenv("BOARD_HEAT_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setattr(board_heat, "OpenAI", _FakeOpenAI)

    board_heat._get_research_client("deepseek-v4.1-flash")

    assert captured["timeout"] == board_heat.DEFAULT_RESEARCH_TIMEOUT_SECONDS
    assert captured["max_retries"] == 1


def test_board_heat_timeout_env_override_and_invalid_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.selection_system import board_heat

    monkeypatch.setenv("BOARD_HEAT_TIMEOUT_SECONDS", "42")
    assert board_heat._resolve_research_timeout() == 42.0

    monkeypatch.setenv("BOARD_HEAT_TIMEOUT_SECONDS", "not-a-number")
    assert board_heat._resolve_research_timeout() == board_heat.DEFAULT_RESEARCH_TIMEOUT_SECONDS

    monkeypatch.setenv("BOARD_HEAT_TIMEOUT_SECONDS", "0")
    assert board_heat._resolve_research_timeout() == board_heat.DEFAULT_RESEARCH_TIMEOUT_SECONDS
