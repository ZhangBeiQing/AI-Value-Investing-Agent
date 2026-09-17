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
