from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from commons import stock_utils


def _reset_throttle(monkeypatch) -> None:
    monkeypatch.setattr(stock_utils, "_last_api_call_at", 0.0)


def test_concurrent_calls_are_globally_spaced(monkeypatch) -> None:
    _reset_throttle(monkeypatch)
    times: list[float] = []
    lock = threading.Lock()

    def api():
        with lock:
            times.append(time.monotonic())
        time.sleep(0.01)
        return "ok"

    def task():
        for _ in range(3):
            assert stock_utils.api_call_with_delay(api, delay=0.05) == "ok"

    with ThreadPoolExecutor(max_workers=6) as executor:
        list(executor.map(lambda _: task(), range(6)))

    times.sort()
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert len(times) == 18
    # 全局最小间隔生效：任意两次调用起点至少相隔 0.05s
    assert min(gaps) >= 0.04
    assert times[-1] - times[0] >= (len(times) - 1) * 0.04


def test_min_interval_env_override() -> None:
    assert (
        stock_utils._configured_api_min_interval(0.25)
        == stock_utils.DEFAULT_API_CALL_DELAY
    )


def test_min_interval_env_override_reads_environment(monkeypatch) -> None:
    monkeypatch.setenv(stock_utils.API_MIN_INTERVAL_ENV, "0.02")
    assert stock_utils._configured_api_min_interval(0.5) == 0.02

    monkeypatch.setenv(stock_utils.API_MIN_INTERVAL_ENV, "-1")
    assert stock_utils._configured_api_min_interval(0.5) == 0.0

    monkeypatch.setenv(stock_utils.API_MIN_INTERVAL_ENV, "not-a-number")
    assert stock_utils._configured_api_min_interval(0.5) == 0.5


def test_exception_is_propagated(monkeypatch) -> None:
    _reset_throttle(monkeypatch)

    def boom():
        raise ValueError("api down")

    with pytest.raises(ValueError):
        stock_utils.api_call_with_delay(boom, delay=0)
