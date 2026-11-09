"""回归测试：价格缓存必须盖住最近交易日收盘（2026-11-02 真实事故）。

事故现场：10-30 收盘后写入的 price.csv 在 11-02 盘中仍在 TTL 内 → 被判新鲜 →
不刷新，20 只里 16 只收盘价停在 10-30，快照与研究包全部建立在过期收盘价上。

同时锁住两件事：
1. 数据源"返回非空"不等于"返回了目标交易日"，必须校验最后一根 K 线；
2. 主回退链全挂时，腾讯日线是第三条腿。
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from commons import parse_symbol
from shared_data_access import cache_registry, price_fetch
from shared_data_access.cache_registry import CacheKind


def _logger() -> logging.Logger:
    return logging.getLogger("test-price-freshness")


@pytest.fixture()
def price_cache_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """缓存 TTL 与价格窗口使用同一个固定时钟，不依赖运行测试的日期。"""
    moment = datetime(2026, 11, 5, 21, 30)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment if tz is None else moment.astimezone(tz)

    monkeypatch.setattr(cache_registry, "UTC", lambda: moment)
    monkeypatch.setattr(cache_registry, "datetime", FixedDateTime)


def _frame(last_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "日期": pd.to_datetime(["2026-10-29", last_date]),
            "开盘": [1.0, 1.0],
            "收盘": [1.0, 1.0],
            "最高": [1.0, 1.0],
            "最低": [1.0, 1.0],
            "成交量": [10.0, 10.0],
        }
    )


def test_frame_covers_trading_day_rejects_stale_frame() -> None:
    # 11-02 是交易日；只到 10-30 的非空帧不能算成功
    assert price_fetch._frame_covers_trading_day(_frame("2026-10-30"), "20261102") is False
    assert price_fetch._frame_covers_trading_day(_frame("2026-11-02"), "20261102") is True


def test_frame_covers_trading_day_skips_non_trading_day() -> None:
    # 2026-11-07 是周六，不强制覆盖
    assert price_fetch._frame_covers_trading_day(_frame("2026-10-30"), "20261107") is True


def test_frame_covers_trading_day_tolerates_unparsable_dates() -> None:
    broken = pd.DataFrame({"日期": ["not-a-date"], "收盘": [1.0]})
    assert price_fetch._frame_covers_trading_day(broken, "20261102") is True


def test_latest_close_barrier_uses_today_after_close() -> None:
    barrier = cache_registry._latest_trading_day_close_barrier(datetime(2026, 11, 5, 21, 0))
    assert barrier == datetime(2026, 11, 5, 15, 0)


def test_latest_close_barrier_falls_back_to_previous_session_before_close() -> None:
    barrier = cache_registry._latest_trading_day_close_barrier(datetime(2026, 11, 5, 10, 0))
    assert barrier == datetime(2026, 11, 4, 15, 0)


def test_latest_close_barrier_skips_weekend() -> None:
    barrier = cache_registry._latest_trading_day_close_barrier(datetime(2026, 11, 7, 12, 0))
    assert barrier == datetime(2026, 11, 6, 15, 0)


def _seed_price_cache(base: Path, *, last_updated: datetime) -> None:
    symbol_info = parse_symbol("600406.SH")
    cache_dir = cache_registry.build_cache_dir(
        symbol_info,
        CacheKind.PRICE_SERIES,
        base_dir=base,
        ensure=True,
    )
    today = datetime(2026, 11, 5)
    dates = pd.date_range(end=today, periods=60, freq="D")
    pd.DataFrame(
        {
            "日期": dates.strftime("%Y-%m-%d"),
            "开盘": 1.0,
            "收盘": 1.0,
            "最高": 1.0,
            "最低": 1.0,
            "成交量": 10.0,
        }
    ).to_csv(cache_dir / "price.csv", index=False)
    cache_registry.record_cache_refresh(cache_dir, requested_start_date="20260901")
    meta = cache_registry._load_meta(cache_dir) or {}
    meta["last_updated"] = last_updated.isoformat()
    cache_registry._meta_path(cache_dir).write_text(
        cache_registry.json.dumps(meta), encoding="utf-8"
    )


def test_price_refresh_forced_when_cache_predates_latest_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    price_cache_clock: None,
) -> None:
    _seed_price_cache(tmp_path, last_updated=datetime(2026, 11, 5, 14, 0))
    cache_dir = cache_registry.build_cache_dir(
        parse_symbol("600406.SH"), CacheKind.PRICE_SERIES, base_dir=tmp_path,
    )
    # 必须在 TTL 内，才能证明是收盘屏障而不是 TTL 触发了刷新。
    assert cache_registry.should_refresh(cache_dir, CacheKind.PRICE_SERIES) is False
    monkeypatch.setattr(
        cache_registry,
        "_latest_trading_day_close_barrier",
        lambda now=None: datetime(2026, 11, 5, 15, 0),
    )
    fetched: list[str] = []

    def fake_fetch(symbol_info, start_date, end_date, adjust="qfq", logger=None):
        fetched.append(symbol_info.symbol)
        return price_fetch._finalize_price_frame(_frame("2026-11-05"))

    monkeypatch.setattr(cache_registry, "fetch_cn_a_daily_with_fallback", fake_fetch)

    cache_registry.update_price_data_cached(
        parse_symbol("600406.SH"),
        lookback_days=60,
        base_data_dir=tmp_path,
        logger=_logger(),
    )

    assert fetched == ["600406.SH"]


def test_price_refresh_skipped_when_cache_written_after_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    price_cache_clock: None,
) -> None:
    _seed_price_cache(tmp_path, last_updated=datetime(2026, 11, 5, 21, 0))
    monkeypatch.setattr(
        cache_registry,
        "_latest_trading_day_close_barrier",
        lambda now=None: datetime(2026, 11, 5, 15, 0),
    )
    fetched: list[str] = []
    monkeypatch.setattr(
        cache_registry,
        "fetch_cn_a_daily_with_fallback",
        lambda *args, **kwargs: fetched.append("called") or pd.DataFrame(),
    )

    cache_registry.update_price_data_cached(
        parse_symbol("600406.SH"),
        lookback_days=60,
        base_data_dir=tmp_path,
        logger=_logger(),
    )

    assert fetched == []


def test_cn_fallback_rejects_stale_sources_and_uses_tencent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale = _frame("2026-11-01")
    monkeypatch.setattr(
        price_fetch,
        "_fetch_via_stock_zh_a_daily",
        lambda *args, **kwargs: stale.copy(),
    )
    monkeypatch.setattr(price_fetch.ak, "stock_zh_a_hist", lambda **kwargs: pd.DataFrame())
    tencent_frame = price_fetch._finalize_price_frame(_frame("2026-11-02"))
    monkeypatch.setattr(
        price_fetch, "_fetch_tencent_daily_frame", lambda *args, **kwargs: tencent_frame.copy()
    )

    df = price_fetch.fetch_cn_a_daily_with_fallback(
        parse_symbol("600406.SH"), "20261020", "20261102", logger=_logger()
    )

    assert df["日期"].iloc[-1].date() == date(2026, 11, 2)


def test_hk_fallback_raises_with_all_three_chains_when_nothing_covers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        price_fetch, "_fetch_hk_daily_from_sina", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("sina-ssl-eof"))
    )
    monkeypatch.setattr(
        price_fetch,
        "_fetch_hk_hist_from_eastmoney",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("eastmoney-unreachable")),
    )
    monkeypatch.setattr(
        price_fetch,
        "_fetch_tencent_hk_daily",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("tencent-down")),
    )

    with pytest.raises(ValueError) as captured:
        price_fetch.fetch_hk_a_daily_with_fallback(
            parse_symbol("01810.HK"), "20261020", "20261102", logger=_logger()
        )

    message = str(captured.value)
    assert "sina-ssl-eof" in message
    assert "eastmoney-unreachable" in message
    assert "tencent-down" in message
