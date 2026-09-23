from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd

from shared_data_access import cache_registry
from commons.stock_utils import SymbolInfo


LOGGER = logging.getLogger("TestPriceIncrementalRefresh")
PRICE_COLUMNS = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "换手率"]


def _symbol_info() -> SymbolInfo:
    return SymbolInfo(
        symbol="603893.SH",
        code="603893",
        suffix="SH",
        market="CN_A",
        calendar="CN",
        stock_name="瑞芯微",
        description="",
    )


def _price_rows(dates: list[pd.Timestamp], closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "日期": date.strftime("%Y-%m-%d"),
                "开盘": close,
                "最高": close,
                "最低": close,
                "收盘": close,
                "成交量": 100.0,
                "成交额": 1000.0,
                "换手率": 0.01,
            }
            for date, close in zip(dates, closes)
        ],
        columns=PRICE_COLUMNS,
    )


def _write_price_cache(base_dir: Path, frame: pd.DataFrame, requested_start: str) -> Path:
    cache_dir = cache_registry.build_cache_dir(
        _symbol_info(),
        cache_registry.CacheKind.PRICE_SERIES,
        base_dir=base_dir,
    )
    frame.to_csv(cache_dir / "price.csv", index=False)
    cache_registry.record_cache_refresh(
        cache_dir, requested_start_date=requested_start
    )
    return cache_dir


def _read_price_cache(base_dir: Path) -> pd.DataFrame:
    cache_dir = cache_registry.build_cache_dir(
        _symbol_info(),
        cache_registry.CacheKind.PRICE_SERIES,
        base_dir=base_dir,
    )
    return pd.read_csv(cache_dir / "price.csv")


def test_daily_refresh_fetches_only_incremental_window(tmp_path: Path, monkeypatch) -> None:
    base = pd.Timestamp.now().normalize()
    existing_dates = [base - timedelta(days=offset) for offset in (4, 3, 2, 1)]
    _write_price_cache(
        tmp_path,
        _price_rows(existing_dates, [10.0, 11.0, 12.0, 13.0]),
        requested_start="20210101",
    )

    fresh_dates = [base - timedelta(days=offset) for offset in (2, 1, 0)]
    calls: list[str] = []

    def fake_fetch(symbol_info, start_date, end_date, logger):
        calls.append(start_date)
        return _price_rows(fresh_dates, [12.0, 13.0, 14.0])

    monkeypatch.setattr(cache_registry, "_fetch_daily_price_frame", fake_fetch)

    result = cache_registry.update_price_data_cached(
        _symbol_info(),
        lookback_days=1800,
        force_refresh=True,
        base_data_dir=tmp_path,
        logger=LOGGER,
    )

    assert calls == [(base - timedelta(days=11)).strftime("%Y%m%d")]
    assert len(result) == 5
    assert result["日期"].tolist() == [
        (base - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in (4, 3, 2, 1, 0)
    ]

    meta = cache_registry._load_meta(
        cache_registry.build_cache_dir(
            _symbol_info(),
            cache_registry.CacheKind.PRICE_SERIES,
            base_dir=tmp_path,
        )
    )
    assert meta["requested_start_date"] == "20210101"


def test_ex_rights_rebasing_triggers_full_refresh(tmp_path: Path, monkeypatch) -> None:
    base = pd.Timestamp.now().normalize()
    existing_dates = [base - timedelta(days=offset) for offset in (4, 3, 2, 1)]
    _write_price_cache(
        tmp_path,
        _price_rows(existing_dates, [10.0, 11.0, 12.0, 13.0]),
        requested_start="20210101",
    )

    overlap_dates = [base - timedelta(days=offset) for offset in (2, 1, 0)]
    full_dates = [base - timedelta(days=offset) for offset in (4, 3, 2, 1, 0)]
    calls: list[str] = []

    def fake_fetch(symbol_info, start_date, end_date, logger):
        calls.append(start_date)
        if len(calls) == 1:
            # Overlap closes disagree -> signals a qfq re-basing (ex-rights).
            return _price_rows(overlap_dates, [6.0, 6.5, 7.0])
        return _price_rows(full_dates, [5.0, 5.5, 6.0, 6.5, 7.0])

    monkeypatch.setattr(cache_registry, "_fetch_daily_price_frame", fake_fetch)

    result = cache_registry.update_price_data_cached(
        _symbol_info(),
        lookback_days=1800,
        force_refresh=True,
        base_data_dir=tmp_path,
        logger=LOGGER,
    )

    full_start = (base - timedelta(days=1800)).strftime("%Y%m%d")
    assert len(calls) == 2
    assert calls[1] == full_start
    assert result["收盘"].tolist() == [5.0, 5.5, 6.0, 6.5, 7.0]


def test_missing_cache_uses_full_lookback(tmp_path: Path, monkeypatch) -> None:
    base = pd.Timestamp.now().normalize()
    calls: list[str] = []

    def fake_fetch(symbol_info, start_date, end_date, logger):
        calls.append(start_date)
        return _price_rows([base], [10.0])

    monkeypatch.setattr(cache_registry, "_fetch_daily_price_frame", fake_fetch)

    cache_registry.update_price_data_cached(
        _symbol_info(),
        lookback_days=1800,
        force_refresh=True,
        base_data_dir=tmp_path,
        logger=LOGGER,
    )

    assert calls == [(base - timedelta(days=1800)).strftime("%Y%m%d")]


def test_insufficient_history_triggers_full_backfill(tmp_path: Path, monkeypatch) -> None:
    """覆盖不足且非数据源上限时，必须全量回补早期历史，且第二次调用不再抓取。

    回归：旧实现只做增量合并、requested_start_date 又停留在旧值，导致判定每天为真、
    每天对同一批股票空刷一次价格 API。
    """
    base = pd.Timestamp.now().normalize()
    recent_dates = [base - timedelta(days=offset) for offset in (4, 3, 2, 1)]
    _write_price_cache(
        tmp_path,
        _price_rows(recent_dates, [10.0, 11.0, 12.0, 13.0]),
        requested_start=(base - timedelta(days=5)).strftime("%Y%m%d"),
    )

    full_start = base - timedelta(days=1800)
    full_dates = [full_start, base - timedelta(days=1), base]
    calls: list[str] = []

    def fake_fetch(symbol_info, start_date, end_date, logger):
        calls.append(start_date)
        return _price_rows(full_dates, [5.0, 6.0, 7.0])

    monkeypatch.setattr(cache_registry, "_fetch_daily_price_frame", fake_fetch)

    result = cache_registry.update_price_data_cached(
        _symbol_info(),
        lookback_days=1800,
        force_refresh=False,
        base_data_dir=tmp_path,
        logger=LOGGER,
    )

    # 走全量回补（起点=full lookback），而不是只抓最近窗口
    assert calls == [full_start.strftime("%Y%m%d")]
    assert result["日期"].min() == full_start.strftime("%Y-%m-%d")

    cache_dir = cache_registry.build_cache_dir(
        _symbol_info(),
        cache_registry.CacheKind.PRICE_SERIES,
        base_dir=tmp_path,
    )
    assert cache_registry._load_meta(cache_dir)["requested_start_date"] == full_start.strftime(
        "%Y%m%d"
    )

    # 回补后覆盖满足，第二次调用不应再触发抓取
    calls.clear()
    cache_registry.update_price_data_cached(
        _symbol_info(),
        lookback_days=1800,
        force_refresh=False,
        base_data_dir=tmp_path,
        logger=LOGGER,
    )
    assert calls == []

