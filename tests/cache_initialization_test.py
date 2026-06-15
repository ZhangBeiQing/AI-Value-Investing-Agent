from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from shared_data_access import cache_registry
from utlity.stock_utils import SymbolInfo


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


def _write_financial_cache(base_dir: Path, symbol_info: SymbolInfo) -> None:
    cache_dir = cache_registry.build_cache_dir(
        symbol_info,
        cache_registry.CacheKind.FINANCIALS,
        base_dir=base_dir,
    )
    for filename in cache_registry.get_cache_spec(
        cache_registry.CacheKind.FINANCIALS
    ).required_files:
        pd.DataFrame([{"NOTICE_DATE": "2025-12-31", "value": 1}]).to_csv(
            cache_dir / filename,
            index=False,
        )
    cache_registry.record_cache_refresh(cache_dir)


def test_missing_share_cache_is_initialized_independently(
    tmp_path: Path,
    monkeypatch,
) -> None:
    symbol_info = _symbol_info()
    _write_financial_cache(tmp_path, symbol_info)
    share_updates: list[bool] = []

    def fake_update_share_info_cached(
        symbol_info: SymbolInfo,
        force_refresh: bool = False,
        base_data_dir: str | Path = "data",
        logger: logging.Logger | None = None,
    ) -> None:
        share_updates.append(force_refresh)
        cache_dir = cache_registry.build_cache_dir(
            symbol_info,
            cache_registry.CacheKind.SHARE_INFO,
            base_dir=base_data_dir,
        )
        pd.DataFrame(
            [{"变动日期": "2025-12-31", "总股本": 42099.46, "已流通股份": 42091.26}]
        ).to_csv(cache_dir / "stock_share_change_cninfo.csv", index=False)
        cache_registry.record_cache_refresh(cache_dir)

    monkeypatch.setattr(
        cache_registry,
        "update_share_info_cached",
        fake_update_share_info_cached,
    )
    monkeypatch.setattr(
        cache_registry,
        "update_price_data_cached",
        lambda *args, **kwargs: pd.DataFrame(),
    )

    cache_registry.ensure_symbol_data(
        tmp_path,
        symbol_info,
        logger=logging.getLogger("TestCacheInitialization"),
        lookback_price_days=30,
        force_refresh_price=True,
        skip_financial_refresh=True,
    )

    assert share_updates == [True]


def test_incomplete_financial_directory_is_not_treated_as_ready(
    tmp_path: Path,
    monkeypatch,
) -> None:
    symbol_info = _symbol_info()
    cache_registry.build_cache_dir(
        symbol_info,
        cache_registry.CacheKind.FINANCIALS,
        base_dir=tmp_path,
    )
    financial_updates: list[bool] = []

    def fake_update_financial_data_cached(
        symbol_info: SymbolInfo,
        base_data_dir: str | Path = "data",
        force_refresh: bool = False,
        force_refresh_financials: bool | None = None,
        logger: logging.Logger | None = None,
    ) -> dict:
        financial_updates.append(bool(force_refresh_financials))
        _write_financial_cache(Path(base_data_dir), symbol_info)
        return {}

    monkeypatch.setattr(
        cache_registry,
        "update_financial_data_cached",
        fake_update_financial_data_cached,
    )
    monkeypatch.setattr(
        cache_registry,
        "update_share_info_cached",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        cache_registry,
        "update_price_data_cached",
        lambda *args, **kwargs: pd.DataFrame(),
    )

    cache_registry.ensure_symbol_data(
        tmp_path,
        symbol_info,
        logger=logging.getLogger("TestCacheInitialization"),
        lookback_price_days=30,
        force_refresh_price=True,
        skip_financial_refresh=True,
    )

    assert financial_updates == [True]
