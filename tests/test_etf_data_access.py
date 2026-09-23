from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from shared_data_access import cache_registry
from commons.stock_utils import is_etf_symbol, parse_symbol


def test_forced_hk_leveraged_product_is_recognized_as_etf() -> None:
    symbol_info = parse_symbol("07709.HK")

    assert symbol_info.stock_name == "南方东英海力士2倍做多"
    assert is_etf_symbol(symbol_info)


def test_hk_leveraged_product_only_refreshes_price_data(
    tmp_path: Path,
    monkeypatch,
) -> None:
    symbol_info = parse_symbol("07709.HK")
    calls: list[str] = []

    monkeypatch.setattr(
        cache_registry,
        "update_financial_data_cached",
        lambda *args, **kwargs: calls.append("financials"),
    )
    monkeypatch.setattr(
        cache_registry,
        "update_share_info_cached",
        lambda *args, **kwargs: calls.append("shares"),
    )
    monkeypatch.setattr(
        cache_registry,
        "update_disclosures_cached",
        lambda *args, **kwargs: calls.append("disclosures"),
    )
    monkeypatch.setattr(
        cache_registry,
        "update_chip_distribution_cached",
        lambda *args, **kwargs: calls.append("chip_distribution"),
    )
    monkeypatch.setattr(
        cache_registry,
        "update_price_data_cached",
        lambda *args, **kwargs: (
            calls.append("prices"),
            pd.DataFrame(),
        )[1],
    )

    cache_registry.ensure_symbol_data(
        tmp_path,
        symbol_info,
        logger=logging.getLogger("TestEtfDataAccess"),
        lookback_price_days=30,
        force_refresh_price=True,
        include_disclosures=True,
        include_chip_distribution=True,
    )

    assert calls == ["prices"]
