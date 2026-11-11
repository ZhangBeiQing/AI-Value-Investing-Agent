import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from commons import parse_symbol
from shared_data_access import cache_registry
from shared_data_access.data_access import SharedDataAccess


@pytest.mark.parametrize("symbol,period", [("NVDA.US", "2026-07-26"), ("01810.HK", "2026-06-30")])
@pytest.mark.parametrize("skip", [False, True])
def test_fresh_foreign_cache_does_not_force_refresh_for_cn_quarter(tmp_path, monkeypatch, symbol, period, skip):
    info = parse_symbol(symbol)
    folder = cache_registry.build_cache_dir(info, cache_registry.CacheKind.FINANCIALS, base_dir=tmp_path)
    for name in cache_registry.get_cache_spec(cache_registry.CacheKind.FINANCIALS).required_files:
        pd.DataFrame([{"REPORT_DATE": period}]).to_csv(folder / name, index=False)
    cache_registry.record_cache_refresh(folder)
    monkeypatch.setattr(cache_registry, "_expected_latest_report_period", lambda today=None: "20260930")
    monkeypatch.setattr(cache_registry, "_csv_cache_has_rows", lambda _: True)
    monkeypatch.setattr(cache_registry, "update_price_data_cached", lambda *a, **k: None)
    calls = []
    monkeypatch.setattr(cache_registry, "update_financial_data_cached", lambda *a, **k: calls.append(k))

    for _ in range(2):
        cache_registry.ensure_symbol_data(tmp_path, info, logger=logging.getLogger("TestCache"),
            lookback_price_days=30, skip_financial_refresh=skip)

    assert all(not call["force_refresh_financials"] for call in calls)
    if skip:
        assert calls == []


def test_cache_only_dataset_missing_cache_never_attempts_initialization(tmp_path, monkeypatch):
    from shared_data_access import data_access
    from shared_data_access.exceptions import CacheIntegrityError
    calls = []
    monkeypatch.setattr(data_access, "ensure_symbol_data", lambda *a, **k: calls.append(k))
    service = SharedDataAccess(base_dir=tmp_path, logger=logging.getLogger("TestCache"))
    with pytest.raises(CacheIntegrityError):
        service.prepare_dataset(symbolInfo=parse_symbol("NVDA.US"), as_of_date="2026-07-02", cache_only=True)
    assert calls == []


def test_read_only_research_scope_reaches_nested_dataset_loaders(tmp_path, monkeypatch):
    from shared_data_access.cache_policy import cached_data_only, is_cached_data_only
    from shared_data_access import data_access
    calls = []
    monkeypatch.setattr(data_access, "ensure_symbol_data", lambda *a, **k: calls.append(k))
    service = SharedDataAccess(base_dir=tmp_path, logger=logging.getLogger("TestCache"))
    monkeypatch.setattr(service, "_load_financial_bundle", lambda *a: "financials")
    monkeypatch.setattr(service, "_load_share_info", lambda *a: "shares")
    monkeypatch.setattr(service, "_load_price_bundle", lambda *a, **k: SimpleNamespace(allow_stale=k["allow_stale_cache"]))
    with cached_data_only():
        assert is_cached_data_only()
        result = service.prepare_dataset(symbolInfo=parse_symbol("NVDA.US"), as_of_date="2026-07-02")
    assert not is_cached_data_only()
    assert result.prices.allow_stale
    assert calls == []


def test_backtest_research_enters_cache_only_scope(monkeypatch):
    from services.pipeline.steps import build_stock_research as research
    from shared_data_access.cache_policy import is_cached_data_only
    states = []
    monkeypatch.setattr(research, "analyze_stock_dynamics_and_valuation", lambda *a: states.append(is_cached_data_only()) or {})
    monkeypatch.setattr(research, "search_stock_news", lambda *a, **k: "{}")
    monkeypatch.setattr(research, "get_financial_report_summary", lambda *a, **k: {})
    monkeypatch.setattr(research, "_artifact_input_fingerprint", lambda *a: {})
    research._build_read_only_base_artifact("600150.SH", "2026-07-02", snapshot_payload={}, source_data_root=None)
    assert states == [True]
    assert not is_cached_data_only()


def test_cache_only_peers_do_not_call_model_when_missing(tmp_path, monkeypatch):
    from commons import similar_stocks
    from shared_data_access.cache_policy import cached_data_only
    calls = []
    monkeypatch.setattr(similar_stocks, "_call_deepseek_chat", lambda *a, **k: calls.append(a))
    with cached_data_only():
        assert similar_stocks.get_similar_stocks(parse_symbol("600406.SH"), tmp_path) == []
    assert calls == []


def test_cache_only_scope_is_worker_local_and_restored_on_failure():
    from concurrent.futures import ThreadPoolExecutor
    from shared_data_access.cache_policy import cached_data_only, is_cached_data_only
    with pytest.raises(RuntimeError):
        with cached_data_only():
            with ThreadPoolExecutor(max_workers=1) as pool:
                assert pool.submit(is_cached_data_only).result() is False
            assert is_cached_data_only()
            raise RuntimeError("failure")
    assert not is_cached_data_only()


def test_backtest_basic_snapshot_enforces_cache_only(monkeypatch):
    from services.snapshot import basic_snapshot
    seen = []
    monkeypatch.setattr(basic_snapshot, "basic_info", lambda *a, **k: seen.append(k) or {})
    basic_snapshot.build_basic_snapshot(["NVDA.US"], "2026-07-02", backtest_read_only=True)
    assert seen[0]["cache_only"] is True
    assert seen[0]["skip_financial_refresh"] is True
