import json
from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from commons import get_stock_data_dir, parse_symbol
from core.logging import get_logger
from shared_data_access import data_access
from shared_data_access.hk_disclosure_dates import load_hk_financial_notice_dates
from shared_data_access.paths import financial_cache_dir


def test_hk_snapshot_ps_uses_ttm_instead_of_latest_quarter_revenue():
    from services.snapshot.basic_snapshot import BasicStockInfoService

    service = BasicStockInfoService.__new__(BasicStockInfoService)
    profit = pd.DataFrame({
        "REPORT_DATE": ["2025-03-31", "2025-12-31", "2026-03-31"],
        "DATE_TYPE_CODE": [3, 1, 3],
        "OPERATE_INCOME": [80.0, 400.0, 100.0],
    })
    abstract = pd.DataFrame({"OPERATE_INCOME": [100.0]})
    assert service._current_ps(abstract, 10.0, 42.0, profit_sheet=profit) == 1.0
    # HK statement pivot can duplicate revenue labels; the preferred abstract
    # carries the verified company revenue for the same public periods.
    abstract = profit.copy()
    doubled_profit = profit.copy()
    doubled_profit["OPERATE_INCOME"] *= 2
    assert service._current_ps(abstract, 10.0, 42.0, profit_sheet=doubled_profit) == 1.0


def test_hk_bundle_filters_statements_and_row_abstract_by_actual_publication(tmp_path, monkeypatch):
    symbol = parse_symbol("01810.HK")
    root = get_stock_data_dir(symbol, base_dir=tmp_path)
    index = root / "disclosures/index.json"
    index.parent.mkdir(parents=True)
    index.write_text(json.dumps({
        "q1": {"title": "截至2026年3月31日止三个月之业绩公告", "date": "2026-05-26", "is_financial_report": True},
        "h1": {"title": "截至2026年6月30日止六个月之中期业绩公告", "date": "2026-08-18", "is_financial_report": True},
        "preview": {"title": "2026年第三季度业绩预告", "date": "2026-07-01", "is_financial_report": True},
    }, ensure_ascii=False))
    cache = financial_cache_dir(symbol, base_dir=tmp_path)
    cache.mkdir(parents=True)
    periods = ["2026-03-31", "2026-06-30", "2026-09-30"]
    for filename in ("profit_sheet.csv", "balance_sheet.csv", "cash_flow_sheet.csv"):
        pd.DataFrame({"REPORT_DATE": periods, "NETPROFIT": [100, 900, 999]}).to_csv(cache / filename, index=False)
    pd.DataFrame({"REPORT_DATE": periods, "EPS_TTM": [1, 9, 99]}).to_csv(cache / "financial_abstract.csv", index=False)
    monkeypatch.setattr(data_access, "check_cache", lambda *_: SimpleNamespace(missing_files=[], stale=False))

    access = data_access.SharedDataAccess(logger=get_logger("HKDateTest"), base_dir=tmp_path)
    bundle = access._load_financial_bundle(symbol, datetime(2026, 7, 1))

    for frame in (bundle.profit_sheet, bundle.balance_sheet, bundle.cash_flow_sheet, bundle.financial_abstract):
        assert frame["REPORT_DATE"].tolist() == ["2026-03-31"]
        assert pd.Timestamp(frame.iloc[0]["NOTICE_DATE"]) == pd.Timestamp("2026-05-26")
    assert "NOTICE_DATE" not in pd.read_csv(cache / "profit_sheet.csv").columns
    mapping = load_hk_financial_notice_dates(symbol, base_dir=tmp_path)
    assert mapping["20260630"] == datetime(2026, 8, 18)
    assert "20260930" not in mapping
