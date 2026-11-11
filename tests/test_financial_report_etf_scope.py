"""ETF must never enter company financial-report research preparation."""

import pytest

from services.research import financial_report_skill as service
from services.research.financial_daily_brief import daily_brief_task


@pytest.mark.parametrize("symbol", ["518800.SH", "512880.SH", "159611.SZ"])
def test_build_bundles_with_etf_skips_disclosure_and_summary_lookup(monkeypatch, symbol):
    selected = []

    def select(stock_symbol, **kwargs):
        selected.append(stock_symbol)
        return None, None

    monkeypatch.setattr(service, "_select_latest_two_reports", select)
    bundles = service.build_stock_report_bundles(
        "2026-11-10",
        extra_items=[
            {"symbol": symbol, "stock_name": "ETF"},
            {"symbol": "603501.SH", "stock_name": "豪威集团"},
        ],
        skip_queue=True,
    )

    assert selected == ["603501.SH"]
    assert [bundle.symbol for bundle in bundles] == ["603501.SH"]


def test_daily_brief_task_with_existing_etf_report_does_not_queue_summary(tmp_path):
    report = tmp_path / "20250829.md"
    report.write_text("误遗留在ETF目录下的历史财报总结" * 100, encoding="utf-8")

    assert daily_brief_task(
        report, symbol="518800.SH", as_of_date="2026-11-10",
    ) is None
