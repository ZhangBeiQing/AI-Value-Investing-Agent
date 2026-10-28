"""财报公告可见性截断口径的回归测试。

背景（2026-10-26 真实事故）：公司在交易日 D 的**晚间**发布财报，巨潮/交易所把公告
日期登记为 D+1。若按 D 截断公告抓取窗口，当晚这一批财报会整批丢失，第二天的研究包
与交易决策就建立在过期信息上。因此在实盘/日常模式下，公告可见性截断日应为 D+1，
而价格与估值仍以 D 为分析日；历史回测必须保持 D，否则构成时间穿越。
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.news import disclosures_builder as db


@pytest.mark.parametrize(
    ("analysis_date", "expected"),
    [
        ("2026-10-26", "2026-10-27"),
        ("2026-10-30", "2026-10-31"),
        ("2026-10-31", "2026-11-01"),
        ("2026-12-31", "2027-01-01"),
        ("2027-02-28", "2027-03-01"),
    ],
)
def test_resolve_disclosure_cutoff_next_day_returns_next_date(analysis_date, expected):
    assert db.resolve_disclosure_cutoff_date(analysis_date) == expected


def test_resolve_disclosure_cutoff_strict_mode_keeps_analysis_day():
    assert (
        db.resolve_disclosure_cutoff_date("2026-10-26", extend_to_next_day=False)
        == "2026-10-26"
    )


def test_resolve_disclosure_cutoff_without_date_returns_none():
    assert db.resolve_disclosure_cutoff_date(None) is None


def _run_sync(monkeypatch, tmp_path, **kwargs):
    captured = {}

    class FakeAccess:
        def prepare_dataset(self, **call_kwargs):
            captured.update(call_kwargs)
            return SimpleNamespace(disclosures=None)

    monkeypatch.setattr(db, "index_path", lambda symbol_info: tmp_path / "index.json")
    monkeypatch.setattr(db, "load_index", lambda path: {})
    db.sync_financial_reports_for_stock(
        SimpleNamespace(symbol="603283.SH", stock_name="赛腾股份"),
        data_access=FakeAccess(),
        **kwargs,
    )
    return captured


def test_sync_financial_reports_defaults_to_next_day_disclosure_cutoff(monkeypatch, tmp_path):
    captured = _run_sync(monkeypatch, tmp_path, as_of_date="2026-10-26")

    assert captured["disclosure_as_of_date"] == "2026-10-27"
    # 价格/财务等其他数据仍以分析日为基准，不能被截断日顶替。
    assert captured["as_of_date"] == "2026-10-26"
    assert captured["skip_price_refresh"] is True


def test_sync_financial_reports_backtest_keeps_analysis_day(monkeypatch, tmp_path):
    captured = _run_sync(
        monkeypatch,
        tmp_path,
        as_of_date="2026-10-26",
        extend_disclosure_cutoff=False,
    )

    assert captured["disclosure_as_of_date"] == "2026-10-26"


def test_prepare_script_defaults_to_next_day_and_forces_strict_in_backtest():
    from scripts.prepare_financial_report_skill import (
        build_parser,
        resolve_sync_disclosure_cutoff,
    )

    live_args = build_parser().parse_args(["--date", "2026-10-26"])
    assert resolve_sync_disclosure_cutoff(live_args) == "next-day"

    backtest_args = build_parser().parse_args(
        [
            "--date",
            "2026-10-26",
            "--backtest-context",
            "data/skill_runs/2026-10-26/fixed_tracked/00_backtest_context.md",
        ]
    )
    assert resolve_sync_disclosure_cutoff(backtest_args) == "analysis-day"


def test_build_bundles_passes_visibility_cutoff_to_report_selection(monkeypatch):
    from services.research import financial_report_skill as frs

    captured = {}

    def fake_select(symbol, *, available_on_date=None):
        captured[symbol] = available_on_date
        return None, None

    monkeypatch.setattr(frs, "_select_latest_two_reports", fake_select)
    frs.build_stock_report_bundles(
        "2026-10-26",
        extra_items=[{"symbol": "603283.SH", "stock_name": "赛腾股份"}],
        skip_queue=True,
        report_visibility_cutoff="2026-10-27",
    )

    assert captured["603283.SH"] == "2026-10-27"


def test_build_bundles_without_cutoff_falls_back_to_run_date(monkeypatch):
    from services.research import financial_report_skill as frs

    captured = {}

    def fake_select(symbol, *, available_on_date=None):
        captured[symbol] = available_on_date
        return None, None

    monkeypatch.setattr(frs, "_select_latest_two_reports", fake_select)
    frs.build_stock_report_bundles(
        "2026-10-24",
        extra_items=[{"symbol": "603283.SH", "stock_name": "赛腾股份"}],
        skip_queue=True,
    )

    assert captured["603283.SH"] == "2026-10-24"


def test_load_disclosure_bundle_cutoff_includes_next_day_announcements(tmp_path):
    """公告截断日为 D+1 时，D+1 零点登记的财报必须进入 bundle。"""

    import logging

    import pandas as pd

    from commons.stock_utils import parse_symbol
    from shared_data_access.cache_registry import CacheKind, build_cache_dir
    from shared_data_access.data_access import SharedDataAccess

    symbol_info = parse_symbol("603283.SH")
    access = SharedDataAccess(logger=logging.getLogger("test"), base_dir=str(tmp_path))
    cache_dir = build_cache_dir(
        symbol_info, CacheKind.DISCLOSURES, base_dir=str(tmp_path), ensure=True
    )
    pd.DataFrame(
        [
            {
                "代码": "603283",
                "简称": "赛腾股份",
                "公告标题": "2026年半年度报告",
                "公告时间": "2026-08-25 00:00:00",
                "announcementId": "111",
            },
            {
                "代码": "603283",
                "简称": "赛腾股份",
                "公告标题": "2026年第三季度报告",
                "公告时间": "2026-10-27 00:00:00",
                "announcementId": "222",
            },
        ]
    ).to_csv(cache_dir / "cninfo_list.csv", index=False)

    from datetime import datetime

    strict = access._load_disclosure_bundle(
        symbol_info, datetime(2026, 10, 26), 550
    )
    extended = access._load_disclosure_bundle(
        symbol_info, datetime(2026, 10, 26), 550, cutoff_dt=datetime(2026, 10, 27)
    )

    assert "2026年第三季度报告" not in set(strict.frame["公告标题"])
    assert "2026年第三季度报告" in set(extended.frame["公告标题"])


def _report_meta(announcement_id: str, title: str, report_type: str = "q3") -> object:
    from services.research.financial_report_skill import FinancialReportMeta

    quarter = {"q1": 1, "interim": 2, "q3": 3, "annual": 4}[report_type]
    return FinancialReportMeta(
        announcement_id=announcement_id,
        title=title,
        date="2026-10-27",
        report_type=report_type,
        quarter=quarter,
        fiscal_year=2026,
        report_kind=None,
        priority=0,
        pdf_path=None,
        md_path=None,
    )


def test_summary_entry_from_preview_does_not_cover_actual_report():
    """业绩预告的总结不能顶替同一报告期的正式财报，否则正式财报会被判成已总结。"""

    from services.research.financial_report_skill import _summary_entry_covers_report

    latest = _report_meta("222", "2026年三季度报告")
    preview_entry = {
        "announcement_id": "111",
        "report_type": "q3",
        "fiscal_year": 2026,
        "quarter": 3,
    }
    titles = {"111": "2026年前三季度业绩预告"}

    assert _summary_entry_covers_report(preview_entry, latest, titles) is False


def test_summary_entry_from_periodic_report_covers_same_period():
    from services.research.financial_report_skill import _summary_entry_covers_report

    latest = _report_meta("222", "2026年三季度报告")
    real_entry = {
        "announcement_id": "111",
        "report_type": "q3",
        "fiscal_year": 2026,
        "quarter": 3,
    }
    titles = {"111": "2026年三季度报告"}

    assert _summary_entry_covers_report(real_entry, latest, titles) is True


def test_is_preview_announcement_title_detects_preview_and_ignores_real_report():
    from services.research.financial_report_skill import _is_preview_announcement_title

    assert _is_preview_announcement_title("2026年前三季度业绩预告") is True
    assert _is_preview_announcement_title("2026年第三季度业绩快报") is True
    assert _is_preview_announcement_title("2026年三季度报告") is False
    assert _is_preview_announcement_title("2026年半年度报告") is False
