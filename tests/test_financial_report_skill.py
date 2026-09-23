from pathlib import Path

from shared_data_access import stockstar_financial_report as stockstar_fallback
from services.research.financial_report_skill import (
    FinancialReportMeta,
    _resolve_fiscal_year,
    _select_latest_two_reports,
    _summary_entry_covers_report,
)
from commons.stock_utils import parse_symbol


def _report(*, announcement_id: str, report_type: str, quarter: int, fiscal_year: int) -> FinancialReportMeta:
    return FinancialReportMeta(
        announcement_id=announcement_id,
        title="",
        date="2026-07-26",
        report_type=report_type,
        quarter=quarter,
        fiscal_year=fiscal_year,
        report_kind="full_report",
        priority=0,
        pdf_path=Path("report.pdf"),
        md_path=None,
    )


def test_summary_entry_covers_duplicate_interim_disclosure() -> None:
    latest = _report(
        announcement_id="1225442172",
        report_type="interim",
        quarter=2,
        fiscal_year=2026,
    )
    registered = {
        "announcement_id": "1225441586",
        "report_date": "2026-07-25",
        "report_type": "半年报",
    }

    assert _summary_entry_covers_report(registered, latest)


def test_stockstar_fallback_validates_and_writes_full_report(tmp_path, monkeypatch) -> None:
    title = "浙江三美化工股份有限公司2026年半年度报告"
    listing = f'<a href="//stock.stockstar.com/notice/SN1.shtml">三美股份: {title}</a>'
    report = (
        f'<div class="article_content">{title} 公司代码：603379 '
        + "重要提示 公司简介和主要财务指标 管理层讨论与分析 财务报告 "
        + ("完整财报正文 " * 4000)
        + "</div>"
    )
    pages = iter((listing, report))
    monkeypatch.setattr(stockstar_fallback, "_request_html", lambda _url: next(pages))

    target = tmp_path / "report.md"
    result = stockstar_fallback.fetch_stockstar_financial_report_markdown(
        parse_symbol("603379.SH"), title=title, output_path=target
    )

    assert result == (target, "https://stock.stockstar.com/notice/SN1.shtml")
    assert target.read_text(encoding="utf-8").startswith(f"# {title}")


def test_stockstar_fallback_skips_summary(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        stockstar_fallback,
        "_request_html",
        lambda _url: (_ for _ in ()).throw(AssertionError("摘要不应访问备用源")),
    )

    result = stockstar_fallback.fetch_stockstar_financial_report_markdown(
        parse_symbol("603379.SH"),
        title="浙江三美化工股份有限公司2026年半年度报告摘要",
        output_path=Path(tmp_path) / "summary.md",
    )

    assert result is None


def test_stockstar_fallback_accepts_report_without_important_notice(tmp_path, monkeypatch) -> None:
    title = "2026年半年度报告"
    listing = f'<a href="//stock.stockstar.com/notice/SN2.shtml">银河磁体: {title}</a>'
    report = (
        f'<div class="article_content">成都银河磁体股份有限公司 {title} 公司代码：300127 '
        + "公司基本情况 主要会计数据和财务指标 管理层讨论与分析 财务报告 "
        + ("完整财报正文 " * 4000)
        + "</div>"
    )
    pages = iter((listing, report))
    monkeypatch.setattr(stockstar_fallback, "_request_html", lambda _url: next(pages))

    target = tmp_path / "report.md"
    result = stockstar_fallback.fetch_stockstar_financial_report_markdown(
        parse_symbol("300127.SZ"), title=title, output_path=target
    )

    assert result is not None
    assert target.is_file()


def test_summary_entry_does_not_cover_new_quarter() -> None:
    latest = _report(
        announcement_id="new-q3",
        report_type="q3",
        quarter=3,
        fiscal_year=2026,
    )
    registered = {
        "announcement_id": "old-interim",
        "report_date": "2026-07-25",
        "report_type": "半年报",
    }

    assert not _summary_entry_covers_report(registered, latest)


def test_summary_entry_uses_persisted_period_when_available() -> None:
    latest = _report(
        announcement_id="duplicate-annual",
        report_type="annual",
        quarter=4,
        fiscal_year=2025,
    )
    registered = {
        "announcement_id": "original-annual",
        "report_date": "2026-03-10",
        "report_type": "annual",
        "fiscal_year": 2025,
        "quarter": 4,
    }

    assert _summary_entry_covers_report(registered, latest)


def test_resolve_hk_financial_year_without_year_suffix() -> None:
    assert _resolve_fiscal_year("2026财务年度报告", "2026-06-18") == 2026
    assert _resolve_fiscal_year("2026 财政年度业绩公告", "2026-05-13") == 2026


def test_previous_report_skips_duplicate_same_period(monkeypatch) -> None:
    full_report = _report(
        announcement_id="full-annual",
        report_type="annual",
        quarter=4,
        fiscal_year=2026,
    )
    announcement = FinancialReportMeta(
        **{
            **full_report.__dict__,
            "announcement_id": "annual-announcement",
            "report_kind": "earnings_announcement",
            "priority": 2,
        }
    )
    previous_q3 = _report(
        announcement_id="previous-q3",
        report_type="q3",
        quarter=3,
        fiscal_year=2026,
    )
    monkeypatch.setattr(
        "services.research.financial_report_skill._load_financial_report_entries",
        lambda symbol: [full_report, announcement, previous_q3],
    )

    latest, previous = _select_latest_two_reports("09988.HK")

    assert latest == full_report
    assert previous == previous_q3
