"""美股财报原文（SEC EDGAR → disclosures/md）的回归测试。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from commons import parse_symbol
from services.news.disclosures_builder import _is_quarter_end
from services.research.financial_report_skill import (
    _classify_report_title,
    _resolve_fiscal_year,
)
from shared_data_access.us_disclosures import (
    filing_report_period,
    is_earnings_release_text,
    is_us_financial_filing,
)
from shared_data_access.us_financial_documents import (
    _format_statement_frame,
    html_to_markdown,
    parse_statement_reports,
)
from shared_data_access import us_financial_documents as documents


def _build_mock_document(monkeypatch, statements, primary):
    monkeypatch.setattr(documents, "fetch_filing_summary", lambda *a, **kw: "")
    monkeypatch.setattr(
        documents, "fetch_filing_text",
        lambda cik, accession, name, **kw: primary if name == "main.htm" else statements[name],
    )
    # Keep the real report renderer and HTML conversion in the regression path.
    monkeypatch.setattr(
        documents, "parse_statement_reports",
        lambda _: {key: key for key in statements},
    )
    return documents.build_us_financial_document_markdown(
        symbol_info=parse_symbol("TSM.US"), cik="1", form="10-Q",
        filing_date="2026-11-01", report_period="2026-09-30",
        accession="test", primary_document="main.htm",
    )


def test_document_definition_cleanup_preserves_all_three_statements(monkeypatch):
    statements = {
        key: (
            f"<table><tr><td>Item</td><td>Value</td></tr>"
            f"<tr><td>{key} fact</td><td>100</td></tr></table>"
            "<table><tr><td>- Definition</td><td>Technical metadata</td></tr>"
            "<tr><td>Name:</td><td>UnusedConcept</td></tr></table>"
        )
        for key in ("income", "balance", "cashflow")
    }
    actual = _build_mock_document(monkeypatch, statements, "<p>MD&amp;A notes</p>")
    for key in statements:
        assert f"{key} fact" in actual
    assert "Definition" not in actual
    assert "UnusedConcept" not in actual
    assert "MD&A notes" in actual


def test_document_statement_budget_preserves_primary_body(monkeypatch):
    actual = _build_mock_document(
        monkeypatch, {"income": "<p>" + "S" * 90_000 + "</p>"},
        "<p>" + "B" * 200_000 + "END_OF_NOTES</p>",
    )
    assert "S" * 80_001 not in actual
    assert "结构化报表段超长" in actual
    assert "END_OF_NOTES" in actual
    assert "原文超长" not in actual


def test_document_default_limit_truncates_at_400000_characters(monkeypatch):
    actual = _build_mock_document(monkeypatch, {}, "<p>" + "B" * 400_001 + "END</p>")
    assert "B" * 400_000 in actual
    assert "B" * 400_001 not in actual
    assert "已截断至 400000 字符" in actual


def test_is_us_financial_filing_10q_returns_true() -> None:
    # Arrange / Act
    actual = is_us_financial_filing("10-Q", "季度报告（10-Q）（报告期 2026-09-30）")

    # Assert
    assert actual is True


def test_is_us_financial_filing_monthly_revenue_returns_false() -> None:
    # 月营收 6-K 不能被当成财报，否则会把 12 份月报灌进研究链路
    actual = is_us_financial_filing(
        "6-K",
        "临时公告（6-K）（报告期 2026-09-30）",
        "TSMC September 2026 Revenue Report",
    )

    assert actual is False


def test_is_us_financial_filing_generic_6k_title_returns_false() -> None:
    # 索引里的 6-K 标题只有表单名，静态判定必须保守，交给同步阶段看正文要点
    actual = is_us_financial_filing("6-K", "临时公告（6-K）（报告期 2026-09-30）")

    assert actual is False


def test_is_earnings_release_text_financial_statements_returns_true() -> None:
    actual = is_earnings_release_text(
        "English Translation of Financial Statements Originally Issued in Chinese"
    )

    assert actual is True


def test_is_earnings_release_text_dividend_returns_false() -> None:
    actual = is_earnings_release_text("Adjustment to Cash Dividend Per Share")

    assert actual is False


def test_filing_report_period_prefers_title_over_filing_date() -> None:
    # 标题里的报告期是会计期间末，申报日期是披露日，二者不能混用
    actual = filing_report_period(
        "临时公告（6-K）（报告期 2026-09-30）",
        "2026-10-15",
    )

    assert actual == "2026-09-30"


def test_classify_report_title_us_6k_returns_q3_earnings_announcement() -> None:
    # Arrange
    symbol = "TSM.US"

    # Act
    report_type, quarter, kind, priority = _classify_report_title(
        symbol,
        "临时公告（6-K）（报告期 2026-09-30）",
    )

    # Assert
    assert (report_type, quarter, kind, priority) == ("q3", 3, "earnings_announcement", 2)


def test_classify_report_title_us_20f_returns_full_report() -> None:
    report_type, quarter, kind, priority = _classify_report_title(
        "TSM.US",
        "年度报告（20-F）（报告期 2025-12-31）",
    )

    assert (report_type, quarter, kind) == ("annual", 4, "full_report")
    assert priority == 0


def test_resolve_fiscal_year_us_annual_keeps_period_year() -> None:
    # A 股「年报次年披露」要减一；美股报告期就是财年末，不能减
    actual = _resolve_fiscal_year("年度报告（20-F）（报告期 2025-12-31）", "2026-04-16")

    assert actual == 2025


def test_parse_statement_reports_picks_three_statements() -> None:
    # Arrange
    summary = """
    <Report><HtmlFileName>R2.htm</HtmlFileName><ShortName>Condensed Consolidated Statements of Income</ShortName></Report>
    <Report><HtmlFileName>R4.htm</HtmlFileName><ShortName>Condensed Consolidated Balance Sheets</ShortName></Report>
    <Report><HtmlFileName>R7.htm</HtmlFileName><ShortName>Condensed Consolidated Statements of Cash Flows</ShortName></Report>
    <Report><HtmlFileName>R8.htm</HtmlFileName><ShortName>Summary of Significant Accounting Policies</ShortName></Report>
    """

    # Act
    actual = parse_statement_reports(summary)

    # Assert
    assert actual == {"income": "R2.htm", "balance": "R4.htm", "cashflow": "R7.htm"}


def test_html_to_markdown_renders_table_only_once() -> None:
    # 表格外层 div 的文本会把整张表再输出一遍，必须只保留表格版本
    html = (
        "<div><p>Headline</p>"
        "<table><tr><td>A</td><td>B</td></tr>"
        "<tr><td>Net sales</td><td>100</td></tr></table></div>"
    )

    actual = html_to_markdown(html)

    assert actual.count("Net sales") == 1
    assert "| Net sales | 100 |" in actual


def test_format_statement_frame_keeps_recent_rows_without_scientific_notation() -> None:
    # Arrange
    frame = pd.DataFrame(
        {
            "REPORT_DATE": [f"20{year}-12-31" for year in range(10, 20)],
            "TOTAL_ASSETS": [1.19695e11 + index for index in range(10)],
        }
    )

    # Act
    actual = _format_statement_frame(frame, {"TOTAL_ASSETS": "总资产"})

    # Assert
    assert list(actual.columns) == ["报告期", "总资产"]
    assert actual["报告期"].tolist()[0] == "2019-12-31"
    assert "e+" not in str(actual["总资产"].tolist()[0])


def test_is_quarter_end_rejects_event_dates() -> None:
    assert _is_quarter_end("2026-09-30") is True
    assert _is_quarter_end("2026-06-04") is False
    assert _is_quarter_end("") is False


def test_8k_results_item_on_event_date_is_financial_but_regfd_is_not():
    assert is_us_financial_filing(
        "8-K", "重大事项公告（8-K）（报告期 2026-09-02；事项 2.02,8.01,9.01）",
        "Financial Results and Quarterly Dividend",
    )
    assert not is_us_financial_filing(
        "8-K", "重大事项公告（8-K）（报告期 2026-09-02；事项 7.01,9.01）",
    )


def test_sync_8k_results_on_non_quarter_end_materializes_earnings(monkeypatch, tmp_path):
    from services.news import disclosures_builder as builder

    meta = builder.AnnouncementMeta(
        announcement_id="earnings", org_id="1730168", stock_code="AVGO.US",
        title="重大事项公告（8-K）（报告期 2026-09-02；事项 2.02,8.01,9.01）",
        date="2026-09-02", url="", category="8-K", primary_document="main.htm",
    )
    items = {"earnings": meta}
    monkeypatch.setattr(builder, "index_path", lambda _: tmp_path / "index.json")
    monkeypatch.setattr(builder, "load_index", lambda _: items)
    monkeypatch.setattr(builder, "save_index", lambda *args: None)
    monkeypatch.setattr(builder, "md_dir", lambda _: tmp_path)
    monkeypatch.setattr(builder, "build_us_financial_document_markdown", lambda **kw: "quarterly financial results")

    class Access:
        def prepare_dataset(self, **kwargs):
            pass

    assert builder._sync_us_financial_documents(parse_symbol("AVGO.US"), data_access=Access()) == 1
    assert meta.is_financial_report
    assert Path(meta.md_path).read_text() == "quarterly financial results"


def test_select_8k_results_with_stale_false_marker_before_full_10q(monkeypatch, tmp_path):
    from services.news import disclosures_builder as builder
    from services.research import financial_report_skill as skill

    root = tmp_path / "AVGO.US"
    (root / "disclosures").mkdir(parents=True)
    (root / "disclosures/index.json").write_text("{}")
    entries = {}
    for ident, form, period, day, marked in [
        ("q2", "10-Q", "2026-05-03", "2026-06-09", True),
        ("release", "8-K", "2026-09-02", "2026-09-02", False),
        ("q3", "10-Q", "2026-08-02", "2026-09-10", True),
    ]:
        title = f"财报（{form}）（报告期 {period}）"
        if form == "8-K":
            title += "（事项 2.02,8.01,9.01）"
        entries[ident] = builder.AnnouncementMeta(
            announcement_id=ident, org_id="1", stock_code="AVGO.US", title=title,
            date=day, url="", category=form, is_financial_report=marked,
        )
    monkeypatch.setattr(skill, "_stock_root", lambda _: root)
    monkeypatch.setattr(skill, "load_index", lambda _: entries)
    latest, previous = skill.select_latest_two_reports("AVGO.US", available_on_date="2026-09-04")
    assert latest.announcement_id == "release"
    assert previous.announcement_id == "q2"


def test_us_full_report_after_release_uses_filing_date_not_8k_event_quarter(monkeypatch, tmp_path):
    from services.news import disclosures_builder as builder
    from services.research import financial_report_skill as skill

    (tmp_path / "disclosures").mkdir()
    (tmp_path / "disclosures/index.json").write_text("{}")
    entries = {}
    for ident, form, period, day in [
        ("q1", "10-Q", "2026-03-31", "2026-04-30"),
        ("release", "8-K", "2026-07-22", "2026-07-22"),
        ("q2", "10-Q", "2026-06-30", "2026-07-23"),
    ]:
        title = f"财报（{form}）（报告期 {period}）"
        if form == "8-K":
            title += "（事项 2.02,9.01）"
        entries[ident] = builder.AnnouncementMeta(
            announcement_id=ident, org_id="1", stock_code="GOOGL.US", title=title,
            date=day, url="", category=form, is_financial_report=form == "10-Q",
        )
    monkeypatch.setattr(skill, "_stock_root", lambda _: tmp_path)
    monkeypatch.setattr(skill, "load_index", lambda _: entries)
    latest, previous = skill.select_latest_two_reports("GOOGL.US", available_on_date="2026-09-04")
    assert latest.announcement_id == "q2"
    assert previous.announcement_id == "q1"


def test_document_missing_primary_does_not_return_tables_only(monkeypatch):
    actual = _build_mock_document(monkeypatch, {"income": "<p>Income 100</p>"}, "")
    assert actual == ""


def test_sync_us_legacy_cache_rebuilt_and_current_cache_skipped(monkeypatch, tmp_path):
    from services.news import disclosures_builder as builder

    old_path = tmp_path / "old.md"
    current_path = tmp_path / "current.md"
    old_path.write_text("old\n> 原文超长，已截断至 120000 字符", encoding="utf-8")
    current_path.write_text("current", encoding="utf-8")
    items = {
        key: builder.AnnouncementMeta(
            announcement_id=key, org_id="1", stock_code="TSM.US",
            title="10-Q", date="2026-08-01", url="", category="10-Q",
            primary_document="main.htm", md_path=str(path),
        )
        for key, path in (("old", old_path), ("current", current_path))
    }
    monkeypatch.setattr(builder, "index_path", lambda _: tmp_path / "index.json")
    monkeypatch.setattr(builder, "load_index", lambda _: items)
    monkeypatch.setattr(builder, "save_index", lambda *args: None)
    called = []

    def generate(**kwargs):
        called.append(kwargs["accession"])
        return ""

    monkeypatch.setattr(builder, "build_us_financial_document_markdown", generate)

    class Access:
        def prepare_dataset(self, **kwargs):
            pass

    # Failure leaves the old file and its index reference intact for retry.
    assert builder._sync_us_financial_documents(parse_symbol("TSM.US"), data_access=Access()) == 0
    assert "120000" in old_path.read_text(encoding="utf-8")
    assert called == ["old"]
    monkeypatch.setattr(builder, "build_us_financial_document_markdown", lambda **kw: "complete")
    assert builder._sync_us_financial_documents(parse_symbol("TSM.US"), data_access=Access()) == 1
    assert old_path.read_text(encoding="utf-8") == "complete"
    assert current_path.read_text(encoding="utf-8") == "current"
    assert not list(tmp_path.glob("*.tmp"))


def test_us_markdown_replace_failure_preserves_original(monkeypatch, tmp_path):
    from pathlib import Path
    import pytest
    from services.news.disclosures_builder import _write_us_financial_markdown

    target = tmp_path / "old.md"
    target.write_text("original", encoding="utf-8")

    def fail_replace(*args):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        _write_us_financial_markdown(target, "new")
    assert target.read_text(encoding="utf-8") == "original"
    assert list(tmp_path.iterdir()) == [target]
