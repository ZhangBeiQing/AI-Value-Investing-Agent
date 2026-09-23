from pathlib import Path
import json

from services.research import stock_analysis

from services.research.financial_report_context import (
    build_financial_report_context,
    calculate_pro_forma_ttm,
    extract_pre_announcement_research_sections,
    extract_markdown_section_by_prefix,
    find_historical_research_package,
    resolve_pre_announcement_market_date,
)
from services.research import financial_report_skill
from services.research.financial_report_skill import (
    FinancialReportMeta,
    StockReportBundle,
    prepare_financial_report_workdir,
    validate_deep_research_artifacts,
)


def test_announcement_without_time_uses_previous_weekday() -> None:
    assert resolve_pre_announcement_market_date("2026-08-03") == "2026-07-31"


def test_after_close_announcement_can_use_same_day() -> None:
    assert (
        resolve_pre_announcement_market_date(
            "2026-08-03",
            "2026-08-03 16:30:00",
        )
        == "2026-08-03"
    )


def test_before_close_announcement_uses_previous_weekday() -> None:
    assert (
        resolve_pre_announcement_market_date(
            "2026-08-03",
            "2026-08-03 09:15:00",
        )
        == "2026-07-31"
    )


def test_historical_package_never_crosses_cutoff_and_obeys_book_priority(tmp_path: Path) -> None:
    for run_date, book in (
        ("2026-07-30", "short_book"),
        ("2026-07-31", "short_book"),
        ("2026-07-31", "fixed_tracked"),
        ("2026-08-01", "fixed_tracked"),
    ):
        folder = tmp_path / run_date / book / "04_stock_research"
        folder.mkdir(parents=True)
        (folder / f"示例_000001.SZ_{run_date}_research.md").write_text(
            "# research",
            encoding="utf-8",
        )

    result = find_historical_research_package(
        "000001.SZ",
        "2026-07-31",
        skill_runs_root=tmp_path,
    )
    assert result is not None
    assert result.snapshot_date == "2026-07-31"
    assert result.book_type == "fixed_tracked"


def test_extract_pre_context_excludes_financial_summary_and_trade_memory() -> None:
    source = """# 股票研究包

## 1. 股票指标与估值

### 1.1 Price Report JSON
price

### 1.2 Valuation Report (Markdown)
valuation

## 2. 新闻与公告
guidance

## 3. 财报摘要、机构一致预期与 Forecast

### A股机构一致预期（同花顺汇总）
annual consensus

---

# 旧财报总结
old report conclusion

## 4. 持仓与投资逻辑记忆
recommended_action: BUY
"""
    result = extract_pre_announcement_research_sections(source)
    combined = "\n".join(result.values())
    assert "price" in combined
    assert "valuation" in combined
    assert "guidance" in combined
    assert "annual consensus" in combined
    assert "old report conclusion" not in combined
    assert "recommended_action" not in combined


def test_extract_hk_consensus_heading_with_fiscal_year_suffix() -> None:
    source = """## 3. 财报摘要、机构一致预期与 Forecast

### 港股机构一致预期（经济通，2026财年）
| 证券商 | 纯利/亏损 |
| --- | ---: |
| 汇丰 | 21947 |

### 港股目标价统计
统计内容
"""
    section = extract_markdown_section_by_prefix(
        source,
        "港股机构一致预期（经济通",
    )
    assert "汇丰" in section
    extracted = extract_pre_announcement_research_sections(source)
    assert "21947" in extracted["annual_consensus"]


def test_pre_context_without_historical_package_keeps_cutoff_consensus() -> None:
    from services.research.financial_report_context import _render_pre_context

    rendered = _render_pre_context(
        symbol="00175.HK",
        stock_name="吉利汽车",
        announcement_date="2026-08-17",
        announcement_datetime="2026-08-17 12:16:00",
        requested_pre_date="2026-08-14",
        evidence_cutoff="2026-08-17 12:16:00",
        historical_research=None,
        hk_consensus="| 证券商 | 纯利/亏损 |\n| --- | ---: |\n| 汇丰 | 21947 |",
        hk_consensus_path=Path("profit_forecast.csv"),
    )
    assert "汇丰" in rendered
    assert "要求更新日期早于 2026-08-17" in rendered
    assert "公告前证据截止：2026-08-17 12:16:00" in rendered
    assert "按证据截止时间重建" in rendered


def test_pro_forma_ttm() -> None:
    assert calculate_pro_forma_ttm(100.0, 40.0, 25.0) == 115.0


def test_enhanced_valuation_uses_latest_available_dated_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    analysis_dir = tmp_path / "pe_pb_analysis"
    analysis_dir.mkdir()
    prefix = "示例公司_000001.SZ"
    json_path = analysis_dir / f"{prefix}_20260811_enhanced_pe_analysis.json"
    md_path = analysis_dir / f"{prefix}_20260811_enhanced_pe_analysis.md"
    json_path.write_text("{}", encoding="utf-8")
    md_path.write_text("# valuation", encoding="utf-8")

    class FakeAnalyzer:
        def __init__(self, **_kwargs) -> None:
            pass

        def analyze_stock(self, *_args, **_kwargs) -> None:
            pass

        def _prepare_paths(self, _symbol):
            return {"root": tmp_path / prefix, "analysis": analysis_dir}

    monkeypatch.setattr(stock_analysis, "EnhancedPEPBAnalyzer", FakeAnalyzer)
    monkeypatch.setattr(
        stock_analysis,
        "_resolve_effective_trade_datetime",
        lambda *_args: __import__("datetime").datetime(2026, 8, 12),
    )

    result = stock_analysis.run_enhanced_pe_pb_analysis("000001.SZ", "2026-08-12")

    assert result["analysis_date"] == "2026-08-11"
    assert result["analysis_json_path"] == str(json_path)


def test_quality_gate_accepts_direct_final_after_single_challenge(
    tmp_path: Path,
) -> None:
    outputs = {
        "pre_announcement_expectations": tmp_path / "expectation.md",
        "draft_v1": tmp_path / "draft_v1.md",
        "challenge_round_01": tmp_path / "challenge.md",
    }
    outputs["pre_announcement_expectations"].write_text(
        "公告前预期研究" * 50,
        encoding="utf-8",
    )
    outputs["draft_v1"].write_text(
        "有效研究内容" * 50
        + "\n\n## 18. 未解决问题与披露限制\n草稿过程记录"
        + "\n\n## 19. 证据与来源\n草稿来源记录",
        encoding="utf-8",
    )
    outputs["challenge_round_01"].write_text(
        "## 问题：现金流背离\n\n严重度：high\n\n" + "审计证据" * 50,
        encoding="utf-8",
    )
    final_report = tmp_path / "final.md"
    final_report.write_text(
        "## 16. 风险、替代解释与证伪条件\n现金流背离风险已经核验。\n\n"
        "## 17. 下一季度验证清单\n继续检查现金流。\n\n"
        + "最终基本面研究" * 50,
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "output_path": str(final_report),
                "research_output_paths": {
                    key: str(path)
                    for key, path in outputs.items()
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    errors = validate_deep_research_artifacts(
        manifest_path=manifest,
        final_report_path=final_report,
    )
    assert not any("closure_review" in error for error in errors)
    assert errors == []


def test_quality_gate_rejects_process_appendices_in_final_report(
    tmp_path: Path,
) -> None:
    outputs = {
        "pre_announcement_expectations": tmp_path / "expectation.md",
        "draft_v1": tmp_path / "draft_v1.md",
        "challenge_round_01": tmp_path / "challenge.md",
    }
    for path in outputs.values():
        path.write_text("有效研究内容" * 50, encoding="utf-8")
    final_report = tmp_path / "final.md"
    final_report.write_text(
        "最终基本面研究" * 50
        + "\n\n## 18. 未解决问题与披露限制\n不应进入最终报告"
        + "\n\n## 19. 证据与来源\n不应进入最终报告",
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "output_path": str(final_report),
                "research_output_paths": {
                    key: str(path)
                    for key, path in outputs.items()
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    errors = validate_deep_research_artifacts(
        manifest_path=manifest,
        final_report_path=final_report,
    )

    assert any("禁止的过程性独立章节" in error for error in errors)


def test_quality_gate_rejects_urls_and_markdown_links_in_final_report(tmp_path: Path) -> None:
    expectation = tmp_path / "expectation.md"
    draft = tmp_path / "draft.md"
    challenge = tmp_path / "challenge.md"
    final_report = tmp_path / "final.md"
    expectation.write_text("公告前预期" * 100, encoding="utf-8")
    draft.write_text("初稿" * 100, encoding="utf-8")
    challenge.write_text("质询" * 100, encoding="utf-8")
    final_report.write_text(
        "最终报告" * 100 + "\nhttps://example.com\n[来源](https://example.com/source)",
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "output_path": str(final_report),
                "research_output_paths": {
                    "pre_announcement_expectations": str(expectation),
                    "draft_v1": str(draft),
                    "challenge_round_01": str(challenge),
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    errors = validate_deep_research_artifacts(
        manifest_path=manifest,
        final_report_path=final_report,
    )
    assert any("原始 URL" in error for error in errors)
    assert any("Markdown 链接" in error for error in errors)


def test_quality_gate_rejects_skipped_expectation_search(tmp_path: Path) -> None:
    expectation = tmp_path / "expectation.md"
    draft = tmp_path / "draft.md"
    challenge = tmp_path / "challenge.md"
    final_report = tmp_path / "final.md"
    expectation.write_text(
        "search_status = not_performed_time_contamination_risk\n" + "预期研究" * 50,
        encoding="utf-8",
    )
    draft.write_text("初稿" * 100, encoding="utf-8")
    challenge.write_text("质询" * 100, encoding="utf-8")
    final_report.write_text("最终报告" * 100, encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "output_path": str(final_report),
                "research_output_paths": {
                    "pre_announcement_expectations": str(expectation),
                    "draft_v1": str(draft),
                    "challenge_round_01": str(challenge),
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    errors = validate_deep_research_artifacts(
        manifest_path=manifest,
        final_report_path=final_report,
    )
    assert any("跳过了联网搜索" in error for error in errors)


def test_quality_gate_rejects_denial_when_broker_consensus_exists(tmp_path: Path) -> None:
    expectation = tmp_path / "expectation.md"
    draft = tmp_path / "draft.md"
    challenge = tmp_path / "challenge.md"
    final_report = tmp_path / "final.md"
    expectation.write_text("未取得正式年度一致预期。" + "预期研究" * 50, encoding="utf-8")
    draft.write_text("初稿" * 100, encoding="utf-8")
    challenge.write_text("质询" * 100, encoding="utf-8")
    final_report.write_text("最终报告" * 100, encoding="utf-8")
    (tmp_path / "pre_announcement_market_context.md").write_text(
        "| 证券商 | 更新日期 | 纯利/亏损 |\n| --- | --- | ---: |\n| 汇丰 | 2026-06-12 | 21947 |",
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "output_path": str(final_report),
                "research_output_paths": {
                    "pre_announcement_expectations": str(expectation),
                    "draft_v1": str(draft),
                    "challenge_round_01": str(challenge),
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    errors = validate_deep_research_artifacts(
        manifest_path=manifest,
        final_report_path=final_report,
    )
    assert any("错误声称未取得年度一致预期" in error for error in errors)


def test_context_builder_writes_all_deterministic_inputs(tmp_path: Path) -> None:
    workdir = tmp_path / "financial_report_workdir"
    workdir.mkdir()
    stock_root = tmp_path / "stock"
    stock_root.mkdir()
    result = build_financial_report_context(
        symbol="000001.SZ",
        stock_name="示例公司",
        industry_name="示例行业",
        analysis_date="2026-08-03",
        announcement_date="2026-08-03",
        current_announcement_id="announcement-1",
        workdir=workdir,
        stock_root=stock_root,
        summary_index_path=stock_root / "financial_reports" / "summary_index.json",
        generate_current_market=False,
    )
    assert result.pre_announcement_market_date == "2026-07-31"
    for path in (
        result.pre_context_path,
        result.current_context_path,
        result.prior_memory_path,
        result.existing_industry_research_path,
    ):
        assert path.exists()
        assert path.read_text(encoding="utf-8").strip()


def test_latest_report_selection_respects_analysis_date(monkeypatch) -> None:
    older = FinancialReportMeta(
        announcement_id="old",
        title="2026年半年度报告",
        date="2026-07-30",
        report_type="interim",
        quarter=2,
        fiscal_year=2026,
        report_kind="full_report",
        priority=0,
        pdf_path=None,
        md_path=None,
    )
    future = FinancialReportMeta(
        announcement_id="future",
        title="2026年三季度报告",
        date="2026-08-03",
        report_type="q3",
        quarter=3,
        fiscal_year=2026,
        report_kind="full_report",
        priority=0,
        pdf_path=None,
        md_path=None,
    )
    monkeypatch.setattr(
        financial_report_skill,
        "_load_financial_report_entries",
        lambda symbol: [future, older],
    )
    latest, _ = financial_report_skill._select_latest_two_reports(
        "000001.SZ",
        available_on_date="2026-07-31",
    )
    assert latest == older


def test_prepare_workdir_writes_run_paths_and_policy_copy(tmp_path: Path, monkeypatch) -> None:
    latest_source = tmp_path / "2026年半年度报告.md"
    previous_source = tmp_path / "2026年一季度报告.md"
    latest_source.write_text("最新财报原文" * 50, encoding="utf-8")
    previous_source.write_text("上一期财报原文" * 50, encoding="utf-8")
    report = FinancialReportMeta(
        announcement_id="announcement-2",
        title="2026年半年度报告",
        date="2026-08-03",
        report_type="interim",
        quarter=2,
        fiscal_year=2026,
        report_kind="full_report",
        priority=0,
        pdf_path=None,
        md_path=latest_source,
    )
    previous = FinancialReportMeta(
        announcement_id="announcement-1",
        title="2026年一季度报告",
        date="2026-04-30",
        report_type="q1",
        quarter=1,
        fiscal_year=2026,
        report_kind="full_report",
        priority=0,
        pdf_path=None,
        md_path=previous_source,
    )
    bundle = StockReportBundle(
        symbol="000001.SZ",
        stock_name="示例公司",
        final_mandate="manual",
        industry_name="示例行业",
        latest_report=report,
        previous_report=previous,
        output_path=tmp_path / "financial_reports" / "20260803.md",
        summary_index_path=tmp_path / "financial_reports" / "summary_index.json",
    )
    target_workdir = tmp_path / "workdir"
    monkeypatch.setattr(
        financial_report_skill,
        "financial_report_workdir",
        lambda symbol: target_workdir,
    )
    result = prepare_financial_report_workdir(
        bundle,
        latest_path=latest_source,
        previous_path=previous_source,
        analysis_date="2026-08-03",
        generate_current_market=False,
    )
    assert result == target_workdir
    for filename in (
        "01_latest_report.md",
        "02_previous_report.md",
        "03_report_analysis_prompt.md",
        "04_future_outlook_prompt.md",
        "05_agent_input.md",
        "pre_announcement_market_context.md",
        "current_market_context.md",
        "valuation_framework.md",
        "prior_fundamental_memory.md",
        "existing_industry_research.md",
        "manifest.json",
    ):
        assert (target_workdir / filename).exists()
    manifest = json.loads((target_workdir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["analysis_date"] == "2026-08-03"
    assert manifest["pre_announcement_market_date"] == "2026-07-31"
    assert "draft_v1" in manifest["research_output_paths"]
    assert "draft_v2" not in manifest["research_output_paths"]
    agent_input = (target_workdir / "05_agent_input.md").read_text(encoding="utf-8")
    assert "历史披露按需回溯（只读）" in agent_input
    assert str(tmp_path / "disclosures" / "md") in agent_input
    assert str(tmp_path / "disclosures" / "pdfs") in agent_input
    assert "不得批量转换 PDF" in agent_input
    assert "自动调用 PDF 转换" in agent_input
