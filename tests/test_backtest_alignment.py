import json
from pathlib import Path

import pandas as pd
import pytest

from configs.stock_pool import fixed_tracked_symbols, USD_CNY_FX_RATE
from commons import parse_symbol
from shared_data_access.paths import price_cache_dir
from services.backtest.experiment import create_backtest_experiment, load_backtest_experiment
from services.backtest.execution import simulate_post_trade
from services.backtest.ledger import BacktestLedger
from services.backtest.metrics import finalize_backtest
from services.backtest.universe import build_daily_universe
from services.backtest.prices import quote_date_as_of
from services.research import financial_report as reports
from services.research.financial_report_validation import is_raw_financial_report, historical_summary_issue
from services.research.financial_report_skill import _classify_report_title, update_summary_index
from scripts.register_financial_report_summary import _is_meaningful_summary
from services.document_conversion.pdf_markdown import PDFConversionResult, write_pdf_conversion_artifacts

D = "2026-07-01"
E = "2026-07-02"
RAW = "证券代码：301536\n# 星宸科技股份有限公司\n# 2026 年第一季度报告\n本公司及董事会全体成员保证财务报告的真实、准确、完整。\n## 主要财务数据\n" + "财务报表原文数据" * 50


def experiment(tmp_path, cash=1000):
    return create_backtest_experiment(start_date=D, end_date=E, initial_cash=cash,
        experiment_id="case", network_mode="disabled", source_data_root=tmp_path / "source",
        backtests_root=tmp_path / "backtests")


def write_prices(ex, symbol, rows):
    path = price_cache_dir(parse_symbol(symbol), base_dir=ex.context.source_data_root)
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path / "price.csv", index=False)


def decision_entry(symbol, action, qty, confidence=0.5):
    return dict(symbol=symbol, stock_name=symbol, scan="量价扫描", delta_summary="变化",
        key_facts=["事实"], inferences=["判断"], court=dict(pro=["正方"],con=["反方"],verdict="独立裁决"),
        price_impression="合理",recommended_action="次日预案",action_type=action,action_num=qty,
        key_risks=["风险"],next_day_watchlist=["验证"],confidence_score=confidence)


def execute(ex, entries):
    book=ex.context.skill_runs_root / D / "fixed_tracked"
    book.mkdir(parents=True,exist_ok=True)
    (book / "05_decision.json").write_text(json.dumps(dict(summary_date=D,system_risk_notes=[],
        system_focus_items=[],stock_decisions=entries)))
    log,_,_=simulate_post_trade(ex,decision_date=D,execution_date=E)
    return json.loads(log.read_text())


def test_fixed_scope_matches_daily_pool_without_quant_candidates(tmp_path):
    ex=experiment(tmp_path)
    assert list(ex.base_universe)==fixed_tracked_symbols()
    folder=ex.context.source_data_root / "selection_runs" / D
    folder.mkdir(parents=True)
    (folder / "12_quant_prefilter_long.csv").write_text("symbol,score\n000792.SZ,1\n")
    ledger=BacktestLedger(agent_data_root=ex.context.agent_data_root,signature=ex.signature,source_data_root=ex.context.source_data_root)
    scope,_=build_daily_universe(ex,D,ledger)
    assert scope==fixed_tracked_symbols()
    assert "000792.SZ" not in scope


def test_old_experiment_is_not_silently_resumed(tmp_path):
    ex=experiment(tmp_path);p=ex.root / "experiment.json";data=json.loads(p.read_text())
    data["schema_version"]=1;p.write_text(json.dumps(data))
    with pytest.raises(ValueError,match="旧版回测"):
        load_backtest_experiment("case",backtests_root=tmp_path / "backtests")


@pytest.mark.parametrize("quantity,reason",[(200,"insufficient_cash"),(150,"invalid_trade_unit")])
def test_buy_is_rejected_without_automatic_quantity_change(tmp_path,quantity,reason):
    ex=experiment(tmp_path)
    write_prices(ex,"600150.SH",[{"日期":D,"开盘":10,"收盘":10},{"日期":E,"开盘":10,"收盘":10}])
    result=execute(ex,[decision_entry("600150.SH","BUY",quantity)])
    assert result["orders"][0]["reason"]==reason
    assert result["orders"][0]["filled_shares"]==0
    assert result["ending_positions"]["CASH"]==1000
    assert result["orders"][0]["adjustments"]==[]


def test_buy_batch_is_atomic_and_does_not_rank_by_confidence(tmp_path):
    ex=experiment(tmp_path,cash=1500)
    for sym in ("600150.SH","600406.SH"):
        write_prices(ex,sym,[{"日期":E,"开盘":10,"收盘":10}])
    result=execute(ex,[decision_entry("600150.SH","BUY",100,0.1),decision_entry("600406.SH","BUY",100,0.99)])
    assert [x["symbol"] for x in result["orders"]]==["600150.SH","600406.SH"]
    assert all(x["reason"]=="insufficient_cash" for x in result["orders"])
    assert result["actions"]==[]


def test_us_purchase_and_mark_to_market_use_shared_cny_conversion(tmp_path):
    ex=experiment(tmp_path,cash=10000)
    write_prices(ex,"TSM.US",[{"日期":"2026-06-30","开盘":90,"收盘":95},
        {"日期":D,"开盘":98,"收盘":100},{"日期":E,"开盘":100,"收盘":999}])
    result=execute(ex,[decision_entry("TSM.US","BUY",10)])
    assert result["ending_positions"]["CASH"]==round(10000-100*10*USD_CNY_FX_RATE,2)
    assert result["ending_total_value"]==10000
    assert result["orders"][0]["price_local"]==100
    assert result["orders"][0]["execution_price"]==100*USD_CNY_FX_RATE
    assert quote_date_as_of("TSM.US",E)==D


def test_finalize_rejects_incomplete_period(tmp_path):
    ex=experiment(tmp_path)
    with pytest.raises(ValueError,match="未完成"):
        finalize_backtest(ex)


def test_raw_disclosure_is_rejected_by_summary_registration(tmp_path):
    p=tmp_path / "20260424.md";p.write_text(RAW)
    assert is_raw_financial_report(RAW)
    assert not _is_meaningful_summary(p)
    with pytest.raises(ValueError,match="原文"):
        update_summary_index(symbol="301536.SZ",stock_name="星宸科技",latest_report=None,previous_report=None,output_path=p)


def test_pdf_conversion_cannot_publish_raw_text_to_summary_directory(tmp_path):
    result=PDFConversionResult(markdown=RAW,profile="financial_report",converter_version=1,
        converter_signature="test",engine="test",engine_version=None,metrics={},generated_at=D)
    with pytest.raises(ValueError,match="总结目录"):
        write_pdf_conversion_artifacts(tmp_path / "financial_reports/20260424.md",result,tmp_path / "source.pdf")
    assert not (tmp_path / "financial_reports/20260424.md").exists()


def test_historical_selection_uses_alias_and_skips_future_or_raw_reports(tmp_path,monkeypatch):
    stockroot=tmp_path / "current_688213.SH";alias=tmp_path / "old_688213.SH"
    for root in (stockroot,alias):(root / "financial_reports").mkdir(parents=True)
    (stockroot / "financial_reports/20260430.md").write_text("# 深度研究\n截至 2026 年 8 月 17 日。"+"研究"*100)
    (stockroot / "financial_reports/20260501.md").write_text(RAW)
    valid=alias / "financial_reports/20260430.md"
    valid.write_text("# 基本面深度研究\n信息截止：2026-04-30\n"+"核验事实与情景"*100)
    monkeypatch.setattr(reports,"_stock_root_dir",lambda *_:stockroot)
    result=reports.get_financial_report_summary("688213.SH",D,include_consensus=False,
        include_price_drift=False,require_historical_cutoff=True)
    assert result["report_path"]==str(valid)
    assert len(result["rejected_historical_reports"])==1
    assert "8 月 17" not in result["content"]


def test_unknown_knowledge_date_cannot_be_inferred_from_filename(tmp_path):
    p=tmp_path / "20260430.md";p.write_text("# 深度研究\n"+"正文"*100)
    assert "无法确认" in historical_summary_issue(p,D)


@pytest.mark.parametrize("cutoff", [None, "2026-09-05"])
def test_financial_period_heading_does_not_establish_research_cutoff(tmp_path, cutoff):
    from services.research.financial_report_validation import report_knowledge_cutoff

    path = tmp_path / "20260831.md"
    content = "# 智谱2026年中期（截至2026-06-30）季度基本面深度研究\n"
    if cutoff:
        content += f"知识截止日：{cutoff}\n"
    path.write_text(content + "研究正文" * 100)

    actual = report_knowledge_cutoff(path)
    assert (actual.isoformat() if actual else None) == cutoff
    issue = historical_summary_issue(path, "2026-09-04")
    assert ("晚于分析日" if cutoff else "无法确认") in issue


@pytest.mark.parametrize("header,expected", [
    ("本文事实与市场数据严格截止2026-08-19。", "2026-08-19"),
    ("本报告严格以2026年8月17日为信息截止日；2026年8月18日发布的半年报均不纳入。", "2026-08-17"),
])
def test_historical_cutoff_recognizes_explicit_bound_without_crossing_clause(tmp_path, header, expected):
    from services.research.financial_report_validation import report_knowledge_cutoff

    path = tmp_path / "20260430.md"
    path.write_text("# 基本面深度研究\n" + header + "\n" + "正文" * 100)
    assert str(report_knowledge_cutoff(path)) == expected
    assert expected in historical_summary_issue(path, D)


def test_operating_data_is_not_excluded_only_because_of_its_title():
    assert _classify_report_title("603501.SH","2026年第三季度主要经营数据的公告")[2] is not None


def test_sponsor_supervision_annual_report_is_not_company_financial_report():
    title = "中国国际金融股份有限公司关于宁波德业科技股份有限公司2025年度持续督导年度报告书"
    assert _classify_report_title("605117.SH", title)[2] is None
    assert _classify_report_title("605117.SH", "2025年年度报告")[2] == "full_report"


def test_action_plan_interim_report_does_not_replace_financial_statements():
    title = "华泰证券股份有限公司2026年度“提质增效重回报”行动方案落实情况半年度报告"
    assert _classify_report_title("601688.SH", title)[2] is None
    assert _classify_report_title("601688.SH", "2026年半年度报告")[2] == "full_report"


def test_historical_disclosure_input_excludes_unbound_structured_supplement(tmp_path):
    from services.research.financial_report_skill import _copy_report_to_workdir

    source = tmp_path / "cached.md"
    official = "# SEC 6-K\n公司原始一季报、资产负债表及现金流量表\n"
    source.write_text(official + "\n## 补充：结构化财务报表（第三方数据）\n2026-09-30 后续报表\n")
    historical = tmp_path / "historical.md"
    live = tmp_path / "live.md"
    _copy_report_to_workdir(source, historical, historical_mode=True)
    _copy_report_to_workdir(source, live, historical_mode=False)
    assert historical.read_text() == official
    assert live.read_bytes() == source.read_bytes()
    assert "2026-09-30" in source.read_text()


def test_prepare_removes_legacy_expectation_snapshot_even_on_same_day(tmp_path):
    from services.research.financial_report_skill import _clear_stale_research_outputs

    legacy = tmp_path / "expectation_snapshot.md"
    current = tmp_path / "pre_announcement_expectations.md"
    legacy.write_text("旧任务未来快照")
    current.write_text("本次独立Scout结果")
    _clear_stale_research_outputs(tmp_path,
        existing_manifest={"latest_announcement_id": "q1", "analysis_date": D},
        current_announcement_id="q1", current_analysis_date=D)
    assert not legacy.exists()
    assert current.read_text() == "本次独立Scout结果"


def test_historical_revision_wins_same_release_without_overwriting_original(tmp_path):
    from datetime import datetime

    original = tmp_path / "20260430.md"
    revised = tmp_path / "20260430_asof_20260701.md"
    original.write_text("# 深研\n信息截止：2026-04-30\n" + "事实" * 100)
    revised.write_text("# 深研\n信息截止：2026-04-30\n" + "核验事实" * 100)
    future_revision = tmp_path / "20260430_asof_20260702.md"
    future_revision.write_text("# 深研\n信息截止：2026-04-30\n" + "核验事实" * 100)
    selected, _ = reports._select_reports(
        [original, revised, future_revision], datetime.fromisoformat(D), prefer_historical_revision=True,
    )
    assert selected.path == revised
    selected, _ = reports._select_reports([original, revised], datetime.fromisoformat(D))
    assert selected.path == original


def test_prior_memory_rejects_rewritten_future_research_and_uses_earlier_safe_version(tmp_path):
    from services.research.financial_report_context import _select_prior_summary

    future = tmp_path / "20260331.md"
    safe = tmp_path / "20251030.md"
    future.write_text("# 深研\n信息截止：2026-08-17\n" + "未来事实" * 100)
    safe.write_text("# 深研\n信息截止：2025-10-30\n" + "旧假设" * 100)
    payload = {"history": [
        {"announcement_id": "future", "report_date": "2026-03-31", "output_path": str(future)},
        {"announcement_id": "safe", "report_date": "2025-10-30", "output_path": str(safe)},
    ]}
    assert _select_prior_summary(payload, "current", "2026-04-30", historical_cutoff_date="2026-04-29") == safe


def test_financial_context_uses_available_foreign_quote_in_historical_mode(tmp_path, monkeypatch):
    from services.research import financial_report_context as context
    from services.research import stock_analysis
    from services.backtest import prices

    calls = []
    monkeypatch.setattr(prices, "quote_date_as_of", lambda symbol, day: "2026-06-30")
    monkeypatch.setattr(stock_analysis, "analyze_stock_dynamics_and_valuation",
        lambda symbol, day: calls.append((symbol, day)) or {})
    def forbidden_live_package(*_):
        raise AssertionError("historical mode must not reuse an unverified live research package")

    monkeypatch.setattr(context, "find_historical_research_package", forbidden_live_package)
    monkeypatch.setattr(context, "_render_hk_pre_announcement_consensus", lambda *args, **kwargs: ("", None))
    monkeypatch.setattr(context, "_render_pre_announcement_price_context", lambda *args: ("", None))
    result = context.build_financial_report_context(
        symbol="TSM.US", stock_name="台积电", industry_name="晶圆代工", analysis_date=D,
        announcement_date="2026-05-15", current_announcement_id="q1", workdir=tmp_path,
        stock_root=tmp_path, summary_index_path=tmp_path / "summary_index.json", historical_mode=True,
    )
    assert calls == [("TSM.US", "2026-06-30")]
    assert result.analysis_date == D


@pytest.mark.parametrize("form,filing_day,release_day,period", [
    ("6-K", "2026-05-15", "2026-04-16", "2026-03-31"),
    ("10-Q", "2026-06-09", "2026-06-03", "2026-05-03"),
])
def test_expectations_use_earnings_release_before_full_sec_filing(tmp_path, monkeypatch, form, filing_day, release_day, period):
    from types import SimpleNamespace
    from services.news import disclosures_builder
    from services.research.financial_report_context import resolve_expectation_announcement_date

    (tmp_path / "disclosures").mkdir()
    (tmp_path / "disclosures/index.json").write_text("{}")
    current = SimpleNamespace(title=f"财报（{form}）（报告期 {period}）", date=filing_day,
        category=form, is_financial_report=True)
    release = SimpleNamespace(title=f"临时公告（6-K）（报告期 {period}）" if form == "6-K" else "重大事项公告（8-K）（事项 2.02,9.01）",
        date=release_day, category="6-K" if form == "6-K" else "8-K", is_financial_report=form == "6-K")
    monthly = SimpleNamespace(title=f"临时公告（6-K）（报告期 {period}）", date="2026-04-10",
        category="6-K", is_financial_report=False)
    monkeypatch.setattr(disclosures_builder, "load_index", lambda _: {"current": current, "release": release, "monthly": monthly})
    assert resolve_expectation_announcement_date("TSM.US", tmp_path, "current", filing_day) == release_day


def test_us_snapshot_uses_completed_session_not_same_day_close(monkeypatch):
    from services.pipeline.steps import build_agent_input
    calls = []

    def snapshot(symbols, day, **kwargs):
        calls.append((symbols, day))
        return {"stocks": {symbol: {"latest_price": 10} for symbol in symbols}}

    monkeypatch.setattr(build_agent_input, "build_basic_snapshot", snapshot)
    payload = build_agent_input.build_snapshot_payload(D, ["600150.SH", "TSM.US"], backtest_read_only=True)
    assert calls == [(["600150.SH"], D), (["TSM.US"], "2026-06-30")]
    assert payload["stocks"]["TSM.US"]["quote_date"] == "2026-06-30"


def test_partial_archive_is_not_reported_as_completed_or_reexecuted(tmp_path):
    from services.backtest.coverage import inspect_backtest_progress
    ex = experiment(tmp_path)
    write_prices(ex, "600150.SH", [{"日期": E, "开盘": 10, "收盘": 10}])
    execute(ex, [decision_entry("600150.SH", "BUY", 100)])
    summary = ex.context.skill_runs_root / D / "fixed_tracked/07_daily_summary.json"
    summary.unlink()
    assert inspect_backtest_progress(ex)["completed_execution_count"] == 0
    with pytest.raises(RuntimeError, match="禁止再次成交"):
        simulate_post_trade(ex, decision_date=D, execution_date=E)


def test_empty_research_directory_is_not_ready(tmp_path):
    from services.backtest.day_inputs import missing_day_inputs, REQUIRED_INPUTS
    for name in REQUIRED_INPUTS:
        (tmp_path / name).write_text("{}")
    (tmp_path / "04_stock_research").mkdir()
    missing = missing_day_inputs(tmp_path, ["600150.SH"])
    assert "04_stock_research/600150.SH" in missing
    assert "02_basic_snapshot_payload.json/600150.SH" in missing


def test_prepare_cannot_skip_an_unfinished_decision_day(tmp_path):
    from services.backtest.day_inputs import prepare_backtest_day
    ex = create_backtest_experiment(start_date="2026-06-30", end_date=E,
        experiment_id="sequential", source_data_root=tmp_path / "source",
        backtests_root=tmp_path / "backtests")
    with pytest.raises(ValueError, match="禁止跳到"):
        prepare_backtest_day(ex, D)


def test_macro_reuse_with_only_future_cache_requires_research(tmp_path):
    from services.backtest.macro import inspect_experiment_macro
    ex = experiment(tmp_path)
    folder = ex.context.source_data_root / "macro_economy"
    folder.mkdir(parents=True)
    (folder / "20260702.md").write_text("未来宏观")
    status, content = inspect_experiment_macro(ex, D)
    assert status["status"] == "needs_macro_research"
    assert content is None


def test_missing_open_price_blocks_buy_batch_without_substitution(tmp_path):
    ex = experiment(tmp_path, cash=5000)
    write_prices(ex, "600150.SH", [{"日期": E, "开盘": 10, "收盘": 10}])
    write_prices(ex, "600406.SH", [{"日期": D, "开盘": 1, "收盘": 1}])
    result = execute(ex, [decision_entry("600150.SH", "BUY", 100), decision_entry("600406.SH", "BUY", 100)])
    assert all(order["status"] == "pending" for order in result["orders"])
    assert result["actions"] == []
    assert result["ending_positions"]["CASH"] == 5000


def test_us_holiday_cannot_fill_even_if_bad_cache_contains_a_price(tmp_path):
    from services.backtest.execution import _execution_price
    ex = experiment(tmp_path)
    write_prices(ex, "TSM.US", [{"日期": "2026-07-03", "开盘": 100, "收盘": 100}])
    assert _execution_price(ex, "TSM.US", "2026-07-03") is None


def test_hk_holiday_uses_last_actual_close_without_reporting_a_missing_day(tmp_path):
    from services.backtest.shared_inputs import inspect_shared_inputs
    from commons import get_stock_data_dir
    ex = experiment(tmp_path)
    write_prices(ex, "01810.HK", [{"日期": "2026-06-30", "开盘": 30, "收盘": 30}])
    root = get_stock_data_dir(parse_symbol("01810.HK"), base_dir=ex.context.source_data_root)
    (root / "disclosures").mkdir()
    (root / "disclosures/index.json").write_text("{}")
    assert quote_date_as_of("01810.HK", D) == "2026-06-30"
    assert inspect_shared_inputs(ex, D, ["01810.HK"])["status"] == "ready"
