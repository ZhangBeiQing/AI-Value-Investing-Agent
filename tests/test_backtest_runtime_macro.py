import pytest
from types import SimpleNamespace

from services.backtest.experiment import create_backtest_experiment, load_backtest_experiment
from services.backtest.macro import REQUIRED_SECTIONS, inspect_experiment_macro
from services.backtest import financials
from services.pipeline.steps import build_global_context as global_context


def _experiment(tmp_path, **kwargs):
    return create_backtest_experiment(
        start_date="2026-07-01", end_date="2026-07-02",
        source_data_root=tmp_path / "source",
        backtests_root=tmp_path / "backtests", experiment_id="case", **kwargs,
    )


def test_experiment_records_model_metadata_and_reload_preserves_it(tmp_path):
    supplied = {"model": "test-model", "reasoning_effort": "medium", "runtime": "codex"}
    experiment = _experiment(tmp_path, model_config=supplied)
    reloaded = load_backtest_experiment("case", backtests_root=tmp_path / "backtests")
    assert reloaded.payload["model_config"]["model"] == "test-model"
    assert reloaded.payload["model_config"]["reasoning_effort"] == "medium"
    assert reloaded.payload["model_config"]["role_model_policy"] == "inherit_current"
    assert "role_model_policy" not in supplied
    assert experiment.payload["macro_summary_policy"] == "reuse"


@pytest.mark.parametrize("problem", ["missing", "too_long", "wrong_date", "missing_sections"])
def test_macro_regeneration_rejects_invalid_draft(tmp_path, problem):
    experiment = _experiment(tmp_path, macro_summary_policy="regenerate")
    target = experiment.root / "macro_economy/20260701.md"
    target.parent.mkdir(exist_ok=True)
    text = "# 2026-07-01\n" + "\n".join(f"## {section}\n历史信息。" for section in REQUIRED_SECTIONS)
    if problem == "too_long":
        text += "长" * 3001
    elif problem == "wrong_date":
        text = text.replace("2026-07-01", "2026-07-02")
    elif problem == "missing_sections":
        text = "# 2026-07-01\n无章节。"
    if problem != "missing":
        target.write_text(text)
    result, override = inspect_experiment_macro(experiment, "2026-07-01")
    assert result["status"] == "needs_macro_research"
    assert override is None


def test_isolated_macro_overrides_shared_macro_without_reading_it(tmp_path, monkeypatch):
    experiment = _experiment(tmp_path, macro_summary_policy="regenerate")
    target = experiment.root / "macro_economy/20260701.md"
    target.parent.mkdir(exist_ok=True)
    text = "# 2026-07-01\n" + "\n".join(f"## {section}\n历史信息。" for section in REQUIRED_SECTIONS)
    target.write_text(text)
    result, override = inspect_experiment_macro(experiment, "2026-07-01")
    assert result["status"] == "ready"

    def fail_shared_read(**kwargs):
        pytest.fail("隔离重写模式不应读取共享宏观稿")

    monkeypatch.setattr(global_context, "get_macro_summary", fail_shared_read)
    monkeypatch.setattr(global_context, "_build_read_only_index_payload", lambda *_: {})
    output = global_context.write_global_context(
        "2026-07-01", tmp_path / "output", backtest_read_only=True,
        macro_summary_override=override,
    )
    assert text in output.read_text()


def test_macro_reuse_reads_only_historical_cache(tmp_path):
    experiment = _experiment(tmp_path)
    folder = experiment.context.source_data_root / "macro_economy"
    folder.mkdir(parents=True)
    (folder / "20260630.md").write_text("历史宏观")
    (folder / "20260702.md").write_text("未来宏观")
    result, override = inspect_experiment_macro(experiment, "2026-07-01")
    assert result["status"] == "ready"
    assert override == "历史宏观"


@pytest.mark.parametrize("brief_missing", [True, False])
def test_backtest_financial_gate_requires_daily_brief_for_completed_report(
    tmp_path, monkeypatch, brief_missing,
):
    experiment = _experiment(tmp_path)
    report = tmp_path / "20260430.md"
    report.write_text("已有完整深研")
    bundle = SimpleNamespace(
        symbol="600150.SH", stock_name="中国船舶",
        latest_report=SimpleNamespace(date="2026-04-30", announcement_id="report-1"),
        output_path=report, summary_index_path=tmp_path / "index.json",
        skip_reason="already_summarized_latest_report",
    )
    monkeypatch.setattr(financials, "build_stock_report_bundles", lambda *a, **kw: [bundle])
    monkeypatch.setattr(financials, "get_financial_report_summary", lambda *a, **kw: {"report_path": str(report)})
    task = {"symbol": "600150.SH", "full_report_path": str(report)}
    monkeypatch.setattr(financials, "daily_brief_task", lambda *a, **kw: task if brief_missing else None)
    result = financials.inspect_backtest_financial_research(
        experiment, "2026-07-01", ["600150.SH"],
        backtest_context_path=tmp_path / "00_backtest_context.md",
    )
    assert result["status"] == ("needs_financial_research" if brief_missing else "ready")
    assert result["required_items"] == []
    assert result["daily_brief_items"] == ([task] if brief_missing else [])
    assert "--require-daily-brief" in result["registration_command_template"]
