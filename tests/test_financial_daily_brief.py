import hashlib
import json

import pytest

from services.research.financial_daily_brief import (
    BRIEF_SECTIONS, brief_path, daily_financial_content, load_daily_brief,
    daily_brief_task,
)


def _write_brief(tmp_path):
    report = tmp_path / "20261015.md"
    report.write_text("完整财报独有正文" * 200, encoding="utf-8")
    payload = {
        "schema_version": 1, "symbol": "TSM.US",
        "report_filename": report.name,
        "source_sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
        "knowledge_cutoff": "2026-11-09",
        "content": "\n\n".join(
            f"## {heading}\n" + "测试用财务事实与已知不确定性，具体计算定位完整报告§8。" * 5
            for heading in BRIEF_SECTIONS
        ),
    }
    brief_path(report).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return report, payload


def test_daily_brief_replaces_full_content_but_keeps_original_path(tmp_path):
    report, payload = _write_brief(tmp_path)
    text, metadata = daily_financial_content(
        report, report.read_text(), symbol="TSM.US", as_of_date="2026-11-09",
    )
    assert payload["content"] in text
    assert "完整财报独有正文" not in text
    assert str(report.resolve()) in text
    assert metadata["mode"] == "daily_brief"


def test_existing_report_only_queues_brief_until_valid_brief_exists(tmp_path):
    report, _ = _write_brief(tmp_path)
    assert daily_brief_task(report, symbol="TSM.US", as_of_date="2026-11-09") is None
    brief_path(report).unlink()
    task = daily_brief_task(report, symbol="TSM.US", as_of_date="2026-11-09")
    assert task["full_report_path"] == str(report)
    assert task["brief_status"] == "missing"
    assert daily_brief_task(tmp_path / "不存在.md", symbol="TSM.US", as_of_date="2026-11-09") is None


@pytest.mark.parametrize("failure", ["missing", "source_changed", "future", "symbol", "oversize", "missing_section", "url", "not_object"])
def test_invalid_brief_keeps_full_report_with_explicit_status(tmp_path, failure):
    report, payload = _write_brief(tmp_path)
    if failure == "missing":
        brief_path(report).unlink()
    elif failure == "source_changed":
        report.write_text("修订后的完整财报", encoding="utf-8")
    else:
        if failure == "future":
            payload["knowledge_cutoff"] = "2026-11-10"
        elif failure == "symbol":
            payload["symbol"] = "NVDA.US"
        elif failure == "oversize":
            payload["content"] += "数据" * 4000
        elif failure == "missing_section":
            payload["content"] = payload["content"].replace("## 风险与反面证据", "## 其他")
        elif failure == "url":
            payload["content"] += "https://example.com"
        elif failure == "not_object":
            payload = []
        brief_path(report).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    text, metadata = daily_financial_content(
        report, report.read_text(), symbol="TSM.US", as_of_date="2026-11-09",
    )
    assert report.read_text() in text
    assert metadata["mode"] == "full_report_fallback"
    assert metadata["brief_status"] != "valid"


def test_brief_does_not_replace_daily_consensus_or_forecast(tmp_path, monkeypatch):
    from services.research import financial_report as service

    report, payload = _write_brief(tmp_path)
    monkeypatch.setattr(service, "_list_report_files", lambda *_: [report])
    monkeypatch.setattr(service, "_format_cn_profit_forecast_sections", lambda *_: "每日最新一致预期")
    monkeypatch.setattr(service, "_load_latest_forecast_markdown", lambda *_: ("每日最新预测", None))
    result = service.get_financial_report_summary(
        "TSM.US", "2026-11-09", prefer_daily_brief=True, include_price_drift=False,
    )
    assert result["daily_input"]["mode"] == "daily_brief"
    assert payload["content"] in result["content"]
    assert "每日最新一致预期" in result["content"]
    assert "每日最新预测" in result["content"]
    # Non-daily callers continue receiving the full original report.
    full = service.get_financial_report_summary("TSM.US", "2026-11-09", include_price_drift=False)
    assert "完整财报独有正文" in full["content"]
