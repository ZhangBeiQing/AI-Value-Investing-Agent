from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from services.research import financial_report


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GATE_SCRIPT = PROJECT_ROOT / "scripts" / "prepare_universe_scoring_inputs.py"
RUN_DATE = "2026-08-21"


def _report_file(tmp_path: Path, filename: str) -> Path:
    path = tmp_path / filename
    path.write_text("财报正文", encoding="utf-8")
    return path


def test_has_usable_report_respects_release_cutoff(tmp_path, monkeypatch) -> None:
    # Arrange：仅有一份 2026-04-28 的财报
    files = [_report_file(tmp_path, "公司_000063.SZ_20260428.md")]
    monkeypatch.setattr(financial_report, "_list_report_files", lambda info: files)

    # Act / Assert
    assert financial_report.has_usable_financial_report("000063.SZ", RUN_DATE, slack_days=1) is True
    assert financial_report.has_usable_financial_report("000063.SZ", "2026-01-01", slack_days=0) is False


def test_has_usable_report_missing_dir_and_etf(monkeypatch) -> None:
    monkeypatch.setattr(financial_report, "_list_report_files", lambda info: [])

    # 普通股票无财报 → 视为缺失
    assert financial_report.has_usable_financial_report("000063.SZ", RUN_DATE) is False
    # ETF/基金没有季度财报 → 不视为缺失
    assert financial_report.has_usable_financial_report("518800.SH", RUN_DATE) is True


def test_find_missing_preserves_order_and_dedupes(tmp_path, monkeypatch) -> None:
    files = [_report_file(tmp_path, "公司_000063.SZ_20260428.md")]

    def _list(info):
        return files if info.symbol == "000063.SZ" else []

    monkeypatch.setattr(financial_report, "_list_report_files", _list)

    missing = financial_report.find_symbols_missing_financial_reports(
        ["300502.SZ", "000063.SZ", "300502.SZ"], RUN_DATE, slack_days=1
    )

    assert missing == ["300502.SZ"]


def test_prepare_gate_hard_fails_when_reports_missing(tmp_path: Path) -> None:
    # Arrange：股票数据目录为空，目标股票没有财报
    base_dir = tmp_path / "base"
    env = {**os.environ, "STOCK_DATA_DIR": str(tmp_path / "stock_info")}

    # Act
    result = subprocess.run(
        [
            sys.executable,
            str(GATE_SCRIPT),
            "--date",
            RUN_DATE,
            "--symbols",
            "000063.SZ",
            "--base-dir",
            str(base_dir),
            "--no-prepare-missing-reports",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env=env,
    )

    # Assert：硬失败并写出缺失清单
    assert result.returncode == 2
    report = base_dir / "skill_runs" / RUN_DATE / "universe" / "_missing_financial_reports.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["missing_symbols"] == ["000063.SZ"]
    assert payload["date"] == RUN_DATE
