"""回归测试：每日数据准备范围收敛为 TRACKED_A_STOCKS 固定池。

2026-11-05 用户要求"只生成固定股池的数据"，避免每天刷整个 master_universe。
这里锁住三件事：
1. 一键刷新默认只刷固定池，且不再跑因子库/评分/量化初筛；
2. `run_daily_pipeline` 的 manifest 不再并入长期候选与量化初筛；
3. 把开关打开后，旧行为可以恢复。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from configs import feature_flags
from configs.stock_pool import FIXED_TRACKED_US_SYMBOLS, TRACKED_A_STOCKS
from services.data_refresh import refresh_orchestrator
from services.pipeline.daily_pipeline import build_run_manifest


class _StepResult:
    status = "success"
    duration_sec = 0.0
    message = ""


def test_refresh_only_covers_tracked_pool_and_skips_prefilter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(feature_flags, "ENABLE_FACTOR_PREFILTER_LAYER", False)
    commands: list[list[str]] = []

    def fake_run_step(name: str, command: list[str]) -> _StepResult:
        commands.append([name, *command])
        return _StepResult()

    monkeypatch.setattr(refresh_orchestrator, "_run_step", fake_run_step)
    monkeypatch.setattr(
        refresh_orchestrator,
        "_clear_research_artifact_cache",
        lambda run_date: _StepResult(),
    )

    refresh_orchestrator.run_refresh_pipeline(
        "2026-11-05",
        base_dir=str(tmp_path),
        generate_prefilter=True,
    )

    names = [item[0] for item in commands]
    assert "selection.build-factor-store" not in names
    assert "selection.build-factor-scores" not in names
    assert "selection.build-quant-prefilter" not in names

    manage_cmd = next(item for item in commands if item[0] == "manage_daily_data")
    symbols = manage_cmd[manage_cmd.index("--symbols") + 1 :]
    assert symbols == [entry.symbol for entry in TRACKED_A_STOCKS] + list(FIXED_TRACKED_US_SYMBOLS)


def test_manifest_defaults_to_tracked_pool_only(tmp_path: Path) -> None:
    selection = tmp_path / "selection_runs" / "2026-11-05"
    selection.mkdir(parents=True)
    override_dir = tmp_path / "agent_data" / "book-fixed_tracked" / "position"
    override_dir.mkdir(parents=True)
    (override_dir / "manual_position_override.json").write_text(
        json.dumps({"positions": {"公牛集团": {"shares": 300}}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (selection / "09_long_book_candidates.json").write_text(
        json.dumps({"symbols": ["000001.SZ"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (selection / "12_quant_prefilter_short.csv").write_text(
        "symbol,score\n000002.SZ,0.9\n", encoding="utf-8"
    )

    manifest = build_run_manifest("2026-11-05", base_dir=str(tmp_path))
    fixed = next(
        book for book in manifest["books"] if book["book_type"] == "fixed_tracked"
    )

    assert fixed["selection_universe_merged"] is False
    assert "000001.SZ" not in fixed["symbols"]
    assert "000002.SZ" not in fixed["symbols"]
    assert fixed["symbol_sources"]["long_book_candidates"] == []
    assert fixed["symbol_sources"]["short_quant_prefilter"] == []
    assert fixed["symbol_sources"]["manual_position_override"] == []
    assert fixed["symbols"] == [entry.symbol for entry in TRACKED_A_STOCKS] + list(FIXED_TRACKED_US_SYMBOLS)


def test_manifest_can_restore_selection_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(feature_flags, "ENABLE_SELECTION_UNIVERSE_MERGE", True)
    selection = tmp_path / "selection_runs" / "2026-11-05"
    selection.mkdir(parents=True)
    (selection / "12_quant_prefilter_short.csv").write_text(
        "symbol,score\n000002.SZ,0.9\n", encoding="utf-8"
    )

    manifest = build_run_manifest("2026-11-05", base_dir=str(tmp_path))
    fixed = next(
        book for book in manifest["books"] if book["book_type"] == "fixed_tracked"
    )

    assert fixed["selection_universe_merged"] is True
    assert "000002.SZ" in fixed["symbols"]
