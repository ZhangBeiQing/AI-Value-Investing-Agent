"""回归测试：渐进式新闻总结与板块热度层已按 feature_flags 暂停。

2026-11-05 用户因 token 成本要求暂停这两层。这里锁住四件事：
1. 一键刷新的编排层不再执行 run-news / build-board-heat-state；
2. 后续 skill 清单不再提示 gradual-hot-news-summary；
3. fixed_tracked 主 Agent 输入不再列出 06/05 文件；
4. 板块热度构建函数被显式调用时也不再打 LLM 请求。
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from configs import feature_flags
from services.data_refresh import refresh_orchestrator
from services.pipeline.steps import build_agent_input
from services.selection_system import board_heat, shared_context


@pytest.fixture()
def paused_layers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(feature_flags, "ENABLE_GRADUAL_NEWS_SUMMARY", False)
    monkeypatch.setattr(feature_flags, "ENABLE_BOARD_HEAT_LAYER", False)


def test_orchestrator_skips_news_and_board_heat_when_paused(
    monkeypatch: pytest.MonkeyPatch,
    paused_layers: None,
) -> None:
    executed: list[list[str]] = []

    class _Result:
        status = "success"
        duration_sec = 0.0
        message = ""

    def fake_run_step(name: str, command: list[str]) -> _Result:
        executed.append([name, *command])
        return _Result()

    monkeypatch.setattr(refresh_orchestrator, "_run_step", fake_run_step)
    monkeypatch.setattr(
        refresh_orchestrator,
        "_clear_research_artifact_cache",
        lambda run_date: _Result(),
    )

    refresh_orchestrator.run_refresh_pipeline("2026-11-05", base_dir="data")

    names = [item[0] for item in executed]
    assert "selection.run-news" not in names
    assert "selection.build-board-heat-state" not in names
    assert "manage_daily_data" in names


def test_followup_checklist_omits_gradual_news_when_paused(paused_layers: None) -> None:
    checklist = refresh_orchestrator.format_followup_checklist("2026-11-05")

    assert "/gradual-hot-news-summary" not in checklist
    assert "已暂停：渐进式新闻总结" in checklist
    assert "/daily-macro-summary" in checklist


def test_fixed_main_user_query_omits_news_and_board_files_when_paused(
    paused_layers: None,
) -> None:
    query = build_agent_input.build_fixed_main_user_query([], "2026-11-05")

    assert "06_hot_news_state.json" not in query
    assert "05_board_heat_digest.json" not in query
    assert "01_global_context.md" in query


def test_board_heat_state_skips_llm_when_paused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    paused_layers: None,
) -> None:
    def _fail_quant_snapshot(*_args, **_kwargs):  # pragma: no cover - 不应被调用
        raise AssertionError("暂停时不得构建板块量化快照/调用 LLM")

    monkeypatch.setattr(board_heat, "build_board_quant_snapshot", _fail_quant_snapshot)

    outputs = board_heat.build_board_heat_state("2026-11-05", base_dir=tmp_path)

    assert outputs == {}


def test_shared_context_marks_news_and_board_heat_paused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    paused_layers: None,
) -> None:
    run_dir = tmp_path / "selection_runs" / "2026-09-23"
    run_dir.mkdir(parents=True)
    (run_dir / "06_hot_news_state.json").write_text(
        json.dumps({"run_date": "2026-09-23", "active_themes": [{"theme_name": "AI算力"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        shared_context,
        "load_master_universe",
        lambda _paths: SimpleNamespace(universe_name="test", stocks=[], updated_at="2026-09-23"),
    )

    text = shared_context.render_shared_selection_context(
        "2026-09-23",
        base_dir=tmp_path,
        announcements_payload_override={},
    )

    assert "已暂停：渐进式新闻总结" in text
    assert "已暂停：板块热度层" in text
    assert "05_board_heat_state.json" not in text
    assert "### AI算力" not in text
