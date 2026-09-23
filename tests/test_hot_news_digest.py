from __future__ import annotations

import json
from types import SimpleNamespace
from pathlib import Path

from services.selection_system import shared_context
from services.selection_system.hot_news_digest import (
    build_hot_news_digest,
    render_hot_news_digest_markdown,
    write_hot_news_digest,
)


def _state() -> dict:
    return {
        "run_date": "2026-09-23",
        "market_regime_bridge": "滞胀交易回归，A股缩量分化。",
        "active_themes": [
            {
                "theme_id": "theme_x",
                "theme_name": "AI算力",
                "status": "active",
                "strength": "strengthening",
                "history_anchor": "很长很长的历史锚点，不应该进入 digest。" * 20,
                "today_update": "今天新增事件。" * 20,
                "current_state": "结构性切换，资金切向上游材料。" * 20,
                "scenarios": [{"scenario": "延续", "market_impact": "xx"}],
                "key_risks": [
                    {"risk": "海外费半回落", "probability_band": "medium_to_high"},
                    {"risk": "成交萎缩", "probability_band": "medium"},
                ],
                "why_it_matters": "成长资金核心锚。",
                "next_day_watchlist": ["跟踪元件成交额"],
                "outside_universe_names_to_check": ["某材料股"],
            }
        ],
        "cooling_themes": [{"theme_name": "旧主题", "current_state": "退潮"}],
        "new_themes": [{"theme_name": "新主题", "current_state": "刚启动"}],
        "universe_expansion_hints": ["补充上游材料标的"],
    }


def test_digest_drops_heavy_fields_and_truncates() -> None:
    digest = build_hot_news_digest(_state())
    serialized = json.dumps(digest, ensure_ascii=False)

    assert "history_anchor" not in serialized
    assert "scenarios" not in serialized
    assert "today_update" not in serialized

    theme = digest["active_themes"][0]
    assert theme["theme_name"] == "AI算力"
    assert theme["key_risks"] == ["海外费半回落（medium_to_high）", "成交萎缩（medium）"]
    assert len(theme["current_state"]) <= 240
    assert theme["current_state"].endswith("…")


def test_digest_markdown_is_compact() -> None:
    markdown = render_hot_news_digest_markdown(build_hot_news_digest(_state()))

    assert "### AI算力" in markdown
    assert "当前状态：" in markdown
    assert "关键风险：" in markdown
    assert "明日跟踪：" in markdown
    assert "history_anchor" not in markdown
    assert "scenarios" not in markdown


def test_write_hot_news_digest_materializes_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "selection_runs" / "2026-09-23"
    run_dir.mkdir(parents=True)
    (run_dir / "06_hot_news_state.json").write_text(
        json.dumps(_state(), ensure_ascii=False), encoding="utf-8"
    )

    target = write_hot_news_digest("2026-09-23", base_dir=tmp_path)

    assert target == run_dir / "06_hot_news_digest.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["active_themes"][0]["theme_name"] == "AI算力"


def test_shared_context_injects_digest_not_full_state(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "selection_runs" / "2026-09-23"
    run_dir.mkdir(parents=True)
    (run_dir / "06_hot_news_state.json").write_text(
        json.dumps(_state(), ensure_ascii=False), encoding="utf-8"
    )
    monkeypatch.setattr(
        shared_context,
        "load_master_universe",
        lambda _paths: SimpleNamespace(
            universe_name="test", stocks=[], updated_at="2026-09-23"
        ),
    )

    text = shared_context.render_shared_selection_context(
        "2026-09-23",
        base_dir=tmp_path,
        announcements_payload_override={},
    )

    assert "## 3. 主题主线摘要" in text
    assert "### AI算力" in text
    assert "history_anchor" not in text
    assert "scenarios" not in text
