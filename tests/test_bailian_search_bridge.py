"""Focused checks for the Bailian WebSearch stdio bridge used by the dashboard chat."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.recommendation_dashboard import bailian_search_mcp as bridge


class _TextBlock:
    def __init__(self, text: str) -> None:
        self.text = text


def _write_global_config(path: Path, url: str, headers: dict[str, str]) -> None:
    path.write_text(
        json.dumps({"mcp": {"WebSearch": {"url": url, "headers": headers}}}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_remote_config_reads_url_and_headers_from_opencode_config(tmp_path: Path, monkeypatch: Any) -> None:
    # Arrange
    config = tmp_path / "opencode.json"
    _write_global_config(config, "https://example.invalid/mcp", {"Authorization": "Bearer test"})
    monkeypatch.setattr(bridge, "GLOBAL_CONFIG", config)

    # Act
    url, headers = bridge._remote_config()

    # Assert
    assert url == "https://example.invalid/mcp"
    assert headers == {"Authorization": "Bearer test"}


def test_remote_config_without_mcp_entry_raises(tmp_path: Path, monkeypatch: Any) -> None:
    # Arrange
    config = tmp_path / "opencode.json"
    config.write_text(json.dumps({"mcp": {}}), encoding="utf-8")
    monkeypatch.setattr(bridge, "GLOBAL_CONFIG", config)

    # Act / Assert
    try:
        bridge._remote_config()
    except RuntimeError as error:
        assert "百炼" in str(error)
    else:  # pragma: no cover - failure path guard
        raise AssertionError("缺少 mcp.WebSearch 时应抛 RuntimeError")


def test_content_text_joins_text_blocks() -> None:
    assert bridge._content_text([_TextBlock("a"), _TextBlock("b")]) == "a\nb"


def test_bailian_web_search_empty_query_raises() -> None:
    try:
        asyncio.run(bridge.bailian_web_search("   "))
    except ValueError as error:
        assert "不能为空" in str(error)
    else:  # pragma: no cover - failure path guard
        raise AssertionError("空查询应抛 ValueError")
