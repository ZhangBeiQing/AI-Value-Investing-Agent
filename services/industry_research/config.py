"""Configuration loading and validation for industry research."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

from .paths import PROJECT_ROOT


DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "industry_research" / "theme_registry.yaml"


def load_industry_research_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    path = Path(config_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"行业研究配置必须是 mapping: {path}")

    config = dict(payload)
    radar = config.get("radar")
    themes = config.get("themes")
    states = config.get("states")
    if not isinstance(radar, Mapping):
        raise ValueError("行业研究配置缺少 radar mapping")
    if not isinstance(themes, list) or not themes:
        raise ValueError("行业研究配置必须包含非空 themes 列表")
    if not isinstance(states, list) or not states:
        raise ValueError("行业研究配置必须包含非空 states 列表")

    seen_ids: set[str] = set()
    normalized_themes: list[dict[str, Any]] = []
    for raw_theme in themes:
        if not isinstance(raw_theme, Mapping):
            raise ValueError("themes 中的每个主题必须是 mapping")
        theme = dict(raw_theme)
        theme_id = str(theme.get("theme_id") or "").strip()
        name = str(theme.get("name") or "").strip()
        aliases = [str(item).strip() for item in theme.get("aliases") or [] if str(item).strip()]
        if not theme_id or not name or not aliases:
            raise ValueError("每个主题必须包含 theme_id、name 和非空 aliases")
        if theme_id in seen_ids:
            raise ValueError(f"theme_id 重复: {theme_id}")
        seen_ids.add(theme_id)
        theme["theme_id"] = theme_id
        theme["name"] = name
        theme["aliases"] = aliases
        normalized_themes.append(theme)

    config["themes"] = normalized_themes
    config["radar"] = dict(radar)
    config["states"] = [str(item).strip() for item in states if str(item).strip()]
    config["config_path"] = str(path)
    return config


def get_theme(config: Mapping[str, Any], theme_id: str) -> dict[str, Any]:
    for theme in config.get("themes") or []:
        if isinstance(theme, Mapping) and theme.get("theme_id") == theme_id:
            return dict(theme)
    raise KeyError(f"未注册产业主题: {theme_id}")
