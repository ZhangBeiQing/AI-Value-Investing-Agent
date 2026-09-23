"""Compact projection of ``06_hot_news_state.json`` for downstream AI prompts.

The full hot-news state is a theme-level research memory that grows daily
(``history_anchor`` / ``scenarios`` / ``key_risks`` ...). Feeding it verbatim to
every downstream agent is token-heavy, so this module projects it into a small
digest that keeps only the fields needed for orientation:

    theme_name / strength / current_state / why_it_matters / key_risks /
    next_day_watchlist / outside_universe_names_to_check

The full ``06_hot_news_state.json`` is left untouched as the archival record.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from core.logging import get_logger

from .paths import SelectionSystemPaths
from .store import load_json_file, save_json_file


LOGGER = get_logger("HotNewsDigest")

MAX_CURRENT_STATE_CHARS = 240
MAX_WHY_IT_MATTERS_CHARS = 120
MAX_RISKS = 3


def _one_line(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").replace("\n", " ").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _string_list(value: Any, *, limit: int | None = None) -> list[str]:
    if not isinstance(value, list):
        return []
    items = [str(item).strip() for item in value if str(item or "").strip()]
    return items[:limit] if limit is not None else items


def _project_risks(value: Any) -> list[str]:
    risks: list[str] = []
    if not isinstance(value, list):
        return risks
    for item in value:
        if isinstance(item, Mapping):
            risk = _one_line(item.get("risk"), 120)
            band = _one_line(item.get("probability_band"), 24)
            if risk:
                risks.append(f"{risk}（{band}）" if band else risk)
        elif str(item or "").strip():
            risks.append(_one_line(item, 120))
    return risks[:MAX_RISKS]


def _project_active_theme(theme: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "theme_name": _one_line(theme.get("theme_name"), 80),
        "strength": _one_line(theme.get("strength"), 24),
        "current_state": _one_line(theme.get("current_state"), MAX_CURRENT_STATE_CHARS),
        "why_it_matters": _one_line(theme.get("why_it_matters"), MAX_WHY_IT_MATTERS_CHARS),
        "key_risks": _project_risks(theme.get("key_risks")),
        "next_day_watchlist": _string_list(theme.get("next_day_watchlist")),
        "outside_universe_names_to_check": _string_list(
            theme.get("outside_universe_names_to_check")
        ),
    }


def _project_brief_theme(theme: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "theme_name": _one_line(theme.get("theme_name"), 80),
        "current_state": _one_line(theme.get("current_state"), MAX_CURRENT_STATE_CHARS),
    }


def build_hot_news_digest(state: Mapping[str, Any]) -> dict[str, Any]:
    """Project a full hot-news state into a compact digest payload."""

    if not isinstance(state, Mapping):
        state = {}

    def _themes(key: str, projector) -> list[dict[str, Any]]:
        raw = state.get(key)
        if not isinstance(raw, list):
            return []
        return [projector(item) for item in raw if isinstance(item, Mapping)]

    return {
        "run_date": str(state.get("run_date") or ""),
        "market_regime_bridge": _one_line(state.get("market_regime_bridge"), 200),
        "active_themes": _themes("active_themes", _project_active_theme),
        "cooling_themes": _themes("cooling_themes", _project_brief_theme),
        "new_themes": _themes("new_themes", _project_brief_theme),
        "universe_expansion_hints": _string_list(state.get("universe_expansion_hints")),
    }


def _load_state(run_date: str, *, base_dir: str | Path) -> Mapping[str, Any]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    payload = load_json_file(paths.run_hot_news_state_path(run_date), default={})
    return payload if isinstance(payload, Mapping) else {}


def load_or_build_hot_news_digest(
    run_date: str,
    *,
    base_dir: str | Path = "data",
) -> dict[str, Any]:
    """Prefer the materialized digest file, fall back to projecting the state."""

    paths = SelectionSystemPaths.from_base_dir(base_dir)
    digest_path = paths.run_hot_news_digest_path(run_date)
    if digest_path.exists():
        payload = load_json_file(digest_path, default={})
        if isinstance(payload, Mapping) and payload.get("active_themes") is not None:
            return dict(payload)
    return build_hot_news_digest(_load_state(run_date, base_dir=base_dir))


def write_hot_news_digest(
    run_date: str,
    *,
    base_dir: str | Path = "data",
) -> Path | None:
    """Materialize ``06_hot_news_digest.json`` from the full state if it exists."""

    state = _load_state(run_date, base_dir=base_dir)
    if not state:
        LOGGER.warning("未找到 06_hot_news_state.json，跳过 digest 生成: %s", run_date)
        return None
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    digest = build_hot_news_digest(state)
    target = paths.run_hot_news_digest_path(run_date)
    save_json_file(target, digest)
    LOGGER.info("hot news digest 已写入: %s", target)
    return target


def render_hot_news_digest_markdown(digest: Mapping[str, Any]) -> str:
    """Render the digest as compact markdown for prompt injection."""

    lines: list[str] = []
    bridge = _one_line(digest.get("market_regime_bridge"), 200)
    if bridge:
        lines.append(f"- market_regime_bridge: {bridge}")

    active = digest.get("active_themes") or []
    lines.append(f"- active_theme_count: {len(active)}")
    for theme in active:
        if not isinstance(theme, Mapping):
            continue
        name = _one_line(theme.get("theme_name"), 80)
        strength = _one_line(theme.get("strength"), 24)
        header = f"### {name}" + (f"（{strength}）" if strength else "")
        lines.append(header)
        for label, key in (
            ("当前状态", "current_state"),
            ("为什么重要", "why_it_matters"),
        ):
            value = _one_line(theme.get(key), MAX_CURRENT_STATE_CHARS)
            if value:
                lines.append(f"- {label}：{value}")
        risks = _string_list(theme.get("key_risks"))
        if risks:
            lines.append("- 关键风险：" + "；".join(risks))
        watchlist = _string_list(theme.get("next_day_watchlist"))
        if watchlist:
            lines.append("- 明日跟踪：" + "；".join(watchlist))
        outside = _string_list(theme.get("outside_universe_names_to_check"))
        if outside:
            lines.append("- 宇宙外候选：" + "；".join(outside))

    for key, title in (("cooling_themes", "降温主题"), ("new_themes", "新主题")):
        items = digest.get(key) or []
        if items:
            names = "；".join(
                _one_line(item.get("theme_name"), 80)
                for item in items
                if isinstance(item, Mapping)
            )
            if names:
                lines.append(f"- {title}：{names}")

    hints = _string_list(digest.get("universe_expansion_hints"))
    if hints:
        lines.append("- 宇宙扩充提示：" + "；".join(hints))
    return "\n".join(lines)


__all__ = [
    "build_hot_news_digest",
    "load_or_build_hot_news_digest",
    "render_hot_news_digest_markdown",
    "write_hot_news_digest",
]
