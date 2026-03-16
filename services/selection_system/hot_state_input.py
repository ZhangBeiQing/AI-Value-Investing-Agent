"""Render compact markdown inputs for the future hot-news state skill."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from core.logging import get_logger

from .paths import SelectionSystemPaths
from .store import load_json_file, save_json_file


LOGGER = get_logger("SelectionHotStateInput")


def render_hot_state_input(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    news_limit: int = 40,
    board_limit: int = 12,
    universe_heat_limit: int = 30,
    outside_heat_limit: int = 15,
) -> Path:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    news_payload = load_json_file(paths.run_news_enriched_path(run_date), default={}) or {}
    board_payload = _load_with_fallback(
        primary_path=paths.run_board_signals_path(run_date),
        fallback_path=paths.board_signals_daily_path(run_date),
    )
    heat_payload = _load_with_fallback(
        primary_path=paths.run_stock_heat_path(run_date),
        fallback_path=paths.stock_heat_daily_path(run_date),
    )

    markdown = _build_markdown(
        run_date,
        news_items=_limit_items(news_payload.get("items"), news_limit),
        board_items=_limit_items(board_payload.get("items"), board_limit),
        heat_items=heat_payload.get("items"),
        source_statuses=_collect_source_statuses(news_payload, board_payload, heat_payload),
        universe_heat_limit=universe_heat_limit,
        outside_heat_limit=outside_heat_limit,
    )
    output_path = paths.run_hot_state_input_path(run_date)
    output_path.write_text(markdown, encoding="utf-8")
    LOGGER.info("hot state markdown 已写入: %s", output_path)
    return output_path


def _build_markdown(
    run_date: str,
    *,
    news_items: List[Mapping[str, Any]],
    board_items: List[Mapping[str, Any]],
    heat_items: Any,
    source_statuses: List[Mapping[str, Any]],
    universe_heat_limit: int,
    outside_heat_limit: int,
) -> str:
    all_heat_items = heat_items if isinstance(heat_items, list) else []
    universe_hits = [item for item in all_heat_items if item.get("in_master_universe")][: max(universe_heat_limit, 0)]
    outside_hits = [item for item in all_heat_items if not item.get("in_master_universe")][: max(outside_heat_limit, 0)]

    lines = [
        f"# Hot State Input {run_date}",
        "",
        f"生成时间：{datetime.now().isoformat()}",
        "",
        "## 数据完整性",
    ]
    if source_statuses:
        for item in source_statuses:
            status = str(item.get("status") or "")
            source = str(item.get("source") or "")
            if status == "ok":
                suffix = f"rows={item.get('rows')}" if item.get("rows") is not None else "ok"
                lines.append(f"- {source}: {suffix}")
            else:
                lines.append(f"- {source}: error={_compact_error_text(item.get('error'))}")
    else:
        lines.append("- 无状态信息")

    lines.extend(
        [
            "",
            f"## 新闻正文输入（{len(news_items)}条）",
        ]
    )
    if not news_items:
        lines.append("- 无新闻数据")
    else:
        for item in news_items:
            lines.extend(_render_news_item(item))

    lines.extend(
        [
            "",
            f"## 板块异动输入（{len(board_items)}条）",
        ]
    )
    if not board_items:
        lines.append("- 无板块异动数据")
    else:
        for item in board_items:
            lines.append(_render_board_item(item))

    lines.extend(
        [
            "",
            f"## 宇宙内个股热度输入（{len(universe_hits)}条）",
        ]
    )
    if not universe_hits:
        lines.append("- 无宇宙内热度命中")
    else:
        for item in universe_hits:
            lines.append(_render_heat_item(item))

    lines.extend(
        [
            "",
            f"## 宇宙外高热个股参考（{len(outside_hits)}条）",
        ]
    )
    if not outside_hits:
        lines.append("- 无宇宙外高热个股")
    else:
        for item in outside_hits:
            lines.append(_render_heat_item(item))

    lines.append("")
    return "\n".join(lines)


def _render_news_item(item: Mapping[str, Any]) -> Iterable[str]:
    news_id = str(item.get("news_id") or "")
    published_at = str(item.get("published_at") or "")
    source = str(item.get("source") or "")
    title = str(item.get("title") or "").strip()
    content = str(item.get("content") or "").strip()
    yield f"### [{news_id}] {published_at} | {source} | {title}"
    yield content or "(无正文)"
    yield ""


def _render_board_item(item: Mapping[str, Any]) -> str:
    change_types = item.get("change_types")
    change_text = "、".join(
        f"{entry.get('type')}:{entry.get('count')}"
        if entry.get("count") is not None
        else str(entry.get("type") or "")
        for entry in change_types
    ) if isinstance(change_types, list) else ""
    return (
        f"- {item.get('board_name')} | 涨跌幅={item.get('change_pct')}% | 主力净流入={item.get('main_net_inflow_wan')}万"
        f" | 异动次数={item.get('change_count')} | 龙头={item.get('leading_stock_name')} {item.get('leading_stock_code')}"
        f" | 方向={item.get('leading_action')} | 异动类型={change_text}"
    )


def _render_heat_item(item: Mapping[str, Any]) -> str:
    parts = [
        f"{item.get('name')} {item.get('symbol')}",
        f"最新价={item.get('latest_price')}",
        f"信号源={','.join(item.get('source_hits') or [])}",
    ]
    if item.get("sector") or item.get("industry"):
        parts.append(f"行业={item.get('sector')}/{item.get('industry')}")
    if item.get("em_rank") is not None:
        parts.append(f"东财人气={item.get('em_rank')}")
    if item.get("em_change_pct") is not None:
        parts.append(f"东财涨跌幅={item.get('em_change_pct')}%")
    if item.get("xq_tweet_rank") is not None:
        parts.append(f"雪球讨论={item.get('xq_tweet_rank')}")
    if item.get("xq_follow_rank") is not None:
        parts.append(f"雪球关注={item.get('xq_follow_rank')}")
    if item.get("xq_deal_rank") is not None:
        parts.append(f"雪球交易={item.get('xq_deal_rank')}")
    return "- " + " | ".join(parts)


def _limit_items(value: Any, limit: int) -> List[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value[: max(limit, 0)] if isinstance(item, Mapping)]


def _collect_source_statuses(*payloads: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    collected: List[Mapping[str, Any]] = []
    for payload in payloads:
        for item in payload.get("source_status", []) if isinstance(payload, Mapping) else []:
            if isinstance(item, Mapping):
                collected.append(item)
    return collected


def _load_with_fallback(*, primary_path: Path, fallback_path: Path) -> Dict[str, Any]:
    payload = load_json_file(primary_path, default=None)
    if isinstance(payload, dict):
        return payload

    fallback = load_json_file(fallback_path, default={}) or {}
    if isinstance(fallback, dict) and fallback and not primary_path.exists():
        save_json_file(primary_path, fallback)
    return fallback if isinstance(fallback, dict) else {}


def _compact_error_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    if "NameResolutionError" in text:
        marker = "host='"
        if marker in text:
            host = text.split(marker, 1)[1].split("'", 1)[0]
            return f"dns_fail:{host}"
        return "dns_fail"
    if len(text) <= 120:
        return text
    return text[:117] + "..."
