"""Build the monthly fundamental monitor for approved structural themes."""

from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from core.logging import get_logger
from services.selection_system.store import save_json_file
from shared_data_access.industry_financial_panel import (
    load_industry_financial_snapshot_cached,
)

from .config import DEFAULT_CONFIG_PATH, load_industry_research_config
from .paths import IndustryResearchPaths


LOGGER = get_logger("IndustryResearchRadar")


def build_monthly_industry_radar(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> dict[str, Path]:
    """Monitor structural space and leading fundamentals without market narratives."""

    run_day = _parse_iso_date(run_date)
    config = load_industry_research_config(config_path)
    paths = IndustryResearchPaths.from_base_dir(base_dir)
    paths.ensure_radar_dir(run_date)

    structural_pool, structural_pool_path = _load_latest_structural_pool(paths, run_date)
    financial_panel, financial_panel_path = load_industry_financial_snapshot_cached(
        run_date,
        base_dir=paths.base_dir,
        allow_previous=True,
    )
    raw_themes, pool_status = _resolve_monitor_themes(structural_pool, config["themes"])
    monitored_themes = [
        _build_theme_monitor(
            theme,
            run_day=run_day,
            paths=paths,
            radar_config=config["radar"],
            financial_panel=financial_panel,
            pool_status=pool_status,
        )
        for theme in raw_themes
    ]
    monitored_themes.sort(key=_theme_sort_key)

    payload = {
        "schema_version": 3,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "cadence": "monthly",
        "methodology": "structural_space_then_leading_fundamentals",
        "excluded_inputs": [
            "hot_news",
            "board_heat",
            "fund_flow",
            "stock_price_momentum",
        ],
        "decision_boundary": "fundamental_monitor_only_no_trade_side_effects",
        "config_path": config["config_path"],
        "source_status": [
            {
                "source": "human_approved_structural_pool",
                "path": str(structural_pool_path) if structural_pool_path else None,
                "status": "ok" if structural_pool_path else "missing",
                "required": True,
            },
            {
                "source": "quarterly_industry_financial_validation",
                "path": str(financial_panel_path) if financial_panel_path else None,
                "status": "ok" if financial_panel_path else "warning",
                "required": False,
            },
        ],
        "summary": {
            "pool_status": pool_status,
            "theme_count": len(monitored_themes),
            "human_approved_theme_count": sum(
                1 for item in monitored_themes if item["human_approved"]
            ),
            "fundamental_right_candidate_count": sum(
                1
                for item in monitored_themes
                if item["monitor_status"] == "fundamental_right_candidate"
            ),
            "needs_indicator_update_count": sum(
                1 for item in monitored_themes if item["monitor_status"] == "needs_indicator_update"
            ),
        },
        "themes": monitored_themes,
        "human_gate": {
            "required": True,
            "maximum_monthly_deep_research_themes": 1,
            "instruction": "领先指标满足只代表基本面右侧候选，必须人工确认后才能开展全产业链深研。",
        },
    }
    save_json_file(paths.radar_json_path(run_date), payload)
    save_json_file(paths.latest_radar_path, payload)
    paths.agent_review_path(run_date).write_text(
        _render_monthly_monitor(payload),
        encoding="utf-8",
    )
    LOGGER.info(
        "月度产业基本面监控已生成: run_date=%s themes=%d fundamental_right=%d output=%s",
        run_date,
        len(monitored_themes),
        payload["summary"]["fundamental_right_candidate_count"],
        paths.radar_json_path(run_date),
    )
    return {
        "industry_radar": paths.radar_json_path(run_date),
        "agent_review_input": paths.agent_review_path(run_date),
        "latest_radar": paths.latest_radar_path,
    }


def _build_theme_monitor(
    theme: Mapping[str, Any],
    *,
    run_day: date,
    paths: IndustryResearchPaths,
    radar_config: Mapping[str, Any],
    financial_panel: Mapping[str, Any],
    pool_status: str,
) -> dict[str, Any]:
    theme_id = str(theme.get("theme_id") or "").strip()
    definitions = [
        dict(item)
        for item in theme.get("leading_indicators") or []
        if isinstance(item, Mapping) and item.get("indicator_id")
    ]
    history_path = paths.theme_indicator_history_path(theme_id)
    history = _load_indicator_history(history_path, run_day)
    latest = _latest_indicator_observations(history)
    stale_days = int(radar_config.get("indicator_stale_days", 45))
    fresh_latest = {
        indicator_id: item
        for indicator_id, item in latest.items()
        if (run_day - _parse_iso_date(str(item["as_of_date"]))).days <= stale_days
    }
    improving = [item for item in fresh_latest.values() if item.get("signal") == "improving"]
    deteriorating = [item for item in fresh_latest.values() if item.get("signal") == "deteriorating"]
    improving_categories = {
        str(item.get("category") or "unknown")
        for item in improving
    }
    minimum_improving = int(radar_config.get("minimum_improving_indicators", 2))
    minimum_categories = int(radar_config.get("minimum_independent_indicator_categories", 2))
    maximum_deteriorating = int(radar_config.get("maximum_deteriorating_indicators", 1))
    leading_confirmation = (
        len(improving) >= minimum_improving
        and len(improving_categories) >= minimum_categories
        and len(deteriorating) <= maximum_deteriorating
    )

    structural_space = dict(theme.get("structural_space") or {})
    classification = str(theme.get("classification") or "unreviewed")
    human_approved = bool(theme.get("human_approved"))
    structural_passed = (
        classification == "structural_growth"
        and bool(structural_space.get("passed"))
    )
    if pool_status == "missing":
        monitor_status = "needs_structural_scan"
    elif not human_approved:
        monitor_status = "awaiting_human_approval"
    elif not structural_passed:
        monitor_status = "not_structural_candidate"
    elif not definitions or len(fresh_latest) < min(len(definitions), minimum_improving):
        monitor_status = "needs_indicator_update"
    elif leading_confirmation:
        monitor_status = "fundamental_right_candidate"
    elif deteriorating:
        monitor_status = "fundamentals_deteriorating"
    else:
        monitor_status = "watch"

    missing_indicators = [
        item["indicator_id"]
        for item in definitions
        if item["indicator_id"] not in fresh_latest
    ]
    financial_validation = _financial_validation_for_theme(theme, financial_panel)
    financial_validation_mode = str(
        theme.get("financial_validation_mode") or "sw_industry"
    )
    return {
        "theme_id": theme_id,
        "theme_name": theme.get("theme_name") or theme.get("name") or theme_id,
        "classification": classification,
        "human_approved": human_approved,
        "structural_space": structural_space,
        "monitor_status": monitor_status,
        "leading_confirmation": leading_confirmation,
        "leading_indicators": definitions,
        "latest_indicator_observations": list(fresh_latest.values()),
        "missing_or_stale_indicator_ids": missing_indicators,
        "signal_summary": {
            "fresh_indicator_count": len(fresh_latest),
            "improving_count": len(improving),
            "improving_categories": sorted(improving_categories),
            "deteriorating_count": len(deteriorating),
        },
        "quarterly_financial_validation": financial_validation,
        "financial_validation_mode": financial_validation_mode,
        "financial_role": (
            "disabled_until_theme_exposure_mapping_exists"
            if financial_validation_mode == "disabled_without_theme_exposure_mapping"
            else "confirmation_only_not_industry_discovery"
        ),
        "demand_branches": list(theme.get("demand_branches") or []),
        "primary_growth_branch": theme.get("primary_growth_branch"),
        "indicator_history_path": str(history_path),
        "investable_chain_nodes": list(
            theme.get("investable_chain_nodes")
            or theme.get("chain_nodes")
            or []
        ),
        "key_uncertainties": list(theme.get("key_uncertainties") or []),
        "human_confirmation_required": True,
    }


def _financial_validation_for_theme(
    theme: Mapping[str, Any],
    financial_panel: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if (
        theme.get("financial_validation_mode")
        == "disabled_without_theme_exposure_mapping"
    ):
        return []
    accepted_names = {
        _industry_key(value)
        for value in (
            list(theme.get("source_sw_industries") or [])
            + list(theme.get("financial_industries") or [])
        )
        if value
    }
    if not accepted_names:
        return []
    return [
        dict(item)
        for item in financial_panel.get("industries") or []
        if isinstance(item, Mapping)
        and _industry_key(item.get("industry_name")) in accepted_names
    ]


def _resolve_monitor_themes(
    structural_pool: Mapping[str, Any],
    configured_themes: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    pool_themes = structural_pool.get("themes") if isinstance(structural_pool, Mapping) else []
    if isinstance(pool_themes, list) and pool_themes:
        config_map = {
            str(item.get("theme_id")): dict(item)
            for item in configured_themes
            if item.get("theme_id")
        }
        merged: list[dict[str, Any]] = []
        for raw_theme in pool_themes:
            if not isinstance(raw_theme, Mapping) or not raw_theme.get("theme_id"):
                continue
            theme = dict(config_map.get(str(raw_theme["theme_id"]), {}))
            theme.update(dict(raw_theme))
            if not theme.get("leading_indicators"):
                theme["leading_indicators"] = config_map.get(
                    str(raw_theme["theme_id"]), {}
                ).get("leading_indicators", [])
            merged.append(theme)
        return merged, "loaded"

    provisional = []
    for configured in configured_themes:
        theme = dict(configured)
        theme.update(
            {
                "theme_name": theme.get("name"),
                "classification": "unreviewed",
                "structural_space": {"passed": False},
                "human_approved": False,
            }
        )
        provisional.append(theme)
    return provisional, "missing"


def _load_latest_structural_pool(
    paths: IndustryResearchPaths,
    run_date: str,
) -> tuple[dict[str, Any], Path | None]:
    root = paths.structural_scans_root_dir
    if not root.exists():
        return {}, None
    cutoff = _parse_iso_date(run_date)
    candidates: list[tuple[date, Path]] = []
    for run_dir in root.iterdir():
        if not run_dir.is_dir():
            continue
        parsed = _try_parse_iso_date(run_dir.name)
        pool_path = run_dir / "structural_opportunity_pool.json"
        if parsed is not None and parsed <= cutoff and pool_path.exists():
            candidates.append((parsed, pool_path))
    if not candidates:
        return {}, None
    candidates.sort(key=lambda item: item[0])
    selected_path = candidates[-1][1]
    return _load_json(selected_path), selected_path


def _load_indicator_history(path: Path, run_day: date) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    items: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            item = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"指标历史JSONL损坏: path={path} line={line_number}") from exc
        as_of_date = str(item.get("as_of_date") or "")
        parsed = _try_parse_iso_date(as_of_date)
        if parsed is not None and parsed <= run_day and item.get("indicator_id"):
            items.append(dict(item))
    return items


def _latest_indicator_observations(
    history: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for raw_item in history:
        item = dict(raw_item)
        indicator_id = str(item["indicator_id"])
        previous = latest.get(indicator_id)
        if previous is None or str(item["as_of_date"]) >= str(previous["as_of_date"]):
            latest[indicator_id] = item
    return latest


def _render_monthly_monitor(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload['run_date']} 月度产业基本面监控",
        "",
        "本监控只看结构空间与产业领先指标，明确排除热点新闻、板块涨幅、资金流和股票动量。",
        "季度财务数据只用于验证，不用于发现爆发式增长行业。",
        "",
        "| 产业主题 | 结构分类 | 结构空间 | 新鲜指标 | 改善指标 | 恶化指标 | 当前状态 |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for theme in payload.get("themes") or []:
        signals = theme["signal_summary"]
        lines.append(
            "| {name} | {classification} | {space} | {fresh} | {improving} | {deteriorating} | {status} |".format(
                name=theme["theme_name"],
                classification=theme["classification"],
                space="通过" if theme["structural_space"].get("passed") else "未通过/未评估",
                fresh=signals["fresh_indicator_count"],
                improving=signals["improving_count"],
                deteriorating=signals["deteriorating_count"],
                status=theme["monitor_status"],
            )
        )
    lines.extend(["", "## 本月需要更新的领先指标", ""])
    missing_any = False
    for theme in payload.get("themes") or []:
        missing = theme.get("missing_or_stale_indicator_ids") or []
        if not missing:
            continue
        missing_any = True
        lines.append(f"- {theme['theme_name']}: {', '.join(missing)}")
    if not missing_any:
        lines.append("- 无")
    lines.extend(
        [
            "",
            "## Agent更新要求",
            "",
            "1. 只搜索已批准结构性主题的指标，不扫描市场热点。",
            "2. 每个指标记录真实数值、期间、发布日期、来源和 `improving/stable/deteriorating/unknown`。",
            "3. 至少两个不同类别的指标改善，且没有多项恶化，才可标记基本面右侧候选。",
            "4. 财报高增长不能替代终端需求、订单、库存、价格、交期或资本开支证据。",
            "5. 最多建议一个主题进入全产业链深研，并暂停等待用户确认。",
            "",
        ]
    )
    return "\n".join(lines)


def _theme_sort_key(item: Mapping[str, Any]) -> tuple[int, str]:
    order = {
        "fundamental_right_candidate": 0,
        "fundamentals_deteriorating": 1,
        "watch": 2,
        "needs_indicator_update": 3,
        "awaiting_human_approval": 4,
        "needs_structural_scan": 5,
        "not_structural_candidate": 6,
    }
    return order.get(str(item.get("monitor_status")), 99), str(item.get("theme_id") or "")


def _industry_key(value: Any) -> str:
    text = "".join(str(value or "").split())
    return text.replace("Ⅱ", "").replace("Ⅲ", "").casefold()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _try_parse_iso_date(value: str) -> date | None:
    try:
        return _parse_iso_date(value)
    except ValueError:
        return None
