"""Prepare a human-confirmed theme workdir for agent deep research."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Mapping

from core.logging import get_logger
from services.selection_system.store import save_json_file

from .config import DEFAULT_CONFIG_PATH, get_theme, load_industry_research_config
from .paths import IndustryResearchPaths


LOGGER = get_logger("IndustryResearchWorkdir")


def prepare_theme_research_workdir(
    run_date: str,
    theme_id: str,
    *,
    base_dir: str | Path = "data",
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    force: bool = False,
) -> dict[str, Path]:
    """Create deep-research inputs after the user has approved one radar theme."""

    config = load_industry_research_config(config_path)
    paths = IndustryResearchPaths.from_base_dir(base_dir)
    radar_path = paths.radar_json_path(run_date)
    if not radar_path.exists():
        raise FileNotFoundError(f"月度行业雷达不存在，请先运行 build-radar: {radar_path}")
    radar = json.loads(radar_path.read_text(encoding="utf-8"))
    theme_radar = _find_theme_radar(radar, theme_id)
    try:
        theme = get_theme(config, theme_id)
    except KeyError:
        summary = theme_radar["summary"]
        theme = {
            "theme_id": theme_id,
            "name": summary.get("theme_name") or theme_id,
            "chain_nodes": list(summary.get("investable_chain_nodes") or []),
            "indicator_blueprint": [
                item.get("name") or item.get("indicator_name") or item.get("indicator_id")
                for item in summary.get("leading_indicators") or []
                if isinstance(item, Mapping)
            ],
        }

    workdir = paths.theme_workdir(run_date, theme_id)
    workdir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "research_brief": workdir / "00_research_brief.json",
        "deep_research_prompt": workdir / "01_deep_research_prompt.md",
        "industry_card_template": workdir / "02_industry_card_template.json",
        "profit_map_template": workdir / "03_profit_map_template.json",
    }
    existing = [path for path in outputs.values() if path.exists()]
    if existing and not force:
        names = ", ".join(str(path) for path in existing)
        raise FileExistsError(f"深研工作目录已有产物；如需重建请显式使用 force: {names}")

    card_dir = paths.theme_card_dir(theme_id)
    card_dir.mkdir(parents=True, exist_ok=True)
    research_output = card_dir / "research" / f"{run_date}.md"
    industry_card_output = card_dir / "industry_card.json"
    profit_map_output = card_dir / "profit_map.json"
    state_history_output = card_dir / "state_history.jsonl"

    brief = {
        "schema_version": 1,
        "run_date": run_date,
        "prepared_at": datetime.now().isoformat(),
        "theme": theme,
        "radar_evidence": theme_radar,
        "source_radar_path": str(radar_path),
        "research_boundary": {
            "maximum_theme_count": 1,
            "trade_decision_allowed": False,
            "portfolio_change_allowed": False,
            "required_human_confirmation_before_research": True,
        },
        "expected_outputs": {
            "research_markdown": str(research_output),
            "industry_card": str(industry_card_output),
            "profit_map": str(profit_map_output),
            "state_history": str(state_history_output),
        },
    }
    save_json_file(outputs["research_brief"], brief)
    outputs["deep_research_prompt"].write_text(
        _render_deep_research_prompt(brief, outputs),
        encoding="utf-8",
    )
    save_json_file(
        outputs["industry_card_template"],
        _industry_card_template(theme, run_date, config.get("states") or []),
    )
    save_json_file(outputs["profit_map_template"], _profit_map_template(theme, run_date))
    LOGGER.info(
        "产业主题深研工作目录已准备: run_date=%s theme=%s workdir=%s",
        run_date,
        theme_id,
        workdir,
    )
    return {"workdir": workdir, **outputs}


def _find_theme_radar(radar: Mapping[str, Any], theme_id: str) -> dict[str, Any]:
    registered_themes = radar.get("themes") or []
    theme_summary = next(
        (
            dict(item)
            for item in registered_themes
            if isinstance(item, Mapping) and item.get("theme_id") == theme_id
        ),
        None,
    )
    if theme_summary is None:
        raise KeyError(f"月度雷达中没有主题: {theme_id}")
    return {"summary": theme_summary}


def _render_deep_research_prompt(brief: Mapping[str, Any], outputs: Mapping[str, Path]) -> str:
    theme = brief["theme"]
    expected = brief["expected_outputs"]
    indicators = "\n".join(f"- {item}" for item in theme.get("indicator_blueprint") or [])
    chain_nodes = "\n".join(f"- {item}" for item in theme.get("chain_nodes") or [])
    return f"""# {theme['name']} 月度深度研究任务

本任务只研究一个已由用户确认的产业主题，不作股票交易决策，也不修改任何交易文件。

## 必读统一规则

1. `configs/research/industry_chain_research_policy.md`
2. `configs/research/web_research_policy.md`
3. `.codex/skills/monthly-industry-research/references/output-contract.md`

## 必读输入

1. `{outputs['research_brief']}`
2. `{outputs['industry_card_template']}`
3. `{outputs['profit_map_template']}`
4. 源月度雷达 `{brief['source_radar_path']}`

## 产业链初始节点

{chain_nodes}

## 初始指标蓝图

{indicators}

## 固定研究顺序

1. 先验证雷达信号，判断它属于结构性增长、普通周期反弹还是短期事件。
2. 先验证 `terminal_theme → subchain → value_chain_node → company_exposure`，再拆分终端分支并建立三至五年悲观/基准/乐观 TAM 公式。
3. 用单位耗用量把下游 GWh 传导到电芯、中游材料和上游资源，避免把单个应用场景误当成完整行业。
4. 拆解需求、供给、库存、价格、交期、资本开支和扩产周期，定位真实供需错配。
5. 绘制全产业链，判断收入增长最终在哪些节点转化为利润，并解释供给为何不能快速响应。
6. 在最受益节点内研究所有对结论重大的候选公司，强制记录主题收入占比、主题毛利、披露口径与其他主营业务；不预设公司是龙头，不规定公司数量。
7. 综合公司必须采用分部加总思路；无法分拆时不得用公司总业绩冒充主题分部表现。
8. 给出当前状态与状态迁移条件，并列出可观测的证伪信号。

## 搜索与证据要求

- 所有外部搜索首先使用百炼 `bailian_web_search` 能力；不可用时失败关闭，不静默切换搜索引擎。
- 优先使用政府、交易所、行业协会、公司公告、业绩会和客户/供应商材料。
- 普通新闻只能作为线索；关键结论至少需要两个相互独立的来源，或一个权威原始来源。
- 每条关键事实记录 `period_end`、`release_date`、`fetched_at`、来源标题和URL。
- 对缺失、冲突或无法验证的数据明确降置信度，不得用叙事补数。
- 财报只作验证；行业发现优先使用订单、招标、销量、价格、库存、交期和资本开支等领先或同步指标。

## 输出

- 深度研究报告：`{expected['research_markdown']}`
- 完整行业卡片：`{expected['industry_card']}`
- 产业链利润映射：`{expected['profit_map']}`
- 状态历史：`{expected['state_history']}`

先完成研究报告、行业卡片与利润映射。状态历史只追加一条本次结论，不覆盖旧记录。
结束时向用户汇报证据最强的结论、最大不确定性，并暂停等待是否纳入长期观察池的人工决定。
"""


def _industry_card_template(theme: Mapping[str, Any], run_date: str, states: list[str]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "theme_id": theme["theme_id"],
        "theme_name": theme["name"],
        "as_of_date": run_date,
        "research_status": "draft",
        "current_state": "observation",
        "allowed_states": states,
        "thesis": "",
        "classification": "structural_growth | cyclical_rebound | event_driven | insufficient_evidence",
        "terminal_demand": {
            "unit": "",
            "current_volume": None,
            "current_penetration_pct": None,
            "formula": "",
        },
        "demand_branches": list(theme.get("demand_branches") or []),
        "demand_to_upstream_transmission": [],
        "tam_scenarios": {
            "bear": {"horizon_year": None, "revenue": None, "assumptions": []},
            "base": {"horizon_year": None, "revenue": None, "assumptions": []},
            "bull": {"horizon_year": None, "revenue": None, "assumptions": []},
        },
        "demand_indicators": [],
        "supply_indicators": [],
        "price_inventory_lead_time": [],
        "state_transition_conditions": {
            "upgrade": [],
            "downgrade": [],
        },
        "invalidation_conditions": [],
        "key_uncertainties": [],
        "evidence": [],
        "confidence": "low | medium | high",
        "human_decision": "pending",
    }


def _profit_map_template(theme: Mapping[str, Any], run_date: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "theme_id": theme["theme_id"],
        "theme_name": theme["name"],
        "as_of_date": run_date,
        "chain_nodes": [
            {
                "node_name": node,
                "demand_driver": "",
                "supply_constraint": "",
                "pricing_power": "low | medium | high",
                "capacity_response_time": "",
                "profit_pool_direction": "shrinking | stable | expanding | unknown",
                "listed_beneficiaries": [],
                "key_evidence_ids": [],
            }
            for node in theme.get("chain_nodes") or []
        ],
        "most_benefited_nodes": [],
        "company_ranking": [],
        "company_exposure_matrix": [],
        "company_evidence_rules": {
            "theme_revenue_share_required": True,
            "theme_gross_profit_or_margin_required": True,
            "disclosure_scope_required": True,
            "company_total_metrics_cannot_validate_theme_segment": True,
        },
        "market_cap_scenarios": [],
        "human_review_required": True,
    }
