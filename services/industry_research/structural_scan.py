"""Prepare the semiannual structural-growth scan for agent research."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from core.logging import get_logger
from services.selection_system.store import save_json_file
from shared_data_access.industry_catalog import load_industry_catalog_cached
from shared_data_access.industry_financial_panel import (
    load_industry_financial_snapshot_cached,
)

from .paths import IndustryResearchPaths


LOGGER = get_logger("IndustryStructuralScan")


def build_structural_scan_input(
    run_date: str,
    *,
    base_dir: str | Path = "data",
) -> dict[str, Path]:
    """Build a 131-industry precheck input without making structural conclusions."""

    paths = IndustryResearchPaths.from_base_dir(base_dir)
    paths.ensure_structural_scan_dir(run_date)
    catalog, catalog_path = load_industry_catalog_cached(
        run_date,
        base_dir=paths.base_dir,
        allow_previous=True,
    )
    if catalog_path is None:
        raise FileNotFoundError("缺少申万行业目录缓存，请先运行 refresh-catalog")
    financial_panel, financial_path = load_industry_financial_snapshot_cached(
        run_date,
        base_dir=paths.base_dir,
        allow_previous=True,
    )
    financial_map = {
        _industry_key(item.get("industry_name")): dict(item)
        for item in financial_panel.get("industries") or []
        if isinstance(item, Mapping) and item.get("industry_name")
    }

    level2 = ((catalog.get("levels") or {}).get("level2") or [])
    industries: list[dict[str, Any]] = []
    for raw_item in level2:
        if not isinstance(raw_item, Mapping):
            continue
        item = dict(raw_item)
        financial = financial_map.get(_industry_key(item.get("industry_name")))
        industries.append(
            {
                "sw_industry_code": item.get("industry_code"),
                "sw_industry_name": item.get("industry_name"),
                "parent_industry": item.get("parent_industry"),
                "constituent_count": item.get("constituent_count"),
                "valuation_reference": {
                    "pe_ttm": item.get("pe_ttm"),
                    "pb": item.get("pb"),
                },
                "quarterly_financial_validation": financial,
                "structural_space_precheck": {
                    "terminal_demand_unit": None,
                    "current_market_size": None,
                    "base_terminal_market_size_3_5y": None,
                    "base_revenue_multiple": None,
                    "base_revenue_cagr_pct": None,
                    "current_penetration_pct": None,
                    "target_penetration_pct": None,
                    "growth_driver": None,
                    "precheck_classification": "unreviewed",
                },
            }
        )

    payload = {
        "schema_version": 1,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "cadence": "semiannual",
        "decision_boundary": "structural_space_precheck_only",
        "source_status": [
            {"source": "sw_industry_catalog", "path": str(catalog_path), "status": "ok"},
            {
                "source": "quarterly_industry_financial_validation",
                "path": str(financial_path) if financial_path else None,
                "status": "ok" if financial_path else "warning",
            },
        ],
        "summary": {
            "sw_level2_industry_count": len(industries),
            "financial_matched_count": sum(
                1 for item in industries if item["quarterly_financial_validation"] is not None
            ),
        },
        "required_questions": [
            "终端需求的计量单位是什么？",
            "当前市场规模与三至五年基准终局空间是多少？",
            "增长来自渗透率、单位价值、国产替代、全球份额还是短期涨价？",
            "增长空间能否被经济总量和下游付费能力承载？",
            "A股是否存在真正承接利润的产业链节点？",
        ],
        "industries": industries,
        "human_gate": {
            "required": True,
            "target_structural_theme_count": "10-20",
            "maximum_monthly_deep_research_themes": 1,
        },
    }
    save_json_file(paths.structural_scan_input_path(run_date), payload)
    paths.structural_scan_markdown_path(run_date).write_text(
        _render_structural_scan_markdown(payload),
        encoding="utf-8",
    )
    save_json_file(
        paths.structural_pool_template_path(run_date),
        _structural_pool_template(run_date),
    )
    LOGGER.info(
        "半年结构空间预检输入已生成: run_date=%s industries=%d output=%s",
        run_date,
        len(industries),
        paths.structural_scan_input_path(run_date),
    )
    return {
        "scan_input": paths.structural_scan_input_path(run_date),
        "scan_markdown": paths.structural_scan_markdown_path(run_date),
        "structural_pool_template": paths.structural_pool_template_path(run_date),
        "structural_pool": paths.structural_pool_path(run_date),
    }


def _render_structural_scan_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload['run_date']} 申万二级行业结构空间预检",
        "",
        "本任务只做大空间、低渗透率预检，不使用热点新闻、板块涨幅或股票动量。",
        "先对全部行业做轻量分类，再只对最可能的20个方向联网补证，最终保留10-20个结构性候选。",
        "",
        "## 统一问题",
        "",
    ]
    lines.extend(f"- {question}" for question in payload.get("required_questions") or [])
    lines.extend(
        [
            "",
            "## 申万二级行业",
            "",
            "| 代码 | 行业 | 上级行业 | 成分数 | 财务样本 | 营收增速中位数 | 利润增速中位数 |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in payload.get("industries") or []:
        financial = item.get("quarterly_financial_validation") or {}
        lines.append(
            "| {code} | {name} | {parent} | {count} | {sample} | {revenue} | {profit} |".format(
                code=item.get("sw_industry_code") or "-",
                name=item.get("sw_industry_name") or "-",
                parent=item.get("parent_industry") or "-",
                count=item.get("constituent_count") or 0,
                sample=financial.get("company_count") or 0,
                revenue=_format_pct(financial.get("median_revenue_growth_yoy_pct")),
                profit=_format_pct(financial.get("median_net_income_growth_yoy_pct")),
            )
        )
    lines.extend(
        [
            "",
            "## 输出约束",
            "",
            "- `structural_growth`：三至五年基准收入空间达到约2倍且增长来源可持续。",
            "- `cyclical`：主要由价格、库存周期或短期供给冲击驱动。",
            "- `mature`：终局空间受限或渗透率已经较高。",
            "- `uncertain`：空间可能很大，但商业化时间和付费能力无法验证。",
            "- 不因当前财务高增长直接判定为结构性增长。",
            "- 最终结果写入模板声明的 `structural_opportunity_pool.json`，等待用户批准。",
            "",
        ]
    )
    return "\n".join(lines)


def _structural_pool_template(run_date: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "as_of_date": run_date,
        "status": "draft",
        "methodology": "large_terminal_space_low_penetration",
        "themes": [
            {
                "theme_id": "",
                "theme_name": "",
                "source_sw_industries": [],
                "classification": "structural_growth",
                "structural_space": {
                    "terminal_demand_unit": "",
                    "current_market_size": None,
                    "base_terminal_market_size_3_5y": None,
                    "base_revenue_multiple": None,
                    "base_revenue_cagr_pct": None,
                    "current_penetration_pct": None,
                    "target_penetration_pct": None,
                    "growth_driver": "penetration | unit_value | localization | global_share | mixed",
                    "passed": False,
                },
                "leading_indicators": [],
                "investable_chain_nodes": [],
                "key_uncertainties": [],
                "evidence": [],
                "human_approved": False,
            }
        ],
    }


def _industry_key(value: Any) -> str:
    text = "".join(str(value or "").split())
    return text.replace("Ⅱ", "").replace("Ⅲ", "").casefold()


def _format_pct(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "-"


__all__ = ["build_structural_scan_input"]
