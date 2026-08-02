from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from services.industry_research.radar import build_monthly_industry_radar
from services.industry_research.structural_scan import build_structural_scan_input
from services.industry_research.workdir import prepare_theme_research_workdir
from shared_data_access.industry_financial_panel import (
    aggregate_industry_financials,
    completed_report_periods,
)


def _write_config(path: Path) -> None:
    path.write_text(
        """schema_version: 1
radar:
  indicator_stale_days: 45
  minimum_improving_indicators: 2
  minimum_independent_indicator_categories: 2
  maximum_deteriorating_indicators: 1
states: [observation, emerging, fundamental_right, expansion, overheated, downturn, archived]
themes:
  - theme_id: test_theme
    name: 测试产业链
    research_status: pilot
    aliases: [测试产业]
    source_sw_industries: [测试产业]
    chain_nodes: [上游, 中游, 下游]
    leading_indicators:
      - indicator_id: demand_volume
        name: 终端销量
        category: demand
        frequency: monthly
        positive_direction: higher
      - indicator_id: delivery_lead_time
        name: 交付周期
        category: supply
        frequency: monthly
        positive_direction: longer
    indicator_blueprint: [终端销量, 交付周期]
""",
        encoding="utf-8",
    )


def _industry_item() -> dict:
    return {
        "industry_name": "测试产业",
        "company_count": 3,
        "comparable_company_count": 3,
        "median_revenue_growth_yoy_pct": 35.0,
        "median_net_income_growth_yoy_pct": 50.0,
        "positive_revenue_growth_count": 3,
        "positive_revenue_growth_ratio": 1.0,
        "positive_net_income_growth_count": 3,
        "positive_net_income_growth_ratio": 1.0,
        "both_positive_count": 3,
        "both_positive_ratio": 1.0,
        "median_revenue_growth_acceleration_pct": 20.0,
        "median_net_income_growth_acceleration_pct": 30.0,
        "revenue_accelerating_count": 3,
        "revenue_accelerating_ratio": 1.0,
        "net_income_accelerating_count": 3,
        "net_income_accelerating_ratio": 1.0,
        "both_accelerating_count": 3,
        "both_accelerating_ratio": 1.0,
        "median_gross_margin_pct": 35.0,
        "median_roe_pct": 12.0,
        "leaders_by_revenue": [
            {"stock_code": "000001", "stock_name": "测试甲", "revenue": 300.0},
            {"stock_code": "000002", "stock_name": "测试乙", "revenue": 200.0},
            {"stock_code": "000003", "stock_name": "测试丙", "revenue": 100.0},
        ],
        "leaders_by_net_income": [],
        "leaders_by_growth_acceleration": [],
    }


def _write_industry_panel(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "as_of_date": "2026-07-13",
                "current_report_period": "20260331",
                "previous_report_period": "20251231",
                "industries": [_industry_item()],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _write_catalog(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_date": "2026-07-13",
                "levels": {
                    "level2": [
                        {
                            "level": "level2",
                            "industry_code": "801001.SI",
                            "industry_name": "测试产业",
                            "parent_industry": "测试上级",
                            "constituent_count": 3,
                            "pe_ttm": 20.0,
                            "pb": 2.0,
                        },
                        {
                            "level": "level2",
                            "industry_code": "801002.SI",
                            "industry_name": "成熟产业",
                            "parent_industry": "测试上级",
                            "constituent_count": 10,
                            "pe_ttm": 12.0,
                            "pb": 1.0,
                        },
                    ]
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _write_structural_pool(path: Path, *, approved: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "as_of_date": "2026-07-13",
                "status": "approved" if approved else "draft",
                "themes": [
                    {
                        "theme_id": "test_theme",
                        "theme_name": "测试产业链",
                        "source_sw_industries": ["测试产业"],
                        "classification": "structural_growth",
                        "structural_space": {
                            "base_revenue_multiple": 2.5,
                            "base_revenue_cagr_pct": 25.0,
                            "current_penetration_pct": 10.0,
                            "target_penetration_pct": 40.0,
                            "passed": True,
                        },
                        "leading_indicators": [
                            {
                                "indicator_id": "demand_volume",
                                "name": "终端销量",
                                "category": "demand",
                                "frequency": "monthly",
                            },
                            {
                                "indicator_id": "delivery_lead_time",
                                "name": "交付周期",
                                "category": "supply",
                                "frequency": "monthly",
                            },
                        ],
                        "investable_chain_nodes": ["中游"],
                        "human_approved": approved,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _write_indicator_history(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "as_of_date": "2026-07-01",
            "indicator_id": "demand_volume",
            "indicator_name": "终端销量",
            "category": "demand",
            "signal": "improving",
            "value": 120,
            "unit": "万台",
            "release_date": "2026-07-05",
            "source_title": "行业协会月报",
        },
        {
            "as_of_date": "2026-07-01",
            "indicator_id": "delivery_lead_time",
            "indicator_name": "交付周期",
            "category": "supply",
            "signal": "improving",
            "value": 16,
            "unit": "周",
            "release_date": "2026-07-06",
            "source_title": "龙头公司业绩会",
        },
    ]
    path.write_text(
        "\n".join(json.dumps(item, ensure_ascii=False) for item in records) + "\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    ("as_of_date", "expected"),
    [
        ("2026-02-01", ("20250930", "20250630")),
        ("2026-07-13", ("20260331", "20251231")),
        ("2026-09-01", ("20260630", "20260331")),
        ("2026-11-01", ("20260930", "20260630")),
    ],
)
def test_completed_report_periods_uses_completed_disclosure_windows(
    as_of_date: str,
    expected: tuple[str, str],
) -> None:
    assert completed_report_periods(as_of_date) == expected


def test_aggregate_industry_financials_calculates_breadth_and_acceleration() -> None:
    current = pd.DataFrame(
        {
            "stock_code": ["000001", "000002", "000003"],
            "stock_name": ["测试甲", "测试乙", "测试丙"],
            "em_industry": ["测试产业"] * 3,
            "revenue": [300.0, 200.0, 100.0],
            "net_income": [30.0, 20.0, 10.0],
            "revenue_growth_yoy": [30.0, 20.0, -5.0],
            "net_income_growth_yoy": [40.0, 10.0, -10.0],
            "gross_margin": [40.0, 35.0, 30.0],
            "roe": [15.0, 12.0, 9.0],
        }
    )
    previous = pd.DataFrame(
        {
            "stock_code": ["000001", "000002", "000003"],
            "revenue_growth_yoy": [10.0, 25.0, -20.0],
            "net_income_growth_yoy": [20.0, 5.0, -5.0],
        }
    )
    payload = aggregate_industry_financials(
        current,
        previous,
        as_of_date="2026-07-13",
        current_period="20260331",
        previous_period="20251231",
    )
    industry = payload["industries"][0]
    assert industry["median_revenue_growth_yoy_pct"] == 20.0
    assert industry["both_positive_count"] == 2
    assert industry["both_accelerating_count"] == 1
    assert industry["median_revenue_growth_acceleration_pct"] == 15.0


def test_build_structural_scan_covers_sw_level2_without_making_conclusions(tmp_path: Path) -> None:
    _write_catalog(
        tmp_path / "global_cache" / "industry_catalog_sw" / "snapshots" / "2026-07-13.json"
    )
    _write_industry_panel(
        tmp_path
        / "global_cache"
        / "industry_financial_panel"
        / "snapshots"
        / "2026-07-13.json"
    )

    outputs = build_structural_scan_input("2026-07-13", base_dir=tmp_path)

    payload = json.loads(outputs["scan_input"].read_text(encoding="utf-8"))
    assert payload["summary"]["sw_level2_industry_count"] == 2
    assert payload["summary"]["financial_matched_count"] == 1
    assert payload["industries"][0]["structural_space_precheck"]["precheck_classification"] == "unreviewed"
    assert outputs["structural_pool_template"].exists()


def test_monthly_radar_requires_structural_scan_before_selecting_industry(tmp_path: Path) -> None:
    config_path = tmp_path / "theme_registry.yaml"
    _write_config(config_path)

    outputs = build_monthly_industry_radar(
        "2026-07-13",
        base_dir=tmp_path,
        config_path=config_path,
    )

    radar = json.loads(outputs["industry_radar"].read_text(encoding="utf-8"))
    assert radar["summary"]["pool_status"] == "missing"
    assert radar["themes"][0]["monitor_status"] == "needs_structural_scan"
    assert radar["summary"]["fundamental_right_candidate_count"] == 0
    assert "hot_news" in radar["excluded_inputs"]
    assert "board_heat" in radar["excluded_inputs"]


def test_monthly_radar_marks_right_side_only_with_approved_space_and_indicators(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "theme_registry.yaml"
    _write_config(config_path)
    _write_structural_pool(
        tmp_path
        / "industry_research"
        / "structural_scans"
        / "2026-07-13"
        / "structural_opportunity_pool.json"
    )
    _write_indicator_history(
        tmp_path / "industry_research" / "cards" / "test_theme" / "indicator_history.jsonl"
    )
    _write_industry_panel(
        tmp_path
        / "global_cache"
        / "industry_financial_panel"
        / "snapshots"
        / "2026-07-13.json"
    )

    outputs = build_monthly_industry_radar(
        "2026-07-13",
        base_dir=tmp_path,
        config_path=config_path,
    )

    radar = json.loads(outputs["industry_radar"].read_text(encoding="utf-8"))
    theme = radar["themes"][0]
    assert radar["schema_version"] == 3
    assert theme["monitor_status"] == "fundamental_right_candidate"
    assert theme["leading_confirmation"] is True
    assert theme["signal_summary"]["improving_categories"] == ["demand", "supply"]
    assert len(theme["quarterly_financial_validation"]) == 1
    assert theme["financial_role"] == "confirmation_only_not_industry_discovery"


def test_monthly_radar_does_not_use_broad_industry_financials_for_theme_segments(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "theme_registry.yaml"
    _write_config(config_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "    research_status: pilot\n",
            "    research_status: pilot\n"
            "    financial_validation_mode: disabled_without_theme_exposure_mapping\n",
        ),
        encoding="utf-8",
    )
    _write_structural_pool(
        tmp_path
        / "industry_research"
        / "structural_scans"
        / "2026-07-13"
        / "structural_opportunity_pool.json"
    )
    _write_indicator_history(
        tmp_path / "industry_research" / "cards" / "test_theme" / "indicator_history.jsonl"
    )
    _write_industry_panel(
        tmp_path
        / "global_cache"
        / "industry_financial_panel"
        / "snapshots"
        / "2026-07-13.json"
    )

    outputs = build_monthly_industry_radar(
        "2026-07-13",
        base_dir=tmp_path,
        config_path=config_path,
    )

    theme = json.loads(outputs["industry_radar"].read_text(encoding="utf-8"))["themes"][0]
    assert theme["quarterly_financial_validation"] == []
    assert theme["financial_role"] == "disabled_until_theme_exposure_mapping_exists"


def test_prepare_theme_research_workdir_requires_explicit_overwrite(tmp_path: Path) -> None:
    config_path = tmp_path / "theme_registry.yaml"
    _write_config(config_path)
    _write_structural_pool(
        tmp_path
        / "industry_research"
        / "structural_scans"
        / "2026-07-13"
        / "structural_opportunity_pool.json"
    )
    _write_indicator_history(
        tmp_path / "industry_research" / "cards" / "test_theme" / "indicator_history.jsonl"
    )
    build_monthly_industry_radar(
        "2026-07-13",
        base_dir=tmp_path,
        config_path=config_path,
    )
    outputs = prepare_theme_research_workdir(
        "2026-07-13",
        "test_theme",
        base_dir=tmp_path,
        config_path=config_path,
    )
    assert outputs["research_brief"].exists()
    profit_map = json.loads(outputs["profit_map_template"].read_text(encoding="utf-8"))
    assert "company_exposure_matrix" in profit_map
    assert profit_map["company_evidence_rules"]["company_total_metrics_cannot_validate_theme_segment"] is True
    assert not (tmp_path / "skill_runs").exists()
    with pytest.raises(FileExistsError, match="已有产物"):
        prepare_theme_research_workdir(
            "2026-07-13",
            "test_theme",
            base_dir=tmp_path,
            config_path=config_path,
        )
