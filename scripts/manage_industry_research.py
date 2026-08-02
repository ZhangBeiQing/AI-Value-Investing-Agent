#!/usr/bin/env python3
"""CLI for the independent monthly industry-research layer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger
from services.industry_research import (
    build_monthly_industry_radar,
    build_structural_scan_input,
    prepare_theme_research_workdir,
)
from services.industry_research.config import DEFAULT_CONFIG_PATH, load_industry_research_config
from shared_data_access.industry_catalog import update_industry_catalog_cached
from shared_data_access.industry_financial_panel import update_industry_financial_panel_cached


LOGGER = init_component_logger(
    "IndustryResearch",
    group="industry_research",
    filename_prefix="manage_industry_research",
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="管理独立的月度行业景气研究层。")
    parser.add_argument("--base-dir", default="data", help="数据根目录，默认 data。")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="产业主题注册配置。")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh_parser = subparsers.add_parser(
        "refresh-data",
        help="刷新月度全A业绩横截面并生成行业财务面板。",
    )
    refresh_parser.add_argument("--date", required=True, help="研究截止日，格式 YYYY-MM-DD。")
    refresh_parser.add_argument("--force-refresh", action="store_true", help="忽略30天TTL并强制刷新。")

    catalog_parser = subparsers.add_parser(
        "refresh-catalog",
        help="刷新半年级申万一二三级行业目录。",
    )
    catalog_parser.add_argument("--force-refresh", action="store_true", help="忽略180天TTL并强制刷新。")

    structural_parser = subparsers.add_parser(
        "build-structural-scan",
        help="生成申万二级行业结构空间预检输入。",
    )
    structural_parser.add_argument("--date", required=True, help="研究截止日，格式 YYYY-MM-DD。")

    radar_parser = subparsers.add_parser(
        "build-radar",
        help="从已批准结构池和领先指标历史生成月度基本面监控。",
    )
    radar_parser.add_argument("--date", required=True, help="分析截止日，格式 YYYY-MM-DD。")

    list_parser = subparsers.add_parser("list-themes", help="列出已注册的产业研究主题。")
    list_parser.add_argument("--json", action="store_true", help="以 JSON 写入日志。")

    prepare_parser = subparsers.add_parser(
        "prepare-theme-research",
        help="在用户确认后，为单个产业主题准备深研工作目录。",
    )
    prepare_parser.add_argument("--date", required=True, help="对应月度雷达日期。")
    prepare_parser.add_argument("--theme-id", required=True, help="theme_registry 中的主题ID。")
    prepare_parser.add_argument("--force", action="store_true", help="显式覆盖已有工作目录模板。")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.command == "refresh-data":
        outputs = update_industry_financial_panel_cached(
            args.date,
            base_dir=args.base_dir,
            force_refresh=args.force_refresh,
        )
        LOGGER.info("行业财务面板输出: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
        return 0
    if args.command == "refresh-catalog":
        outputs = update_industry_catalog_cached(
            base_dir=args.base_dir,
            force_refresh=args.force_refresh,
        )
        LOGGER.info("申万行业目录输出: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
        return 0
    if args.command == "build-structural-scan":
        outputs = build_structural_scan_input(
            args.date,
            base_dir=args.base_dir,
        )
        LOGGER.info("结构空间预检输入: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
        return 0
    if args.command == "build-radar":
        outputs = build_monthly_industry_radar(
            args.date,
            base_dir=args.base_dir,
            config_path=args.config,
        )
        LOGGER.info("月度行业雷达输出: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
        return 0
    if args.command == "list-themes":
        config = load_industry_research_config(args.config)
        themes = [
            {
                "theme_id": item["theme_id"],
                "name": item["name"],
                "research_status": item.get("research_status"),
            }
            for item in config["themes"]
        ]
        rendered = json.dumps(themes, ensure_ascii=False, indent=2 if args.json else None)
        LOGGER.info("已注册产业主题: %s", rendered)
        return 0
    if args.command == "prepare-theme-research":
        outputs = prepare_theme_research_workdir(
            args.date,
            args.theme_id,
            base_dir=args.base_dir,
            config_path=args.config,
            force=args.force,
        )
        LOGGER.info("产业主题深研工作目录: %s", json.dumps({k: str(v) for k, v in outputs.items()}, ensure_ascii=False))
        return 0
    raise ValueError(f"不支持的命令: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
