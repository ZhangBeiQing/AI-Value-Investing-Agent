#!/usr/bin/env python3
"""Register an existing manual financial report summary into summary_index.json."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.research.financial_report_skill import (
    _output_path,
    financial_report_workdir,
    select_latest_two_reports,
    update_summary_index,
    validate_deep_research_artifacts,
)
from utlity.stock_utils import parse_symbol


def _is_meaningful_summary(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    stripped = content.strip()
    if not stripped:
        return False
    # 防止只建了一个占位空标题、注释或极短内容就被误登记
    non_whitespace_len = len("".join(stripped.split()))
    return non_whitespace_len >= 80


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="将已有手动财报分析文档登记到 summary_index.json。")
    parser.add_argument("--symbol", required=True, help="股票代码，例如 300750.SZ")
    parser.add_argument(
        "--path",
        help="可选：手动指定已有总结 markdown 路径。若不提供，则默认使用当前最新财报公告日对应的 financial_reports/YYYYMMDD.md",
    )
    parser.add_argument(
        "--require-deep-research",
        action="store_true",
        help="注册前强制检查 draft/challenge/revision/closure 等季度深研过程产物。",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    symbol_info = parse_symbol(args.symbol)
    latest_report, previous_report = select_latest_two_reports(symbol_info.symbol)
    if latest_report is None:
        raise SystemExit(f"未找到 {symbol_info.symbol} 的最新财报公告，无法登记。")

    summary_path = Path(args.path) if args.path else _output_path(symbol_info.symbol, latest_report.date)
    if not _is_meaningful_summary(summary_path):
        raise SystemExit(
            "目标财报分析文档不存在、为空、或疑似只是未来公告占位文件，已拒绝登记到 summary_index.json。"
        )
    if args.require_deep_research:
        quality_errors = validate_deep_research_artifacts(
            manifest_path=financial_report_workdir(symbol_info.symbol) / "manifest.json",
            final_report_path=summary_path,
        )
        if quality_errors:
            detail = "\n".join(f"- {item}" for item in quality_errors)
            raise SystemExit(f"季度深研质量门禁未通过，拒绝登记：\n{detail}")

    update_summary_index(
        symbol=symbol_info.symbol,
        stock_name=symbol_info.stock_name,
        latest_report=latest_report,
        previous_report=previous_report,
        output_path=summary_path,
    )

    print(f"已登记: {symbol_info.symbol} -> {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
