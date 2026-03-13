#!/usr/bin/env python3
"""Step 2 for the skill pipeline: generate 01_global_context.md."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agent_tools.tool_macro_summary import get_macro_summary as _tool_get_macro_summary
from agent_tools.tool_stock_analysis import (
    analyze_stock_dynamics_and_valuation as _tool_analyze_stock_dynamics_and_valuation,
)


get_macro_summary = getattr(_tool_get_macro_summary, "fn", _tool_get_macro_summary)
analyze_stock_dynamics_and_valuation = getattr(
    _tool_analyze_stock_dynamics_and_valuation,
    "fn",
    _tool_analyze_stock_dynamics_and_valuation,
)

INDEX_SYMBOL = "000001.IDX"


def build_global_context(run_date: str) -> str:
    macro_text = get_macro_summary(today_time=run_date)
    index_payload = analyze_stock_dynamics_and_valuation(INDEX_SYMBOL, run_date)

    sections = [
        "# 全局宏观与上证指数上下文",
        "",
        "## 1. 宏观总结",
        "",
        macro_text.strip() if isinstance(macro_text, str) else str(macro_text),
        "",
        "## 2. 上证指数分析",
        "",
        "### 2.1 Price Report JSON",
        "```json",
        json.dumps(index_payload.get("price_report"), ensure_ascii=False, indent=2),
        "```",
        "",
        "### 2.2 Valuation Report (Markdown)",
        index_payload.get("valuation_report") or "> 无估值数据或标的不支持。",
    ]
    reason = index_payload.get("valuation_unavailable_reason")
    if reason:
        sections.extend(["", f"> 说明：{reason}"])
    sections.append("")
    return "\n".join(sections)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build 01_global_context.md")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--signature", default="")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    content = build_global_context(args.run_date)
    (output_dir / "01_global_context.md").write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
