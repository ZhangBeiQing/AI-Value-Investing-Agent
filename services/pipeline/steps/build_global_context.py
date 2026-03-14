"""Step 2: build the shared global context markdown."""

from __future__ import annotations

import json
from pathlib import Path

from services.research.macro_summary import get_macro_summary
from services.research.stock_analysis import analyze_stock_dynamics_and_valuation


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


def write_global_context(run_date: str, output_dir: str | Path) -> Path:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / "01_global_context.md"
    output_path.write_text(build_global_context(run_date), encoding="utf-8")
    return output_path

