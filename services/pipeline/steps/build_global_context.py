"""Step 2: build the shared global context markdown."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from services.research.macro_summary import get_macro_summary
from services.research.stock_analysis import analyze_stock_dynamics_and_valuation
from services.selection_system.store import load_json_file


PROJECT_ROOT = Path(__file__).resolve().parents[3]
INDEX_SYMBOL = "000001.IDX"


def _latest_deep_scan_per_symbol(
    signature: str,
    symbols: Iterable[str],
) -> List[Dict[str, Any]]:
    """按 symbol 提取该账本 decision_summary 中最近一次【深度分析】记录。

    返回顺序与入参 `symbols` 一致；对于没有历史深度记录的 symbol 直接跳过，不返回空行。
    """
    if not signature:
        return []
    symbol_list = [s for s in symbols if isinstance(s, str) and s]
    if not symbol_list:
        return []
    summary_path = PROJECT_ROOT / "data" / "agent_data" / signature / "decision_summary.json"
    if not summary_path.exists():
        return []
    records = load_json_file(summary_path, default=[]) or []
    if not isinstance(records, list):
        return []

    target_set = set(symbol_list)
    best: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        sym = rec.get("stock_code") or rec.get("symbol")
        if not isinstance(sym, str) or sym not in target_set:
            continue
        analysis_type = rec.get("analysis_type") or ""
        if "深度分析" not in analysis_type:
            continue
        end_date = rec.get("end_date") or ""
        prev = best.get(sym)
        if prev is None or (end_date and end_date > (prev.get("end_date") or "")):
            best[sym] = rec
    return [best[sym] for sym in symbol_list if sym in best]


def _format_number(value: Any) -> str:
    if value in (None, "", 0, 0.0):
        return "-"
    if isinstance(value, (int, float)):
        if float(value) == 0:
            return "-"
        return f"{value:g}"
    return str(value)


def render_yesterday_digest(records: List[Dict[str, Any]]) -> str:
    if not records:
        return ""
    lines = [
        "## 3. 昨日账本轻量摘要（yesterday_digest）",
        "",
        "> 用途：Step 2 建立 P0/P1/P2 优先级队列、执行防饥饿机制时的轻量参考。",
        "> - `last_deep_scan_date` 为该股最近一次【深度分析】的分析日期（用于防饥饿判断，连续 >10 天未深度分析需强制升到 P0）。",
        "> - `last_action_type` 为最近一次分析的最终动作，仅作延续性参考。",
        "> - `price_target` / `stop_loss` 为最近一次估值锚点；是否重锚由当日 Step 3 重新判断，本表不替代单股 `_research.md` 中的完整历史交易总结。",
        "",
        "| symbol | stock_name | last_deep_scan_date | last_action_type | price_target | stop_loss |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for rec in records:
        sym = rec.get("stock_code") or rec.get("symbol") or "-"
        name = rec.get("stock_name") or "-"
        deep_date = rec.get("end_date") or "-"
        action = rec.get("action_type") or "-"
        lines.append(
            f"| {sym} | {name} | {deep_date} | {action} "
            f"| {_format_number(rec.get('price_target'))} "
            f"| {_format_number(rec.get('stop_loss'))} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_global_context(
    run_date: str,
    *,
    signature: str = "",
    symbols: Iterable[str] = (),
) -> str:
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

    digest_records = _latest_deep_scan_per_symbol(signature, symbols)
    digest_section = render_yesterday_digest(digest_records)
    if digest_section:
        sections.append(digest_section)
    return "\n".join(sections)


def write_global_context(
    run_date: str,
    output_dir: str | Path,
    *,
    signature: str = "",
    symbols: Iterable[str] = (),
) -> Path:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / "01_global_context.md"
    output_path.write_text(
        build_global_context(run_date, signature=signature, symbols=symbols),
        encoding="utf-8",
    )
    return output_path
