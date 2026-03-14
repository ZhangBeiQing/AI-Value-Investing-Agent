"""Step 3: build stock research packages."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List

from configs.stock_pool import TRACKED_A_STOCKS
from services.research.financial_report import get_financial_report_summary
from services.research.news_summary import search_stock_news
from services.research.stock_analysis import analyze_stock_dynamics_and_valuation
from utlity import ensure_stock_subdir, get_stock_data_dir, parse_symbol


def _json_block(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _format_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return _json_block(value)
    return str(value)


def _format_news_item(item: dict) -> List[str]:
    title = item.get("title", "未命名")
    dt = item.get("datetime", "")
    category = item.get("category", "")
    impact = item.get("impact_level", "")
    sentiment = item.get("sentiment", "")

    header_parts = []
    if category:
        header_parts.append(f"category: {category}")
    if impact:
        header_parts.append(f"impact: {impact}")
    if sentiment:
        header_parts.append(f"sentiment: {sentiment}")
    header_suffix = (" [" + " / ".join(header_parts) + "]") if header_parts else ""

    lines = [f"- **{title}** ({dt}){header_suffix}"]
    used = {"title", "datetime", "category", "impact_level", "sentiment"}
    preferred_order = [
        "summary",
        "validity_period",
        "audit_analysis",
        "financial_implication",
        "price_driver",
        "risk_warning",
        "source",
        "url",
        "link",
    ]
    for key in preferred_order:
        if key in item and item.get(key) not in (None, ""):
            lines.append(f"  - {key}: {_format_scalar(item.get(key))}")
            used.add(key)

    for key in sorted(k for k in item.keys() if k not in used):
        value = item.get(key)
        if value not in (None, ""):
            lines.append(f"  - {key}: {_format_scalar(value)}")

    return lines


def build_research_markdown(symbol: str, run_date: str) -> str:
    symbol_info = parse_symbol(symbol)
    stock_name = symbol_info.stock_name or symbol_info.symbol
    price_payload = analyze_stock_dynamics_and_valuation(symbol_info.symbol, run_date)
    news_raw = search_stock_news(symbol_info.symbol, run_date)
    financial_payload = get_financial_report_summary(symbol_info.symbol, run_date)

    try:
        news_payload = json.loads(news_raw)
    except Exception:
        news_payload = {"raw_text": news_raw}

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# {stock_name}（{symbol_info.symbol}）研究包")
    lines.append("")
    lines.append(f"- 请求日期: {run_date}")
    lines.append(f"- 生成时间: {generated_at}")
    lines.append("")
    lines.append("## 1. 股票指标与估值")
    lines.append("")
    lines.append("### 1.1 Price Report JSON")
    lines.append("```json")
    lines.append(_json_block(price_payload.get("price_report")))
    lines.append("```")
    lines.append("")
    lines.append("### 1.2 Valuation Report (Markdown)")
    lines.append(price_payload.get("valuation_report") or "> 无估值数据或标的不支持。")
    if price_payload.get("valuation_unavailable_reason"):
        lines.extend(["", f"> 说明：{price_payload['valuation_unavailable_reason']}"])
    lines.append("")
    lines.append("## 2. 新闻与公告")
    lines.append("")
    news_items = news_payload.get("news_items") or []
    if news_items:
        for item in news_items:
            if isinstance(item, dict):
                lines.extend(_format_news_item(item))
            else:
                lines.append(f"- {item}")
    else:
        lines.append("> 未找到新闻或工具返回空结果。")
    diagnostics = news_payload.get("diagnostics") or []
    if diagnostics:
        lines.extend(["", "诊断信息："])
        lines.extend(f"- {entry}" for entry in diagnostics)
    lines.append("")
    lines.append("## 3. 财报摘要、机构一致预期与 Forecast")
    lines.append("")
    if financial_payload.get("error"):
        lines.append(f"> 获取失败：{financial_payload['error']}")
    else:
        lines.append((financial_payload.get("content") or "").strip() or "> 未获取到财报内容。")
        metadata = financial_payload.get("metadata")
        if metadata:
            lines.extend(["", "**财报元数据**", "```json", _json_block(metadata), "```"])
    lines.append("")
    return "\n".join(lines)


def research_output_path(symbol: str, run_date: str, output_dir: str | Path) -> Path:
    symbol_info = parse_symbol(symbol)
    stock_root = get_stock_data_dir(symbol_info)
    ensure_stock_subdir(symbol_info, "financial_reports")
    ensure_stock_subdir(symbol_info, "forecast")
    filename = f"{stock_root.name}_{run_date}_research.md"
    target_dir = Path(output_dir) / "04_stock_research"
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / filename


def write_stock_research_bundle(
    run_date: str,
    output_dir: str | Path,
    symbols: Iterable[str] | None = None,
) -> None:
    target_symbols = list(symbols) if symbols is not None else [entry.symbol for entry in TRACKED_A_STOCKS]
    for symbol in target_symbols:
        content = build_research_markdown(symbol, run_date)
        research_output_path(symbol, run_date, output_dir).write_text(content, encoding="utf-8")

