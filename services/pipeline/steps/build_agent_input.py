"""Step 4: build basic snapshot and agent input markdown."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from configs.stock_pool import TRACKED_A_STOCKS
from services.prompting.system_prompt import get_skill_system_prompt
from services.snapshot.basic_snapshot import build_basic_snapshot


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROMPT_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "skill_flow.json"


def resolve_signature(raw_signature: str) -> str:
    if raw_signature:
        return raw_signature
    if os.environ.get("SIGNATURE"):
        return os.environ["SIGNATURE"]
    return os.environ.get("DEFAULT_SIGNATURE", "deepseek-reasoner")


def build_user_query(research_files: List[Path], run_date: str) -> str:
    lines = [
        f"今天是 {run_date} 早上，股市还没开盘。",
        "请遵循 SYSTEM_PROMPT 的规范和价值投资原则，基于已提供的输入文件进行分析并决策今日的持仓调整。",
        "",
        "【强制输出（在任何分析之前）】",
        "1) 用你自己的话复述你对 configs/prompt_flow/skill_flow.json 股票分析流程与关键规则的理解。",
        "2) 从输入信息中提取并输出：当前总资金、可用现金、持仓明细（股票代码+股数）。",
        "",
        "## 外部输入文件（请按顺序打开）",
        "",
        "在完成 USER_QUERY 的两条强制输出前，不要打开任何外部文件。",
        "",
        "- 02_basic_snapshot_payload.json（《基本面数据概览》：basic_stock_info 生成的 basic snapshot；用于 Step 0 快速扫描与定价基准）",
        "- 01_global_context.md（宏观/上证/渐进式新闻总结）",
        "",
        "",
        "## 个股研究包索引（按需逐只打开，禁止批量读取）",
        "",
    ]
    for file_path in research_files:
        lines.append(f"- {file_path.name}")
    lines.extend(["", "> 需要深度分析的股票请逐只打开对应研究包文件阅读。"])
    return "\n".join(lines)


def build_agent_input(run_date: str, signature: str, prompt_config: str | Path, output_dir: str | Path) -> str:
    prompt_path = Path(prompt_config)
    system_prompt = get_skill_system_prompt(run_date, signature, prompt_path)
    research_files = sorted((Path(output_dir) / "04_stock_research").glob("*_research.md"))
    generated_at = datetime.now()

    sections = [
        "# Agent 输入",
        "",
        f"- 生成日期: {run_date}",
        f"- 生成时间: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- PROMPT_FLOW_CONFIG: {prompt_path.resolve()}",
        f"- SIGNATURE: {signature}",
        "",
        "## SYSTEM_PROMPT",
        "",
        "```text",
        system_prompt,
        "```",
        "",
        "## USER_QUERY",
        "",
        build_user_query(research_files, run_date),
        "",
    ]
    return "\n".join(sections)


def build_snapshot_payload(run_date: str) -> Dict[str, Any]:
    symbols = [entry.symbol for entry in TRACKED_A_STOCKS]
    return build_basic_snapshot(symbols, run_date)


def write_agent_input_bundle(
    run_date: str,
    output_dir: str | Path,
    *,
    signature: str = "",
    prompt_config: str | Path | None = None,
) -> None:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    resolved_signature = resolve_signature(signature)
    resolved_prompt_config = Path(prompt_config) if prompt_config else DEFAULT_PROMPT_CONFIG

    snapshot_payload = build_snapshot_payload(run_date)
    (target_dir / "02_basic_snapshot_payload.json").write_text(
        json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    agent_input = build_agent_input(run_date, resolved_signature, resolved_prompt_config, target_dir)
    (target_dir / "03_agent_input.md").write_text(agent_input, encoding="utf-8")
