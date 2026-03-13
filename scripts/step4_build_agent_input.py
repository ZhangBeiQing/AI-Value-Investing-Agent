#!/usr/bin/env python3
"""Step 4 for the skill pipeline: generate 02_basic_snapshot_payload.json and 03_agent_input.md."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from basic_stock_info import DEFAULT_PRICE_LOOKBACK_DAYS, basic_info
from configs.stock_pool import TRACKED_A_STOCKS
from prompts.agent_prompt import get_agent_system_prompt


DEFAULT_PROMPT_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "skill_flow.json"


def _load_default_signature() -> str:
    config_path = PROJECT_ROOT / "configs" / "default_config.json"
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return "deepseek-reasoner"

    models = payload.get("models") or []
    for model in models:
        if model.get("enabled", True):
            signature = model.get("signature")
            if isinstance(signature, str) and signature.strip():
                return signature.strip()
    return "deepseek-reasoner"


def _resolve_signature(raw_signature: str) -> str:
    if raw_signature:
        return raw_signature
    if os.environ.get("SIGNATURE"):
        return os.environ["SIGNATURE"]
    return _load_default_signature()


def _build_basic_snapshot(run_date: str) -> Dict[str, Any]:
    symbols = [entry.symbol for entry in TRACKED_A_STOCKS]
    return basic_info(
        symbols,
        today_time=run_date,
        price_lookback_days=DEFAULT_PRICE_LOOKBACK_DAYS,
        force_refresh=False,
        force_refresh_financials=False,
        use_cache=True,
    )


def _build_user_query(research_files: List[Path], run_date: str) -> str:
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


def build_agent_input(run_date: str, signature: str, prompt_config: Path, output_dir: Path) -> str:
    os.environ["PROMPT_FLOW_CONFIG"] = str(prompt_config.resolve())
    system_prompt = get_agent_system_prompt(run_date, signature)
    research_files = sorted((output_dir / "04_stock_research").glob("*_research.md"))
    generated_at = datetime.now()

    sections = [
        "# Agent 输入",
        "",
        f"- 生成日期: {run_date}",
        f"- 生成时间: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- PROMPT_FLOW_CONFIG: {prompt_config.resolve()}",
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
        _build_user_query(research_files, run_date),
        "",
    ]
    return "\n".join(sections)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build skill agent input files.")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--signature", default="")
    parser.add_argument("--prompt-config", default=os.environ.get("PROMPT_FLOW_CONFIG", str(DEFAULT_PROMPT_CONFIG)))
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    signature = _resolve_signature(args.signature)
    prompt_config = Path(args.prompt_config)

    snapshot_payload = _build_basic_snapshot(args.run_date)
    (output_dir / "02_basic_snapshot_payload.json").write_text(
        json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    agent_input = build_agent_input(args.run_date, signature, prompt_config, output_dir)
    (output_dir / "03_agent_input.md").write_text(agent_input, encoding="utf-8")


if __name__ == "__main__":
    main()
