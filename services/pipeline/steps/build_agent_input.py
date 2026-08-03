"""Step 4: build basic snapshot and agent input markdown."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from services.prompting.system_prompt import (
    get_skill_prompt_context,
    get_skill_system_prompt,
)
from services.snapshot.basic_snapshot import build_basic_snapshot
from utlity.stock_utils import parse_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROMPT_CONFIG = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "main_policy.md"
)
FIXED_INVESTMENT_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "investment_policy.md"
)
FIXED_STOCK_ANALYSIS_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "stock_analysis_policy.md"
)
WEB_RESEARCH_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "research"
    / "web_research_policy.md"
)


def resolve_signature(raw_signature: str) -> str:
    if raw_signature:
        return raw_signature
    if os.environ.get("SIGNATURE"):
        return os.environ["SIGNATURE"]
    return os.environ.get("DEFAULT_SIGNATURE", "deepseek-reasoner")


def _build_stock_pool_block(symbols: Iterable[str]) -> str:
    lines = []
    for idx, symbol in enumerate(symbols, start=1):
        info = parse_symbol(symbol)
        lines.append(f"{idx}. {symbol} {info.stock_name or symbol}")
    return "\n".join(lines)


def build_user_query(research_files: List[Path], run_date: str, book_type: str) -> str:
    lines = [
        f"今天是 {run_date} 晚上，股市已经收盘。你需要复盘今天的股票，然后为明天的交易做决策",
        f"当前分析账本为 `{book_type}`。你只能基于当前账本的持仓、研究包和历史交易总结做决策，不能把其他账本的仓位或锚点混入本账本。",
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
        "- 02_basic_snapshot_payload.json（《基本面数据概览》：basic_stock_info 生成的 basic snapshot；仅供主agent在 Step 0 做全组合快扫与定价基准）",
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


def build_fixed_main_user_query(
    research_files: List[Path],
    run_date: str,
) -> str:
    """Build the fixed_tracked main-agent-only runtime input."""
    lines = [
        f"分析日期为 `{run_date}`，收盘数据已经产生。当前账本为 `fixed_tracked`。",
        "本文件只供主 Agent 做组合扫描、系统风险判断和 P0 队列选择。",
        "人工确认点和后续 Agent 调度只服从 auto-trading-fixed-tracked Skill；本 USER_QUERY 不另定义状态机。",
        "不要读取个股研究包，不要在派发给个股角色的 prompt 中复述或添加分析结论。",
        "",
        "## 主 Agent 外部输入",
        "",
        "- 02_basic_snapshot_payload.json",
        "- 01_global_context.md",
        f"- data/selection_runs/{run_date}/06_hot_news_state.json（可选）",
        f"- data/selection_runs/{run_date}/05_board_heat_digest.json（可选）",
        "- data/skill_runs/_analysis_index.json（不存在时按冷启动处理）",
        "",
        "## 个股研究包索引（仅用于确认文件存在，主 Agent 不打开内容）",
        "",
    ]
    for file_path in research_files:
        lines.append(f"- {file_path.name}")
    return "\n".join(lines)


def build_agent_input(
    run_date: str,
    signature: str,
    prompt_config: str | Path,
    output_dir: str | Path,
    *,
    stock_codes: Optional[List[str]] = None,
    book_type: str = "fixed_tracked",
    prompt_context: Optional[Dict[str, str]] = None,
) -> str:
    prompt_path = Path(prompt_config)
    target_symbols = stock_codes or []
    role_prompt = get_skill_system_prompt(
        run_date,
        signature,
        prompt_path,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    if book_type == "fixed_tracked":
        shared_policy = get_skill_system_prompt(
            run_date,
            signature,
            FIXED_INVESTMENT_POLICY,
            stock_codes=target_symbols,
            stock_pool_block_override=_build_stock_pool_block(target_symbols),
            prompt_context=prompt_context,
        )
        system_prompt = f"{shared_policy}\n\n---\n\n{role_prompt}"
    else:
        system_prompt = role_prompt
    research_files = sorted((Path(output_dir) / "04_stock_research").glob("*_research.md"))
    generated_at = datetime.now()

    system_prompt_section = (
        [system_prompt]
        if prompt_path.suffix.lower() == ".md"
        else ["```text", system_prompt, "```"]
    )
    sections = [
        "# Agent 输入",
        "",
        f"- 生成日期: {run_date}",
        f"- 生成时间: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- BOOK_TYPE: {book_type}",
        f"- PROMPT_FLOW_CONFIG: {prompt_path.resolve()}",
        *(
            [f"- SHARED_POLICY_SOURCE: {FIXED_INVESTMENT_POLICY.resolve()}"]
            if book_type == "fixed_tracked"
            else []
        ),
        f"- SIGNATURE: {signature}",
        "",
        "## SYSTEM_PROMPT",
        "",
        *system_prompt_section,
        "",
        "## USER_QUERY",
        "",
        (
            build_fixed_main_user_query(research_files, run_date)
            if book_type == "fixed_tracked"
            else build_user_query(research_files, run_date, book_type)
        ),
        "",
    ]
    return "\n".join(sections)


def build_stock_analysis_input(
    run_date: str,
    signature: str,
    *,
    stock_codes: Optional[List[str]] = None,
    prompt_context: Optional[Dict[str, str]] = None,
) -> str:
    """Build the common fixed_tracked input read by stock debate roles."""
    target_symbols = stock_codes or []
    shared_policy = get_skill_system_prompt(
        run_date,
        signature,
        FIXED_INVESTMENT_POLICY,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    research_policy = get_skill_system_prompt(
        run_date,
        signature,
        FIXED_STOCK_ANALYSIS_POLICY,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    web_research_policy = get_skill_system_prompt(
        run_date,
        signature,
        WEB_RESEARCH_POLICY,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    common_policy = (
        f"{shared_policy}\n\n---\n\n"
        f"{web_research_policy}\n\n---\n\n"
        f"{research_policy}"
    )
    generated_at = datetime.now()
    sections = [
        "# fixed_tracked 个股共同分析输入",
        "",
        f"- 生成日期: {run_date}",
        f"- 生成时间: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- SHARED_POLICY_SOURCE: {FIXED_INVESTMENT_POLICY.resolve()}",
        f"- WEB_RESEARCH_POLICY_SOURCE: {WEB_RESEARCH_POLICY.resolve()}",
        f"- RESEARCH_POLICY_SOURCE: {FIXED_STOCK_ANALYSIS_POLICY.resolve()}",
        f"- SIGNATURE: {signature}",
        "",
        common_policy,
        "",
        "## 角色规则",
        "",
        "本文件不定义 Bull、Bear、Rebuttal、Juror 或 finalizer 的具体职责。",
        "每个角色必须另外直接读取主 Agent 指定的 Skill reference，不得由主 Agent复述。",
        "",
    ]
    return "\n".join(sections)


def build_snapshot_payload(
    run_date: str,
    symbols: List[str],
    *,
    max_workers: int = 1,
) -> Dict[str, Any]:
    return build_basic_snapshot(symbols, run_date, max_workers=max_workers)


def write_agent_input_bundle(
    run_date: str,
    output_dir: str | Path,
    *,
    symbols: List[str],
    book_type: str = "fixed_tracked",
    signature: str = "",
    prompt_config: str | Path | None = None,
    snapshot_payload: Dict[str, Any] | None = None,
    max_workers: int = 1,
) -> None:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    resolved_signature = resolve_signature(signature)
    resolved_prompt_config = Path(prompt_config) if prompt_config else DEFAULT_PROMPT_CONFIG
    prompt_context = get_skill_prompt_context(
        run_date,
        resolved_signature,
        stock_codes=symbols,
    )

    snapshot_payload = snapshot_payload or build_snapshot_payload(
        run_date,
        symbols,
        max_workers=max_workers,
    )
    (target_dir / "02_basic_snapshot_payload.json").write_text(
        json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    agent_input = build_agent_input(
        run_date,
        resolved_signature,
        resolved_prompt_config,
        target_dir,
        stock_codes=symbols,
        book_type=book_type,
        prompt_context=prompt_context,
    )
    (target_dir / "03_agent_input.md").write_text(agent_input, encoding="utf-8")
    if book_type == "fixed_tracked":
        stock_analysis_input = build_stock_analysis_input(
            run_date,
            resolved_signature,
            stock_codes=symbols,
            prompt_context=prompt_context,
        )
        (target_dir / "03_stock_analysis_input.md").write_text(
            stock_analysis_input,
            encoding="utf-8",
        )
