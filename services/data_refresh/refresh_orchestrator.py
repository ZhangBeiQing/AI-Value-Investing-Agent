"""一键刷新每日分析所需数据的编排层。

面向「早上 7 点起床，分析昨天收盘」的日常节奏：
- `--date` 语义统一为「要分析的交易日」（默认 today - 1 日历日）。
- 轻量档（默认）：强刷宏观 panel、行情快照、板块快照、新闻、选股输入等易变数据。
- 重量档（可选）：在轻量档基础上，额外强刷财报结构化数据等重缓存。
- 公告 PDF、历史日线、财报 PDF 等重缓存按各自增量逻辑走，不在此处强刷。
- 本编排层只跑 Python 脚本链路，宏观/新闻/选股/财报/交易等 skill 由最终清单提示人工触发。
"""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Sequence

from core.logging import get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER = get_logger("RefreshOrchestrator")


@dataclass
class StepResult:
    name: str
    command: Sequence[str]
    status: str  # "success" | "failed" | "skipped"
    duration_sec: float
    message: str = ""


@dataclass
class OrchestratorResult:
    run_date: str
    fresh_heavy: bool
    steps: List[StepResult] = field(default_factory=list)
    overall_status: str = "success"

    @property
    def succeeded(self) -> bool:
        return self.overall_status == "success"


def _run_step(name: str, command: Sequence[str]) -> StepResult:
    start = time.time()
    LOGGER.info("▶ [%s] %s", name, " ".join(command))
    try:
        subprocess.run(command, check=True, cwd=str(PROJECT_ROOT))
    except subprocess.CalledProcessError as exc:
        duration = round(time.time() - start, 2)
        LOGGER.error("✘ [%s] 失败 (exit=%s, %.1fs)", name, exc.returncode, duration)
        return StepResult(
            name=name,
            command=list(command),
            status="failed",
            duration_sec=duration,
            message=f"exit={exc.returncode}",
        )
    duration = round(time.time() - start, 2)
    LOGGER.info("✓ [%s] 完成 (%.1fs)", name, duration)
    return StepResult(name=name, command=list(command), status="success", duration_sec=duration)


def _manage_daily_data_cmd(
    run_date: str,
    *,
    fresh_heavy: bool,
    max_workers: int,
    look_back_days: int,
    signature: str,
) -> List[str]:
    cmd = [
        sys.executable,
        "scripts/manage_daily_data.py",
        "--date",
        run_date,
        "--max-workers",
        str(max_workers),
        "--look-back-days",
        str(look_back_days),
        "--force-refresh-price",
    ]
    if fresh_heavy:
        cmd.append("--force-refresh")
    if signature:
        cmd.extend(["--signature", signature])
    return cmd


def _selection_cmd(subcommand: str, run_date: str, *extra: str, base_dir: str = "data") -> List[str]:
    return [
        sys.executable,
        "scripts/manage_selection_system.py",
        "--base-dir",
        base_dir,
        subcommand,
        "--date",
        run_date,
        *extra,
    ]


def run_refresh_pipeline(
    run_date: str,
    *,
    fresh_heavy: bool = False,
    max_workers: int = 4,
    look_back_days: int = 0,
    signature: str = "",
    base_dir: str = "data",
    stop_on_failure: bool = True,
) -> OrchestratorResult:
    """按固定顺序刷新每日分析所需数据。

    顺序依赖解释：
    1. manage_daily_data：宏观客观面板 + 行情快照 + basic_stock_info + 公告同步，
       是后续所有模块的上游。
    2. run-news：当日新闻抓取/去重/富化，产出 03_news_prompt_input.json。
    3. run-signals：板块变动 + 个股热度，产出 05_market_signals 基础数据。
    4. build-board-heat-state：基于 3 的快照合成 05_board_heat_digest/state（force 刷新）。
    5. build-announcements：聚合主 universe 最近公告 → 04_recent_company_announcements.json。
    6. build-shared-context：整合 03/04/05 → 07_shared_selection_context.md。
    7. build-candidate-pools：产出 08/09 选股输入，供选股 skill 消费。
    """
    result = OrchestratorResult(run_date=run_date, fresh_heavy=fresh_heavy)

    steps: List[tuple[str, List[str]]] = [
        (
            "manage_daily_data",
            _manage_daily_data_cmd(
                run_date,
                fresh_heavy=fresh_heavy,
                max_workers=max_workers,
                look_back_days=look_back_days,
                signature=signature,
            ),
        ),
        (
            "selection.run-news",
            _selection_cmd("run-news", run_date, base_dir=base_dir),
        ),
        (
            "selection.run-signals",
            _selection_cmd("run-signals", run_date, base_dir=base_dir),
        ),
        (
            "selection.build-board-heat-state",
            _selection_cmd("build-board-heat-state", run_date, "--force-refresh", base_dir=base_dir),
        ),
        (
            "selection.build-announcements",
            _selection_cmd("build-announcements", run_date, base_dir=base_dir),
        ),
        (
            "selection.build-shared-context",
            _selection_cmd("build-shared-context", run_date, base_dir=base_dir),
        ),
        (
            "selection.build-candidate-pools",
            _selection_cmd("build-candidate-pools", run_date, base_dir=base_dir),
        ),
    ]

    for name, cmd in steps:
        step_result = _run_step(name, cmd)
        result.steps.append(step_result)
        if step_result.status == "failed":
            result.overall_status = "failed"
            if stop_on_failure:
                break

    return result


def format_followup_checklist(run_date: str) -> str:
    """打印后续需要人工触发的 skill / 脚本清单。

    这些动作要么依赖 LLM / 联网分析（skill），要么位于人工确认节点之后
    （交易执行与后处理），不纳入一键刷新流水线自动执行。
    """
    lines = [
        "",
        "=" * 72,
        f"数据刷新完成（交易日 {run_date}）。接下来请依次人工触发：",
        "=" * 72,
        "",
        "【分析 skill（需 LLM / 联网）】",
        "  1. /daily-macro-summary              → data/macro_economy/"
        + run_date.replace("-", "")
        + ".md",
        "  2. /gradual-hot-news-summary         → 06_hot_news_state.json",
        "  3. /auto-selection-daily-pipeline    → 08/09/10/11 候选池 & 深研队列",
        "",
        "【财报 skill（依赖选股深研队列）】",
        f"  4. python scripts/prepare_financial_report_skill.py --date {run_date} --mandate all --sync-first --json",
        "  5. /financial-report-summary         → 各股 financial_reports/*.md",
        "",
        "【三账本 01-04 产物】",
        f"  6. python scripts/run_daily_pipeline.py --date {run_date} --max-workers 6",
        "",
        "【三账本交易 skill（生成 05_decision.json 后人工确认）】",
        "  7. /auto-trading-fixed-tracked",
        "  8. /auto-trading-short-book",
        "  9. /auto-trading-long-book",
        "",
        "【人工确认后分别执行后处理】",
        f"  10. python scripts/run_post_trade.py --date {run_date} --book-type fixed_tracked --signature book-fixed_tracked",
        f"  11. python scripts/run_post_trade.py --date {run_date} --book-type short_book --signature book-short_book",
        f"  12. python scripts/run_post_trade.py --date {run_date} --book-type long_book --signature book-long_book",
        "=" * 72,
    ]
    return "\n".join(lines)


def summarize_result(result: OrchestratorResult) -> str:
    lines = [
        "",
        f"刷新结果汇总（date={result.run_date}, heavy={result.fresh_heavy}）：",
    ]
    for step in result.steps:
        marker = {"success": "✓", "failed": "✘", "skipped": "–"}.get(step.status, "?")
        extra = f" {step.message}" if step.message else ""
        lines.append(f"  {marker} {step.name:40s} {step.duration_sec:>7.1f}s{extra}")
    lines.append(f"总体状态: {result.overall_status}")
    return "\n".join(lines)
