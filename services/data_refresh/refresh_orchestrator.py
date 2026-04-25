"""一键刷新每日分析所需数据的编排层。

面向「早上 7 点起床，分析昨天收盘」的日常节奏：
- `--date` 语义统一为「要分析的交易日」（默认 today - 1 日历日）。
- 轻量档（默认）：强刷宏观 panel、行情快照、板块快照、新闻、选股输入等易变数据。
- 重量档（可选）：在轻量档基础上，额外强刷财报结构化数据等重缓存。
- 公告 PDF、历史日线、财报 PDF 等重缓存按各自增量逻辑走，不在此处强刷。
- 本编排层只跑 Python 脚本链路，宏观/新闻/选股/财报/交易等 skill 由最终清单提示人工触发。
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Sequence

from core.logging import get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER = get_logger("RefreshOrchestrator")


def _load_daily_refresh_symbols(base_dir: str = "data") -> List[str]:
    """合并 TRACKED_A_STOCKS ∪ master_universe，得到"今日应刷新股票清单"。

    设计归属：本编排层是**每日刷新策略的唯一决策源**——回答"今天哪些股票
    需要 fresh 的价格 / 财报 / basic_info 数据"。下游 `selection_system`
    只做缓存消费者，不再承担 universe 的主刷新责任。
    """
    from configs.stock_pool import TRACKED_A_STOCKS

    ordered: List[str] = []
    seen: set = set()
    for entry in TRACKED_A_STOCKS:
        if entry.symbol and entry.symbol not in seen:
            ordered.append(entry.symbol)
            seen.add(entry.symbol)

    universe_path = PROJECT_ROOT / base_dir / "universe" / "master_universe.json"
    if universe_path.exists():
        try:
            payload = json.loads(universe_path.read_text(encoding="utf-8"))
            for stock in payload.get("stocks") or []:
                symbol = (stock or {}).get("symbol")
                if isinstance(symbol, str) and symbol and symbol not in seen:
                    ordered.append(symbol)
                    seen.add(symbol)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("读取 master_universe.json 失败，仅使用 TRACKED 列表: %s", exc)
    else:
        LOGGER.warning("未找到 master_universe.json，仅使用 TRACKED 列表: %s", universe_path)

    return ordered


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


def _clear_research_artifact_cache(run_date: str) -> StepResult:
    """清理 `data/research_artifact_cache/{run_date}/`。

    研究产物缓存（services/pipeline/steps/build_stock_research.py）的
    input_fingerprint 不感知 prices 缓存的更新。一键刷新一旦执行就视为
    上游数据已变，因此在所有数据刷新步骤之后无条件清理这层缓存，让
    下一次 run_daily_pipeline 必然基于最新价格/财报/新闻重建 04 产物。
    """
    name = "clear_research_artifact_cache"
    start = time.time()
    cache_dir = PROJECT_ROOT / "data" / "research_artifact_cache" / run_date
    command = ["rm", "-rf", str(cache_dir)]
    LOGGER.info("▶ [%s] %s", name, " ".join(command))
    try:
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            message = "已清理过期 artifact 缓存"
        else:
            message = "缓存目录不存在，跳过"
    except OSError as exc:
        duration = round(time.time() - start, 2)
        LOGGER.error("✘ [%s] 失败 (%s, %.1fs)", name, exc, duration)
        return StepResult(
            name=name,
            command=command,
            status="failed",
            duration_sec=duration,
            message=str(exc),
        )
    duration = round(time.time() - start, 2)
    LOGGER.info("✓ [%s] 完成 (%.1fs) — %s", name, duration, message)
    return StepResult(
        name=name,
        command=command,
        status="success",
        duration_sec=duration,
        message=message,
    )


def _manage_daily_data_cmd(
    run_date: str,
    *,
    fresh_heavy: bool,
    max_workers: int,
    look_back_days: int,
    signature: str,
    symbols: Sequence[str],
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
        "--skip-disclosures",
    ]
    if fresh_heavy:
        cmd.append("--force-refresh")
    if signature:
        cmd.extend(["--signature", signature])
    cmd.extend(["--symbols", *symbols])
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
    max_workers: int = 8,
    look_back_days: int = 0,
    signature: str = "",
    base_dir: str = "data",
    stop_on_failure: bool = True,
) -> OrchestratorResult:
    """按固定顺序刷新每日分析所需数据。

    **策略归属**：本编排层是"每日 fresh 数据"策略的唯一决策源。它决定：
    - 每日应刷新的股票范围 = TRACKED_A_STOCKS ∪ master_universe（约 11 + 105 ≈ 116 只）
    - 价格 / 财报结构化 / basic_info 由 manage_daily_data 负责
    - 公告由 selection_system.build-announcements 单独负责（增量 + audit），
      因此 manage_daily_data 以 --skip-disclosures 跳过重复扫描

    顺序依赖解释：
    1. manage_daily_data：宏观客观面板 + 行情快照 + basic_stock_info（TRACKED ∪ universe），
       是后续所有模块的上游。
    2. run-news：当日新闻抓取/去重/富化，产出 03_news_prompt_input.json。
    3. run-signals：板块变动 + 个股热度，产出 05_market_signals 基础数据。
    4. build-board-heat-state：基于 3 的快照合成 05_board_heat_digest/state（force 刷新）。
    5. build-announcements：聚合主 universe 最近公告 → 04_recent_company_announcements.json。
    6. build-shared-context：整合 03/04/05 → 07_shared_selection_context.md。
    7. build-candidate-pools：产出 08/09 选股输入，供选股 skill 消费。
    """
    result = OrchestratorResult(run_date=run_date, fresh_heavy=fresh_heavy)

    refresh_symbols = _load_daily_refresh_symbols(base_dir=base_dir)
    LOGGER.info(
        "本次一键刷新覆盖 %d 只股票（TRACKED_A_STOCKS ∪ master_universe）",
        len(refresh_symbols),
    )

    steps: List[tuple[str, List[str]]] = [
        (
            "manage_daily_data",
            _manage_daily_data_cmd(
                run_date,
                fresh_heavy=fresh_heavy,
                max_workers=max_workers,
                look_back_days=look_back_days,
                signature=signature,
                symbols=refresh_symbols,
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

    # 数据刷新一旦发生（即使中途失败），下游研究产物缓存都视为过期，
    # 必须清掉以保证下一次 run_daily_pipeline 重建 04 产物。
    cleanup_result = _clear_research_artifact_cache(run_date)
    result.steps.append(cleanup_result)
    if cleanup_result.status == "failed" and result.overall_status != "failed":
        result.overall_status = "failed"

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
