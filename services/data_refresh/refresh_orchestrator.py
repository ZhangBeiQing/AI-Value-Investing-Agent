"""一键刷新每日分析所需数据的编排层。

面向「当天晚上 9 点，分析当天收盘，为下一交易日出预案」的日常节奏：
- `--date` 语义统一为「要分析的交易日」（默认 `today`，不做减一）。
- 轻量档（默认）：强刷宏观 panel、行情快照等易变数据。
- 重量档（可选）：在轻量档基础上，额外强刷财报结构化数据等重缓存。
- 公告 PDF、历史日线、财报 PDF 等重缓存按各自增量逻辑走，不在此处强刷。
- 本编排层只跑 Python 脚本链路，宏观/选股/财报/交易等 skill 由最终清单提示人工触发。
- 新闻采集与板块热度层受 `configs/feature_flags.py` 控制，当前默认**暂停**（见该文件说明）。
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

from configs.feature_flags import (
    board_heat_layer_enabled,
    factor_prefilter_layer_enabled,
    gradual_news_summary_enabled,
)
from core.logging import get_logger
from shared_data_access.market_calendar import inspect_market_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER = get_logger("RefreshOrchestrator")


def _load_daily_refresh_symbols(
    base_dir: str = "data",
    *,
    include_selection_universe: bool = False,
) -> List[str]:
    """得到"今日应刷新股票清单"。

    默认只刷新 `TRACKED_A_STOCKS`，以缩短固定股票池日常流程耗时。
    仅当 `include_selection_universe=True` 时，才恢复旧口径：
    `TRACKED_A_STOCKS ∪ master_universe`。
    """
    from configs.stock_pool import FIXED_TRACKED_US_SYMBOLS, TRACKED_A_STOCKS

    ordered: List[str] = []
    seen: set = set()
    for entry in TRACKED_A_STOCKS:
        if entry.symbol and entry.symbol not in seen:
            ordered.append(entry.symbol)
            seen.add(entry.symbol)

    # 固定股池内的美股标的与 A 股/港股一起刷新，共用同一条数据缓存链路。
    for symbol in FIXED_TRACKED_US_SYMBOLS:
        if symbol and symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)

    if not include_selection_universe:
        return ordered

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
    skipped_non_trading_date: bool = False

    @property
    def succeeded(self) -> bool:
        return self.overall_status in {"success", "skipped"}


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
    include_selection_universe: bool = False,
    generate_prefilter: bool = True,
    skip_news_boards: bool = False,
    allow_non_trading_date: bool = False,
) -> OrchestratorResult:
    """按固定顺序刷新每日分析所需数据。

    **策略归属**：本编排层是"每日 fresh 数据"策略的唯一决策源。它决定：
    - 默认每日应刷新的股票范围 = TRACKED_A_STOCKS
    - `generate_prefilter=True` 且 `ENABLE_FACTOR_PREFILTER_LAYER=True` 时，
      扩展至 master_universe 并生成量化初筛结果；当前该开关默认关闭，
      因此刷新范围就是 `TRACKED_A_STOCKS`
    - `include_selection_universe=True` 时，扩展至 master_universe 并启用完整选股管线
    - 价格 / 财报结构化 / basic_info 由 manage_daily_data 负责
    - 新闻采集 / 板块热度 受 `configs/feature_flags.py` 控制；
      当前 `ENABLE_GRADUAL_NEWS_SUMMARY=False`、`ENABLE_BOARD_HEAT_LAYER=False`，
      因此默认不执行这两步；`skip_news_boards=True` 可显式跳过
    - 公告由 selection_system.build-announcements 单独负责（增量 + audit），
      因此 manage_daily_data 以 --skip-disclosures 跳过重复扫描
    - 选股系统（信号/公告/共享上下文/候选池）仅在 `include_selection_universe=True` 时启用

    顺序依赖解释：
    1. manage_daily_data：宏观客观面板 + 行情快照 + basic_stock_info
    2. selection.run-news：全市场新闻采集/去重/增强 → 03_news_prompt_input.json
       （仅在 `ENABLE_GRADUAL_NEWS_SUMMARY=True` 时执行）
    3. selection.build-board-heat-state：板块热度分析 → 05_board_heat_state/digest.json
       （仅在 `ENABLE_BOARD_HEAT_LAYER=True` 时执行）
    4. generate_prefilter 或 include_selection_universe 根据参数选择后续步骤。
    """
    result = OrchestratorResult(run_date=run_date, fresh_heavy=fresh_heavy)
    session = inspect_market_session(
        run_date,
        market="CN",
        base_dir=base_dir,
    )
    if not session.is_trading_day and not allow_non_trading_date:
        message = (
            f"{run_date} 不是 A 股交易日，跳过全部刷新；"
            f"previous={session.previous_trading_day}, "
            f"next={session.next_trading_day}, source={session.source}"
        )
        LOGGER.info(message)
        result.overall_status = "skipped"
        result.skipped_non_trading_date = True
        result.steps.append(
            StepResult(
                name="trading_day_guard",
                command=[],
                status="skipped",
                duration_sec=0.0,
                message=message,
            )
        )
        return result
    if not session.is_trading_day:
        message = (
            f"已显式允许非交易日分析：date={run_date}, "
            f"previous={session.previous_trading_day}, "
            f"next={session.next_trading_day}, source={session.source}；"
            "本次只刷新研究与预案输入，不表示该日可以成交"
        )
        LOGGER.warning(message)
        result.steps.append(
            StepResult(
                name="trading_day_guard",
                command=[],
                status="success",
                duration_sec=0.0,
                message=message,
            )
        )

    # 仅显式启用完整选股管线时才扩展到 master_universe。
    # 2026-11-05 起 generate_prefilter 不再隐式扩池，避免每日刷 234 只股票。
    expand_universe = include_selection_universe

    refresh_symbols = _load_daily_refresh_symbols(
        base_dir=base_dir,
        include_selection_universe=expand_universe,
    )
    if expand_universe:
        LOGGER.info(
            "本次一键刷新覆盖 %d 只股票（TRACKED_A_STOCKS ∪ master_universe）",
            len(refresh_symbols),
        )
    else:
        LOGGER.info(
            "本次一键刷新覆盖 %d 只固定股票池股票（TRACKED_A_STOCKS）",
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
    ]

    news_enabled = gradual_news_summary_enabled()
    board_heat_enabled = board_heat_layer_enabled()
    if skip_news_boards or not (news_enabled or board_heat_enabled):
        LOGGER.info(
            "跳过新闻采集 / 板块热度层: skip_news_boards=%s news=%s board_heat=%s",
            skip_news_boards,
            news_enabled,
            board_heat_enabled,
        )
    if news_enabled and not skip_news_boards:
        steps.append(
            (
                "selection.run-news",
                _selection_cmd("run-news", run_date, base_dir=base_dir),
            )
        )
    if board_heat_enabled and not skip_news_boards:
        steps.append(
            (
                "selection.build-board-heat-state",
                _selection_cmd("build-board-heat-state", run_date, "--force-refresh", base_dir=base_dir),
            )
        )

    prefilter_enabled = factor_prefilter_layer_enabled() and generate_prefilter
    if not prefilter_enabled:
        LOGGER.info(
            "跳过因子库/因子评分/量化初筛: feature_flag=%s generate_prefilter=%s",
            factor_prefilter_layer_enabled(),
            generate_prefilter,
        )
    if prefilter_enabled:
        steps.extend([
            (
                "selection.build-factor-store",
                _selection_cmd("build-factor-store", run_date, base_dir=base_dir),
            ),
            (
                "selection.build-factor-scores",
                _selection_cmd("build-factor-scores", run_date, base_dir=base_dir),
            ),
            (
                "selection.build-quant-prefilter",
                _selection_cmd("build-quant-prefilter", run_date, base_dir=base_dir),
            ),
        ])
    elif include_selection_universe:
        steps.extend([
            (
                "selection.run-signals",
                _selection_cmd("run-signals", run_date, base_dir=base_dir),
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
        ])

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


def _skill_checklist_lines(run_date: str) -> list[str]:
    """后续需要人工触发的 skill 清单（受 feature_flags 影响）。

    改用无编号的 `- ` 列表：新闻总结一路是条件项，编号会在两种口径下错位。
    """

    lines = [
        "【宏观总结 skill（需 LLM / 联网）】",
        f"  - /daily-macro-summary              → data/macro_economy/{run_date.replace('-', '')}.md",
    ]
    if gradual_news_summary_enabled():
        lines.append(
            f"  - /gradual-hot-news-summary         → data/selection_runs/{run_date}/06_hot_news_state.json"
        )
    return lines


def format_followup_checklist(
    run_date: str,
    *,
    include_selection_universe: bool = False,
    allow_non_trading_date: bool = False,
) -> str:
    """打印后续需要人工触发的 skill / 脚本清单。"""

    non_trading_arg = " --allow-non-trading-date" if allow_non_trading_date else ""
    date_label = "分析日期" if allow_non_trading_date else "交易日"
    non_trading_notice = (
        "注意：当前为休市日，只生成研究与下一交易日预案；"
        "不要在休市日执行 run_post_trade。"
        if allow_non_trading_date
        else ""
    )
    paused_note = (
        "已暂停：渐进式新闻总结与 05_board_heat_* 板块热度层（configs/feature_flags.py）。"
        if not (gradual_news_summary_enabled() or board_heat_layer_enabled())
        else ""
    )

    if include_selection_universe:
        lines = [
            "",
            "=" * 72,
            f"数据刷新完成（{date_label} {run_date}）。接下来请依次人工触发：",
            "=" * 72,
            non_trading_notice,
            paused_note,
            "",
            *_skill_checklist_lines(run_date),
            "  - /auto-selection-daily-pipeline    → 08/09/10/11 候选池 & 深研队列",
            "",
            "【财报 skill（依赖选股深研队列）】",
            f"  - python scripts/prepare_financial_report_skill.py --date {run_date} --mandate all --sync-first --json --include-queue",
            "  - /financial-report-summary         → 各股 financial_reports/*.md",
            "",
            "【综合 fixed_tracked 01-04 产物】",
            f"  - python scripts/run_daily_pipeline.py --date {run_date} --max-workers 6{non_trading_arg}",
            "",
            "【固定股池交易 skill（生成 05_decision.json 后人工确认）】",
            "  - /auto-trading-fixed-tracked",
            "",
            "【人工确认后执行后处理】",
            f"  - python scripts/run_post_trade.py --date {run_date} --book-type fixed_tracked --signature book-fixed_tracked",
            "=" * 72,
        ]
        return "\n".join(line for line in lines if line is not None)

    lines = [
        "",
        "=" * 72,
        f"数据刷新 & 量化初筛完成（{date_label} {run_date}）。接下来请依次人工触发：",
        "=" * 72,
        non_trading_notice,
        paused_note,
        "",
        *_skill_checklist_lines(run_date),
        "",
        "【财报准备与总结（fixed_tracked + 量化初筛短期股）】",
        f"  - python scripts/prepare_financial_report_skill.py --date {run_date} --sync-first --json",
        "  - /financial-report-summary         → 各股 financial_reports/*.md",
        "",
        "【综合 fixed_tracked 01-04 产物】",
        f"  - python scripts/run_daily_pipeline.py --date {run_date} --max-workers 6{non_trading_arg}",
        "",
        "【固定股池交易 skill（生成 05_decision.json 后人工确认）】",
        "  - /auto-trading-fixed-tracked",
        "",
        "【人工确认后执行后处理】",
        f"  - python scripts/run_post_trade.py --date {run_date} --book-type fixed_tracked --signature book-fixed_tracked",
        "",
        "说明：每日只生成 TRACKED_A_STOCKS 固定池的研究包；量化初筛与长期候选不再并入"
        "（configs/feature_flags.py 的 ENABLE_SELECTION_UNIVERSE_MERGE=False）。",
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
