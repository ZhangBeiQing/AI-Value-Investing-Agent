"""Read-only coverage audit for a requested backtest range."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.backtest.experiment import BacktestExperiment
from shared_data_access.market_calendar import market_sessions_between


LOGGER = get_logger("BacktestCoverage")
REQUIRED_INPUTS = (
    "01_global_context.md",
    "02_basic_snapshot_payload.json",
    "03_agent_input.md",
    "03_stock_analysis_input.md",
)


def resolve_experiment_trading_dates(
    experiment: BacktestExperiment,
) -> list[str]:
    sessions = market_sessions_between(
        experiment.start_date,
        experiment.end_date,
        market="CN",
        base_dir=experiment.context.source_data_root,
    )
    return list(sessions.dates)


def ensure_experiment_decision_date(
    experiment: BacktestExperiment,
    run_date: str,
) -> None:
    dates = resolve_experiment_trading_dates(experiment)
    if run_date in dates[:-1]:
        return
    if run_date == (dates[-1] if dates else None):
        raise ValueError(
            f"{run_date} 是实验最后一个交易日，只用于最终收盘估值，"
            "不再生成区间外无法成交的决策"
        )
    previous = max((item for item in dates if item < run_date), default=None)
    following = min((item for item in dates if item > run_date), default=None)
    raise ValueError(
        f"{run_date} 不是本实验的有效决策交易日；"
        f"previous={previous}, next={following}"
    )


def next_experiment_trading_date(
    experiment: BacktestExperiment,
    decision_date: str,
) -> str:
    dates = resolve_experiment_trading_dates(experiment)
    if decision_date not in dates:
        raise ValueError(f"{decision_date} 不是本实验交易日")
    index = dates.index(decision_date)
    if index + 1 >= len(dates):
        raise ValueError(f"{decision_date} 之后没有实验范围内的交易日")
    return dates[index + 1]


def audit_experiment_coverage(
    experiment: BacktestExperiment,
) -> dict[str, Any]:
    dates = resolve_experiment_trading_dates(experiment)
    calendar_source = market_sessions_between(
        experiment.start_date,
        experiment.end_date,
        market="CN",
        base_dir=experiment.context.source_data_root,
    ).source
    rows: list[dict[str, Any]] = []
    source_root = experiment.context.source_data_root
    for run_date in dates:
        source_book = source_root / "skill_runs" / run_date / "fixed_tracked"
        missing_inputs = [
            name for name in REQUIRED_INPUTS if not (source_book / name).exists()
        ]
        research_dir = source_book / "04_stock_research"
        prefilter = (
            source_root
            / "selection_runs"
            / run_date
            / "12_quant_prefilter_long.csv"
        )
        macro_exact = (
            source_root
            / "macro_economy"
            / f"{run_date.replace('-', '')}.md"
        )
        rows.append(
            {
                "date": run_date,
                "existing_inputs_complete": not missing_inputs and research_dir.is_dir(),
                "missing_inputs": missing_inputs,
                "research_file_count": (
                    len(list(research_dir.iterdir()))
                    if research_dir.is_dir()
                    else 0
                ),
                "long_prefilter_exists": prefilter.exists(),
                "macro_exact_exists": macro_exact.exists(),
            }
        )
    payload = {
        "schema_version": 1,
        "experiment_id": experiment.experiment_id,
        "start_date": experiment.start_date,
        "end_date": experiment.end_date,
        "trading_date_count": len(dates),
        "calendar_source": calendar_source,
        "existing_inputs_complete_count": sum(
            1 for row in rows if row["existing_inputs_complete"]
        ),
        "long_prefilter_count": sum(
            1 for row in rows if row["long_prefilter_exists"]
        ),
        "macro_exact_count": sum(1 for row in rows if row["macro_exact_exists"]),
        "dates": rows,
    }
    output = experiment.root / "coverage.json"
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    LOGGER.info(
        "回测覆盖率扫描完成: experiment=%s dates=%d existing=%d",
        experiment.experiment_id,
        len(dates),
        payload["existing_inputs_complete_count"],
    )
    return payload


def inspect_backtest_progress(
    experiment: BacktestExperiment,
) -> dict[str, Any]:
    dates = resolve_experiment_trading_dates(experiment)
    decision_dates = dates[:-1]
    completed: list[str] = []
    next_date: str | None = None
    next_stage = "finalize"
    for run_date in decision_dates:
        book_dir = (
            experiment.context.skill_runs_root / run_date / "fixed_tracked"
        )
        decision_path = book_dir / "05_decision.json"
        execution_path = book_dir / "06_execution_log.json"
        execution_success = False
        if execution_path.exists():
            try:
                execution_success = (
                    json.loads(execution_path.read_text(encoding="utf-8")).get(
                        "status"
                    )
                    == "success"
                )
            except Exception:
                execution_success = False
        if execution_success:
            completed.append(run_date)
            continue
        next_date = run_date
        if not (book_dir / "00_prepare_status.json").exists():
            next_stage = "prepare_day"
        elif not decision_path.exists():
            next_stage = "agent_decision"
        else:
            next_stage = "execute_day"
        break
    return {
        "trading_date_count": len(dates),
        "decision_date_count": len(decision_dates),
        "completed_execution_count": len(completed),
        "last_completed_date": completed[-1] if completed else None,
        "next_date": next_date,
        "next_stage": next_stage,
        "final_mark_to_market_date": dates[-1] if dates else None,
    }


__all__ = [
    "REQUIRED_INPUTS",
    "audit_experiment_coverage",
    "ensure_experiment_decision_date",
    "inspect_backtest_progress",
    "next_experiment_trading_date",
    "resolve_experiment_trading_dates",
]
