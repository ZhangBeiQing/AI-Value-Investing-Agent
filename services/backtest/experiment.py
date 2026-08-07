"""Experiment creation and immutable backtest configuration."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from configs.stock_pool import TRACKED_A_STOCKS
from core.logging import get_logger
from core.run_context import DEFAULT_BACKTESTS_ROOT, DEFAULT_DATA_ROOT, RunContext


LOGGER = get_logger("FixedTrackedBacktest")
EXPERIMENT_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")


def _git_metadata() -> tuple[str, bool]:
    project_root = Path(__file__).resolve().parents[2]
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=project_root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
        return commit, dirty
    except Exception:
        return "", True


def _default_experiment_id(start_date: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"fixed_{start_date.replace('-', '')}_{stamp}"


def _prompt_hashes() -> dict[str, str]:
    project_root = Path(__file__).resolve().parents[2]
    roots = [
        project_root / "configs" / "prompt_flow" / "fixed_tracked",
        project_root / "configs" / "research" / "web_research_policy.md",
        project_root / ".codex" / "skills" / "auto-trading-fixed-tracked",
        project_root / ".codex" / "skills" / "backtest-fixed-tracked",
    ]
    files: list[Path] = []
    for root in roots:
        if root.is_file():
            files.append(root)
        elif root.is_dir():
            files.extend(
                path
                for path in root.rglob("*")
                if path.is_file() and path.suffix.lower() in {".md", ".json"}
            )
    return {
        str(path.relative_to(project_root)): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in sorted(set(files))
    }


@dataclass(frozen=True)
class BacktestExperiment:
    experiment_id: str
    start_date: str
    end_date: str
    initial_cash: float
    base_universe: tuple[str, ...]
    network_mode: str
    execution_rule: str
    context: RunContext
    payload: dict[str, Any]

    @property
    def root(self) -> Path:
        return self.context.run_output_root

    @property
    def signature(self) -> str:
        return f"backtest-{self.experiment_id}"


def create_backtest_experiment(
    *,
    start_date: str,
    end_date: str,
    initial_cash: float = 500000.0,
    experiment_id: str = "",
    network_mode: str = "guarded_web",
    source_data_root: str | Path = DEFAULT_DATA_ROOT,
    backtests_root: str | Path = DEFAULT_BACKTESTS_ROOT,
) -> BacktestExperiment:
    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")
    if start_date > end_date:
        raise ValueError("start_date 不能晚于 end_date")
    if initial_cash <= 0:
        raise ValueError("initial_cash 必须大于 0")
    resolved_id = experiment_id or _default_experiment_id(start_date)
    if not EXPERIMENT_ID_PATTERN.match(resolved_id):
        raise ValueError(f"experiment_id 非法: {resolved_id}")

    experiment_root = Path(backtests_root).resolve() / resolved_id
    if (experiment_root / "experiment.json").exists():
        raise FileExistsError(f"回测实验已存在: {experiment_root}")
    context = RunContext.backtest(
        experiment_root,
        source_data_root=source_data_root,
        allowed_backtests_root=backtests_root,
    )
    context.ensure_output_dirs()
    for relative in ("checkpoints", "results"):
        (experiment_root / relative).mkdir(parents=True, exist_ok=True)

    commit, dirty = _git_metadata()
    base_universe = tuple(entry.symbol for entry in TRACKED_A_STOCKS)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "experiment_id": resolved_id,
        "created_at": datetime.now().isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "initial_cash": float(initial_cash),
        "initial_positions": {},
        "base_universe": list(base_universe),
        "universe_mode": "frozen_current_tracked_plus_daily_quant_long",
        "long_candidate_policy": "daily_12_quant_prefilter_long",
        "survivorship_bias": True,
        "network_mode": network_mode,
        "lookahead_risk": network_mode != "disabled",
        "execution_rule": "next_trading_day_open",
        "commission_rate": 0.0,
        "stamp_duty_rate": 0.0,
        "slippage_bps": 0.0,
        "git_commit": commit,
        "working_tree_dirty": dirty,
        "model_config": {},
        "prompt_hashes": _prompt_hashes(),
        "paths": context.as_dict(),
    }
    (experiment_root / "experiment.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    from services.backtest.ledger import BacktestLedger

    BacktestLedger(
        agent_data_root=context.agent_data_root,
        signature=f"backtest-{resolved_id}",
        source_data_root=context.source_data_root,
    ).initialize(
        start_date=start_date,
        initial_cash=float(initial_cash),
        symbols=list(base_universe),
    )
    LOGGER.info("回测实验已创建: %s", experiment_root)
    return BacktestExperiment(
        experiment_id=resolved_id,
        start_date=start_date,
        end_date=end_date,
        initial_cash=float(initial_cash),
        base_universe=base_universe,
        network_mode=network_mode,
        execution_rule="next_trading_day_open",
        context=context,
        payload=payload,
    )


def extend_backtest_experiment(
    experiment: BacktestExperiment,
    *,
    new_end_date: str,
) -> tuple[BacktestExperiment, dict[str, Any]]:
    """Extend an existing experiment in place without changing its identity."""

    parsed_new_end = datetime.strptime(new_end_date, "%Y-%m-%d")
    parsed_old_end = datetime.strptime(experiment.end_date, "%Y-%m-%d")
    if parsed_new_end <= parsed_old_end:
        raise ValueError(
            f"新结束日期必须晚于当前结束日期 {experiment.end_date}: {new_end_date}"
        )

    from services.backtest.coverage import (
        audit_experiment_coverage,
        resolve_experiment_trading_dates,
    )
    from services.backtest.locking import experiment_lock

    with experiment_lock(experiment.root):
        config_path = experiment.root / "experiment.json"
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        current_end_date = str(payload.get("end_date") or "")
        if current_end_date != experiment.end_date:
            raise RuntimeError(
                "实验配置已被其他进程修改，请重新执行 status 后再扩展："
                f"loaded={experiment.end_date}, current={current_end_date}"
            )

        extended_at = datetime.now().isoformat()
        history = payload.get("extension_history")
        if not isinstance(history, list):
            history = []
        history.append(
            {
                "extended_at": extended_at,
                "old_end_date": experiment.end_date,
                "new_end_date": new_end_date,
            }
        )
        payload["end_date"] = new_end_date
        payload["updated_at"] = extended_at
        payload["extension_history"] = history

        extended = BacktestExperiment(
            experiment_id=experiment.experiment_id,
            start_date=experiment.start_date,
            end_date=new_end_date,
            initial_cash=experiment.initial_cash,
            base_universe=experiment.base_universe,
            network_mode=experiment.network_mode,
            execution_rule=experiment.execution_rule,
            context=experiment.context,
            payload=payload,
        )
        old_trading_dates = resolve_experiment_trading_dates(experiment)
        new_trading_dates = resolve_experiment_trading_dates(extended)
        if len(new_trading_dates) <= len(old_trading_dates):
            raise ValueError(
                f"{experiment.end_date} 到 {new_end_date} 之间没有新增交易日，"
                "无需扩展实验"
            )

        temporary_path = config_path.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(config_path)

        coverage = audit_experiment_coverage(extended)

    LOGGER.info(
        "回测实验区间已扩展: experiment=%s old_end=%s new_end=%s",
        experiment.experiment_id,
        experiment.end_date,
        new_end_date,
    )
    return extended, coverage


def load_backtest_experiment(
    experiment_id: str,
    *,
    source_data_root: str | Path | None = None,
    backtests_root: str | Path = DEFAULT_BACKTESTS_ROOT,
) -> BacktestExperiment:
    if not EXPERIMENT_ID_PATTERN.match(experiment_id):
        raise ValueError(f"experiment_id 非法: {experiment_id}")
    root = Path(backtests_root).resolve() / experiment_id
    path = root / "experiment.json"
    if not path.exists():
        raise FileNotFoundError(f"回测实验不存在: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    resolved_source_root = (
        source_data_root
        or (payload.get("paths") or {}).get("source_data_root")
        or DEFAULT_DATA_ROOT
    )
    context = RunContext.backtest(
        root,
        source_data_root=resolved_source_root,
        allowed_backtests_root=backtests_root,
    )
    return BacktestExperiment(
        experiment_id=experiment_id,
        start_date=payload["start_date"],
        end_date=payload["end_date"],
        initial_cash=float(payload["initial_cash"]),
        base_universe=tuple(payload.get("base_universe") or []),
        network_mode=payload.get("network_mode") or "guarded_web",
        execution_rule=payload.get("execution_rule") or "next_trading_day_open",
        context=context,
        payload=payload,
    )


__all__ = [
    "BacktestExperiment",
    "create_backtest_experiment",
    "extend_backtest_experiment",
    "load_backtest_experiment",
]
