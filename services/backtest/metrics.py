"""Portfolio metrics derived from the isolated position ledger."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.backtest.coverage import resolve_experiment_trading_dates
from services.backtest.experiment import BacktestExperiment
from services.backtest.ledger import BacktestLedger
from services.backtest.locking import locked_experiment_stage


LOGGER = get_logger("BacktestMetrics")


def _read_orders(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    orders: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            orders.append(payload)
    return orders


@locked_experiment_stage
def finalize_backtest(
    experiment: BacktestExperiment,
) -> tuple[Path, Path]:
    ledger = BacktestLedger(
        agent_data_root=experiment.context.agent_data_root,
        signature=experiment.signature,
        source_data_root=experiment.context.source_data_root,
    )
    ledger.initialize(
        start_date=experiment.start_date,
        initial_cash=experiment.initial_cash,
        symbols=list(experiment.base_universe),
    )
    dates = resolve_experiment_trading_dates(experiment)
    curve: list[dict[str, Any]] = []
    peak = float(experiment.initial_cash)
    previous_value: float | None = None
    max_drawdown = 0.0
    for run_date in dates:
        state = ledger.latest(on_or_before=run_date)
        total_value, stale = ledger.mark_to_market(run_date, state.positions)
        peak = max(peak, total_value)
        drawdown = (total_value / peak - 1.0) if peak else 0.0
        max_drawdown = min(max_drawdown, drawdown)
        daily_return = (
            total_value / previous_value - 1.0
            if previous_value not in (None, 0)
            else 0.0
        )
        cash = float(state.positions.get("CASH", 0.0) or 0.0)
        curve.append(
            {
                "date": run_date,
                "cash": round(cash, 4),
                "market_value": round(total_value - cash, 4),
                "total_value": total_value,
                "daily_return": round(daily_return, 8),
                "drawdown": round(drawdown, 8),
                "stale_price_symbols": "|".join(stale),
            }
        )
        previous_value = total_value

    results_dir = experiment.root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    curve_path = results_dir / "equity_curve.csv"
    with curve_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "date",
            "cash",
            "market_value",
            "total_value",
            "daily_return",
            "drawdown",
            "stale_price_symbols",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(curve)

    orders = _read_orders(ledger.orders_file)
    filled = [order for order in orders if order.get("status") == "filled"]
    final_value = curve[-1]["total_value"] if curve else experiment.initial_cash
    elapsed_days = max(
        (
            datetime.strptime(experiment.end_date, "%Y-%m-%d")
            - datetime.strptime(experiment.start_date, "%Y-%m-%d")
        ).days,
        1,
    )
    total_return = final_value / experiment.initial_cash - 1.0
    annualized_return = (1.0 + total_return) ** (365.0 / elapsed_days) - 1.0
    final_state = ledger.latest(on_or_before=experiment.end_date)
    final_cash = float(final_state.positions.get("CASH", 0.0) or 0.0)
    summary = {
        "schema_version": 1,
        "experiment_id": experiment.experiment_id,
        "start_date": experiment.start_date,
        "end_date": experiment.end_date,
        "finalized_at": datetime.now().isoformat(),
        "initial_value": experiment.initial_cash,
        "final_value": round(float(final_value), 4),
        "cash": round(final_cash, 4),
        "market_value": round(float(final_value) - final_cash, 4),
        "total_return": round(total_return, 8),
        "annualized_return": round(annualized_return, 8),
        "max_drawdown": round(max_drawdown, 8),
        "trade_count": len(filled),
        "buy_count": sum(1 for order in filled if order.get("action") == "BUY"),
        "sell_count": sum(1 for order in filled if order.get("action") == "SELL"),
        "pending_orders_at_end": sum(
            1 for order in orders if order.get("status") == "pending"
        ),
        "completed_decision_days": len(
            list(experiment.context.skill_runs_root.glob("*/fixed_tracked/05_decision.json"))
        ),
        "network_mode": experiment.network_mode,
        "lookahead_risk": experiment.payload.get("lookahead_risk", True),
        "survivorship_bias": True,
    }
    summary_path = results_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    LOGGER.info(
        "回测结果已生成: experiment=%s final=%.2f return=%.4f",
        experiment.experiment_id,
        final_value,
        total_return,
    )
    return summary_path, curve_path


__all__ = ["finalize_backtest"]
