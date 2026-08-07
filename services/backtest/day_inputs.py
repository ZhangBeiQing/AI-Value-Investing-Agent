"""Materialize an isolated daily backtest work directory."""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.pipeline.daily_pipeline import SKILL_FLOW_CONFIG, run_book_pipeline
from services.backtest.announcements import prepare_backtest_announcements
from services.backtest.coverage import ensure_experiment_decision_date
from services.backtest.experiment import BacktestExperiment
from services.backtest.financials import (
    inspect_backtest_financial_research,
    prepare_backtest_financial_disclosures,
)
from services.backtest.ledger import BacktestLedger
from services.backtest.locking import locked_experiment_stage
from services.backtest.universe import build_daily_universe
from shared_data_access.historical_prices import close_on_or_before
from utlity import parse_symbol
from services.trading.trade_summary import (
    get_portfolio_historical_context,
    use_agent_data_root,
)


LOGGER = get_logger("BacktestDayInputs")


def _load_filled_orders(agent_data_root: Path, signature: str) -> list[dict[str, Any]]:
    """读取回测订单流，返回 execution_date <= run_date 的已成交记录。"""
    orders_file = agent_data_root / signature / "orders.jsonl"
    if not orders_file.exists():
        return []
    orders: list[dict[str, Any]] = []
    for line in orders_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            orders.append(payload)
    return orders


def _position_label(symbol: str) -> str:
    """返回带名称的持仓标签，如 海康威视_002415.SZ；CASH 保持原样。"""
    if symbol == "CASH":
        return "CASH"
    try:
        name = parse_symbol(symbol).stock_name
    except Exception:
        name = ""
    return f"{name}_{symbol}" if name else symbol


def _portfolio_position_summary(
    experiment: BacktestExperiment,
    run_date: str,
    positions: dict[str, float],
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """按已成交订单计算每只持仓的加权平均成本、浮动盈亏与盈亏率。

    返回 (costs, profits, return_pcts)，键为带名称的持仓标签（如 海康威视_002415.SZ）；
    只含当前持股 > 0 的股票。
    成本 = 已成交 BUY 成交额合计 / 已成交 BUY 股数合计（忽略手续费，回测费率 0）。
    浮动盈亏 = (run_date 收盘 - 加权成本) * 当前持股数。
    """
    orders = _load_filled_orders(
        experiment.context.agent_data_root,
        experiment.signature,
    )
    bought_shares: dict[str, float] = {}
    bought_amount: dict[str, float] = {}
    for order in orders:
        if order.get("status") != "filled":
            continue
        exec_date = str(order.get("execution_date") or "")
        if exec_date and exec_date > run_date:
            continue
        symbol = order.get("symbol")
        if not symbol or symbol == "CASH":
            continue
        action = str(order.get("action") or "").upper()
        if action != "BUY":
            continue
        shares = float(order.get("filled_shares") or 0)
        price = float(order.get("execution_price") or 0)
        if shares <= 0 or price <= 0:
            continue
        bought_shares[symbol] = bought_shares.get(symbol, 0.0) + shares
        bought_amount[symbol] = bought_amount.get(symbol, 0.0) + shares * price

    costs: dict[str, float] = {}
    profits: dict[str, float] = {}
    return_pcts: dict[str, float] = {}
    for symbol, raw_shares in positions.items():
        if symbol == "CASH":
            continue
        shares = float(raw_shares or 0)
        if shares <= 0:
            continue
        total_shares = bought_shares.get(symbol, 0.0)
        total_amount = bought_amount.get(symbol, 0.0)
        if total_shares <= 0 or total_amount <= 0:
            continue
        avg_cost = total_amount / total_shares
        price = close_on_or_before(
            parse_symbol(symbol),
            run_date,
            base_dir=experiment.context.source_data_root,
        )
        if price is None:
            continue
        _, close = price
        label = _position_label(symbol)
        costs[label] = round(avg_cost, 4)
        profits[label] = round((close - avg_cost) * shares, 2)
        if avg_cost > 0:
            return_pcts[label] = round((close / avg_cost - 1.0) * 100.0, 2)
    return costs, profits, return_pcts
COPY_INPUTS = (
    "01_global_context.md",
    "02_basic_snapshot_payload.json",
    "03_agent_input.md",
    "03_stock_analysis_input.md",
)


@locked_experiment_stage
def write_no_trade_decision(
    experiment: BacktestExperiment,
    run_date: str,
    *,
    reason: str,
) -> Path:
    """Write an explicit empty 05 for a day on which P0 is empty."""

    ensure_experiment_decision_date(experiment, run_date)
    book_dir = experiment.context.skill_runs_root / run_date / "fixed_tracked"
    book_dir.mkdir(parents=True, exist_ok=True)
    target = book_dir / "05_decision.json"
    if target.exists():
        raise FileExistsError(f"05_decision.json 已存在，拒绝覆盖: {target}")
    payload = {
        "summary_date": run_date,
        "system_risk_notes": [reason],
        "system_focus_items": [],
        "stock_decisions": [],
        "backtest_no_trade": True,
    }
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return target


def _write_backtest_context(
    experiment: BacktestExperiment,
    run_date: str,
    book_dir: Path,
) -> tuple[Path, Path]:
    payload = {
        "mode": "historical_backtest",
        "experiment_id": experiment.experiment_id,
        "decision_date": run_date,
        "knowledge_cutoff": f"{run_date}T23:59:59+08:00",
        "network_mode": experiment.network_mode,
        "future_information_forbidden": True,
        "search_query_date_required": True,
        "source_publication_date_must_be_on_or_before": run_date,
        "retrospective_articles_forbidden": True,
    }
    json_path = book_dir / "00_backtest_context.json"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown = f"""# fixed_tracked 历史回测上下文

- 实验：`{experiment.experiment_id}`
- 决策日期：`{run_date}` 收盘后
- 可知信息截止：`{run_date} 23:59:59 +08:00`
- 联网模式：`{experiment.network_mode}`

你正在进行历史回测。搜索词必须写入截止日期。关键资料的公开日期不得晚于
`{run_date}`，不得使用后来回顾、修正或总结当时事件的文章。无法确认公开日期的
资料只能作为待核验线索，不能据此改变 BUY/SELL。找不到合格来源时必须承认缺失。
"""
    md_path = book_dir / "00_backtest_context.md"
    md_path.write_text(markdown, encoding="utf-8")
    return json_path, md_path


@locked_experiment_stage
def prepare_backtest_day(
    experiment: BacktestExperiment,
    run_date: str,
    *,
    reuse_existing_inputs: bool = False,
    build_missing_inputs: bool = False,
    force_rebuild_inputs: bool = False,
    max_workers: int = 4,
) -> dict[str, Any]:
    datetime.strptime(run_date, "%Y-%m-%d")
    if run_date < experiment.start_date or run_date > experiment.end_date:
        raise ValueError("run_date 不在实验日期范围内")
    ensure_experiment_decision_date(experiment, run_date)
    book_dir = (
        experiment.context.skill_runs_root
        / run_date
        / "fixed_tracked"
    )
    book_dir.mkdir(parents=True, exist_ok=True)
    context_json, context_md = _write_backtest_context(
        experiment,
        run_date,
        book_dir,
    )
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
    symbols, universe_metadata = build_daily_universe(experiment, run_date, ledger)
    copied: list[str] = []
    missing: list[str] = []
    source_book = (
        experiment.context.source_data_root
        / "skill_runs"
        / run_date
        / "fixed_tracked"
    )
    if reuse_existing_inputs and source_book.is_dir():
        for filename in COPY_INPUTS:
            source = source_book / filename
            target = book_dir / filename
            if source.exists() and not target.exists():
                shutil.copy2(source, target)
                copied.append(filename)
        source_research = source_book / "04_stock_research"
        target_research = book_dir / "04_stock_research"
        if source_research.is_dir() and not target_research.exists():
            shutil.copytree(source_research, target_research)
            copied.append("04_stock_research")

    for filename in COPY_INPUTS:
        if not (book_dir / filename).exists():
            missing.append(filename)
    if not (book_dir / "04_stock_research").is_dir():
        missing.append("04_stock_research")

    generated_inputs = False
    announcement_preparation: dict[str, Any] = {
        "status": "not_needed",
        "reason": "未重建当日 01-04",
    }
    financial_disclosure_preparation: dict[str, Any] = {
        "status": "not_needed",
        "reason": "未重建当日 01-04",
    }
    financial_research: dict[str, Any] = {
        "status": "not_checked",
        "reason": "未重建当日 01-04",
    }
    if build_missing_inputs:
        announcement_preparation = prepare_backtest_announcements(
            experiment,
            symbols,
            max_workers=max_workers,
        )
        financial_disclosure_preparation = prepare_backtest_financial_disclosures(
            experiment,
            symbols,
            max_workers=max_workers,
        )
        financial_research = inspect_backtest_financial_research(
            experiment,
            run_date,
            symbols,
            backtest_context_path=context_md,
        )
        if (
            financial_research["status"] == "ready"
            and (missing or force_rebuild_inputs)
        ):
            state = ledger.latest(on_or_before=run_date)
            total_value, _ = ledger.mark_to_market(run_date, state.positions)
            with use_agent_data_root(experiment.context.agent_data_root):
                historical_context = get_portfolio_historical_context(
                    experiment.signature,
                    symbols,
                    n=1,
                )
            position_costs, position_profit, position_return_pct = (
                _portfolio_position_summary(
                    experiment,
                    run_date,
                    state.positions,
                )
            )
            held_positions = {
                _position_label(symbol): float(shares or 0)
                for symbol, shares in state.positions.items()
                if symbol != "CASH" and float(shares or 0) > 0
            }
            if float(state.positions.get("CASH", 0.0) or 0.0) > 0:
                held_positions["CASH"] = float(state.positions["CASH"])
            portfolio_value_amount = f"{total_value:,.2f} 元"
            prompt_context = {
                "date": run_date,
                "positions": json.dumps(
                    held_positions,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
                "position_costs": json.dumps(
                    position_costs,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
                "position_profit": json.dumps(
                    position_profit,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
                "position_return_pct": json.dumps(
                    position_return_pct,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
                "STOP_SIGNAL": "<FINISH_SIGNAL>",
                "historical_summary": json.dumps(
                    historical_context,
                    ensure_ascii=False,
                    indent=2,
                ),
                "portfolio_value": f"{portfolio_value_amount}，",
                "portfolio_value_amount": portfolio_value_amount,
            }
            run_book_pipeline(
                run_date,
                output_dir=book_dir,
                symbols=symbols,
                prompt_config=SKILL_FLOW_CONFIG,
                signature=experiment.signature,
                book_type="fixed_tracked",
                max_workers=max_workers,
                source_data_root=experiment.context.source_data_root,
                backtest_read_only=True,
                research_cache_root=experiment.context.research_cache_root,
                agent_data_root=experiment.context.agent_data_root,
                prompt_context_override=prompt_context,
            )
            generated_inputs = True
        missing = [
            filename
            for filename in COPY_INPUTS
            if not (book_dir / filename).exists()
        ]
        if not (book_dir / "04_stock_research").is_dir():
            missing.append("04_stock_research")

    manifest = {
        "run_date": run_date,
        "mode": "historical_backtest",
        "experiment_id": experiment.experiment_id,
        "books": [
            {
                "book_type": "fixed_tracked",
                "signature": experiment.signature,
                "source_type": "frozen_tracked_plus_daily_quant_long",
                "symbols": symbols,
                "symbol_sources": universe_metadata,
            }
        ],
        "default_books": ["fixed_tracked"],
    }
    manifest_path = experiment.context.skill_runs_root / run_date / "run_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    result = {
        "run_date": run_date,
        "book_dir": str(book_dir),
        "backtest_context_json": str(context_json),
        "backtest_context_markdown": str(context_md),
        "manifest": str(manifest_path),
        "symbols": symbols,
        "universe": universe_metadata,
        "copied_inputs": copied,
        "generated_inputs": generated_inputs,
        "force_rebuild_inputs": force_rebuild_inputs,
        "announcement_preparation": announcement_preparation,
        "financial_disclosure_preparation": financial_disclosure_preparation,
        "financial_research": financial_research,
        "missing_inputs": missing,
        "status": (
            "needs_financial_research"
            if financial_research.get("status") == "needs_financial_research"
            else ("ready" if not missing else "needs_input_generation")
        ),
    }
    (book_dir / "00_prepare_status.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    LOGGER.info(
        "回测日期目录已准备: date=%s status=%s missing=%d",
        run_date,
        result["status"],
        len(missing),
    )
    return result


__all__ = ["prepare_backtest_day", "write_no_trade_decision"]
