"""Financial-report source preparation and deep-research gate for backtests."""

from __future__ import annotations

import concurrent.futures
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from core.logging import get_logger
from news.disclosures_builder import sync_financial_reports_for_stock
from services.backtest.experiment import BacktestExperiment
from services.research.financial_report_skill import (
    build_stock_report_bundles,
    synthesize_manual_item,
)
from shared_data_access import SharedDataAccess
from utlity import is_etf_symbol, parse_symbol


LOGGER = get_logger("BacktestFinancials")
CHECKPOINT_SCHEMA_VERSION = 1
CHECKPOINT_FILENAME = "financial_disclosure_preparation.json"
MINIMUM_REPORT_LOOKBACK_DAYS = 900
PRE_START_REPORT_LOOKBACK_DAYS = 550


def _checkpoint_path(experiment: BacktestExperiment) -> Path:
    return experiment.root / "checkpoints" / CHECKPOINT_FILENAME


def _load_checkpoint(experiment: BacktestExperiment) -> dict[str, Any]:
    path = _checkpoint_path(experiment)
    if not path.exists():
        return {"schema_version": CHECKPOINT_SCHEMA_VERSION, "symbols": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("财报原文准备 checkpoint 无法读取，将重新生成: %s", exc)
        return {"schema_version": CHECKPOINT_SCHEMA_VERSION, "symbols": {}}
    if payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        return {"schema_version": CHECKPOINT_SCHEMA_VERSION, "symbols": {}}
    if not isinstance(payload.get("symbols"), dict):
        payload["symbols"] = {}
    return payload


def _save_checkpoint(
    experiment: BacktestExperiment,
    payload: dict[str, Any],
) -> Path:
    path = _checkpoint_path(experiment)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["schema_version"] = CHECKPOINT_SCHEMA_VERSION
    payload["updated_at"] = datetime.now().isoformat()
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _required_lookback_days(experiment: BacktestExperiment) -> int:
    start = datetime.strptime(experiment.start_date, "%Y-%m-%d").date()
    earliest_needed = start - timedelta(days=PRE_START_REPORT_LOOKBACK_DAYS)
    return max(
        (date.today() - earliest_needed).days + 1,
        MINIMUM_REPORT_LOOKBACK_DAYS,
    )


def _sync_one(
    experiment: BacktestExperiment,
    symbol: str,
    *,
    lookback_days: int,
) -> dict[str, Any]:
    symbol_info = parse_symbol(symbol)
    count = sync_financial_reports_for_stock(
        symbol_info,
        lookback_days=lookback_days,
        convert_markdown=False,
        data_access=SharedDataAccess(
            base_dir=experiment.context.source_data_root,
            logger=LOGGER,
        ),
    )
    return {
        "status": "success",
        "prepared_at": datetime.now().isoformat(),
        "lookback_days": lookback_days,
        "synced_report_count": int(count or 0),
    }


def prepare_backtest_financial_disclosures(
    experiment: BacktestExperiment,
    symbols: Iterable[str],
    *,
    max_workers: int = 4,
) -> dict[str, Any]:
    """Populate the shared financial-disclosure cache once per experiment/symbol."""

    checkpoint = _load_checkpoint(experiment)
    symbol_states = checkpoint.setdefault("symbols", {})
    requested = list(dict.fromkeys(str(symbol).strip() for symbol in symbols if symbol))
    skipped_etfs = [
        symbol for symbol in requested if is_etf_symbol(parse_symbol(symbol))
    ]
    pending = [
        symbol
        for symbol in requested
        if symbol not in skipped_etfs
        and (symbol_states.get(symbol) or {}).get("status") != "success"
    ]
    cached_count = sum(
        1
        for symbol in requested
        if symbol not in skipped_etfs
        and (symbol_states.get(symbol) or {}).get("status") == "success"
    )
    if experiment.network_mode == "disabled":
        return {
            "status": "skipped",
            "reason": "network_mode=disabled，仅检查已有财报披露缓存",
            "requested_symbol_count": len(requested),
            "prepared_symbol_count": 0,
            "cached_symbol_count": cached_count,
            "failed_symbols": [],
            "skipped_etfs": skipped_etfs,
            "checkpoint": str(_checkpoint_path(experiment)),
        }

    lookback_days = _required_lookback_days(experiment)
    failures: list[dict[str, str]] = []
    prepared_count = 0
    worker_count = max(1, min(int(max_workers or 1), len(pending) or 1))
    if pending:
        LOGGER.info(
            "开始同步回测财报原文: experiment=%s symbols=%d lookback=%d workers=%d",
            experiment.experiment_id,
            len(pending),
            lookback_days,
            worker_count,
        )
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count,
        ) as executor:
            future_to_symbol = {
                executor.submit(
                    _sync_one,
                    experiment,
                    symbol,
                    lookback_days=lookback_days,
                ): symbol
                for symbol in pending
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    symbol_states[symbol] = future.result()
                    prepared_count += 1
                except Exception as exc:
                    message = str(exc)
                    failures.append({"symbol": symbol, "error": message})
                    symbol_states[symbol] = {
                        "status": "failed",
                        "attempted_at": datetime.now().isoformat(),
                        "lookback_days": lookback_days,
                        "error": message,
                    }
                    LOGGER.warning(
                        "回测财报原文同步失败，将在后续日期重试: symbol=%s error=%s",
                        symbol,
                        message,
                    )

    checkpoint_path = _save_checkpoint(experiment, checkpoint)
    return {
        "status": "warning" if failures else "success",
        "requested_symbol_count": len(requested),
        "prepared_symbol_count": prepared_count,
        "cached_symbol_count": cached_count,
        "failed_symbols": failures,
        "skipped_etfs": skipped_etfs,
        "lookback_days": lookback_days,
        "checkpoint": str(checkpoint_path),
    }


def inspect_backtest_financial_research(
    experiment: BacktestExperiment,
    run_date: str,
    symbols: Iterable[str],
    *,
    backtest_context_path: Path,
    focus_symbols: set[str] | None = None,
) -> dict[str, Any]:
    """Require a registered deep report for each known latest report as of D.

    focus_symbols 非空时，只对 focus_symbols 内的股票严格执行财务门禁；
    其余股票视为放宽（不计入 required_items），用于"单只股票聚焦回测"。
    """

    requested = [
        symbol
        for symbol in dict.fromkeys(str(item).strip() for item in symbols if item)
        if not is_etf_symbol(parse_symbol(symbol))
    ]
    if focus_symbols:
        requested = [symbol for symbol in requested if symbol in focus_symbols]
    items = [
        synthesize_manual_item(symbol, final_mandate="backtest_fixed_tracked")
        for symbol in requested
    ]
    bundles = build_stock_report_bundles(
        run_date,
        extra_items=items,
        skip_queue=True,
    )
    required_items: list[dict[str, Any]] = []
    covered_items: list[dict[str, Any]] = []
    unavailable_items: list[dict[str, Any]] = []
    for bundle in bundles:
        row = {
            "symbol": bundle.symbol,
            "stock_name": bundle.stock_name,
            "latest_report_date": bundle.latest_report.date or None,
            "latest_announcement_id": bundle.latest_report.announcement_id or None,
            "output_path": str(bundle.output_path),
            "summary_index_path": str(bundle.summary_index_path),
        }
        if bundle.skip_reason == "already_summarized_latest_report":
            covered_items.append(row)
        elif bundle.skip_reason == "missing_financial_reports_in_disclosures":
            row["reason"] = bundle.skip_reason
            unavailable_items.append(row)
        else:
            required_items.append(row)

    symbols_arg = ",".join(item["symbol"] for item in required_items)
    preparation_command = (
        "python scripts/prepare_financial_report_skill.py "
        f"--date {run_date} --json --symbols {symbols_arg} "
        f"--backtest-context {backtest_context_path}"
        if symbols_arg
        else None
    )
    return {
        "status": "needs_financial_research" if required_items else "ready",
        "run_date": run_date,
        "required_items": required_items,
        "covered_symbol_count": len(covered_items),
        "unavailable_items": unavailable_items,
        "preparation_command": preparation_command,
        "registration_command_template": (
            "python scripts/register_financial_report_summary.py "
            "--symbol {symbol} --path {output_path} "
            f"--as-of-date {run_date} --require-deep-research"
        ),
        "resume_prepare_day_command": (
            "python scripts/manage_fixed_tracked_backtest.py prepare-day "
            f"--experiment-id {experiment.experiment_id} "
            f"--date {run_date} --build-missing-inputs "
            "--force-rebuild-inputs --max-workers 6"
        ),
        "note": (
            "required_items 必须完整执行 financial-report-summary 的 "
            "Industry/Expectation/Author/Challenger/Author修订流程并注册，"
            "然后重新运行 prepare-day。"
        ),
    }


__all__ = [
    "inspect_backtest_financial_research",
    "prepare_backtest_financial_disclosures",
]
