"""Read-only historical financial-research and daily-brief readiness checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from services.backtest.experiment import BacktestExperiment
from services.research.financial_report_skill import (
    build_stock_report_bundles, synthesize_manual_item, registered_summary_covers_report,
)
from services.research.financial_daily_brief import daily_brief_task
from services.research.financial_report import get_financial_report_summary
from commons import is_etf_symbol, parse_symbol


def inspect_backtest_financial_research(
    experiment: BacktestExperiment,
    run_date: str,
    symbols: Iterable[str],
    *,
    backtest_context_path: Path,
) -> dict[str, Any]:
    """Check historical research and daily briefs for the complete fixed scope."""

    requested = [
        symbol
        for symbol in dict.fromkeys(str(item).strip() for item in symbols if item)
        if not is_etf_symbol(parse_symbol(symbol))
    ]
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
    daily_brief_items: list[dict[str, Any]] = []
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
        selected = get_financial_report_summary(
            bundle.symbol, run_date, report_release_slack_days=0,
            include_consensus=False, include_price_drift=False,
            require_historical_cutoff=True,
        )
        row["existing_historical_research"] = selected.get("report_path")
        row["existing_research_paths"] = selected.get("existing_research_paths", [])
        row["rejected_historical_reports"] = selected.get("rejected_historical_reports", [])
        if bundle.skip_reason == "already_summarized_latest_report":
            report_path = selected.get("report_path")
            if not report_path or (
                Path(report_path).stem[:8] != bundle.latest_report.date.replace("-", "")
                and not registered_summary_covers_report(bundle.symbol, Path(report_path), bundle.latest_report)
            ):
                row["reason"] = "latest_report_needs_historical_research_update"
                required_items.append(row)
                continue
            covered_items.append(row)
            task = daily_brief_task(
                Path(report_path), symbol=bundle.symbol, as_of_date=run_date,
            )
            if task:
                daily_brief_items.append(task)
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
        "status": "needs_financial_research" if required_items or daily_brief_items else "ready",
        "run_date": run_date,
        "required_items": required_items,
        "daily_brief_items": daily_brief_items,
        "covered_symbol_count": len(covered_items),
        "unavailable_items": unavailable_items,
        "preparation_command": preparation_command,
        "registration_command_template": (
            "python scripts/register_financial_report_summary.py "
            "--symbol {symbol} --path {output_path} "
            f"--as-of-date {run_date} --require-deep-research --require-daily-brief"
        ),
        "resume_prepare_day_command": (
            "python scripts/manage_fixed_tracked_backtest.py prepare-day "
            f"--experiment-id {experiment.experiment_id} "
            f"--date {run_date} --build-missing-inputs "
            "--force-rebuild-inputs --max-workers 6"
        ),
        "note": (
            "required_items 必须完整执行 financial-report-summary 的 "
            "Industry/Expectation/Author一次撰写流程并注册，"
            "daily_brief_items 只由 Author 按原文补建历史可见摘要，不重做季度研究；"
            "然后重新运行 prepare-day。"
        ),
    }


__all__ = ["inspect_backtest_financial_research"]
