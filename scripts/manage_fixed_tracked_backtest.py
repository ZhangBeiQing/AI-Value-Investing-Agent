#!/usr/bin/env python3
"""Manage isolated fixed-tracked historical backtest experiments."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger  # noqa: E402
from services.backtest.coverage import (  # noqa: E402
    audit_experiment_coverage,
    inspect_backtest_progress,
    next_experiment_trading_date,
)
from services.backtest.day_inputs import (  # noqa: E402
    prepare_backtest_day,
    write_no_trade_decision,
)
from services.backtest.execution import simulate_post_trade  # noqa: E402
from services.backtest.experiment import (  # noqa: E402
    create_backtest_experiment,
    extend_backtest_experiment,
    load_backtest_experiment,
)
from services.backtest.metrics import finalize_backtest  # noqa: E402


LOGGER = init_component_logger(
    "FixedTrackedBacktest",
    group="main_scripts",
    filename_prefix="fixed_tracked_backtest",
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="管理 fixed_tracked 多 Agent 历史回测实验。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="创建实验并扫描历史输入覆盖率。")
    prepare.add_argument("--start-date", required=True)
    prepare.add_argument("--end-date", required=True)
    prepare.add_argument("--initial-cash", type=float, default=500000.0)
    prepare.add_argument("--experiment-id", default="")
    prepare.add_argument(
        "--network-mode",
        choices=("guarded_web", "disabled"),
        default="guarded_web",
    )

    status = subparsers.add_parser("status", help="查看实验配置和覆盖率。")
    status.add_argument("--experiment-id", required=True)

    extend = subparsers.add_parser(
        "extend",
        help="保持实验身份与原账本不变，仅向后扩展结束日期。",
    )
    extend.add_argument("--experiment-id", required=True)
    extend.add_argument("--end-date", required=True)

    prepare_day = subparsers.add_parser(
        "prepare-day",
        help="创建当日隔离目录、回测上下文和股票集合。",
    )
    prepare_day.add_argument("--experiment-id", required=True)
    prepare_day.add_argument("--date", required=True)
    prepare_day.add_argument(
        "--reuse-existing-inputs",
        action="store_true",
        help="仅在已人工完成日期与 Prompt 兼容审计时复用正式 01-04。",
    )
    prepare_day.add_argument(
        "--build-missing-inputs",
        action="store_true",
        help="缺少 01-04 时基于共享只读缓存构建。",
    )
    prepare_day.add_argument(
        "--force-rebuild-inputs",
        action="store_true",
        help="通过全部门禁后覆盖重建当日 01-04；用于修复旧回测产物。",
    )
    prepare_day.add_argument("--max-workers", type=int, default=4)

    no_trade = subparsers.add_parser(
        "no-trade-day",
        help="P0 为空时生成显式无交易 05，不伪造单股 verdict。",
    )
    no_trade.add_argument("--experiment-id", required=True)
    no_trade.add_argument("--date", required=True)
    no_trade.add_argument("--reason", required=True)

    execute = subparsers.add_parser(
        "execute-day",
        help="读取当日 05 并按下一交易日开盘价模拟后处理。",
    )
    execute.add_argument("--experiment-id", required=True)
    execute.add_argument("--date", required=True)
    execute.add_argument("--execution-date", default="")

    finalize = subparsers.add_parser("finalize", help="生成净值与汇总结果。")
    finalize.add_argument("--experiment-id", required=True)
    return parser


def _next_date(experiment, decision_date: str) -> str:
    return next_experiment_trading_date(experiment, decision_date)


def main() -> int:
    args = _build_parser().parse_args()
    if args.command == "prepare":
        experiment = create_backtest_experiment(
            start_date=args.start_date,
            end_date=args.end_date,
            initial_cash=args.initial_cash,
            experiment_id=args.experiment_id,
            network_mode=args.network_mode,
        )
        coverage = audit_experiment_coverage(experiment)
        print(
            json.dumps(
                {
                    "experiment_id": experiment.experiment_id,
                    "experiment_root": str(experiment.root),
                    "trading_date_count": coverage["trading_date_count"],
                    "existing_inputs_complete_count": coverage[
                        "existing_inputs_complete_count"
                    ],
                    "long_prefilter_count": coverage["long_prefilter_count"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    experiment = load_backtest_experiment(args.experiment_id)
    if args.command == "extend":
        previous_summary_path = experiment.root / "results" / "summary.json"
        previous_results_stale = previous_summary_path.exists()
        extended, coverage = extend_backtest_experiment(
            experiment,
            new_end_date=args.end_date,
        )
        print(
            json.dumps(
                {
                    "experiment_id": extended.experiment_id,
                    "experiment_root": str(extended.root),
                    "start_date": extended.start_date,
                    "old_end_date": experiment.end_date,
                    "new_end_date": extended.end_date,
                    "trading_date_count": coverage["trading_date_count"],
                    "previous_results_stale": previous_results_stale,
                    "next_action": (
                        "运行 status，并从第一个未完成决策日继续；"
                        "全部完成后重新 finalize。"
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.command == "status":
        coverage_path = experiment.root / "coverage.json"
        coverage = (
            json.loads(coverage_path.read_text(encoding="utf-8"))
            if coverage_path.exists()
            else audit_experiment_coverage(experiment)
        )
        summary_path = experiment.root / "results" / "summary.json"
        finalized_summary = (
            json.loads(summary_path.read_text(encoding="utf-8"))
            if summary_path.exists()
            else {}
        )
        print(
            json.dumps(
                {
                    "experiment": experiment.payload,
                    "results": {
                        "exists": summary_path.exists(),
                        "finalized_end_date": finalized_summary.get("end_date"),
                        "stale": (
                            summary_path.exists()
                            and finalized_summary.get("end_date")
                            != experiment.end_date
                        ),
                    },
                    "coverage_summary": {
                        key: coverage.get(key)
                        for key in (
                            "trading_date_count",
                            "existing_inputs_complete_count",
                            "long_prefilter_count",
                            "macro_exact_count",
                        )
                    },
                    "progress": inspect_backtest_progress(experiment),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.command == "prepare-day":
        result = prepare_backtest_day(
            experiment,
            args.date,
            reuse_existing_inputs=args.reuse_existing_inputs,
            build_missing_inputs=args.build_missing_inputs,
            force_rebuild_inputs=args.force_rebuild_inputs,
            max_workers=args.max_workers,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.command == "no-trade-day":
        path = write_no_trade_decision(
            experiment,
            args.date,
            reason=args.reason,
        )
        print(json.dumps({"decision_file": str(path)}, ensure_ascii=False, indent=2))
        return 0
    if args.command == "execute-day":
        execution_date = args.execution_date or _next_date(experiment, args.date)
        paths = simulate_post_trade(
            experiment,
            decision_date=args.date,
            execution_date=execution_date,
        )
        print(
            json.dumps(
                {
                    "execution_log": str(paths[0]),
                    "daily_summary": str(paths[1]),
                    "history_merge": str(paths[2]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.command == "finalize":
        summary_path, curve_path = finalize_backtest(experiment)
        print(
            json.dumps(
                {
                    "summary": str(summary_path),
                    "equity_curve": str(curve_path),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    LOGGER.error("未知命令: %s", args.command)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
