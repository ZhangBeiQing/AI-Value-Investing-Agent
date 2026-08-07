#!/usr/bin/env python3
"""Run trade execution and summary merge for a skill output directory."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.post_trade_pipeline import run_post_trade


def main() -> None:
    parser = argparse.ArgumentParser(description="Run post-trade steps for the skill pipeline")
    parser.add_argument("--date", dest="run_date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--output-dir", default="", help="Override skill output directory.")
    parser.add_argument(
        "--book-type",
        default="",
        choices=["", "fixed_tracked", "short_book", "long_book"],
        help="Run post-trade for a specific book output directory.",
    )
    parser.add_argument("--signature", default="")
    parser.add_argument("--confirm", action="store_true", help="Compatibility flag; execution still proceeds directly.")
    parser.add_argument(
        "--backtest-root",
        default="",
        help="显式回测实验目录；指定后只执行隔离的模拟后处理。",
    )
    parser.add_argument(
        "--execution-date",
        default="",
        help="回测实际成交日 YYYY-MM-DD；回测模式必填。",
    )
    parser.add_argument(
        "--execution-price",
        default="open",
        choices=("open",),
        help="回测成交价格口径；第一版只支持 open。",
    )
    args = parser.parse_args()

    if args.backtest_root:
        if not args.execution_date:
            parser.error("--backtest-root 模式必须提供 --execution-date")
        from services.backtest.execution import simulate_post_trade
        from services.backtest.experiment import load_backtest_experiment

        experiment_root = Path(args.backtest_root).resolve()
        experiment = load_backtest_experiment(
            experiment_root.name,
            backtests_root=experiment_root.parent,
        )
        simulate_post_trade(
            experiment,
            decision_date=args.run_date,
            execution_date=args.execution_date,
        )
        return

    run_post_trade(
        args.run_date,
        base_dir=args.base_dir,
        signature=args.signature,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        book_type=args.book_type,
    )


if __name__ == "__main__":
    main()
