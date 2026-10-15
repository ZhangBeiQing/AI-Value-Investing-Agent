#!/usr/bin/env python3
"""Generate weekly scoring prompts, validate results or merge observation rankings."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import bootstrap_run_logging_from_argv

bootstrap_run_logging_from_argv("weekly_scoring")

from core.logging import get_logger
from services.ranking.weekly_scoring import collect_weekly_scores, generate_scoring_tasks, merge_weekly_scores

LOGGER = get_logger("WeeklyScoring")


def main() -> int:
    parser = argparse.ArgumentParser(description="每周 fixed_tracked 全池评分：生成派单、格式校验与合并排名。")
    parser.add_argument("--date", default=date.today().isoformat(), help="分析日 YYYY-MM-DD，必须匹配既有研究包日期。")
    parser.add_argument("--base-dir", default="data", help="数据根目录，包含 skill_runs 与 weekly_scoring。")
    parser.add_argument("--input-base-dir", help="可选独立输入数据根目录，用于读取正式研究包并将输出写到隔离 base-dir。")
    parser.add_argument("--symbols", nargs="+", help="限定该日池内的标准股票代码。")
    parser.add_argument(
        "--pool-source",
        choices=["fixed_tracked", "universe"],
        default="fixed_tracked",
        help="评分股票池来源：fixed_tracked（默认）或 universe（master_universe 全宇宙）。",
    )
    parser.add_argument("--json", action="store_true", help="输出结构化结果，默认生成阶段打印全部 prompt。")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--generate", action="store_true", help="生成派单（默认模式）。")
    mode.add_argument("--merge-only", action="store_true", help="跳过生成，校验并合并评分文件。")
    mode.add_argument("--validate-only", action="store_true", help="只校验评分，不修改排名文件。")
    args = parser.parse_args()
    try:
        if args.merge_only:
            result = merge_weekly_scores(args.date, base_dir=args.base_dir, symbols=args.symbols, pool_source=args.pool_source)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result["items"] else 1
        if args.validate_only:
            result = collect_weekly_scores(args.date, base_dir=args.base_dir, symbols=args.symbols, pool_source=args.pool_source)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return int(not result["items"] or bool(result["invalid"] or result["missing_symbols"] or result["skipped"]))
        result = generate_scoring_tasks(args.date, base_dir=args.base_dir, input_base_dir=args.input_base_dir, symbols=args.symbols, pool_source=args.pool_source)
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            for task in result["tasks"]:
                print(task["prompt"] + "\n")
        return 0 if result["tasks"] else 1
    except (OSError, ValueError, TypeError, KeyError) as exc:
        LOGGER.error("周度评分失败: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
