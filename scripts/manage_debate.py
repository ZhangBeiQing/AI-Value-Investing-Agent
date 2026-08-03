#!/usr/bin/env python3
"""Prepare, aggregate, and validate fixed_tracked debate artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.debate_pipeline import (
    aggregate_jury_votes,
    prepare_debate_directories,
    validate_debate_artifacts,
)


def _book_dir(base_dir: str, run_date: str, book_type: str) -> Path:
    return Path(base_dir) / run_date / book_type


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="管理管理者模式下的逐股辩论产物",
    )
    parser.add_argument(
        "command",
        choices=("prepare", "aggregate", "validate"),
        help=(
            "prepare 只创建空的角色目录；aggregate 汇总三份 ballot；"
            "validate 校验阶段产物"
        ),
    )
    parser.add_argument("--date", required=True, help="分析交易日 YYYY-MM-DD")
    parser.add_argument(
        "--book-type",
        default="fixed_tracked",
        choices=("fixed_tracked",),
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument(
        "--base-dir",
        default="data/skill_runs",
        help="skill_runs 根目录",
    )
    parser.add_argument(
        "--require-verdict",
        action="store_true",
        help="validate 时要求 final/stock_verdict.json 已存在",
    )
    parser.add_argument(
        "--position-shares",
        type=float,
        help="aggregate 必填；当前股票在本账本的实际持股数量",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    book_dir = _book_dir(args.base_dir, args.date, args.book_type)
    if not book_dir.exists():
        print(f"错误: 账本目录不存在 — {book_dir}")
        return 1

    if args.command == "prepare":
        symbol_dir = prepare_debate_directories(book_dir, args.symbol)
        print(f"辩论空目录已准备（未生成任何 JSON）: {symbol_dir}")
        return 0

    if args.command == "aggregate":
        if args.position_shares is None:
            print("错误: aggregate 必须提供 --position-shares")
            return 1
        summary_path, summary = aggregate_jury_votes(
            book_dir,
            args.symbol,
            position_shares=args.position_shares,
        )
        print(f"投票汇总已写入: {summary_path}")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    errors = validate_debate_artifacts(
        book_dir,
        args.symbol,
        require_verdict=args.require_verdict,
    )
    if errors:
        print("辩论产物校验失败:")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"辩论产物校验通过: {args.symbol}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
