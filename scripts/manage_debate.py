#!/usr/bin/env python3
"""Prepare and validate the Bull/Bear/Adjudicator workflow."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.debate_pipeline import prepare_debate_directories, validate_debate_artifacts, write_review_context


def main() -> int:
    parser = argparse.ArgumentParser(description="准备或校验独立研究与单裁决产物")
    parser.add_argument("command", choices=("prepare", "validate"))
    parser.add_argument("--date", required=True)
    parser.add_argument("--book-type", default="fixed_tracked", choices=("fixed_tracked", "us_tracked"))
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--base-dir", default="data/skill_runs")
    parser.add_argument("--position-shares", type=float, help="prepare 必填：本账本当日实际持股数量")
    parser.add_argument("--require-verdict", action="store_true")
    args = parser.parse_args()
    book_dir = Path(args.base_dir) / args.date / args.book_type
    if not book_dir.is_dir():
        parser.error(f"账本目录不存在: {book_dir}")
    if args.command == "prepare":
        if args.position_shares is None:
            parser.error("prepare 必须提供 --position-shares")
        root = prepare_debate_directories(book_dir, args.symbol)
        write_review_context(book_dir, args.symbol, position_shares=args.position_shares)
        print(f"角色目录和当日持仓上下文已准备，尚未生成研究或决策: {root}")
        return 0
    errors = validate_debate_artifacts(book_dir, args.symbol, require_verdict=args.require_verdict)
    if errors:
        print("单裁决校验失败:\n" + "\n".join(f"- {error}" for error in errors))
        return 1
    print(f"单裁决校验通过: {args.symbol}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
