#!/usr/bin/env python3
"""Incrementally or fully refresh operation_summary.json."""

import argparse
from typing import List
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from trade_summary import (
    process_and_merge_operations,
    read_json_file,
    _stock_operations_file,
)


def load_ops_for_date(signature: str, summary_date: str) -> List[dict]:
    path = _stock_operations_file(signature)
    all_ops = read_json_file(path)
    if not summary_date:
        return all_ops
    return [op for op in all_ops if op.get("operation_date") == summary_date]


def main() -> None:
    parser = argparse.ArgumentParser(description="Update operation_summary.json")
    parser.add_argument("--signature", default="deepseek-reasoner", help="agent signature, e.g. deepseek-reasoner")
    parser.add_argument(
        "--date",
        help="target operation_date. If omitted, perform full rebuild",
    )
    args = parser.parse_args()

    if args.date:
        ops = load_ops_for_date(args.signature, args.date)
        if not ops:
            print(f"No operations found for {args.date}")
            return
        process_and_merge_operations(args.signature, ops)
    else:
        process_and_merge_operations(args.signature)


if __name__ == "__main__":
    # 增量更新
    # python scripts/update_operation_summary.py deepseek-reasoner --date 2025-06-16
    # 全量更新
    # python scripts/update_operation_summary.py deepseek-reasoner
    main()
