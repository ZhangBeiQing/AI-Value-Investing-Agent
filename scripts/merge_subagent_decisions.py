#!/usr/bin/env python3
"""合并 subagent 单股 decision 文件到 05_decision.json。

每个 subagent 分析完一只股票后，将结果写入：
  data/skill_runs/{date}/{book_type}/subagent_result/{stock_name}_{symbol}_{date}_decision.json

本脚本负责：
1. 读取已有的 05_decision.json（继承基线）
2. 遍历 subagent_result/ 下所有单股 decision 文件
3. 按 symbol 匹配后整条替换 stock_decisions 中的对应 entry（新 symbol 则追加）
4. 写入合并后的 05_decision.json
5. 输出合并摘要

用法：
  python scripts/merge_subagent_decisions.py --date 2026-04-28 --book-type fixed_tracked
  python scripts/merge_subagent_decisions.py --date 2026-04-28 --book-type short_book
  python scripts/merge_subagent_decisions.py --date 2026-04-28 --book-type long_book
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SKILL_RUNS = PROJECT_ROOT / "data" / "skill_runs"

BOOK_TYPES = ("fixed_tracked", "short_book", "long_book")


def find_decision_files(subagent_dir: Path, date: str) -> list[Path]:
    """扫描 subagent_result 目录，返回所有匹配日期的 _decision.json 文件。"""
    if not subagent_dir.exists():
        return []
    files = sorted(subagent_dir.glob("*_decision.json"))
    # 过滤：文件名必须以 _YYYY-MM-DD_decision.json 结尾
    suffix = f"_{date}_decision.json"
    return [f for f in files if f.name.endswith(suffix)]


def load_baseline(baseline_path: Path) -> dict:
    """加载已有 05_decision.json 作为基线。"""
    if baseline_path.exists():
        with open(baseline_path, "r", encoding="utf-8") as f:
            return json.load(f)
    # 冷启动：返回空骨架
    return {
        "summary_date": "",
        "stock_decisions": [],
        "system_risk_notes": [],
        "system_focus_items": [],
    }


def merge_decisions(baseline: dict, subagent_files: list[Path]) -> dict:
    """将 subagent 单股 decision 合并到 baseline 的 stock_decisions 中。"""
    decisions = baseline.get("stock_decisions", [])
    # 建立 symbol -> index 映射
    symbol_index: dict[str, int] = {}
    for i, entry in enumerate(decisions):
        sym = entry.get("symbol", "")
        if sym:
            symbol_index[sym] = i

    replaced: list[str] = []
    appended: list[str] = []
    errors: list[str] = []

    for file_path in subagent_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                entry = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            errors.append(f"{file_path.name}: 读取失败 — {e}")
            continue

        symbol = entry.get("symbol", "")
        if not symbol:
            errors.append(f"{file_path.name}: 缺少 symbol 字段，跳过")
            continue

        if symbol in symbol_index:
            idx = symbol_index[symbol]
            decisions[idx] = entry
            replaced.append(symbol)
        else:
            decisions.append(entry)
            symbol_index[symbol] = len(decisions) - 1
            appended.append(symbol)

    baseline["stock_decisions"] = decisions
    return baseline, replaced, appended, errors


def update_analysis_index(merged: dict, book_type: str, date: str, base_dir: Path) -> None:
    """更新持久化分析索引，让主 agent 跨交易日知道每只股票的最后分析日期。"""
    index_path = base_dir / "_analysis_index.json"
    if index_path.exists():
        with open(index_path, "r", encoding="utf-8") as f:
            index = json.load(f)
    else:
        index = {}

    if book_type not in index:
        index[book_type] = {}

    for entry in merged.get("stock_decisions", []):
        symbol = entry.get("symbol", "")
        if not symbol:
            continue
        index[book_type][symbol] = {
            "deep_analysis_date": date,
            "price_impression": entry.get("price_impression", ""),
            "confidence_score": entry.get("confidence_score", 0),
        }

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"  分析索引已更新: {index_path} ({book_type} -> {len(index[book_type])} 只)")

def main() -> int:
    parser = argparse.ArgumentParser(description="合并 subagent 单股 decision 到 05_decision.json")
    parser.add_argument("--date", required=True, help="交易日 YYYY-MM-DD")
    parser.add_argument(
        "--book-type",
        required=True,
        choices=BOOK_TYPES,
        help="账本类型: fixed_tracked / short_book / long_book",
    )
    parser.add_argument(
        "--base-dir",
        default=str(DEFAULT_SKILL_RUNS),
        help=f"skill_runs 根目录，默认 {DEFAULT_SKILL_RUNS}",
    )
    args = parser.parse_args()

    book_dir = Path(args.base_dir) / args.date / args.book_type
    decision_path = book_dir / "05_decision.json"
    subagent_dir = book_dir / "subagent_result"

    if not book_dir.exists():
        print(f"错误: 目录不存在 — {book_dir}")
        return 1

    # 加载基线
    baseline = load_baseline(decision_path)
    orig_count = len(baseline.get("stock_decisions", []))

    # 扫描 subagent 文件
    subagent_files = find_decision_files(subagent_dir, args.date)

    if not subagent_files:
        print(f"subagent_result/ 下无匹配日期的 decision 文件，无需合并")
        print(f"  (搜索目录: {subagent_dir})")
        return 0

    # 合并
    merged, replaced, appended, errors = merge_decisions(baseline, subagent_files)

    # 确保 subagent 写入的 entry 没有遗漏 stock_name（从文件名推断）
    for entry in merged.get("stock_decisions", []):
        if not entry.get("stock_name"):
            symbol = entry.get("symbol", "UNKNOWN")
            entry["stock_name"] = symbol

    # 写入
    with open(decision_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # 更新持久化分析索引
    update_analysis_index(merged, args.book_type, args.date, Path(args.base_dir))

    # 打印摘要
    new_count = len(merged.get("stock_decisions", []))
    print(f"合并完成 → {decision_path}")
    print(f"  subagent 文件数: {len(subagent_files)}")
    print(f"  替换 entry: {len(replaced)} 只 ({', '.join(replaced) if replaced else '无'})")
    print(f"  新增 entry: {len(appended)} 只 ({', '.join(appended) if appended else '无'})")
    print(f"  stock_decisions: {orig_count} → {new_count}")

    if errors:
        print(f"  错误: {len(errors)} 个")
        for err in errors:
            print(f"    - {err}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
