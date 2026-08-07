#!/usr/bin/env python3
"""合并单股 decision 文件到 05_decision.json。

每个 subagent 分析完一只股票后，将结果写入：
  data/skill_runs/{date}/{book_type}/subagent_result/{stock_name}_{symbol}_{date}_decision.json

本脚本负责：
1. 读取已有的 05_decision.json（继承基线）
2. 遍历旧 subagent_result 或新 debate final verdict
3. 按 symbol 匹配后整条替换 stock_decisions 中的对应 entry（新 symbol 则追加）
4. 写入合并后的 05_decision.json
5. 输出合并摘要

用法：
  python scripts/merge_subagent_decisions.py --date 2026-04-28 --book-type fixed_tracked
  python scripts/merge_subagent_decisions.py --date 2026-04-28 --book-type fixed_tracked --source debate
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
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.decision_contract import validate_stock_decision_entry
from services.trading.debate_pipeline import validate_debate_artifacts

BOOK_TYPES = ("fixed_tracked", "short_book", "long_book")


def find_decision_files(subagent_dir: Path, date: str) -> list[Path]:
    """扫描 subagent_result 目录，返回所有匹配日期的 _decision.json 文件。"""
    if not subagent_dir.exists():
        return []
    files = sorted(subagent_dir.glob("*_decision.json"))
    # 过滤：文件名必须以 _YYYY-MM-DD_decision.json 结尾
    suffix = f"_{date}_decision.json"
    return [f for f in files if f.name.endswith(suffix)]


def find_debate_verdict_files(debate_dir: Path) -> list[Path]:
    """扫描每个 symbol 的唯一 final verdict。"""
    if not debate_dir.exists():
        return []
    return sorted(
        path
        for path in debate_dir.glob("*/final/stock_verdict.json")
        if path.is_file()
    )


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


def merge_decisions(
    baseline: dict,
    subagent_files: list[Path],
    *,
    validate_debate: bool = False,
) -> tuple[dict, list[str], list[str], list[str]]:
    """将单股 decision 合并到 baseline 的 stock_decisions 中。"""
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
        if validate_debate:
            verdict_errors = validate_stock_decision_entry(
                entry,
                # 辩论目录现在使用“股票名称_symbol”，不能再把目录名当作代码。
                expected_symbol=symbol,
            )
            if verdict_errors:
                errors.extend(
                    f"{file_path}: {error}"
                    for error in verdict_errors
                )
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


def _snapshot_price_map(base_dir: Path, book_type: str, date: str) -> dict[str, float]:
    """从某交易日的 02_basic_snapshot_payload.json 读取 symbol -> 当日收盘价。"""
    snapshot_path = base_dir / date / book_type / "02_basic_snapshot_payload.json"
    if not snapshot_path.exists():
        return {}
    try:
        with open(snapshot_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    stocks = payload.get("stocks", {}) or {}
    price_map: dict[str, float] = {}
    for symbol, fields in stocks.items():
        price = fields.get("latest_price")
        if isinstance(price, (int, float)):
            price_map[symbol] = float(price)
    return price_map


def update_analysis_index(merged: dict, book_type: str, date: str, base_dir: Path) -> None:
    """更新持久化分析索引，让主 agent 跨交易日知道每只股票的最后分析日期、当日股价与价格印象。

    索引条目字段：
    - deep_analysis_date：最近一次深度分析的交易日
    - last_deep_analysis_price：该次分析当天的收盘价（来自当日 02_basic_snapshot_payload.json 的 latest_price）
    - price_impression：该次分析的价格印象
    - confidence_score：置信度

    历史已有条目若缺失 last_deep_analysis_price，会按各自的 deep_analysis_date 从当日快照回填。
    """
    index_path = base_dir / "_analysis_index.json"
    if index_path.exists():
        with open(index_path, "r", encoding="utf-8") as f:
            index = json.load(f)
    else:
        index = {}

    if book_type not in index:
        index[book_type] = {}

    today_price_map = _snapshot_price_map(base_dir, book_type, date)

    for entry in merged.get("stock_decisions", []):
        symbol = entry.get("symbol", "")
        if not symbol:
            continue
        index[book_type][symbol] = {
            "deep_analysis_date": date,
            "last_deep_analysis_price": today_price_map.get(symbol),
            "price_impression": entry.get("price_impression", ""),
            "confidence_score": entry.get("confidence_score", 0),
        }

    # 回填历史缺失的 last_deep_analysis_price（按各自分析日快照）
    for symbol, entry in index[book_type].items():
        if entry.get("last_deep_analysis_price") is not None:
            continue
        historical_date = entry.get("deep_analysis_date")
        if not historical_date:
            continue
        historical_price = _snapshot_price_map(base_dir, book_type, historical_date).get(
            symbol
        )
        if historical_price is not None:
            entry["last_deep_analysis_price"] = historical_price

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
    parser.add_argument(
        "--source",
        default="subagent_result",
        choices=("subagent_result", "debate"),
        help="单股结果来源；默认保留旧 subagent_result，辩论流程显式使用 debate",
    )
    args = parser.parse_args()

    book_dir = Path(args.base_dir) / args.date / args.book_type
    decision_path = book_dir / "05_decision.json"
    if args.source == "debate" and args.book_type != "fixed_tracked":
        print("错误: debate 来源当前只支持 fixed_tracked")
        return 1

    if not book_dir.exists():
        print(f"错误: 目录不存在 — {book_dir}")
        return 1

    # 加载基线
    baseline = load_baseline(decision_path)
    orig_count = len(baseline.get("stock_decisions", []))

    if args.source == "debate":
        source_dir = book_dir / "debate"
        subagent_files = find_debate_verdict_files(source_dir)
    else:
        source_dir = book_dir / "subagent_result"
        subagent_files = find_decision_files(source_dir, args.date)

    if not subagent_files:
        print(f"{args.source} 下无单股 decision 文件，无需合并")
        print(f"  (搜索目录: {source_dir})")
        return 0

    if args.source == "debate":
        artifact_errors: list[str] = []
        for verdict_path in subagent_files:
            try:
                verdict_payload = json.loads(
                    verdict_path.read_text(encoding="utf-8")
                )
            except (json.JSONDecodeError, OSError) as exc:
                artifact_errors.append(f"{verdict_path}: 读取失败 — {exc}")
                continue
            symbol = verdict_payload.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                artifact_errors.append(f"{verdict_path}: 缺少合法 symbol 字段")
                continue
            artifact_errors.extend(
                validate_debate_artifacts(
                    book_dir,
                    symbol,
                    require_verdict=True,
                )
            )
        if artifact_errors:
            print("辩论链路校验失败，未写入 05_decision.json:")
            for err in artifact_errors:
                print(f"  - {err}")
            return 1

    # 合并
    merged, replaced, appended, errors = merge_decisions(
        baseline,
        subagent_files,
        validate_debate=args.source == "debate",
    )
    if args.source == "debate" and errors:
        print("辩论 verdict 校验失败，未写入 05_decision.json:")
        for err in errors:
            print(f"  - {err}")
        return 1

    # 确保 subagent 写入的 entry 没有遗漏 stock_name（从文件名推断）
    for entry in merged.get("stock_decisions", []):
        if not entry.get("stock_name"):
            symbol = entry.get("symbol", "UNKNOWN")
            entry["stock_name"] = symbol

    merged["summary_date"] = args.date

    # 写入；该命令只应在人工确认后调用。
    with open(decision_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # 更新持久化分析索引
    update_analysis_index(merged, args.book_type, args.date, Path(args.base_dir))

    # 打印摘要
    new_count = len(merged.get("stock_decisions", []))
    print(f"合并完成 → {decision_path}")
    print(f"  来源: {args.source}")
    print(f"  单股文件数: {len(subagent_files)}")
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
