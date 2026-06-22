#!/usr/bin/env python
"""批量重建过去一年的 12_quant_prefilter_long.csv。

用法:
    python scripts/batch_rebuild_quant_prefilter.py

并行度:
    --workers 4    # 默认 4 路并行
    --dry-run      # 仅列出将要处理的日期
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

PYTHON = sys.executable
MANAGE_SCRIPT = str(PROJECT_ROOT / "scripts" / "manage_selection_system.py")
RUNS_DIR = PROJECT_ROOT / "data" / "selection_runs"


def get_trading_days(start: date, end: date) -> list[str]:
    """使用已有的 selection_runs 目录推断交易日。

    如果某个日期目录下有 12_quant_prefilter_long.csv 或
    factor_store/by_date 下有该日期的 csv，则认为是已处理的交易日。
    同时使用简单的周一到周五过滤。
    """
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # 周一到周五
            days.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return days


def run_step(date_str: str, step: str) -> tuple[str, str, bool]:
    """运行单个 pipeline 步骤。返回 (date, step, success)。"""
    cmd = [PYTHON, MANAGE_SCRIPT, step, "--date", date_str]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        ok = result.returncode == 0
        if not ok:
            print(f"  [{date_str}] {step} FAILED (exit={result.returncode})", flush=True)
            if result.stderr:
                # 只打印最后几行错误
                lines = result.stderr.strip().split("\n")
                for line in lines[-5:]:
                    print(f"    {line}")
        return date_str, step, ok
    except subprocess.TimeoutExpired:
        print(f"  [{date_str}] {step} TIMEOUT")
        return date_str, step, False
    except Exception as exc:
        print(f"  [{date_str}] {step} ERROR: {exc}")
        return date_str, step, False


def process_date(date_str: str) -> bool:
    """对一个日期执行完整的三步流水线。"""
    output_file = RUNS_DIR / date_str / "12_quant_prefilter_long.csv"

    print(f"  [{date_str}] 开始...", flush=True)

    # Step 1: build-factor-store
    _, _, ok = run_step(date_str, "build-factor-store")
    if not ok:
        return False

    # Step 2: build-factor-scores
    _, _, ok = run_step(date_str, "build-factor-scores")
    if not ok:
        return False

    # Step 3: build-quant-prefilter
    _, _, ok = run_step(date_str, "build-quant-prefilter")
    if not ok:
        return False

    if output_file.exists():
        size = output_file.stat().st_size
        print(f"  [{date_str}] ✓ 完成 ({size:,} bytes)", flush=True)
        return True
    else:
        print(f"  [{date_str}] ✗ 输出文件不存在", flush=True)
        return False


def main():
    parser = argparse.ArgumentParser(description="批量重建量化预筛选文件")
    parser.add_argument("--workers", type=int, default=4, help="并行 worker 数")
    parser.add_argument("--dry-run", action="store_true", help="仅列出日期，不执行")
    parser.add_argument("--start", type=str, default="2025-06-18", help="起始日期")
    parser.add_argument("--end", type=str, default=None, help="结束日期(默认今天)")
    args = parser.parse_args()

    end_str = args.end or date.today().strftime("%Y-%m-%d")
    end_date = date.fromisoformat(end_str)
    start_date = date.fromisoformat(args.start) if args.start else end_date - timedelta(days=365)

    all_days = get_trading_days(start_date, end_date)

    # 过滤掉已存在的
    todo = []
    skipped = 0
    for d in all_days:
        output = RUNS_DIR / d / "12_quant_prefilter_long.csv"
        if output.exists():
            skipped += 1
        else:
            todo.append(d)

    print(f"日期范围: {args.start} ~ {end_str}")
    print(f"交易日总数: {len(all_days)}")
    print(f"已存在: {skipped}, 待处理: {len(todo)}")

    if args.dry_run:
        print(f"\n待处理日期 ({len(todo)}):")
        for d in todo:
            print(f"  {d}")
        return

    if not todo:
        print("无需处理 ✓")
        return

    print(f"\n开始处理，{args.workers} 路并行...")
    success = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_date, d): d for d in todo}
        for future in concurrent.futures.as_completed(futures):
            d = futures[future]
            try:
                if future.result():
                    success += 1
                else:
                    failed += 1
            except Exception as exc:
                print(f"  [{d}] 异常: {exc}")
                failed += 1
            print(f"  进度: {success + failed}/{len(todo)} (成功={success}, 失败={failed})")

    print(f"\n完成: 成功={success}, 失败={failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
