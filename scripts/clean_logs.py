"""Prune old log directories/files so ``logs/`` stays bounded."""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import (  # noqa: E402
    DEFAULT_RUN_LOG_BASE,
    DEFAULT_RUN_LOG_KEEP_DAYS,
    prune_run_logs,
)


LEGACY_LOG_DIRS = (
    "main_scripts",
    "services",
    "research",
    "selection_system",
    "industry_research",
    "analysis",
    "tools",
    "unknown_model",
)


def _prune_debug_logs(*, keep_days: int) -> int:
    debug_root = PROJECT_ROOT / "logs" / "debug"
    if not debug_root.exists():
        return 0
    cutoff = time.time() - keep_days * 86400
    removed = 0
    for path in sorted(debug_root.rglob("*"), reverse=True):
        try:
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
            elif path.is_dir() and not any(path.iterdir()):
                path.rmdir()
        except OSError:
            continue
    return removed


def _purge_legacy(*, yes: bool) -> list[str]:
    targets = [PROJECT_ROOT / "logs" / name for name in LEGACY_LOG_DIRS]
    existing = [path for path in targets if path.exists()]
    if not existing:
        return []
    if not yes:
        print("以下历史目录将被删除（需 --yes 确认）:")
        for path in existing:
            print(f"  - {path}")
        return []
    for path in existing:
        shutil.rmtree(path, ignore_errors=True)
    return [str(path) for path in existing]


def main() -> int:
    parser = argparse.ArgumentParser(description="清理 logs 下的历史运行日志")
    parser.add_argument(
        "--keep-days",
        type=int,
        default=DEFAULT_RUN_LOG_KEEP_DAYS,
        help=f"保留最近多少天的运行日志，默认 {DEFAULT_RUN_LOG_KEEP_DAYS}",
    )
    parser.add_argument("--base-dir", default=DEFAULT_RUN_LOG_BASE, help="运行日志根目录")
    parser.add_argument("--include-debug", action="store_true", help="同时按 mtime 清理 logs/debug")
    parser.add_argument(
        "--purge-legacy",
        action="store_true",
        help="一并删除旧的按组件/模型分层目录（main_scripts、services、unknown_model 等）",
    )
    parser.add_argument("--yes", action="store_true", help="确认执行 --purge-legacy")
    args = parser.parse_args()

    removed_runs = prune_run_logs(keep_days=args.keep_days, base_dir=args.base_dir)
    print(f"runs 清理: 删除 {removed_runs} 个日期目录 (keep_days={args.keep_days})")

    if args.include_debug:
        removed_debug = _prune_debug_logs(keep_days=args.keep_days)
        print(f"debug 清理: 删除 {removed_debug} 个文件/空目录")

    if args.purge_legacy:
        purged = _purge_legacy(yes=args.yes)
        if purged:
            print(f"legacy 目录已删除 {len(purged)} 个")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
