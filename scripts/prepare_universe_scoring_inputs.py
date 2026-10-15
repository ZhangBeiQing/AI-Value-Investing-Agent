#!/usr/bin/env python3
"""Prepare universe scoring inputs (snapshot + research) outside the trading pipeline."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import bootstrap_run_logging_from_argv

bootstrap_run_logging_from_argv("universe_scoring_inputs")

from core.logging import get_logger
from services.pipeline.steps.build_agent_input import build_snapshot_payload
from services.pipeline.steps.build_stock_research import (
    FINANCIAL_REPORT_SELECTION_SLACK_DAYS,
    write_stock_research_bundle,
)
from services.research.financial_report import find_symbols_missing_financial_reports

LOGGER = get_logger("PrepareUniverseScoringInputs")
UNIVERSE_FILE_NAME = "master_universe.json"
FINANCIAL_REPORT_PREPARE_SCRIPT = PROJECT_ROOT / "scripts" / "prepare_financial_report_skill.py"


def _load_universe_symbols(base_dir: Path) -> list[str]:
    path = base_dir / "universe" / UNIVERSE_FILE_NAME
    if not path.is_file():
        raise FileNotFoundError(f"缺少全宇宙文件: {path}")
    document = json.loads(path.read_text(encoding="utf-8"))
    stocks = document.get("stocks") if isinstance(document, dict) else None
    if not isinstance(stocks, list):
        raise ValueError(f"master_universe.stocks 必须是列表: {path}")
    symbols: list[str] = []
    for entry in stocks:
        symbol = entry.get("symbol") if isinstance(entry, dict) else entry
        if isinstance(symbol, str) and symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _run_financial_report_prepare(run_date: str, symbols: list[str]) -> bool:
    """按财报 skill 的准备层为缺失股票生成 workdir（纯 Python，不派发 subagent）。"""
    command = [
        sys.executable,
        str(FINANCIAL_REPORT_PREPARE_SCRIPT),
        "--date",
        run_date,
        "--symbols",
        ",".join(symbols),
        "--sync-first",
        "--json",
    ]
    LOGGER.info("检测到缺财报，运行财报准备层: %s", " ".join(command))
    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    return completed.returncode == 0


def _report_missing_financial_reports(
    run_date: str,
    symbols: list[str],
    report_path: Path,
    prepare_status: str,
) -> None:
    symbol_lines = "\n".join(f"  - {symbol}" for symbol in symbols)
    LOGGER.error(
        "以下 %d 只待分析股票在 %s 之前没有可用财报，已阻止生成研究包：\n%s\n"
        "  财报准备层状态: %s\n"
        "下一步（缺一不可）：\n"
        "  1) 若准备层未成功，先运行: python scripts/prepare_financial_report_skill.py --date %s "
        "--symbols %s --sync-first --json\n"
        "  2) 由主 agent 按 .codex/skills/financial-report-summary/SKILL.md，为 ready_items 逐股撰写 "
        "data/stock_info/{name}_{symbol}/financial_reports/<YYYYMMDD>.md 并注册 summary_index.json\n"
        "  3) 补齐后重跑: python scripts/prepare_universe_scoring_inputs.py --date %s%s\n"
        "缺失清单: %s",
        len(symbols),
        run_date,
        symbol_lines,
        prepare_status,
        run_date,
        ",".join(symbols),
        run_date,
        "",
        report_path,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="为 weekly_scoring --pool-source universe 准备当日快照与研究包，不触碰 fixed_tracked 交易产物。"
    )
    parser.add_argument("--date", default=date.today().isoformat(), help="分析日 YYYY-MM-DD。")
    parser.add_argument("--symbols", nargs="+", help="限定要准备的股票代码；默认 master_universe 全部。")
    parser.add_argument("--limit", type=int, help="默认全宇宙时最多准备前 N 只。")
    parser.add_argument("--base-dir", default="data", help="数据根目录（默认 data）。")
    parser.add_argument("--max-workers", type=int, default=4, help="快照/研究包生成的并发数。")
    parser.add_argument(
        "--ensure-financial-reports",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="生成研究包前校验待分析股票是否已有对应日期财报；缺失则按财报 skill 准备并硬失败（默认启用，"
        "用 --no-ensure-financial-reports 关闭）。",
    )
    parser.add_argument(
        "--prepare-missing-reports",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="检测到缺财报时自动运行 prepare_financial_report_skill.py 生成 workdir（默认启用）。",
    )
    args = parser.parse_args()

    base_root = (PROJECT_ROOT / args.base_dir).resolve() if not Path(args.base_dir).is_absolute() else Path(args.base_dir)
    run_dir = base_root / "skill_runs" / args.date
    universe_dir = run_dir / "universe"

    if args.symbols:
        target = list(dict.fromkeys(args.symbols))
    else:
        target = _load_universe_symbols(base_root)
        if args.limit:
            target = target[: args.limit]

    universe_dir.mkdir(parents=True, exist_ok=True)

    if args.ensure_financial_reports:
        missing = find_symbols_missing_financial_reports(
            target,
            args.date,
            slack_days=FINANCIAL_REPORT_SELECTION_SLACK_DAYS,
        )
        if missing:
            report_path = universe_dir / "_missing_financial_reports.json"
            report_path.write_text(
                json.dumps(
                    {"date": args.date, "checked": len(target), "missing_symbols": missing},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            if args.prepare_missing_reports:
                prepare_status = (
                    "已自动运行 prepare 层（纯 Python，生成 workdir）"
                    if _run_financial_report_prepare(args.date, missing)
                    else "prepare 层执行失败，见上方组件日志"
                )
            else:
                prepare_status = "未运行（--no-prepare-missing-reports）"
            _report_missing_financial_reports(args.date, missing, report_path, prepare_status)
            return 2

    snapshot_path = universe_dir / "02_basic_snapshot_payload.json"
    stock_records: dict[str, object] = {}
    if snapshot_path.is_file():
        existing = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if str(existing.get("timestamp", ""))[:10] == args.date and isinstance(existing.get("stocks"), dict):
            stock_records = existing["stocks"]

    for symbol in target:
        stock_records.setdefault(symbol, {})

    snapshot = build_snapshot_payload(args.date, list(stock_records), max_workers=args.max_workers)
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("universe 快照已写入: %s stocks=%d", snapshot_path, len(snapshot.get("stocks", {})))

    write_stock_research_bundle(
        args.date,
        universe_dir,
        symbols=target,
        book_type="fixed_tracked",
        max_workers=args.max_workers,
    )
    for symbol in target:
        matches = sorted((universe_dir / "04_stock_research").glob(f"*_{symbol}_{args.date}_research.md"))
        LOGGER.info("universe 研究包: symbol=%s files=%d", symbol, len(matches))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
