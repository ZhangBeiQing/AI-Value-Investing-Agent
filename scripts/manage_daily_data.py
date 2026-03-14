#!/usr/bin/env python3
"""Unified daily data refresh entry point."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from basic_stock_info import DEFAULT_PRICE_LOOKBACK_DAYS  # type: ignore
from configs.stock_pool import TRACKED_A_STOCKS  # type: ignore
from core.logging import init_component_logger  # type: ignore
from shared_data_access.data_access import SharedDataAccess  # type: ignore
from utlity import ensure_stock_subdir, parse_symbol  # type: ignore

LOG_DIR = PROJECT_ROOT / "logs" / "main_scripts" / "ManageDailyData"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOGGER = init_component_logger("ManageDailyData", group="main_scripts", filename_prefix="manage_daily_data")

DEFAULT_INDEX_SYMBOL = os.getenv("PRICE_DYNAMICS_INDEX", "000001.IDX")


def _load_symbols_from_file(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"symbols file not found: {path}")
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return [line for line in lines if line]


def collect_target_symbols(args: argparse.Namespace) -> List[str]:
    if args.symbols:
        return [s.strip() for s in args.symbols if s.strip()]
    if args.symbols_file:
        return _load_symbols_from_file(Path(args.symbols_file))
    return [entry.symbol for entry in TRACKED_A_STOCKS]


def refresh_shared_data(
    target_date: str,
    symbols: Sequence[str],
    *,
    force_refresh_prices: bool,
    force_refresh_financials: bool,
    log_file: Path,
) -> Dict[str, object]:
    start = time.time()
    sda = SharedDataAccess(
        base_dir=None,
        logger=LOGGER,
        price_lookback_days=DEFAULT_PRICE_LOOKBACK_DAYS,
    )
    skip_financial_refresh = force_refresh_prices and not force_refresh_financials
    force_price_flag = force_refresh_prices or force_refresh_financials
    with log_file.open("a", encoding="utf-8") as log:
        log.write(f"[shared_data] start {target_date} | symbols={len(symbols)}")
        for symbol in symbols:
            info = parse_symbol(symbol)
            log.write(f"  >> refresh {info.symbol}")
            sda.prepare_dataset(
                symbolInfo=info,
                as_of_date=target_date,
                force_refresh=force_refresh_financials,
                force_refresh_price=force_price_flag,
                force_refresh_financials=force_refresh_financials,
                skip_financial_refresh=skip_financial_refresh,
            )
        log.write("[shared_data] done")
    return {
        "name": "refresh_shared_data",
        "status": "success",
        "duration_sec": round(time.time() - start, 2),
    }


def ensure_manual_research_dirs(symbols: Sequence[str]) -> None:
    for symbol in symbols:
        info = parse_symbol(symbol)
        ensure_stock_subdir(info, "financial_reports")
        ensure_stock_subdir(info, "forecast")


def run_subprocess(step_name: str, cmd: Sequence[str], log_file: Path) -> Dict[str, object]:
    start = time.time()
    # Popen usage to stream output
    with log_file.open("a", encoding="utf-8") as log:
        log.write(f"[{step_name}] CMD: {' '.join(cmd)}\n")
        log.flush()
        
        process = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Merge stderr into stdout
            text=True,
            bufsize=1,  # Line buffered
        )
        
        # Read lines as they come
        if process.stdout:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log.write(line)
                log.flush()
        
        ret = process.wait()
        log.write(f"[{step_name}] done with return code {ret}\n")
        
        if ret != 0:
            raise subprocess.CalledProcessError(ret, cmd)

    return {
        "name": step_name,
        "status": "success",
        "command": " ".join(cmd),
        "duration_sec": round(time.time() - start, 2),
    }


def manage_daily_data(args: argparse.Namespace) -> int:
    target_date = args.date or datetime.now().strftime("%Y-%m-%d")
    log_file = LOG_DIR / f"refresh_{target_date.replace('-', '')}.log"
    symbols = collect_target_symbols(args)
    refresh_symbols = list(symbols)
    if DEFAULT_INDEX_SYMBOL and DEFAULT_INDEX_SYMBOL not in refresh_symbols:
        refresh_symbols.append(DEFAULT_INDEX_SYMBOL)
    steps: List[Dict[str, object]] = []
    status = {"date": target_date, "signature": args.signature, "steps": steps, "status": "success"}

    try:
        ensure_manual_research_dirs(symbols)
        LOGGER.info("开始刷新 shared data: date=%s, symbols=%d", target_date, len(refresh_symbols))
        steps.append(
            refresh_shared_data(
                target_date,
                refresh_symbols,
                force_refresh_prices=args.force_refresh_price,
                force_refresh_financials=args.force_refresh,
                log_file=log_file,
            )
        )
        LOGGER.info("shared data 刷新完成")

        basic_cmd = [
            sys.executable,
            "-u",
            "basic_stock_info.py",
            "--today-time",
            target_date,
            "--get-look-back-days",
            str(args.look_back_days),
            "--max-workers",
            str(args.max_workers),
            "--symbols",
            *symbols,
        ]
        LOGGER.info("开始运行 basic_stock_info")
        steps.append(run_subprocess('basic_stock_info', basic_cmd, log_file))
        LOGGER.info("basic_stock_info 完成")

        LOGGER.info("开始运行 get_daily_price")
        steps.append(run_subprocess('get_daily_price', [sys.executable, "-u", 'data/get_daily_price.py'], log_file))
        LOGGER.info("get_daily_price 完成")
        LOGGER.info("开始运行 merge_jsonl")
        steps.append(run_subprocess('merge_jsonl', [sys.executable, "-u", 'data/merge_jsonl.py'], log_file))
        LOGGER.info("merge_jsonl 完成")

        status_path = LOG_DIR / "latest_status.json"
        status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

        # disclosures_builder
        # 如果 args.symbols 被指定，或者 symbols 列表数量少于 TRACKED_A_STOCKS 总数，
        # 则说明是部分更新，应该逐个调用（或修改 builder 支持列表，但这里我们先逐个调用以支持现有逻辑）
        # 注意：disclosures_builder 目前只支持 --all 或 --symbol 单个
        
        # 简单判断：如果 args.symbols 或 args.symbols_file 存在，则视为部分更新
        try:
            if args.symbols or args.symbols_file:
                 LOGGER.info("开始按指定股票运行 disclosures_builder: symbols=%d", len(symbols))
                 for sym in symbols:
                    cmd = [
                        sys.executable, "-u", "news/disclosures_builder.py",
                        "--symbol", sym,
                        "--model", "qwen-doc-turbo",
                        "--audit-model", "deepseek-v3.2-exp"
                    ]
                    steps.append(run_subprocess(f'disclosures_builder_{sym}', cmd, log_file))
            else:
                disclosures_cmd = [sys.executable, "-u", "news/disclosures_builder.py", "--all", "--model", "qwen-doc-turbo", "--audit-model", "deepseek-v3.2-exp"]
                LOGGER.info("开始全量运行 disclosures_builder")
                steps.append(run_subprocess('disclosures_builder_all', disclosures_cmd, log_file))
            LOGGER.info("disclosures_builder 完成")
        except subprocess.CalledProcessError as exc:
            warning_message = f"disclosures_builder failed and was skipped: {exc}"
            steps.append({"name": "disclosures_builder", "status": "warning", "message": warning_message})
            status["status"] = "warning"
            LOGGER.warning("%s", warning_message)

        return 0
    except Exception as exc:  # pragma: no cover - top-level guard
        steps.append({"name": "error", "status": "failed", "message": str(exc)})
        status["status"] = "failed"
        status_path = LOG_DIR / "latest_status.json"
        status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        LOGGER.exception("data refresh failed: %s", exc)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run unified daily data refresh pipeline")
    parser.add_argument("--date", help="Target date YYYY-MM-DD (default: today)")
    parser.add_argument("--signature", default="deepseek-reasoner", help="Agent signature for downstream scripts")
    parser.add_argument("--symbols", nargs="*", help="Override stock symbols list")
    parser.add_argument("--symbols-file", help="Path to file listing symbols (one per line)")
    parser.add_argument("--force-refresh", action="store_true", help="Force refresh shared data cache")
    parser.add_argument(
        "--force-refresh-price",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Force refresh price cache for all target symbols (default: disabled)",
    )
    parser.add_argument("--max-workers", type=int, default=4, help="basic_stock_info max workers")
    parser.add_argument("--look-back-days", type=int, default=0, help="basic_stock_info look back days")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(manage_daily_data(args))


if __name__ == "__main__":
    main()
