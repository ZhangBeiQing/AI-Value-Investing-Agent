#!/usr/bin/env python3
"""Unified daily data refresh entry point."""

from __future__ import annotations

import argparse
import concurrent.futures
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
from shared_data_access.exceptions import SymbolNotListedAsOfDateError  # type: ignore
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
    max_workers: int,
) -> Dict[str, object]:
    start = time.time()
    macro_sda = SharedDataAccess(
        base_dir=None,
        logger=LOGGER,
        price_lookback_days=DEFAULT_PRICE_LOOKBACK_DAYS,
    )
    macro_panel = macro_sda.build_macro_objective_panel(
        target_date,
        force_refresh=force_refresh_prices or force_refresh_financials,
    )
    skip_financial_refresh = force_refresh_prices and not force_refresh_financials
    force_price_flag = force_refresh_prices or force_refresh_financials

    def _refresh_symbol(
        symbol: str,
    ) -> tuple[str, str | None, str | None]:
        try:
            info = parse_symbol(symbol)
            local_sda = SharedDataAccess(
                base_dir=None,
                logger=LOGGER,
                price_lookback_days=DEFAULT_PRICE_LOOKBACK_DAYS,
            )
            local_sda.prepare_dataset(
                symbolInfo=info,
                as_of_date=target_date,
                force_refresh=force_refresh_financials,
                force_refresh_price=force_price_flag,
                force_refresh_financials=force_refresh_financials,
                skip_financial_refresh=skip_financial_refresh,
            )
            return info.symbol, None, None
        except SymbolNotListedAsOfDateError as exc:
            return exc.symbol, None, str(exc)
        except Exception as exc:
            return symbol, str(exc), None

    worker_count = max(1, min(int(max_workers or 1), len(symbols) or 1))
    failures: Dict[str, str] = {}
    skipped_not_listed: Dict[str, str] = {}
    with log_file.open("a", encoding="utf-8") as log:
        log.write(f"[shared_data] start {target_date} | symbols={len(symbols)}\n")
        log.write(
            "[shared_data] macro_objective_panel status="
            f"{len(macro_panel.get('indicators', {}))} indicators, "
            f"{len(macro_panel.get('central_banks', {}))} central_banks\n"
        )
        if worker_count <= 1:
            for symbol in symbols:
                symbol_name, error, skipped_reason = _refresh_symbol(symbol)
                log.write(f"  >> refresh {symbol_name}\n")
                if skipped_reason:
                    skipped_not_listed[symbol_name] = skipped_reason
                    log.write(
                        f"  -- skipped_not_listed {symbol_name}: "
                        f"{skipped_reason}\n"
                    )
                if error:
                    failures[symbol_name] = error
                    log.write(f"  !! failed {symbol_name}: {error}\n")
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_map = {
                    executor.submit(_refresh_symbol, symbol): symbol
                    for symbol in symbols
                }
                for future in concurrent.futures.as_completed(future_map):
                    raw_symbol = future_map[future]
                    symbol_name, error, skipped_reason = future.result()
                    log.write(f"  >> refresh {symbol_name or raw_symbol}\n")
                    if skipped_reason:
                        skipped_not_listed[symbol_name or raw_symbol] = skipped_reason
                        log.write(
                            f"  -- skipped_not_listed "
                            f"{symbol_name or raw_symbol}: {skipped_reason}\n"
                        )
                    if error:
                        failures[symbol_name or raw_symbol] = error
                        log.write(f"  !! failed {symbol_name or raw_symbol}: {error}\n")
        log.write("[shared_data] done\n")
    if failures:
        details = "\n".join(f"- {symbol}: {message}" for symbol, message in sorted(failures.items()))
        raise RuntimeError(f"shared data 刷新失败:\n{details}")
    if skipped_not_listed:
        LOGGER.info(
            "历史日期早于上市首个交易日，跳过 %d 只股票: %s",
            len(skipped_not_listed),
            ", ".join(sorted(skipped_not_listed)),
        )
    return {
        "name": "refresh_shared_data",
        "status": "success",
        "duration_sec": round(time.time() - start, 2),
        "macro_objective_panel": {
            "indicator_count": len(macro_panel.get("indicators", {})),
            "central_bank_count": len(macro_panel.get("central_banks", {})),
        },
        "skipped_not_listed_count": len(skipped_not_listed),
        "skipped_not_listed_symbols": sorted(skipped_not_listed),
        "skipped_not_listed_reasons": skipped_not_listed,
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
        shared_refresh_result = refresh_shared_data(
            target_date,
            refresh_symbols,
            force_refresh_prices=args.force_refresh_price,
            force_refresh_financials=args.force_refresh,
            log_file=log_file,
            max_workers=args.max_workers,
        )
        steps.append(shared_refresh_result)
        skipped_not_listed = set(
            shared_refresh_result.get("skipped_not_listed_symbols") or []
        )
        active_symbols = [
            symbol for symbol in symbols if symbol not in skipped_not_listed
        ]
        LOGGER.info("shared data 刷新完成")

        if active_symbols:
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
                *active_symbols,
            ]
            if args.force_refresh:
                basic_cmd.append("--force-refresh-financials")
            else:
                basic_cmd.append("--skip-financial-refresh")
            LOGGER.info(
                "开始运行 basic_stock_info: active=%d skipped_not_listed=%d",
                len(active_symbols),
                len(skipped_not_listed),
            )
            steps.append(run_subprocess("basic_stock_info", basic_cmd, log_file))
            LOGGER.info("basic_stock_info 完成")
        else:
            steps.append(
                {
                    "name": "basic_stock_info",
                    "status": "skipped",
                    "message": "全部目标股票在该历史日期尚未上市",
                }
            )

        status_path = LOG_DIR / "latest_status.json"
        status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

        # disclosures_builder
        # 如果 args.symbols 被指定，或者 symbols 列表数量少于 TRACKED_A_STOCKS 总数，
        # 则说明是部分更新，应该逐个调用（或修改 builder 支持列表，但这里我们先逐个调用以支持现有逻辑）
        # 注意：disclosures_builder 目前只支持 --all 或 --symbol 单个

        if args.skip_disclosures:
            LOGGER.info("按 --skip-disclosures 跳过 disclosures_builder；公告刷新由上层模块（如 manage_selection_system build-announcements）单独负责")
            steps.append({"name": "disclosures_builder", "status": "skipped", "message": "--skip-disclosures 指定，跳过"})
            return 0

        # 简单判断：如果 args.symbols 或 args.symbols_file 存在，则视为部分更新
        try:
            if args.symbols or args.symbols_file:
                if not active_symbols:
                    steps.append(
                        {
                            "name": "disclosures_builder_selected",
                            "status": "skipped",
                            "message": "没有该历史日期已上市的目标股票",
                        }
                    )
                    return 0
                LOGGER.info(
                    "开始按指定股票并发运行 disclosures_builder: symbols=%d",
                    len(active_symbols),
                )
                cmd = [
                    sys.executable,
                    "-u",
                    "news/disclosures_builder.py",
                    "--symbols",
                    *active_symbols,
                    "--model",
                    "qwen-doc-turbo",
                    "--audit-model",
                    "deepseek-v4-flash",
                    "--max-workers",
                    str(args.max_workers),
                ]
                steps.append(run_subprocess("disclosures_builder_selected", cmd, log_file))
            else:
                disclosures_cmd = [
                    sys.executable,
                    "-u",
                    "news/disclosures_builder.py",
                    "--all",
                    "--model",
                    "qwen-doc-turbo",
                    "--audit-model",
                    "deepseek-v4-flash",
                    "--max-workers",
                    str(args.max_workers),
                ]
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
        help="Force refresh price cache for all target symbols (default: enabled)",
    )
    parser.add_argument("--max-workers", type=int, default=4, help="basic_stock_info max workers")
    parser.add_argument("--look-back-days", type=int, default=0, help="basic_stock_info look back days")
    parser.add_argument(
        "--skip-disclosures",
        action="store_true",
        help="跳过 disclosures_builder 阶段；用于公告由上层模块（如 manage_selection_system build-announcements）单独负责的场景",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(manage_daily_data(args))


if __name__ == "__main__":
    main()
