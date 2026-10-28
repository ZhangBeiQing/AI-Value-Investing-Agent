#!/usr/bin/env python3
"""为单只股票准备分析所需的全部 Python 链路数据。

`refresh_all_for_date.py` 面向「整个固定股票池」；当只想研究某一只股票时，
它会把全池都刷一遍。本入口把已有的单股能力串成两步，只处理指定标的：

    1. `manage_daily_data.py --symbols <symbol>`：价格、财报结构化缓存、
       basic_info 与该公司自己的公告/财报 PDF（内部走 disclosures_builder --symbols）；
    2. `prepare_financial_report_skill.py --symbols <symbol> --sync-first`：
       生成 `financial_report_workdir/` 全套财报研究输入。

`--symbols` 在这两个脚本里都是排他作用域，不会自动扩到 tracked / queue / 量化初筛，
所以本入口不会触碰其他股票的数据。

典型用法：

    python scripts/prepare_single_stock.py --date 2026-10-28 --symbol 600601.SH
    python scripts/prepare_single_stock.py --symbol 600601.SH          # 默认今天
    python scripts/prepare_single_stock.py --symbol 600601.SH --skip-market-context
    python scripts/prepare_single_stock.py --symbol 600601.SH --only financial
    python scripts/prepare_single_stock.py --symbol 600601.SH --only data
"""

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

from core.logging import (  # noqa: E402
    bootstrap_run_logging_from_argv,
    init_component_logger,
)

bootstrap_run_logging_from_argv("data_prep")

from core.network import install_network_timeouts  # noqa: E402


LOGGER = init_component_logger(
    "PrepareSingleStock",
    group="main_scripts",
    filename_prefix="prepare_single_stock",
)


def _default_date() -> str:
    """默认分析日 = 今天（项目统一口径，不做减一）。"""
    return date.today().strftime("%Y-%m-%d")


def _normalize_symbol(raw: str) -> str:
    """把用户输入规范成 `600601.SH` 形式，兼容纯数字与大小写混写。"""
    text = str(raw or "").strip().upper()
    if not text:
        raise ValueError("--symbol 不能为空")
    if "." in text:
        return text
    if len(text) != 6 or not text.isdigit():
        raise ValueError(
            f"无法识别的股票代码: {raw!r}；请提供带交易所后缀的形式，例如 600601.SH"
        )
    if text.startswith(("600", "601", "603", "605", "688", "689")):
        return f"{text}.SH"
    if text.startswith(("000", "001", "002", "003", "300", "301")):
        return f"{text}.SZ"
    if text.startswith(("4", "8", "9")):
        return f"{text}.BJ"
    raise ValueError(f"无法推断交易所后缀: {raw!r}；请显式提供，例如 600601.SH")


def _run_step(
    name: str,
    cmd: list[str],
    *,
    extra_env: dict[str, str] | None = None,
) -> int:
    """前台运行一个子步骤，输出直接进终端（不吞日志）。"""
    LOGGER.info("▶ [%s] %s", name, " ".join(cmd))
    env = None
    if extra_env:
        import os

        env = {**os.environ, **extra_env}
    completed = subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env)
    LOGGER.info("✓ [%s] 完成，退出码=%s", name, completed.returncode)
    return completed.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="为单只股票准备分析数据：行情/财报缓存 + 公告/财报 PDF + 财报研究 workdir。",
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="股票代码，例如 600601.SH（也接受 600601，会自动补后缀）。",
    )
    parser.add_argument(
        "--date",
        default=_default_date(),
        help="要分析的交易日 YYYY-MM-DD（默认为今天）。",
    )
    parser.add_argument(
        "--only",
        choices=("all", "data", "financial"),
        default="all",
        help=(
            "只跑某一段：data=行情/财报缓存与公告；"
            "financial=财报研究 workdir；all=两段都跑（默认）。"
        ),
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="传给 manage_daily_data，强刷共享数据缓存（含财报结构化数据）。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="shared data / 公告刷新的并发度，默认 4（单股场景无需很高）。",
    )
    parser.add_argument(
        "--skip-market-context",
        action="store_true",
        help="传给财报准备脚本，跳过市场上下文（仅调试用；正式研究不要加）。",
    )
    parser.add_argument(
        "--disclosure-cutoff",
        choices=("next-day", "analysis-day"),
        default="next-day",
        help=(
            "财报公告可见性截断口径，默认 next-day（D 日晚间披露、登记为 D+1 的公告可见）。"
            "历史回测场景才用 analysis-day。"
        ),
    )
    parser.add_argument(
        "--output",
        default="",
        help="可选：把财报准备脚本的 JSON 结果另存到指定路径。",
    )
    return parser


def main() -> int:
    install_network_timeouts()
    args = build_parser().parse_args()

    try:
        symbol = _normalize_symbol(args.symbol)
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 2

    LOGGER.info(
        "开始单股数据准备: symbol=%s date=%s only=%s",
        symbol,
        args.date,
        args.only,
    )

    python = sys.executable
    step_codes: dict[str, int] = {}

    if args.only in ("all", "data"):
        cmd = [
            python,
            "-u",
            "scripts/manage_daily_data.py",
            "--date",
            args.date,
            "--symbols",
            symbol,
            "--max-workers",
            str(args.max_workers),
        ]
        if args.force_refresh:
            cmd.append("--force-refresh")
        step_codes["data"] = _run_step("manage_daily_data", cmd)
        if step_codes["data"] != 0:
            LOGGER.error("行情/公告数据准备失败，停止后续步骤。请查看上方日志。")
            return step_codes["data"]

    if args.only in ("all", "financial"):
        cmd = [
            python,
            "-u",
            "scripts/prepare_financial_report_skill.py",
            "--date",
            args.date,
            "--symbols",
            symbol,
            "--sync-first",
            "--json",
            "--disclosure-cutoff",
            args.disclosure_cutoff,
        ]
        if args.skip_market_context:
            cmd.append("--skip-market-context")
        if args.output:
            cmd.extend(["--output", args.output])
        step_codes["financial"] = _run_step("prepare_financial_report_skill", cmd)
        if step_codes["financial"] != 0:
            LOGGER.error("财报研究 workdir 准备失败，请查看上方日志。")
            return step_codes["financial"]

    LOGGER.info("单股数据准备完成: symbol=%s date=%s", symbol, args.date)
    LOGGER.info(
        "下一步：按 financial-report-summary / auto-trading-fixed-tracked 的流程，"
        "为该股派发 Industry Researcher / Expectation Scout / Financial Author。"
    )
    if args.only in ("all", "financial"):
        LOGGER.info(
            "财报研究输入目录：data/stock_info/<名称>_%s/financial_report_workdir/",
            symbol,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
