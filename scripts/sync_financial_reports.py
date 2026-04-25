#!/usr/bin/env python3
"""Sync financial report disclosures for deep research queue stocks."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from news.disclosures_builder import sync_financial_reports_for_stock
from services.research.financial_report_skill import (
    load_deep_research_items,
    load_tracked_items,
    synthesize_manual_item,
)
from utlity.stock_utils import parse_symbol


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 deep research queue 股票的财报公告原始件。")
    parser.add_argument("--date", help="selection_runs 日期，默认自动使用最近日期。")
    parser.add_argument(
        "--mandate",
        default="all",
        choices=["all", "short_book", "long_book"],
        help="限制 short_book/long_book/all。",
    )
    parser.add_argument("--lookback-days", type=int, default=550, help="公告回溯天数。")
    parser.add_argument("--with-markdown", action="store_true", help="同步时立即转换 markdown。默认只下载 PDF。")
    parser.add_argument("--json", action="store_true", help="输出 JSON 结果。")
    parser.add_argument(
        "--symbols",
        help="额外要同步的股票代码，逗号分隔；可用于 queue 与 TRACKED_A_STOCKS 之外的股票。",
    )
    parser.add_argument(
        "--include-tracked",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否把 configs.stock_pool.TRACKED_A_STOCKS 的全部股票也加入同步列表（默认启用，用 --no-include-tracked 关闭）。",
    )
    parser.add_argument(
        "--no-queue",
        action="store_true",
        help="跳过 deep research queue，仅同步 --symbols / --include-tracked 指定的股票。",
    )
    return parser


def _parse_symbol_list(raw):
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _build_items(args) -> list:
    if args.no_queue:
        items: list = []
    else:
        items = list(load_deep_research_items(args.date, mandate=args.mandate))
    seen = {item.get("symbol") for item in items if item.get("symbol")}
    tracked_by_symbol = {entry.get("symbol"): entry for entry in load_tracked_items()}
    if args.include_tracked:
        for symbol, entry in tracked_by_symbol.items():
            if symbol and symbol not in seen:
                items.append(entry)
                seen.add(symbol)
    for symbol in _parse_symbol_list(args.symbols):
        if symbol in seen:
            continue
        items.append(tracked_by_symbol.get(symbol) or synthesize_manual_item(symbol))
        seen.add(symbol)
    return items


def main() -> int:
    args = build_parser().parse_args()
    items = _build_items(args)
    results = []
    for item in items:
        symbol = item.get("symbol")
        if not symbol:
            continue
        symbol_info = parse_symbol(symbol)
        try:
            count = sync_financial_reports_for_stock(
                symbol_info,
                lookback_days=args.lookback_days,
                convert_markdown=args.with_markdown,
            )
            results.append(
                {
                    "symbol": symbol,
                    "stock_name": item.get("stock_name") or symbol,
                    "final_mandate": item.get("final_mandate"),
                    "synced_count": count,
                    "status": "ok",
                }
            )
        except Exception as exc:
            results.append(
                {
                    "symbol": symbol,
                    "stock_name": item.get("stock_name") or symbol,
                    "final_mandate": item.get("final_mandate"),
                    "synced_count": 0,
                    "status": "error",
                    "error": str(exc),
                }
            )

    if args.json:
        print(json.dumps({"items": results}, ensure_ascii=False, indent=2))
    else:
        for item in results:
            print(f"{item['symbol']} {item['stock_name']} synced={item['synced_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
