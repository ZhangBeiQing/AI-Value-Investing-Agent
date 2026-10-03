#!/usr/bin/env python3
"""Repair disclosures/index.json from already-generated financial report summaries.

Background:
    The backtest financial gate (services/backtest/financials.py) decides whether a
    symbol's latest report as-of the decision date is covered by looking at the
    shared ``disclosures/index.json`` first.  Some symbols keep only recent
    announcements in that index (or none at all), even though their deep-research
    summaries already exist under ``financial_reports/``.  As a result the gate
    reports ``missing_financial_reports_in_disclosures`` for reports that were in
    fact already summarized.

What this script does:
    For every stock with a ``financial_reports/summary_index.json``, it adds the
    corresponding financial-report entries (real announcement ids / report dates
    recorded in the summary index) into ``disclosures/index.json`` if they are not
    present yet.  It is purely additive (``save_index_merge``) and idempotent, and
    it never downloads or fabricates data.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import get_logger
from services.news.disclosures_builder import (
    AnnouncementMeta,
    index_path,
    load_index,
    pdfs_dir,
    save_index_merge,
)
from commons.stock_utils import parse_symbol


LOGGER = get_logger("RepairDisclosuresFromSummaries")
STOCK_INFO_ROOT = PROJECT_ROOT / "data" / "stock_info"

REPORT_TITLE_BY_TYPE = {
    "q1": "第一季度报告",
    "interim": "半年度报告",
    "q3": "第三季度报告",
    "annual": "年度报告",
}
NORMALIZED_TYPES = {
    "q1": "q1",
    "interim": "interim",
    "q2": "interim",
    "q3": "q3",
    "annual": "annual",
    "q4": "annual",
}


def _normalize_type(raw: Any) -> Optional[str]:
    key = str(raw or "").strip().lower()
    return NORMALIZED_TYPES.get(key)


def _period(entry: Dict[str, Any]) -> Optional[tuple[int, int]]:
    report_type = _normalize_type(entry.get("report_type"))
    report_date = str(entry.get("report_date") or "").strip()
    if not report_type or len(report_date) < 4 or not report_date[:4].isdigit():
        return None
    explicit_year = entry.get("fiscal_year")
    explicit_quarter = entry.get("quarter")
    if explicit_year and explicit_quarter:
        try:
            return int(explicit_year), int(explicit_quarter)
        except (TypeError, ValueError):
            pass
    year = int(report_date[:4])
    if report_type == "annual":
        return year - 1, 4
    return year, {"q1": 1, "interim": 2, "q3": 3}[report_type]


def _iter_summary_entries(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    latest = payload.get("latest_completed_report")
    if isinstance(latest, dict):
        yield latest
    history = payload.get("history")
    if isinstance(history, list):
        for item in history:
            if isinstance(item, dict):
                yield item


def _find_pdf(symbol: str, announcement_id: str) -> Optional[str]:
    directory = pdfs_dir(parse_symbol(symbol))
    if not directory.is_dir():
        return None
    for candidate in directory.glob(f"*{announcement_id}*"):
        if candidate.is_file():
            return str(candidate)
    return None


def repair_symbol(stock_dir: Path) -> Optional[dict[str, Any]]:
    summary_path = stock_dir / "financial_reports" / "summary_index.json"
    if not summary_path.exists():
        return None
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("summary_index 读取失败: %s (%s)", summary_path, exc)
        return None
    symbol = str(payload.get("symbol") or "").strip()
    if not symbol:
        return None

    additions: Dict[str, AnnouncementMeta] = {}
    for entry in _iter_summary_entries(payload):
        announcement_id = str(entry.get("announcement_id") or "").strip()
        report_date = str(entry.get("report_date") or "").strip()
        if not announcement_id or not report_date:
            continue
        period = _period(entry)
        if period is None:
            continue
        report_type = _normalize_type(entry.get("report_type"))
        fiscal_year, _quarter = period
        title = f"{fiscal_year}年{REPORT_TITLE_BY_TYPE[report_type]}"
        pdf_path = _find_pdf(symbol, announcement_id)
        additions[announcement_id] = AnnouncementMeta(
            announcement_id=announcement_id,
            org_id=None,
            stock_code=symbol.split(".")[0],
            title=title,
            date=report_date,
            url="",
            pdf_path=pdf_path,
            downloaded=bool(pdf_path),
            category="Financial_Report",
            is_financial_report=True,
            dedupe_key=announcement_id,
        )

    if not additions:
        return None

    target = index_path(parse_symbol(symbol))
    existing = load_index(target)
    merged = dict(existing)
    added = 0
    for announcement_id, meta in additions.items():
        current = merged.get(announcement_id)
        if current is None:
            merged[announcement_id] = meta
            added += 1
            continue
        if not current.is_financial_report:
            current.is_financial_report = True
            current.category = current.category or "Financial_Report"
            added += 1
    if added == 0:
        return {"symbol": symbol, "added": 0, "total": len(merged)}
    save_index_merge(target, merged)
    return {"symbol": symbol, "added": added, "total": len(merged)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="从已有财报深度总结回填 disclosures/index.json 的财报条目。",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="逗号分隔的股票列表；为空时扫描 data/stock_info 下所有有 summary_index 的股票。",
    )
    parser.add_argument(
        "--stock-info-root",
        default=str(STOCK_INFO_ROOT),
        help="stock_info 根目录，默认 data/stock_info。",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    root = Path(args.stock_info_root)
    targets: list[Path]
    if args.symbols:
        targets = []
        for raw in args.symbols.split(","):
            symbol = raw.strip()
            if not symbol:
                continue
            info = parse_symbol(symbol)
            targets.append(root / f"{info.stock_name}_{info.symbol}")
    else:
        targets = sorted(path for path in root.iterdir() if path.is_dir())

    results = []
    for stock_dir in targets:
        outcome = repair_symbol(stock_dir)
        if outcome:
            results.append(outcome)
    changed = [row for row in results if row["added"] > 0]
    print(
        json.dumps(
            {
                "scanned": len(targets),
                "with_summary": len(results),
                "repaired": len(changed),
                "details": changed,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
