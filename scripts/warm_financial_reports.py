#!/usr/bin/env python3
"""Warm the financial-report cache for the whole stock universe.

只做两件事，无需任何参数即可运行：
    1. 下载每只股票最近 2 份财报 PDF（已存在则跳过）
    2. 把这些 PDF 转成 Markdown（已转过且新鲜则跳过）

用法：
    python scripts/warm_financial_reports.py                    # 全宇宙跑（推荐）
    python scripts/warm_financial_reports.py --symbol 600519.SH # 仅调试单只
"""

from __future__ import annotations

import argparse
import concurrent.futures
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger
from news.disclosures_builder import (
    AnnouncementMeta,
    _hash_key,
    _slugify,
    download_pdf,
    index_path,
    is_financial_report,
    load_index,
    parse_announcement_row,
    pdfs_dir,
    save_index_merge,
)
from services.data_refresh.refresh_orchestrator import _load_daily_refresh_symbols
from services.research.financial_report_skill import (
    _classify_report_title,
    _load_financial_report_entries,
    _report_period_rank,
    _resolve_fiscal_year,
)
from shared_data_access import SharedDataAccess
from utlity.stock_utils import parse_symbol

LOGGER = init_component_logger(
    "WarmFinancialReports",
    group="research",
    filename_prefix="warm_financial_reports",
)

# 内部常量。每只股票最近 N 份财报；同步阶段拉公告的回溯窗口（A 股季度 + HK 半年度都够找到最近 2 份）；
# 下载阶段并发线程数。
MAX_REPORTS_PER_STOCK = 2
DISCLOSURE_LOOKBACK_DAYS = 400
SYNC_WORKERS = 8

_PDF_CONVERTER = None


def _get_pdf_converter():
    global _PDF_CONVERTER
    if _PDF_CONVERTER is None:
        from news.gemini_utility import PDFMarkdownConverter

        LOGGER.info("首次创建 PDF 转 Markdown 转换器，后续财报转换将复用当前进程内模型")
        _PDF_CONVERTER = PDFMarkdownConverter()
    return _PDF_CONVERTER


def _default_markdown_path_for_pdf(pdf_path: Path) -> Path:
    """复用 prepare_financial_report_skill 里同名逻辑：disclosures/pdfs/*.pdf -> disclosures/md/*.md。"""
    if pdf_path.parent.name == "pdfs" and pdf_path.parent.parent.exists():
        return pdf_path.parent.parent / "md" / f"{pdf_path.stem}.md"
    return pdf_path.with_suffix(".md")


def _convert_one(pdf_path: Path, md_path: Path) -> Tuple[str, str]:
    """Returns (status, detail). status ∈ {'cached', 'converted', 'error'}."""
    from news.gemini_utility import (
        is_pdf_markdown_cache_current,
        write_pdf_conversion_artifacts,
    )

    if is_pdf_markdown_cache_current(md_path, pdf_path, profile="financial_report"):
        return "cached", ""
    try:
        converter = _get_pdf_converter()
        result = converter.convert_with_details(
            str(pdf_path), output_dir=None, profile="financial_report"
        )
        write_pdf_conversion_artifacts(md_path, result, pdf_path)
        return "converted", ""
    except Exception as exc:  # pragma: no cover - defensive
        return "error", str(exc)


def _sync_one_symbol(symbol: str) -> Tuple[str, int, str]:
    """只下载该股票最近 MAX_REPORTS_PER_STOCK 份财报 PDF（与转换阶段排序一致）。

    Returns (symbol, downloaded_count, error_or_empty).
    """
    try:
        symbol_info = parse_symbol(symbol)
        access = SharedDataAccess(logger=LOGGER)
        prepared = access.prepare_dataset(
            symbolInfo=symbol_info,
            as_of_date=datetime.now().strftime("%Y-%m-%d"),
            include_disclosures=True,
            disclosure_lookback_days=DISCLOSURE_LOOKBACK_DAYS,
        )
        bundle = prepared.disclosures
        if bundle is None or bundle.frame.empty:
            return symbol, 0, ""

        candidates = []
        for _, row in bundle.frame.iterrows():
            row_dict = row.to_dict()
            stock_code, title, date, url, ann_id = parse_announcement_row(row_dict)
            if not is_financial_report(title):
                continue
            report_type, quarter, _kind, priority = _classify_report_title(symbol, title)
            if not report_type or not quarter or priority >= 99:
                continue
            fiscal_year = _resolve_fiscal_year(title, date)
            sort_key = (
                -_report_period_rank(fiscal_year, quarter),
                priority,
                -(int(date.replace("-", "")) if date else 0),
            )
            candidates.append((sort_key, row_dict, stock_code, title, date, url, ann_id))

        if not candidates:
            return symbol, 0, ""
        candidates.sort(key=lambda item: item[0])
        top = candidates[:MAX_REPORTS_PER_STOCK]

        idx_path = index_path(symbol_info)
        idx = load_index(idx_path)
        downloaded_count = 0
        idx_dirty = False

        for _sort_key, row_dict, stock_code, title, date, url, ann_id in top:
            key = ann_id or _hash_key(title, date)
            meta = idx.get(key)
            if meta is None:
                meta = AnnouncementMeta(
                    announcement_id=ann_id or key,
                    org_id=str(row_dict.get("orgId") or ""),
                    stock_code=stock_code,
                    title=title,
                    date=date,
                    url=url,
                    category="Financial_Report",
                    is_financial_report=True,
                    dedupe_key=key,
                )
                idx_dirty = True
            else:
                if not meta.is_financial_report:
                    meta.is_financial_report = True
                    idx_dirty = True
                if not meta.category:
                    meta.category = "Financial_Report"
                    idx_dirty = True

            pdf_present = (
                meta.downloaded
                and meta.pdf_path
                and Path(meta.pdf_path).exists()
            )
            if not pdf_present:
                file_name = f"{date}__{stock_code}__{meta.announcement_id}__{_slugify(title)}.pdf"
                out = pdfs_dir(symbol_info) / file_name
                if download_pdf(url, out):
                    meta.pdf_path = str(out)
                    meta.downloaded = True
                    downloaded_count += 1
                    idx_dirty = True

            idx[key] = meta

        if idx_dirty:
            save_index_merge(idx_path, idx)
        return symbol, downloaded_count, ""
    except Exception as exc:  # pragma: no cover - defensive
        return symbol, 0, str(exc)


def _run_sync_phase(symbols: List[str]) -> Tuple[int, int]:
    """阶段 1：按 symbol 并发同步财报 PDF。返回 (新下载条数合计, 失败股票数)."""
    total = len(symbols)
    LOGGER.info(
        "开始同步财报 PDF: symbols=%d, lookback_days=%d, workers=%d, max_per_stock=%d",
        total,
        DISCLOSURE_LOOKBACK_DAYS,
        SYNC_WORKERS,
        MAX_REPORTS_PER_STOCK,
    )
    synced_total = 0
    error_count = 0
    completed = 0
    tic = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=SYNC_WORKERS) as executor:
        future_to_symbol = {
            executor.submit(_sync_one_symbol, symbol): symbol for symbol in symbols
        }
        for future in concurrent.futures.as_completed(future_to_symbol):
            completed += 1
            symbol, count, err = future.result()
            if err:
                error_count += 1
                LOGGER.error("[%d/%d] %s: 同步失败: %s", completed, total, symbol, err)
            elif count:
                synced_total += count
                LOGGER.info("[%d/%d] %s: 新下载 %d 份财报 PDF", completed, total, symbol, count)
    LOGGER.info(
        "同步阶段完成: 新下载=%d, 失败股票=%d, 耗时=%.1fs",
        synced_total,
        error_count,
        time.time() - tic,
    )
    return synced_total, error_count


def _run_convert_phase(symbols: List[str]) -> Tuple[int, int, int, int, int]:
    """阶段 2：顺序遍历转 Markdown。返回 (含财报股票数, PDF 检查数, 已缓存数, 转换数, 失败数)."""
    total = len(symbols)
    stocks_with_reports = 0
    total_pdfs_checked = 0
    total_cached = 0
    total_converted = 0
    total_errors = 0
    tic = time.time()
    for idx, symbol in enumerate(symbols, 1):
        try:
            entries = _load_financial_report_entries(symbol)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("[%d/%d] %s: 加载 disclosures 索引失败: %s", idx, total, symbol, exc)
            continue
        entries = entries[:MAX_REPORTS_PER_STOCK]
        valid_entries = [e for e in entries if e.pdf_path and e.pdf_path.exists()]
        if not valid_entries:
            LOGGER.info("[%d/%d] %s: 无可用财报 PDF，跳过", idx, total, symbol)
            continue
        stocks_with_reports += 1
        LOGGER.info(
            "[%d/%d] %s: 发现 %d 份财报待检查",
            idx,
            total,
            symbol,
            len(valid_entries),
        )
        for entry in valid_entries:
            md_path = entry.md_path if entry.md_path else _default_markdown_path_for_pdf(entry.pdf_path)
            total_pdfs_checked += 1
            ctic = time.time()
            status, detail = _convert_one(entry.pdf_path, md_path)
            elapsed = time.time() - ctic
            if status == "cached":
                total_cached += 1
            elif status == "converted":
                total_converted += 1
                LOGGER.info("  ✓ %s (%.1fs) -> %s", entry.pdf_path.name, elapsed, md_path.name)
            else:
                total_errors += 1
                LOGGER.error("  ✗ %s: %s", entry.pdf_path.name, detail)
    LOGGER.info(
        "转换阶段完成: 含财报股票=%d, PDF 检查=%d, 已缓存=%d, 本轮转换=%d, 失败=%d, 耗时=%.1fs",
        stocks_with_reports,
        total_pdfs_checked,
        total_cached,
        total_converted,
        total_errors,
        time.time() - tic,
    )
    return stocks_with_reports, total_pdfs_checked, total_cached, total_converted, total_errors


def run(symbols: List[str]) -> int:
    wall_start = time.time()
    sync_synced, sync_errors = _run_sync_phase(symbols)
    stocks_with_reports, total_pdfs_checked, total_cached, total_converted, total_errors = (
        _run_convert_phase(symbols)
    )
    LOGGER.info(
        "汇总: 股票=%d, 新下载财报=%d (同步失败股票=%d), 含财报股票=%d, "
        "PDF 检查=%d, 已缓存=%d, 本轮转换=%d, 转换失败=%d, 总耗时=%.1fs",
        len(symbols),
        sync_synced,
        sync_errors,
        stocks_with_reports,
        total_pdfs_checked,
        total_cached,
        total_converted,
        total_errors,
        time.time() - wall_start,
    )
    return 1 if (total_errors or sync_errors) else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "预先下载并把每只股票最近 2 份财报 PDF 转成 Markdown，加速后续 /financial-report-summary。"
            "不加任何参数即可运行，默认覆盖 TRACKED_A_STOCKS ∪ master_universe。"
        )
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=None,
        help="仅处理指定 symbol（可多次传），用于调试单只股票",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.symbol:
        symbols = [s.strip() for s in args.symbol if s.strip()]
    else:
        symbols = _load_daily_refresh_symbols(base_dir="data")
    LOGGER.info("开始预热财报缓存: symbols=%d", len(symbols))
    return run(symbols)


if __name__ == "__main__":
    sys.exit(main())
