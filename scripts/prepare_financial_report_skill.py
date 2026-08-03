#!/usr/bin/env python3
"""Prepare financial report skill inputs for fixed tracked stocks or queue stocks."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger
from services.research.financial_report_skill import (
    build_stock_report_bundles,
    load_tracked_items,
    prepare_financial_report_workdir,
    synthesize_manual_item,
)

LOGGER = init_component_logger(
    "PrepareFinancialReportSkill",
    group="research",
    filename_prefix="prepare_financial_report_skill",
)
_PDF_CONVERTER: Any | None = None


def _get_pdf_converter() -> Any:
    global _PDF_CONVERTER
    if _PDF_CONVERTER is None:
        from services.document_conversion import PDFMarkdownConverter

        LOGGER.info("首次创建 MinerU API 客户端，后续财报转换将复用当前连接配置")
        _PDF_CONVERTER = PDFMarkdownConverter()
    return _PDF_CONVERTER


def _default_markdown_path_for_pdf(pdf_path: Path) -> Path:
    """为 disclosures/pdfs 下的 PDF 推导同级 md 目录路径。"""
    if pdf_path.parent.name == "pdfs" and pdf_path.parent.parent.exists():
        return pdf_path.parent.parent / "md" / f"{pdf_path.stem}.md"
    return pdf_path.with_suffix(".md")


def _convert_pdf_to_markdown(pdf_path: Path, md_path: Path | None = None) -> Path:
    md_path = md_path or _default_markdown_path_for_pdf(pdf_path)
    from services.document_conversion import (
        is_pdf_markdown_cache_current,
        write_pdf_conversion_artifacts,
    )

    if is_pdf_markdown_cache_current(md_path, pdf_path, profile="financial_report"):
        return md_path
    converter = _get_pdf_converter()
    LOGGER.info("转换财报 PDF 为 Markdown: %s -> %s", pdf_path, md_path)
    result = converter.convert_with_details(str(pdf_path), output_dir=None, profile="financial_report")
    write_pdf_conversion_artifacts(md_path, result, pdf_path)
    return md_path


def _ensure_markdown_path(md_path: Path | None, pdf_path: Path | None) -> Path | None:
    if md_path and pdf_path and pdf_path.exists():
        from services.document_conversion import is_pdf_markdown_cache_current

        if is_pdf_markdown_cache_current(md_path, pdf_path, profile="financial_report"):
            return md_path
        return _convert_pdf_to_markdown(pdf_path, md_path)
    if md_path and md_path.exists():
        return md_path
    if pdf_path and pdf_path.exists():
        return _convert_pdf_to_markdown(pdf_path, md_path)
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="为财报总结 skill 准备固定股票池或深研队列股票的财报输入。")
    parser.add_argument(
        "--date",
        help="本次分析日（YYYY-MM-DD），同时用于 selection_runs 队列和财报可见性截止；默认今天。",
    )
    parser.add_argument(
        "--mandate",
        default="all",
        choices=["all", "short_book", "long_book"],
        help="只准备某个账本的股票，默认 all。",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="输出 JSON 结果，方便后续 agent 直接消费。",
    )
    parser.add_argument(
        "--output",
        help="可选：将准备结果写入指定 JSON 文件。",
    )
    parser.add_argument(
        "--sync-first",
        action="store_true",
        help="先同步财报公告到 disclosures，再准备输入。",
    )
    parser.add_argument(
        "--symbols",
        help="只处理这些股票代码，逗号分隔；一旦提供，将不再自动加入 tracked、queue 或 quant prefilter 股票。",
    )
    parser.add_argument(
        "--include-tracked",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否把 configs.stock_pool.TRACKED_A_STOCKS 的全部股票也加入处理列表（默认启用，用 --no-include-tracked 关闭）。",
    )
    parser.add_argument(
        "--include-queue",
        action="store_true",
        help="额外把 deep research queue 的股票也加入处理列表（默认关闭，仅处理固定股票池）。",
    )
    parser.add_argument(
        "--include-quant-prefilter",
        action="store_true",
        help="额外把 12_quant_prefilter_short.csv 中的股票也加入处理列表。",
    )
    parser.add_argument(
        "--skip-market-context",
        action="store_true",
        help="仅调试准备层时跳过当前价格与增强估值生成；正式财报研究不建议使用。",
    )
    parser.add_argument(
        "--force-reprepare",
        action="store_true",
        help="即使最新财报已登记，也只重建 workdir 供调试/评审；不覆盖最终财报，不修改 summary_index。",
    )
    return parser


def _parse_symbol_list(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _apply_explicit_symbol_scope(args: argparse.Namespace) -> List[str]:
    """Make --symbols an exclusive stock scope for this invocation."""
    explicit_symbols = _parse_symbol_list(args.symbols)
    if not explicit_symbols:
        return []

    ignored_sources = []
    if args.include_tracked:
        ignored_sources.append("tracked")
    if args.include_queue:
        ignored_sources.append("queue")
    if args.include_quant_prefilter:
        ignored_sources.append("quant_prefilter")
    if ignored_sources:
        LOGGER.info(
            "--symbols 已指定，仅处理显式股票；忽略扩池来源: %s",
            ", ".join(ignored_sources),
        )

    args.include_tracked = False
    args.include_queue = False
    args.include_quant_prefilter = False
    args.symbols = ",".join(explicit_symbols)
    return explicit_symbols


def _load_quant_prefilter_symbols(run_date: str) -> List[str]:
    """Read symbols from 12_quant_prefilter_short.csv and 12_quant_prefilter_long.csv."""
    import csv

    base_dir = PROJECT_ROOT / "data" / "selection_runs" / run_date
    symbols: List[str] = []
    for filename in ("12_quant_prefilter_short.csv", "12_quant_prefilter_long.csv"):
        prefilter_path = base_dir / filename
        if not prefilter_path.exists():
            LOGGER.warning("量化初筛文件不存在: %s", prefilter_path)
            continue
        with open(prefilter_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                symbol = (row.get("symbol") or "").strip()
                if symbol and symbol not in symbols:
                    symbols.append(symbol)
        LOGGER.info("从 %s 加载 %d 个 symbol", filename, len(symbols))
    return symbols


def _collect_extra_items(args: argparse.Namespace) -> List[dict]:
    extra: List[dict] = []
    seen: set[str] = set()
    if args.include_tracked:
        for item in load_tracked_items():
            symbol = item.get("symbol")
            if symbol and symbol not in seen:
                extra.append(item)
                seen.add(symbol)
    for symbol in _parse_symbol_list(args.symbols):
        if symbol in seen:
            continue
        tracked_match = next(
            (entry for entry in load_tracked_items() if entry.get("symbol") == symbol),
            None,
        )
        if tracked_match is not None:
            extra.append(tracked_match)
        else:
            extra.append(synthesize_manual_item(symbol))
        seen.add(symbol)
    return extra


def main() -> int:
    args = build_parser().parse_args()
    analysis_date = args.date or datetime.now().date().isoformat()
    extra_symbols = _apply_explicit_symbol_scope(args)
    if not extra_symbols:
        extra_symbols = _parse_symbol_list(args.symbols)
    if args.include_quant_prefilter and args.date:
        prefilter_symbols = _load_quant_prefilter_symbols(args.date)
        for s in prefilter_symbols:
            if s not in extra_symbols:
                extra_symbols.append(s)
        if extra_symbols:
            args.symbols = ",".join(extra_symbols)
    if args.sync_first:
        sync_cmd = [
            sys.executable,
            "scripts/sync_financial_reports.py",
            "--mandate",
            args.mandate,
            "--json",
        ]
        if args.date:
            sync_cmd.extend(["--date", args.date])
        sync_cmd.append("--include-tracked" if args.include_tracked else "--no-include-tracked")
        if args.include_queue:
            sync_cmd.append("--include-queue")
        if extra_symbols:
            sync_cmd.extend(["--symbols", ",".join(extra_symbols)])
        subprocess.run(
            sync_cmd,
            cwd=PROJECT_ROOT,
            check=True,
        )
    extra_items = _collect_extra_items(args)
    bundles = build_stock_report_bundles(
        args.date,
        mandate=args.mandate,
        extra_items=extra_items or None,
        skip_queue=not args.include_queue,
    )
    ready = []
    skipped = []
    for bundle in bundles:
        forced_reprepare = bool(
            args.force_reprepare
            and bundle.skipped
            and bundle.skip_reason == "already_summarized_latest_report"
        )
        if bundle.skipped and not forced_reprepare:
            skipped.append(
                {
                    "symbol": bundle.symbol,
                    "stock_name": bundle.stock_name,
                    "final_mandate": bundle.final_mandate,
                    "skip_reason": bundle.skip_reason,
                }
            )
            continue
        latest_path = _ensure_markdown_path(bundle.latest_report.md_path, bundle.latest_report.pdf_path)
        previous_path = None
        if bundle.previous_report:
            previous_path = _ensure_markdown_path(bundle.previous_report.md_path, bundle.previous_report.pdf_path)
        if latest_path is None or not latest_path.exists():
            skipped.append(
                {
                    "symbol": bundle.symbol,
                    "stock_name": bundle.stock_name,
                    "final_mandate": bundle.final_mandate,
                    "skip_reason": "latest_report_markdown_missing_after_prepare",
                }
            )
            continue
        workdir = prepare_financial_report_workdir(
            bundle,
            latest_path=latest_path,
            previous_path=previous_path,
            analysis_date=analysis_date,
            generate_current_market=not args.skip_market_context,
        )
        ready.append(
            {
                "symbol": bundle.symbol,
                "stock_name": bundle.stock_name,
                "final_mandate": bundle.final_mandate,
                "latest_announcement_id": bundle.latest_report.announcement_id,
                "latest_report_date": bundle.latest_report.date,
                "analysis_date": analysis_date,
                "forced_reprepare": forced_reprepare,
                "latest_report_path": str(workdir / "01_latest_report.md"),
                "previous_report_path": str(workdir / "02_previous_report.md") if previous_path else None,
                "output_path": str(bundle.output_path),
                "summary_index_path": str(bundle.summary_index_path),
                "workdir": str(workdir),
                "manifest_path": str(workdir / "manifest.json"),
            }
        )

    payload = {"ready_items": ready, "skipped_items": skipped}
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for item in ready:
            print(f"{item['symbol']} {item['stock_name']} -> {item['output_path']}")
            print(f"  latest: {item['latest_report_path']}")
            print(f"  previous: {item['previous_report_path'] or 'N/A'}")
        for item in skipped:
            print(f"SKIP {item['symbol']} {item['stock_name']} reason={item['skip_reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
