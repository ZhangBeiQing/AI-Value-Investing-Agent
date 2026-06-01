#!/usr/bin/env python3
"""Prepare financial report skill inputs for fixed tracked stocks or queue stocks."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
import shutil
from typing import Any, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger
from services.research.financial_report_skill import (
    build_stock_report_bundles,
    financial_report_workdir,
    load_tracked_items,
    synthesize_manual_item,
)
from services.research.financial_report_summary_prompts import (
    FUTURE_OUTLOOK_PROMPT_TEMPLATE,
    REPORT_ANALYSIS_PROMPT_TEMPLATE,
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
        from news.gemini_utility import PDFMarkdownConverter

        LOGGER.info("首次创建 PDF 转 Markdown 转换器，后续财报转换将复用当前进程内模型")
        _PDF_CONVERTER = PDFMarkdownConverter()
    return _PDF_CONVERTER


def _default_markdown_path_for_pdf(pdf_path: Path) -> Path:
    """为 disclosures/pdfs 下的 PDF 推导同级 md 目录路径。"""
    if pdf_path.parent.name == "pdfs" and pdf_path.parent.parent.exists():
        return pdf_path.parent.parent / "md" / f"{pdf_path.stem}.md"
    return pdf_path.with_suffix(".md")


def _convert_pdf_to_markdown(pdf_path: Path, md_path: Path | None = None) -> Path:
    md_path = md_path or _default_markdown_path_for_pdf(pdf_path)
    from news.gemini_utility import (
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
        from news.gemini_utility import is_pdf_markdown_cache_current

        if is_pdf_markdown_cache_current(md_path, pdf_path, profile="financial_report"):
            return md_path
        return _convert_pdf_to_markdown(pdf_path, md_path)
    if md_path and md_path.exists():
        return md_path
    if pdf_path and pdf_path.exists():
        return _convert_pdf_to_markdown(pdf_path, md_path)
    return None


def _copy_to_workdir(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)


def _infer_report_period(source_path: Path) -> str:
    stem = source_path.stem
    if "一季度" in stem or "一季报" in stem or "Q1" in stem:
        return "一季度"
    if "半年度" in stem or "半年报" in stem or "中报" in stem or "Q2" in stem:
        return "半年度"
    if "三季度" in stem or "三季报" in stem or "Q3" in stem:
        return "三季度"
    if "年度" in stem or "年报" in stem or "Q4" in stem:
        return "年度"
    return "最新季度"


def _write_workdir(bundle, latest_path: Path, previous_path: Path | None, industry_name: str) -> Path:
    workdir = financial_report_workdir(bundle.symbol)
    workdir.mkdir(parents=True, exist_ok=True)
    latest_target = workdir / "01_latest_report.md"
    previous_target = workdir / "02_previous_report.md"
    report_prompt_target = workdir / "03_report_analysis_prompt.md"
    outlook_prompt_target = workdir / "04_future_outlook_prompt.md"
    agent_input_target = workdir / "05_agent_input.md"
    manifest_target = workdir / "manifest.json"

    _copy_to_workdir(latest_path, latest_target)
    if previous_path:
        _copy_to_workdir(previous_path, previous_target)
    elif previous_target.exists():
        previous_target.unlink()

    report_period = _infer_report_period(latest_path)
    report_prompt = REPORT_ANALYSIS_PROMPT_TEMPLATE.format(
        company_name=bundle.stock_name,
        report_period=report_period,
    )
    outlook_prompt = FUTURE_OUTLOOK_PROMPT_TEMPLATE.format(
        company_name=bundle.stock_name,
        industry_name=industry_name,
    )
    report_prompt_target.write_text(report_prompt, encoding="utf-8")
    outlook_prompt_target.write_text(outlook_prompt, encoding="utf-8")

    agent_input = "\n".join(
        [
            f"# {bundle.stock_name} ({bundle.symbol}) 财报分析输入",
            "",
            "## 任务目标",
            "你是一名只负责当前这一只股票的财报研究 subagent。你的任务不是复述财报，而是基于最近两份关键财报原文，结合联网搜索得到的高可信外部信息，完成一份可直接用于投资研究的深度财报分析文档。",
            "",
            "## 强制阅读顺序",
            "1. 必须先完整阅读 `01_latest_report.md`。",
            "2. 若存在，再完整阅读 `02_previous_report.md`。",
            "3. 再完整阅读 `03_report_analysis_prompt.md`。",
            "4. 再完整阅读 `04_future_outlook_prompt.md`。",
            "5. 最后阅读 `manifest.json`，确认输出路径、公告日期和当前输入元信息。",
            "",
            "说明：上述文件必须按顺序完整读完，不能只看局部片段、关键词命中或抽样段落后就开始下结论。",
            "",
            "## 研究方式",
            "1. `03_report_analysis_prompt.md` 定义的是‘历史与当前财报验证任务’。你必须围绕它主动搜索市场一致预期、券商财报前预测、财报后快评、公司业绩演示材料、交易所补充公告等高可信信息。",
            "2. `04_future_outlook_prompt.md` 定义的是‘未来 6-12 个月行业与经营前瞻任务’。你必须围绕它主动搜索行业景气、政策、成本、需求、竞争格局、公司催化剂、未来一致预期与风险。",
            "3. **关键要求——同比与环比必须同时分析：** 你必须主动搜索并获取最新季度的**归母净利润同比增速**和**归母净利润环比增速**，两者缺一不可。很多 agent 会遗漏环比分析，但环比净利润变化是判断盈利拐点的最早信号——当同比仍为正但环比已连续下滑时，往往预示着基本面恶化已经开始。你的分析报告中必须明确列示同比和环比两个维度的净利润增速，并分别给出解读。",
            "4. 不要把联网搜索限制为少数固定问题。你应该根据这两个 prompt 自己判断还缺什么信息，并继续搜索，直到能完整回答两个 prompt 的核心问题。",
            "5. 优先使用权威来源：公司财报、公司演示材料、交易所公告、Bloomberg/Refinitiv/FactSet 摘要（若可得）、主流券商研报、权威行业资料。",
            "6. 严禁引用未经证实的市场传言。若某项预期或数据无法高可信获取，必须明确写‘未找到高可信信息’，而不是猜测。",
            "",
            "## 输出要求",
            f"1. 最终输出必须写入 `{bundle.output_path}`。",
            "2. 输出必须是完整 Markdown 文档，不要输出 JSON，不要输出对话式说明，不要输出 fenced code block 包裹的 markdown。",
            "3. 文档必须同时回答：",
            "   - 当前这期财报相对市场预期是超预期、符合预期还是低于预期；",
            "   - 经营质量如何，核心变化来自哪里；",
            "   - 这期财报对原有投资逻辑是强化、削弱还是微调；",
            "   - 未来 6-12 个月行业和公司经营最关键的催化剂与风险是什么。",
            "4. 文档中必须尽量区分‘已核实事实’与‘基于事实的推断’。",
            "5. 若一致预期不足，必须说明你使用了哪些替代来源，以及这些替代来源的局限性。",
            "",
            "## 建议篇幅",
            "- 建议正文篇幅控制在 4000-7000 字。",
            "- 普通季报以 4000-5500 字为宜；信息密度高的年报或争议较大的公司可到 6000-7000 字。",
            "- 不建议少于 3000 字，否则通常不足以同时覆盖‘财报验证 + 未来前瞻’两部分；也不建议无节制膨胀到 9000 字以上，以免变成低信噪比堆砌。",
            "",
            "## 当前输入",
            f"- 最新财报: {latest_target}",
            f"- 上一期关键财报: {previous_target if previous_path else '无'}",
            f"- 财报分析 prompt: {report_prompt_target}",
            f"- 未来前瞻 prompt: {outlook_prompt_target}",
            f"- 当前工作目录: {workdir}",
            f"- summary_index: {bundle.summary_index_path}",
        ]
    )
    agent_input_target.write_text(agent_input, encoding="utf-8")

    manifest = {
        "symbol": bundle.symbol,
        "stock_name": bundle.stock_name,
        "final_mandate": bundle.final_mandate,
        "latest_announcement_id": bundle.latest_report.announcement_id,
        "latest_report_date": bundle.latest_report.date,
        "latest_report_path": str(latest_target),
        "previous_announcement_id": bundle.previous_report.announcement_id if bundle.previous_report else None,
        "previous_report_date": bundle.previous_report.date if bundle.previous_report else None,
        "previous_report_path": str(previous_target) if previous_path else None,
        "report_analysis_prompt_path": str(report_prompt_target),
        "future_outlook_prompt_path": str(outlook_prompt_target),
        "agent_input_path": str(agent_input_target),
        "output_path": str(bundle.output_path),
        "summary_index_path": str(bundle.summary_index_path),
        "industry_name": industry_name,
    }
    manifest_target.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return workdir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="为财报总结 skill 准备固定股票池或深研队列股票的财报输入。")
    parser.add_argument("--date", help="selection_runs 日期，默认自动取最近一个有 11_deep_research_queue.json 的日期。")
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
        help="额外要处理的股票代码，逗号分隔；可用于 queue 与 TRACKED_A_STOCKS 之外的股票。",
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
    return parser


def _parse_symbol_list(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


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
        if bundle.skipped:
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
        workdir = _write_workdir(bundle, latest_path, previous_path, bundle.industry_name)
        ready.append(
            {
                "symbol": bundle.symbol,
                "stock_name": bundle.stock_name,
                "final_mandate": bundle.final_mandate,
                "latest_announcement_id": bundle.latest_report.announcement_id,
                "latest_report_date": bundle.latest_report.date,
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
