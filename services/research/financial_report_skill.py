"""Financial report summary skill helpers based on disclosures cache."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

from configs.stock_pool import TRACKED_A_STOCKS
from core.logging import get_logger
from news.disclosures_builder import AnnouncementMeta, load_index
from services.research.financial_report_summary_prompts import (
    FUTURE_OUTLOOK_PROMPT_TEMPLATE,
    REPORT_ANALYSIS_PROMPT_TEMPLATE,
)
from utlity.stock_utils import parse_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"
STOCK_INFO_ROOT = DATA_ROOT / "stock_info"
SELECTION_RUNS_ROOT = DATA_ROOT / "selection_runs"
RESEARCH_CONFIG_ROOT = PROJECT_ROOT / "configs" / "research"
LOGGER = get_logger("FinancialReportSkill")

REPORT_PERIOD_PATTERNS = [
    (re.compile(r"三季度|三季报|Q3|截至\d{4}年\d{1,2}月\d{1,2}日止九个月|截至\d{4}年\d{1,2}月\d{1,2}日止三个月及九个月", re.IGNORECASE), "q3", 3),
    (re.compile(r"半年度|半年报|中报|Q2|中期报告|中期业绩|截至\d{4}年\d{1,2}月\d{1,2}日止六个月|截至\d{4}年\d{1,2}月\d{1,2}日止三个月及六个月", re.IGNORECASE), "interim", 2),
    (re.compile(r"一季度|一季报|1季报|Q1|截至\d{4}年\d{1,2}月\d{1,2}日止三个月", re.IGNORECASE), "q1", 1),
    (re.compile(r"年度报告|年报|全年业绩|年度业绩|Q4|截至\d{4}年\d{1,2}月\d{1,2}日止年度", re.IGNORECASE), "annual", 4),
]

_CN_DIGITS = str.maketrans({
    '零': '0', '〇': '0',
    '一': '1', '二': '2', '三': '3', '四': '4',
    '五': '5', '六': '6', '七': '7', '八': '8', '九': '9',
})


def _chinese_num_to_int(text: str) -> int:
    if '十' in text:
        parts = text.split('十', 1)
        before = parts[0]
        after = parts[1] if len(parts) > 1 else ''
        value = 0
        if before:
            value = int(before.translate(_CN_DIGITS)) * 10 if before else 10
        else:
            value = 10
        if after:
            value += int(after.translate(_CN_DIGITS)) if after else 0
        return value
    return int(text.translate(_CN_DIGITS))


_CN_DATE_RE = re.compile(
    r'截至([零一二三四五六七八九十〇]+)年([零一二三四五六七八九十〇]+)月([零一二三四五六七八九十〇]+)日'
)


def _convert_chinese_date(title: str) -> str:
    return _CN_DATE_RE.sub(
        lambda m: f'截至{_chinese_num_to_int(m.group(1))}年{_chinese_num_to_int(m.group(2))}月{_chinese_num_to_int(m.group(3))}日',
        title,
    )

FULL_REPORT_HINTS = (
    "年度报告", "年报", "半年度报告", "半年度报告全文", "半年报", "中期报告", "中期报告全文",
    "一季度报告", "第一季度报告", "季度报告", "三季度报告", "第三季度报告", "三季报",
)
SUMMARY_REPORT_HINTS = ("报告摘要", "摘要")
EARNINGS_ANNOUNCEMENT_HINTS = (
    "业绩公告", "业绩公布", "全年业绩", "年度业绩", "中期业绩", "季度业绩公告", "季度业绩公布",
)
PRE_DISCLOSURE_HINTS = (
    "业绩预告", "盈利预告", "业绩快报", "主要经营数据公告", "主要经营数据的公告", "主要经营数据",
)
REPORT_EXCLUDE_HINTS = (
    "说明会", "制度", "问询", "回复", "利润分配", "权益分派", "募集资金", "非经营性资金占用",
    "关联资金往来", "审计委员会", "董事会", "监事会", "自愿性披露", "环境", "ESG", "社会责任",
    "英文版", "英文简版", "更正", "修订", "补充", "通告", "股东大会", "回购", "可转债", "规程",
    "募集说明书", "披露提示",
)

PREVIOUS_PERIOD = {
    ("q1", 1): ("annual", 4),
    ("interim", 2): ("q1", 1),
    ("q3", 3): ("interim", 2),
    ("annual", 4): ("q3", 3),
}


@dataclass(frozen=True)
class FinancialReportMeta:
    announcement_id: str
    title: str
    date: str
    report_type: str
    quarter: int
    fiscal_year: int
    report_kind: Optional[str]
    priority: int
    pdf_path: Optional[Path]
    md_path: Optional[Path]
    announcement_datetime: Optional[str] = None


@dataclass(frozen=True)
class StockReportBundle:
    symbol: str
    stock_name: str
    final_mandate: str
    industry_name: str
    latest_report: FinancialReportMeta
    previous_report: Optional[FinancialReportMeta]
    output_path: Path
    summary_index_path: Path
    skipped: bool = False
    skip_reason: Optional[str] = None


def financial_report_workdir(symbol: str) -> Path:
    return _stock_root(symbol) / "financial_report_workdir"


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


def _copy_to_workdir(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def _clear_stale_research_outputs(
    research_outputs_dir: Path,
    *,
    existing_manifest: Dict[str, Any],
    current_announcement_id: str,
    current_analysis_date: str,
) -> None:
    """Remove only known generated artifacts when the workdir advances report."""

    if (
        existing_manifest.get("latest_announcement_id") == current_announcement_id
        and existing_manifest.get("analysis_date") == current_analysis_date
    ):
        return
    for filename in (
        "industry_scope.json",
        "industry_chain_research.md",
        "expectation_snapshot.md",
        "draft_v1.md",
        "challenge_round_01.md",
        "draft_v2.md",  # 仅清理旧流程遗留文件；新流程不再生成。
    ):
        target = research_outputs_dir / filename
        if target.is_file():
            target.unlink()


def prepare_financial_report_workdir(
    bundle: StockReportBundle,
    *,
    latest_path: Path,
    previous_path: Optional[Path],
    analysis_date: str,
    generate_current_market: bool = True,
    backtest_context_path: Optional[Path] = None,
) -> Path:
    """Prepare deterministic inputs for the multi-agent research workflow."""

    from services.research.financial_report_context import build_financial_report_context

    workdir = financial_report_workdir(bundle.symbol)
    workdir.mkdir(parents=True, exist_ok=True)
    research_outputs_dir = workdir / "research_outputs"
    research_outputs_dir.mkdir(parents=True, exist_ok=True)
    manifest_target = workdir / "manifest.json"
    existing_manifest: Dict[str, Any] = {}
    if manifest_target.exists():
        try:
            existing_manifest = json.loads(manifest_target.read_text(encoding="utf-8"))
        except Exception:
            existing_manifest = {}
    _clear_stale_research_outputs(
        research_outputs_dir,
        existing_manifest=existing_manifest,
        current_announcement_id=bundle.latest_report.announcement_id,
        current_analysis_date=analysis_date,
    )

    latest_target = workdir / "01_latest_report.md"
    previous_target = workdir / "02_previous_report.md"
    report_prompt_target = workdir / "03_report_analysis_prompt.md"
    outlook_prompt_target = workdir / "04_future_outlook_prompt.md"
    agent_input_target = workdir / "05_agent_input.md"
    backtest_context_target = workdir / "00_backtest_context.md"
    valuation_framework_target = workdir / "valuation_framework.md"
    disclosures_md_dir = workdir.parent / "disclosures" / "md"
    disclosures_pdf_dir = workdir.parent / "disclosures" / "pdfs"

    _copy_to_workdir(latest_path, latest_target)
    if backtest_context_path is not None:
        _copy_to_workdir(backtest_context_path, backtest_context_target)
    elif backtest_context_target.exists():
        backtest_context_target.unlink()
    if previous_path:
        _copy_to_workdir(previous_path, previous_target)
    elif previous_target.exists():
        previous_target.unlink()

    report_period = _infer_report_period(latest_path)
    report_prompt_target.write_text(
        REPORT_ANALYSIS_PROMPT_TEMPLATE.format(
            company_name=bundle.stock_name,
            report_period=report_period,
        ),
        encoding="utf-8",
    )
    outlook_prompt_target.write_text(
        FUTURE_OUTLOOK_PROMPT_TEMPLATE.format(
            company_name=bundle.stock_name,
            industry_name=bundle.industry_name,
        ),
        encoding="utf-8",
    )

    valuation_framework_source = RESEARCH_CONFIG_ROOT / "company_valuation_framework.md"
    if not valuation_framework_source.exists():
        raise FileNotFoundError(f"缺少公司估值统一规则: {valuation_framework_source}")
    _copy_to_workdir(valuation_framework_source, valuation_framework_target)

    context = build_financial_report_context(
        symbol=bundle.symbol,
        stock_name=bundle.stock_name,
        industry_name=bundle.industry_name,
        analysis_date=analysis_date,
        announcement_date=bundle.latest_report.date,
        announcement_datetime=bundle.latest_report.announcement_datetime,
        current_announcement_id=bundle.latest_report.announcement_id,
        workdir=workdir,
        stock_root=workdir.parent,
        summary_index_path=bundle.summary_index_path,
        generate_current_market=generate_current_market,
        historical_mode=backtest_context_path is not None,
    )

    policy_paths = {
        "financial_fundamental": RESEARCH_CONFIG_ROOT / "financial_fundamental_research_policy.md",
        "expectation_gap": RESEARCH_CONFIG_ROOT / "expectation_gap_research_policy.md",
        "industry_chain": RESEARCH_CONFIG_ROOT / "industry_chain_research_policy.md",
        "web_research": RESEARCH_CONFIG_ROOT / "web_research_policy.md",
        "output_schema": RESEARCH_CONFIG_ROOT / "financial_report_output_schema.md",
        "valuation_framework": valuation_framework_target,
    }
    missing_policies = [str(path) for path in policy_paths.values() if not path.exists()]
    if missing_policies:
        raise FileNotFoundError(f"缺少财报研究规则文件: {missing_policies}")

    outputs = {
        "industry_chain_research": research_outputs_dir / "industry_chain_research.md",
        "expectation_snapshot": research_outputs_dir / "expectation_snapshot.md",
        "draft_v1": research_outputs_dir / "draft_v1.md",
        "challenge_round_01": research_outputs_dir / "challenge_round_01.md",
        "final_report": bundle.output_path,
    }
    agent_input_lines = [
        f"# {bundle.stock_name} ({bundle.symbol}) 季度基本面研究运行输入",
        "",
        "本文件只保存本次运行的路径、时间边界和写入权限。固定研究方法必须由各角色直接完整读取下列规则文件，主 Agent 不得转述或改写。",
        "",
        "## 本次运行",
        "",
        f"- 股票：{bundle.stock_name} ({bundle.symbol})",
        f"- 候选行业标签：{bundle.industry_name}（仅作线索，不代表已验证的细分产业链或龙头身份）",
        f"- 分析日期：{analysis_date}",
        f"- 最新财报公告日期：{bundle.latest_report.date}",
        f"- 公告时间：{bundle.latest_report.announcement_datetime or '未取得'}",
        f"- 财报前市场日：{context.pre_announcement_market_date}",
        f"- workdir：`{workdir}`",
        (
            f"- 运行模式：历史回测；全体角色必须先完整读取 `{backtest_context_target}`，"
            "任何联网来源不得晚于其中的知识截止日"
            if backtest_context_path is not None
            else "- 运行模式：日常季度基本面研究"
        ),
        "",
        "## 固定规则",
        "",
        *[f"- {name}：`{path}`" for name, path in policy_paths.items()],
        "",
        "## 本地输入",
        "",
        f"- 最新财报原文：`{latest_target}`",
        f"- 上一期关键财报：`{previous_target}`" if previous_path else "- 上一期关键财报：无",
        f"- 本期事实分析任务：`{report_prompt_target}`",
        f"- 未来经营推演任务：`{outlook_prompt_target}`",
        f"- 财报前市场上下文：`{context.pre_context_path}`",
        f"- 当前市场上下文：`{context.current_context_path}`",
        f"- 上期基本面记忆：`{context.prior_memory_path}`",
        f"- 既有产业研究候选：`{context.existing_industry_research_path}`",
        f"- 冻结研究包来源：`{context.frozen_research_path}`" if context.frozen_research_path else "- 冻结研究包来源：未找到",
        "",
        "## 历史披露按需回溯（只读）",
        "",
        f"- Markdown 原文目录：`{disclosures_md_dir}`",
        f"- PDF 原文目录：`{disclosures_pdf_dir}`",
        "- 正常研究不批量读取多年财报。只有固定基本面规则定义的明确历史缺口出现时，才先在 Markdown 目录按文件名、报告期和关键词定位；目标报告没有 Markdown 时再读取对应 PDF。",
        "- 不得修改披露缓存，不得批量转换 PDF，不得因本轮研究自动调用 MinerU。找不到或无法可靠提取时，记录数据缺口及其影响。",
        "- 所有角色继续服从各自时间边界和禁读规则；目录中存在文件不代表该角色有权读取。",
        "",
        "## 角色读取边界",
        "",
        "- Expectation Scout 禁止读取最新财报、当前市场上下文和任何财报后信息；只能读取财报前上下文、上一期原始财报、上期基本面记忆中可验证的旧假设，以及公告前来源。",
        "- Industry Researcher 可读取公司财报以识别业务暴露，但必须独立验证细分产业链，不得沿用候选行业标签下结论。",
        "- Financial Author 第一遍不得读取上期基本面记忆；形成当前事实判断后再读取它做假设兑现检查。",
        "- Research Challenger 读取初稿和全部合法输入，负责审计、追问、补搜和替代解释，不投票。",
        "",
        "## 单写者输出",
        "",
        f"- Industry Researcher：`{outputs['industry_chain_research']}`",
        f"- Expectation Scout：`{outputs['expectation_snapshot']}`",
        f"- Financial Author 初稿：`{outputs['draft_v1']}`",
        f"- Research Challenger：`{outputs['challenge_round_01']}`",
        f"- Financial Author 修订并发布：`{outputs['final_report']}`",
        "",
        "任何角色不得写其他角色文件，不得修改 `summary_index.json`。最终文件通过质量门禁后，由主 Agent 调用注册脚本。",
    ]
    agent_input_target.write_text("\n".join(agent_input_lines).strip() + "\n", encoding="utf-8")

    manifest = {
        "symbol": bundle.symbol,
        "stock_name": bundle.stock_name,
        "final_mandate": bundle.final_mandate,
        "analysis_date": analysis_date,
        "run_mode": "historical_backtest" if backtest_context_path is not None else "live",
        "backtest_context_path": (
            str(backtest_context_target)
            if backtest_context_path is not None
            else None
        ),
        "latest_announcement_id": bundle.latest_report.announcement_id,
        "latest_report_date": bundle.latest_report.date,
        "latest_announcement_datetime": bundle.latest_report.announcement_datetime,
        "pre_announcement_market_date": context.pre_announcement_market_date,
        "frozen_research_path": str(context.frozen_research_path) if context.frozen_research_path else None,
        "latest_report_path": str(latest_target),
        "previous_announcement_id": bundle.previous_report.announcement_id if bundle.previous_report else None,
        "previous_report_date": bundle.previous_report.date if bundle.previous_report else None,
        "previous_report_path": str(previous_target) if previous_path else None,
        "report_analysis_prompt_path": str(report_prompt_target),
        "future_outlook_prompt_path": str(outlook_prompt_target),
        "pre_announcement_market_context_path": str(context.pre_context_path),
        "current_market_context_path": str(context.current_context_path),
        "valuation_framework_path": str(valuation_framework_target),
        "prior_fundamental_memory_path": str(context.prior_memory_path),
        "existing_industry_research_path": str(context.existing_industry_research_path),
        "research_policy_paths": {name: str(path) for name, path in policy_paths.items()},
        "agent_input_path": str(agent_input_target),
        "research_output_paths": {name: str(path) for name, path in outputs.items()},
        "output_path": str(bundle.output_path),
        "summary_index_path": str(bundle.summary_index_path),
        "industry_name": bundle.industry_name,
    }
    manifest_target.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info(
        "财报研究 workdir 已准备: symbol=%s analysis_date=%s workdir=%s",
        bundle.symbol,
        analysis_date,
        workdir,
    )
    return workdir


def validate_deep_research_artifacts(
    *,
    manifest_path: Path,
    final_report_path: Path,
) -> List[str]:
    """Run deterministic publication checks without judging investment content."""

    errors: List[str] = []
    if not manifest_path.exists():
        return [f"缺少 manifest.json: {manifest_path}"]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [f"manifest.json 无法解析: {exc}"]

    declared_final = manifest.get("output_path")
    if declared_final and Path(declared_final).resolve() != final_report_path.resolve():
        errors.append(
            f"最终报告路径与 manifest 不一致: declared={declared_final} actual={final_report_path}"
        )

    output_paths = manifest.get("research_output_paths") or {}
    required = {
        "draft_v1": output_paths.get("draft_v1"),
        "challenge_round_01": output_paths.get("challenge_round_01"),
    }
    contents: Dict[str, str] = {}
    for name, raw_path in required.items():
        if not raw_path:
            errors.append(f"manifest 缺少 research_output_paths.{name}")
            continue
        target = Path(raw_path)
        if not target.exists():
            errors.append(f"缺少研究过程文件: {target}")
            continue
        content = target.read_text(encoding="utf-8", errors="ignore").strip()
        contents[name] = content
        if len("".join(content.split())) < 80:
            errors.append(f"研究过程文件为空或疑似占位: {target}")

    if not final_report_path.exists():
        errors.append(f"缺少最终财报报告: {final_report_path}")
        return errors
    final_content = final_report_path.read_text(encoding="utf-8", errors="ignore").strip()
    if len("".join(final_content.split())) < 80:
        errors.append(f"最终财报报告为空或疑似占位: {final_report_path}")
    if re.search(r"\bTODO\b|PLACEHOLDER|待补充|待完善", final_content, re.IGNORECASE):
        errors.append("最终财报报告仍包含占位符")
    forbidden_fields = (
        "recommended_action",
        "action_num",
        "price_target",
        "stop_loss",
    )
    present_forbidden = [field for field in forbidden_fields if field in final_content]
    if present_forbidden:
        errors.append(f"最终财报报告包含交易字段: {present_forbidden}")

    forbidden_section_pattern = re.compile(
        r"^#{1,6}\s*(?:\d+\s*[.、．]?\s*)?"
        r"(?:未解决问题与披露限制|证据与来源)\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    forbidden_sections = forbidden_section_pattern.findall(final_content)
    if forbidden_sections:
        errors.append("最终财报报告包含禁止的过程性独立章节：未解决问题与披露限制/证据与来源")

    challenge = contents.get("challenge_round_01") or ""
    if re.search(r"严重度\s*[：:]\s*high\b", challenge, re.IGNORECASE):
        # 不要求最终报告暴露质询/修订过程；high 问题应被吸收到风险和验证章节。
        has_risk_section = bool(
            re.search(r"^#{1,6}\s*16\s*[.、．]?", final_content, re.MULTILINE)
        )
        has_watchlist_section = bool(
            re.search(r"^#{1,6}\s*17\s*[.、．]?", final_content, re.MULTILINE)
        )
        if not has_risk_section or not has_watchlist_section:
            errors.append("Challenger 存在 high 问题，但最终报告缺少 §16 风险或 §17 验证清单")
    return errors


def _empty_summary_index(symbol: str, stock_name: Optional[str] = None) -> Dict[str, Any]:
    resolved_stock_name = stock_name or parse_symbol(symbol).stock_name
    return {
        "symbol": symbol,
        "stock_name": resolved_stock_name,
        "latest_completed_report": None,
        "history": [],
    }


def _normalize_summary_entry(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    announcement_id = raw.get("announcement_id")
    report_date = raw.get("report_date") or raw.get("date")
    output_path = raw.get("output_path") or raw.get("path")
    if not announcement_id and not report_date and not output_path:
        return None
    return {
        "announcement_id": announcement_id,
        "report_date": report_date,
        "report_type": raw.get("report_type"),
        "paired_previous_announcement_id": raw.get("paired_previous_announcement_id") or raw.get("previous_announcement_id"),
        "paired_previous_report_date": raw.get("paired_previous_report_date") or raw.get("previous_report_date"),
        "output_path": output_path,
        "generated_at": raw.get("generated_at"),
    }


def _summary_entry_identity(entry: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(entry.get("announcement_id") or ""),
        str(entry.get("report_date") or ""),
        str(entry.get("output_path") or ""),
    )


def _merge_summary_entry(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if merged.get(key) in (None, "") and value not in (None, ""):
            merged[key] = value
    return merged


def _summary_entry_sort_key(entry: Dict[str, Any]) -> tuple[str, str, str]:
    report_date = str(entry.get("report_date") or "").replace("-", "")
    generated_at = str(entry.get("generated_at") or "")
    announcement_id = str(entry.get("announcement_id") or "")
    return report_date, generated_at, announcement_id


def normalize_summary_index_payload(symbol: str, payload: Any) -> Dict[str, Any]:
    stock_name = None
    raw_entries: List[Dict[str, Any]] = []

    if isinstance(payload, dict):
        stock_name = payload.get("stock_name")
        latest_entry = _normalize_summary_entry(payload.get("latest_completed_report") or payload.get("latest"))
        if latest_entry is not None:
            raw_entries.append(latest_entry)
        for key in ("history", "records", "reports"):
            items = payload.get(key)
            if not isinstance(items, list):
                continue
            for item in items:
                normalized = _normalize_summary_entry(item)
                if normalized is not None:
                    raw_entries.append(normalized)
    elif isinstance(payload, list):
        for item in payload:
            normalized = _normalize_summary_entry(item)
            if normalized is not None:
                raw_entries.append(normalized)

    canonical = _empty_summary_index(symbol, stock_name=stock_name)
    merged_entries: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for entry in raw_entries:
        identity = _summary_entry_identity(entry)
        if identity in merged_entries:
            merged_entries[identity] = _merge_summary_entry(merged_entries[identity], entry)
        else:
            merged_entries[identity] = entry

    history = sorted(merged_entries.values(), key=_summary_entry_sort_key, reverse=True)[:20]
    canonical["history"] = history
    canonical["latest_completed_report"] = history[0] if history else None
    return canonical


def load_summary_index(symbol: str) -> Dict[str, Any]:
    path = _summary_index_path(symbol)
    if not path.exists():
        return _empty_summary_index(symbol)
    try:
        return normalize_summary_index_payload(symbol, json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return _empty_summary_index(symbol)


def save_summary_index(symbol: str, payload: Any) -> Path:
    path = _summary_index_path(symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_summary_index_payload(symbol, payload)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def update_summary_index(
    *,
    symbol: str,
    stock_name: str,
    latest_report: Any,
    previous_report: Optional[Any],
    output_path: Path,
) -> Path:
    latest_announcement_id = getattr(latest_report, "announcement_id", None) or latest_report["announcement_id"]
    latest_date = getattr(latest_report, "date", None) or latest_report["date"]
    latest_report_type = getattr(latest_report, "report_type", None) or latest_report.get("report_type") or "unknown"
    prev_announcement_id = None
    prev_date = None
    if previous_report is not None:
        prev_announcement_id = getattr(previous_report, "announcement_id", None) or previous_report.get("announcement_id")
        prev_date = getattr(previous_report, "date", None) or previous_report.get("date")
    current = load_summary_index(symbol)
    history = current.get("history") or []
    entry = {
        "announcement_id": latest_announcement_id,
        "report_date": latest_date,
        "report_type": latest_report_type,
        "paired_previous_announcement_id": prev_announcement_id,
        "paired_previous_report_date": prev_date,
        "output_path": str(output_path),
        "generated_at": datetime.now().isoformat(),
    }
    history = [item for item in history if item.get("announcement_id") != latest_announcement_id]
    history.insert(0, entry)
    payload = {
        "symbol": symbol,
        "stock_name": stock_name,
        "latest_completed_report": entry,
        "history": history[:20],
    }
    return save_summary_index(symbol, payload)


def _stock_root(symbol: str) -> Path:
    info = parse_symbol(symbol)
    return STOCK_INFO_ROOT / f"{info.stock_name}_{info.symbol}"


def _summary_dir(symbol: str) -> Path:
    return _stock_root(symbol) / "financial_reports"


def _summary_index_path(symbol: str) -> Path:
    return _summary_dir(symbol) / "summary_index.json"


def _output_path(symbol: str, report_date: str) -> Path:
    return _summary_dir(symbol) / f"{report_date.replace('-', '')}.md"


def _resolve_report_type(title: str) -> tuple[Optional[str], Optional[int]]:
    cleaned = title.replace(" ", "")
    cleaned = _convert_chinese_date(cleaned)
    for pattern, report_type, quarter in REPORT_PERIOD_PATTERNS:
        if pattern.search(cleaned):
            return report_type, quarter
    return None, None


def _classify_report_title(symbol: str, title: str) -> Tuple[Optional[str], Optional[int], Optional[str], int]:
    symbol_info = parse_symbol(symbol)
    text = title.strip()
    report_type, quarter = _resolve_report_type(text)
    if not report_type or not quarter:
        return None, None, None, 99

    if any(hint in text for hint in REPORT_EXCLUDE_HINTS):
        return report_type, quarter, None, 99

    # A 股代码优先使用 A 股口径正文，避免误拿 H 股镜像披露
    if symbol_info.is_cn_market() and "H股公告" in text:
        return report_type, quarter, None, 99

    if any(hint in text for hint in SUMMARY_REPORT_HINTS):
        return report_type, quarter, "summary_report", 1
    if any(hint in text for hint in FULL_REPORT_HINTS):
        return report_type, quarter, "full_report", 0
    if any(hint in text for hint in EARNINGS_ANNOUNCEMENT_HINTS):
        return report_type, quarter, "earnings_announcement", 2
    if any(hint in text for hint in PRE_DISCLOSURE_HINTS):
        return report_type, quarter, "pre_disclosure", 3

    # 港股很多正式财报标题就是“全年业绩公告/中期业绩公告”
    if symbol_info.is_hk_market() and "公告" in text and any(
        token in text for token in ("全年业绩", "中期业绩", "截至", "年度", "六个月", "九个月", "三个月")
    ):
        return report_type, quarter, "earnings_announcement", 2

    return report_type, quarter, "other_report_like", 5


def _resolve_fiscal_year(title: str, report_date: str) -> int:
    year_match = re.search(r"(20\d{2}|\d{2})年", title)
    if year_match:
        raw = year_match.group(1)
        return int(raw) if len(raw) == 4 else 2000 + int(raw)
    if not report_date:
        return 0
    base_year = int(report_date[:4])
    report_type, quarter = _resolve_report_type(title)
    # 年报/全年业绩通常在次年披露，对应上一财年
    if report_type == "annual" and quarter == 4:
        return base_year - 1
    return base_year


def _report_period_rank(fiscal_year: int, quarter: int) -> int:
    month_by_quarter = {1: 3, 2: 6, 3: 9, 4: 12}
    return fiscal_year * 12 + month_by_quarter.get(quarter, 0)


def _extract_announcement_datetime(meta: AnnouncementMeta) -> Optional[str]:
    """Recover the provider timestamp when it is preserved in the source URL."""

    if not meta.url:
        return None
    try:
        values = parse_qs(urlparse(meta.url).query).get("announcementTime") or []
        if not values:
            return None
        raw_value = unquote(str(values[0])).strip()
        parsed = datetime.fromisoformat(raw_value)
        if parsed.time() == datetime.min.time():
            # 巨潮常用 00:00:00 作为“仅有日期”的占位，不代表真实盘前时间。
            return None
        return parsed.isoformat(sep=" ")
    except Exception:
        return None


def resolve_latest_deep_research_queue(run_date: Optional[str] = None) -> Path:
    if run_date:
        candidate = SELECTION_RUNS_ROOT / run_date / "11_deep_research_queue.json"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"未找到 deep research queue: {candidate}")
    dated_dirs = sorted([path for path in SELECTION_RUNS_ROOT.iterdir() if path.is_dir()], reverse=True)
    for path in dated_dirs:
        candidate = path / "11_deep_research_queue.json"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("未找到任何 11_deep_research_queue.json")


def load_deep_research_items(run_date: Optional[str] = None, mandate: str = "all") -> List[Dict[str, Any]]:
    payload = json.loads(resolve_latest_deep_research_queue(run_date).read_text(encoding="utf-8"))
    items = payload.get("items") or []
    if mandate == "all":
        return items
    return [item for item in items if item.get("final_mandate") == mandate]


def _load_financial_report_entries(symbol: str) -> List[FinancialReportMeta]:
    disclosures_index = _stock_root(symbol) / "disclosures" / "index.json"
    if not disclosures_index.exists():
        return []
    index: Dict[str, AnnouncementMeta] = load_index(disclosures_index)
    results: List[FinancialReportMeta] = []
    for meta in index.values():
        if not meta.is_financial_report:
            continue
        report_type, quarter, report_kind, priority = _classify_report_title(symbol, meta.title)
        if not report_type or not quarter:
            continue
        if priority >= 99:
            continue
        report_date = meta.date or ""
        results.append(
            FinancialReportMeta(
                announcement_id=meta.announcement_id,
                title=meta.title,
                date=report_date,
                report_type=report_type,
                quarter=quarter,
                fiscal_year=_resolve_fiscal_year(meta.title, report_date),
                report_kind=report_kind,
                priority=priority,
                pdf_path=Path(meta.pdf_path) if meta.pdf_path else None,
                md_path=Path(meta.md_path) if meta.md_path else None,
                announcement_datetime=_extract_announcement_datetime(meta),
            )
        )
    results.sort(
        key=lambda item: (
            -_report_period_rank(item.fiscal_year, item.quarter),
            item.priority,
            -(int(item.date.replace("-", "")) if item.date else 0),
            item.announcement_id,
        )
    )
    return results


def _select_latest_two_reports(
    symbol: str,
    *,
    available_on_date: Optional[str] = None,
) -> tuple[Optional[FinancialReportMeta], Optional[FinancialReportMeta]]:
    reports = _load_financial_report_entries(symbol)
    if available_on_date:
        reports = [
            report
            for report in reports
            if report.date and report.date <= available_on_date
        ]
    if not reports:
        return None, None
    latest = reports[0]
    prev_type = PREVIOUS_PERIOD.get((latest.report_type, latest.quarter))
    if prev_type is None:
        return latest, reports[1] if len(reports) > 1 else None
    target_report_type, target_quarter = prev_type
    target_year = latest.fiscal_year - 1 if latest.report_type == "q1" else latest.fiscal_year
    previous = next(
        (
            report
            for report in reports[1:]
            if report.report_type == target_report_type
            and report.quarter == target_quarter
            and report.fiscal_year == target_year
        ),
        None,
    )
    if previous is None and len(reports) > 1:
        previous = reports[1]
    return latest, previous


def select_latest_two_reports(
    symbol: str,
    *,
    available_on_date: Optional[str] = None,
) -> tuple[Optional[FinancialReportMeta], Optional[FinancialReportMeta]]:
    return _select_latest_two_reports(
        symbol,
        available_on_date=available_on_date,
    )


def _should_skip(symbol: str, latest: FinancialReportMeta) -> bool:
    summary_index = load_summary_index(symbol)
    completed = [summary_index.get("latest_completed_report") or {}]
    completed.extend(summary_index.get("history") or [])
    for entry in completed:
        if not isinstance(entry, dict):
            continue
        if entry.get("announcement_id") != latest.announcement_id:
            continue
        raw_path = entry.get("output_path")
        if not raw_path:
            continue
        output_path = Path(raw_path)
        if not output_path.is_absolute():
            output_path = PROJECT_ROOT / output_path
        if output_path.exists() and output_path.stat().st_size > 0:
            return True
    return False


def synthesize_manual_item(
    symbol: str,
    *,
    stock_name: Optional[str] = None,
    industry: Optional[str] = None,
    final_mandate: str = "manual",
) -> Dict[str, Any]:
    """Build a deep-research-queue-shaped item for stocks outside of the queue.

    用于把 TRACKED_A_STOCKS 或用户通过 --symbols 指定的股票拼成与 queue items
    兼容的字典，使其可直接喂给 build_stock_report_bundles。
    """
    resolved_name = stock_name or parse_symbol(symbol).stock_name or symbol
    item: Dict[str, Any] = {
        "symbol": symbol,
        "stock_name": resolved_name,
        "final_mandate": final_mandate,
    }
    if industry:
        item["industry"] = industry
    return item


def load_tracked_items(final_mandate: str = "tracked") -> List[Dict[str, Any]]:
    """TRACKED_A_STOCKS → queue-shaped items（描述字段当作 industry）。"""
    return [
        synthesize_manual_item(
            entry.symbol,
            stock_name=entry.name,
            industry=entry.description,
            final_mandate=final_mandate,
        )
        for entry in TRACKED_A_STOCKS
    ]


def build_stock_report_bundles(
    run_date: Optional[str] = None,
    mandate: str = "all",
    *,
    extra_items: Optional[List[Dict[str, Any]]] = None,
    skip_queue: bool = False,
) -> List[StockReportBundle]:
    if skip_queue:
        items: List[Dict[str, Any]] = []
    else:
        items = list(load_deep_research_items(run_date, mandate))
    if extra_items:
        seen = {item.get("symbol") for item in items if item.get("symbol")}
        for extra in extra_items:
            symbol = extra.get("symbol")
            if not symbol or symbol in seen:
                continue
            items.append(extra)
            seen.add(symbol)
    bundles: List[StockReportBundle] = []
    for item in items:
        symbol = item.get("symbol")
        stock_name = item.get("stock_name") or symbol
        final_mandate = item.get("final_mandate") or "unknown"
        industry_name = str(item.get("primary_board") or item.get("board") or item.get("industry") or "所属行业")
        if not isinstance(symbol, str) or not symbol:
            continue
        latest, previous = _select_latest_two_reports(
            symbol,
            available_on_date=run_date,
        )
        summary_index_path = _summary_index_path(symbol)
        if latest is None:
            bundles.append(
                StockReportBundle(
                    symbol=symbol,
                    stock_name=stock_name,
                    final_mandate=final_mandate,
                    industry_name=industry_name,
                    latest_report=FinancialReportMeta("", "", "", "", 0, 0, None, 99, None, None),
                    previous_report=None,
                    output_path=_output_path(symbol, "1970-01-01"),
                    summary_index_path=summary_index_path,
                    skipped=True,
                    skip_reason="missing_financial_reports_in_disclosures",
                )
            )
            continue
        skipped = _should_skip(symbol, latest)
        bundles.append(
            StockReportBundle(
                symbol=symbol,
                stock_name=stock_name,
                final_mandate=final_mandate,
                industry_name=industry_name,
                latest_report=latest,
                previous_report=previous,
                output_path=_output_path(symbol, latest.date),
                summary_index_path=summary_index_path,
                skipped=skipped,
                skip_reason="already_summarized_latest_report" if skipped else None,
            )
        )
    return bundles
