"""Financial report summary skill helpers based on disclosures cache."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from news.disclosures_builder import AnnouncementMeta, load_index
from utlity.stock_utils import parse_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"
STOCK_INFO_ROOT = DATA_ROOT / "stock_info"
SELECTION_RUNS_ROOT = DATA_ROOT / "selection_runs"

REPORT_PERIOD_PATTERNS = [
    (re.compile(r"一季度|一季报|1季报|Q1|截至\d{4}年\d{1,2}月\d{1,2}日止三个月", re.IGNORECASE), "q1", 1),
    (re.compile(r"半年度|半年报|中报|Q2|中期报告|中期业绩|截至\d{4}年\d{1,2}月\d{1,2}日止六个月", re.IGNORECASE), "interim", 2),
    (re.compile(r"三季度|三季报|Q3|截至\d{4}年\d{1,2}月\d{1,2}日止九个月", re.IGNORECASE), "q3", 3),
    (re.compile(r"年度报告|年报|全年业绩|年度业绩|Q4|截至\d{4}年\d{1,2}月\d{1,2}日止年度", re.IGNORECASE), "annual", 4),
]

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


def load_summary_index(symbol: str) -> Dict[str, Any]:
    path = _summary_index_path(symbol)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_summary_index(symbol: str, payload: Dict[str, Any]) -> Path:
    path = _summary_index_path(symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
    for pattern, report_type, quarter in REPORT_PERIOD_PATTERNS:
        if pattern.search(title):
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


def _select_latest_two_reports(symbol: str) -> tuple[Optional[FinancialReportMeta], Optional[FinancialReportMeta]]:
    reports = _load_financial_report_entries(symbol)
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


def select_latest_two_reports(symbol: str) -> tuple[Optional[FinancialReportMeta], Optional[FinancialReportMeta]]:
    return _select_latest_two_reports(symbol)


def _should_skip(symbol: str, latest: FinancialReportMeta) -> bool:
    summary_index = load_summary_index(symbol)
    latest_completed = summary_index.get("latest_completed_report") or {}
    return latest_completed.get("announcement_id") == latest.announcement_id


def build_stock_report_bundles(run_date: Optional[str] = None, mandate: str = "all") -> List[StockReportBundle]:
    items = load_deep_research_items(run_date, mandate)
    bundles: List[StockReportBundle] = []
    for item in items:
        symbol = item.get("symbol")
        stock_name = item.get("stock_name") or symbol
        final_mandate = item.get("final_mandate") or "unknown"
        industry_name = str(item.get("primary_board") or item.get("board") or item.get("industry") or "所属行业")
        if not isinstance(symbol, str) or not symbol:
            continue
        latest, previous = _select_latest_two_reports(symbol)
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
