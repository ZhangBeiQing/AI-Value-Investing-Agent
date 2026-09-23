"""Build time-isolated local context for quarterly fundamental research."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from core.logging import get_logger
from shared_data_access import update_hk_profit_forecast_cached
from commons import get_latest_trading_day, parse_symbol


LOGGER = get_logger("FinancialReportContext")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"
SKILL_RUNS_ROOT = DATA_ROOT / "skill_runs"
INDUSTRY_CARDS_ROOT = DATA_ROOT / "industry_research" / "cards"
BOOK_PRIORITY = ("fixed_tracked", "long_book", "short_book")
DATE_DIR_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
VALUATION_DATE_PATTERN = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")


@dataclass(frozen=True)
class HistoricalResearchPackage:
    """Optional dated research material available before the announcement."""

    snapshot_date: str
    book_type: str
    path: Path


@dataclass(frozen=True)
class FinancialReportContextResult:
    """Paths and metadata generated for one stock workdir."""

    analysis_date: str
    announcement_date: str
    announcement_datetime: Optional[str]
    pre_announcement_market_date: str
    historical_research_path: Optional[Path]
    pre_context_path: Path
    current_context_path: Path
    prior_memory_path: Path
    existing_industry_research_path: Path


def _parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _previous_weekday(value: date) -> date:
    candidate = value - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def resolve_pre_announcement_market_date(
    announcement_date: str,
    announcement_datetime: str | datetime | None = None,
    *,
    market_close: time = time(15, 0),
) -> str:
    """Resolve the last market snapshot visible before a financial announcement.

    When only an announcement date is available, use the previous weekday
    conservatively. A known after-close timestamp may use the same trading day.
    """

    announced_on = _parse_iso_date(announcement_date)
    parsed_datetime: Optional[datetime] = None
    if isinstance(announcement_datetime, datetime):
        parsed_datetime = announcement_datetime
    elif announcement_datetime:
        parsed = pd.to_datetime(announcement_datetime, errors="coerce")
        if parsed is not None and not pd.isna(parsed):
            parsed_datetime = parsed.to_pydatetime()

    if (
        parsed_datetime is not None
        and parsed_datetime.date() == announced_on
        and parsed_datetime.time() >= market_close
        and announced_on.weekday() < 5
    ):
        return announced_on.isoformat()
    return _previous_weekday(announced_on).isoformat()


def extract_markdown_section(markdown: str, heading: str) -> str:
    """Extract an exact Markdown heading and its body until the next peer heading."""

    heading_match = re.search(
        rf"(?m)^(?P<marks>#+)\s+{re.escape(heading.strip())}\s*$",
        markdown,
    )
    if heading_match is None:
        return ""
    level = len(heading_match.group("marks"))
    next_heading = re.search(
        rf"(?m)^#{{1,{level}}}\s+.+$",
        markdown[heading_match.end() :],
    )
    end = heading_match.end() + next_heading.start() if next_heading else len(markdown)
    return markdown[heading_match.start() : end].strip()


def extract_markdown_section_by_prefix(markdown: str, heading_prefix: str) -> str:
    """Extract a heading whose text starts with a stable prefix."""

    heading_match = re.search(
        rf"(?m)^(?P<marks>#+)\s+{re.escape(heading_prefix.strip())}.*$",
        markdown,
    )
    if heading_match is None:
        return ""
    level = len(heading_match.group("marks"))
    next_heading = re.search(
        rf"(?m)^#{{1,{level}}}\s+.+$",
        markdown[heading_match.end() :],
    )
    end = heading_match.end() + next_heading.start() if next_heading else len(markdown)
    return markdown[heading_match.start() : end].strip()


def extract_pre_announcement_research_sections(markdown: str) -> dict[str, str]:
    """Extract only market/valuation/consensus inputs, excluding trade memory."""

    market = extract_markdown_section(markdown, "1. 股票指标与估值")
    news = extract_markdown_section(markdown, "2. 新闻与公告")
    consensus = ""
    for heading_prefix in (
        "A股机构一致预期（同花顺汇总",
        "港股机构一致预期（经济通",
    ):
        section = extract_markdown_section_by_prefix(markdown, heading_prefix)
        if section:
            consensus = "\n\n".join(part for part in (consensus, section) if part)
    return {
        "market_and_valuation": market,
        "news_and_guidance": news,
        "annual_consensus": consensus,
    }


def _render_hk_pre_announcement_consensus(
    symbol: str,
    announcement_date: str,
    *,
    historical_mode: bool,
) -> tuple[str, Optional[Path]]:
    """Load HK broker forecasts and remove records updated after the cutoff."""

    symbol_info = parse_symbol(symbol)
    if not symbol_info.is_hk_market():
        return "", None

    stock_root = DATA_ROOT / "stock_info" / f"{symbol_info.stock_name}_{symbol_info.symbol}"
    forecast_dir = stock_root / "profit_forecast"
    prior_calendar_date = (_parse_iso_date(announcement_date) - timedelta(days=1)).isoformat()
    cutoff_snapshot = forecast_dir / "snapshots" / f"{prior_calendar_date.replace('-', '')}.csv"
    current_path = forecast_dir / "profit_forecast.csv"

    if historical_mode:
        source_path = cutoff_snapshot if cutoff_snapshot.exists() else None
        frame = pd.read_csv(source_path) if source_path else pd.DataFrame()
    else:
        frame = update_hk_profit_forecast_cached(
            symbol_info,
            base_data_dir=DATA_ROOT,
            force_refresh=False,
            logger=LOGGER,
        )
        source_path = current_path if current_path.exists() else None

    if frame is None or frame.empty or "更新日期" not in frame.columns:
        return "", source_path

    work = frame.copy()
    work["更新日期"] = pd.to_datetime(work["更新日期"], errors="coerce")
    # AkShare only supplies a date, not an intraday timestamp. Same-day rows
    # cannot be proven to predate the announcement, so require date < announcement date.
    cutoff = pd.Timestamp(announcement_date)
    work = work.loc[work["更新日期"].notna() & (work["更新日期"] < cutoff)].copy()
    if work.empty:
        return "", source_path

    work["更新日期"] = work["更新日期"].dt.strftime("%Y-%m-%d")
    preferred = [
        "财政年度",
        "纯利/亏损",
        "每股盈利",
        "每股派息",
        "证券商",
        "评级",
        "目标价",
        "更新日期",
    ]
    columns = [column for column in preferred if column in work.columns]
    work = work.sort_values(["财政年度", "更新日期"], ascending=[True, False])
    return work[columns].to_markdown(index=False), source_path


def _render_pre_announcement_price_context(
    symbol: str,
    cutoff_date: str,
) -> tuple[str, Optional[Path]]:
    """Build point-in-time price context directly from cached daily prices."""

    symbol_info = parse_symbol(symbol)
    stock_root = DATA_ROOT / "stock_info" / f"{symbol_info.stock_name}_{symbol_info.symbol}"
    price_path = stock_root / "prices" / "price.csv"
    if not price_path.exists():
        return "", None
    try:
        frame = pd.read_csv(price_path)
    except Exception as exc:
        LOGGER.warning("读取财报前价格缓存失败: path=%s error=%s", price_path, exc)
        return "", price_path
    date_column = next((column for column in ("日期", "date", "Date") if column in frame.columns), None)
    close_column = next((column for column in ("收盘", "收盘价", "close", "Close") if column in frame.columns), None)
    if not date_column or not close_column:
        return "", price_path
    work = frame[[date_column, close_column]].copy()
    work[date_column] = pd.to_datetime(work[date_column], errors="coerce")
    work[close_column] = pd.to_numeric(work[close_column], errors="coerce")
    work = work.dropna().loc[lambda value: value[date_column] <= pd.Timestamp(cutoff_date)]
    work = work.sort_values(date_column)
    if work.empty:
        return "", price_path
    latest = work.iloc[-1]
    latest_price = float(latest[close_column])

    def _return_for_sessions(sessions: int) -> str:
        if len(work) <= sessions:
            return "样本不足"
        base = float(work.iloc[-sessions - 1][close_column])
        return f"{(latest_price / base - 1) * 100:.2f}%" if base else "无法计算"

    trailing_252 = work.tail(252)
    lines = [
        f"- 截止日收盘：{latest_price:.3f}（{latest[date_column].date().isoformat()}）",
        f"- 近 5 个交易日收益：{_return_for_sessions(5)}",
        f"- 近 20 个交易日收益：{_return_for_sessions(20)}",
        f"- 近 60 个交易日收益：{_return_for_sessions(60)}",
        f"- 近 252 个交易日收盘区间：{float(trailing_252[close_column].min()):.3f} - {float(trailing_252[close_column].max()):.3f}",
    ]
    return "\n".join(lines), price_path


def find_historical_research_package(
    symbol: str,
    cutoff_date: str,
    *,
    skill_runs_root: Path = SKILL_RUNS_ROOT,
) -> Optional[HistoricalResearchPackage]:
    """Find the nearest optional research package not later than cutoff_date."""

    if not skill_runs_root.exists():
        return None
    dated_dirs = sorted(
        (
            path
            for path in skill_runs_root.iterdir()
            if path.is_dir()
            and DATE_DIR_PATTERN.match(path.name)
            and path.name <= cutoff_date
        ),
        key=lambda path: path.name,
        reverse=True,
    )
    for dated_dir in dated_dirs:
        for book_type in BOOK_PRIORITY:
            research_dir = dated_dir / book_type / "04_stock_research"
            if not research_dir.is_dir():
                continue
            matches = sorted(
                path
                for path in research_dir.iterdir()
                if path.is_file()
                and path.suffix.lower() == ".md"
                and symbol in path.name
            )
            if matches:
                return HistoricalResearchPackage(
                    snapshot_date=dated_dir.name,
                    book_type=book_type,
                    path=matches[0],
                )
    return None


def calculate_pro_forma_ttm(
    last_fiscal_year_value: float,
    current_year_to_date_value: float,
    prior_year_to_date_value: float,
) -> float:
    """Calculate pro-forma TTM from the latest cumulative report."""

    return last_fiscal_year_value + current_year_to_date_value - prior_year_to_date_value


def detect_valuation_basis_dates(valuation_markdown: str | None) -> list[str]:
    """Return financial/report dates mentioned by a generated valuation report."""

    if not valuation_markdown:
        return []
    return list(dict.fromkeys(VALUATION_DATE_PATTERN.findall(valuation_markdown)))


def _render_current_consensus(
    stock_root: Path,
    analysis_date: str,
    *,
    historical_mode: bool = False,
) -> tuple[str, Optional[Path]]:
    forecast_dir = stock_root / "profit_forecast"
    snapshot = forecast_dir / "snapshots" / f"{analysis_date.replace('-', '')}.csv"
    current = forecast_dir / "profit_forecast.csv"
    source_path = (
        snapshot
        if snapshot.exists()
        else (None if historical_mode else current)
    )
    if source_path is None:
        return (
            "> 未找到分析日不可变机构一致预期快照；历史回测禁止回退到当前缓存。",
            None,
        )
    if not source_path.exists():
        return "> 未找到分析日机构一致预期缓存。", None
    try:
        frame = pd.read_csv(source_path)
    except Exception as exc:
        LOGGER.warning("读取一致预期缓存失败: path=%s error=%s", source_path, exc)
        return f"> 一致预期缓存存在但读取失败：{exc}", source_path
    if frame.empty:
        return "> 分析日机构一致预期缓存为空。", source_path
    return frame.to_markdown(index=False), source_path


def _render_pre_context(
    *,
    symbol: str,
    stock_name: str,
    announcement_date: str,
    announcement_datetime: Optional[str],
    requested_pre_date: str,
    evidence_cutoff: str = "",
    historical_research: Optional[HistoricalResearchPackage],
    hk_consensus: str = "",
    hk_consensus_path: Optional[Path] = None,
    price_context: str = "",
    price_context_path: Optional[Path] = None,
) -> str:
    lines = [
        f"# {stock_name} ({symbol}) 财报前市场上下文",
        "",
        "## 时间边界",
        "",
        f"- 财报公告日期：{announcement_date}",
        f"- 财报公告时间：{announcement_datetime or '未取得；按保守规则处理'}",
        f"- 请求的财报前市场日：{requested_pre_date}",
        f"- 公告前证据截止：{evidence_cutoff or announcement_datetime or announcement_date}",
    ]
    if historical_research is None:
        lines.extend(
            [
                "- 历史 04 研究包：未找到（可选资料，不影响本次重建）",
                "",
                "> 本上下文按证据截止时间重建。不得使用财报公告后的机构预测、价格反应或评论形成本次公告前预期；研究发生在公告后不影响回溯检索公告前资料。",
            ]
        )
        if price_context:
            lines.extend(
                [
                    "",
                    "## 财报前价格表现",
                    "",
                    f"- 数据来源：`{price_context_path}`" if price_context_path else "- 数据来源：历史行情缓存",
                    f"- 强制时间截断：不晚于 {requested_pre_date}",
                    "",
                    price_context,
                ]
            )
        if hk_consensus:
            lines.extend(
                [
                    "",
                    "## 财报前年度一致预期",
                    "",
                    f"- 缓存来源：`{hk_consensus_path}`" if hk_consensus_path else "- 缓存来源：AkShare 港股盈利预测",
                    f"- 时间准入：AkShare 记录只有日期，故要求更新日期早于 {announcement_date}；有可核验时分的外部资料可使用至公告时刻前",
                    "",
                    hk_consensus,
                    "",
                    "> 该表为机构年度预测，不是季度一致预期。Expectation Scout 仍须联网搜索公告前的季度预测、公司指引和产业隐含预期。",
                ]
            )
        return "\n".join(lines).strip() + "\n"

    sections = extract_pre_announcement_research_sections(
        historical_research.path.read_text(encoding="utf-8", errors="ignore")
    )
    lines.extend(
        [
            f"- 可选历史研究资料日期：{historical_research.snapshot_date}",
            f"- 可选资料账本来源：{historical_research.book_type}",
            f"- 可选历史研究包：`{historical_research.path}`",
            "",
            "> 历史研究包只作补充资料；本次公告前预期仍须按证据截止时间独立重建，不得继承旧结论或交易计划。",
            "",
            "## 财报前价格与估值",
            "",
            price_context or sections["market_and_valuation"] or "> 未取得截止日前价格或估值资料。",
            "",
            "## 财报前公告、经营数据与正式指引",
            "",
            sections["news_and_guidance"] or "> 历史研究包未提供公告前新闻；Expectation Scout 必须联网回溯检索。",
            "",
            "## 财报前年度一致预期",
            "",
            hk_consensus or sections["annual_consensus"] or "> 本地未取得年度机构预测；Expectation Scout 必须联网回溯检索。",
            "",
            "> 上述同花顺预测是年度锚，不是季度一致预期。没有高可信财报前季度预测时，不得据此计算精确的季度 surprise。",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _render_current_context(
    *,
    symbol: str,
    stock_name: str,
    analysis_date: str,
    announcement_date: str,
    market_payload: Optional[dict[str, Any]],
    market_error: Optional[str],
    stock_root: Path,
    historical_mode: bool = False,
) -> str:
    consensus_markdown, consensus_path = _render_current_consensus(
        stock_root,
        analysis_date,
        historical_mode=historical_mode,
    )
    if market_payload:
        price_report = json.dumps(
            market_payload.get("price_report"),
            ensure_ascii=False,
            indent=2,
        )
        valuation_report = market_payload.get("valuation_report") or (
            "> 当前估值不可用："
            + str(market_payload.get("valuation_unavailable_reason") or "未知原因")
        )
        basis_dates = detect_valuation_basis_dates(str(valuation_report))
    else:
        price_report = f"> 当前价格报告生成失败：{market_error or '未知原因'}"
        valuation_report = f"> 当前估值报告生成失败：{market_error or '未知原因'}"
        basis_dates = []

    lines = [
        f"# {stock_name} ({symbol}) 当前市场与估值上下文",
        "",
        "## 时间与口径",
        "",
        f"- 分析日期：{analysis_date}",
        f"- 最新财报公告日期：{announcement_date}",
        f"- 当前估值报告出现的日期：{', '.join(basis_dates) if basis_dates else '未识别'}",
        "",
        "> 增强估值报告可能只更新了价格、仍沿用新财报发布前的财务基准期。必须先核对报告期，再使用 PE/PEG。不得默认下列估值已经吸收本期新财报。",
        "",
        "## 当前价格报告",
        "",
        "```json",
        price_report,
        "```",
        "",
        "## 当前增强估值报告",
        "",
        str(valuation_report),
        "",
        "## 当前年度一致预期",
        "",
        f"- 缓存来源：`{consensus_path}`" if consensus_path else "- 缓存来源：未找到",
        (
            "- 快照状态：命中分析日不可变快照"
            if consensus_path and consensus_path.parent.name == "snapshots"
            else "- 快照状态：仅命中兼容的当前缓存；若分析日是历史日期，不得把它当作当时可见预测"
        ),
        "",
        consensus_markdown,
        "",
        "> 该表仍是年度预测。应与财报前冻结快照比较预测修正，并计算“全年预测减累计实际”的剩余业绩要求。",
        "",
        "## 新财报口径估值重算",
        "",
        "Python 准备层未从非统一格式的财报 Markdown 猜测利润数字。Financial Author 必须从财报原文核对单位并计算：",
        "",
        "```text",
        "pro-forma TTM = 上一完整财年值 + 本年累计值 - 上年同期累计值",
        "新口径 PE = 当前总市值 / 新口径 TTM 扣非归母净利润",
        "Forward PE = 当前总市值 / 对应年度可追溯盈利预测",
        "```",
        "",
        "如任一输入无法核实，明确写“无法计算”，不得用旧口径估值冒充新财报口径。",
    ]
    return "\n".join(lines).strip() + "\n"


def _select_prior_summary(
    summary_index_payload: dict[str, Any],
    current_announcement_id: str,
    current_report_date: str,
) -> Optional[Path]:
    for entry in summary_index_payload.get("history") or []:
        if entry.get("announcement_id") == current_announcement_id:
            continue
        report_date = str(entry.get("report_date") or "")
        if not report_date or report_date >= current_report_date:
            continue
        raw_path = entry.get("output_path")
        if not raw_path:
            continue
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        if candidate.exists():
            return candidate
    return None


def _extract_prior_memory(markdown: str) -> list[str]:
    allowed = re.compile(r"假设|指引|待核验|未解决|验证清单|证伪")
    blocked = re.compile(r"交易|买入|卖出|操作|目标价|仓位|止损|推荐")
    headings = list(re.finditer(r"(?m)^(#{1,6})\s+(.+?)\s*$", markdown))
    sections: list[str] = []
    for index, match in enumerate(headings):
        title = match.group(2)
        if not allowed.search(title) or blocked.search(title):
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(markdown)
        body = markdown[match.start() : end].strip()
        if body:
            sections.append(body)
    return sections


def _write_prior_memory(
    path: Path,
    *,
    summary_index_path: Path,
    current_announcement_id: str,
    current_report_date: str,
) -> None:
    payload: dict[str, Any] = {}
    if summary_index_path.exists():
        try:
            payload = json.loads(summary_index_path.read_text(encoding="utf-8"))
        except Exception as exc:
            LOGGER.warning("读取财报 summary_index 失败: path=%s error=%s", summary_index_path, exc)
    prior_path = _select_prior_summary(
        payload,
        current_announcement_id,
        current_report_date,
    )
    lines = [
        "# 上期基本面记忆",
        "",
        "> Expectation Scout 可用它核对公告前已经存在的旧指引、旧假设和待验证问题；Financial Author 只能在完成当前财报第一遍独立分析后读取。它不包含旧交易动作，也不得作为当前结论的默认锚。",
        "",
    ]
    if prior_path is None:
        lines.append("未找到早于本期公告的既有基本面报告。")
    else:
        sections = _extract_prior_memory(prior_path.read_text(encoding="utf-8", errors="ignore"))
        lines.extend([f"- 来源：`{prior_path}`", ""])
        if sections:
            lines.extend(sections)
        else:
            lines.append("上期报告未发现可安全抽取的“假设、指引、未解决问题或验证清单”章节；为避免历史投资结论锚定，本次不自动复制其他内容。")
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _find_industry_card_candidates(
    *,
    symbol: str,
    stock_name: str,
    industry_name: str,
    cards_root: Path = INDUSTRY_CARDS_ROOT,
    cutoff_date: Optional[str] = None,
) -> list[Path]:
    if not cards_root.exists():
        return []
    tokens = [
        token.casefold()
        for token in (symbol, stock_name, industry_name)
        if token and token != "所属行业" and len(token.strip()) >= 3
    ]
    candidates: list[tuple[int, Path]] = []
    for path in cards_root.rglob("*.md"):
        if cutoff_date is not None:
            date_match = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", path.name)
            if date_match is None:
                continue
            card_date = "-".join(date_match.groups())
            if card_date > cutoff_date:
                continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").casefold()
        except OSError:
            continue
        score = sum(1 for token in tokens if token in text)
        if score:
            candidates.append((score, path))
    candidates.sort(key=lambda item: (item[0], item[1].stat().st_mtime), reverse=True)
    return [path for _, path in candidates]


def _write_existing_industry_research(
    path: Path,
    *,
    symbol: str,
    stock_name: str,
    industry_name: str,
    cutoff_date: Optional[str] = None,
) -> None:
    candidates = _find_industry_card_candidates(
        symbol=symbol,
        stock_name=stock_name,
        industry_name=industry_name,
        cutoff_date=cutoff_date,
    )
    lines = [
        "# 既有产业研究候选索引",
        "",
        "> 这些文件只用于发现可复用线索，不代表本季度结论。Industry Researcher 必须先确认 `subchain` 完全相同，并刷新需求、订单、供给、库存、价格、交期、CAPEX 和技术路线等可变事实。",
        "",
    ]
    if candidates:
        lines.extend(f"- `{candidate}`" for candidate in candidates)
    else:
        lines.append("未找到与公司代码、公司名称或现有行业标签直接匹配的产业研究卡片。")
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def build_financial_report_context(
    *,
    symbol: str,
    stock_name: str,
    industry_name: str,
    analysis_date: str,
    announcement_date: str,
    current_announcement_id: str,
    workdir: Path,
    stock_root: Path,
    summary_index_path: Path,
    announcement_datetime: str | None = None,
    generate_current_market: bool = True,
    historical_mode: bool = False,
) -> FinancialReportContextResult:
    """Generate all deterministic, non-conclusive context files for one stock."""

    pre_market_date = resolve_pre_announcement_market_date(
        announcement_date,
        announcement_datetime,
    )
    try:
        symbol_info = parse_symbol(symbol)
        pre_market_date = get_latest_trading_day(
            _parse_iso_date(pre_market_date),
            symbol_info.calendar,
        ).isoformat()
    except Exception as exc:
        LOGGER.warning(
            "交易日历解析失败，保留工作日回退结果: symbol=%s proposed=%s error=%s",
            symbol,
            pre_market_date,
            exc,
        )
    historical_research = find_historical_research_package(symbol, pre_market_date)
    hk_consensus, hk_consensus_path = _render_hk_pre_announcement_consensus(
        symbol,
        announcement_date,
        historical_mode=historical_mode,
    )
    price_context, price_context_path = _render_pre_announcement_price_context(
        symbol,
        pre_market_date,
    )
    pre_path = workdir / "pre_announcement_market_context.md"
    current_path = workdir / "current_market_context.md"
    prior_path = workdir / "prior_fundamental_memory.md"
    industry_path = workdir / "existing_industry_research.md"

    pre_path.write_text(
        _render_pre_context(
            symbol=symbol,
            stock_name=stock_name,
            announcement_date=announcement_date,
            announcement_datetime=announcement_datetime,
            requested_pre_date=pre_market_date,
            evidence_cutoff=announcement_datetime or announcement_date,
            historical_research=historical_research,
            hk_consensus=hk_consensus,
            hk_consensus_path=hk_consensus_path,
            price_context=price_context,
            price_context_path=price_context_path,
        ),
        encoding="utf-8",
    )

    market_payload: Optional[dict[str, Any]] = None
    market_error: Optional[str] = None
    if generate_current_market:
        try:
            from services.research.stock_analysis import analyze_stock_dynamics_and_valuation

            market_payload = analyze_stock_dynamics_and_valuation(symbol, analysis_date)
        except Exception as exc:
            market_error = str(exc)
            LOGGER.warning(
                "生成当前价格与估值上下文失败: symbol=%s date=%s error=%s",
                symbol,
                analysis_date,
                exc,
            )
    else:
        market_error = "调用方显式跳过当前价格与估值生成"

    current_path.write_text(
        _render_current_context(
            symbol=symbol,
            stock_name=stock_name,
            analysis_date=analysis_date,
            announcement_date=announcement_date,
            market_payload=market_payload,
            market_error=market_error,
            stock_root=stock_root,
            historical_mode=historical_mode,
        ),
        encoding="utf-8",
    )
    _write_prior_memory(
        prior_path,
        summary_index_path=summary_index_path,
        current_announcement_id=current_announcement_id,
        current_report_date=announcement_date,
    )
    _write_existing_industry_research(
        industry_path,
        symbol=symbol,
        stock_name=stock_name,
        industry_name=industry_name,
        cutoff_date=analysis_date if historical_mode else None,
    )
    return FinancialReportContextResult(
        analysis_date=analysis_date,
        announcement_date=announcement_date,
        announcement_datetime=announcement_datetime,
        pre_announcement_market_date=pre_market_date,
        historical_research_path=(
            historical_research.path if historical_research else None
        ),
        pre_context_path=pre_path,
        current_context_path=current_path,
        prior_memory_path=prior_path,
        existing_industry_research_path=industry_path,
    )


__all__ = [
    "FinancialReportContextResult",
    "HistoricalResearchPackage",
    "build_financial_report_context",
    "calculate_pro_forma_ttm",
    "detect_valuation_basis_dates",
    "extract_markdown_section",
    "extract_markdown_section_by_prefix",
    "extract_pre_announcement_research_sections",
    "find_historical_research_package",
    "resolve_pre_announcement_market_date",
]
