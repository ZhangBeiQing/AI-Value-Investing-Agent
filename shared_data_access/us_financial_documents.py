"""美股财报原文：SEC EDGAR → ``disclosures/md/*.md``。

A 股 / 港股的财报原文由巨潮（cninfo）提供，同步后落盘为 ``disclosures/pdfs`` +
``disclosures/md``。美股没有巨潮口径，本模块把 SEC EDGAR 的申报正文渲染成同构
Markdown 并写回同一目录，使 ``services/research/financial_report_skill`` 不需要区分市场。

来源分两档：

- ``10-K / 10-Q / 20-F / 40-F``：主文档即完整财报；另外用 ``FilingSummary.xml``
  里的 ``R*.htm`` 渲染三大报表（带上年同期列），口径最干净。
- ``6-K``：外国私人发行人（如台积电）的季度财报稿通常是新闻稿，只含摘要与指引；
  完整三表要等季末约 45 天后的「Financial Statements」6-K。缺失的资产负债表、
  现金流量表与费用明细由东财结构化财报补足，并在文档中显式标注为非公司披露原文。

抓取永远面向真实当前时间；时间守卫由调用方在读取阶段完成。
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from commons import SymbolInfo
from core.logging import get_logger
from shared_data_access.cache_registry import CacheKind, build_cache_dir
from shared_data_access.us_disclosures import (
    EARNINGS_RELEASE_FORMS,
    SEC_DEFAULT_USER_AGENT,
    SEC_TIMEOUT_SECONDS,
    STATEMENT_FORMS,
    _www_headers,
    filing_report_period,
    is_earnings_release_text,
    is_us_financial_filing,
)

LOGGER = get_logger("UsFinancialDocuments")

# XBRL 报表匹配规则；顺序决定优先级
STATEMENT_PATTERNS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("income", ("statements of income", "statements of operations", "income statement")),
    ("balance", ("balance sheet",)),
    ("cashflow", ("statements of cash flows", "cash flow statement")),
)

STATEMENT_TITLES = {
    "income": "利润表",
    "balance": "资产负债表",
    "cashflow": "现金流量表",
}


FINANCIAL_CACHE_FILES: Tuple[Tuple[str, str], ...] = (
    ("利润表", "profit_sheet.csv"),
    ("资产负债表", "balance_sheet.csv"),
    ("现金流量表", "cash_flow_sheet.csv"),
)

# 只输出对基本面判断最有用的列；东财长表列很多，全量输出会淹没正文
STATEMENT_COLUMN_LABELS: Dict[str, Dict[str, str]] = {
    "利润表": {
        "TOTAL_OPERATE_INCOME": "营业收入",
        "OPERATE_COST": "营业成本",
        "GROSS_PROFIT": "毛利",
        "RESEARCH_EXPENSE": "研发费用",
        "SALE_EXPENSE": "销售费用",
        "MANAGE_EXPENSE": "管理费用",
        "OPERATE_PROFIT": "营业利润",
        "TOTAL_PROFIT": "税前利润",
        "INCOME_TAX": "所得税",
        "NETPROFIT": "净利润",
        "MINORITY_INTEREST": "少数股东损益",
        "PARENT_NETPROFIT": "归母净利润",
        "BASIC_EPS": "每股收益",
    },
    "资产负债表": {
        "TOTAL_ASSETS": "总资产",
        "TOTAL_CURRENT_ASSETS": "流动资产",
        "MONETARYFUNDS": "货币资金",
        "ACCOUNTS_RECE": "应收账款",
        "INVENTORY": "存货",
        "FIXED_ASSET": "固定资产",
        "TOTAL_LIABILITIES": "总负债",
        "TOTAL_CURRENT_LIAB": "流动负债",
        "LONG_LOAN": "长期借款",
        "TOTAL_PARENT_EQUITY": "归母权益",
        "TOTAL_EQUITY": "股东权益",
    },
    "现金流量表": {
        "NETCASH_OPERATE": "经营活动净现金流",
        "CONSTRUCT_LONG_ASSET": "购建长期资产",
        "NETCASH_INVEST": "投资活动净现金流",
        "NETCASH_FINANCE": "筹资活动净现金流",
        "ASSIGN_DIVIDEND_PORFIT": "分配股利",
        "REPURCHASE": "回购",
        "CCE_ADD": "现金净增加额",
        "END_CASH": "期末现金",
    },
}

# 单份原文的软上限；超过后截断并显式说明，避免把整份 10-Q / 20-F 注记灌进上下文。
# 12 万字符会在「结构化三大报表」这一段就被耗尽，导致 MD&A、附注、分部表、合同负债
# 与客户集中度全部落在截断线之外（2026-11-08 实测：5 只美股的全部 10-Q/10-K 都停在
# 120412~120419 字节，附件注记一律缺失）。放宽到 40 万字符后，10-Q 通常可完整保留；
# 仍超长时由 STATEMENT_MAX_CHARS 单独约束报表段，优先把预算留给正文。
DEFAULT_MAX_CHARS = 400_000

# 结构化报表段的独立预算：XBRL 的 Definition 脚注极长且信息密度低，先剥掉定义块，
# 再给报表段一个上限，保证正文（MD&A、附注）不会被报表挤出截断线。
STATEMENT_MAX_CHARS = 80_000

# 兜底表格只保留最近若干期，防止十年前的空列混进来
FALLBACK_ROWS = 8


def _logger(logger: Optional[logging.Logger]) -> logging.Logger:
    return logger or LOGGER


def sec_headers() -> Dict[str, str]:
    """SEC 要求带可识别 User-Agent；与 ``us_disclosures`` 共用同一份配置。"""

    headers = _www_headers()
    headers.setdefault("User-Agent", SEC_DEFAULT_USER_AGENT)
    return headers

# ---------------------------------------------------------------------------
# HTML → Markdown
# ---------------------------------------------------------------------------


def _clean_cell(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()
    return cleaned.replace("|", "/")


def _render_table(table: Any) -> str:
    rows: List[List[str]] = []
    for tr in table.find_all("tr"):
        cells = [_clean_cell(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        if any(cells):
            rows.append(cells)
    if len(rows) < 2:
        return ""

    width = max(len(row) for row in rows)
    rows = [row + [""] * (width - len(row)) for row in rows]
    header, body = rows[0], rows[1:]
    if not any(header):
        header, body = body[0], body[1:]

    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in range(width)) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in body)
    return "\n".join(lines)


def _make_soup(html_text: str) -> BeautifulSoup:
    """SEC 的 XBRL R 文件是 XHTML，用 XML 解析器更稳；普通申报用 HTML 解析器。"""

    head = (html_text or "").lstrip()[:64].lower()
    parser = "lxml-xml" if head.startswith("<?xml") else "lxml"
    return BeautifulSoup(html_text or "", parser)


def html_to_markdown(html_text: str) -> str:
    """把申报 HTML 转成 Markdown，表格保留为管道表。"""

    soup = _make_soup(html_text)
    for tag in soup(["script", "style"]):
        tag.decompose()

    blocks: List[str] = []
    for element in soup.find_all(["table", "p", "div", "h1", "h2", "h3", "h4"]):
        if element.find_parent("table") is not None:
            continue
        if element.name == "table":
            rendered = _render_table(element)
            if rendered:
                blocks.append(rendered)
            continue
        # 只取叶子块：含嵌套 div/p/table 的容器会把子内容的文本重复输出一遍
        if element.find(["table", "p", "div"]) is not None:
            continue
        text = _clean_cell(element.get_text(" ", strip=True))
        if len(text) < 2:
            continue
        if element.name in {"h1", "h2", "h3", "h4"}:
            blocks.append(f"{'#' * int(element.name[1])} {text}")
        else:
            blocks.append(text)

    deduped: List[str] = []
    for block in blocks:
        if deduped and block == deduped[-1]:
            continue
        deduped.append(block)
    return "\n\n".join(deduped).strip()


# ---------------------------------------------------------------------------
# SEC 抓取
# ---------------------------------------------------------------------------


def _archive_url(cik: str, accession: str) -> str:
    cik_int = str(int(cik))
    accession_compact = str(accession).replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_compact}"


def fetch_filing_index(
    cik: str,
    accession: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """取申报目录（附件清单），失败时返回空字典让上层降级。"""

    if not cik or not accession:
        return {}
    url = f"{_archive_url(cik, accession)}/index.json"
    try:
        response = requests.get(url, headers=sec_headers(), timeout=SEC_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json() or {}
    except Exception as exc:  # noqa: BLE001 - 网络异常统一降级为缺数据
        _logger(logger).warning("SEC 申报目录获取失败 %s: %s", accession, exc)
        return {}


def fetch_filing_text(
    cik: str,
    accession: str,
    document: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> str:
    """取单份申报文档的 HTML 原文。"""

    if not cik or not accession or not document:
        return ""
    url = f"{_archive_url(cik, accession)}/{document}"
    try:
        response = requests.get(url, headers=sec_headers(), timeout=SEC_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.text or ""
    except Exception as exc:  # noqa: BLE001
        _logger(logger).warning("SEC 文档获取失败 %s/%s: %s", accession, document, exc)
        return ""


def fetch_filing_summary(
    cik: str,
    accession: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> str:
    """取 FilingSummary.xml；没有 XBRL 的 6-K 会返回空串。"""

    if not cik or not accession:
        return ""
    url = f"{_archive_url(cik, accession)}/FilingSummary.xml"
    try:
        response = requests.get(url, headers=sec_headers(), timeout=SEC_TIMEOUT_SECONDS)
        if response.status_code != 200:
            return ""
        return response.text or ""
    except Exception as exc:  # noqa: BLE001
        _logger(logger).debug("FilingSummary 获取失败 %s: %s", accession, exc)
        return ""


def _filing_documents(index_payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    directory = (index_payload or {}).get("directory") or {}
    items = directory.get("item") or []
    return [item for item in items if isinstance(item, dict)]


def pick_earnings_exhibit(
    index_payload: Mapping[str, Any],
    primary_document: str,
) -> Optional[str]:
    """在 6-K / 8-K 里挑最像业绩稿的附件。"""

    candidates: List[Tuple[int, int, str]] = []
    for item in _filing_documents(index_payload):
        name = str(item.get("name") or "")
        lower = name.lower()
        if not lower.endswith((".htm", ".html")):
            continue
        if lower == (primary_document or "").lower() or "index" in lower:
            continue
        try:
            size = int(item.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        keyword_hits = sum(
            1
            for token in ("earn", "press", "release", "result", "ex99", "ex-99", "q1", "q2", "q3", "q4")
            if token in lower
        )
        candidates.append((keyword_hits, size, name))
    if not candidates:
        return None
    candidates.sort(key=lambda entry: (entry[0], entry[1]), reverse=True)
    return candidates[0][2]


def parse_statement_reports(summary_xml: str) -> Dict[str, str]:
    """解析 FilingSummary.xml，按类别选出利润表/资产负债表/现金流量表。"""

    if not summary_xml:
        return {}
    chosen: Dict[str, str] = {}
    for block in re.findall(r"<Report[^>]*>(.*?)</Report>", summary_xml, re.S):
        file_match = re.search(r"<HtmlFileName>(.*?)</HtmlFileName>", block, re.S)
        name_match = re.search(r"<ShortName>(.*?)</ShortName>", block, re.S)
        if not file_match or not name_match:
            continue
        file_name = file_match.group(1).strip()
        short_name = re.sub(r"\s+", " ", name_match.group(1)).strip().lower()
        for key, patterns in STATEMENT_PATTERNS:
            if key in chosen:
                continue
            if any(pattern in short_name for pattern in patterns):
                chosen[key] = file_name
    return chosen


def _strip_xbrl_definition_block(statement_markdown: str) -> str:
    """剥掉 XBRL R 文件尾部的 ``Definition`` 脚注块。

    SEC 的 ``R*.htm`` 在表格后附了逐行科目定义（`| - Definition ...`、`Name:`、
    `Namespace:` 等），单份就能占数万字符，但不提供任何财务事实。把它们整块删掉，
    可以把预算让给正文的 MD&A 与附注。
    """

    if not statement_markdown:
        return statement_markdown
    lines = statement_markdown.splitlines()
    kept: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("| - Definition") or stripped.startswith("- Definition"):
            break
        if stripped.startswith("| Name:") or stripped.startswith("Name:"):
            break
        if stripped.startswith("| Namespace:") or stripped.startswith("Namespace:"):
            break
        kept.append(line)
    return "\n".join(kept).rstrip()


def render_statement_tables(
    cik: str,
    accession: str,
    statements: Mapping[str, str],
    *,
    logger: Optional[logging.Logger] = None,
) -> str:
    """把三大报表的 XBRL R 文件渲染成 Markdown 小节。"""

    blocks: List[str] = []
    for key in ("income", "balance", "cashflow"):
        file_name = statements.get(key)
        if not file_name:
            continue
        html_text = fetch_filing_text(cik, accession, file_name, logger=logger)
        if not html_text:
            continue
        # 必须逐表清理；合并后再截去 Definition 尾部会连后续两张报表一起删除。
        markdown = _strip_xbrl_definition_block(html_to_markdown(html_text))
        if not markdown:
            continue
        blocks.append(f"### {STATEMENT_TITLES[key]}\n\n{markdown}")
    return "\n\n".join(blocks)


# ---------------------------------------------------------------------------
# 东财结构化财报兜底
# ---------------------------------------------------------------------------


def _financials_cache_dir(symbol_info: SymbolInfo, base_dir: str | Path | None) -> Path:
    return build_cache_dir(symbol_info, CacheKind.FINANCIALS, base_dir=base_dir, ensure=False)


def _financials_meta(symbol_info: SymbolInfo, base_dir: str | Path | None) -> Dict[str, Any]:
    meta_path = _financials_cache_dir(symbol_info, base_dir) / "us_financial_meta.json"
    if not meta_path.is_file():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - 元信息损坏不应阻断财报原文生成
        return {}


def _format_statement_frame(frame: pd.DataFrame, labels: Mapping[str, str]) -> pd.DataFrame:
    """把结构化缓存整理成可读表格：只保留关键列、最近若干期、去掉科学计数法。"""

    value_columns = [
        column
        for column in labels
        if column in frame.columns and pd.api.types.is_numeric_dtype(frame[column])
    ]
    if not value_columns or "REPORT_DATE" not in frame.columns:
        return pd.DataFrame()

    work = frame[["REPORT_DATE"] + value_columns].copy()
    work = work.sort_values("REPORT_DATE", ascending=False).head(FALLBACK_ROWS)
    for column in value_columns:
        work[column] = work[column].map(
            lambda value: "" if pd.isna(value) else f"{float(value):,.0f}"
        )
    work = work.rename(columns={"REPORT_DATE": "报告期", **{c: labels[c] for c in value_columns}})
    return work.reset_index(drop=True)


def render_structured_financials_fallback(
    symbol_info: SymbolInfo,
    *,
    base_dir: str | Path | None = None,
) -> str:
    """把东财结构化报表渲染成补充小节，补足 6-K 缺失的资产负债表与现金流量表。

    只补这两张表：利润摘要在 6-K 正文里已经有了，再灌一遍只会增加噪声。
    """

    cache_dir = _financials_cache_dir(symbol_info, base_dir)
    if not cache_dir.is_dir():
        return ""

    meta = _financials_meta(symbol_info, base_dir)
    currency = str(meta.get("currency") or "")
    fx = meta.get("fx_local_per_usd")
    notes = ["以下数字来自第三方结构化财报接口，**不是公司披露原文**"]
    if currency and currency != "USD" and fx:
        notes.append(f"原始币种 {currency}，已按 1 USD = {float(fx):.4f} {currency} 折算为美元")
    notes.append("与公司披露原文冲突时以原文为准")

    blocks: List[str] = [
        "## 补充：结构化财务报表（第三方数据）",
        "",
        "> " + "；".join(notes) + "。",
        "> 6-K 财报稿通常只有利润摘要，不含完整资产负债表与现金流量表，因此在此补足。",
        "> 金额单位：美元；资产负债表为期末时点，现金流量表为年内累计。",
    ]

    rendered = 0
    for title, file_name in FINANCIAL_CACHE_FILES:
        path = cache_dir / file_name
        if not path.is_file():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:  # noqa: BLE001
            continue
        if frame.empty:
            continue

        subset = _format_statement_frame(frame, STATEMENT_COLUMN_LABELS.get(title, {}))
        if subset.empty:
            continue
        blocks.append(f"### {title}\n")
        # tabulate 默认会把 "1,234.00" 这类字符串重新解析成浮点再格式化，必须关闭
        blocks.append(subset.to_markdown(index=False, disable_numparse=True))
        rendered += 1

    if not rendered:
        return ""
    return "\n\n".join(blocks)


# ---------------------------------------------------------------------------
# 文档组装
# ---------------------------------------------------------------------------


def _metadata_block(
    *,
    form: str,
    filing_date: str,
    report_period: str,
    accession: str,
    document: str,
) -> str:
    lines = [
        f"- 申报表单：{form}",
        f"- 申报日期：{filing_date}",
    ]
    if report_period:
        lines.append(f"- 报告期：{report_period}")
    lines.extend(
        [
            f"- SEC 受理号：{accession}",
            f"- 原始文档：{document}",
            "- 本文由 SEC EDGAR 申报正文自动转换，未经人工改写。",
        ]
    )
    return "\n".join(lines)


def build_us_financial_document_markdown(
    *,
    symbol_info: SymbolInfo,
    cik: str,
    form: str,
    filing_date: str,
    report_period: str,
    accession: str,
    primary_document: str,
    base_dir: str | Path | None = None,
    max_chars: int = DEFAULT_MAX_CHARS,
    logger: Optional[logging.Logger] = None,
) -> str:
    """抓取并组装一份美股财报原文 Markdown。"""

    resolved_logger = _logger(logger)
    normalized = (form or "").strip().upper()
    source_document = primary_document
    body = ""

    if normalized in STATEMENT_FORMS:
        # 三大报表放最前面：10-Q/10-K 主文档动辄十几万字符，先拼正文会把
        # 最有价值的结构化报表挤出截断线之外。
        statements = parse_statement_reports(
            fetch_filing_summary(cik, accession, logger=resolved_logger)
        )
        statement_md = render_statement_tables(
            cik, accession, statements, logger=resolved_logger
        )
        primary_html = fetch_filing_text(
            cik, accession, primary_document, logger=resolved_logger
        )
        if not primary_html.strip():
            return ""
        # XBRL 的 Definition 脚注极长且几乎无信息量，先整块剥掉，否则报表段会
        # 把正文（MD&A、附注、分部表）全部挤出预算。
        if len(statement_md) > STATEMENT_MAX_CHARS:
            statement_md = (
                statement_md[:STATEMENT_MAX_CHARS]
                + "\n\n> 结构化报表段超长，已截断；正文附注另行保留。"
            )
        body_parts: List[str] = []
        if statement_md:
            body_parts.append("## 结构化三大报表（XBRL）\n\n" + statement_md)
        body_parts.append(html_to_markdown(primary_html))
        body = "\n\n".join(part for part in body_parts if part).strip()
    else:
        index_payload = fetch_filing_index(cik, accession, logger=resolved_logger)
        exhibit = pick_earnings_exhibit(index_payload, primary_document) or primary_document
        if exhibit:
            source_document = exhibit
        exhibit_html = fetch_filing_text(
            cik, accession, exhibit, logger=resolved_logger
        )
        if not exhibit_html.strip():
            return ""
        body_parts = [html_to_markdown(exhibit_html)]
        fallback = render_structured_financials_fallback(symbol_info, base_dir=base_dir)
        if fallback:
            body_parts.append(fallback)
        body = "\n\n".join(part for part in body_parts if part).strip()

    if not body:
        return ""
    if len(body) > max_chars:
        body = body[:max_chars] + (
            f"\n\n> 原文超长，已截断至 {max_chars} 字符；"
            f"完整内容见 SEC 受理号 {accession}。"
        )

    stock_name = symbol_info.stock_name or symbol_info.symbol
    header = f"# {stock_name} {report_period or ''} 财报原文（SEC {form}）".replace("  ", " ")
    metadata = _metadata_block(
        form=form,
        filing_date=filing_date,
        report_period=report_period,
        accession=accession,
        document=source_document,
    )
    return f"{header}\n\n{metadata}\n\n---\n\n{body}\n"


def markdown_file_name(
    *,
    filing_date: str,
    stock_code: str,
    accession: str,
    slug: str,
) -> str:
    """与巨潮口径保持同一命名：``{date}__{code}__{id}__{slug}.md``。"""

    return f"{filing_date}__{stock_code}__{accession}__{slug}.md"


__all__ = [
    "DEFAULT_MAX_CHARS",
    "EARNINGS_RELEASE_FORMS",
    "STATEMENT_FORMS",
    "build_us_financial_document_markdown",
    "filing_report_period",
    "html_to_markdown",
    "is_us_financial_filing",
    "markdown_file_name",
    "parse_statement_reports",
    "pick_earnings_exhibit",
    "render_structured_financials_fallback",
]
