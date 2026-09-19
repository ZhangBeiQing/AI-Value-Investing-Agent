"""证券之星财报全文备用读取。

仅在官方公告 PDF 无法取得时使用。正文写入既有 disclosures/md 缓存，
调用方负责在公告索引中记录来源，避免把网页正文误标为官方 PDF 转换结果。
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core.logging import get_logger
from utlity.stock_utils import SymbolInfo


LOGGER = get_logger("StockstarFinancialReport")
STOCKSTAR_BASE_URL = "https://stock.stockstar.com"
MIN_REPORT_TEXT_LENGTH = 20_000


def _normalized_title(value: str) -> str:
    value = re.sub(r"^[^:：]{1,24}[:：]\s*", "", value)
    return re.sub(r"\s+", "", value).replace("（", "(").replace("）", ")")


def _request_html(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    errors: list[str] = []
    for trust_env in (True, False):
        try:
            with requests.Session() as session:
                session.trust_env = trust_env
                response = session.get(url, headers=headers, timeout=(5, 30))
                response.raise_for_status()
                response.encoding = response.apparent_encoding or response.encoding
                return response.text
        except requests.RequestException as exc:
            errors.append(f"{'环境代理' if trust_env else '直连'}: {exc}")
    raise RuntimeError("证券之星页面读取失败；" + "；".join(errors))


def _find_notice_url(symbol_info: SymbolInfo, expected_title: str) -> str | None:
    listing_url = f"https://stock.quote.stockstar.com/info_{symbol_info.code}.shtml"
    soup = BeautifulSoup(_request_html(listing_url), "html.parser")
    expected = _normalized_title(expected_title)
    matches: list[str] = []
    for anchor in soup.find_all("a", href=True):
        title = _normalized_title(anchor.get_text(" ", strip=True))
        href = str(anchor.get("href") or "")
        if title == expected and "/notice/SN" in href:
            matches.append(urljoin(STOCKSTAR_BASE_URL, href))
    return matches[0] if matches else None


def _validate_report_text(text: str, symbol_info: SymbolInfo, expected_title: str) -> None:
    compact = re.sub(r"\s+", "", text)
    expected = _normalized_title(expected_title)
    if len(compact) < MIN_REPORT_TEXT_LENGTH:
        raise ValueError(f"证券之星财报正文过短：{len(compact)} 字符")
    if expected not in compact[:5000]:
        raise ValueError("证券之星页面标题与目标财报不一致")
    if symbol_info.code not in compact[:10_000]:
        raise ValueError("证券之星财报正文未包含目标股票代码")
    for marker in ("管理层讨论与分析", "财务报告"):
        if marker not in compact:
            raise ValueError(f"证券之星财报正文缺少完整性标记：{marker}")
    profile_markers = ("公司简介和主要财务指标", "主要会计数据和财务指标", "公司基本情况")
    if not any(marker in compact for marker in profile_markers):
        raise ValueError("证券之星财报正文缺少公司概况或主要财务指标章节")


def fetch_stockstar_financial_report_markdown(
    symbol_info: SymbolInfo,
    *,
    title: str,
    output_path: Path,
) -> tuple[Path, str] | None:
    """查找并缓存完整定期报告正文，返回路径和来源页面。"""
    normalized = _normalized_title(title)
    if "报告" not in normalized or "报告摘要" in normalized or "业绩说明会" in normalized:
        return None
    notice_url = _find_notice_url(symbol_info, title)
    if not notice_url:
        LOGGER.warning("证券之星未找到匹配财报: %s %s", symbol_info.symbol, title)
        return None
    soup = BeautifulSoup(_request_html(notice_url), "html.parser")
    article = soup.select_one(".article_content") or soup.select_one(".article")
    if article is None:
        raise ValueError("证券之星财报页面缺少正文节点")
    text = article.get_text("\n", strip=True).replace("\u2002", " ").replace("\u3000", " ")
    _validate_report_text(text, symbol_info, title)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = f"# {title}\n\n{text}\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=output_path.parent, prefix=".stockstar-", delete=False
    ) as handle:
        temp_path = Path(handle.name)
        handle.write(content)
    temp_path.replace(output_path)
    LOGGER.info("证券之星财报正文已缓存: %s -> %s", notice_url, output_path)
    return output_path, notice_url


__all__ = ["fetch_stockstar_financial_report_markdown"]
