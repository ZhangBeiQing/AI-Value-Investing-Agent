"""HKEXnews official PDF fallback for Hong Kong financial disclosures."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core.logging import get_logger
from utlity.stock_utils import SymbolInfo


LOGGER = get_logger("HkexFinancialReport")
HKEX_BASE_URL = "https://www1.hkexnews.hk"
MIN_PDF_BYTES = 1024


def _request(url: str, *, params: dict[str, str] | None = None, stream: bool = False) -> requests.Response:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
        "Referer": f"{HKEX_BASE_URL}/search/titlesearch.xhtml",
    }
    errors: list[str] = []
    for trust_env in (True, False):
        try:
            session = requests.Session()
            session.trust_env = trust_env
            response = session.get(url, params=params, headers=headers, timeout=(5, 30), stream=stream)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            errors.append(f"{'环境代理' if trust_env else '直连'}: {exc}")
    raise RuntimeError("港交所披露易请求失败；" + "；".join(errors))


def _resolve_stock_id(symbol_info: SymbolInfo) -> str:
    response = _request(
        f"{HKEX_BASE_URL}/search/prefix.do",
        params={
            "callback": "callback",
            "lang": "ZH",
            "type": "A",
            "name": symbol_info.code,
            "market": "SEHK",
        },
    )
    match = re.fullmatch(r"\s*callback\((.*)\);?\s*", response.text, flags=re.DOTALL)
    if not match:
        raise ValueError("港交所股票代码查询返回格式异常")
    payload = json.loads(match.group(1))
    expected_code = symbol_info.code.zfill(5)
    for item in payload.get("stockInfo") or []:
        if str(item.get("code") or "").zfill(5) == expected_code:
            return str(item["stockId"])
    raise ValueError(f"港交所未找到股票代码 {symbol_info.code}")


def _normalize_title(value: str) -> str:
    return re.sub(r"\s+", "", value).replace("（", "(").replace("）", ")")


def _find_pdf_url(symbol_info: SymbolInfo, *, title: str, announcement_date: str) -> str:
    stock_id = _resolve_stock_id(symbol_info)
    response = _request(
        f"{HKEX_BASE_URL}/search/titlesearch.xhtml",
        params={"category": "0", "market": "SEHK", "stockId": stock_id, "lang": "ZH"},
    )
    soup = BeautifulSoup(response.text, "html.parser")
    date_path = f"/{announcement_date[:4]}/{announcement_date[5:7]}{announcement_date[8:10]}/"
    expected_title = _normalize_title(title)
    same_day: list[tuple[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        if date_path not in href or not href.lower().endswith(".pdf"):
            continue
        candidate_title = " ".join(anchor.get_text(" ", strip=True).split())
        same_day.append((candidate_title, urljoin(HKEX_BASE_URL, href)))
        if _normalize_title(candidate_title) == expected_title:
            return urljoin(HKEX_BASE_URL, href)
    report_markers = ("中期業績", "中期報告", "年度業績", "年度報告", "季度業績")
    financial_matches = [url for candidate, url in same_day if any(marker in candidate for marker in report_markers)]
    if len(financial_matches) == 1:
        return financial_matches[0]
    raise ValueError(f"港交所未唯一匹配财报公告：{symbol_info.symbol} {announcement_date} {title}")


def _is_valid_pdf(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size < MIN_PDF_BYTES:
        return False
    with path.open("rb") as handle:
        if not handle.read(5).startswith(b"%PDF"):
            return False
        handle.seek(max(0, path.stat().st_size - 1024))
        return b"%%EOF" in handle.read()


def fetch_hkex_financial_report_pdf(
    symbol_info: SymbolInfo,
    *,
    title: str,
    announcement_date: str,
    output_path: Path,
) -> tuple[Path, str] | None:
    """Locate and atomically cache the matching official HKEXnews PDF."""
    if not symbol_info.symbol.endswith(".HK"):
        return None
    pdf_url = _find_pdf_url(symbol_info, title=title, announcement_date=announcement_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        response = _request(pdf_url, stream=True)
        with tempfile.NamedTemporaryFile("wb", dir=output_path.parent, prefix=".hkex-", delete=False) as handle:
            temp_path = Path(handle.name)
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if chunk:
                    handle.write(chunk)
        if not _is_valid_pdf(temp_path):
            raise ValueError(f"港交所财报 PDF 无效或不完整：{pdf_url}")
        os.replace(temp_path, output_path)
        temp_path = None
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()
    LOGGER.info("港交所财报 PDF 已缓存: %s -> %s", pdf_url, output_path)
    return output_path, pdf_url


__all__ = ["fetch_hkex_financial_report_pdf"]
