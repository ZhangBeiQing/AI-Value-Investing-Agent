"""US disclosures backed by SEC EDGAR.

A 股/港股公告走巨潮（cninfo），美股没有对应口径。本模块用 SEC EDGAR 的
官方接口替代：

* ``https://www.sec.gov/files/company_tickers.json``：TICKER → CIK；
* ``https://data.sec.gov/submissions/CIK##########.json``：申报列表。

输出两套结构：

1. ``cninfo_list.csv`` / ``index.json``：与既有公告缓存同构，供
   ``SharedDataAccess._load_disclosure_bundle`` 与缓存契约直接复用；
2. ``build_us_filing_news_items``：把申报转成 ``04`` 研究包第 2 章可直接
   消费的新闻条目（不下载 PDF、不调用 LLM，离线可复现）。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

from commons import SymbolInfo, resolve_base_dir
from shared_data_access.cache_registry import (
    CacheKind,
    build_cache_dir,
    record_cache_refresh,
    should_refresh,
)

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVES_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"
)
SEC_CACHE_TTL_SECONDS = 24 * 3600
SEC_TIMEOUT_SECONDS = 30.0
SEC_DEFAULT_USER_AGENT = (
    "AI-Value-Investing-Agent/1.0 (local equity research; contact: admin@example.com)"
)

HIGH_IMPACT_FORMS = {"10-K", "10-Q", "8-K", "20-F", "40-F"}
MEDIUM_IMPACT_FORMS = {"6-K", "DEF 14A", "SC 13D", "SC 13D/A", "S-1", "F-1", "424B3", "424B4"}
SKIP_FORMS = {"3", "3/A", "4", "4/A", "5", "5/A"}

# ---------------------------------------------------------------------------
# 财报识别
#
# 美股没有巨潮口径的"财报公告"，财报原文分布在两类表单里：
# - 10-K / 10-Q / 20-F / 40-F：主文档即完整财报；
# - 6-K / 8-K：外国私人发行人（如台积电）的季度业绩稿走 6-K，但同一表单
#   也承载月营收、股利调整、持股变动，必须看标题/正文要点才能区分。
# ---------------------------------------------------------------------------

STATEMENT_FORMS = {"10-K", "10-Q", "20-F", "40-F"}

EARNINGS_RELEASE_FORMS = {"6-K", "8-K"}

EARNINGS_RELEASE_HINTS = (
    "reports",
    "announces",
    "results",
    "financial statements",
    "annual report",
    "quarterly report",
)

# 月营收、股利调整、董事持股等必须排除，否则会被误当成财报
EARNINGS_RELEASE_EXCLUDE = (
    "revenue report",
    "monthly revenue",
    "shareholding",
    "shareholdings",
    "dividend",
    "changes in",
    "board of directors",
    "minutes",
    "conference call",
)

REPORT_PERIOD_RE = re.compile(r"报告期\s*(\d{4}-\d{2}-\d{2})")


def is_earnings_release_text(text: str) -> bool:
    """标题/要点是否像一份季度业绩稿（而不是月营收、股利或持股变动）。"""

    lowered = (text or "").lower()
    if not lowered.strip():
        return False
    if any(token in lowered for token in EARNINGS_RELEASE_EXCLUDE):
        return False
    return any(token in lowered for token in EARNINGS_RELEASE_HINTS)


def is_us_financial_filing(
    form: str,
    title: str = "",
    description: str = "",
) -> bool:
    """判断一条 SEC 申报是否应作为"财报"进入财务研究链路。

    ``6-K`` / ``8-K`` 只有在标题已含业绩稿特征时才判为财报；索引里的 6-K 标题
    通常只有表单名，这类申报由 ``sync_financial_reports_for_stock`` 在读取正文
    要点后回填 ``is_financial_report``。
    """

    normalized = (form or "").strip().upper()
    if normalized in STATEMENT_FORMS:
        return True
    if normalized not in EARNINGS_RELEASE_FORMS:
        return False
    return is_earnings_release_text(f"{title or ''} {description or ''}")


def filing_report_period(title: str, report_date: str = "") -> Optional[str]:
    """取申报对应的会计期间末。

    美股披露标题形如 ``临时公告（6-K）（报告期 2026-09-30）``，报告期即会计期间末；
    取不到时回退到 SEC 的 ``reportDate``。
    """

    match = REPORT_PERIOD_RE.search(title or "")
    if match:
        return match.group(1)
    text = (report_date or "").strip()
    return text or None


FORM_LABELS = {
    "10-K": "年度报告（10-K）",
    "10-Q": "季度报告（10-Q）",
    "8-K": "重大事项公告（8-K）",
    "6-K": "临时公告（6-K）",
    "20-F": "年度报告（20-F）",
    "40-F": "年度报告（40-F）",
    "DEF 14A": "股东大会委托书（DEF 14A）",
    "SC 13D": "股权变动披露（SC 13D）",
    "SC 13D/A": "股权变动披露（SC 13D/A）",
    "SC 13G": "股权变动披露（SC 13G）",
    "SC 13G/A": "股权变动披露（SC 13G/A）",
    "S-1": "注册声明（S-1）",
    "F-1": "注册声明（F-1）",
    "424B3": "发行补充文件（424B3）",
    "424B4": "发行补充文件（424B4）",
}


def _resolve_logger(logger: logging.Logger | None) -> logging.Logger:
    return logger if logger is not None else logging.getLogger(__name__)


def _www_headers() -> Dict[str, str]:
    user_agent = os.getenv("SEC_USER_AGENT", "").strip() or SEC_DEFAULT_USER_AGENT
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }


def _sec_headers() -> Dict[str, str]:
    user_agent = os.getenv("SEC_USER_AGENT", "").strip() or SEC_DEFAULT_USER_AGENT
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }


def sec_cache_dir(base_dir: str | Path | None = None) -> Path:
    target = resolve_base_dir(base_dir) / "global_cache" / "sec"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _read_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _is_fresh(path: Path, ttl_seconds: int = SEC_CACHE_TTL_SECONDS) -> bool:
    if not path.exists():
        return False
    age = datetime.now().timestamp() - path.stat().st_mtime
    return age < ttl_seconds


def load_cik_map(
    *,
    base_dir: str | Path | None = None,
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> Dict[str, str]:
    """Return ``{TICKER: CIK10}`` from SEC's company_tickers.json (cached)."""

    resolved_logger = _resolve_logger(logger)
    path = sec_cache_dir(base_dir) / "company_tickers.json"
    payload = None
    if not force_refresh and _is_fresh(path, SEC_CACHE_TTL_SECONDS * 7):
        payload = _read_json(path)
    if payload is None:
        try:
            response = requests.get(
                SEC_TICKERS_URL,
                headers={**_sec_headers(), "Host": "www.sec.gov"},
                timeout=SEC_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            path.write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as exc:
            resolved_logger.warning("获取 SEC ticker→CIK 映射失败: %s", exc)
            return {}

    mapping: Dict[str, str] = {}
    if isinstance(payload, dict):
        for entry in payload.values():
            if not isinstance(entry, dict):
                continue
            ticker = str(entry.get("ticker") or "").strip().upper()
            cik = entry.get("cik_str")
            if ticker and cik is not None:
                mapping[ticker] = f"{int(cik):010d}"
    return mapping


def _cik_cache_path(cik: str, base_dir: str | Path | None) -> Path:
    return sec_cache_dir(base_dir) / f"CIK{cik}.json"


def load_sec_submissions(
    ticker: str,
    *,
    base_dir: str | Path | None = None,
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    resolved_logger = _resolve_logger(logger)
    cik_map = load_cik_map(base_dir=base_dir, logger=resolved_logger)
    cik = cik_map.get(ticker.upper())
    if not cik:
        resolved_logger.warning("SEC 未收录该 ticker: %s", ticker)
        return {}

    path = _cik_cache_path(cik, base_dir)
    payload = None
    if not force_refresh and _is_fresh(path):
        payload = _read_json(path)
    if payload is None:
        try:
            response = requests.get(
                SEC_SUBMISSIONS_URL.format(cik=cik),
                headers=_sec_headers(),
                timeout=SEC_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            resolved_logger.warning("获取 SEC submissions 失败 %s(%s): %s", ticker, cik, exc)
            return {}
    if isinstance(payload, dict):
        payload["_cik"] = cik
    return payload if isinstance(payload, dict) else {}


def _iter_recent_filings(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    filings = payload.get("filings") or {}
    recent = filings.get("recent") or {}
    length = len(recent.get("form") or [])
    for index in range(length):
        row = {key: value[index] for key, value in recent.items() if isinstance(value, list)}
        yield row


def _filing_url(cik: str, accession: str, document: str) -> str:
    accession_compact = str(accession or "").replace("-", "")
    return SEC_ARCHIVES_URL.format(
        cik=str(int(cik)),
        accession=accession_compact,
        document=document,
    )


def normalize_filings(
    payload: Dict[str, Any],
    *,
    lookback_days: int = 900,
    today: Optional[date] = None,
) -> pd.DataFrame:
    """Convert SEC submissions payload into a canonical filing frame."""

    if not payload:
        return pd.DataFrame()
    cik = str(payload.get("_cik") or payload.get("cik") or "").zfill(10)
    ticker = ""
    tickers = payload.get("tickers") or []
    if tickers:
        ticker = str(tickers[0]).upper()
    entity_name = str(payload.get("name") or ticker)

    cutoff = (today or date.today()) - timedelta(days=max(int(lookback_days), 1))
    rows: List[Dict[str, Any]] = []
    for item in _iter_recent_filings(payload):
        form = str(item.get("form") or "").strip()
        if not form or form in SKIP_FORMS:
            continue
        if form not in FORM_LABELS and form not in HIGH_IMPACT_FORMS | MEDIUM_IMPACT_FORMS:
            continue
        filing_date_text = str(item.get("filingDate") or "").strip()
        if not filing_date_text:
            continue
        try:
            filing_date = date.fromisoformat(filing_date_text)
        except ValueError:
            continue
        if filing_date < cutoff:
            continue
        report_date_text = str(item.get("reportDate") or "").strip()
        accession = str(item.get("accessionNumber") or "").strip()
        document = str(item.get("primaryDocument") or "").strip()
        url = _filing_url(cik, accession, document) if accession and document else ""
        rows.append(
            {
                "代码": ticker,
                "名称": entity_name,
                "公告标题": _build_title(form, report_date_text, item),
                "公告时间": filing_date_text,
                "公告类型": form,
                "公告链接": url,
                "报告期": report_date_text,
                "accession": accession,
                "items": str(item.get("items") or ""),
                "primary_document": document,
                "description": str(item.get("primaryDocDescription") or ""),
                "impact_level": _impact_level(form),
                "cik": cik,
            }
        )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame["公告时间"] = pd.to_datetime(frame["公告时间"], errors="coerce")
    frame = frame.dropna(subset=["公告时间"]).sort_values("公告时间", ascending=False)
    return frame.reset_index(drop=True)


DOC_TEXT_CACHE = "sec_doc_text.json"
DOC_ENRICH_FORMS = {"6-K", "8-K", "10-Q", "10-K", "20-F", "40-F"}
DOC_BOILERPLATE = {
    "6-K",
    "8-K",
    "10-Q",
    "10-K",
    "20-F",
    "40-F",
    "EX-99.1",
    "EX-99.2",
    "DOCUMENT",
    "UNITED STATES",
    "SECURITIES AND EXCHANGE COMMISSION",
    "WASHINGTON, D.C. 20549",
}


def _load_doc_cache(cache_dir: Path) -> Dict[str, Dict[str, str]]:
    payload = _read_json(cache_dir / DOC_TEXT_CACHE)
    return payload if isinstance(payload, dict) else {}


def _save_doc_cache(cache_dir: Path, payload: Dict[str, Dict[str, str]]) -> None:
    (cache_dir / DOC_TEXT_CACHE).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _strip_html(text: str) -> List[str]:
    import html
    import re

    without_scripts = re.sub(r"(?is)<(script|style).*?</\1>", " ", text or "")
    without_tags = re.sub(r"(?s)<[^>]+>", "\n", without_scripts)
    decoded = html.unescape(without_tags)
    lines = [re.sub(r"\s+", " ", item).strip() for item in decoded.splitlines()]
    return [item for item in lines if item]


DOC_BOILERPLATE_TOKENS = (
    "SECURITIES AND EXCHANGE COMMISSION",
    "REPORT OF FOREIGN PRIVATE ISSUER",
    "REGISTRATION NO",
    "WASHINGTON, D.C.",
    "PURSUANT TO",
    "INDICATE BY CHECK MARK",
    "SIGNATURES",
    "ADDRESS OF PRINCIPAL",
    "TRANSLATION OF REGISTRANT",
    "COMMISSION FILE NUMBER",
    "FORM 20-F",
    "FORM 40-F",
    "SECURITIES EXCHANGE ACT",
    "HSINCHU SCIENCE PARK",
)
DOC_HEADLINE_HINTS = (
    "REPORTS",
    "ANNOUNCES",
    "REVENUE",
    "EPS",
    "RESULTS",
    "FINANCIAL STATEMENTS",
    "THIS IS TO REPORT",
    "CHANGES IN",
    "AGREEMENT",
    "DIVIDEND",
)


def _is_meaningful_line(line: str) -> bool:
    if len(line) < 12:
        return False
    upper = line.upper()
    if upper in DOC_BOILERPLATE:
        return False
    if any(token in upper for token in DOC_BOILERPLATE_TOKENS):
        return False
    if set(line) <= {"_", "-", " ", "."}:
        return False
    if line.lower().endswith((".htm", ".html", ".txt", ".jpg", ".png")):
        return False
    if line.isdigit():
        return False
    return True


def _pick_exhibit(index_payload: Dict[str, Any], primary_document: str) -> Optional[str]:
    directory = (index_payload or {}).get("directory") or {}
    items = directory.get("item") or []
    candidates: List[Dict[str, Any]] = []
    for item in items:
        name = str(item.get("name") or "")
        lower = name.lower()
        if not lower.endswith((".htm", ".html")):
            continue
        if lower == str(primary_document or "").lower():
            continue
        if "index" in lower:
            continue
        size = 0
        try:
            size = int(item.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        candidates.append({"name": name, "size": size, "lower": lower})
    if not candidates:
        return None

    def score(entry: Dict[str, Any]) -> Tuple[int, int]:
        keyword = any(
            token in entry["lower"]
            for token in ("earn", "press", "release", "result", "ex99", "ex-99", "q1", "q2", "q3", "q4", "a3q", "a1q")
        )
        return (1 if keyword else 0, entry["size"])

    candidates.sort(key=score, reverse=True)
    return candidates[0]["name"]


def _fetch_filing_text(cik: str, accession: str, primary_document: str) -> Dict[str, str]:
    cik_int = str(int(cik))
    accession_compact = str(accession).replace("-", "")
    index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_compact}/index.json"
    )
    headers = _www_headers()
    response = requests.get(index_url, headers=headers, timeout=SEC_TIMEOUT_SECONDS)
    response.raise_for_status()
    exhibit = _pick_exhibit(response.json(), primary_document)
    document = exhibit or primary_document
    if not document:
        return {}
    document_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_compact}/{document}"
    )
    doc_response = requests.get(document_url, headers=headers, timeout=SEC_TIMEOUT_SECONDS)
    doc_response.raise_for_status()
    lines = _strip_html(doc_response.text)
    meaningful = [line for line in lines if _is_meaningful_line(line)]
    if not meaningful:
        return {}

    headline_index = None
    for index, line in enumerate(meaningful):
        upper = line.upper()
        if 20 <= len(line) <= 220 and any(token in upper for token in DOC_HEADLINE_HINTS):
            headline_index = index
            break
    if headline_index is None:
        headline_index = 0
    headline = meaningful[headline_index]
    if len(headline) > 160:
        headline = headline[:157] + "…"
    body = " ".join(meaningful[headline_index + 1 : headline_index + 10])[:600]
    return {"headline": headline, "excerpt": body, "document": document}


def enrich_filings_with_headlines(
    frame: pd.DataFrame,
    *,
    cache_dir: Path,
    cik: str,
    limit: int = 20,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Attach exhibit headline/excerpt so 公告章节有可读要点（带本地缓存）。"""

    resolved_logger = _resolve_logger(logger)
    if frame is None or frame.empty:
        return frame
    work = frame.copy()
    cache = _load_doc_cache(cache_dir)
    changed = False
    enriched = 0
    for index, row in work.iterrows():
        if enriched >= limit:
            break
        form = str(row.get("公告类型") or "")
        if form not in DOC_ENRICH_FORMS:
            continue
        accession = str(row.get("accession") or "")
        if not accession:
            continue
        cached = cache.get(accession)
        if cached is None:
            try:
                cached = _fetch_filing_text(
                    cik,
                    accession,
                    str(row.get("primary_document") or ""),
                )
            except Exception as exc:
                resolved_logger.debug("抓取 SEC 文档失败 %s: %s", accession, exc)
                cached = {}
            cache[accession] = cached
            changed = True
            time.sleep(0.15)
        if cached:
            work.at[index, "摘要"] = cached.get("headline", "")
            work.at[index, "文档要点"] = cached.get("excerpt", "")
            work.at[index, "文档文件"] = cached.get("document", "")
            enriched += 1
    if changed:
        _save_doc_cache(cache_dir, cache)
    resolved_logger.info("SEC 公告要点补全: %d 条", enriched)
    return work



def filing_headline(
    *,
    cik: str,
    accession: str,
    primary_document: str,
    cache_dir: Path,
    logger: logging.Logger | None = None,
) -> str:
    """读取（或抓取并缓存）单份申报的标题级要点。

    索引里的 6-K 标题只有表单名，判断一份 6-K 到底是业绩稿还是月营收稿，
    必须看申报正文的首条要点。本函数复用 ``sec_doc_text.json`` 缓存，
    避免与 ``enrich_filings_with_headlines`` 重复抓取。
    """

    resolved_logger = _resolve_logger(logger)
    if not accession:
        return ""
    cache = _load_doc_cache(cache_dir)
    cached = cache.get(accession)
    if cached is None:
        try:
            cached = _fetch_filing_text(cik, accession, primary_document)
        except Exception as exc:  # noqa: BLE001 - 抓取失败按无要点处理
            resolved_logger.debug("抓取 SEC 文档失败 %s: %s", accession, exc)
            cached = {}
        cache[accession] = cached
        _save_doc_cache(cache_dir, cache)
        time.sleep(0.15)
    return str((cached or {}).get("headline") or "")


def _build_title(form: str, report_date: str, item: Dict[str, Any]) -> str:
    label = FORM_LABELS.get(form, f"SEC 申报（{form}）")
    extras: List[str] = []
    if report_date:
        extras.append(f"报告期 {report_date}")
    items = str(item.get("items") or "").strip()
    if items:
        extras.append(f"事项 {items}")
    suffix = f"（{'；'.join(extras)}）" if extras else ""
    return f"{label}{suffix}"


def _impact_level(form: str) -> str:
    if form in HIGH_IMPACT_FORMS:
        return "High"
    if form in MEDIUM_IMPACT_FORMS:
        return "Medium"
    return "Low"


ANNUAL_FORMS = {"20-F", "10-K", "40-F"}
QUARTERLY_FORMS = {"6-K", "10-Q", "8-K"}


def filing_notice_date_map(filings: pd.DataFrame) -> Dict[pd.Timestamp, pd.Timestamp]:
    """Map report period end → the SEC filing date that first carried the period.

    不能简单取「报告期后最早的一条申报」：台积电每个季度都会先发月营收 6-K
    （如 10-08），财报 6-K 要到 10-15 才发布。为保持时间因果，季度报告期取
    「报告期后 10~75 天内最晚的一条 6-K/10-Q/8-K」，年报期取 20-F/10-K/40-F
    的申报日；找不到时退回区间内最早一条申报。
    """

    mapping: Dict[pd.Timestamp, pd.Timestamp] = {}
    if filings is None or filings.empty or "报告期" not in filings.columns:
        return mapping

    grouped: Dict[pd.Timestamp, List[Tuple[pd.Timestamp, str]]] = {}
    for _, row in filings.iterrows():
        report_text = str(row.get("报告期") or "").strip()
        if not report_text:
            continue
        try:
            report_date = pd.Timestamp(report_text)
        except Exception:
            continue
        filing_date = pd.Timestamp(row.get("公告时间"))
        if pd.isna(report_date) or pd.isna(filing_date):
            continue
        form = str(row.get("公告类型") or "").strip()
        grouped.setdefault(report_date, []).append((filing_date, form))

    for report_date, entries in grouped.items():
        annual_period = report_date.month == 12
        if annual_period:
            annual_entries = [
                item
                for item in entries
                if item[1] in ANNUAL_FORMS
                and timedelta(days=0) <= (item[0] - report_date) <= timedelta(days=150)
            ]
            if annual_entries:
                mapping[report_date] = min(item[0] for item in annual_entries)
                continue
        quarterly_entries = [
            item
            for item in entries
            if item[1] in QUARTERLY_FORMS
            and timedelta(days=9) <= (item[0] - report_date) <= timedelta(days=75)
        ]
        if quarterly_entries:
            mapping[report_date] = max(item[0] for item in quarterly_entries)
            continue
        later = [item[0] for item in entries if item[0] >= report_date]
        if later:
            mapping[report_date] = min(later)
    return mapping


def update_us_disclosures_cached(
    symbol_info: SymbolInfo,
    *,
    lookback_days: int,
    base_data_dir: str | Path = "data",
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    resolved_logger = _resolve_logger(logger)
    cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.DISCLOSURES,
        base_dir=base_data_dir,
        ensure=True,
    )
    if not force_refresh and not should_refresh(cache_dir, CacheKind.DISCLOSURES):
        return _read_disclosure_csv(cache_dir / "cninfo_list.csv")

    payload = load_sec_submissions(
        symbol_info.code,
        base_dir=base_data_dir,
        force_refresh=force_refresh,
        logger=resolved_logger,
    )
    frame = normalize_filings(payload, lookback_days=lookback_days)
    if frame.empty:
        resolved_logger.warning("SEC 未返回 %s 的有效申报", symbol_info.symbol)
        return frame

    frame = enrich_filings_with_headlines(
        frame,
        cache_dir=cache_dir,
        cik=str(payload.get("_cik") or frame.iloc[0].get("cik") or ""),
        logger=resolved_logger,
    )
    csv_path = cache_dir / "cninfo_list.csv"
    frame.to_csv(csv_path, index=False, encoding="utf-8")
    _write_index(cache_dir / "index.json", symbol_info, frame)
    record_cache_refresh(cache_dir)
    resolved_logger.info(
        "已缓存 %s 的 SEC 申报 %d 条到 %s",
        symbol_info.symbol,
        len(frame),
        csv_path,
    )
    return frame


def _read_disclosure_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _write_index(path: Path, symbol_info: SymbolInfo, frame: pd.DataFrame) -> None:
    index: Dict[str, Any] = {}
    for _, row in frame.iterrows():
        accession = str(row.get("accession") or "")
        key = accession or hashlib.sha256(
            f"{row.get('公告标题')}|{row.get('公告时间')}".encode("utf-8")
        ).hexdigest()[:16]
        index[key] = {
            "announcement_id": accession,
            "org_id": str(row.get("cik") or ""),
            "stock_code": symbol_info.symbol,
            "title": str(row.get("公告标题") or ""),
            "date": pd.Timestamp(row.get("公告时间")).strftime("%Y-%m-%d")
            if pd.notna(row.get("公告时间"))
            else "",
            "url": str(row.get("公告链接") or ""),
            "category": str(row.get("公告类型") or ""),
            "is_financial_report": is_us_financial_filing(
                str(row.get("公告类型") or ""),
                str(row.get("公告标题") or ""),
                str(row.get("description") or ""),
            ),
            "primary_document": str(row.get("primary_document") or ""),
            "downloaded": False,
            "summarized": True,
            "summary_json_path": str(path.parent / "cninfo_list.csv"),
            "audited": False,
            "content_source": "sec_edgar",
        }
    path.write_text(
        json.dumps(index, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_us_disclosures(
    symbol_info: SymbolInfo,
    *,
    base_dir: str | Path = "data",
) -> pd.DataFrame:
    cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.DISCLOSURES,
        base_dir=base_dir,
        ensure=False,
    )
    return _read_disclosure_csv(cache_dir / "cninfo_list.csv")


def build_us_filing_news_items(
    symbol_info: SymbolInfo,
    *,
    base_dir: str | Path = "data",
    lookback_days: int = 180,
    limit: int = 20,
    today: Optional[date] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Turn cached SEC filings into ``news_items`` for the research pack."""

    diagnostics: List[str] = []
    frame = load_us_disclosures(symbol_info, base_dir=base_dir)
    if frame.empty:
        diagnostics.append("SEC 申报缓存为空或尚未刷新。")
        return [], diagnostics

    reference = pd.Timestamp(today or date.today())
    cutoff = reference - pd.Timedelta(days=max(int(lookback_days), 1))
    work = frame.copy()
    work["公告时间"] = pd.to_datetime(work["公告时间"], errors="coerce")
    work = work.dropna(subset=["公告时间"])
    work = work[(work["公告时间"] <= reference) & (work["公告时间"] >= cutoff)]
    work = work.sort_values("公告时间", ascending=False).head(limit)

    items: List[Dict[str, Any]] = []
    insider_seen = 0
    for _, row in work.iterrows():
        form = str(row.get("公告类型") or "")
        headline = str(row.get("摘要") or "").strip()
        # 内部人持股变动类 6-K 每季度重复出现，只保留最近一条，避免淹没经营公告
        if "shareholdings of" in headline.lower() and "board of directors" in headline.lower():
            insider_seen += 1
            if insider_seen > 1:
                continue
        base_title = str(row.get("公告标题") or FORM_LABELS.get(form, form))
        title = f"{headline}｜{base_title}" if headline else base_title
        filing_dt = pd.Timestamp(row.get("公告时间"))
        report_text = str(row.get("报告期") or "").strip()
        summary_parts = [f"SEC {form} 申报"]
        if report_text:
            summary_parts.append(f"报告期 {report_text}")
        excerpt = str(row.get("文档要点") or "").strip()
        if excerpt:
            summary_parts.append(excerpt)
        items.append(
            {
                "title": title,
                "datetime": filing_dt.strftime("%Y-%m-%d 00:00:00"),
                "category": f"SEC {form}",
                "impact_level": _impact_level(form),
                "sentiment": "neutral",
                "summary": "；".join(summary_parts),
                "announcement_id": str(row.get("accession") or ""),
                "url": str(row.get("公告链接") or ""),
            }
        )
    diagnostics.append(f"SEC 申报：近 {lookback_days} 天命中 {len(items)} 条。")
    return items, diagnostics


__all__ = [
    "build_us_filing_news_items",
    "filing_notice_date_map",
    "load_cik_map",
    "load_sec_submissions",
    "load_us_disclosures",
    "normalize_filings",
    "update_us_disclosures_cached",
]
