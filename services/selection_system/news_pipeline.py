"""News ingestion and state aggregation for the selection system."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import akshare as ak
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI

from core.logging import get_logger
from services.research.macro_summary import get_macro_summary
from utlity import get_stock_data_dir

from .models import MasterUniverseDocument, MasterUniverseStock
from .paths import SelectionSystemPaths
from .state_models import NewsItem, RawNewsItem
from .store import load_json_file, save_json_file
from .symbols import stock_to_symbol_info


LOGGER = get_logger("SelectionNews")
load_dotenv(".env")

PRIMARY_MARKET_SOURCES = {"cls_key", "ths_global", "em_global"}
AUX_MARKET_SOURCES = {"em_breakfast"}
GENERIC_THEME_NAMES = {"telegraph", "breakfast", "macro", "market_context", "industry_catalyst"}
DISCLOSURE_LOOKBACK_DAYS = 14
PRIMARY_NEWS_LOOKBACK_DAYS = 7
CORE_HOT_NEWS_DAYS = 3
MAX_ITEMS_PER_SOURCE = {
    "cls_key": 80,
    "ths_global": 80,
    "em_global": 60,
    "em_breakfast": 20,
}
DISCLOSURE_KEEP_CATEGORIES = {
    "Financial_Report",
    "Contract",
    "M&A",
    "Litigation",
    "Regulation",
    "Personnel",
    "Equity_Change",
    "Operation",
}
DISCLOSURE_KEEP_IMPACT = {"High", "Medium"}

POSITIVE_KEYWORDS = ("增长", "上调", "中标", "回购", "突破", "催化", "景气", "订单", "扩产", "创新高")
NEGATIVE_KEYWORDS = ("下滑", "处罚", "亏损", "风险", "减持", "诉讼", "暴跌", "违约", "调查", "质押")
THEME_KEYWORDS: Dict[str, Sequence[str]] = {
    "AI": ("ai", "人工智能", "大模型", "算力"),
    "算力": ("算力", "服务器", "gpu", "数据中心"),
    "半导体": ("半导体", "晶圆", "芯片", "存储", "光刻", "封测"),
    "光模块": ("光模块", "cpo", "光通信"),
    "机器人": ("机器人", "自动化", "伺服"),
    "智能驾驶": ("智能驾驶", "自动驾驶", "智驾"),
    "新能源车": ("新能源车", "电动车", "动力电池", "锂电"),
    "储能": ("储能", "储能电池"),
    "光伏": ("光伏", "逆变器", "硅料"),
    "风电": ("风电", "海风"),
    "核电": ("核电", "核能"),
    "军工": ("军工", "装备", "国防"),
    "黄金": ("黄金", "金价"),
    "稀土": ("稀土",),
    "高股息": ("高股息", "红利", "分红"),
    "中概互联网": ("互联网", "电商", "云计算"),
    "消费电子": ("手机", "消费电子", "面板", "折叠屏"),
    "创新药": ("创新药", "新药", "药品"),
    "医药器械": ("医疗器械", "ivd"),
    "旅游酒店": ("旅游", "酒店", "出行"),
    "物流": ("物流", "快递"),
    "煤化工": ("煤化工", "煤制烯烃"),
}


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def _parse_dt(value: Any, *, fallback: str | None = None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        value = ""
    text = str(value).strip()
    if not text and fallback:
        text = fallback
    if not text:
        return None
    candidates = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _load_disclosure_items(stock: MasterUniverseStock) -> List[Dict[str, Any]]:
    info = stock_to_symbol_info(stock)
    news_dir = get_stock_data_dir(info) / "news"
    for filename in ("news_audited.json", "news.json"):
        payload = load_json_file(news_dir / filename, default={})
        if isinstance(payload, dict) and isinstance(payload.get("news_items"), list):
            items = []
            for item in payload["news_items"]:
                if isinstance(item, dict):
                    enriched = dict(item)
                    enriched["_selection_source_file"] = filename
                    items.append(enriched)
            if items:
                return items
    return []


def _should_keep_disclosure_item(item: Dict[str, Any], run_dt: datetime) -> bool:
    published_dt = _parse_dt(item.get("datetime"), fallback=run_dt.isoformat())
    if published_dt is None:
        return False
    if published_dt.date() > run_dt.date():
        return False
    if published_dt < run_dt - timedelta(days=DISCLOSURE_LOOKBACK_DAYS):
        return False

    category = str(item.get("category") or "").strip()
    impact = str(item.get("impact_level") or "").strip()
    title = str(item.get("title") or "")
    summary = str(item.get("summary") or "")
    text = f"{title}\n{summary}"

    if category and category not in DISCLOSURE_KEEP_CATEGORIES:
        return False
    if impact and impact not in DISCLOSURE_KEEP_IMPACT:
        if not any(keyword in text for keyword in ("回购", "减持", "订单", "中标", "业绩", "处罚", "并购", "增持", "分红")):
            return False
    if any(keyword in text for keyword in ("监事会决议", "法律意见书", "核查意见", "质押", "担保")) and impact != "High":
        return False
    return True


def collect_raw_news_items(
    universe: MasterUniverseDocument,
    run_date: str,
    *,
    include_live_feeds: bool = True,
    lookback_days: int = PRIMARY_NEWS_LOOKBACK_DAYS,
    max_feed_rows: int = 80,
) -> List[Dict[str, Any]]:
    run_dt = _parse_dt(run_date, fallback=run_date) or datetime.now()
    collected_at = datetime.now().isoformat()
    raw_items: List[Dict[str, Any]] = []

    for stock in universe.stocks:
        disclosure_items = _load_disclosure_items(stock)
        for item in disclosure_items:
            published_dt = _parse_dt(item.get("datetime"), fallback=run_date)
            if not _should_keep_disclosure_item(item, run_dt) or published_dt is None:
                continue
            title = str(item.get("title") or stock.name).strip()
            content_parts = [
                item.get("summary"),
                item.get("raw_facts"),
                item.get("audit_analysis"),
                item.get("financial_implication"),
                item.get("price_driver"),
                item.get("risk_warning"),
            ]
            content = "\n".join(str(part).strip() for part in content_parts if part).strip()
            raw_items.append(
                RawNewsItem(
                    raw_id=f"disclosure::{stock.symbol}::{_sha1(title + published_dt.isoformat())}",
                    source="disclosure",
                    source_type="disclosure",
                    collected_at=collected_at,
                    published_at=published_dt.isoformat(),
                    title=title,
                    content=content or title,
                    url=str(item.get("url") or item.get("link") or ""),
                    raw_tags=[
                        str(item.get("category") or ""),
                        str(item.get("impact_level") or ""),
                        str(item.get("sentiment") or ""),
                    ],
                    extra={
                        "symbol": stock.symbol,
                        "name": stock.name,
                        "sector": stock.sector,
                        "industry": stock.industry,
                        "structured_item": item,
                        "selection_role": "symbol_catalyst",
                    },
                ).to_dict()
            )

    macro_text = get_macro_summary(today_time=run_date)
    if isinstance(macro_text, str) and macro_text.strip() and not macro_text.startswith("未找到"):
        raw_items.append(
            RawNewsItem(
                raw_id=f"macro::{run_date}",
                source="macro_summary",
                source_type="macro_note",
                collected_at=collected_at,
                published_at=run_dt.isoformat(),
                title=f"{run_date} 宏观总结",
                content=macro_text.strip(),
                raw_tags=["宏观", "市场"],
                extra={"selection_role": "market_context"},
            ).to_dict()
        )

    if include_live_feeds:
        raw_items.extend(_collect_live_feed_items(run_dt, collected_at, max_feed_rows=max_feed_rows))

    deduped: Dict[str, Dict[str, Any]] = {}
    for item in raw_items:
        key = item["raw_id"]
        deduped[key] = item
    source_counts = Counter(item.get("source") for item in deduped.values())
    LOGGER.info("raw_news_items 收集完成: total=%d source_counts=%s", len(deduped), dict(source_counts))
    return list(deduped.values())


def _collect_live_feed_items(run_dt: datetime, collected_at: str, *, max_feed_rows: int) -> List[Dict[str, Any]]:
    sources = [
        ("em_breakfast", "breakfast", ak.stock_info_cjzc_em),
        ("em_global", "telegraph", ak.stock_info_global_em),
        ("ths_global", "telegraph", ak.stock_info_global_ths),
        ("cls_key", "telegraph", lambda: ak.stock_info_global_cls(symbol="重点")),
    ]
    items: List[Dict[str, Any]] = []
    for source_name, source_type, loader in sources:
        try:
            frame = loader()
        except Exception as exc:
            LOGGER.warning("实时新闻源拉取失败: source=%s error=%s", source_name, exc)
            continue
        if frame is None or getattr(frame, "empty", True):
            continue
        source_limit = min(max_feed_rows, MAX_ITEMS_PER_SOURCE.get(source_name, max_feed_rows))
        for _, row in frame.head(source_limit).iterrows():
            title = str(row.get("标题") or row.get("内容") or row.get("摘要") or "").strip()
            content = str(row.get("内容") or row.get("摘要") or title).strip()
            published = _resolve_feed_published_at(row, run_dt)
            published_dt = _parse_dt(published, fallback=run_dt.isoformat()) or run_dt
            if not title:
                continue
            if source_name in PRIMARY_MARKET_SOURCES and published_dt < run_dt - timedelta(days=PRIMARY_NEWS_LOOKBACK_DAYS):
                continue
            if source_name in AUX_MARKET_SOURCES and published_dt.date() != run_dt.date():
                continue
            items.append(
                RawNewsItem(
                    raw_id=f"{source_name}::{_sha1(title + published)}",
                    source=source_name,
                    source_type=source_type,
                    collected_at=collected_at,
                    published_at=published,
                    title=title,
                    content=_enrich_breakfast_content(title, content, str(row.get("链接") or row.get("url") or "")) if source_name == "em_breakfast" else content,
                    url=str(row.get("链接") or row.get("url") or ""),
                    raw_tags=[],
                    extra={
                        "row": {key: str(value) for key, value in row.to_dict().items()},
                        "selection_role": "market_theme",
                        "source_rank": _source_rank(source_name),
                    },
                ).to_dict()
            )
    return items


def _enrich_breakfast_content(title: str, summary: str, url: str) -> str:
    if not url:
        return summary
    try:
        enriched = _fetch_breakfast_article_payload(url)
        body_text = enriched.get("body_text", "").strip()
        global_market_text = enriched.get("global_market_text", "").strip()
        sections = [title.strip(), summary.strip()]
        if body_text:
            sections.append("全文内容：\n" + body_text)
        if global_market_text:
            sections.append("环球市场图片提取：\n" + global_market_text)
        return "\n\n".join(section for section in sections if section).strip()
    except Exception as exc:
        LOGGER.warning("财经早餐增强失败: url=%s error=%s", url, exc)
        return summary


def _fetch_breakfast_article_payload(url: str) -> Dict[str, str]:
    article_id = _extract_article_id(url)
    cache_dir = Path("data/market_state/raw_news_assets/breakfast")
    cache_dir.mkdir(parents=True, exist_ok=True)
    html_path = cache_dir / f"{article_id}.html"
    image_path = cache_dir / f"{article_id}_global_market.png"
    image_json_path = cache_dir / f"{article_id}_global_market.json"

    html = _download_breakfast_html(url)
    html_path.write_text(html, encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("div#ContentBody")
    if content is None:
        raise ValueError("未找到财经早餐正文容器 div#ContentBody")

    body_text = _extract_breakfast_body_text(content)
    global_market_img_url = _extract_global_market_image_url(content)
    global_market_text = ""
    if global_market_img_url:
        _download_breakfast_image(global_market_img_url, image_path)
        global_market_payload = _extract_global_market_with_qwen(image_path, image_json_path)
        global_market_text = _format_global_market_payload(global_market_payload)

    return {
        "body_text": body_text,
        "global_market_text": global_market_text,
    }


def _extract_article_id(url: str) -> str:
    match = re.search(r"/a/(\\d+)\\.html", url)
    if match:
        return match.group(1)
    return _sha1(url)


def _download_breakfast_html(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    return response.text


def _extract_breakfast_body_text(content: Any) -> str:
    parts: List[str] = []
    for child in content.find_all(["h3", "p"], recursive=False):
        if child.name == "h3":
            heading = child.get_text(" ", strip=True).replace("\u3000", "").strip()
            if heading:
                parts.append(f"## {heading}")
            continue
        if child.find("img") and "环球市场" in " ".join(parts[-1:]):
            continue
        text = child.get_text("\n", strip=True).replace("\u3000", "").strip()
        if text:
            parts.append(text)
    return "\n".join(parts).strip()


def _extract_global_market_image_url(content: Any) -> str:
    for heading in content.find_all("h3", recursive=False):
        heading_text = heading.get_text(" ", strip=True).replace("\u3000", "")
        if "环球市场" not in heading_text:
            continue
        sibling = heading.find_next_sibling()
        while sibling is not None:
            if getattr(sibling, "name", None) == "p" and sibling.find("img"):
                src = sibling.find("img").get("src") or ""
                return src.replace("https://", "http://", 1)
            if getattr(sibling, "name", None) == "h3":
                break
            sibling = sibling.find_next_sibling()
    return ""


def _download_breakfast_image(url: str, image_path: Path) -> None:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    image_path.write_bytes(response.content)


def _extract_global_market_with_qwen(image_path: Path, image_json_path: Path) -> Dict[str, Any]:
    if image_json_path.exists():
        try:
            return json.loads(image_json_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    api_key = os.getenv("EXTRACTION_MODEL_API_KEY")
    base_url = os.getenv("EXTRACTION_MODEL_BASE_URL")
    if not api_key or not base_url:
        raise ValueError("缺少 EXTRACTION_MODEL_API_KEY 或 EXTRACTION_MODEL_BASE_URL")

    client = OpenAI(api_key=api_key, base_url=base_url)
    file_obj = client.files.create(file=image_path, purpose="file-extract")
    file_id = getattr(file_obj, "id", None)
    prompt = (
        "请读取这张财经图片，并只输出一个JSON对象。"
        "字段要求：capture_time, sections。"
        "sections 是数组，每个 section 包含 section_name, rows。"
        "rows 是数组，每行包含 market, name, latest, change, change_pct。"
        "数字按图片原文输出，不要自行解释，不要输出额外文字。"
    )
    completion = client.chat.completions.create(
        model="qwen-doc-turbo",
        messages=[
            {"role": "system", "content": "你是一个严谨的财经OCR结构化助手。"},
            {"role": "system", "content": f"fileid://{file_id}"},
            {"role": "user", "content": prompt},
        ],
    )
    payload = _parse_json_like(completion.choices[0].message.content)
    image_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def _parse_json_like(text: str) -> Dict[str, Any]:
    content = text.strip()
    match = re.search(r"```json\\s*([\\s\\S]*?)```", content, re.IGNORECASE)
    if match:
        content = match.group(1).strip()
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"无法解析图片OCR JSON: {text[:500]}")
    parsed = json.loads(content[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("图片OCR返回不是对象")
    return parsed


def _format_global_market_payload(payload: Dict[str, Any]) -> str:
    lines = []
    capture_time = str(payload.get("capture_time") or "").strip()
    if capture_time:
        lines.append(f"截点时间: {capture_time}")
    for section in payload.get("sections", []) or []:
        section_name = str(section.get("section_name") or "").strip()
        if section_name:
            lines.append(f"[{section_name}]")
        for row in section.get("rows", []) or []:
            market = str(row.get("market") or "").strip()
            name = str(row.get("name") or "").strip()
            latest = str(row.get("latest") or "").strip()
            change = str(row.get("change") or "").strip()
            change_pct = str(row.get("change_pct") or "").strip()
            row_text = " | ".join(part for part in [market, name, latest, change, change_pct] if part)
            if row_text:
                lines.append(row_text)
    return "\n".join(lines).strip()


def _resolve_feed_published_at(row: Any, run_dt: datetime) -> str:
    for field in ("发布时间", "发布日期", "时间"):
        dt = _parse_dt(row.get(field))
        if dt is not None:
            return dt.isoformat()
    publish_date = str(row.get("发布日期") or run_dt.strftime("%Y-%m-%d")).strip()
    publish_time = str(row.get("发布时间") or "00:00:00").strip()
    dt = _parse_dt(f"{publish_date} {publish_time}", fallback=run_dt.isoformat())
    return (dt or run_dt).isoformat()


def build_news_items(raw_items: Sequence[Dict[str, Any]], universe: MasterUniverseDocument) -> List[Dict[str, Any]]:
    news_items = []
    for raw in raw_items:
        structured = raw.get("extra", {}).get("structured_item")
        if isinstance(structured, dict):
            item = _build_news_item_from_disclosure(raw)
        else:
            item = _build_news_item_from_raw(raw, universe)
        news_items.append(item.to_dict())
    LOGGER.info("news_items 构建完成: total=%d", len(news_items))
    return news_items


def _build_news_item_from_disclosure(raw: Dict[str, Any]) -> NewsItem:
    structured = raw["extra"]["structured_item"]
    symbol = str(raw["extra"].get("symbol") or "")
    name = str(raw["extra"].get("name") or "")
    sector = str(raw["extra"].get("sector") or "")
    industry = str(raw["extra"].get("industry") or "")
    raw_facts = str(structured.get("raw_facts") or structured.get("summary") or raw.get("content") or "")
    key_points = _compact_points(
        [
            structured.get("summary"),
            structured.get("financial_implication"),
            structured.get("price_driver"),
            structured.get("risk_warning"),
        ]
    )
    sentiment = str(structured.get("sentiment") or "Neutral")
    impact_level = str(structured.get("impact_level") or "Medium")
    bull_points = _compact_points([structured.get("financial_implication"), structured.get("price_driver")]) if sentiment.lower() == "positive" else []
    bear_points = _compact_points([structured.get("risk_warning"), structured.get("audit_analysis")]) if sentiment.lower() != "positive" else []
    themes = _extract_theme_tags(
        " ".join(
            [
                raw.get("title", ""),
                raw_facts,
                sector,
                industry,
            ]
        ),
        sector=sector,
        industry=industry,
    )
    if not themes:
        themes = [sector or industry or str(structured.get("category") or "公告事件")]
    return NewsItem(
        item_id=f"news::{_sha1(raw['raw_id'])}",
        raw_id=raw["raw_id"],
        source=raw["source"],
        source_type=raw["source_type"],
        published_at=raw["published_at"],
        title=str(raw["title"]),
        url=str(raw.get("url") or ""),
        event_type=_map_disclosure_event_type(str(structured.get("category") or "")),
        scope="symbol",
        raw_facts=raw_facts,
        key_points=key_points,
        quantitative_data=dict(structured.get("quantitative_data") or {}),
        entities={
            "symbols": [symbol] if symbol else [],
            "companies": [name] if name else [],
            "sectors": [sector] if sector else [],
            "themes": themes,
            "people": [],
            "institutions": [],
        },
        bull_points=bull_points,
        bear_points=bear_points,
        time_sensitivity=str(structured.get("validity_period") or "Medium"),
        importance_hint={"high": 0.9, "medium": 0.65, "low": 0.35}.get(impact_level.lower(), 0.5),
        novelty_hint=0.6,
        sentiment_hint=sentiment,
        dedupe_hash=_sha1(str(raw["title"]) + raw["published_at"]),
        extra={
            "impact_level": impact_level,
            "audit_analysis": structured.get("audit_analysis"),
            "price_driver": structured.get("price_driver"),
            "risk_warning": structured.get("risk_warning"),
            "selection_role": "symbol_catalyst",
        },
    )


def _build_news_item_from_raw(raw: Dict[str, Any], universe: MasterUniverseDocument) -> NewsItem:
    text = f"{raw.get('title', '')}\n{raw.get('content', '')}"
    if str(raw.get("source_type")) == "macro_note":
        matched = []
    else:
        matched = _match_universe_stocks(text, universe.stocks)
    sectors = sorted({stock.sector for stock in matched if stock.sector})
    themes = _extract_theme_tags(text, sector=" ".join(sectors), industry=" ".join(stock.industry for stock in matched if stock.industry))
    if not themes and sectors:
        themes = sectors[:3]
    if not themes:
        fallback_theme = _infer_fallback_theme(text)
        themes = [fallback_theme] if fallback_theme else []
    sentiment = _infer_sentiment(text)
    key_points = _compact_points(_split_sentences(text))
    bull_points = [point for point in key_points if any(keyword in point for keyword in POSITIVE_KEYWORDS)]
    bear_points = [point for point in key_points if any(keyword in point for keyword in NEGATIVE_KEYWORDS)]
    return NewsItem(
        item_id=f"news::{_sha1(raw['raw_id'])}",
        raw_id=raw["raw_id"],
        source=str(raw["source"]),
        source_type=str(raw["source_type"]),
        published_at=str(raw["published_at"]),
        title=str(raw["title"]),
        url=str(raw.get("url") or ""),
        event_type=_guess_event_type(text, raw.get("source_type")),
        scope=_infer_scope(matched, raw),
        raw_facts=str(raw.get("content") or raw.get("title") or ""),
        key_points=key_points,
        quantitative_data=_extract_numbers(text),
        entities={
            "symbols": [stock.symbol for stock in matched],
            "companies": [stock.name for stock in matched],
            "sectors": sectors,
            "themes": themes,
            "people": [],
            "institutions": [],
        },
        bull_points=bull_points,
        bear_points=bear_points,
        time_sensitivity="Short",
        importance_hint=_infer_importance(text, raw.get("source_type"), len(matched)),
        novelty_hint=0.5,
        sentiment_hint=sentiment,
        dedupe_hash=_sha1(str(raw["title"]) + str(raw["published_at"])),
        extra={
            "selection_role": raw.get("extra", {}).get("selection_role", "market_theme"),
            "source_rank": raw.get("extra", {}).get("source_rank", _source_rank(str(raw.get("source")))),
        },
    )


def load_recent_news_items(paths: SelectionSystemPaths, run_date: str, *, lookback_days: int = 45) -> List[Dict[str, Any]]:
    end_dt = _parse_dt(run_date, fallback=run_date) or datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days)
    items: List[Dict[str, Any]] = []
    for run_dir in paths.iter_run_dirs():
        run_dt = _parse_dt(run_dir.name, fallback=run_dir.name)
        if run_dt is None or run_dt < start_dt or run_dt > end_dt:
            continue
        payload = load_json_file(run_dir / "02_news_items.json", default={})
        for item in payload.get("items", []) if isinstance(payload, dict) else []:
            if isinstance(item, dict):
                items.append(item)
    return items


def rebuild_theme_state(news_items: Sequence[Dict[str, Any]], run_date: str) -> Dict[str, Any]:
    run_dt = _parse_dt(run_date, fallback=run_date) or datetime.now()
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in news_items:
        if item.get("source_type") == "disclosure" and not item.get("entities", {}).get("themes"):
            continue
        entities = item.get("entities") or {}
        themes = entities.get("themes") or entities.get("sectors") or [item.get("event_type") or "市场事件"]
        for theme in themes:
            if theme and str(theme).strip() not in GENERIC_THEME_NAMES:
                groups[str(theme)].append(item)

    themes_payload = []
    for theme_name, evidence in groups.items():
        evidence_sorted = sorted(evidence, key=lambda entry: entry.get("published_at", ""), reverse=True)
        symbol_counts = Counter(symbol for item in evidence_sorted for symbol in (item.get("entities", {}).get("symbols") or []))
        recent_scores = [
            _recency_weight(item.get("published_at"), run_dt, source=item.get("source"))
            * float(item.get("importance_hint") or 0.5)
            * _source_rank(str(item.get("source")))
            for item in evidence_sorted
        ]
        heat_score = min(100.0, round(sum(score * 28 for score in recent_scores), 2))
        importance_score = min(100.0, round(sum(float(item.get("importance_hint") or 0.5) for item in evidence_sorted) / max(len(evidence_sorted), 1) * 100, 2))
        recent_3d = sum(1 for item in evidence_sorted if _days_ago(item.get("published_at"), run_dt) <= CORE_HOT_NEWS_DAYS)
        recent_7d = sum(1 for item in evidence_sorted if _days_ago(item.get("published_at"), run_dt) <= 7)
        novelty_score = round(min(100.0, recent_3d / max(len(evidence_sorted), 1) * 100), 2)
        persistence_score = round(min(100.0, len({str(item.get("published_at"))[:10] for item in evidence_sorted}) * 8.0), 2)
        crowdedness_score = round(min(100.0, max(len(evidence_sorted) - 1, 0) * 10.0), 2)
        market_source_count = sum(1 for item in evidence_sorted if str(item.get("source")) in PRIMARY_MARKET_SOURCES)
        confidence_score = round(min(100.0, (market_source_count / max(len(evidence_sorted), 1)) * 85 + 15), 2)
        themes_payload.append(
            {
                "theme_id": f"theme::{_sha1(theme_name)}",
                "theme_name": theme_name,
                "theme_type": "theme",
                "status": "active" if recent_7d else "cooling",
                "first_seen_at": evidence_sorted[-1].get("published_at"),
                "last_seen_at": evidence_sorted[0].get("published_at"),
                "last_merged_at": datetime.now().isoformat(),
                "one_line_summary": f"{theme_name} 近阶段累计 {len(evidence_sorted)} 条事件，最新聚焦 {', '.join(item.get('title', '') for item in evidence_sorted[:2])}",
                "thesis_summary": f"{theme_name} 的核心线索来自 {', '.join(item.get('source', '') for item in evidence_sorted[:3])}，主要关联 {', '.join(symbol_counts.keys()) if symbol_counts else '市场级事件'}。",
                "today_delta": [item.get("title") for item in evidence_sorted if str(item.get("published_at", "")).startswith(run_date)][:5],
                "bull_case": _unique_points(point for item in evidence_sorted for point in item.get("bull_points", [])),
                "bear_case": _unique_points(point for item in evidence_sorted for point in item.get("bear_points", [])),
                "open_questions": [],
                "heat_score": heat_score,
                "importance_score": importance_score,
                "novelty_score": novelty_score,
                "persistence_score": persistence_score,
                "crowdedness_score": crowdedness_score,
                "confidence_score": confidence_score,
                "trend": "up" if recent_3d >= max(1, recent_7d - recent_3d) else ("flat" if recent_7d else "down"),
                "decay_days": 21,
                "archive_after_days": 60,
                "linked_symbols": list(symbol_counts.keys())[:10],
                "leader_candidates": [symbol for symbol, _ in symbol_counts.most_common(5)],
                "related_sectors": _unique_points(sector for item in evidence_sorted for sector in item.get("entities", {}).get("sectors", [])),
                "evidence_item_ids_recent": [item.get("item_id") for item in evidence_sorted[:20]],
            }
        )

    themes_payload.sort(key=lambda item: item.get("heat_score", 0), reverse=True)
    return {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "themes": themes_payload,
    }


def rebuild_symbol_hot_state(
    universe: MasterUniverseDocument,
    news_items: Sequence[Dict[str, Any]],
    theme_state: Dict[str, Any],
    snapshots_by_symbol: Dict[str, Dict[str, Any]],
    run_date: str,
) -> Dict[str, Any]:
    theme_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for theme in theme_state.get("themes", []):
        for symbol in theme.get("linked_symbols", []):
            theme_index[str(symbol)].append(theme)

    direct_news: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in news_items:
        for symbol in item.get("entities", {}).get("symbols", []):
            direct_news[str(symbol)].append(item)

    symbols_payload = []
    for stock in universe.stocks:
        snapshot = snapshots_by_symbol.get(stock.symbol, {})
        themes = sorted(theme_index.get(stock.symbol, []), key=lambda entry: entry.get("heat_score", 0), reverse=True)
        news = sorted(direct_news.get(stock.symbol, []), key=lambda entry: entry.get("published_at", ""), reverse=True)
        hotness_score = round(min(100.0, sum(min(float(theme.get("heat_score", 0)), 40.0) for theme in themes[:3]) * 0.7 + len(news) * 8.0), 2)
        liquidity = _as_float(snapshot.get("liquidity_score"))
        turnover = _as_float(snapshot.get("turnover_rate"))
        daily_change = abs(_as_float(snapshot.get("daily_change_pct")))
        actionability_score = round(min(100.0, hotness_score * 0.55 + liquidity * 30 + min(turnover * 2, 20) + min(daily_change * 1.5, 15)), 2)
        leader_score = round(min(100.0, len([theme for theme in themes if stock.symbol in theme.get("leader_candidates", [])]) * 18.0 + liquidity * 20), 2)
        symbols_payload.append(
            {
                "symbol": stock.symbol,
                "name": stock.name,
                "last_updated_at": datetime.now().isoformat(),
                "hot_themes": [
                    {
                        "theme_name": theme.get("theme_name"),
                        "heat_score": theme.get("heat_score"),
                        "trend": theme.get("trend"),
                    }
                    for theme in themes[:5]
                ],
                "hot_thesis_summary": f"{stock.name} 当前关联主题：{', '.join(theme.get('theme_name') for theme in themes[:3]) or '暂无显著热点'}。",
                "today_news_delta": [item.get("title") for item in news if str(item.get("published_at", "")).startswith(run_date)][:5],
                "short_term_risks": _unique_points(point for item in news for point in item.get("bear_points", []))[:5],
                "hotness_score": hotness_score,
                "actionability_score": actionability_score,
                "leader_score": leader_score,
                "must_track_today": hotness_score >= 55 or any(str(item.get("published_at", "")).startswith(run_date) for item in news),
                "snapshot": snapshot,
            }
        )

    symbols_payload.sort(key=lambda item: item.get("hotness_score", 0), reverse=True)
    return {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "symbols": symbols_payload,
    }


def persist_raw_news(path: Path, run_date: str, raw_items: Sequence[Dict[str, Any]]) -> Path:
    return save_json_file(
        path,
        {
            "schema_version": 1,
            "run_date": run_date,
            "updated_at": datetime.now().isoformat(),
            "items": list(raw_items),
        },
    )


def persist_news_items(path: Path, run_date: str, news_items: Sequence[Dict[str, Any]]) -> Path:
    return save_json_file(
        path,
        {
            "schema_version": 1,
            "run_date": run_date,
            "updated_at": datetime.now().isoformat(),
            "items": list(news_items),
        },
    )


def _compact_points(parts: Iterable[Any], *, limit: int = 5) -> List[str]:
    result = []
    seen = set()
    for part in parts:
        text = str(part or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
        if len(result) >= limit:
            break
    return result


def _split_sentences(text: str) -> List[str]:
    sentences = re.split(r"[。\n；;!！?？]+", str(text or ""))
    return [sentence.strip() for sentence in sentences if sentence and sentence.strip()]


def _extract_numbers(text: str) -> Dict[str, Any]:
    return {
        "number_mentions": re.findall(r"\d+(?:\.\d+)?(?:亿|万|%)?", str(text or ""))[:12],
    }


def _match_universe_stocks(text: str, stocks: Sequence[MasterUniverseStock]) -> List[MasterUniverseStock]:
    lowered = str(text or "").lower()
    matched = []
    for stock in sorted(stocks, key=lambda item: len(item.name), reverse=True):
        if stock.name and stock.name.lower() in lowered:
            matched.append(stock)
            continue
        if stock.symbol.split(".", 1)[0] in lowered:
            matched.append(stock)
    return matched


def _extract_theme_tags(text: str, *, sector: str = "", industry: str = "") -> List[str]:
    lowered = f"{text} {sector} {industry}".lower()
    themes = [theme for theme, keywords in THEME_KEYWORDS.items() if any(keyword in lowered for keyword in keywords)]
    if sector:
        themes.append(str(sector))
    return _unique_points(themes)[:6]


def _infer_sentiment(text: str) -> str:
    lowered = str(text or "").lower()
    pos = sum(1 for keyword in POSITIVE_KEYWORDS if keyword in lowered)
    neg = sum(1 for keyword in NEGATIVE_KEYWORDS if keyword in lowered)
    if pos > neg:
        return "Positive"
    if neg > pos:
        return "Negative"
    return "Neutral"


def _guess_event_type(text: str, source_type: Any) -> str:
    lowered = str(text or "").lower()
    if "政策" in lowered or "监管" in lowered:
        return "policy"
    if "财报" in lowered or "业绩" in lowered:
        return "earnings"
    if "中标" in lowered or "合同" in lowered or "订单" in lowered:
        return "contract"
    if "回购" in lowered or "减持" in lowered:
        return "capital_market"
    if str(source_type) == "macro_note":
        return "macro"
    return "industry_catalyst"


def _infer_fallback_theme(text: str) -> str:
    lowered = str(text or "").lower()
    keyword_map = {
        "政策": "政策催化",
        "机器人": "机器人",
        "半导体": "半导体",
        "芯片": "半导体",
        "电池": "新能源车",
        "汽车": "新能源车",
        "黄金": "黄金",
        "医药": "创新药",
        "算力": "AI",
        "ai": "AI",
    }
    for keyword, theme in keyword_map.items():
        if keyword in lowered:
            return theme
    return ""


def _map_disclosure_event_type(category: str) -> str:
    mapping = {
        "Financial_Report": "earnings",
        "Contract": "contract",
        "M&A": "mna",
        "Litigation": "litigation",
        "Regulation": "regulation",
        "Personnel": "personnel",
        "Equity_Change": "capital_market",
        "Operation": "operation",
    }
    return mapping.get(category, "disclosure_event")


def _infer_scope(matched: Sequence[MasterUniverseStock], raw: Dict[str, Any]) -> str:
    if raw.get("source_type") == "macro_note":
        return "market"
    if len(matched) >= 2:
        return "industry"
    if len(matched) == 1:
        return "symbol"
    return "market"


def _infer_importance(text: str, source_type: Any, matched_count: int) -> float:
    score = 0.35
    if str(source_type) == "macro_note":
        score += 0.15
    if str(source_type) == "telegraph":
        score += 0.2
    if any(keyword in str(text or "") for keyword in ("政策", "中标", "业绩", "回购", "减持", "处罚")):
        score += 0.25
    if matched_count:
        score += min(0.2, matched_count * 0.05)
    return min(score, 0.95)


def _recency_weight(published_at: Any, run_dt: datetime, *, source: Any = None) -> float:
    days = _days_ago(published_at, run_dt)
    source_name = str(source or "")
    if source_name in PRIMARY_MARKET_SOURCES:
        if days <= 1:
            return 1.0
        if days <= CORE_HOT_NEWS_DAYS:
            return 0.85
        if days <= 7:
            return 0.55
        return 0.15
    if source_name == "disclosure":
        if days <= 3:
            return 0.75
        if days <= 7:
            return 0.5
        if days <= DISCLOSURE_LOOKBACK_DAYS:
            return 0.25
        return 0.05
    if days <= 3:
        return 1.0
    if days <= 7:
        return 0.7
    if days <= 30:
        return 0.4
    return 0.2


def _days_ago(published_at: Any, run_dt: datetime) -> int:
    published_dt = _parse_dt(published_at, fallback=run_dt.isoformat()) or run_dt
    return max(0, (run_dt.date() - published_dt.date()).days)


def _unique_points(values: Iterable[Any]) -> List[str]:
    result = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _as_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(str(value).replace("%", ""))
    except Exception:
        return 0.0


def _source_rank(source: str) -> float:
    ranks = {
        "cls_key": 1.2,
        "ths_global": 1.0,
        "em_global": 0.9,
        "em_breakfast": 0.6,
        "disclosure": 0.7,
        "macro_summary": 0.5,
    }
    return ranks.get(source, 0.7)
