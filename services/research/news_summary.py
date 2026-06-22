"""News summary service implementation for the skill-only architecture."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from core.logging import init_tool_logger
from configs.stock_pool import TRACKED_A_STOCKS
from utlity import is_etf_symbol, parse_symbol


load_dotenv()

logger = init_tool_logger("stock_news_search")

SYMBOL_NAME_MAP = {entry.symbol: entry.name for entry in TRACKED_A_STOCKS}
NEWS_BASE_DIR = Path(os.getenv("NEWS_BASE_DIR", "data/stock_info")).resolve()


def parse_datetime(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_today_date(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def load_json_payload(text: str, source: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and start < end:
            snippet = text[start : end + 1]
            try:
                return json.loads(snippet)
            except json.JSONDecodeError:
                logger.warning("无法解析 JSON 片段: %s", source)
                return None
    return None


def collect_news_items(stock_name: str, stock_code: str, diagnostics: List[str]) -> List[Dict[str, Any]]:
    target_dir = NEWS_BASE_DIR / f"{stock_name}_{stock_code}" / "news"
    if not target_dir.is_dir():
        diagnostics.append(f"未找到目录: {target_dir}")
        logger.warning("新闻目录不存在: %s", target_dir)
        return []

    audited_json = target_dir / "news_audited.json"
    if audited_json.exists():
        try:
            content = audited_json.read_text(encoding="utf-8")
            payload = json.loads(content)
            items = payload.get("news_items") or []
            if isinstance(items, list):
                return items
            diagnostics.append(f"主新闻文件结构异常: {audited_json}")
        except Exception as exc:
            diagnostics.append(f"读取主新闻文件失败 {audited_json.name}: {exc}")
    return []


def filter_news_before_today(
    items: List[Dict[str, Any]],
    today_time: datetime,
    diagnostics: List[str],
    days: int = 95,
) -> List[Dict[str, Any]]:
    start_time = today_time - timedelta(days=days)
    filtered: List[Dict[str, Any]] = []
    for item in items:
        dt_value = parse_datetime(item.get("datetime"))
        if dt_value is None:
            title = str(item.get("title", ""))
            if any(kw in title for kw in ["质押", "回购", "担保"]):
                continue
            diagnostics.append(f"跳过新闻（无法解析日期）: {item.get('title', '未知标题')}")
            continue
        if dt_value >= today_time or dt_value < start_time:
            continue
        item["_datetime_obj"] = dt_value
        filtered.append(item)
    filtered.sort(key=lambda x: x["_datetime_obj"], reverse=True)
    return filtered


def _deduplicate_recurring_events(items: List[Dict[str, Any]], diagnostics: List[str]) -> List[Dict[str, Any]]:
    keywords = ["质押", "回购", "担保"]
    to_remove_indices = set()

    for keyword in keywords:
        match_indices = []
        for idx, item in enumerate(items):
            title = str(item.get("title") or "")
            if keyword in title:
                match_indices.append(idx)
        if not match_indices:
            continue

        latest_idx = None
        latest_dt = None
        for idx in match_indices:
            item = items[idx]
            dt_value = item.get("_datetime_obj")
            if not isinstance(dt_value, datetime):
                dt_value = parse_datetime(item.get("datetime"))
            if dt_value is None:
                continue
            if latest_dt is None or dt_value > latest_dt:
                latest_dt = dt_value
                latest_idx = idx

        if latest_idx is None:
            latest_idx = match_indices[0]
        for idx in match_indices:
            if idx != latest_idx:
                to_remove_indices.add(idx)

    if not to_remove_indices:
        return items

    trimmed = []
    removed_count = 0
    for idx, item in enumerate(items):
        if idx in to_remove_indices:
            removed_count += 1
            continue
        trimmed.append(item)

    diagnostics.append(f"同质化公告过滤(质押/回购/担保): 移除 {removed_count} 条冗余信息")
    return trimmed


def _strip_internal_datetime(items: List[Dict[str, Any]]) -> None:
    for entry in items:
        entry.pop("_datetime_obj", None)


def _filter_low_impact_noise(items: List[Dict[str, Any]], diagnostics: List[str]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    dropped = 0
    for item in items:
        impact = str(item.get("impact_level", "")).strip().lower()
        sentiment = str(item.get("sentiment", "")).strip().lower()
        if impact == "low" and sentiment == "neutral":
            dropped += 1
            continue
        cleaned.append(item)
    if dropped:
        diagnostics.append(f"过滤低影响/中性新闻 {dropped} 条，保留 {len(cleaned)} 条")
    return cleaned


def _try_update_disclosures_for_stock(stock_name: str, stock_code: str, lookback_days: int = 365) -> None:
    from news.disclosures_builder import update_disclosures_for_stock

    update_disclosures_for_stock(stock_name, stock_code, lookback_days=lookback_days)


def search_stock_news(symbol: str, today_time: str) -> str:
    diagnostics: List[str] = []
    stock_code = symbol.strip()
    stock_name = SYMBOL_NAME_MAP.get(stock_code, stock_code)

    if is_etf_symbol(stock_code):
        payload = {
            "stock": f"{stock_name} ({stock_code})",
            "today": today_time,
            "news_items": [],
            "diagnostics": ["ETF/基金类标的不提供公告/新闻摘要，请选择股票标的。"],
        }
        logger.info("search_stock_news ETF 检测: %s", stock_code)
        return json.dumps(payload, ensure_ascii=False, indent=2)

    try:
        symbol_info = parse_symbol(stock_code)
        stock_name = symbol_info.stock_name or stock_name
    except Exception:
        pass

    logger.info("search_stock_news 请求: stock=%s(%s), today=%s", stock_name, stock_code, today_time)
    today_dt = parse_today_date(today_time)
    if today_dt is None:
        payload = {
            "error": f"无法解析 today_time: {today_time}",
            "hint": "只支持 YYYY-MM-DD 格式，例如 2025-11-07。",
        }
        logger.error("search_stock_news 日期解析失败: %s", today_time)
        return json.dumps(payload, ensure_ascii=False, indent=2)

    items = collect_news_items(stock_name, stock_code, diagnostics)
    if not items:
        try:
            _try_update_disclosures_for_stock(stock_name, stock_code, lookback_days=365)
            items = collect_news_items(stock_name, stock_code, diagnostics)
        except Exception as exc:
            diagnostics.append(f"公告构建失败: {exc}")
        payload = {
            "stock": f"{stock_name} ({stock_code})",
            "today": today_time,
            "news_items": [],
            "diagnostics": diagnostics or ["未找到任何可用的新闻文件。"],
        }
        logger.warning("search_stock_news 未找到新闻: stock=%s(%s)", stock_name, stock_code)
        return json.dumps(payload, ensure_ascii=False, indent=2)

    filtered = filter_news_before_today(items, today_dt, diagnostics)
    filtered = _deduplicate_recurring_events(filtered, diagnostics)
    filtered = _filter_low_impact_noise(filtered, diagnostics)
    _strip_internal_datetime(filtered)

    payload = {
        "stock": f"{stock_name} ({stock_code})",
        "today": today_time,
        "news_items": filtered,
        "diagnostics": diagnostics,
    }
    logger.info("search_stock_news 完成: items=%d, diagnostics=%d", len(filtered), len(diagnostics))
    return json.dumps(payload, ensure_ascii=False, indent=2)


__all__ = ["search_stock_news"]
