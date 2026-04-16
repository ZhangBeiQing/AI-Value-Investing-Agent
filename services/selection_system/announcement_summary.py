"""Build recent company-announcement inputs for the selection system."""

from __future__ import annotations

import concurrent.futures
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping

from core.logging import get_logger
from news.disclosures_builder import audit_news_json, update_disclosures_for_stock
from shared_data_access import SharedDataAccess
from utlity.stock_utils import get_stock_data_dir

from .master_universe import load_master_universe
from .paths import SelectionSystemPaths
from .store import load_json_file, save_json_file
from .symbols import build_symbol_info_map


LOGGER = get_logger("SelectionAnnouncements")
DEFAULT_DISCLOSURE_MODEL = "qwen-doc-turbo"
DEFAULT_AUDIT_MODEL = "deepseek-v3.2-exp"
DEFAULT_ANNOUNCEMENT_LOOKBACK_DAYS = 30
AUDITED_PREPARE_LOOKBACK_DAYS = 120
MAX_PREPARE_WORKERS = 8
RECURRING_EVENT_KEYWORDS = ("质押", "回购", "担保")


def build_recent_company_announcements(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    lookback_days: int = DEFAULT_ANNOUNCEMENT_LOOKBACK_DAYS,
    max_items_per_symbol: int = 6,
    refresh_missing: bool = True,
    force_refresh_disclosures: bool = False,
) -> Dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    payload = collect_recent_company_announcements(
        run_date,
        base_dir=base_dir,
        lookback_days=lookback_days,
        max_items_per_symbol=max_items_per_symbol,
        refresh_missing=refresh_missing,
        force_refresh_disclosures=force_refresh_disclosures,
    )
    save_json_file(paths.run_recent_company_announcements_path(run_date), payload)
    LOGGER.info("最近公告摘要已写入: %s", paths.run_recent_company_announcements_path(run_date))
    return {
        "recent_company_announcements": paths.run_recent_company_announcements_path(run_date),
    }


def collect_recent_company_announcements(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    lookback_days: int = DEFAULT_ANNOUNCEMENT_LOOKBACK_DAYS,
    max_items_per_symbol: int = 6,
    refresh_missing: bool = True,
    force_refresh_disclosures: bool = False,
) -> Dict[str, Any]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    universe = load_master_universe(paths)
    symbol_info_map = build_symbol_info_map(universe)
    run_dt = datetime.strptime(run_date, "%Y-%m-%d")
    window_start = run_dt - timedelta(days=max(int(lookback_days), 1) - 1)
    window_end = run_dt + timedelta(days=1)

    _ensure_news_summaries(
        symbol_info_map,
        base_dir=base_dir,
        lookback_days=lookback_days,
        refresh_missing=refresh_missing,
        force_refresh_disclosures=force_refresh_disclosures,
    )

    items: list[dict[str, Any]] = []
    for stock in universe.stocks:
        symbol = stock.symbol
        symbol_info = symbol_info_map[symbol]
        news_path = _audited_news_json_path(symbol_info, base_dir=base_dir)
        news_payload = load_json_file(news_path, default={}) or {}
        news_items = news_payload.get("news_items") if isinstance(news_payload, Mapping) else []
        rows = _extract_recent_rows(
            news_items,
            symbol=symbol,
            stock_name=stock.name,
            window_start=window_start,
            window_end=window_end,
            max_items=max_items_per_symbol,
        )
        if rows:
            items.extend(rows)

    items.sort(
        key=lambda item: (str(item.get("published_at") or ""), str(item.get("symbol") or "")),
        reverse=True,
    )
    payload = {
        "schema_version": 2,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "lookback_days": lookback_days,
        "source": "news_audited.json.summary",
        "summary": {
            "universe_symbol_count": len(universe.stocks),
            "covered_symbol_count": len({item["symbol"] for item in items}),
            "announcement_count": len(items),
        },
        "items": items,
    }
    return payload


def _ensure_news_summaries(
    symbol_info_map: Mapping[str, Any],
    *,
    base_dir: str | Path,
    lookback_days: int,
    refresh_missing: bool,
    force_refresh_disclosures: bool,
) -> None:
    tasks: list[tuple[str, Any]] = list(symbol_info_map.items())

    prepare_lookback_days = AUDITED_PREPARE_LOOKBACK_DAYS
    worker_count = max(1, min(MAX_PREPARE_WORKERS, len(tasks)))
    LOGGER.info(
        "开始为股票宇宙全量更新公告摘要与审计结果: symbols=%d audited_lookback_days=%d output_lookback_days=%d workers=%d",
        len(tasks),
        prepare_lookback_days,
        int(lookback_days),
        worker_count,
    )

    def _run_one(symbol: str, symbol_info: Any) -> tuple[str, int | None, str | None]:
        try:
            added = update_disclosures_for_stock(
                symbol_info,
                lookback_days=prepare_lookback_days,
                model=DEFAULT_DISCLOSURE_MODEL,
                data_access=SharedDataAccess(base_dir=base_dir, logger=LOGGER),
            )
            audit_news_json(symbol_info, DEFAULT_AUDIT_MODEL)
            return symbol, added, None
        except Exception as exc:  # pragma: no cover - network/LLM defensive path
            return symbol, None, str(exc)

    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_symbol = {
            executor.submit(_run_one, symbol, symbol_info): symbol
            for symbol, symbol_info in tasks
        }
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            result_symbol, added, error = future.result()
            if error is not None:
                failures += 1
                LOGGER.warning("公告摘要准备失败: symbol=%s error=%s", result_symbol, error)
                continue
            LOGGER.info("公告摘要准备完成: symbol=%s added=%s", symbol, added)

    LOGGER.info("股票宇宙公告摘要补齐完成: total=%d failures=%d", len(tasks), failures)


def _news_json_path(symbol_info: Any, *, base_dir: str | Path) -> Path:
    return get_stock_data_dir(symbol_info, base_dir=base_dir) / "news" / "news.json"


def _audited_news_json_path(symbol_info: Any, *, base_dir: str | Path) -> Path:
    return get_stock_data_dir(symbol_info, base_dir=base_dir) / "news" / "news_audited.json"


def _extract_recent_rows(
    news_items: Any,
    *,
    symbol: str,
    stock_name: str,
    window_start: datetime,
    window_end: datetime,
    max_items: int,
) -> list[Dict[str, Any]]:
    if not isinstance(news_items, list) or not news_items:
        return []

    filtered: list[dict[str, Any]] = []
    for item in news_items:
        if not isinstance(item, Mapping):
            continue
        published_dt = _parse_item_datetime(item.get("datetime") or item.get("date"))
        if published_dt is None:
            continue
        if not (window_start <= published_dt < window_end):
            continue
        summary = _normalize_summary(item.get("summary"))
        if not summary:
            continue
        filtered.append(
            {
                "symbol": symbol,
                "stock_name": stock_name,
                "published_at": published_dt.strftime("%Y-%m-%d"),
                "summary": summary,
                "_datetime_obj": published_dt,
                "_impact_level": str(item.get("impact_level") or "").strip(),
                "_sentiment": str(item.get("sentiment") or "").strip(),
                "_title": str(item.get("title") or "").strip(),
            }
        )
    filtered.sort(key=lambda item: item["_datetime_obj"], reverse=True)
    filtered = _deduplicate_recurring_events(filtered)
    filtered = _filter_low_impact_noise(filtered)

    items: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for item in filtered:
        dedupe_key = (str(item.get("published_at") or ""), str(item.get("summary") or ""))
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        items.append(
            {
                "symbol": symbol,
                "stock_name": stock_name,
                "published_at": str(item.get("published_at") or ""),
                "summary": str(item.get("summary") or ""),
            }
        )
        if len(items) >= max(max_items, 1):
            break
    return items


def _deduplicate_recurring_events(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    to_remove_indices: set[int] = set()
    for keyword in RECURRING_EVENT_KEYWORDS:
        match_indices = [idx for idx, item in enumerate(items) if keyword in str(item.get("_title") or "")]
        if not match_indices:
            continue
        latest_idx = match_indices[0]
        latest_dt = items[latest_idx].get("_datetime_obj")
        for idx in match_indices[1:]:
            dt_value = items[idx].get("_datetime_obj")
            if isinstance(dt_value, datetime) and isinstance(latest_dt, datetime) and dt_value > latest_dt:
                latest_dt = dt_value
                latest_idx = idx
        for idx in match_indices:
            if idx != latest_idx:
                to_remove_indices.add(idx)
    if not to_remove_indices:
        return items
    return [item for idx, item in enumerate(items) if idx not in to_remove_indices]


def _filter_low_impact_noise(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    for item in items:
        impact = str(item.get("_impact_level") or "").strip().lower()
        sentiment = str(item.get("_sentiment") or "").strip().lower()
        if impact == "low" and sentiment == "neutral":
            continue
        kept.append(item)
    return kept


def _parse_item_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    matched = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
    if not matched:
        return None
    try:
        year, month, day = (int(part) for part in matched.groups())
        return datetime(year, month, day)
    except ValueError:
        return None


def _normalize_summary(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"\s+", " ", text)


__all__ = [
    "build_recent_company_announcements",
    "collect_recent_company_announcements",
]
