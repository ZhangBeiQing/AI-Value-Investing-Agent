"""Prepare shared audited announcement caches for historical backtests."""

from __future__ import annotations

import concurrent.futures
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from core.logging import get_logger
from news.disclosures_builder import (
    audit_news_json,
    index_path,
    load_index,
    news_json_path,
    update_disclosures_for_stock,
)
from services.backtest.experiment import BacktestExperiment
from services.selection_system.announcement_summary import (
    DEFAULT_AUDIT_MODEL,
    DEFAULT_DISCLOSURE_MODEL,
)
from shared_data_access import SharedDataAccess
from utlity import is_etf_symbol, parse_symbol


LOGGER = get_logger("BacktestAnnouncements")
CHECKPOINT_SCHEMA_VERSION = 1
CHECKPOINT_FILENAME = "announcement_preparation.json"
RESEARCH_NEWS_LOOKBACK_DAYS = 95


def _checkpoint_path(experiment: BacktestExperiment) -> Path:
    return experiment.root / "checkpoints" / CHECKPOINT_FILENAME


def _load_checkpoint(experiment: BacktestExperiment) -> dict[str, Any]:
    path = _checkpoint_path(experiment)
    if not path.exists():
        return {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "symbols": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("公告准备 checkpoint 无法读取，将重新生成: %s", exc)
        return {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "symbols": {},
        }
    if payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        return {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "symbols": {},
        }
    if not isinstance(payload.get("symbols"), dict):
        payload["symbols"] = {}
    return payload


def _save_checkpoint(
    experiment: BacktestExperiment,
    payload: dict[str, Any],
) -> Path:
    path = _checkpoint_path(experiment)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["schema_version"] = CHECKPOINT_SCHEMA_VERSION
    payload["updated_at"] = datetime.now().isoformat()
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _required_fetch_lookback_days(experiment: BacktestExperiment) -> int:
    """Fetch enough history for the first decision day's 95-day read window."""

    start = datetime.strptime(experiment.start_date, "%Y-%m-%d").date()
    earliest_needed = start - timedelta(days=RESEARCH_NEWS_LOOKBACK_DAYS)
    return max((date.today() - earliest_needed).days + 1, 365)


def _pending_audit_count(symbol: str) -> int:
    symbol_info = parse_symbol(symbol)
    return sum(
        1
        for meta in load_index(index_path(symbol_info)).values()
        if meta.summarized and not meta.audited
    )


def _news_item_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    items = payload.get("news_items") if isinstance(payload, dict) else None
    return len(items) if isinstance(items, list) else 0


def _prepare_one_symbol(
    experiment: BacktestExperiment,
    symbol: str,
    *,
    lookback_days: int,
) -> dict[str, Any]:
    symbol_info = parse_symbol(symbol)
    added = update_disclosures_for_stock(
        symbol_info,
        lookback_days=lookback_days,
        model=DEFAULT_DISCLOSURE_MODEL,
        data_access=SharedDataAccess(
            base_dir=experiment.context.source_data_root,
            logger=LOGGER,
        ),
    )
    audit_news_json(symbol_info, DEFAULT_AUDIT_MODEL)

    raw_news_path = news_json_path(symbol_info)
    audited_path = raw_news_path.with_name("news_audited.json")
    pending_count = _pending_audit_count(symbol)
    raw_count = _news_item_count(raw_news_path)
    audited_count = _news_item_count(audited_path)
    if pending_count:
        raise RuntimeError(f"仍有 {pending_count} 条公告摘要未完成审计")
    if raw_count and not audited_path.exists():
        raise RuntimeError("news.json 已有公告，但 news_audited.json 尚未生成")

    return {
        "status": "success",
        "prepared_at": datetime.now().isoformat(),
        "lookback_days": lookback_days,
        "added_summary_count": int(added or 0),
        "raw_news_item_count": raw_count,
        "audited_news_item_count": audited_count,
    }


def prepare_backtest_announcements(
    experiment: BacktestExperiment,
    symbols: Iterable[str],
    *,
    max_workers: int = 4,
) -> dict[str, Any]:
    """Incrementally prepare each symbol's shared audited announcement cache once.

    The external fetch always reaches the real current cache. Historical causality
    is enforced later when ``search_stock_news`` filters items by decision date.
    """

    checkpoint = _load_checkpoint(experiment)
    symbol_states = checkpoint.setdefault("symbols", {})
    requested = list(dict.fromkeys(str(symbol).strip() for symbol in symbols if symbol))
    skipped_etfs = [symbol for symbol in requested if is_etf_symbol(symbol)]
    pending = [
        symbol
        for symbol in requested
        if symbol not in skipped_etfs
        and (symbol_states.get(symbol) or {}).get("status") != "success"
    ]
    cached_count = sum(
        1
        for symbol in requested
        if symbol not in skipped_etfs
        and (symbol_states.get(symbol) or {}).get("status") == "success"
    )

    if experiment.network_mode == "disabled":
        return {
            "status": "skipped",
            "reason": "network_mode=disabled，仅使用已有 news_audited.json",
            "requested_symbol_count": len(requested),
            "prepared_symbol_count": 0,
            "cached_symbol_count": cached_count,
            "failed_symbols": [],
            "skipped_etfs": skipped_etfs,
            "checkpoint": str(_checkpoint_path(experiment)),
        }

    lookback_days = _required_fetch_lookback_days(experiment)
    failures: list[dict[str, str]] = []
    prepared_count = 0
    worker_count = max(1, min(int(max_workers or 1), len(pending) or 1))
    if pending:
        LOGGER.info(
            "开始补齐回测公告摘要与审计缓存: experiment=%s symbols=%d lookback=%d workers=%d",
            experiment.experiment_id,
            len(pending),
            lookback_days,
            worker_count,
        )
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count,
        ) as executor:
            future_to_symbol = {
                executor.submit(
                    _prepare_one_symbol,
                    experiment,
                    symbol,
                    lookback_days=lookback_days,
                ): symbol
                for symbol in pending
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    symbol_states[symbol] = future.result()
                    prepared_count += 1
                except Exception as exc:
                    message = str(exc)
                    failures.append({"symbol": symbol, "error": message})
                    symbol_states[symbol] = {
                        "status": "failed",
                        "attempted_at": datetime.now().isoformat(),
                        "lookback_days": lookback_days,
                        "error": message,
                    }
                    LOGGER.warning(
                        "回测公告准备失败，将在后续日期重试: symbol=%s error=%s",
                        symbol,
                        message,
                    )

    checkpoint_path = _save_checkpoint(experiment, checkpoint)
    return {
        "status": "warning" if failures else "success",
        "requested_symbol_count": len(requested),
        "prepared_symbol_count": prepared_count,
        "cached_symbol_count": cached_count,
        "failed_symbols": failures,
        "skipped_etfs": skipped_etfs,
        "lookback_days": lookback_days,
        "checkpoint": str(checkpoint_path),
    }


__all__ = ["prepare_backtest_announcements"]
