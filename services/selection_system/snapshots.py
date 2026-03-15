"""Snapshot builders for the selection system."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from core.logging import get_logger
from services.snapshot.basic_snapshot import (
    DEFAULT_PRICE_LOOKBACK_DAYS,
    MIN_PRICE_LOOKBACK_DAYS,
    BasicStockInfoService,
    _load_cached_stocks,
    _parse_analysis_time,
)
from utlity import get_latest_trading_day, resolve_base_dir

from .models import MasterUniverseDocument
from .store import load_json_file, save_json_file
from .symbols import build_symbol_info_map


LOGGER = get_logger("SelectionSnapshot")


def build_universe_snapshot(
    universe: MasterUniverseDocument,
    run_date: str,
    *,
    base_dir: str | Path | None = None,
    max_workers: int = 6,
    price_lookback_days: int = DEFAULT_PRICE_LOOKBACK_DAYS,
    force_refresh: bool = False,
    force_refresh_financials: bool = False,
    cache_only: bool = False,
) -> Dict[str, Any]:
    analysis_dt = _parse_analysis_time(run_date)
    analysis_date = analysis_dt.date() if analysis_dt is not None else datetime.now().date()
    resolved_base_dir = resolve_base_dir(base_dir)

    symbol_infos = build_symbol_info_map(universe)
    target_dates: Dict[str, date] = {}
    valid_symbols: List[str] = []
    errors: Dict[str, str] = {}

    for stock in universe.stocks:
        info = symbol_infos[stock.symbol]
        try:
            target_dates[stock.symbol] = get_latest_trading_day(analysis_date, info.calendar, LOGGER)
            valid_symbols.append(stock.symbol)
        except Exception as exc:
            errors[stock.symbol] = f"无法确定交易日: {exc}"

    cached_stocks: Dict[str, Dict[str, Any]] = {}
    missing_symbols: List[str] = list(valid_symbols)
    if valid_symbols and not force_refresh:
        cached_stocks, missing_symbols = _load_cached_stocks(
            valid_symbols,
            resolved_base_dir,
            target_dates,
        )
        if missing_symbols:
            fallback_cached, still_missing = _load_latest_available_cached_stocks(
                missing_symbols,
                resolved_base_dir,
                target_dates,
            )
            cached_stocks.update(fallback_cached)
            missing_symbols = still_missing

    computed_stocks: Dict[str, Dict[str, Any]] = {}
    if missing_symbols and not cache_only:
        service = BasicStockInfoService(
            base_dir=resolved_base_dir,
            max_workers=max_workers,
            price_lookback_days=max(price_lookback_days, MIN_PRICE_LOOKBACK_DAYS),
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
            analysis_datetime=analysis_dt,
            symbol_infos=symbol_infos,
            target_dates=target_dates,
        )
        payload = service.build_payload_from_normalized(missing_symbols)
        computed_stocks = payload.get("stocks", {})
        errors.update(payload.get("errors", {}))
    elif missing_symbols and cache_only:
        for symbol in missing_symbols:
            errors[symbol] = "cache_only 模式下未找到 basic_info_cache"

    combined: Dict[str, Dict[str, Any]] = {}
    combined.update(cached_stocks)
    combined.update(computed_stocks)

    simplified = []
    for stock in universe.stocks:
        payload = combined.get(stock.symbol, {})
        simplified.append(
            {
                "symbol": stock.symbol,
                "name": stock.name,
                "sector": stock.sector,
                "industry": stock.industry,
                "snapshot_date": target_dates.get(stock.symbol, analysis_date).strftime("%Y-%m-%d"),
                "latest_price": payload.get("latest_price"),
                "daily_change_pct": payload.get("daily_change_pct"),
                "latest_volume": payload.get("latest_volume"),
                "market_cap": payload.get("market_cap"),
                "pe_ttm": payload.get("pe_ttm"),
                "pb": payload.get("pb"),
                "ps": payload.get("ps"),
                "pe_2y_percentile": payload.get("pe_2y_percentile"),
                "pe_current_vs_median": payload.get("pe_current_vs_median"),
                "return_3m": payload.get("return_3m"),
                "return_6m": payload.get("return_6m"),
                "return_1y": payload.get("return_1y"),
                "max_drawdown_3m": payload.get("max_drawdown_3m"),
                "revenue_growth_yoy": payload.get("revenue_growth_yoy"),
                "net_income_growth_yoy": payload.get("net_income_growth_yoy"),
                "gross_margin": payload.get("gross_margin"),
                "net_profit_margin": payload.get("net_profit_margin"),
                "roe": payload.get("roe"),
                "turnover_rate": payload.get("turnover_rate"),
                "avg_turnover_30d": payload.get("avg_turnover_30d"),
                "liquidity_score": payload.get("liquidity_score"),
                "source": "basic_info_cache",
                "missing": not bool(payload),
            }
        )

    result: Dict[str, Any] = {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "symbols": simplified,
    }
    if errors:
        result["errors"] = errors
    return result


def snapshot_map(snapshot_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {item["symbol"]: item for item in snapshot_payload.get("symbols", []) if isinstance(item, dict)}


def persist_snapshot(path: Path, snapshot_payload: Dict[str, Any]) -> Path:
    LOGGER.info("写入简化 snapshot: %s", path)
    return save_json_file(path, snapshot_payload)


def _load_latest_available_cached_stocks(
    symbols: List[str],
    base_dir: Path,
    target_dates: Dict[str, date],
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    cached: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    snapshot_root = resolve_base_dir(base_dir) / "basic_info_cache"
    for symbol in symbols:
        file_path = snapshot_root / f"basic_info_{symbol}.json"
        if not file_path.exists():
            missing.append(symbol)
            continue
        payload = load_json_file(file_path, default={})
        series = payload.get("Time Series (Daily)", {}) if isinstance(payload, dict) else {}
        if not isinstance(series, dict) or not series:
            missing.append(symbol)
            continue
        target_date = target_dates.get(symbol)
        selected_key = None
        if target_date is not None:
            for key in sorted(series.keys(), reverse=True):
                try:
                    key_date = datetime.strptime(key, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if key_date <= target_date:
                    selected_key = key
                    break
        if selected_key is None:
            selected_key = sorted(series.keys(), reverse=True)[0]
        record = series.get(selected_key)
        if isinstance(record, dict):
            cached[symbol] = record
        else:
            missing.append(symbol)
    return cached, missing
