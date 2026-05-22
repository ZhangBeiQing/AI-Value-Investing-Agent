"""Build factor-store views from existing local datasets.

This module builds two complementary views:

- by_symbol: one file per symbol, tracking factor history over time.
- by_date: one file per date, comparing all symbols on the same as-of date.

The factor store is a derived feature layer. It reads existing raw caches and
analysis artifacts, but does not fetch external data directly.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from core.logging import get_logger
from indicator_library.calculators.momentum import macd_indicator, rsi_indicator
from services.selection_system.factor_registry import factor_registry_payload
from services.selection_system.master_universe import load_master_universe
from services.selection_system.paths import SelectionSystemPaths
from services.selection_system.store import save_json_file
from services.snapshot.basic_snapshot import BasicStockInfoService
from shared_data_access.cache_registry import CacheKind, build_cache_dir
from shared_data_access.chip_distribution import build_latest_chip_distribution_from_price_frame
from shared_data_access.data_access import SharedDataAccess
from shared_data_access.exceptions import CacheIntegrityError, DataUnavailableError
from shared_data_access.models import PreparedData, PriceDataBundle
from utlity.stock_utils import SymbolFormatError, get_stock_data_dir, parse_symbol


LOGGER = get_logger("SelectionFactorStore")
QUIET_DATA_LOGGER = logging.getLogger("SelectionFactorStoreDataAccess")
QUIET_DATA_LOGGER.setLevel(logging.ERROR)

DEFAULT_MAX_STALENESS_DAYS = 10
PARQUET_ENGINE = "pyarrow"

@dataclass(frozen=True)
class FactorStoreConfig:
    """Configuration for building a factor snapshot."""

    max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS
    write_parquet: bool = True
    write_csv: bool = True
    max_workers: int = 1


def build_factor_store_for_date(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    config: FactorStoreConfig | None = None,
) -> dict[str, Path]:
    """Build by-symbol history rows and a by-date factor snapshot."""

    config = config or FactorStoreConfig()
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    universe = load_master_universe(paths)
    rows: list[dict[str, Any]] = []
    max_workers = max(int(config.max_workers or 1), 1)
    if max_workers <= 1 or len(universe.stocks) <= 1:
        for stock in universe.stocks:
            row = _build_stock_factor_row(stock, run_date, paths.base_dir, config)
            if row is not None:
                rows.append(row)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_build_stock_factor_row, stock, run_date, paths.base_dir, config): stock.symbol
                for stock in universe.stocks
            }
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    row = future.result()
                except Exception as exc:  # pragma: no cover - defensive aggregation path
                    LOGGER.warning("因子行构建失败: symbol=%s error=%s", symbol, exc)
                    continue
                if row is not None:
                    rows.append(row)

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = _normalize_numeric_columns(frame)
        frame = _apply_factor_sanity_rules(frame)
        frame = frame.sort_values(["data_quality_score", "symbol"], ascending=[False, True]).reset_index(drop=True)
        frame.insert(0, "factor_rank", range(1, len(frame) + 1))

    factor_dir = paths.base_dir / "factor_store"
    by_symbol_dir = factor_dir / "by_symbol"
    by_date_dir = factor_dir / "by_date"
    by_symbol_dir.mkdir(parents=True, exist_ok=True)
    by_date_dir.mkdir(parents=True, exist_ok=True)

    written_symbol_paths: list[str] = []
    for row in frame.to_dict(orient="records") if not frame.empty else []:
        symbol_path = _write_by_symbol_row(row, by_symbol_dir, config=config)
        if symbol_path is not None:
            written_symbol_paths.append(str(symbol_path))

    date_stem = run_date
    by_date_csv = by_date_dir / f"{date_stem}.csv"
    by_date_parquet = by_date_dir / f"{date_stem}.parquet"
    if config.write_csv:
        frame.to_csv(by_date_csv, index=False)
    if config.write_parquet:
        _write_parquet(frame, by_date_parquet)

    selection_csv = paths.run_dir(run_date) / "12_factor_snapshot.csv"
    selection_json = paths.run_dir(run_date) / "12_factor_snapshot.json"
    frame.to_csv(selection_csv, index=False)
    save_json_file(selection_json, _factor_snapshot_payload(run_date, frame, config=config))

    manifest_path = factor_dir / "manifest.json"
    registry_path = factor_dir / "factor_registry.json"
    save_json_file(registry_path, factor_registry_payload())
    save_json_file(
        manifest_path,
        {
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "latest_run_date": run_date,
            "factor_registry": str(registry_path),
            "row_count": int(len(frame)),
            "by_date_csv": str(by_date_csv),
            "by_date_parquet": str(by_date_parquet) if config.write_parquet else None,
            "selection_snapshot_csv": str(selection_csv),
            "selection_snapshot_json": str(selection_json),
            "written_symbol_count": len(written_symbol_paths),
        },
    )
    LOGGER.info("因子库快照已生成: run_date=%s rows=%d", run_date, len(frame))
    return {
        "factor_store_manifest": manifest_path,
        "factor_by_date_csv": by_date_csv,
        "factor_by_date_parquet": by_date_parquet,
        "factor_snapshot_csv": selection_csv,
        "factor_snapshot_json": selection_json,
        "factor_registry": registry_path,
    }


def _build_stock_factor_row(stock: Any, run_date: str, base_dir: Path, config: FactorStoreConfig) -> dict[str, Any] | None:
    try:
        return build_symbol_factor_row(
            stock.symbol,
            run_date,
            stock_name=stock.name,
            sector=stock.sector,
            industry=stock.industry,
            stock_type=stock.stock_type,
            base_dir=base_dir,
            max_staleness_days=config.max_staleness_days,
        )
    except Exception as exc:  # pragma: no cover - defensive aggregation path
        LOGGER.warning("因子行构建失败: symbol=%s error=%s", stock.symbol, exc)
        return None


def build_symbol_factor_row(
    symbol: str,
    run_date: str,
    *,
    stock_name: str = "",
    sector: str = "",
    industry: str = "",
    stock_type: str = "growth",
    base_dir: str | Path = "data",
    max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS,
) -> dict[str, Any] | None:
    """Build one symbol's factor row for a date from local artifacts."""

    try:
        symbol_info = parse_symbol(symbol)
    except SymbolFormatError as exc:
        LOGGER.warning("跳过非法 symbol: %s error=%s", symbol, exc)
        return None

    base_path = Path(base_dir)
    stock_root = get_stock_data_dir(symbol_info, base_dir=base_path)
    basic_row = _basic_info_row(symbol, run_date, base_path, max_staleness_days=max_staleness_days)
    if basic_row is None:
        basic_row = _computed_basic_info_row(symbol_info, run_date, stock_root, base_path)
    if basic_row is None:
        return None

    row: dict[str, Any] = {
        "date": run_date,
        "symbol": symbol,
        "stock_name": stock_name or symbol_info.stock_name or basic_row.get("stock_name") or symbol,
        "sector": sector,
        "industry": industry,
        "stock_type": stock_type or "growth",
    }
    row.update(basic_row)

    price_features = _price_features(stock_root / "prices" / "price.csv", run_date)
    row.update(price_features)

    technical_features = _technical_features(stock_root / "analysis", run_date)
    row.update(technical_features)

    valuation_features = _valuation_distribution_features(stock_root / "pe_pb_analysis", run_date, row.get("latest_price"))
    row.update(valuation_features)
    if _needs_pb_history_fallback(row):
        fallback_pb = _fallback_pb_history_features(symbol_info, stock_root, base_path, run_date, row.get("pb"))
        if fallback_pb.get("pb_3_5y_percentile") is not None:
            row.update(fallback_pb)
    row.update(_fallback_valuation_features(row))

    chip_dir = build_cache_dir(
        symbol_info,
        CacheKind.CHIP_DISTRIBUTION,
        base_dir=base_path,
        ensure=False,
    )
    chip_features = _chip_distribution_features(chip_dir / "chip_distribution.csv", run_date, row.get("close") or row.get("latest_price"))
    if not chip_features:
        chip_features = _local_chip_distribution_features(
            stock_root / "prices" / "price.csv",
            run_date,
            row.get("close") or row.get("latest_price"),
        )
    row.update(chip_features)

    row["data_quality_score"] = _data_quality_score(row)
    return row


def _basic_info_row(
    symbol: str,
    run_date: str,
    base_dir: Path,
    *,
    max_staleness_days: int,
) -> dict[str, Any] | None:
    path = base_dir / "basic_info_cache" / f"basic_info_{symbol.replace('/', '_')}.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("读取 basic_info 失败: %s error=%s", path, exc)
        return None
    series = payload.get("Time Series (Daily)") or {}
    if not isinstance(series, Mapping):
        return None
    asof = pd.Timestamp(run_date).normalize()
    candidates = []
    for date_key, item in series.items():
        if not isinstance(item, Mapping):
            continue
        dt = pd.to_datetime(date_key, errors="coerce")
        if pd.isna(dt):
            continue
        dt = pd.Timestamp(dt).normalize()
        if dt <= asof:
            candidates.append((dt, dict(item)))
    if not candidates:
        return None
    snapshot_date, item = max(candidates, key=lambda value: value[0])
    if (asof - snapshot_date).days > max_staleness_days:
        return None
    item["basic_info_asof_date"] = snapshot_date.strftime("%Y-%m-%d")
    item["amount"] = item.get("latest_volume")
    item["amount_basic_info"] = item.get("latest_volume")
    item["daily_change_pct_num"] = _parse_percent(item.get("daily_change_pct"))
    return item


def _computed_basic_info_row(
    symbol_info: Any,
    run_date: str,
    stock_root: Path,
    base_dir: Path,
) -> dict[str, Any] | None:
    """Build a basic_info-compatible row from local caches for historical dates."""

    asof = pd.Timestamp(run_date).to_pydatetime()
    price_df = _read_price_frame(stock_root / "prices" / "price.csv", run_date)
    if price_df.empty:
        return None

    service = BasicStockInfoService(
        base_dir=base_dir,
        max_workers=1,
        price_lookback_days=1800,
        force_refresh=False,
        force_refresh_financials=False,
        skip_price_refresh=True,
        skip_financial_refresh=True,
        analysis_datetime=asof,
    )
    access = SharedDataAccess(
        base_dir=base_dir,
        price_lookback_days=1800,
        logger=QUIET_DATA_LOGGER,
    )

    is_etf = symbol_info.is_cn_market() and symbol_info.code.startswith(("51", "58", "15", "16", "50", "53"))
    is_index = symbol_info.market == "CN_INDEX"
    try:
        if is_etf or is_index:
            financials = access._load_financial_bundle(symbol_info, asof)  # type: ignore[attr-defined]
            share_info = access._load_share_info(symbol_info, asof)  # type: ignore[attr-defined]
        else:
            financials = access._load_financial_bundle(symbol_info, asof)  # type: ignore[attr-defined]
            share_info = access._load_share_info(symbol_info, asof)  # type: ignore[attr-defined]
    except (CacheIntegrityError, DataUnavailableError, ValueError) as exc:
        LOGGER.debug("历史本地 basic_info 组装失败: symbol=%s date=%s error=%s", symbol_info.symbol, run_date, exc)
        return None

    dataset = PreparedData(
        symbolInfo=symbol_info,
        as_of=asof,
        financials=financials,
        prices=PriceDataBundle(
            frame=price_df,
            start=price_df.index.min().to_pydatetime(),
            end=asof,
            source_path=stock_root / "prices" / "price.csv",
        ),
        share_info=share_info,
    )

    price_df = service._normalize_price_frame(dataset.prices.frame)  # type: ignore[attr-defined]
    indicator_price_df = service._build_indicator_gateway_frame(symbol_info, price_df)  # type: ignore[attr-defined]
    latest_price = service._latest_close(symbol_info.symbol, price_df)  # type: ignore[attr-defined]
    latest_volume = service._latest_volume(symbol_info.symbol, price_df)  # type: ignore[attr-defined]

    payload: dict[str, Any] = {
        "stock_name": symbol_info.stock_name,
        "latest_price": latest_price,
        "daily_change_pct": service._daily_change_pct(price_df),  # type: ignore[attr-defined]
        "latest_volume": latest_volume,
        "basic_info_asof_date": run_date,
        "amount": latest_volume,
        "amount_basic_info": latest_volume,
        "daily_change_pct_num": _parse_percent(service._daily_change_pct(price_df)),  # type: ignore[attr-defined]
    }

    if not is_etf and not is_index:
        try:
            valuation = service._compute_valuation_fields(symbol_info, dataset, price_df)  # type: ignore[attr-defined]
            reference_date = pd.Timestamp(run_date).date()
            history_stats = service._compute_pe_history(  # type: ignore[attr-defined]
                valuation["pe_history"],
                reference_date,
                valuation.get("pe_for_stats"),
            )
            pb_history_stats = _compute_pb_history_stats(
                financials.balance_sheet,
                price_df,
                share_info.total_shares,
                run_date,
                valuation.get("pb"),
            )
            financial_metrics = service._compute_financial_metrics(financials.financial_abstract)  # type: ignore[attr-defined]
            liquidity = service._compute_liquidity_metrics(symbol_info, indicator_price_df, valuation)  # type: ignore[attr-defined]
            payload.update(
                {
                    "latest_price": valuation.get("latest_price"),
                    "latest_volume": valuation.get("latest_volume"),
                    "pe_ttm": valuation.get("pe_ttm"),
                    "pb": valuation.get("pb"),
                    "ps": valuation.get("ps"),
                    "market_cap": valuation.get("market_cap"),
                    **history_stats,
                    **pb_history_stats,
                    **financial_metrics,
                    **liquidity,
                }
            )
        except Exception as exc:
            LOGGER.debug("历史本地估值/财务指标计算失败: symbol=%s date=%s error=%s", symbol_info.symbol, run_date, exc)
            return None
    else:
        liquidity = {"liquidity_score": None}

    risk_metrics = service._compute_risk_metrics(symbol_info, indicator_price_df)  # type: ignore[attr-defined]
    payload.update(risk_metrics)
    if is_etf or is_index:
        payload.update(liquidity)
    if payload.get("latest_volume") is not None:
        payload["amount"] = payload.get("latest_volume")
        payload["amount_basic_info"] = payload.get("latest_volume")
    return payload


def _compute_pb_history_stats(
    balance_sheet: pd.DataFrame,
    price_df: pd.DataFrame,
    total_shares: float | None,
    run_date: str,
    current_pb: Any,
) -> dict[str, Any]:
    default = {
        "pb_3_5y_median": None,
        "pb_3_5y_percentile": None,
        "pb_3_5y_std": None,
        "pb_current_vs_median": None,
        "pb_3_5y_sample_count": None,
    }
    if balance_sheet.empty or price_df.empty or not total_shares:
        return default
    work = balance_sheet.copy()
    notice_col = _first_existing_column(work, ("NOTICE_DATE", "公告日期", "REPORT_DATE"))
    if notice_col is None or "REPORT_DATE" not in work.columns:
        return default
    work[notice_col] = pd.to_datetime(work[notice_col], errors="coerce")
    work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
    work = work.dropna(subset=[notice_col, "REPORT_DATE"])
    asof = pd.Timestamp(run_date).normalize()
    window_start = asof - pd.Timedelta(days=int(365 * 3.5))
    work = work[work[notice_col] <= asof]
    if work.empty:
        return default

    close = pd.to_numeric(price_df.get("收盘"), errors="coerce")
    price_series = pd.Series(close.values, index=pd.to_datetime(price_df.index, errors="coerce")).dropna().sort_index()
    if price_series.empty:
        return default

    current_value = _safe_float(current_pb)
    equity_events: list[tuple[pd.Timestamp, float]] = []
    for _, item in work.sort_values(notice_col).iterrows():
        equity_value = None
        for field in ("TOTAL_PARENT_EQUITY", "TOTAL_EQUITY"):
            value = _safe_float(item.get(field))
            if value is not None and value > 0:
                equity_value = value
                break
        if equity_value is None:
            continue
        equity_events.append((pd.Timestamp(item[notice_col]).normalize(), equity_value))
    if not equity_events:
        return default

    equity_series = pd.Series(
        [value for _, value in equity_events],
        index=[date for date, _ in equity_events],
        dtype=float,
    ).sort_index()
    equity_series = equity_series[~equity_series.index.duplicated(keep="last")]
    daily_index = price_series.index.union(equity_series.index).sort_values()
    daily_equity = equity_series.reindex(daily_index).ffill().reindex(price_series.index)
    pb_series = price_series * float(total_shares) / daily_equity.replace(0, np.nan)
    pb_series = pb_series.replace([np.inf, -np.inf], np.nan).dropna()
    latest_pb_candidates = pb_series[pb_series.index <= asof]
    local_current_pb = _safe_float(latest_pb_candidates.iloc[-1]) if not latest_pb_candidates.empty else None
    if local_current_pb is not None and local_current_pb > 0:
        current_value = local_current_pb
    pb_series = pb_series[(pb_series.index >= window_start) & (pb_series.index <= asof)]
    pb_series = pb_series[pb_series > 0]
    if pb_series.empty:
        return default

    values = pb_series.to_numpy(dtype=float)
    median = float(np.median(values))
    current_positive = current_value is not None and current_value > 0
    return {
        "pb_3_5y_median": median,
        "pb_3_5y_percentile": float((values <= current_value).mean()) if current_positive else None,
        "pb_3_5y_std": float(np.std(values, ddof=0)),
        "pb_current_vs_median": current_value / median - 1.0 if current_positive and median > 0 else None,
        "pb_3_5y_sample_count": int(len(values)),
        "pb": current_value if current_positive else current_pb,
    }


def _fallback_pb_history_features(
    symbol_info: Any,
    stock_root: Path,
    base_dir: Path,
    run_date: str,
    current_pb: Any,
) -> dict[str, Any]:
    price_df = _read_price_frame(stock_root / "prices" / "price.csv", run_date)
    if price_df.empty:
        return {}
    access = SharedDataAccess(
        base_dir=base_dir,
        price_lookback_days=1800,
        logger=QUIET_DATA_LOGGER,
    )
    try:
        asof = pd.Timestamp(run_date).to_pydatetime()
        financials = access._load_financial_bundle(symbol_info, asof)  # type: ignore[attr-defined]
        share_info = access._load_share_info(symbol_info, asof)  # type: ignore[attr-defined]
    except (CacheIntegrityError, DataUnavailableError, ValueError) as exc:
        LOGGER.debug("PB历史分位 fallback 失败: symbol=%s date=%s error=%s", symbol_info.symbol, run_date, exc)
        return {}
    return _compute_pb_history_stats(
        financials.balance_sheet,
        price_df,
        share_info.total_shares,
        run_date,
        current_pb,
    )


def _needs_pb_history_fallback(row: Mapping[str, Any]) -> bool:
    percentile = _safe_float(row.get("pb_3_5y_percentile"))
    sample_count = _safe_float(row.get("pb_3_5y_sample_count"))
    if percentile is None:
        return True
    if sample_count is None:
        return True
    return sample_count < 60


def _first_existing_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _read_price_frame(path: Path, run_date: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取历史价格失败: %s error=%s", path, exc)
        return pd.DataFrame()
    if frame.empty or "日期" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame = frame.dropna(subset=["日期"]).sort_values("日期")
    asof = pd.Timestamp(run_date).normalize()
    frame = frame[frame["日期"] <= asof]
    if frame.empty:
        return pd.DataFrame()
    start = asof - pd.Timedelta(days=1800)
    frame = frame[frame["日期"] >= start]
    frame = frame.set_index("日期")
    return frame


def _price_features(path: Path, run_date: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not path.exists():
        return result
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取价格缓存失败: %s error=%s", path, exc)
        return result
    if frame.empty or "日期" not in frame.columns or "收盘" not in frame.columns:
        return result
    frame = frame.copy()
    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame = frame.dropna(subset=["日期"]).sort_values("日期")
    asof = pd.Timestamp(run_date).normalize()
    frame = frame[frame["日期"] <= asof]
    if frame.empty:
        return result

    close = pd.to_numeric(frame["收盘"], errors="coerce").dropna()
    if close.empty:
        return result
    latest_close = float(close.iloc[-1])
    result["price_asof_date"] = pd.Timestamp(frame.iloc[-1]["日期"]).strftime("%Y-%m-%d")
    result["latest_price"] = latest_close
    result["close"] = latest_close

    for window in (5, 10, 20, 60):
        if len(close) >= window:
            ma_value = float(close.tail(window).mean())
            result[f"ma{window}"] = ma_value
            result[f"close_vs_ma{window}"] = latest_close / ma_value - 1.0 if ma_value else None

    ma_flags = [
        latest_close > result.get("ma5", np.inf),
        result.get("ma5", -np.inf) > result.get("ma10", np.inf),
        result.get("ma10", -np.inf) > result.get("ma20", np.inf),
        result.get("ma20", -np.inf) > result.get("ma60", np.inf),
    ]
    result["ma_bullish_score"] = sum(bool(flag) for flag in ma_flags) / len(ma_flags)

    returns = close.pct_change().dropna()
    if len(returns) >= 20:
        result["volatility_20d"] = float(returns.tail(20).std() * np.sqrt(252))
    if len(returns) >= 60:
        result["volatility_60d"] = float(returns.tail(60).std() * np.sqrt(252))

    if len(close) >= 252:
        high_52w = float(close.tail(252).max())
        result["high_52w"] = high_52w
        result["distance_to_52w_high"] = latest_close / high_52w - 1.0 if high_52w else None

    price_window_start = asof - pd.Timedelta(days=int(365 * 3.5))
    price_window = frame.loc[frame["日期"] >= price_window_start, ["日期", "收盘"]].copy()
    price_window["收盘"] = pd.to_numeric(price_window["收盘"], errors="coerce")
    price_window = price_window.dropna(subset=["收盘"])
    if not price_window.empty:
        price_low = float(price_window["收盘"].min())
        price_high = float(price_window["收盘"].max())
        result["price_low_3_5y"] = price_low
        result["price_high_3_5y"] = price_high
        if price_high > price_low:
            result["price_position_3_5y"] = (latest_close - price_low) / (price_high - price_low)
            result["distance_to_3_5y_high"] = latest_close / price_high - 1.0
            result["distance_to_3_5y_low"] = latest_close / price_low - 1.0 if price_low else None
            result["price_percentile_3_5y"] = float((price_window["收盘"] <= latest_close).mean())
            bucket_count = 10
            edges = np.linspace(price_low, price_high, bucket_count + 1)
            bucket_index = int(np.searchsorted(edges, latest_close, side="right") - 1)
            bucket_index = max(0, min(bucket_index, bucket_count - 1))
            result["price_bucket_index_3_5y"] = bucket_index
            lower = edges[bucket_index]
            upper = edges[bucket_index + 1]
            if bucket_index == bucket_count - 1:
                in_bucket = price_window["收盘"].between(lower, upper, inclusive="both")
            else:
                in_bucket = (price_window["收盘"] >= lower) & (price_window["收盘"] < upper)
            result["price_bucket_occupancy_3_5y"] = float(in_bucket.mean() * 100)

    if len(close) >= 2:
        result["daily_return_price"] = latest_close / float(close.iloc[-2]) - 1.0
        result["daily_change_pct_num"] = result["daily_return_price"] * 100
        result["daily_change_pct"] = _format_percent_value(result["daily_change_pct_num"])

    indexed_frame = frame.set_index("日期").sort_index()
    try:
        macd_frame = macd_indicator(indexed_frame, logger=LOGGER)
        if not macd_frame.empty and "MACD" in macd_frame.columns:
            result["macd"] = _safe_float(macd_frame["MACD"].dropna().iloc[-1])
    except Exception as exc:
        LOGGER.debug("MACD 计算失败: path=%s date=%s error=%s", path, run_date, exc)
    try:
        rsi_frame = rsi_indicator(indexed_frame, logger=LOGGER)
        if not rsi_frame.empty and "RSI(14)" in rsi_frame.columns:
            result["rsi_14"] = _safe_float(rsi_frame["RSI(14)"].dropna().iloc[-1])
    except Exception as exc:
        LOGGER.debug("RSI 计算失败: path=%s date=%s error=%s", path, run_date, exc)

    if "成交额" in frame.columns:
        amount = pd.to_numeric(frame["成交额"], errors="coerce")
        if amount.notna().any():
            latest_amount = float(amount.dropna().iloc[-1]) / 1e8
            result["amount_price"] = latest_amount
            result["amount"] = latest_amount
            if len(amount.dropna()) >= 20:
                amount_5 = float(amount.dropna().tail(5).mean())
                amount_20 = float(amount.dropna().tail(20).mean())
                result["volume_ratio_5d_20d"] = amount_5 / amount_20 if amount_20 else None

    if "换手率" in frame.columns:
        turnover = pd.to_numeric(frame["换手率"], errors="coerce").dropna()
        if not turnover.empty:
            # price.csv stores turnover as ratio, while basic_info stores percent.
            result["turnover_rate_price"] = float(turnover.iloc[-1] * 100)

    return result


def _fallback_valuation_features(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if row.get("peg") is None:
        pe_ttm = _safe_float(row.get("pe_ttm"))
        growth = (
            _safe_float(row.get("deduct_net_income_growth_yoy"))
            or _safe_float(row.get("net_income_growth_yoy"))
            or _safe_float(row.get("revenue_growth_yoy"))
        )
        if pe_ttm is not None and pe_ttm > 0 and growth is not None and growth > 0:
            result["peg"] = pe_ttm / growth
    return result


def _local_chip_distribution_features(path: Path, run_date: str, latest_price: Any) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取价格缓存计算本地筹码失败: %s error=%s", path, exc)
        return {}
    if frame.empty or "日期" not in frame.columns:
        return {}
    work = frame.copy()
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce")
    work = work.dropna(subset=["日期"]).sort_values("日期")
    asof = pd.Timestamp(run_date).normalize()
    work = work[work["日期"] <= asof]
    if work.empty:
        return {}
    chip_frame = build_latest_chip_distribution_from_price_frame(
        work,
        source_lookback_rows=210,
        cyq_window=120,
    )
    if chip_frame.empty:
        return {}
    return _chip_row_features(chip_frame.iloc[-1], latest_price, source="local_price")


def _technical_features(analysis_dir: Path, run_date: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not analysis_dir.exists():
        return result
    path = _latest_dated_file(analysis_dir, "technical_indicators_*.csv", run_date)
    if path is None:
        return result
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取技术指标失败: %s error=%s", path, exc)
        return result
    if frame.empty or "日期" not in frame.columns:
        return result
    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame = frame.dropna(subset=["日期"]).sort_values("日期")
    asof = pd.Timestamp(run_date).normalize()
    frame = frame[frame["日期"] <= asof]
    if frame.empty:
        return result
    item = frame.iloc[-1]
    result["technical_asof_date"] = pd.Timestamp(item["日期"]).strftime("%Y-%m-%d")
    result["macd"] = _safe_float(item.get("MACD"))
    result["rsi_14"] = _safe_float(item.get("RSI(14)"))
    result["technical_change_pct"] = _safe_float(item.get("涨跌幅(%)"))
    result["technical_amount"] = _safe_float(item.get("成交额(亿元)"))
    result["technical_turnover_rate"] = _safe_float(item.get("换手率(%)"))
    if result.get("technical_amount") is not None:
        result["amount"] = result["technical_amount"]
    if result.get("technical_turnover_rate") is not None:
        result["turnover_rate"] = result["technical_turnover_rate"]
    return result


def _valuation_distribution_features(valuation_dir: Path, run_date: str, latest_price: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not valuation_dir.exists():
        return result
    path = _latest_dated_file(valuation_dir, "*_enhanced_pe_analysis.json", run_date)
    if path is None:
        return result
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("读取增强估值 JSON 失败: %s error=%s", path, exc)
        return result
    target = payload.get("target") if isinstance(payload, Mapping) else {}
    if not isinstance(target, Mapping):
        return result
    result["valuation_asof_file"] = path.name

    pe_stats = target.get("PE统计") or {}
    if isinstance(pe_stats, Mapping):
        result["peg"] = _safe_float(pe_stats.get("当前PEG"))
        result["deduct_net_income_growth_yoy"] = _safe_float(pe_stats.get("净利润增长率"))
        if result["deduct_net_income_growth_yoy"] is not None:
            result["deduct_net_income_growth_yoy"] *= 100

    pb_stats = target.get("PB统计") or {}
    historical_pe_rows = target.get("历史PE数据") or []
    if isinstance(pb_stats, Mapping) and isinstance(historical_pe_rows, list):
        current_pb = _safe_float(pb_stats.get("当前PB"))
        pb_values = [
            value
            for item in historical_pe_rows
            if isinstance(item, Mapping)
            for value in [_safe_float(item.get("PB"))]
            if value is not None and value > 0
        ]
        if current_pb is not None and current_pb > 0:
            pb_values.append(current_pb)
        if pb_values:
            result["pb_3_5y_median"] = float(np.median(pb_values))
            result["pb_3_5y_std"] = float(np.std(pb_values, ddof=0))
            if current_pb is not None and current_pb > 0:
                result["pb_3_5y_percentile"] = sum(value <= current_pb for value in pb_values) / len(pb_values)
                result["pb_current_vs_median"] = current_pb / result["pb_3_5y_median"] - 1.0 if result["pb_3_5y_median"] else None

    price_distribution = target.get("价格区间分布") or {}
    summary = price_distribution.get("summary") if isinstance(price_distribution, Mapping) else {}
    table = price_distribution.get("table") if isinstance(price_distribution, Mapping) else []
    current_price = _safe_float(latest_price) or _safe_float(target.get("当前价格"))
    low = _safe_float((summary or {}).get("lowest_close")) if isinstance(summary, Mapping) else None
    high = _safe_float((summary or {}).get("highest_close")) if isinstance(summary, Mapping) else None
    if low is not None:
        result["price_low_3_5y"] = low
    if high is not None:
        result["price_high_3_5y"] = high
    if current_price is not None and low is not None and high is not None and high > low:
        result["price_position_3_5y"] = (current_price - low) / (high - low)
        result["distance_to_3_5y_high"] = current_price / high - 1.0
        result["distance_to_3_5y_low"] = current_price / low - 1.0

    if isinstance(table, list) and current_price is not None:
        bucket = _find_price_bucket(table, current_price)
        if bucket is not None:
            idx, item = bucket
            result["price_bucket_index_3_5y"] = idx
            result["price_bucket_occupancy_3_5y"] = _safe_float(item.get("出现概率(%)"))
            result["price_bucket_avg_pe"] = _safe_float(item.get("平均PE"))
            result["price_bucket_avg_pb"] = _safe_float(item.get("平均PB"))
            if len(table) > 1:
                result["price_percentile_3_5y"] = idx / (len(table) - 1)
            current_pe = _safe_float(target.get("PE统计", {}).get("扣非PE(TTM)") if isinstance(target.get("PE统计"), Mapping) else None)
            current_pb = _safe_float(target.get("PB统计", {}).get("当前PB") if isinstance(target.get("PB统计"), Mapping) else None)
            avg_pe = result.get("price_bucket_avg_pe")
            avg_pb = result.get("price_bucket_avg_pb")
            result["pe_vs_price_bucket_avg"] = current_pe / avg_pe - 1.0 if current_pe and avg_pe else None
            result["pb_vs_price_bucket_avg"] = current_pb / avg_pb - 1.0 if current_pb and avg_pb else None
    return result


def _chip_distribution_features(path: Path, run_date: str, latest_price: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not path.exists():
        return result
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取筹码分布缓存失败: %s error=%s", path, exc)
        return result
    if frame.empty or "日期" not in frame.columns:
        return result

    work = frame.copy()
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce")
    work = work.dropna(subset=["日期"]).sort_values("日期")
    asof = pd.Timestamp(run_date).normalize()
    work = work[work["日期"] <= asof]
    if work.empty:
        return result

    item = work.iloc[-1]
    return _chip_row_features(item, latest_price, source="cache")


def _chip_row_features(item: pd.Series, latest_price: Any, *, source: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    result["chip_asof_date"] = pd.Timestamp(item["日期"]).strftime("%Y-%m-%d")
    result["chip_source"] = source
    result["chip_profit_ratio"] = _safe_float(item.get("获利比例"))
    result["chip_avg_cost"] = _safe_float(item.get("平均成本"))
    result["chip_cost_70_low"] = _safe_float(item.get("70成本-低"))
    result["chip_cost_70_high"] = _safe_float(item.get("70成本-高"))
    result["chip_concentration_70"] = _safe_float(item.get("70集中度"))
    result["chip_cost_90_low"] = _safe_float(item.get("90成本-低"))
    result["chip_cost_90_high"] = _safe_float(item.get("90成本-高"))
    result["chip_concentration_90"] = _safe_float(item.get("90集中度"))

    current_price = _safe_float(latest_price)
    if current_price is None or current_price <= 0:
        return result

    avg_cost = result.get("chip_avg_cost")
    if avg_cost:
        result["price_vs_chip_avg_cost"] = current_price / avg_cost - 1.0

    cost_70_low = result.get("chip_cost_70_low")
    cost_70_high = result.get("chip_cost_70_high")
    cost_90_high = result.get("chip_cost_90_high")
    if cost_70_low:
        result["chip_support_distance_70"] = current_price / cost_70_low - 1.0
    if cost_70_high:
        result["overhead_pressure_70"] = max(cost_70_high / current_price - 1.0, 0.0)
    if cost_90_high:
        result["overhead_pressure_90"] = max(cost_90_high / current_price - 1.0, 0.0)
    return result


def _find_price_bucket(table: Sequence[Any], current_price: float) -> tuple[int, Mapping[str, Any]] | None:
    for idx, raw in enumerate(table):
        if not isinstance(raw, Mapping):
            continue
        low = _safe_float(raw.get("区间下限(元)"))
        high = _safe_float(raw.get("区间上限(元)"))
        if low is None or high is None:
            continue
        if low <= current_price <= high or (idx == len(table) - 1 and current_price >= low):
            return idx, raw
    return None


def _latest_dated_file(directory: Path, pattern: str, run_date: str) -> Path | None:
    asof = pd.Timestamp(run_date).normalize()
    candidates: list[tuple[pd.Timestamp, Path]] = []
    for path in directory.glob(pattern):
        date_text = _last_date_token(path.stem)
        if not date_text:
            continue
        dt = pd.to_datetime(date_text, format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            continue
        dt = pd.Timestamp(dt).normalize()
        if dt <= asof:
            candidates.append((dt, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _last_date_token(value: str) -> str:
    import re

    matches = re.findall(r"20\d{6}", value)
    return matches[-1] if matches else ""


def _normalize_numeric_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    skip = {
        "date",
        "symbol",
        "stock_name",
        "sector",
        "industry",
        "stock_type",
        "daily_change_pct",
        "basic_info_asof_date",
        "price_asof_date",
        "technical_asof_date",
        "valuation_asof_file",
        "chip_asof_date",
    }
    for col in frame.columns:
        if col in skip:
            continue
        frame[col] = pd.to_numeric(frame[col], errors="ignore")
    return frame


def _apply_factor_sanity_rules(frame: pd.DataFrame) -> pd.DataFrame:
    """Guard factor semantics that cannot be fixed by raw numeric ranking."""

    frame = frame.copy()
    if "pe_ttm" not in frame.columns:
        return frame
    pe = pd.to_numeric(frame["pe_ttm"], errors="coerce")
    pe_valid = pe > 0
    frame["pe_valuation_valid"] = pe_valid
    invalid_mask = ~pe_valid.fillna(False)
    if invalid_mask.any():
        for field in ("pe_3_5y_percentile", "pe_current_vs_median", "peg"):
            if field not in frame.columns:
                continue
            raw_field = f"{field}_raw"
            if raw_field not in frame.columns:
                frame[raw_field] = frame[field]
            frame.loc[invalid_mask, field] = np.nan
    return frame


def _data_quality_score(row: Mapping[str, Any]) -> float:
    required = [
        "latest_price",
        "pe_ttm",
        "pb",
        "return_3m",
        "roe",
        "macd",
        "rsi_14",
        "price_position_3_5y",
    ]
    present = sum(1 for field in required if row.get(field) is not None and not pd.isna(row.get(field)))
    return present / len(required)


def _write_by_symbol_row(row: Mapping[str, Any], by_symbol_dir: Path, *, config: FactorStoreConfig) -> Path | None:
    symbol = str(row.get("symbol") or "").strip()
    if not symbol:
        return None
    csv_path = by_symbol_dir / f"{symbol}.csv"
    parquet_path = by_symbol_dir / f"{symbol}.parquet"
    new_frame = pd.DataFrame([dict(row)])
    existing = _read_existing_symbol_frame(csv_path, parquet_path)
    if existing is not None and not existing.empty:
        combined = pd.concat([existing, new_frame], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "symbol"], keep="last")
    else:
        combined = new_frame
    combined = combined.sort_values("date")
    if config.write_csv:
        combined.to_csv(csv_path, index=False)
    if config.write_parquet:
        _write_parquet(combined, parquet_path)
    return csv_path


def _read_existing_symbol_frame(csv_path: Path, parquet_path: Path) -> pd.DataFrame | None:
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except Exception:
            pass
    if csv_path.exists():
        try:
            return pd.read_csv(csv_path)
        except Exception:
            pass
    return None


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    try:
        frame.to_parquet(path, index=False, engine=PARQUET_ENGINE)
    except Exception as exc:
        LOGGER.warning("写 parquet 失败，已保留 CSV 输出: path=%s error=%s", path, exc)


def _factor_snapshot_payload(run_date: str, frame: pd.DataFrame, *, config: FactorStoreConfig) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "factor_store_version": "v1",
        "summary": {
            "row_count": int(len(frame)),
            "max_staleness_days": config.max_staleness_days,
        },
        "columns": list(frame.columns),
        "items": frame.replace({np.nan: None}).to_dict(orient="records"),
    }


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None
    return numeric


def _parse_percent(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace("%", "")
    return _safe_float(value)


def _format_percent_value(value: Any) -> str | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    if abs(numeric) < 1e-8:
        return "0%"
    return f"{numeric:+.2f}%"


__all__ = [
    "FactorStoreConfig",
    "build_factor_store_for_date",
    "build_symbol_factor_row",
]
