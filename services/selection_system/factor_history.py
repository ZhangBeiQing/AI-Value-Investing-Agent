"""Backfill factor snapshots by reusing the existing basic-info factor pipeline."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from core.logging import get_logger
from services.selection_system.factor_scoring import FactorScoringConfig, build_factor_scores_for_date
from services.selection_system.factor_store import (
    FactorStoreConfig,
    _apply_factor_sanity_rules,
    _chip_row_features,
    _data_quality_score,
    _factor_snapshot_payload,
    _normalize_numeric_columns,
    _safe_float,
    _write_parquet,
    build_factor_store_for_date,
)
from services.selection_system.master_universe import load_master_universe
from services.selection_system.models import MasterUniverseStock
from services.selection_system.paths import SelectionSystemPaths
from services.selection_system.quant_prefilter import QuantPrefilterConfig, build_quant_prefilter_for_date
from services.selection_system.store import save_json_file
from services.snapshot.basic_snapshot import basic_info
from indicator_library.calculators.fundamental import calculate_rolling_ttm_profit
from indicator_library.calculators.liquidity import liquidity_score
from indicator_library.calculators.momentum import macd_indicator, rsi_indicator
from shared_data_access.cache_registry import CacheKind, build_cache_dir
from utlity.stock_utils import SymbolFormatError, get_stock_data_dir, parse_symbol


LOGGER = get_logger("SelectionFactorHistory")
DEFAULT_HISTORY_YEARS = 4
DEFAULT_HISTORY_PRICE_LOOKBACK_DAYS = 1800


@dataclass(frozen=True)
class FactorHistoryConfig:
    """Configuration for historical factor snapshot backfill."""

    start_date: str | None = None
    end_date: str | None = None
    years: int = DEFAULT_HISTORY_YEARS
    source: str = "local_factor_store"
    min_symbol_count: int = 1
    max_staleness_days: int = 10
    price_lookback_days: int = DEFAULT_HISTORY_PRICE_LOOKBACK_DAYS
    max_workers: int = 1
    write_parquet: bool = True
    build_scores: bool = True
    build_prefilter: bool = False
    skip_existing: bool = True
    top_n: int = 20
    scoring_config_path: str | Path = "configs/selection_system/factor_scoring.yaml"


def build_factor_history(
    *,
    base_dir: str | Path = "data",
    config: FactorHistoryConfig | None = None,
) -> dict[str, Path]:
    """Backfill historical by-date factor snapshots.

    `local_factor_store` builds factor snapshots directly from local caches.
    `computed_basic_info` reuses services.snapshot.basic_snapshot.basic_info,
    which is the same validated factor path used by the daily pipeline.
    `basic_info_cache` only rebuilds factor_store from already cached dates.
    """

    config = config or FactorHistoryConfig()
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()

    if config.source in {"local_factor_store", "computed_basic_info"}:
        dates = available_price_dates(
            paths.base_dir,
            symbols=[stock.symbol for stock in load_master_universe(paths).stocks],
            start_date=config.start_date,
            end_date=config.end_date,
            years=config.years,
            min_symbol_count=config.min_symbol_count,
        )
        if config.source == "local_factor_store":
            built_rows = _build_local_factor_history_batch(paths, dates, config)
            return _write_history_manifest(paths, config, dates, built_rows)
        if config.source == "computed_basic_info":
            _compute_basic_info_history(paths, dates, config)
    elif config.source == "basic_info_cache":
        dates = available_basic_info_dates(
            paths.base_dir,
            start_date=config.start_date,
            end_date=config.end_date,
            min_symbol_count=config.min_symbol_count,
        )
    elif config.source == "factor_store_cache":
        dates = available_factor_store_dates(
            paths.base_dir,
            start_date=config.start_date,
            end_date=config.end_date,
            years=config.years,
            min_symbol_count=config.min_symbol_count,
        )
        built_rows = _build_existing_factor_outputs_for_dates(paths, dates, config)
        return _write_history_manifest(paths, config, dates, built_rows)
    else:
        raise ValueError("source 必须是 local_factor_store、computed_basic_info、basic_info_cache 或 factor_store_cache")

    built_rows = _build_factor_outputs_for_dates(paths, dates, config)
    return _write_history_manifest(paths, config, dates, built_rows)


def _build_local_factor_history_batch(
    paths: SelectionSystemPaths,
    dates: list[str],
    config: FactorHistoryConfig,
) -> list[dict[str, Any]]:
    """Build historical factor views by loading each symbol's local cache once."""

    if not dates:
        return []
    factor_dir = paths.base_dir / "factor_store"
    by_symbol_dir = factor_dir / "by_symbol"
    by_date_dir = factor_dir / "by_date"
    by_symbol_dir.mkdir(parents=True, exist_ok=True)
    by_date_dir.mkdir(parents=True, exist_ok=True)

    stocks = load_master_universe(paths).stocks
    symbol_frames: list[pd.DataFrame] = []
    for stock in stocks:
        frame = _build_symbol_factor_history_frame(stock, dates, paths.base_dir)
        if frame.empty:
            continue
        frame = _normalize_numeric_columns(frame)
        frame = _apply_factor_sanity_rules(frame)
        frame = frame.sort_values(["date", "symbol"]).reset_index(drop=True)
        _write_symbol_history(frame, by_symbol_dir, config)
        symbol_frames.append(frame)

    if not symbol_frames:
        return []

    history = pd.concat(symbol_frames, ignore_index=True)
    history = history[history["date"].isin(dates)].copy()
    built_rows: list[dict[str, Any]] = []
    for run_date in dates:
        if config.skip_existing and _history_outputs_exist(paths, run_date, config):
            built_rows.append(
                {
                    "run_date": run_date,
                    "factor_snapshot_csv": str(paths.run_dir(run_date) / "12_factor_snapshot.csv"),
                    "factor_by_date_csv": str(by_date_dir / f"{run_date}.csv"),
                    "factor_scores_csv": str(paths.run_dir(run_date) / "13_factor_scores.csv") if config.build_scores else "",
                    "quant_prefilter_csv": str(paths.run_dir(run_date) / "12_quant_prefilter.csv") if config.build_prefilter else "",
                    "skipped_existing": "true",
                }
            )
            continue

        frame = history[history["date"] == run_date].copy()
        if frame.empty or len(frame) < config.min_symbol_count:
            continue
        frame = frame.sort_values(["data_quality_score", "symbol"], ascending=[False, True]).reset_index(drop=True)
        if "factor_rank" in frame.columns:
            frame = frame.drop(columns=["factor_rank"])
        frame.insert(0, "factor_rank", range(1, len(frame) + 1))
        factor_outputs = _write_date_factor_outputs(run_date, frame, paths, config)

        score_outputs: dict[str, Path] = {}
        prefilter_outputs: dict[str, Path] = {}
        if config.build_scores:
            score_outputs = build_factor_scores_for_date(
                run_date,
                base_dir=paths.base_dir,
                config=FactorScoringConfig(
                    config_path=config.scoring_config_path,
                    max_staleness_days=config.max_staleness_days,
                ),
                ensure_factor_store=False,
            )
        if config.build_prefilter:
            prefilter_outputs = build_quant_prefilter_for_date(
                run_date,
                base_dir=paths.base_dir,
                config=QuantPrefilterConfig(
                    top_n=config.top_n,
                    max_staleness_days=config.max_staleness_days,
                ),
                ensure_factor_store=False,
            )
        built_rows.append(_history_item(run_date, factor_outputs, score_outputs, prefilter_outputs))

    save_json_file(
        factor_dir / "manifest.json",
        {
            "schema_version": 2,
            "updated_at": datetime.now().isoformat(),
            "latest_run_date": dates[-1],
            "row_count": int(len(history)),
            "symbol_count": int(history["symbol"].nunique()),
            "date_count": len(dates),
            "build_mode": "by_symbol_batch",
        },
    )
    LOGGER.info("本地历史因子批量生成完成: symbols=%d dates=%d rows=%d", len(symbol_frames), len(dates), len(history))
    return built_rows


def _build_symbol_factor_history_frame(
    stock: MasterUniverseStock,
    dates: list[str],
    base_dir: Path,
) -> pd.DataFrame:
    try:
        symbol_info = parse_symbol(stock.symbol)
    except SymbolFormatError as exc:
        LOGGER.warning("跳过非法 symbol: %s error=%s", stock.symbol, exc)
        return pd.DataFrame()

    stock_root = get_stock_data_dir(symbol_info, base_dir=base_dir)
    price = _load_price_history(stock_root / "prices" / "price.csv", dates)
    if price.empty:
        return pd.DataFrame()

    target_index = pd.DatetimeIndex(pd.to_datetime(dates, errors="coerce")).dropna().normalize()
    target_index = target_index.intersection(price.index.normalize())
    if target_index.empty:
        return pd.DataFrame()

    rows = _price_factor_history(price, target_index)
    rows["symbol"] = stock.symbol
    rows["stock_name"] = stock.name or symbol_info.stock_name or stock.symbol
    rows["sector"] = stock.sector
    rows["industry"] = stock.industry
    rows["stock_type"] = stock.stock_type or "growth"

    share_history = _load_share_history(stock_root / "share_info" / "stock_share_change_cninfo.csv")
    financial_history = _financial_factor_history(
        stock.symbol,
        price,
        stock_root / "financials_cache",
        share_history,
        target_index,
    )
    if not financial_history.empty:
        rows = rows.merge(financial_history, on="date", how="left")

    chip_history = _chip_factor_history(symbol_info, stock_root, base_dir, price, target_index)
    if not chip_history.empty:
        rows = rows.merge(chip_history, on="date", how="left")

    rows = rows.sort_values("date").reset_index(drop=True)
    rows["data_quality_score"] = rows.apply(lambda row: _data_quality_score(row.to_dict()), axis=1)

    leading = ["date", "symbol", "stock_name", "sector", "industry", "stock_type"]
    ordered = [column for column in leading if column in rows.columns]
    ordered.extend(column for column in rows.columns if column not in ordered)
    return rows[ordered]


def _load_price_history(path: Path, dates: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取历史价格失败: %s error=%s", path, exc)
        return pd.DataFrame()
    required = {"日期", "收盘"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()

    work = frame.copy()
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce")
    work = work.dropna(subset=["日期"]).sort_values("日期")
    start = pd.Timestamp(dates[0]).normalize() - pd.Timedelta(days=int(365 * 3.7))
    end = pd.Timestamp(dates[-1]).normalize()
    work = work[(work["日期"] >= start) & (work["日期"] <= end)].copy()
    if work.empty:
        return pd.DataFrame()
    for column in ("开盘", "收盘", "最高", "最低", "成交额", "成交量", "换手率"):
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")
    return work.set_index("日期", drop=False).sort_index()


def _price_factor_history(price: pd.DataFrame, target_index: pd.DatetimeIndex) -> pd.DataFrame:
    close = pd.to_numeric(price["收盘"], errors="coerce")
    result = pd.DataFrame(index=price.index)
    result["date"] = result.index.strftime("%Y-%m-%d")
    result["basic_info_asof_date"] = result["date"]
    result["price_asof_date"] = result["date"]
    result["latest_price"] = close
    result["close"] = close
    result["daily_return_price"] = close.pct_change()
    result["daily_change_pct_num"] = result["daily_return_price"] * 100
    result["daily_change_pct"] = result["daily_change_pct_num"].map(_format_percent_value)

    if "成交额" in price.columns:
        amount = pd.to_numeric(price["成交额"], errors="coerce") / 1e8
        result["latest_volume"] = amount
        result["amount"] = amount
        result["amount_price"] = amount
        result["amount_basic_info"] = amount
        result["volume_ratio_5d_20d"] = amount.rolling(5, min_periods=5).mean() / amount.rolling(20, min_periods=20).mean()
    if "成交量" in price.columns:
        volume = pd.to_numeric(price["成交量"], errors="coerce")
        result["avg_volume_30d_wan"] = volume.rolling(30, min_periods=5).mean() / 10000
    if "换手率" in price.columns:
        turnover = pd.to_numeric(price["换手率"], errors="coerce")
        result["turnover_rate_price"] = turnover * 100
        result["turnover_rate"] = turnover * 100
        result["avg_turnover_30d"] = turnover.rolling(30, min_periods=5).mean() * 100
    if {"turnover_rate", "avg_turnover_30d", "avg_volume_30d_wan"}.issubset(result.columns):
        result["liquidity_score"] = [
            liquidity_score(turnover_value, average_turnover, None, avg_volume)
            for turnover_value, average_turnover, avg_volume in zip(
                result["turnover_rate"],
                result["avg_turnover_30d"],
                result["avg_volume_30d_wan"],
            )
        ]

    for window in (5, 10, 20, 60):
        ma = close.rolling(window, min_periods=window).mean()
        result[f"ma{window}"] = ma
        result[f"close_vs_ma{window}"] = close / ma - 1.0

    ma_flags = pd.concat(
        [
            close > result["ma5"],
            result["ma5"] > result["ma10"],
            result["ma10"] > result["ma20"],
            result["ma20"] > result["ma60"],
        ],
        axis=1,
    )
    result["ma_bullish_score"] = ma_flags.mean(axis=1)

    returns = close.pct_change()
    result["volatility_20d"] = returns.rolling(20, min_periods=20).std() * np.sqrt(252)
    result["volatility_60d"] = returns.rolling(60, min_periods=60).std() * np.sqrt(252)
    for days, prefix in ((63, "3m"), (126, "6m"), (252, "1y")):
        result[f"return_{prefix}"] = close / close.shift(days) - 1.0
        result[f"volatility_{prefix}"] = returns.rolling(days, min_periods=max(20, days // 3)).std() * np.sqrt(252) * 100
        result[f"sharpe_{prefix}"] = (
            returns.rolling(days, min_periods=max(20, days // 3)).mean()
            / returns.rolling(days, min_periods=max(20, days // 3)).std()
            * np.sqrt(252)
        )
        result[f"max_drawdown_{prefix}"] = _target_max_drawdown(close, days, target_index) * 100

    high_52w = close.rolling(252, min_periods=60).max()
    result["high_52w"] = high_52w
    result["distance_to_52w_high"] = close / high_52w - 1.0

    price_distribution = _price_distribution_history(close, target_index)
    result = result.join(price_distribution)

    indexed_price = price.drop(columns=["日期"], errors="ignore").copy()
    try:
        macd_frame = macd_indicator(indexed_price, logger=LOGGER)
        if not macd_frame.empty and "MACD" in macd_frame.columns:
            result["macd"] = pd.to_numeric(macd_frame["MACD"], errors="coerce")
    except Exception as exc:
        LOGGER.debug("历史 MACD 批量计算失败: error=%s", exc)
    try:
        rsi_frame = rsi_indicator(indexed_price, logger=LOGGER)
        if not rsi_frame.empty and "RSI(14)" in rsi_frame.columns:
            result["rsi_14"] = pd.to_numeric(rsi_frame["RSI(14)"], errors="coerce")
    except Exception as exc:
        LOGGER.debug("历史 RSI 批量计算失败: error=%s", exc)

    result = result.loc[result.index.normalize().isin(target_index)].copy()
    return result.reset_index(drop=True)


def _target_max_drawdown(close: pd.Series, window: int, target_index: pd.DatetimeIndex) -> pd.Series:
    result = pd.Series(np.nan, index=close.index, dtype=float)
    close_series = close.dropna().sort_index()
    if close_series.empty:
        return result
    close_index = close_series.index.to_numpy(dtype="datetime64[ns]")
    close_values = close_series.to_numpy(dtype=float)
    min_periods = max(20, window // 3)
    for dt in target_index:
        end_pos = np.searchsorted(close_index, np.datetime64(dt), side="right")
        start_pos = max(0, end_pos - window)
        window_values = close_values[start_pos:end_pos]
        window_values = window_values[np.isfinite(window_values)]
        if len(window_values) < min_periods:
            continue
        running_max = np.maximum.accumulate(window_values)
        drawdown = window_values / running_max - 1.0
        result.loc[dt] = float(drawdown.min())
    return result


def _price_distribution_history(close: pd.Series, target_index: pd.DatetimeIndex) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    window_days = int(365 * 3.5)
    for dt, value in close.items():
        if pd.Timestamp(dt).normalize() not in target_index:
            rows.append({})
            continue
        latest = _safe_float(value)
        if latest is None:
            rows.append({})
            continue
        start = pd.Timestamp(dt).normalize() - pd.Timedelta(days=window_days)
        window = close[(close.index >= start) & (close.index <= dt)].dropna()
        if window.empty:
            rows.append({})
            continue
        low = float(window.min())
        high = float(window.max())
        item: dict[str, Any] = {
            "price_low_3_5y": low,
            "price_high_3_5y": high,
        }
        if high > low:
            item["price_position_3_5y"] = (latest - low) / (high - low)
            item["distance_to_3_5y_high"] = latest / high - 1.0
            item["distance_to_3_5y_low"] = latest / low - 1.0 if low else None
            item["price_percentile_3_5y"] = float((window <= latest).mean())
            bucket_count = 10
            edges = np.linspace(low, high, bucket_count + 1)
            bucket_index = int(np.searchsorted(edges, latest, side="right") - 1)
            bucket_index = max(0, min(bucket_index, bucket_count - 1))
            item["price_bucket_index_3_5y"] = bucket_index
            lower = edges[bucket_index]
            upper = edges[bucket_index + 1]
            if bucket_index == bucket_count - 1:
                in_bucket = window.between(lower, upper, inclusive="both")
            else:
                in_bucket = (window >= lower) & (window < upper)
            item["price_bucket_occupancy_3_5y"] = float(in_bucket.mean() * 100)
        rows.append(item)
    return pd.DataFrame(rows, index=close.index)


def _load_share_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["effective_date", "total_shares", "float_shares"])
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取股本历史失败: %s error=%s", path, exc)
        return pd.DataFrame(columns=["effective_date", "total_shares", "float_shares"])
    if frame.empty or "变动日期" not in frame.columns or "总股本" not in frame.columns:
        return pd.DataFrame(columns=["effective_date", "total_shares", "float_shares"])
    work = frame.copy()
    work["effective_date"] = pd.to_datetime(work["变动日期"], errors="coerce")
    work["total_shares"] = pd.to_numeric(work["总股本"], errors="coerce") * 10000
    float_source = "已流通股份" if "已流通股份" in work.columns else "流通受限股份"
    work["float_shares"] = pd.to_numeric(work.get(float_source), errors="coerce") * 10000
    work["float_shares"] = work["float_shares"].fillna(work["total_shares"])
    work = work.dropna(subset=["effective_date", "total_shares"])
    return work[["effective_date", "total_shares", "float_shares"]].sort_values("effective_date")


def _financial_factor_history(
    symbol: str,
    price: pd.DataFrame,
    financial_dir: Path,
    share_history: pd.DataFrame,
    target_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    profit = _read_financial_csv(financial_dir / "profit_sheet.csv")
    abstract = _read_financial_csv(financial_dir / "financial_abstract.csv")
    if profit.empty and abstract.empty:
        return pd.DataFrame()

    work = pd.DataFrame(index=target_index)
    work["date"] = work.index.strftime("%Y-%m-%d")
    close = pd.to_numeric(price["收盘"], errors="coerce")
    total_shares = _share_series_for_dates(share_history, price.index)
    close_target = close.reindex(target_index)
    shares_target = total_shares.reindex(target_index)
    work["market_cap"] = close_target * shares_target / 1e8

    abstract_metrics = _abstract_metric_history(abstract, target_index)
    if not abstract_metrics.empty:
        work = work.join(abstract_metrics)
    daily_abstract_metrics = _abstract_metric_history(abstract, price.index)
    bps_daily = (
        pd.to_numeric(daily_abstract_metrics["BPS"], errors="coerce")
        if "BPS" in daily_abstract_metrics.columns
        else pd.Series(dtype=float)
    )

    ttm = _ttm_profit_history(profit)
    deduct_ttm = (
        _ttm_profit_history(profit, profit_column="DEDUCT_PARENT_NETPROFIT")
        if "DEDUCT_PARENT_NETPROFIT" in profit.columns
        else pd.DataFrame()
    )
    selected_ttm = deduct_ttm if not deduct_ttm.empty else ttm
    if not selected_ttm.empty:
        pe_history = _valuation_from_ttm_history(selected_ttm, close, total_shares, target_index)
        work = work.join(pe_history)

    if not bps_daily.empty:
        work["BPS"] = bps_daily.reindex(target_index)
    if "BPS" in work.columns:
        work["pb"] = close_target / pd.to_numeric(work["BPS"], errors="coerce").replace(0, np.nan)
    if "revenue_ttm" in work.columns:
        revenue = pd.to_numeric(work["revenue_ttm"], errors="coerce")
        work["ps"] = close_target * shares_target / revenue.replace(0, np.nan)
    if "pe_ttm" in work.columns:
        growth = None
        for column in ("deduct_net_income_growth_yoy", "net_income_growth_yoy", "revenue_growth_yoy"):
            if column in work.columns:
                growth = work[column] if growth is None else growth.fillna(work[column])
        if growth is not None:
            work["peg"] = work["pe_ttm"] / growth.where(growth > 0)

    pb_history = _pb_history_from_daily_bps(bps_daily, close, target_index)
    if not pb_history.empty:
        work = work.join(pb_history)
    return work.reset_index(drop=True)


def _read_financial_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:
        LOGGER.warning("读取财务缓存失败: %s error=%s", path, exc)
        return pd.DataFrame()


def _share_series_for_dates(share_history: pd.DataFrame, dates: pd.DatetimeIndex) -> pd.Series:
    if share_history.empty:
        return pd.Series(np.nan, index=dates)
    series = share_history.set_index("effective_date")["total_shares"].sort_index()
    result = series.reindex(series.index.union(dates)).sort_index().ffill().reindex(dates)
    return pd.to_numeric(result, errors="coerce")


def _abstract_metric_history(abstract: pd.DataFrame, dates: pd.DatetimeIndex) -> pd.DataFrame:
    result = pd.DataFrame(index=dates)
    if abstract.empty:
        return result
    if "指标" in abstract.columns:
        metrics = {
            "revenue_growth_yoy": ("营业总收入", "yoy"),
            "net_income_growth_yoy": ("归母净利润", "yoy"),
            "deduct_net_income_growth_yoy": ("扣非净利润", "yoy"),
            "gross_margin": ("毛利率", "latest"),
            "net_profit_margin": ("销售净利率", "latest"),
            "roe": ("ROE", "latest"),
            "BPS": ("每股净资产", "latest"),
            "revenue_ttm": ("营业总收入", "ttm"),
        }
        date_cols = sorted(col for col in abstract.columns if isinstance(col, str) and col.isdigit() and len(col) == 8)
        for out_col, (keyword, mode) in metrics.items():
            row = _find_abstract_row(abstract, keyword)
            if row is None:
                continue
            metric_series = _metric_series_from_row(row, date_cols, mode)
            result[out_col] = _align_report_series(metric_series, dates)
        return result

    column_metrics = {
        "revenue_growth_yoy": ("OPERATE_INCOME_YOY", "TOTAL_OPERATE_INCOME_YOY"),
        "net_income_growth_yoy": ("HOLDER_PROFIT_YOY", "NETPROFIT_YOY"),
        "deduct_net_income_growth_yoy": ("DEDUCT_PARENT_NETPROFIT_YOY",),
        "gross_margin": ("GROSS_PROFIT_RATIO",),
        "net_profit_margin": ("NET_PROFIT_RATIO",),
        "roe": ("ROE_AVG", "ROE_YEARLY"),
        "BPS": ("BPS",),
        "revenue_ttm": ("OPERATE_INCOME_TTM", "TOTAL_OPERATE_INCOME_TTM"),
    }
    report_col = "REPORT_DATE" if "REPORT_DATE" in abstract.columns else None
    notice_col = "NOTICE_DATE" if "NOTICE_DATE" in abstract.columns else "公告日期" if "公告日期" in abstract.columns else report_col
    if report_col is None or notice_col is None:
        return result
    work = abstract.copy()
    work[report_col] = pd.to_datetime(work[report_col], errors="coerce")
    work[notice_col] = pd.to_datetime(work[notice_col], errors="coerce")
    work = work.dropna(subset=[report_col, notice_col]).sort_values(notice_col)
    for out_col, candidates in column_metrics.items():
        source = next((column for column in candidates if column in work.columns), None)
        if source is None:
            continue
        series = pd.Series(pd.to_numeric(work[source], errors="coerce").values, index=work[notice_col])
        result[out_col] = _align_event_series(series, dates)
    return result


def _find_abstract_row(frame: pd.DataFrame, keyword: str) -> pd.Series | None:
    matches = frame[frame["指标"].astype(str).str.contains(keyword, regex=False, na=False)]
    if matches.empty:
        return None
    return matches.iloc[0]


def _metric_series_from_row(row: pd.Series, date_cols: list[str], mode: str) -> pd.Series:
    values = pd.Series({pd.Timestamp(col): _safe_float(row.get(col)) for col in date_cols}, dtype=float).dropna()
    if values.empty:
        return values
    if mode == "latest":
        return values
    if mode == "yoy":
        data: dict[pd.Timestamp, float] = {}
        raw = {col: _safe_float(row.get(col)) for col in date_cols}
        for col in date_cols:
            value = raw.get(col)
            previous = raw.get(str(int(col[:4]) - 1) + col[4:])
            if value is None or previous in (None, 0):
                continue
            data[pd.Timestamp(col)] = (value - previous) / abs(previous) * 100
        return pd.Series(data, dtype=float)
    if mode == "ttm":
        data = {}
        raw = {col: _safe_float(row.get(col)) for col in date_cols}
        for col in date_cols:
            total = _ttm_from_cumulative_mapping(raw, col)
            if total is not None:
                data[pd.Timestamp(col)] = total
        return pd.Series(data, dtype=float)
    return values


def _ttm_from_cumulative_mapping(values: Mapping[str, float | None], latest_col: str) -> float | None:
    year = int(latest_col[:4])
    quarter_suffixes = ["0331", "0630", "0930", "1231"]
    latest_suffix = latest_col[4:]
    if latest_suffix not in quarter_suffixes:
        return None
    latest_index = quarter_suffixes.index(latest_suffix)
    quarters: list[float] = []
    for offset in range(4):
        index = latest_index - offset
        item_year = year
        if index < 0:
            index += 4
            item_year -= 1
        col = f"{item_year}{quarter_suffixes[index]}"
        cumulative = values.get(col)
        if cumulative is None:
            continue
        if col.endswith("0331"):
            quarter = cumulative
        else:
            previous_suffix = quarter_suffixes[quarter_suffixes.index(col[4:]) - 1]
            previous = values.get(f"{col[:4]}{previous_suffix}")
            quarter = cumulative - previous if previous is not None else cumulative
        quarters.append(quarter)
    if len(quarters) < 3:
        return None
    return float(sum(quarters))


def _align_report_series(series: pd.Series, dates: pd.DatetimeIndex) -> pd.Series:
    if series.empty:
        return pd.Series(np.nan, index=dates)
    report_dates = pd.to_datetime(series.index, errors="coerce")
    release_dates = pd.Series(report_dates + pd.Timedelta(days=45), index=report_dates)
    year_end = report_dates.month == 12
    release_dates.loc[year_end] = report_dates[year_end] + pd.Timedelta(days=120)
    event_series = pd.Series(series.values, index=release_dates.values).sort_index()
    return _align_event_series(event_series, dates)


def _align_event_series(series: pd.Series, dates: pd.DatetimeIndex) -> pd.Series:
    if series.empty:
        return pd.Series(np.nan, index=dates)
    event_series = pd.Series(pd.to_numeric(series, errors="coerce").values, index=pd.to_datetime(series.index, errors="coerce"))
    event_series = event_series.dropna().sort_index()
    event_series = event_series[~event_series.index.duplicated(keep="last")]
    aligned = event_series.reindex(event_series.index.union(dates)).sort_index().ffill().reindex(dates)
    return pd.to_numeric(aligned, errors="coerce")


def _ttm_profit_history(profit: pd.DataFrame, *, profit_column: str | None = None) -> pd.DataFrame:
    if profit.empty:
        return pd.DataFrame()
    ttm = calculate_rolling_ttm_profit(profit, profit_column=profit_column, logger=LOGGER)
    if ttm.empty or "REPORT_DATE" not in ttm.columns:
        return pd.DataFrame()
    notice_col = "NOTICE_DATE" if "NOTICE_DATE" in profit.columns else "公告日期" if "公告日期" in profit.columns else None
    release_map: dict[pd.Timestamp, pd.Timestamp] = {}
    if notice_col is not None:
        work = profit[["REPORT_DATE", notice_col]].copy()
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work[notice_col] = pd.to_datetime(work[notice_col], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE", notice_col])
        for _, row in work.iterrows():
            release_map[pd.Timestamp(row["REPORT_DATE"]).normalize()] = pd.Timestamp(row[notice_col]).normalize()
    result = ttm.copy()
    result["REPORT_DATE"] = pd.to_datetime(result["REPORT_DATE"], errors="coerce")
    result = result.dropna(subset=["REPORT_DATE"])
    result["NOTICE_DATE"] = result["REPORT_DATE"].map(lambda dt: release_map.get(pd.Timestamp(dt).normalize(), pd.Timestamp(dt).normalize() + pd.Timedelta(days=45)))
    return result.dropna(subset=["NOTICE_DATE"]).sort_values("NOTICE_DATE")


def _valuation_from_ttm_history(
    ttm: pd.DataFrame,
    close: pd.Series,
    total_shares: pd.Series,
    target_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    result = pd.DataFrame(index=target_index)
    samples = ttm.copy()
    samples["NOTICE_DATE"] = pd.to_datetime(samples["NOTICE_DATE"], errors="coerce")
    samples["REPORT_DATE"] = pd.to_datetime(samples["REPORT_DATE"], errors="coerce")
    samples["TTM_NET_PROFIT_RAW"] = pd.to_numeric(samples.get("TTM_NET_PROFIT_RAW"), errors="coerce")
    samples = samples.dropna(subset=["NOTICE_DATE", "REPORT_DATE", "TTM_NET_PROFIT_RAW"]).sort_values("NOTICE_DATE")
    if samples.empty:
        return result

    profit_series = pd.Series(samples["TTM_NET_PROFIT_RAW"].values, index=samples["NOTICE_DATE"])
    report_series = pd.Series(samples["REPORT_DATE"].values, index=samples["NOTICE_DATE"])
    current_profit = _align_event_series(profit_series, target_index)
    report_series = report_series.sort_index()
    report_series = report_series[~report_series.index.duplicated(keep="last")]
    current_report = report_series.reindex(report_series.index.union(target_index)).sort_index().ffill().reindex(target_index)
    eps = current_profit / total_shares.reindex(target_index)
    result["pe_ttm"] = close.reindex(target_index) / eps.replace(0, np.nan)
    result["pe_for_stats"] = result["pe_ttm"]

    sample_pe = _sample_pe_values_from_ttm(samples, close, total_shares)
    notice_values = samples["NOTICE_DATE"].to_numpy(dtype="datetime64[ns]")
    report_values = samples["REPORT_DATE"].to_numpy(dtype="datetime64[ns]")

    medians = []
    percentiles = []
    stds = []
    current_vs = []
    for dt, current_pe in result["pe_ttm"].items():
        current_value = _safe_float(current_pe)
        report_cutoff = pd.to_datetime(current_report.loc[dt], errors="coerce")
        if pd.isna(report_cutoff):
            medians.append(np.nan)
            percentiles.append(np.nan)
            stds.append(np.nan)
            current_vs.append(np.nan)
            continue
        start = np.datetime64(pd.Timestamp(dt).normalize() - pd.Timedelta(days=int(365 * 3.5)))
        end = np.datetime64(pd.Timestamp(dt).normalize())
        report_end = np.datetime64(pd.Timestamp(report_cutoff).normalize())
        mask = (
            (notice_values <= end)
            & (report_values >= start)
            & (report_values <= report_end)
            & np.isfinite(sample_pe)
            & (sample_pe > 0)
        )
        values = sample_pe[mask].astype(float).tolist()
        if current_value is not None and current_value > 0:
            values.append(current_value)
        if values:
            median = float(np.median(values))
            medians.append(median)
            percentiles.append(sum(value <= current_value for value in values) / len(values) if current_value and current_value > 0 else np.nan)
            stds.append(float(np.std(values, ddof=0)))
            current_vs.append(current_value / median - 1.0 if current_value and median > 0 else np.nan)
        else:
            medians.append(np.nan)
            percentiles.append(np.nan)
            stds.append(np.nan)
            current_vs.append(np.nan)
    result["pe_3_5y_median"] = medians
    result["pe_3_5y_percentile"] = percentiles
    result["pe_3_5y_std"] = stds
    result["pe_current_vs_median"] = current_vs
    return result


def _sample_pe_values_from_ttm(ttm: pd.DataFrame, close: pd.Series, total_shares: pd.Series) -> np.ndarray:
    close_series = pd.to_numeric(close, errors="coerce").dropna().sort_index()
    share_series = pd.to_numeric(total_shares, errors="coerce").dropna().sort_index()
    if close_series.empty or share_series.empty:
        return np.full(len(ttm), np.nan)

    close_index = close_series.index.to_numpy(dtype="datetime64[ns]")
    share_index = share_series.index.to_numpy(dtype="datetime64[ns]")
    close_values = close_series.to_numpy(dtype=float)
    share_values = share_series.to_numpy(dtype=float)

    values: list[float] = []
    for _, item in ttm.iterrows():
        report_date = np.datetime64(pd.Timestamp(item["REPORT_DATE"]).normalize())
        price_pos = np.searchsorted(close_index, report_date, side="right") - 1
        share_pos = np.searchsorted(share_index, report_date, side="right") - 1
        if price_pos < 0 or share_pos < 0:
            values.append(np.nan)
            continue
        profit = _safe_float(item.get("TTM_NET_PROFIT_RAW"))
        shares = share_values[share_pos]
        price = close_values[price_pos]
        if profit is None or not np.isfinite(shares) or shares <= 0:
            values.append(np.nan)
            continue
        eps = profit / shares
        value = price / eps if eps else np.nan
        values.append(float(value) if np.isfinite(value) else np.nan)
    return np.array(values, dtype=float)


def _pb_history_from_daily_bps(bps: pd.Series, close: pd.Series, dates: pd.DatetimeIndex) -> pd.DataFrame:
    result = pd.DataFrame(index=dates)
    if bps.empty:
        return result
    close_series = pd.to_numeric(close, errors="coerce").dropna().sort_index()
    bps_series = pd.to_numeric(bps, errors="coerce").dropna().sort_index()
    if close_series.empty or bps_series.empty:
        return result
    all_index = close_series.index.union(bps_series.index).sort_values()
    daily_bps = bps_series.reindex(all_index).ffill().reindex(close_series.index)
    daily_pb = close_series / daily_bps.replace(0, np.nan)
    daily_pb = daily_pb.replace([np.inf, -np.inf], np.nan)
    medians = []
    percentiles = []
    stds = []
    current_vs = []
    sample_counts = []
    for dt in dates:
        current_value = _safe_float(daily_pb.loc[dt] if dt in daily_pb.index else np.nan)
        start = pd.Timestamp(dt).normalize() - pd.Timedelta(days=int(365 * 3.5))
        window = daily_pb[(daily_pb.index >= start) & (daily_pb.index <= dt)].dropna()
        window = window[window > 0]
        values = window.to_numpy(dtype=float)
        if len(values) > 0:
            median = float(np.median(values))
            medians.append(median)
            percentiles.append(float((values <= current_value).mean()) if current_value and current_value > 0 else np.nan)
            stds.append(float(np.std(values, ddof=0)))
            current_vs.append(current_value / median - 1.0 if current_value and median > 0 else np.nan)
            sample_counts.append(int(len(values)))
        else:
            medians.append(np.nan)
            percentiles.append(np.nan)
            stds.append(np.nan)
            current_vs.append(np.nan)
            sample_counts.append(0)
    result["pb_3_5y_median"] = medians
    result["pb_3_5y_percentile"] = percentiles
    result["pb_3_5y_std"] = stds
    result["pb_current_vs_median"] = current_vs
    result["pb_3_5y_sample_count"] = sample_counts
    return result


def _chip_factor_history(
    symbol_info: Any,
    stock_root: Path,
    base_dir: Path,
    price: pd.DataFrame,
    target_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    cache_dir = build_cache_dir(symbol_info, CacheKind.CHIP_DISTRIBUTION, base_dir=base_dir, ensure=False)
    cache_path = cache_dir / "chip_distribution.csv"
    source = "cache"
    if cache_path.exists():
        try:
            chip = pd.read_csv(cache_path)
        except Exception as exc:
            LOGGER.warning("读取筹码缓存失败: %s error=%s", cache_path, exc)
            chip = pd.DataFrame()
    else:
        chip = pd.DataFrame()
    if chip.empty:
        return pd.DataFrame()
    if chip.empty or "日期" not in chip.columns:
        return pd.DataFrame()
    chip = chip.copy()
    chip["日期"] = pd.to_datetime(chip["日期"], errors="coerce")
    chip = chip.dropna(subset=["日期"]).sort_values("日期")
    close = pd.to_numeric(price.set_index("日期")["收盘"], errors="coerce")
    records = []
    for _, item in chip.iterrows():
        dt = pd.Timestamp(item["日期"]).normalize()
        close_candidates = close.loc[close.index <= dt]
        latest_price = close_candidates.iloc[-1] if not close_candidates.empty else np.nan
        payload = _chip_row_features(item, latest_price, source=source)
        payload["date"] = dt.strftime("%Y-%m-%d")
        records.append(payload)
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame(records)
    target = pd.DataFrame(index=target_index)
    target["date"] = target.index.strftime("%Y-%m-%d")
    merged = target.merge(frame, on="date", how="left")
    value_columns = [column for column in merged.columns if column != "date"]
    merged[value_columns] = merged[value_columns].ffill()
    return merged


def _write_symbol_history(frame: pd.DataFrame, by_symbol_dir: Path, config: FactorHistoryConfig) -> None:
    symbol = str(frame["symbol"].iloc[0])
    csv_path = by_symbol_dir / f"{symbol}.csv"
    parquet_path = by_symbol_dir / f"{symbol}.parquet"
    if csv_path.exists():
        try:
            existing = pd.read_csv(csv_path)
        except Exception:
            existing = pd.DataFrame()
        if not existing.empty:
            frame = pd.concat([existing, frame], ignore_index=True)
            frame = frame.drop_duplicates(subset=["date", "symbol"], keep="last")
            frame = frame.sort_values("date").reset_index(drop=True)
    if config.write_parquet:
        _write_parquet(frame, parquet_path)
    frame.to_csv(csv_path, index=False)


def _write_date_factor_outputs(
    run_date: str,
    frame: pd.DataFrame,
    paths: SelectionSystemPaths,
    config: FactorHistoryConfig,
) -> dict[str, Path]:
    by_date_dir = paths.base_dir / "factor_store" / "by_date"
    by_date_dir.mkdir(parents=True, exist_ok=True)
    by_date_csv = by_date_dir / f"{run_date}.csv"
    by_date_parquet = by_date_dir / f"{run_date}.parquet"
    selection_dir = paths.ensure_run_dir(run_date)
    selection_csv = selection_dir / "12_factor_snapshot.csv"
    selection_json = selection_dir / "12_factor_snapshot.json"
    frame.to_csv(by_date_csv, index=False)
    frame.to_csv(selection_csv, index=False)
    if config.write_parquet:
        _write_parquet(frame, by_date_parquet)
    save_json_file(
        selection_json,
        _factor_snapshot_payload(
            run_date,
            frame,
            config=FactorStoreConfig(
                max_staleness_days=config.max_staleness_days,
                write_parquet=config.write_parquet,
                write_csv=True,
                max_workers=config.max_workers,
            ),
        ),
    )
    return {
        "factor_by_date_csv": by_date_csv,
        "factor_by_date_parquet": by_date_parquet,
        "factor_snapshot_csv": selection_csv,
        "factor_snapshot_json": selection_json,
    }


def _format_percent(value: Any) -> str | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return f"{numeric * 100:+.2f}%"


def _format_percent_value(value: Any) -> str | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    if abs(numeric) < 1e-8:
        return "0%"
    return f"{numeric:+.2f}%"


def available_price_dates(
    base_dir: str | Path,
    *,
    symbols: list[str],
    start_date: str | None,
    end_date: str | None,
    years: int,
    min_symbol_count: int,
) -> list[str]:
    """Return trading dates covered by local prices for enough universe symbols."""

    base_path = _resolve_base_path(base_dir)
    resolved_end = end_date or _latest_price_date(base_path, symbols)
    if resolved_end is None:
        return []
    resolved_start = start_date or _years_before(resolved_end, years)

    counts: Counter[str] = Counter()
    for symbol in symbols:
        try:
            symbol_info = parse_symbol(symbol)
        except SymbolFormatError:
            continue
        price_path = get_stock_data_dir(symbol_info, base_dir=base_path) / "prices" / "price.csv"
        if not price_path.exists():
            continue
        try:
            frame = pd.read_csv(price_path, usecols=["日期"])
        except Exception as exc:
            LOGGER.warning("读取价格日期失败: %s error=%s", price_path, exc)
            continue
        dates = pd.to_datetime(frame["日期"], errors="coerce").dropna()
        for dt in dates:
            date_text = pd.Timestamp(dt).strftime("%Y-%m-%d")
            if _date_in_range(date_text, start_date=resolved_start, end_date=resolved_end):
                counts[date_text] += 1
    return sorted(date for date, count in counts.items() if count >= min_symbol_count)


def available_basic_info_dates(
    base_dir: str | Path = "data",
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    min_symbol_count: int = 1,
) -> list[str]:
    """Return dates available in basic_info_cache with enough symbol coverage."""

    base_path = _resolve_base_path(base_dir)
    cache_dir = base_path / "basic_info_cache"
    counts: Counter[str] = Counter()
    for path in sorted(cache_dir.glob("basic_info_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            LOGGER.warning("读取 basic_info 日期失败: %s error=%s", path, exc)
            continue
        series = payload.get("Time Series (Daily)") or {}
        if not isinstance(series, dict):
            continue
        for date_text in series:
            if _date_in_range(date_text, start_date=start_date, end_date=end_date):
                counts[date_text] += 1
    return sorted(date for date, count in counts.items() if count >= min_symbol_count)


def available_factor_store_dates(
    base_dir: str | Path = "data",
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    years: int = DEFAULT_HISTORY_YEARS,
    min_symbol_count: int = 1,
) -> list[str]:
    """Return dates already available in factor_store/by_date."""

    base_path = _resolve_base_path(base_dir)
    by_date_dir = base_path / "factor_store" / "by_date"
    if not by_date_dir.exists():
        return []
    discovered = sorted(path.stem for path in by_date_dir.glob("*.csv") if len(path.stem) == 10)
    if not discovered:
        return []
    resolved_end = end_date or discovered[-1]
    resolved_start = start_date or _years_before(resolved_end, years)
    dates: list[str] = []
    for date_text in discovered:
        if not _date_in_range(date_text, start_date=resolved_start, end_date=resolved_end):
            continue
        path = by_date_dir / f"{date_text}.csv"
        try:
            row_count = sum(1 for _ in path.open("r", encoding="utf-8")) - 1
        except Exception as exc:
            LOGGER.warning("读取因子横截面日期失败: %s error=%s", path, exc)
            continue
        if row_count >= min_symbol_count:
            dates.append(date_text)
    return dates


def _compute_basic_info_history(
    paths: SelectionSystemPaths,
    dates: list[str],
    config: FactorHistoryConfig,
) -> None:
    symbols = [stock.symbol for stock in load_master_universe(paths).stocks]
    for run_date in dates:
        payload = basic_info(
            symbols,
            base_dir=paths.base_dir,
            max_workers=config.max_workers,
            price_lookback_days=config.price_lookback_days,
            force_refresh=False,
            force_refresh_financials=False,
            skip_price_refresh=True,
            skip_financial_refresh=True,
            today_time=run_date,
            use_cache=True,
        )
        LOGGER.info(
            "历史 basic_info 已准备: date=%s stocks=%s errors=%s",
            run_date,
            payload.get("stocks_count", 0),
            len(payload.get("errors", {})),
        )


def _build_factor_outputs_for_dates(
    paths: SelectionSystemPaths,
    dates: list[str],
    config: FactorHistoryConfig,
) -> list[dict[str, Any]]:
    built_rows: list[dict[str, Any]] = []
    for run_date in dates:
        if config.skip_existing and _history_outputs_exist(paths, run_date, config):
            built_rows.append(
                {
                    "run_date": run_date,
                    "factor_snapshot_csv": str(paths.run_dir(run_date) / "12_factor_snapshot.csv"),
                    "factor_by_date_csv": str(paths.base_dir / "factor_store" / "by_date" / f"{run_date}.csv"),
                    "factor_scores_csv": str(paths.run_dir(run_date) / "13_factor_scores.csv") if config.build_scores else "",
                    "quant_prefilter_csv": str(paths.run_dir(run_date) / "12_quant_prefilter.csv") if config.build_prefilter else "",
                    "skipped_existing": "true",
                }
            )
            continue
        factor_outputs = build_factor_store_for_date(
            run_date,
            base_dir=paths.base_dir,
            config=FactorStoreConfig(
                max_staleness_days=config.max_staleness_days,
                write_parquet=config.write_parquet,
                write_csv=True,
                max_workers=config.max_workers,
            ),
        )
        score_outputs: dict[str, Path] = {}
        prefilter_outputs: dict[str, Path] = {}
        if config.build_scores:
            score_outputs = build_factor_scores_for_date(
                run_date,
                base_dir=paths.base_dir,
                config=FactorScoringConfig(
                    config_path=config.scoring_config_path,
                    max_staleness_days=config.max_staleness_days,
                ),
                ensure_factor_store=False,
            )
        if config.build_prefilter:
            prefilter_outputs = build_quant_prefilter_for_date(
                run_date,
                base_dir=paths.base_dir,
                config=QuantPrefilterConfig(
                    top_n=config.top_n,
                    max_staleness_days=config.max_staleness_days,
                ),
                ensure_factor_store=False,
            )
        built_rows.append(_history_item(run_date, factor_outputs, score_outputs, prefilter_outputs))
    return built_rows


def _build_existing_factor_outputs_for_dates(
    paths: SelectionSystemPaths,
    dates: list[str],
    config: FactorHistoryConfig,
) -> list[dict[str, Any]]:
    """Build selection-run outputs from already materialized by-date factor CSVs."""

    built_rows: list[dict[str, Any]] = []
    for run_date in dates:
        if config.skip_existing and _history_outputs_exist(paths, run_date, config):
            built_rows.append(
                {
                    "run_date": run_date,
                    "factor_snapshot_csv": str(paths.run_dir(run_date) / "12_factor_snapshot.csv"),
                    "factor_by_date_csv": str(paths.base_dir / "factor_store" / "by_date" / f"{run_date}.csv"),
                    "factor_scores_csv": str(paths.run_dir(run_date) / "13_factor_scores.csv") if config.build_scores else "",
                    "quant_prefilter_csv": str(paths.run_dir(run_date) / "12_quant_prefilter.csv") if config.build_prefilter else "",
                    "skipped_existing": "true",
                }
            )
            continue

        by_date_csv = paths.base_dir / "factor_store" / "by_date" / f"{run_date}.csv"
        if not by_date_csv.exists():
            continue
        try:
            frame = pd.read_csv(by_date_csv)
        except Exception as exc:
            LOGGER.warning("读取已存在因子横截面失败: date=%s path=%s error=%s", run_date, by_date_csv, exc)
            continue
        factor_outputs = _write_date_factor_outputs(run_date, frame, paths, config)

        score_outputs: dict[str, Path] = {}
        prefilter_outputs: dict[str, Path] = {}
        if config.build_scores:
            score_outputs = build_factor_scores_for_date(
                run_date,
                base_dir=paths.base_dir,
                config=FactorScoringConfig(
                    config_path=config.scoring_config_path,
                    max_staleness_days=config.max_staleness_days,
                ),
                ensure_factor_store=False,
            )
        if config.build_prefilter:
            prefilter_outputs = build_quant_prefilter_for_date(
                run_date,
                base_dir=paths.base_dir,
                config=QuantPrefilterConfig(
                    top_n=config.top_n,
                    max_staleness_days=config.max_staleness_days,
                ),
                ensure_factor_store=False,
            )
        built_rows.append(_history_item(run_date, factor_outputs, score_outputs, prefilter_outputs))
    LOGGER.info("已基于现有 factor_store 横截面生成历史输出: dates=%d", len(built_rows))
    return built_rows


def _write_history_manifest(
    paths: SelectionSystemPaths,
    config: FactorHistoryConfig,
    dates: list[str],
    built_rows: list[dict[str, Any]],
) -> dict[str, Path]:
    manifest_path = paths.base_dir / "factor_store" / "history_manifest.json"
    save_json_file(
        manifest_path,
        {
            "schema_version": 2,
            "generated_at": datetime.now().isoformat(),
            "source": config.source,
            "start_date": dates[0] if dates else None,
            "end_date": dates[-1] if dates else None,
            "date_count": len(dates),
            "min_symbol_count": config.min_symbol_count,
            "max_staleness_days": config.max_staleness_days,
            "price_lookback_days": config.price_lookback_days,
            "max_workers": config.max_workers,
            "write_parquet": config.write_parquet,
            "build_scores": config.build_scores,
            "build_prefilter": config.build_prefilter,
            "skip_existing": config.skip_existing,
            "items": built_rows,
        },
    )
    LOGGER.info("历史因子快照补齐完成: source=%s dates=%d manifest=%s", config.source, len(dates), manifest_path)
    return {"factor_history_manifest": manifest_path}


def _history_item(
    run_date: str,
    factor_outputs: Mapping[str, Any],
    score_outputs: Mapping[str, Any],
    prefilter_outputs: Mapping[str, Any],
) -> dict[str, str]:
    return {
        "run_date": run_date,
        "factor_snapshot_csv": str(factor_outputs.get("factor_snapshot_csv", "")),
        "factor_by_date_csv": str(factor_outputs.get("factor_by_date_csv", "")),
        "factor_scores_csv": str(score_outputs.get("factor_scores_csv", "")),
        "quant_prefilter_csv": str(prefilter_outputs.get("quant_prefilter_csv", "")),
    }


def _history_outputs_exist(paths: SelectionSystemPaths, run_date: str, config: FactorHistoryConfig) -> bool:
    run_dir = paths.run_dir(run_date)
    required = [
        paths.base_dir / "factor_store" / "by_date" / f"{run_date}.csv",
        run_dir / "12_factor_snapshot.csv",
    ]
    if config.build_scores:
        required.append(run_dir / "13_factor_scores.csv")
    if config.build_prefilter:
        required.extend(
            [
                run_dir / "12_quant_prefilter.csv",
                run_dir / "12_quant_prefilter_short.csv",
                run_dir / "12_quant_prefilter_long.csv",
            ]
        )
    return all(path.exists() for path in required)


def _latest_price_date(base_dir: Path, symbols: list[str]) -> str | None:
    latest: pd.Timestamp | None = None
    for symbol in symbols:
        try:
            symbol_info = parse_symbol(symbol)
        except SymbolFormatError:
            continue
        price_path = get_stock_data_dir(symbol_info, base_dir=base_dir) / "prices" / "price.csv"
        if not price_path.exists():
            continue
        try:
            frame = pd.read_csv(price_path, usecols=["日期"])
        except Exception:
            continue
        dates = pd.to_datetime(frame["日期"], errors="coerce").dropna()
        if dates.empty:
            continue
        value = pd.Timestamp(dates.max()).normalize()
        if latest is None or value > latest:
            latest = value
    return latest.strftime("%Y-%m-%d") if latest is not None else None


def _years_before(end_date: str, years: int) -> str:
    return (pd.Timestamp(end_date).normalize() - pd.DateOffset(years=max(int(years), 1))).strftime("%Y-%m-%d")


def _date_in_range(date_text: str, *, start_date: str | None, end_date: str | None) -> bool:
    if len(date_text) != 10:
        return False
    if start_date and date_text < start_date:
        return False
    if end_date and date_text > end_date:
        return False
    return True


def _resolve_base_path(base_dir: str | Path) -> Path:
    base_path = Path(base_dir)
    if not base_path.is_absolute():
        base_path = Path.cwd() / base_path
    return base_path


__all__ = [
    "FactorHistoryConfig",
    "available_basic_info_dates",
    "available_factor_store_dates",
    "available_price_dates",
    "build_factor_history",
]
