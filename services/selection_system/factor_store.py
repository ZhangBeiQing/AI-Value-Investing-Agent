"""Build factor-store views from existing local datasets.

This module builds two complementary views:

- by_symbol: one file per symbol, tracking factor history over time.
- by_date: one file per date, comparing all symbols on the same as-of date.

The factor store is a derived feature layer. It reads existing raw caches and
analysis artifacts, but does not fetch external data directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from core.logging import get_logger
from services.selection_system.master_universe import load_master_universe
from services.selection_system.paths import SelectionSystemPaths
from services.selection_system.store import save_json_file
from utlity.stock_utils import SymbolFormatError, get_stock_data_dir, parse_symbol


LOGGER = get_logger("SelectionFactorStore")

DEFAULT_MAX_STALENESS_DAYS = 10
PARQUET_ENGINE = "pyarrow"

HIGHER_BETTER = {
    "roe",
    "gross_margin",
    "net_profit_margin",
    "revenue_growth_yoy",
    "net_income_growth_yoy",
    "return_3m",
    "return_6m",
    "return_1y",
    "sharpe_3m",
    "sharpe_6m",
    "sharpe_1y",
    "max_drawdown_3m",
    "max_drawdown_6m",
    "max_drawdown_1y",
    "amount",
    "turnover_rate",
    "avg_turnover_30d",
    "liquidity_score",
    "ma_bullish_score",
    "volume_ratio_5d_20d",
    "macd",
    "deduct_net_income_growth_yoy",
}

LOWER_BETTER = {
    "pe_ttm",
    "pb",
    "ps",
    "peg",
    "pe_3_5y_percentile",
    "pe_current_vs_median",
    "price_position_3_5y",
    "price_percentile_3_5y",
    "price_bucket_index_3_5y",
    "volatility_20d",
    "volatility_60d",
}


@dataclass(frozen=True)
class FactorStoreConfig:
    """Configuration for building a factor snapshot."""

    max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS
    write_parquet: bool = True
    write_csv: bool = True


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
    for stock in universe.stocks:
        try:
            row = build_symbol_factor_row(
                stock.symbol,
                run_date,
                stock_name=stock.name,
                sector=stock.sector,
                industry=stock.industry,
                base_dir=paths.base_dir,
                max_staleness_days=config.max_staleness_days,
            )
        except Exception as exc:  # pragma: no cover - defensive aggregation path
            LOGGER.warning("因子行构建失败: symbol=%s error=%s", stock.symbol, exc)
            continue
        if row is None:
            continue
        rows.append(row)

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = _normalize_numeric_columns(frame)
        frame = _add_scores(frame)
        frame = frame.sort_values(["combined_score", "short_score", "long_score"], ascending=False).reset_index(drop=True)
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
    save_json_file(
        manifest_path,
        {
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "latest_run_date": run_date,
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
    }


def build_symbol_factor_row(
    symbol: str,
    run_date: str,
    *,
    stock_name: str = "",
    sector: str = "",
    industry: str = "",
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
        return None

    row: dict[str, Any] = {
        "date": run_date,
        "symbol": symbol,
        "stock_name": stock_name or symbol_info.stock_name or basic_row.get("stock_name") or symbol,
        "sector": sector,
        "industry": industry,
    }
    row.update(basic_row)

    price_features = _price_features(stock_root / "prices" / "price.csv", run_date)
    row.update(price_features)

    technical_features = _technical_features(stock_root / "analysis", run_date)
    row.update(technical_features)

    valuation_features = _valuation_distribution_features(stock_root / "pe_pb_analysis", run_date, row.get("latest_price"))
    row.update(valuation_features)

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

    if len(close) >= 2:
        result["daily_return_price"] = latest_close / float(close.iloc[-2]) - 1.0

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
    skip = {"date", "symbol", "stock_name", "sector", "industry", "daily_change_pct", "basic_info_asof_date", "price_asof_date", "technical_asof_date", "valuation_asof_file"}
    for col in frame.columns:
        if col in skip:
            continue
        frame[col] = pd.to_numeric(frame[col], errors="ignore")
    return frame


def _add_scores(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    for field in HIGHER_BETTER:
        if field in frame.columns:
            frame[f"score_{field}"] = _rank_score(frame[field], lower_better=False)
    for field in LOWER_BETTER:
        if field in frame.columns:
            frame[f"score_{field}"] = _rank_score(frame[field], lower_better=True, positive_only=field in {"pe_ttm", "pb", "ps", "peg"})

    frame["valuation_score"] = _mean_scores(
        frame,
        [
            "score_pe_ttm",
            "score_pb",
            "score_ps",
            "score_peg",
            "score_pe_3_5y_percentile",
            "score_pe_current_vs_median",
            "score_price_position_3_5y",
            "score_price_percentile_3_5y",
            "score_price_bucket_index_3_5y",
        ],
    )
    frame["fundamental_score"] = _mean_scores(
        frame,
        [
            "score_roe",
            "score_gross_margin",
            "score_net_profit_margin",
            "score_revenue_growth_yoy",
            "score_net_income_growth_yoy",
        ],
    )
    frame["trend_score"] = _mean_scores(
        frame,
        [
            "score_return_3m",
            "score_return_6m",
            "score_return_1y",
            "score_sharpe_3m",
            "score_sharpe_6m",
            "score_ma_bullish_score",
        ],
    )
    if "rsi_14" in frame.columns:
        frame["rsi_range_score"] = frame["rsi_14"].map(_rsi_range_score).fillna(0.5)
    else:
        frame["rsi_range_score"] = 0.5
    frame["technical_score"] = _mean_scores(
        frame,
        [
            "score_ma_bullish_score",
            "score_macd",
            "rsi_range_score",
            "score_volume_ratio_5d_20d",
        ],
    )
    frame["liquidity_factor_score"] = _mean_scores(
        frame,
        [
            "score_amount",
            "score_turnover_rate",
            "score_avg_turnover_30d",
            "score_liquidity_score",
        ],
    )
    frame["risk_drawdown_score"] = _mean_scores(
        frame,
        [
            "score_max_drawdown_3m",
            "score_max_drawdown_6m",
            "score_max_drawdown_1y",
            "score_volatility_20d",
            "score_volatility_60d",
        ],
    )
    # Placeholders for Phase 3 factors. They remain neutral until raw datasets exist.
    frame["event_board_score"] = 0.5
    frame["money_chip_score"] = 0.5

    frame["short_score"] = (
        0.25 * frame["trend_score"]
        + 0.20 * frame["technical_score"]
        + 0.15 * frame["liquidity_factor_score"]
        + 0.15 * frame["risk_drawdown_score"]
        + 0.15 * frame["money_chip_score"]
        + 0.10 * frame["event_board_score"]
    )
    frame["long_score"] = (
        0.30 * frame["fundamental_score"]
        + 0.25 * frame["valuation_score"]
        + 0.20 * _mean_scores(frame, ["score_revenue_growth_yoy", "score_net_income_growth_yoy", "score_deduct_net_income_growth_yoy"])
        + 0.10 * frame["trend_score"]
        + 0.10 * frame["risk_drawdown_score"]
        + 0.05 * frame["event_board_score"]
    )
    frame["combined_score"] = 0.5 * frame["short_score"] + 0.5 * frame["long_score"]
    return frame


def _rank_score(series: pd.Series, *, lower_better: bool, positive_only: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if positive_only:
        values = values.where(values > 0)
    if values.notna().sum() <= 1:
        return pd.Series(0.5, index=series.index)
    ranks = values.rank(pct=True, ascending=not lower_better)
    return ranks.fillna(0.5)


def _mean_scores(frame: pd.DataFrame, fields: Sequence[str]) -> pd.Series:
    present = [field for field in fields if field in frame.columns]
    if not present:
        return pd.Series(0.5, index=frame.index)
    return frame[present].mean(axis=1).fillna(0.5)


def _rsi_range_score(value: Any) -> float:
    rsi = _safe_float(value)
    if rsi is None:
        return 0.5
    if 45 <= rsi <= 65:
        return 1.0
    if 35 <= rsi < 45 or 65 < rsi <= 75:
        return 0.7
    if 25 <= rsi < 35 or 75 < rsi <= 85:
        return 0.35
    return 0.15


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


__all__ = [
    "FactorStoreConfig",
    "build_factor_store_for_date",
    "build_symbol_factor_row",
]
