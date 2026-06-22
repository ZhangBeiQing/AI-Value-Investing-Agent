"""Quantitative prefilter and lightweight score backtest for factor snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from configs.stock_pool import FORCED_SHORT_BOOK_STOCKS
from core.logging import get_logger
from services.selection_system.master_universe import load_master_universe
from services.selection_system.paths import SelectionSystemPaths
from services.selection_system.store import load_json_file, save_json_file
from utlity.stock_utils import SymbolFormatError, get_stock_data_dir, parse_symbol


LOGGER = get_logger("SelectionQuantPrefilter")

DEFAULT_TOP_N = 20
DEFAULT_MIN_AMOUNT = 0.5
DEFAULT_MIN_LIQUIDITY_SCORE = 0.05
DEFAULT_BACKTEST_HOLD_DAYS = (1, 3, 5, 10, 20)
DEFAULT_LONG_BACKTEST_HOLD_DAYS = (20, 60, 120, 250)
SCORE_COLUMNS = {"combined_score", "short_score", "long_score"}
PRIMARY_SCORE_COLUMNS = ("short_score", "long_score")


@dataclass(frozen=True)
class QuantPrefilterConfig:
    top_n: int = DEFAULT_TOP_N
    min_amount: float = DEFAULT_MIN_AMOUNT
    min_liquidity_score: float = DEFAULT_MIN_LIQUIDITY_SCORE
    max_staleness_days: int = 10
    score_columns: tuple[str, str] = PRIMARY_SCORE_COLUMNS


def build_quant_prefilter_for_date(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    config: QuantPrefilterConfig | None = None,
    ensure_factor_store: bool = True,
) -> dict[str, Path]:
    """Build TopN combined/short/long prefilter outputs from factor snapshots."""

    config = config or QuantPrefilterConfig()
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)
    frame = load_factor_snapshot_frame(
        run_date,
        base_dir=paths.base_dir,
        ensure_factor_store=ensure_factor_store,
        max_staleness_days=config.max_staleness_days,
    )
    scored = _eligible_frame(frame, run_date, config=config)
    top_groups = {f"{score_column.removesuffix('_score')}_top": _top_items(scored, score_column, config.top_n) for score_column in config.score_columns}
    force_short_items = _forced_short_items(scored, top_groups.get("short_top", []))
    if force_short_items:
        top_groups["short_top"] = [*top_groups.get("short_top", []), *force_short_items]
    short_frame = _top_frame(top_groups.get("short_top", []), "short")
    long_frame = _top_frame(top_groups.get("long_top", []), "long")
    combined_frame = pd.concat([short_frame, long_frame], ignore_index=True)

    csv_path = paths.run_dir(run_date) / "12_quant_prefilter.csv"
    short_csv_path = paths.run_dir(run_date) / "12_quant_prefilter_short.csv"
    long_csv_path = paths.run_dir(run_date) / "12_quant_prefilter_long.csv"
    json_path = paths.run_dir(run_date) / "12_quant_prefilter.json"
    combined_frame.to_csv(csv_path, index=False)
    short_frame.to_csv(short_csv_path, index=False)
    long_frame.to_csv(long_csv_path, index=False)
    save_json_file(
        json_path,
        {
            "schema_version": 2,
            "run_date": run_date,
            "generated_at": datetime.now().isoformat(),
            "selection_method": "factor_store_short_long_rank_v1",
            "top_n": config.top_n,
            "summary": {
                "scored_count": int(len(scored)),
                "selected_count": int(len(combined_frame)),
                "short_top_count": len(top_groups.get("short_top", [])),
                "long_top_count": len(top_groups.get("long_top", [])),
                "forced_short_count": len(force_short_items),
                "min_amount": config.min_amount,
                "min_liquidity_score": config.min_liquidity_score,
                "max_staleness_days": config.max_staleness_days,
            },
            "items": combined_frame.replace({np.nan: None}).to_dict(orient="records"),
            **top_groups,
        },
    )
    LOGGER.info(
        "量化初筛已生成: run_date=%s scored=%d selected=%d",
        run_date,
        len(scored),
        len(combined_frame),
    )
    return {
        "quant_prefilter_csv": csv_path,
        "quant_prefilter_short_csv": short_csv_path,
        "quant_prefilter_long_csv": long_csv_path,
        "quant_prefilter_json": json_path,
    }


def backtest_quant_prefilter(
    *,
    base_dir: str | Path = "data",
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = DEFAULT_TOP_N,
    score_column: str = "short_score",
    hold_days: Sequence[int] = DEFAULT_BACKTEST_HOLD_DAYS,
    min_amount: float = DEFAULT_MIN_AMOUNT,
    min_liquidity_score: float = DEFAULT_MIN_LIQUIDITY_SCORE,
    output_date: str | None = None,
) -> dict[str, Path]:
    """Backtest equal-weight TopN score portfolios using existing by-date factors."""

    if score_column not in SCORE_COLUMNS:
        raise ValueError(f"score_column 必须是 {sorted(SCORE_COLUMNS)} 之一")

    paths = SelectionSystemPaths.from_base_dir(base_dir)
    factor_dates = _available_factor_dates(paths)
    selected_dates = _select_date_window(factor_dates, start_date=start_date, end_date=end_date)
    detail_rows: list[dict[str, Any]] = []
    price_cache: dict[str, pd.Series] = {}
    config = QuantPrefilterConfig(
        top_n=top_n,
        min_amount=min_amount,
        min_liquidity_score=min_liquidity_score,
    )
    for asof_date in selected_dates:
        frame = load_factor_snapshot_frame(
            asof_date,
            base_dir=paths.base_dir,
            ensure_factor_store=False,
            max_staleness_days=config.max_staleness_days,
        )
        eligible = _eligible_frame(frame, asof_date, config=config)
        selected = _top_items(eligible, score_column, top_n)
        for hold_day in hold_days:
            returns = [
                ret
                for item in selected
                if (ret := _forward_return(item["symbol"], asof_date, hold_day, base_dir=paths.base_dir, price_cache=price_cache)) is not None
            ]
            detail_rows.append(
                {
                    "asof_date": asof_date,
                    "score_column": score_column,
                    "hold_days": int(hold_day),
                    "selected_count": len(selected),
                    "priced_count": len(returns),
                    "equal_weight_return": float(np.mean(returns)) if returns else None,
                    "median_return": float(np.median(returns)) if returns else None,
                    "win_rate": float(np.mean([ret > 0 for ret in returns])) if returns else None,
                    "selected_symbols": ",".join(str(item["symbol"]) for item in selected),
                }
            )

    detail = pd.DataFrame(detail_rows)
    metrics = _backtest_metrics(detail)
    target_date = output_date or (selected_dates[-1] if selected_dates else datetime.now().strftime("%Y-%m-%d"))
    paths.ensure_run_dir(target_date)
    score_stem = score_column.removesuffix("_score")
    summary_path = paths.run_dir(target_date) / f"12_quant_prefilter_backtest_{score_stem}.json"
    detail_path = paths.run_dir(target_date) / f"12_quant_prefilter_backtest_{score_stem}_detail.csv"
    detail.to_csv(detail_path, index=False)
    save_json_file(
        summary_path,
        {
            "schema_version": 1,
            "generated_at": datetime.now().isoformat(),
            "method": "factor_store_topn_forward_return",
            "score_column": score_column,
            "start_date": selected_dates[0] if selected_dates else None,
            "end_date": selected_dates[-1] if selected_dates else None,
            "top_n": top_n,
            "hold_days": [int(day) for day in hold_days],
            "summary": {
                "detail_rows": int(len(detail)),
                "metrics": metrics,
            },
        },
    )
    LOGGER.info("量化初筛回测已生成: summary=%s detail=%s", summary_path, detail_path)
    return {
        "quant_prefilter_backtest": summary_path,
        "quant_prefilter_backtest_detail": detail_path,
    }


def load_factor_snapshot_frame(
    run_date: str,
    *,
    base_dir: str | Path,
    ensure_factor_store: bool,
    max_staleness_days: int,
) -> pd.DataFrame:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    scores_path = paths.run_dir(run_date) / "13_factor_scores.csv"
    snapshot_path = paths.run_dir(run_date) / "12_factor_snapshot.csv"
    by_date_path = paths.base_dir / "factor_store" / "by_date" / f"{run_date}.csv"

    if scores_path.exists():
        return pd.read_csv(scores_path)

    raw_exists = snapshot_path.exists() or by_date_path.exists()
    if ensure_factor_store and raw_exists:
        from services.selection_system.factor_scoring import FactorScoringConfig, build_factor_scores_for_date

        try:
            build_factor_scores_for_date(
                run_date,
                base_dir=paths.base_dir,
                config=FactorScoringConfig(max_staleness_days=max_staleness_days),
                ensure_factor_store=False,
            )
            if scores_path.exists():
                return pd.read_csv(scores_path)
        except Exception:
            LOGGER.warning("自动生成 factor scores 失败，退回使用原始快照", exc_info=True)

    for path in (snapshot_path, by_date_path):
        if path.exists():
            return pd.read_csv(path)

    if ensure_factor_store:
        from services.selection_system.factor_scoring import FactorScoringConfig, build_factor_scores_for_date

        build_factor_scores_for_date(
            run_date,
            base_dir=paths.base_dir,
            config=FactorScoringConfig(max_staleness_days=max_staleness_days),
            ensure_factor_store=True,
        )
        for path in (scores_path, snapshot_path, by_date_path):
            if path.exists():
                return pd.read_csv(path)
    raise FileNotFoundError(f"未找到 {run_date} 的 factor snapshot，请先运行 build-factor-store")


def _eligible_frame(frame: pd.DataFrame, run_date: str, *, config: QuantPrefilterConfig) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    work = frame.copy()
    for field in ("combined_score", "short_score", "long_score", "amount", "latest_volume", "liquidity_score"):
        if field in work.columns:
            work[field] = pd.to_numeric(work[field], errors="coerce")
    amount_field = "amount" if "amount" in work.columns else "latest_volume"
    if amount_field in work.columns:
        work = work[work[amount_field].fillna(0) >= config.min_amount]
    if "liquidity_score" in work.columns:
        work = work[work["liquidity_score"].fillna(0) >= config.min_liquidity_score]
    if "basic_info_asof_date" in work.columns:
        asof = pd.Timestamp(run_date).normalize()
        dates = pd.to_datetime(work["basic_info_asof_date"], errors="coerce")
        staleness = (asof - dates).dt.days
        work = work[(staleness >= 0) & (staleness <= config.max_staleness_days)]
    return work.reset_index(drop=True)


def _top_items(frame: pd.DataFrame, score_column: str, top_n: int) -> list[dict[str, Any]]:
    if frame.empty or score_column not in frame.columns:
        return []
    work = frame.copy()
    work[score_column] = pd.to_numeric(work[score_column], errors="coerce")
    ranked = work.dropna(subset=[score_column]).sort_values(score_column, ascending=False).head(max(int(top_n), 0))
    if score_column == "short_score":
        score_fields = ["short_score"]
    elif score_column == "long_score":
        score_fields = ["long_score"]
    else:
        score_fields = [score_column]
    compact_fields = [
        "symbol",
        "stock_name",
        "stock_type",
        *score_fields,
        "basic_info_asof_date",
        "latest_price",
        "daily_change_pct",
        "amount",
        "latest_volume",
        "pe_ttm",
        "pb",
        "ps",
        "pe_3_5y_percentile",
        "pb_3_5y_percentile",
        "peg",
        "return_3m",
        "return_6m",
        "return_1y",
        "sharpe_3m",
        "max_drawdown_3m",
        "max_drawdown_1y",
        "revenue_growth_yoy",
        "net_income_growth_yoy",
        "gross_margin",
        "net_profit_margin",
        "roe",
        "turnover_rate",
        "avg_turnover_30d",
        "liquidity_score",
        "chip_score",
        "chip_profit_ratio",
        "chip_concentration_70",
        "overhead_pressure_70",
        "short_gate_pass",
        "long_gate_pass",
        "exclude_reason",
    ]
    present = [field for field in compact_fields if field in ranked.columns]
    items = ranked[present].replace({np.nan: None}).to_dict(orient="records")
    for idx, item in enumerate(items, start=1):
        item["rank"] = idx
        item["rank_score_column"] = score_column
    return items


def _forced_short_items(frame: pd.DataFrame, existing_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if frame.empty or not FORCED_SHORT_BOOK_STOCKS:
        return []
    if "symbol" not in frame.columns:
        return []

    existing_symbols = {str(item.get("symbol")) for item in existing_items}
    force_by_symbol = {entry.symbol: entry for entry in FORCED_SHORT_BOOK_STOCKS}
    force_symbols = [entry.symbol for entry in FORCED_SHORT_BOOK_STOCKS if entry.symbol not in existing_symbols]
    if not force_symbols:
        return []

    work = frame[frame["symbol"].astype(str).isin(force_symbols)].copy()
    if work.empty:
        LOGGER.warning("人工强制短线股票未在当日因子表中找到: symbols=%s", ",".join(force_symbols))
        return []

    if "short_score" in work.columns:
        work["short_score"] = pd.to_numeric(work["short_score"], errors="coerce")
        work = work.sort_values("short_score", ascending=False, na_position="last")

    items = _top_items(work, "short_score", len(work))
    for item in items:
        symbol = str(item.get("symbol"))
        config_entry = force_by_symbol.get(symbol)
        item["rank"] = None
        item["rank_score_column"] = "manual_force_short"
        item["force_include_reason"] = config_entry.reason if config_entry else "人工强制纳入短线候选"
        if config_entry and not item.get("stock_name"):
            item["stock_name"] = config_entry.name
    missing_symbols = [symbol for symbol in force_symbols if symbol not in {str(item.get("symbol")) for item in items}]
    if missing_symbols:
        LOGGER.warning("人工强制短线股票未能追加: symbols=%s", ",".join(missing_symbols))
    return items


def _top_frame(items: list[dict[str, Any]], book: str) -> pd.DataFrame:
    frame = pd.DataFrame(items)
    if frame.empty:
        return frame
    frame.insert(0, "book", book)
    frame.insert(1, "quant_rank", range(1, len(frame) + 1))
    return frame


def _available_factor_dates(paths: SelectionSystemPaths) -> list[str]:
    by_date_dir = paths.base_dir / "factor_store" / "by_date"
    if not by_date_dir.exists():
        return []
    return sorted(path.stem for path in by_date_dir.glob("*.csv") if _looks_like_date(path.stem))


def _select_date_window(dates: Sequence[str], *, start_date: str | None, end_date: str | None) -> list[str]:
    if not dates:
        return []
    start = start_date or dates[0]
    end = end_date or dates[-1]
    return [date for date in dates if start <= date <= end]


def _looks_like_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _forward_return(
    symbol: str,
    asof_date: str,
    hold_days: int,
    *,
    base_dir: Path,
    price_cache: dict[str, pd.Series],
) -> float | None:
    prices = price_cache.get(symbol)
    if prices is None:
        prices = _load_close_series(symbol, base_dir=base_dir)
        price_cache[symbol] = prices
    if prices.empty:
        return None
    asof = pd.Timestamp(asof_date).normalize()
    eligible = prices[prices.index <= asof]
    if eligible.empty:
        return None
    start_idx = prices.index.get_loc(eligible.index[-1])
    end_idx = start_idx + int(hold_days)
    if end_idx >= len(prices):
        return None
    start_price = float(prices.iloc[start_idx])
    end_price = float(prices.iloc[end_idx])
    if start_price <= 0:
        return None
    return end_price / start_price - 1.0


def _load_close_series(symbol: str, *, base_dir: Path) -> pd.Series:
    try:
        symbol_info = parse_symbol(symbol)
    except SymbolFormatError:
        return pd.Series(dtype=float)
    price_path = get_stock_data_dir(symbol_info, base_dir=base_dir) / "prices" / "price.csv"
    if not price_path.exists():
        return pd.Series(dtype=float)
    try:
        frame = pd.read_csv(price_path)
    except Exception as exc:
        LOGGER.warning("读取回测价格失败: symbol=%s path=%s error=%s", symbol, price_path, exc)
        return pd.Series(dtype=float)
    if frame.empty or "日期" not in frame.columns or "收盘" not in frame.columns:
        return pd.Series(dtype=float)
    dates = pd.to_datetime(frame["日期"], errors="coerce")
    close = pd.to_numeric(frame["收盘"], errors="coerce")
    series = pd.Series(close.values, index=dates).dropna()
    return series[~series.index.isna()].sort_index()


def _backtest_metrics(detail: pd.DataFrame) -> list[dict[str, Any]]:
    if detail.empty:
        return []
    metrics: list[dict[str, Any]] = []
    for hold_days, group in detail.groupby("hold_days"):
        returns = pd.to_numeric(group["equal_weight_return"], errors="coerce").dropna()
        priced_count = pd.to_numeric(group["priced_count"], errors="coerce").dropna()
        if returns.empty:
            continue
        metrics.append(
            {
                "hold_days": int(hold_days),
                "observation_count": int(len(returns)),
                "avg_equal_weight_return": float(returns.mean()),
                "median_equal_weight_return": float(returns.median()),
                "win_rate": float((returns > 0).mean()),
                "best_return": float(returns.max()),
                "worst_return": float(returns.min()),
                "avg_priced_count": float(priced_count.mean()) if not priced_count.empty else None,
            }
        )
    return metrics


def parse_hold_days(raw: str | None, *, default: Sequence[int] = DEFAULT_BACKTEST_HOLD_DAYS) -> tuple[int, ...]:
    if not raw:
        return tuple(default)
    days: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        value = int(token)
        if value <= 0:
            raise ValueError("hold_days 必须为正整数")
        days.append(value)
    return tuple(days or default)


__all__ = [
    "QuantPrefilterConfig",
    "backtest_quant_prefilter",
    "build_quant_prefilter_for_date",
    "parse_hold_days",
]
