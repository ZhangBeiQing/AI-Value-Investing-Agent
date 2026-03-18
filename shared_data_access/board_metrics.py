"""THS board history cache and quantitative snapshot helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from numbers import Real
from typing import Any, Dict, List, Mapping, Sequence
import json
import math
import time

import akshare as ak
import numpy as np
import pandas as pd
import py_mini_racer
import requests
from akshare.datasets import get_ths_js
from bs4 import BeautifulSoup

from core.logging import get_logger

from .cache_registry import (
    CacheKind,
    build_global_cache_dir,
    record_cache_refresh,
    should_refresh,
)


LOGGER = get_logger("BoardMetrics")

RETURN_WINDOWS = (3, 5, 10, 20, 60, 120, 180)
VOLATILITY_WINDOWS = (20, 60)
RISK_WINDOWS = (20, 60, 120)
UP_DAY_WINDOWS = (5, 10, 20)
TOP_HIT_WINDOWS = (10, 20)
DEFAULT_HISTORY_LOOKBACK_DAYS = 900
DEFAULT_STOCKS_PER_BOARD = 3


def update_board_history_ths_cached(
    *,
    base_dir: str | Path = "data",
    lookback_days: int = DEFAULT_HISTORY_LOOKBACK_DAYS,
    force_refresh: bool = False,
) -> Path:
    """Refresh THS board history cache if stale."""

    cache_dir = build_global_cache_dir(CacheKind.BOARD_HISTORY_THS, base_dir=base_dir, ensure=True)
    universe_path = cache_dir / "universe.csv"
    histories_dir = cache_dir / "histories"
    histories_dir.mkdir(parents=True, exist_ok=True)

    if not should_refresh(cache_dir, CacheKind.BOARD_HISTORY_THS, force_refresh) and universe_path.exists():
        LOGGER.info("复用板块历史缓存: %s", cache_dir)
        return cache_dir

    LOGGER.info("开始刷新 THS 板块历史缓存: lookback_days=%d", lookback_days)
    universe_df = ak.stock_board_industry_name_ths().rename(columns={"name": "board_name", "code": "board_code"})
    universe_df["board_name"] = universe_df["board_name"].astype(str).str.strip()
    universe_df["board_code"] = universe_df["board_code"].astype(str).str.strip()
    universe_df = universe_df.dropna(subset=["board_name", "board_code"]).drop_duplicates("board_code").reset_index(drop=True)

    start_date = (pd.Timestamp.now().normalize() - pd.Timedelta(days=max(lookback_days, 365))).strftime("%Y%m%d")
    end_date = pd.Timestamp.now().normalize().strftime("%Y%m%d")

    success_count = 0
    failure_count = 0
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {
            executor.submit(_fetch_board_history_ths, row.board_name, row.board_code, start_date, end_date): row
            for row in universe_df.itertuples(index=False)
        }
        for future in as_completed(future_map):
            row = future_map[future]
            csv_path = histories_dir / f"{row.board_code}.csv"
            try:
                history_df = future.result()
                history_df.to_csv(csv_path, index=False, encoding="utf-8")
                success_count += 1
            except Exception as exc:
                failure_count += 1
                LOGGER.warning("板块历史抓取失败: board=%s code=%s error=%s", row.board_name, row.board_code, exc)

    universe_df.to_csv(universe_path, index=False, encoding="utf-8")
    record_cache_refresh(
        cache_dir,
        board_count=int(len(universe_df.index)),
        success_count=success_count,
        failure_count=failure_count,
        history_start=start_date,
        history_end=end_date,
    )
    LOGGER.info(
        "THS 板块历史缓存刷新完成: board_count=%d success=%d failure=%d",
        len(universe_df.index),
        success_count,
        failure_count,
    )
    return cache_dir


def build_board_quant_snapshot(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    stocks_per_board: int = DEFAULT_STOCKS_PER_BOARD,
    history_lookback_days: int = DEFAULT_HISTORY_LOOKBACK_DAYS,
    force_refresh_history: bool = False,
    force_refresh_market_snapshot: bool = False,
    force_refresh_snapshot: bool = False,
) -> Dict[str, Any]:
    """Build full-universe THS board quantitative metrics snapshot."""

    metrics_cache_dir = build_global_cache_dir(CacheKind.BOARD_METRICS_THS, base_dir=base_dir, ensure=True)
    snapshot_dir = metrics_cache_dir / "daily_snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"{run_date}.json"

    if snapshot_path.exists() and not force_refresh_snapshot:
        LOGGER.info("复用板块量化快照: %s", snapshot_path)
        return _load_json(snapshot_path, default={})

    history_cache_dir = update_board_history_ths_cached(
        base_dir=base_dir,
        lookback_days=history_lookback_days,
        force_refresh=force_refresh_history,
    )
    market_snapshot = load_or_build_board_market_snapshot(
        run_date,
        base_dir=base_dir,
        stocks_per_board=stocks_per_board,
        force_refresh=force_refresh_market_snapshot,
    )
    snapshot = _build_board_quant_snapshot_payload(
        run_date,
        history_cache_dir=history_cache_dir,
        market_snapshot=market_snapshot,
    )

    latest_path = metrics_cache_dir / "latest.json"
    _save_json(snapshot_path, snapshot)
    _save_json(latest_path, snapshot)
    record_cache_refresh(
        metrics_cache_dir,
        latest_run_date=run_date,
        board_count=len(snapshot.get("boards", [])),
        price_as_of_date=snapshot.get("price_as_of_date"),
    )
    LOGGER.info("板块量化快照已写入: %s", snapshot_path)
    return snapshot


def load_or_build_board_market_snapshot(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    stocks_per_board: int = DEFAULT_STOCKS_PER_BOARD,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """Load cached market snapshot or fetch today's live THS board overview."""

    metrics_cache_dir = build_global_cache_dir(CacheKind.BOARD_METRICS_THS, base_dir=base_dir, ensure=True)
    market_snapshot_dir = metrics_cache_dir / "market_snapshots"
    market_snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = market_snapshot_dir / f"{run_date}.json"

    if snapshot_path.exists() and not force_refresh:
        return _load_json(snapshot_path, default={})

    today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
    if run_date != today_str:
        warning_payload = {
            "schema_version": 1,
            "run_date": run_date,
            "collected_at": "",
            "summary": {
                "board_count": 0,
            },
            "boards": [],
            "source_status": [
                {
                    "source": "ths_board_market_snapshot",
                    "status": "warning",
                    "warning": f"缺少 {run_date} 的板块日度快照缓存，且该日期不是今天，宽度指标将回退为空。",
                }
            ],
        }
        return warning_payload

    LOGGER.info("开始抓取 THS 板块市场快照: run_date=%s stocks_per_board=%d", run_date, stocks_per_board)
    headers = _build_ths_headers()
    summary_df = _fetch_industry_summary_ths(headers=headers)
    name_df = ak.stock_board_industry_name_ths().rename(columns={"name": "板块名称", "code": "板块代码"})
    merged_df = summary_df.rename(columns={"板块": "板块名称"}).merge(name_df, on="板块名称", how="left").copy()

    board_stocks = _fetch_board_stocks_parallel(merged_df, stocks_per_board=stocks_per_board)
    boards: List[Dict[str, Any]] = []
    for _, row in merged_df.iterrows():
        board_name = str(row.get("板块名称") or "").strip()
        board_code = str(row.get("板块代码") or "").strip()
        related_stock_hints = board_stocks.get(board_code, [])
        total_turnover = _normalize_ths_summary_money_value(row.get("总成交额"))
        top3_turnover_value = sum(float(stock.get("turnover_value") or 0.0) for stock in related_stock_hints)
        boards.append(
            {
                "board_name": board_name,
                "board_code": board_code,
                "change_pct": _round_float(row.get("涨跌幅")),
                "total_volume": _round_float(row.get("总成交量")),
                "total_turnover": total_turnover,
                "net_inflow": _normalize_ths_summary_money_value(row.get("净流入")),
                "up_count": _round_int(row.get("上涨家数")),
                "down_count": _round_int(row.get("下跌家数")),
                "average_price": _round_float(row.get("均价")),
                "leading_stock_name": str(row.get("领涨股") or "").strip(),
                "leading_stock_change_pct": _round_float(row.get("领涨股-涨跌幅")),
                "related_stock_hints": related_stock_hints,
                "top3_turnover_value": round(float(top3_turnover_value), 4),
            }
        )

    boards.sort(key=lambda item: item.get("change_pct") if item.get("change_pct") is not None else -math.inf, reverse=True)
    payload = {
        "schema_version": 1,
        "run_date": run_date,
        "collected_at": datetime.now().isoformat(),
        "summary": {
            "board_count": len(boards),
        },
        "boards": boards,
        "source_status": [
            {
                "source": "ths_board_market_snapshot",
                "status": "ok",
                "board_count": len(boards),
            }
        ],
    }
    _save_json(snapshot_path, payload)
    _save_json(metrics_cache_dir / "latest_market_snapshot.json", payload)
    return payload


def select_board_candidates_from_snapshot(
    snapshot: Mapping[str, Any],
    *,
    run_date: str,
    top_n: int,
) -> Dict[str, Any]:
    """Select top up/down boards from a full-universe snapshot."""

    boards = snapshot.get("boards") if isinstance(snapshot, Mapping) else []
    if not isinstance(boards, list):
        boards = []

    valid_boards = [item for item in boards if _coerce_float(item.get("change_pct")) is not None]
    top_up = sorted(valid_boards, key=lambda item: float(item.get("change_pct") or 0.0), reverse=True)[: max(top_n, 0)]
    top_down = sorted(valid_boards, key=lambda item: float(item.get("change_pct") or 0.0))[: max(top_n, 0)]

    selected: List[Dict[str, Any]] = []
    for direction, items in (("up", top_up), ("down", top_down)):
        for rank, item in enumerate(items, start=1):
            selected.append(
                {
                    "board_name": item.get("board_name"),
                    "board_code": item.get("board_code"),
                    "direction": direction,
                    "rank": rank,
                    "change_pct": item.get("change_pct"),
                    "up_count": item.get("up_count"),
                    "down_count": item.get("down_count"),
                    "total_turnover": item.get("total_turnover"),
                    "leading_stock_name": item.get("leading_stock_name"),
                    "leading_stock_change_pct": item.get("leading_stock_change_pct"),
                    "related_stock_hints": list(item.get("related_stock_hints") or []),
                    "quant_metrics": dict(item.get("quant_metrics") or {}),
                    "price_as_of_date": item.get("price_as_of_date"),
                }
            )

    return {
        "schema_version": 1,
        "run_date": run_date,
        "collected_at": datetime.now().isoformat(),
        "summary": {
            "top_up_count": sum(1 for item in selected if item.get("direction") == "up"),
            "top_down_count": sum(1 for item in selected if item.get("direction") == "down"),
        },
        "boards": selected,
        "source_status": list(snapshot.get("source_status", [])) if isinstance(snapshot, Mapping) else [],
    }


def _build_board_quant_snapshot_payload(
    run_date: str,
    *,
    history_cache_dir: Path,
    market_snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    universe_path = history_cache_dir / "universe.csv"
    if not universe_path.exists():
        return {
            "schema_version": 1,
            "run_date": run_date,
            "generated_at": datetime.now().isoformat(),
            "price_as_of_date": "",
            "boards": [],
            "summary": {
                "board_count": 0,
            },
            "source_status": [
                {
                    "source": "ths_board_history",
                    "status": "error",
                    "error": f"缺少板块 universe 文件: {universe_path}",
                }
            ],
        }

    universe_df = pd.read_csv(universe_path)
    universe_df["board_name"] = universe_df["board_name"].astype(str).str.strip()
    universe_df["board_code"] = universe_df["board_code"].astype(str).str.strip()

    close_matrix, available_count = _load_close_matrix(history_cache_dir, universe_df, run_date=run_date)
    if close_matrix.empty:
        return {
            "schema_version": 1,
            "run_date": run_date,
            "generated_at": datetime.now().isoformat(),
            "price_as_of_date": "",
            "boards": [],
            "summary": {
                "board_count": int(len(universe_df.index)),
                "history_available_count": 0,
            },
            "source_status": [
                {
                    "source": "ths_board_history",
                    "status": "error",
                    "error": "板块历史收盘价矩阵为空",
                }
            ] + list(market_snapshot.get("source_status", [])) if isinstance(market_snapshot, Mapping) else [],
        }

    price_as_of_date = close_matrix.index.max().strftime("%Y-%m-%d")
    returns_matrix = close_matrix.pct_change(fill_method=None)
    interval_return_frames = {window: close_matrix / close_matrix.shift(window) - 1 for window in RETURN_WINDOWS}
    interval_rank_series = {
        window: interval_return_frames[window].iloc[-1].rank(ascending=False, method="min")
        for window in RETURN_WINDOWS
    }
    daily_return_ranks = returns_matrix.rank(axis=1, ascending=False, method="min")
    market_map = _build_market_snapshot_map(market_snapshot)

    boards: List[Dict[str, Any]] = []
    for row in universe_df.itertuples(index=False):
        board_code = str(row.board_code).strip()
        board_name = str(row.board_name).strip()
        price_series = close_matrix[board_code].dropna() if board_code in close_matrix.columns else pd.Series(dtype=float)
        return_series = returns_matrix[board_code].dropna() if board_code in returns_matrix.columns else pd.Series(dtype=float)
        metric_payload = _build_board_metric_payload(
            board_code=board_code,
            board_name=board_name,
            price_series=price_series,
            return_series=return_series,
            interval_rank_series=interval_rank_series,
            daily_return_ranks=daily_return_ranks,
            market_context=market_map.get(board_code, {}),
            price_as_of_date=price_as_of_date,
        )
        boards.append(metric_payload)

    boards.sort(key=lambda item: item.get("change_pct") if item.get("change_pct") is not None else -math.inf, reverse=True)
    return {
        "schema_version": 1,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "price_as_of_date": price_as_of_date,
        "boards": boards,
        "summary": {
            "board_count": int(len(universe_df.index)),
            "history_available_count": available_count,
            "price_as_of_date": price_as_of_date,
        },
        "source_status": [
            {
                "source": "ths_board_history",
                "status": "ok",
                "board_count": available_count,
                "price_as_of_date": price_as_of_date,
            }
        ] + list(market_snapshot.get("source_status", [])) if isinstance(market_snapshot, Mapping) else [],
    }


def _build_board_metric_payload(
    *,
    board_code: str,
    board_name: str,
    price_series: pd.Series,
    return_series: pd.Series,
    interval_rank_series: Mapping[int, pd.Series],
    daily_return_ranks: pd.DataFrame,
    market_context: Mapping[str, Any],
    price_as_of_date: str,
) -> Dict[str, Any]:
    latest_return_pct = _round_float(return_series.iloc[-1] * 100) if not return_series.empty else None
    market_change_pct = _coerce_float(market_context.get("change_pct"))
    change_pct = _round_float(market_change_pct if market_change_pct is not None else latest_return_pct)

    interval_returns = {
        f"return_{window}d_pct": _compute_interval_return_pct(price_series, window)
        for window in RETURN_WINDOWS
    }
    interval_rankings = {
        f"rank_{window}d": _coerce_int(interval_rank_series.get(window, pd.Series(dtype=float)).get(board_code))
        for window in RETURN_WINDOWS
    }
    volatility_metrics = {
        f"volatility_{window}d_pct": _compute_annualized_volatility_pct(return_series, window)
        for window in VOLATILITY_WINDOWS
    }
    max_drawdown_metrics = {
        f"max_drawdown_{window}d_pct": _compute_max_drawdown_pct(price_series, window)
        for window in RISK_WINDOWS
    }
    sharpe_metrics = {
        f"sharpe_{window}d": _compute_sharpe_ratio(return_series, window)
        for window in RISK_WINDOWS
    }
    continuity_metrics = {
        f"up_days_{window}d": _compute_up_days(return_series, window)
        for window in UP_DAY_WINDOWS
    }
    up_streak, down_streak = _compute_consecutive_streaks(return_series)
    continuity_metrics["consecutive_up_days"] = up_streak
    continuity_metrics["consecutive_down_days"] = down_streak
    continuity_metrics.update(
        {
            f"top10_hits_{window}d": _compute_top_hits(daily_return_ranks, board_code, window, top_n=10)
            for window in TOP_HIT_WINDOWS
        }
    )

    up_count = _coerce_int(market_context.get("up_count"))
    down_count = _coerce_int(market_context.get("down_count"))
    denominator = (up_count or 0) + (down_count or 0)
    top3_turnover_share_pct = None
    total_turnover = _coerce_float(market_context.get("total_turnover"))
    top3_turnover_value = _coerce_float(market_context.get("top3_turnover_value"))
    if total_turnover and top3_turnover_value is not None:
        top3_turnover_share_pct = _round_float(top3_turnover_value / total_turnover * 100)

    leading_stock_change_pct = _coerce_float(market_context.get("leading_stock_change_pct"))
    breadth_metrics = {
        "up_ratio_pct": _round_float((up_count / denominator) * 100) if denominator > 0 and up_count is not None else None,
        "top3_turnover_share_pct": top3_turnover_share_pct,
        "leader_vs_board_deviation_pct": _round_float(leading_stock_change_pct - change_pct)
        if leading_stock_change_pct is not None and change_pct is not None
        else None,
    }

    phase_metrics = {
        "drawdown_from_20d_high_pct": _compute_drawdown_from_high_pct(price_series, 20),
        "drawdown_from_60d_high_pct": _compute_drawdown_from_high_pct(price_series, 60),
        "breakout_20d_high": _compute_breakout(price_series, 20),
        "breakout_60d_high": _compute_breakout(price_series, 60),
        "return_acceleration_5d_minus_20d_pct": _compute_return_spread(interval_returns, 5, 20),
    }

    return {
        "board_name": board_name,
        "board_code": board_code,
        "price_as_of_date": price_as_of_date,
        "change_pct": change_pct,
        "up_count": up_count,
        "down_count": down_count,
        "total_turnover": total_turnover,
        "leading_stock_name": market_context.get("leading_stock_name"),
        "leading_stock_change_pct": _round_float(leading_stock_change_pct),
        "related_stock_hints": list(market_context.get("related_stock_hints") or []),
        "quant_metrics": {
            "interval_returns": interval_returns,
            "interval_rankings": interval_rankings,
            "volatility": volatility_metrics,
            "max_drawdown": max_drawdown_metrics,
            "sharpe": sharpe_metrics,
            "continuity": continuity_metrics,
            "breadth": breadth_metrics,
            "phase": phase_metrics,
        },
    }


def _load_close_matrix(
    history_cache_dir: Path,
    universe_df: pd.DataFrame,
    *,
    run_date: str,
) -> tuple[pd.DataFrame, int]:
    histories_dir = history_cache_dir / "histories"
    cutoff = pd.Timestamp(run_date)
    series_map: Dict[str, pd.Series] = {}
    available_count = 0

    for row in universe_df.itertuples(index=False):
        board_code = str(row.board_code).strip()
        csv_path = histories_dir / f"{board_code}.csv"
        if not csv_path.exists():
            continue
        try:
            frame = pd.read_csv(csv_path)
        except Exception as exc:
            LOGGER.warning("读取板块历史失败: board_code=%s error=%s", board_code, exc)
            continue
        if "date" not in frame.columns or "close" not in frame.columns:
            continue
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.dropna(subset=["date", "close"])
        frame = frame.loc[frame["date"] <= cutoff].sort_values("date")
        if frame.empty:
            continue
        series_map[board_code] = frame.set_index("date")["close"].astype(float)
        available_count += 1

    if not series_map:
        return pd.DataFrame(), 0

    close_matrix = pd.DataFrame(series_map).sort_index()
    close_matrix.index = pd.to_datetime(close_matrix.index)
    return close_matrix, available_count


def _build_market_snapshot_map(market_snapshot: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    boards = market_snapshot.get("boards") if isinstance(market_snapshot, Mapping) else []
    if not isinstance(boards, list):
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for item in boards:
        if not isinstance(item, Mapping):
            continue
        board_code = str(item.get("board_code") or "").strip()
        if not board_code:
            continue
        result[board_code] = dict(item)
    return result


def _fetch_board_history_ths(
    board_name: str,
    board_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    history_df = ak.stock_board_industry_index_ths(
        symbol=board_name,
        start_date=start_date,
        end_date=end_date,
    )
    if history_df.empty:
        raise ValueError(f"板块历史为空: {board_name}")
    frame = history_df.rename(
        columns={
            "日期": "date",
            "开盘价": "open",
            "最高价": "high",
            "最低价": "low",
            "收盘价": "close",
            "成交量": "volume",
            "成交额": "turnover",
        }
    ).copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    numeric_columns = ["open", "high", "low", "close", "volume", "turnover"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    frame["board_name"] = board_name
    frame["board_code"] = board_code
    return frame


def _fetch_board_stocks_parallel(
    merged_df: pd.DataFrame,
    *,
    stocks_per_board: int,
) -> Dict[str, List[Dict[str, Any]]]:
    board_stocks: Dict[str, List[Dict[str, Any]]] = {}
    tasks: List[tuple[str, str]] = []
    for _, row in merged_df.iterrows():
        board_name = str(row.get("板块名称") or "").strip()
        board_code = str(row.get("板块代码") or "").strip()
        if board_name and board_code:
            tasks.append((board_name, board_code))

    with ThreadPoolExecutor(max_workers=6) as executor:
        future_map = {
            executor.submit(_fetch_top_turnover_stocks, board_name, board_code, stocks_per_board): board_code
            for board_name, board_code in tasks
        }
        for future in as_completed(future_map):
            board_code = future_map[future]
            try:
                board_stocks[board_code] = future.result()
            except Exception as exc:
                LOGGER.warning("板块成分股抓取失败: board_code=%s error=%s", board_code, exc)
                board_stocks[board_code] = []
    return board_stocks


def _fetch_industry_summary_ths(*, headers: Mapping[str, str]) -> pd.DataFrame:
    main_url = "https://q.10jqka.com.cn/thshy/"
    main_html = _request_ths_table_html(main_url, headers=headers)
    soup = BeautifulSoup(main_html, "lxml")
    page_info = soup.find(name="span", attrs={"class": "page_info"})
    total_pages = int(page_info.text.split("/")[1]) if page_info and "/" in page_info.text else 1

    page_frames = []
    for page in range(1, total_pages + 1):
        page_url = f"http://q.10jqka.com.cn/thshy/index/field/199112/order/desc/page/{page}/ajax/1/"
        page_html = _request_ths_table_html(page_url, headers=headers)
        page_frames.append(pd.read_html(StringIO(page_html))[0])

    summary_df = pd.concat(page_frames, ignore_index=True)
    summary_df.columns = [
        "序号",
        "板块",
        "涨跌幅",
        "总成交量",
        "总成交额",
        "净流入",
        "上涨家数",
        "下跌家数",
        "均价",
        "领涨股",
        "领涨股-最新价",
        "领涨股-涨跌幅",
    ]
    numeric_columns = [
        "序号",
        "涨跌幅",
        "总成交量",
        "总成交额",
        "净流入",
        "上涨家数",
        "下跌家数",
        "均价",
        "领涨股-最新价",
        "领涨股-涨跌幅",
    ]
    for column in numeric_columns:
        summary_df[column] = pd.to_numeric(summary_df[column], errors="coerce")
    return summary_df


def _fetch_top_turnover_stocks(board_name: str, board_code: str, stocks_per_board: int) -> List[Dict[str, Any]]:
    url = (
        f"http://q.10jqka.com.cn/thshy/detail/code/{board_code}/"
        "field/199112/order/desc/page/1/ajax/1/"
    )
    html = _request_ths_table_html(url, max_retries=4)
    table = pd.read_html(StringIO(html))[0]
    table["代码"] = table["代码"].astype(str).str.zfill(6)
    table["成交额数值"] = table["成交额"].apply(_parse_turnover)
    table["涨跌幅(%)"] = pd.to_numeric(table["涨跌幅(%)"], errors="coerce")
    table["现价"] = pd.to_numeric(table["现价"], errors="coerce")
    table = table.sort_values("成交额数值", ascending=False).head(max(stocks_per_board, 0)).reset_index(drop=True)

    return [
        {
            "symbol": _normalize_cn_symbol(row["代码"]),
            "name": str(row["名称"]),
            "turnover": str(row["成交额"]),
            "turnover_value": _round_float(row["成交额数值"]),
            "latest_price": _round_float(row["现价"]),
            "change_pct": _round_float(row["涨跌幅(%)"]),
            "role_hint": "成交额前排",
        }
        for _, row in table.iterrows()
    ]


def _request_ths_table_html(url: str, headers: Mapping[str, str] | None = None, max_retries: int = 3) -> str:
    last_exception: Exception | None = None
    current_headers = dict(headers or _build_ths_headers())
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=current_headers, timeout=20)
            response.raise_for_status()
            if "<table" not in response.text:
                raise ValueError("页面未返回表格内容")
            return response.text
        except Exception as exc:
            last_exception = exc
            if attempt < max_retries - 1:
                current_headers = _build_ths_headers()
                time.sleep(0.8 * (attempt + 1))
    if last_exception is None:
        raise RuntimeError("同花顺表格请求失败")
    raise last_exception


def _build_ths_headers() -> Dict[str, str]:
    with open(get_ths_js("ths.js"), encoding="utf-8") as file:
        js_content = file.read()
    js_context = py_mini_racer.MiniRacer()
    js_context.eval(js_content)
    v_code = js_context.call("v")
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/89.0.4389.90 Safari/537.36"
        ),
        "Cookie": f"v={v_code}",
    }


def _compute_interval_return_pct(price_series: pd.Series, window: int) -> float | None:
    if price_series.empty or len(price_series.index) <= window:
        return None
    latest_price = _coerce_float(price_series.iloc[-1])
    base_price = _coerce_float(price_series.iloc[-(window + 1)])
    if latest_price is None or base_price is None or base_price <= 0:
        return None
    return _round_float((latest_price / base_price - 1) * 100)


def _compute_annualized_volatility_pct(return_series: pd.Series, window: int) -> float | None:
    window_returns = return_series.dropna().tail(window)
    if len(window_returns.index) < 2:
        return None
    std_return = window_returns.std(ddof=0)
    if pd.isna(std_return):
        return None
    return _round_float(float(std_return) * np.sqrt(252) * 100)


def _compute_sharpe_ratio(return_series: pd.Series, window: int) -> float | None:
    window_returns = return_series.dropna().tail(window)
    if len(window_returns.index) < 2:
        return None
    std_return = window_returns.std(ddof=0)
    if pd.isna(std_return) or std_return <= 0:
        return 0.0
    mean_return = window_returns.mean()
    return _round_float(float(mean_return / std_return) * np.sqrt(252))


def _compute_max_drawdown_pct(price_series: pd.Series, window: int) -> float | None:
    prices = price_series.dropna().tail(window + 1)
    if len(prices.index) < 2:
        return None
    running_max = prices.cummax()
    drawdowns = (prices / running_max) - 1
    if drawdowns.empty:
        return None
    return _round_float(float(drawdowns.min()) * 100)


def _compute_up_days(return_series: pd.Series, window: int) -> int:
    return int((return_series.dropna().tail(window) > 0).sum())


def _compute_consecutive_streaks(return_series: pd.Series) -> tuple[int, int]:
    up_streak = 0
    down_streak = 0
    for value in reversed(return_series.dropna().tolist()):
        numeric = _coerce_float(value)
        if numeric is None or numeric == 0:
            break
        if numeric > 0:
            if down_streak > 0:
                break
            up_streak += 1
            continue
        if up_streak > 0:
            break
        down_streak += 1
    return up_streak, down_streak


def _compute_top_hits(daily_return_ranks: pd.DataFrame, board_code: str, window: int, *, top_n: int) -> int:
    if board_code not in daily_return_ranks.columns:
        return 0
    series = daily_return_ranks[board_code].dropna().tail(window)
    if series.empty:
        return 0
    return int((series <= top_n).sum())


def _compute_drawdown_from_high_pct(price_series: pd.Series, window: int) -> float | None:
    prices = price_series.dropna().tail(window)
    if prices.empty:
        return None
    latest_price = _coerce_float(prices.iloc[-1])
    rolling_high = _coerce_float(prices.max())
    if latest_price is None or rolling_high is None or rolling_high <= 0:
        return None
    return _round_float((latest_price / rolling_high - 1) * 100)


def _compute_breakout(price_series: pd.Series, window: int) -> bool | None:
    prices = price_series.dropna()
    if len(prices.index) <= window:
        return None
    latest_price = _coerce_float(prices.iloc[-1])
    previous_high = _coerce_float(prices.iloc[-(window + 1):-1].max())
    if latest_price is None or previous_high is None:
        return None
    return bool(latest_price > previous_high)


def _compute_return_spread(interval_returns: Mapping[str, float | None], short_window: int, long_window: int) -> float | None:
    short_value = _coerce_float(interval_returns.get(f"return_{short_window}d_pct"))
    long_value = _coerce_float(interval_returns.get(f"return_{long_window}d_pct"))
    if short_value is None or long_value is None:
        return None
    return _round_float(short_value - long_value)


def _normalize_cn_symbol(code: Any) -> str:
    text = str(code or "").strip().zfill(6)
    if not text.isdigit():
        return text
    suffix = ".SH" if text.startswith(("5", "6", "9")) else ".SZ"
    return f"{text}{suffix}"


def _parse_turnover(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    text = str(value).strip().replace(",", "")
    if not text or text == "--":
        return 0.0
    if text.endswith("亿"):
        return float(text[:-1]) * 1e8
    if text.endswith("万"):
        return float(text[:-1]) * 1e4
    return float(text)


def _normalize_ths_summary_money_value(value: Any) -> float | None:
    numeric = _coerce_float(value)
    if numeric is None:
        return None
    if abs(numeric) < 1e7:
        return _round_float(numeric * 1e8)
    return _round_float(numeric)


def _round_float(value: Any) -> float | None:
    numeric = _coerce_float(value)
    if numeric is None:
        return None
    return round(numeric, 4)


def _coerce_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_int(value: Any) -> int | None:
    numeric = _coerce_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _coerce_int(value: Any) -> int | None:
    numeric = _coerce_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _load_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, ValueError):
        return default


def _normalize_json_numbers(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: _normalize_json_numbers(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_normalize_json_numbers(item) for item in payload]
    if isinstance(payload, tuple):
        return [_normalize_json_numbers(item) for item in payload]
    if isinstance(payload, bool) or payload is None or isinstance(payload, int):
        return payload
    if isinstance(payload, Real):
        numeric = float(payload)
        if not math.isfinite(numeric):
            return None
        return round(numeric, 4)
    return payload


def _save_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_json_numbers(payload)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")


__all__ = [
    "build_board_quant_snapshot",
    "load_or_build_board_market_snapshot",
    "select_board_candidates_from_snapshot",
    "update_board_history_ths_cached",
]
