"""Prepare local-agent inputs and merge candidate pools for the selection system."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import pandas as pd

from configs.stock_pool import TRACKED_A_STOCKS, TRACKED_SYMBOLS
from core.logging import get_logger
from services.selection_system.announcement_summary import load_or_build_recent_company_announcements
from services.snapshot.basic_snapshot import (
    DEFAULT_PRICE_LOOKBACK_DAYS,
    basic_info,
    load_basic_snapshot_from_cache,
)
from shared_data_access.data_access import SharedDataAccess
from utlity.stock_utils import parse_symbol

from .master_universe import load_master_universe
from .paths import SelectionSystemPaths
from .shared_context import render_shared_selection_context
from .store import load_json_file, save_json_file

LOGGER = get_logger("SelectionCandidates")
SHORT_BOOK_ANNOUNCEMENT_LOOKBACK_DAYS = 3
LONG_BOOK_ANNOUNCEMENT_LOOKBACK_DAYS = 30
SNAPSHOT_PRICE_LOOKBACK_DAYS = DEFAULT_PRICE_LOOKBACK_DAYS

POSITIVE_ANNOUNCEMENT_KEYWORDS = (
    "回购",
    "中标",
    "订单",
    "签订",
    "获批",
    "增持",
    "产销",
    "分红",
    "预增",
    "扭亏",
)
NEGATIVE_ANNOUNCEMENT_KEYWORDS = (
    "问询",
    "监管",
    "处罚",
    "诉讼",
    "仲裁",
    "减持",
    "冻结",
    "质押",
    "立案",
    "减值",
    "亏损",
    "下滑",
    "终止",
    "延期",
    "辞职",
)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


def _resolve_previous_book_decision_path(
    run_date: str,
    *,
    base_dir: Path,
    book_type: str,
) -> Path | None:
    skill_runs_dir = base_dir / "skill_runs"
    if not skill_runs_dir.exists():
        return None

    candidates = sorted(
        path / book_type / "05_decision.json"
        for path in skill_runs_dir.iterdir()
        if path.is_dir()
        and path.name < run_date
        and (path / book_type / "05_decision.json").exists()
    )
    if not candidates:
        return None
    return candidates[-1]


def _load_book_current_holdings(
    *,
    base_dir: Path,
    signature: str,
) -> tuple[Path, List[Dict[str, Any]]]:
    position_path = base_dir / "agent_data" / signature / "position" / "position.jsonl"
    if not position_path.exists():
        return position_path, []

    try:
        lines = [
            line.strip()
            for line in position_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except OSError:
        return position_path, []
    if not lines:
        return position_path, []

    try:
        latest_record = json.loads(lines[-1])
    except Exception:
        LOGGER.warning("读取持仓文件失败，忽略长期池持仓保留提示: %s", position_path)
        return position_path, []

    positions = latest_record.get("positions") if isinstance(latest_record, Mapping) else {}
    if not isinstance(positions, Mapping):
        return position_path, []

    holdings: List[Dict[str, Any]] = []
    for symbol, qty in positions.items():
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol or normalized_symbol.upper() == "CASH":
            continue
        try:
            shares = int(float(qty))
        except (TypeError, ValueError):
            continue
        if shares <= 0:
            continue
        try:
            stock_name = parse_symbol(normalized_symbol).stock_name
        except Exception:
            stock_name = normalized_symbol
        holdings.append(
            {
                "symbol": normalized_symbol,
                "stock_name": stock_name,
                "shares": shares,
            }
        )

    holdings.sort(key=lambda item: item["symbol"])
    return position_path, holdings


def build_candidate_pools(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    short_count: int = 15,
    long_count: int = 15,
) -> Dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    outputs = prepare_candidate_agent_inputs(
        run_date,
        base_dir=base_dir,
        short_count=short_count,
        long_count=long_count,
    )
    LOGGER.info("短期选股头本地输入已写入: %s", outputs["short_book_input_markdown"])
    LOGGER.info("长期选股头本地输入已写入: %s", outputs["long_book_input_markdown"])
    return outputs


def prepare_candidate_agent_inputs(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    short_count: int = 15,
    long_count: int = 15,
) -> Dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    payloads = collect_candidate_pool_inputs(
        run_date,
        base_dir=base_dir,
        short_count=short_count,
        long_count=long_count,
    )
    save_json_file(paths.run_short_book_input_payload_path(run_date), payloads["short_payload"])
    save_json_file(paths.run_long_book_input_payload_path(run_date), payloads["long_payload"])
    paths.run_short_book_input_markdown_path(run_date).write_text(payloads["short_markdown"], encoding="utf-8")
    paths.run_long_book_input_markdown_path(run_date).write_text(payloads["long_markdown"], encoding="utf-8")
    for stale in (
        paths.run_short_book_candidates_path(run_date),
        paths.run_long_book_candidates_path(run_date),
        paths.run_candidate_merge_path(run_date),
        paths.run_deep_research_queue_path(run_date),
    ):
        if stale.exists():
            stale.unlink()
    return {
        "short_book_input_markdown": paths.run_short_book_input_markdown_path(run_date),
        "short_book_input_payload": paths.run_short_book_input_payload_path(run_date),
        "long_book_input_markdown": paths.run_long_book_input_markdown_path(run_date),
        "long_book_input_payload": paths.run_long_book_input_payload_path(run_date),
        "short_book_candidates": paths.run_short_book_candidates_path(run_date),
        "long_book_candidates": paths.run_long_book_candidates_path(run_date),
    }


def collect_candidate_pool_inputs(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    short_count: int = 15,
    long_count: int = 15,
) -> Dict[str, Any]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    previous_short_book_decision = _resolve_previous_book_decision_path(
        run_date,
        base_dir=paths.base_dir,
        book_type="short_book",
    )
    previous_long_book_decision = _resolve_previous_book_decision_path(
        run_date,
        base_dir=paths.base_dir,
        book_type="long_book",
    )
    long_book_position_path, current_long_book_holdings = _load_book_current_holdings(
        base_dir=paths.base_dir,
        signature="book-long_book",
    )
    universe = load_master_universe(paths)
    hot_news_state = load_json_file(paths.run_hot_news_state_path(run_date), default={}) or {}
    board_heat_state = load_json_file(paths.run_board_heat_state_path(run_date), default={}) or {}
    announcements_payload = load_or_build_recent_company_announcements(
        run_date,
        base_dir=base_dir,
        refresh_missing=False,
    )
    short_announcements_payload = _filter_announcements_payload(
        announcements_payload,
        run_date=run_date,
        lookback_days=SHORT_BOOK_ANNOUNCEMENT_LOOKBACK_DAYS,
    )
    long_announcements_payload = _filter_announcements_payload(
        announcements_payload,
        run_date=run_date,
        lookback_days=LONG_BOOK_ANNOUNCEMENT_LOOKBACK_DAYS,
    )
    universe_symbols = [stock.symbol for stock in universe.stocks]
    snapshot_payload = _ensure_universe_snapshot_coverage(
        run_date,
        universe_symbols,
        base_dir=base_dir,
    )
    snapshot_frame = _snapshot_frame(snapshot_payload)
    short_context = _build_context_maps(hot_news_state, board_heat_state, short_announcements_payload)
    long_context = _build_context_maps(hot_news_state, board_heat_state, long_announcements_payload)
    short_shared_context_text = render_shared_selection_context(
        run_date,
        base_dir=base_dir,
        announcements_payload_override=short_announcements_payload,
    )
    long_shared_context_text = render_shared_selection_context(
        run_date,
        base_dir=base_dir,
        announcements_payload_override=long_announcements_payload,
    )

    short_payload = _build_local_agent_input_payload(
        pool_type="short_book",
        run_date=run_date,
        universe=universe,
        snapshot_frame=snapshot_frame,
        context=short_context,
        shared_context_text=short_shared_context_text,
        required_count=short_count,
    )
    long_payload = _build_local_agent_input_payload(
        pool_type="long_book",
        run_date=run_date,
        universe=universe,
        snapshot_frame=snapshot_frame,
        context=long_context,
        shared_context_text=long_shared_context_text,
        required_count=long_count,
    )

    return {
        "short_payload": short_payload,
        "long_payload": long_payload,
        "short_markdown": _build_local_agent_markdown(
            pool_type="short_book",
            run_date=run_date,
            payload_path=paths.run_short_book_input_payload_path(run_date),
            output_path=paths.run_short_book_candidates_path(run_date),
            required_count=short_count,
            previous_book_decision_path=previous_short_book_decision,
        ),
        "long_markdown": _build_local_agent_markdown(
            pool_type="long_book",
            run_date=run_date,
            payload_path=paths.run_long_book_input_payload_path(run_date),
            output_path=paths.run_long_book_candidates_path(run_date),
            required_count=long_count,
            previous_book_decision_path=previous_long_book_decision,
            current_holdings=current_long_book_holdings,
            holdings_source_path=long_book_position_path,
        ),
    }


def collect_candidate_pools(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    short_count: int = 15,
    long_count: int = 15,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    raise RuntimeError("本地 agent 方案下，build-candidate-pools 只负责准备输入，不再直接产出 08/09 JSON。")


def merge_candidate_pools(
    run_date: str,
    *,
    base_dir: str | Path = "data",
) -> Dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    if not paths.run_short_book_candidates_path(run_date).exists() or not paths.run_long_book_candidates_path(run_date).exists():
        raise FileNotFoundError(
            "未找到 08/09 候选池 JSON。请先运行 build-candidate-pools 生成本地 agent 输入，"
            "再由本地 agent 产出 08_short_book_candidates.json 和 09_long_book_candidates.json。"
        )
    short_payload = load_json_file(paths.run_short_book_candidates_path(run_date), default={}) or {}
    long_payload = load_json_file(paths.run_long_book_candidates_path(run_date), default={}) or {}
    merged_payload, queue_payload = _merge_pools(run_date, short_payload, long_payload)
    save_json_file(paths.run_candidate_merge_path(run_date), merged_payload)
    save_json_file(paths.run_deep_research_queue_path(run_date), queue_payload)
    LOGGER.info("候选池 merge 已写入: %s", paths.run_candidate_merge_path(run_date))
    LOGGER.info("深研队列已写入: %s", paths.run_deep_research_queue_path(run_date))
    return {
        "candidate_merge": paths.run_candidate_merge_path(run_date),
        "deep_research_queue": paths.run_deep_research_queue_path(run_date),
    }


def _build_short_candidates(
    snapshot_frame: pd.DataFrame,
    context: Dict[str, Any],
    *,
    count: int,
) -> List[Dict[str, Any]]:
    df = snapshot_frame.copy()
    df["theme_score"] = df["symbol"].map(lambda s: min(len(context["theme_map"].get(s, [])) / 2.0, 1.0))
    df["board_score"] = df["symbol"].map(lambda s: min(len(context["board_map"].get(s, [])) / 2.0, 1.0))
    df["announcement_positive"] = df["symbol"].map(lambda s: _announcement_score(context["announcement_map"].get(s, []), positive=True))
    df["announcement_negative"] = df["symbol"].map(lambda s: _announcement_score(context["announcement_map"].get(s, []), positive=False))

    df["score_return_3m"] = _rank_score(df["return_3m"])
    df["score_sharpe_3m"] = _rank_score(df["sharpe_3m"])
    df["score_turnover"] = _rank_score(df["turnover_rate"])
    df["score_avg_turnover"] = _rank_score(df["avg_turnover_30d"])
    df["score_liquidity"] = _rank_score(df["liquidity_score"])
    df["score_drawdown"] = _rank_score(df["max_drawdown_3m"])

    df["short_score"] = (
        0.18 * df["theme_score"]
        + 0.14 * df["board_score"]
        + 0.12 * df["announcement_positive"]
        - 0.08 * df["announcement_negative"]
        + 0.14 * df["score_return_3m"]
        + 0.10 * df["score_sharpe_3m"]
        + 0.10 * df["score_turnover"]
        + 0.08 * df["score_avg_turnover"]
        + 0.10 * df["score_liquidity"]
        + 0.12 * df["score_drawdown"]
    )
    ranked = df.sort_values(["short_score", "score_return_3m", "score_liquidity"], ascending=False).head(max(int(count), 0))
    items: list[dict[str, Any]] = []
    for priority_rank, row in enumerate(ranked.to_dict(orient="records"), start=1):
        symbol = str(row["symbol"])
        theme_names = context["theme_map"].get(symbol, [])
        board_names = context["board_map"].get(symbol, [])
        announcements = context["announcement_map"].get(symbol, [])
        items.append(
            {
                "symbol": symbol,
                "stock_name": row.get("stock_name") or symbol,
                "selected_reason": _join_sentences(
                    [
                        _short_theme_reason(theme_names),
                        _board_reason(board_names),
                        _announcement_reason(
                            announcements,
                            lookback_days=int(context.get("announcement_lookback_days") or SHORT_BOOK_ANNOUNCEMENT_LOOKBACK_DAYS),
                        ),
                        f"snapshot 显示近3个月收益 {row.get('return_3m')}、流动性评分 {row.get('liquidity_score')}。",
                    ]
                ),
                "theme_alignment": _short_theme_reason(theme_names),
                "board_confirmation": _board_reason(board_names),
                "announcement_signal": _announcement_reason(
                    announcements,
                    lookback_days=int(context.get("announcement_lookback_days") or SHORT_BOOK_ANNOUNCEMENT_LOOKBACK_DAYS),
                ),
                "snapshot_highlights": {
                    "return_3m": row.get("return_3m"),
                    "sharpe_3m": row.get("sharpe_3m"),
                    "turnover_rate": row.get("turnover_rate"),
                    "avg_turnover_30d": row.get("avg_turnover_30d"),
                    "liquidity_score": row.get("liquidity_score"),
                    "max_drawdown_3m": row.get("max_drawdown_3m"),
                },
                "holding_horizon": "1-2个月",
                "why_short_book": _join_sentences(
                    [
                        "更适合放入短期池，因为当前评分更多来自主题强化、板块确认、公告催化和流动性。",
                        _short_theme_reason(theme_names),
                        _board_reason(board_names),
                    ]
                ),
                "main_risks": _build_short_risks(row, announcements),
                "priority_rank": priority_rank,
                "selection_score": round(float(row.get("short_score") or 0.0), 4),
            }
        )
    return items


def _build_long_candidates(
    snapshot_frame: pd.DataFrame,
    context: Dict[str, Any],
    *,
    count: int,
) -> List[Dict[str, Any]]:
    df = snapshot_frame.copy()
    df["theme_score"] = df["symbol"].map(lambda s: min(len(context["theme_map"].get(s, [])) / 3.0, 1.0))
    df["announcement_positive"] = df["symbol"].map(lambda s: _announcement_score(context["announcement_map"].get(s, []), positive=True))
    df["announcement_negative"] = df["symbol"].map(lambda s: _announcement_score(context["announcement_map"].get(s, []), positive=False))

    df["score_roe"] = _rank_score(df["roe"])
    df["score_revenue_growth"] = _rank_score(df["revenue_growth_yoy"])
    df["score_profit_growth"] = _rank_score(df["net_income_growth_yoy"])
    df["score_gross_margin"] = _rank_score(df["gross_margin"])
    df["score_net_margin"] = _rank_score(df["net_profit_margin"])
    df["score_return_1y"] = _rank_score(df["return_1y"])
    df["score_drawdown_1y"] = _rank_score(df["max_drawdown_1y"])

    valuation_score = pd.concat(
        [
            _inverse_rank_score(df["pe_ttm"]),
            _inverse_rank_score(df["pb"]),
            _inverse_rank_score(df["ps"]),
            _inverse_rank_score(df["pe_3_5y_percentile"]),
        ],
        axis=1,
    ).mean(axis=1).fillna(0.5)
    df["score_valuation"] = valuation_score

    df["long_score"] = (
        0.14 * df["score_roe"]
        + 0.12 * df["score_revenue_growth"]
        + 0.12 * df["score_profit_growth"]
        + 0.10 * df["score_gross_margin"]
        + 0.10 * df["score_net_margin"]
        + 0.14 * df["score_valuation"]
        + 0.08 * df["score_return_1y"]
        + 0.10 * df["score_drawdown_1y"]
        + 0.05 * df["theme_score"]
        + 0.08 * df["announcement_positive"]
        - 0.10 * df["announcement_negative"]
    )
    ranked = df.sort_values(["long_score", "score_valuation", "score_roe"], ascending=False).head(max(int(count), 0))
    items: list[dict[str, Any]] = []
    for priority_rank, row in enumerate(ranked.to_dict(orient="records"), start=1):
        symbol = str(row["symbol"])
        theme_names = context["theme_map"].get(symbol, [])
        announcements = context["announcement_map"].get(symbol, [])
        items.append(
            {
                "symbol": symbol,
                "stock_name": row.get("stock_name") or symbol,
                "selected_reason": _join_sentences(
                    [
                        f"长期评分较高，核心来自质量、增长和估值维度。",
                        f"ROE {row.get('roe')}，收入同比 {row.get('revenue_growth_yoy')}，净利润同比 {row.get('net_income_growth_yoy')}。",
                        f"估值侧 pe_ttm={row.get('pe_ttm')}，pe_3_5y_percentile={row.get('pe_3_5y_percentile')}。",
                    ]
                ),
                "long_term_thesis": _join_sentences(
                    [
                        "适合纳入长期池，因为公司质量/增长/估值的综合评分较高。",
                        _long_theme_reason(theme_names),
                    ]
                ),
                "valuation_view": {
                    "pe_ttm": row.get("pe_ttm"),
                    "pb": row.get("pb"),
                    "ps": row.get("ps"),
                    "pe_3_5y_percentile": row.get("pe_3_5y_percentile"),
                },
                "quality_view": {
                    "roe": row.get("roe"),
                    "gross_margin": row.get("gross_margin"),
                    "net_profit_margin": row.get("net_profit_margin"),
                },
                "earnings_reliability_view": {
                    "revenue_growth_yoy": row.get("revenue_growth_yoy"),
                    "net_income_growth_yoy": row.get("net_income_growth_yoy"),
                    "return_1y": row.get("return_1y"),
                    "max_drawdown_1y": row.get("max_drawdown_1y"),
                },
                "announcement_risk_check": _announcement_reason(
                    announcements,
                    lookback_days=int(context.get("announcement_lookback_days") or LONG_BOOK_ANNOUNCEMENT_LOOKBACK_DAYS),
                ),
                "holding_horizon": "3-12个月",
                "why_long_book": _join_sentences(
                    [
                        "更适合放入长期池，因为当前评分主要来自质量、增长、估值和跨季度稳定性。",
                        _long_theme_reason(theme_names),
                    ]
                ),
                "main_risks": _build_long_risks(row, announcements),
                "priority_rank": priority_rank,
                "selection_score": round(float(row.get("long_score") or 0.0), 4),
            }
        )
    return items


def _build_compact_candidate_rows(
    pool_type: str,
    snapshot_frame: pd.DataFrame,
    universe: Any,
    context: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    universe_meta = {
        stock.symbol: stock
        for stock in universe.stocks
    }
    rows: list[dict[str, Any]] = []
    for row in snapshot_frame.to_dict(orient="records"):
        symbol = str(row.get("symbol") or "")
        stock = universe_meta.get(symbol)
        theme_hits = list(dict.fromkeys((context.get("theme_map") or {}).get(symbol, [])))[:5]
        announcement_summaries = [
            summary
            for summary in (
                str(item.get("summary") or "").strip()
                for item in list((context.get("announcement_map") or {}).get(symbol, []))[:3]
            )
            if summary
        ]
        base = {
            "symbol": symbol,
            "stock_name": row.get("stock_name") or (stock.name if stock else symbol),
            "sector": getattr(stock, "sector", "") if stock else "",
            "industry": getattr(stock, "industry", "") if stock else "",
        }
        if theme_hits:
            base["theme_hits"] = theme_hits
        if announcement_summaries:
            base["recent_announcement_summaries"] = announcement_summaries
        if pool_type == "short_book":
            base.update(
                {
                    "daily_change_pct": row.get("daily_change_pct"),
                    "return_3m": row.get("return_3m"),
                    "sharpe_3m": row.get("sharpe_3m"),
                    "volatility_3m": row.get("volatility_3m"),
                    "max_drawdown_3m": row.get("max_drawdown_3m"),
                    "turnover_rate": row.get("turnover_rate"),
                    "avg_turnover_30d": row.get("avg_turnover_30d"),
                    "liquidity_score": row.get("liquidity_score"),
                    "pe_ttm": row.get("pe_ttm"),
                }
            )
        else:
            base.update(
                {
                    "roe": row.get("roe"),
                    "revenue_growth_yoy": row.get("revenue_growth_yoy"),
                    "net_income_growth_yoy": row.get("net_income_growth_yoy"),
                    "gross_margin": row.get("gross_margin"),
                    "net_profit_margin": row.get("net_profit_margin"),
                    "pe_ttm": row.get("pe_ttm"),
                    "pb": row.get("pb"),
                    "ps": row.get("ps"),
                    "pe_3_5y_percentile": row.get("pe_3_5y_percentile"),
                    "return_1y": row.get("return_1y"),
                    "max_drawdown_1y": row.get("max_drawdown_1y"),
                }
            )
        rows.append(base)
    return rows


def _build_local_agent_input_payload(
    *,
    pool_type: str,
    run_date: str,
    universe: Any,
    snapshot_frame: pd.DataFrame,
    context: Mapping[str, Any],
    shared_context_text: str,
    required_count: int,
) -> Dict[str, Any]:
    compact_rows = _build_compact_candidate_rows(pool_type, snapshot_frame, universe, context)
    payload: Dict[str, Any] = {
        "schema_version": 1,
        "run_date": run_date,
        "pool_type": pool_type,
        "required_count": required_count,
        "market_scope": "A股 + 港股 + ETF",
        "selection_scope": "master_universe",
        "shared_context_excerpt": shared_context_text,
        "universe_summary": {
            "stock_count": len(universe.stocks),
        },
        "candidate_rows": compact_rows,
    }
    if pool_type == "long_book":
        excluded = [
            {"symbol": e.symbol, "name": e.name, "description": e.description}
            for e in TRACKED_A_STOCKS
        ]
        payload["excluded_symbols"] = excluded
        payload["exclusion_rule"] = "以上 excluded_symbols 对应固定跟踪池（fixed_tracked），这些股票由固定池交易系统独立管理。长期池选股时不得包含这些股票，已按需从 candidate_rows 移除。"
    return payload


def _build_local_agent_markdown(
    *,
    pool_type: str,
    run_date: str,
    payload_path: Path,
    output_path: Path,
    required_count: int,
    previous_book_decision_path: Path | None = None,
    current_holdings: List[Dict[str, Any]] | None = None,
    holdings_source_path: Path | None = None,
) -> str:
    current_holdings = current_holdings or []
    if pool_type == "short_book":
        output_fields = [
            "symbol",
            "stock_name",
            "selected_reason",
            "theme_alignment",
            "board_confirmation",
            "announcement_signal",
            "snapshot_highlights",
            "holding_horizon",
            "why_short_book",
            "main_risks",
            "priority_rank",
        ]
        role_text = "短期选股头"
        style_text = "更关注主题强化、板块确认、公告催化、量价与流动性、近端风险。"
        fixed_horizon = "1-2个月"
        previous_candidates_name = "08_short_book_candidates.json"
        previous_decision_text = (
            f"`{_display_path(previous_book_decision_path)}`"
            if previous_book_decision_path is not None
            else "未找到上一交易日 `short_book/05_decision.json`，只能按冷启动处理"
        )
        continuity_block = f"""## 连续性要求

1. 必须直接回看上一交易日的 `data/selection_runs/<上一交易日>/{previous_candidates_name}`；这一步属于强制连续性检查，不视为回读 01-07 原始中间文件。
2. 必须继续回看上一交易日 short_book 的逐股深度分析底稿：{previous_decision_text}。这份 `05_decision.json` 用来判断昨天入池股票的催化是否还在、是否已经变贵、以及是否仍值得继续跟踪。
3. 昨日入池结果只是弱先验，不得因为昨天入池就机械保留。
4. 默认先问：昨天的催化今天是强化、兑现中、钝化，还是证伪？
5. 若昨日股票的催化仍在、价格不算贵、且后续还有跟踪价值，可以继续保留；若催化已证伪、明显走弱，或价格已明显透支短期赔率，应快速剔除。
6. 新进票仍然可以大量替换昨日旧票，不要求短期池名单高稳定性；但你必须能回答：这个新进票为何比被替换的昨日旧票更值得占用今天的 short-book 名额。
7. 若上一交易日 `{previous_candidates_name}` 或上一交易日 short_book `05_decision.json` 缺失，允许按冷启动口径筛选，但必须在你的分析过程中显式说明昨日短期池锚点缺失，无法做连续性比较。
"""
    else:
        output_fields = [
            "symbol",
            "stock_name",
            "selected_reason",
            "long_term_thesis",
            "valuation_view",
            "quality_view",
            "earnings_reliability_view",
            "announcement_risk_check",
            "holding_horizon",
            "why_long_book",
            "main_risks",
            "priority_rank",
        ]
        role_text = "长期选股头"
        style_text = "更关注公司质量、增长持续性、估值赔率、跨季度 thesis 和财报/治理风险。"
        fixed_horizon = "3-12个月"
        previous_candidates_name = "09_long_book_candidates.json"
        previous_decision_text = (
            f"`{_display_path(previous_book_decision_path)}`"
            if previous_book_decision_path is not None
            else "未找到上一交易日 `long_book/05_decision.json`，只能按冷启动处理"
        )
        if current_holdings:
            holdings_lines = "\n".join(
                f"  - `{item['symbol']}` ({item['stock_name']}，当前持仓 {item['shares']} 股)"
                for item in current_holdings
            )
            holdings_block = f"""## 当前 long_book 持仓保留约束

以下持仓来自 `{_display_path(holdings_source_path) if holdings_source_path else 'data/agent_data/book-long_book/position/position.jsonl'}` 的最新记录。**这些股票今天不得从 long_book 候选结果中剔除，只能保留并调整优先级、理由或风险描述。**

{holdings_lines}

"""
            holding_constraint = "8. 上方「当前 long_book 持仓保留约束」中的股票今天不得从 long_book 候选结果中移除；若逻辑转弱，只能降级排序或在理由里明确风险，不能直接踢出池子。\n"
            exclusion_constraint_num = "9"
            json_constraint_num = "10"
        else:
            holdings_block = f"""## 当前 long_book 持仓保留约束

未从 `{_display_path(holdings_source_path) if holdings_source_path else 'data/agent_data/book-long_book/position/position.jsonl'}` 读取到正股持仓。本轮长期池没有“已持仓必须保留”的硬约束，可按候选池口径筛选。

"""
            holding_constraint = ""
            exclusion_constraint_num = "8"
            json_constraint_num = "9"
        continuity_block = f"""## 连续性要求

1. 必须直接回看上一交易日的 `data/selection_runs/<上一交易日>/{previous_candidates_name}`，并把它作为今日 long-book 筛选的主锚；这一步属于强制连续性检查，不视为回读 01-07 原始中间文件。
2. 必须继续回看上一交易日 long_book 的逐股深度分析底稿：{previous_decision_text}。这份 `05_decision.json` 是你判断“旧 thesis 是否被证伪、哪些票该保留/降级/剔除”的逐股主依据。
3. 昨日入池结果是强先验，默认先问：昨天为什么选它，今天这些理由是否仍成立？
4. 若昨日逻辑仍成立，优先保留并只调整排序；若逻辑加强，升级优先级；若逻辑弱化但未证伪，降级观察；若逻辑被证伪，再移出池子。
5. 对于当前已经真实持仓的 long_book 股票，不允许直接从长期池剔除；只能保留，并在排序、理由、风险提示上体现你的最新判断。
6. 新进票必须回答：它为什么比某个昨日老票更值得占用今天的 long-book 名额；若要替换，优先替换“非持仓、且 thesis 已弱化或被证伪”的旧票。
7. 若上一交易日 `{previous_candidates_name}` 或上一交易日 long_book `05_decision.json` 缺失，允许按冷启动口径筛选，但必须在你的分析过程中显式说明长期池历史锚点缺失，无法做连续性比较。
"""

    fields_block = "\n".join(f"- `{field}`" for field in output_fields)
    if pool_type == "long_book":
        excluded_symbols_list = "\n".join(
            f"  - `{e.symbol}` ({e.name}, {e.description})" for e in TRACKED_A_STOCKS
        )
        exclusion_block = f"""## 固定跟踪池排除名单

以下股票属于固定跟踪池（fixed_tracked），由独立的固定跟踪交易系统管理。**长期池选股时必须严格排除这些股票，不得纳入 long_book 候选结果。**

{excluded_symbols_list}

"""
        exclusion_constraint = f"{exclusion_constraint_num}. 长期池不得包含固定跟踪池（fixed_tracked）的股票。这些股票的 symbol 已列在上方「固定跟踪池排除名单」中，由独立交易系统管理，不得出现在 long_book 候选结果中。\n"
    else:
        holdings_block = ""
        holding_constraint = ""
        exclusion_block = ""
        exclusion_constraint = ""
        json_constraint_num = "8"
    return f"""# {role_text} 本地 Agent 输入

- run_date: {run_date}
- pool_type: {pool_type}
- required_count: {required_count}
- payload_file: {payload_path}
- output_file: {output_path}

## 任务定位

你现在不是在做最终交易决策，也不是在做逐股深度研究。
你的任务是从 `master_universe` 中，筛出更值得进入 `{pool_type}` 深研池的股票。

{style_text}

## 读取顺序

1. 先阅读 `{payload_path.name}`
2. 再直接回看上一交易日的 `{previous_candidates_name}`
3. 如有需要，可使用本地 snapshot 查询脚本：
   - `python scripts/query_stock_snapshot.py --date {run_date} --symbol 000977.SZ`
   - `python scripts/rank_stock_snapshot.py --date {run_date} --field roe --top 20`
   - `python scripts/filter_stock_snapshot.py --date {run_date} --expr 'liquidity_score >= 0.6 and pe_ttm <= 25'`
4. 若需要补看板块，可使用：
   - `python scripts/query_board_snapshot.py --date {run_date} --board-name "通信设备"`

{continuity_block}
{holdings_block}
{exclusion_block}## 板块热点使用方法

板块信息层不要只看一个榜单，应合并理解：

1. `05_board_heat_digest.json`
   - 适合快速看近期和今日哪些板块最强
2. `05_board_heat_state.json`
   - 适合看当日热点板块的研究结论、驱动、持续性、结构和风险
3. `query_board_snapshot.py`
   - 适合补查你正在分析的股票所属板块，或主题里提到但不在当日热点研究中的板块

推荐使用顺序：

1. 先从 `{payload_path.name}` 里的 `shared_context_excerpt` 读取完整的 `05_board_heat_digest.json` 和 `05_board_heat_state.json`
2. 如果你要分析某只股票，优先查询它所属板块
3. 如果某个主题提到了板块，但 `05_board_heat_state.json` 里没有详细展开，再用 query 补查

query 用法：

```bash
python scripts/query_board_snapshot.py --date {run_date} --board-name "通信设备"
python scripts/query_board_snapshot.py --date {run_date} --board-name "能源金属" --board-name "电池"
```

## 强制约束

1. 只能从 `master_universe` 中选股
2. 必须输出恰好 `{required_count}` 只股票
3. 不得重复
4. `priority_rank` 从 `1` 递增
5. `holding_horizon` 必须写 `{fixed_horizon}`
6. `main_risks` 必须是字符串数组
7. 若发现某些股票虽然优秀，但不符合 `{pool_type}` 的 mandate，必须舍弃
{holding_constraint}{exclusion_constraint}{json_constraint_num}. 输出必须是唯一 JSON 对象，顶层结构如下：

```json
{{
  "items": [
    {{
      "symbol": "",
      "stock_name": ""
    }}
  ]
}}
```

## 每个 item 必填字段

{fields_block}

## 输出位置

将最终 JSON 保存为：

`{output_path}`
"""


def _merge_pools(
    run_date: str,
    short_payload: Mapping[str, Any],
    long_payload: Mapping[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    short_items = short_payload.get("items") if isinstance(short_payload, Mapping) else []
    long_items = long_payload.get("items") if isinstance(long_payload, Mapping) else []
    short_map = {str(item.get("symbol")): item for item in short_items if isinstance(item, Mapping)}
    long_map = {str(item.get("symbol")): item for item in long_items if isinstance(item, Mapping)}

    symbols = sorted(set(short_map) | set(long_map))
    merge_items: list[dict[str, Any]] = []
    queue_items: list[dict[str, Any]] = []
    for symbol in symbols:
        short_item = short_map.get(symbol)
        long_item = long_map.get(symbol)
        if short_item and long_item:
            short_score = float(short_item.get("selection_score") or 0.0)
            long_score = float(long_item.get("selection_score") or 0.0)
            final_mandate = "short_book" if short_score >= long_score else "long_book"
            selected_from = "both"
            chosen = short_item if final_mandate == "short_book" else long_item
        elif short_item:
            final_mandate = "short_book"
            selected_from = "short_only"
            chosen = short_item
        else:
            final_mandate = "long_book"
            selected_from = "long_only"
            chosen = long_item
        assert chosen is not None

        merge_item = {
            "symbol": symbol,
            "stock_name": chosen.get("stock_name"),
            "selected_from": selected_from,
            "final_mandate": final_mandate,
            "short_score": short_item.get("selection_score") if short_item else None,
            "long_score": long_item.get("selection_score") if long_item else None,
            "resolution_reason": _resolution_reason(short_item, long_item, final_mandate),
        }
        merge_items.append(merge_item)
        queue_items.append(
            {
                "symbol": symbol,
                "stock_name": chosen.get("stock_name"),
                "selected_from": selected_from,
                "final_mandate": final_mandate,
                "priority_rank": len(queue_items) + 1,
                "entry_reasons": _entry_reasons(chosen),
                "questions_to_verify": _questions_to_verify(chosen, final_mandate),
            }
        )

    queue_items.sort(key=lambda item: (item["final_mandate"], item["priority_rank"]))
    for idx, item in enumerate(queue_items, start=1):
        item["priority_rank"] = idx
    merged_payload = {
        "schema_version": 1,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "merged_count": len(merge_items),
            "short_only_count": sum(1 for item in merge_items if item["selected_from"] == "short_only"),
            "long_only_count": sum(1 for item in merge_items if item["selected_from"] == "long_only"),
            "both_count": sum(1 for item in merge_items if item["selected_from"] == "both"),
        },
        "items": merge_items,
    }
    queue_payload = {
        "schema_version": 1,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "queue_count": len(queue_items),
        },
        "items": queue_items,
    }
    return merged_payload, queue_payload


def _build_context_maps(
    hot_news_state: Mapping[str, Any],
    board_heat_state: Mapping[str, Any],
    announcements_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    theme_map: dict[str, list[str]] = defaultdict(list)
    for theme in hot_news_state.get("active_themes", []) if isinstance(hot_news_state, Mapping) else []:
        if not isinstance(theme, Mapping):
            continue
        theme_name = str(theme.get("theme_name") or "").strip()
        for symbol in theme.get("linked_symbols_in_universe", []) or []:
            normalized_symbol = str(symbol or "").strip()
            if normalized_symbol and theme_name:
                theme_map[normalized_symbol].append(theme_name)

    board_map: dict[str, list[str]] = defaultdict(list)
    for board in board_heat_state.get("boards", []) if isinstance(board_heat_state, Mapping) else []:
        if not isinstance(board, Mapping):
            continue
        board_name = str(board.get("board_name") or "").strip()
        for hint in board.get("related_stock_hints", []) or []:
            if not isinstance(hint, Mapping):
                continue
            symbol = str(hint.get("symbol") or "").strip()
            if symbol and board_name:
                board_map[symbol].append(board_name)

    announcement_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in announcements_payload.get("items", []) if isinstance(announcements_payload, Mapping) else []:
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("symbol") or "").strip()
        if symbol:
            announcement_map[symbol].append(dict(item))

    return {
        "theme_map": theme_map,
        "board_map": board_map,
        "announcement_map": announcement_map,
        "announcement_lookback_days": max(int(announcements_payload.get("lookback_days") or 3), 1)
        if isinstance(announcements_payload, Mapping)
        else 3,
    }


def _snapshot_frame(snapshot_payload: Mapping[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for symbol, payload in (snapshot_payload.get("stocks") or {}).items():
        if not isinstance(payload, Mapping):
            continue
        row = {"symbol": symbol}
        row.update(dict(payload))
        rows.append(row)
    return pd.DataFrame(rows)


def _rank_score(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    ranked = numeric.rank(pct=True, method="average")
    return ranked.fillna(0.0)


def _inverse_rank_score(series: pd.Series, *, fill_value: float = 0.5) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    ranked = numeric.rank(pct=True, method="average")
    return (1 - ranked).fillna(fill_value)


def _announcement_score(items: Iterable[Mapping[str, Any]], *, positive: bool) -> float:
    if not items:
        return 0.0
    score = sum(
        1
        for item in items
        if isinstance(item, Mapping) and _announcement_matches(item, positive=positive)
    )
    return min(score / 2.0, 1.0)


def _short_theme_reason(theme_names: List[str]) -> str:
    if not theme_names:
        return "未直接命中当前最强主题，但 snapshot 维度相对较优。"
    return f"命中当前活跃主题：{', '.join(theme_names[:3])}。"


def _long_theme_reason(theme_names: List[str]) -> str:
    if not theme_names:
        return "当前入选主要来自公司质量、增长和估值维度，而不是短期主题热度。"
    return f"同时具备主题承接性：{', '.join(theme_names[:2])}。"


def _board_reason(board_names: List[str]) -> str:
    if not board_names:
        return "未在当日板块热度研究前排中直接命中。"
    return f"获得板块确认：{', '.join(board_names[:3])}。"


def _announcement_reason(items: List[Mapping[str, Any]], *, lookback_days: int) -> str:
    window_text = f"最近{max(int(lookback_days), 1)}天"
    if not items:
        return f"{window_text}未发现显著公告催化。"
    latest = items[0]
    summary = _one_line_text(str(latest.get("summary") or "").strip(), limit=90)
    if not summary:
        return f"{window_text}有公告，但未提取到可用摘要。"
    return f"最近公告摘要：{summary}"


def _filter_announcements_payload(
    announcements_payload: Mapping[str, Any],
    *,
    run_date: str,
    lookback_days: int,
) -> Dict[str, Any]:
    requested_lookback_days = max(int(lookback_days), 1)
    run_dt = datetime.strptime(run_date, "%Y-%m-%d")
    window_start = run_dt - timedelta(days=requested_lookback_days - 1)
    window_end = run_dt + timedelta(days=1)

    filtered_items: list[dict[str, Any]] = []
    for item in announcements_payload.get("items", []) if isinstance(announcements_payload, Mapping) else []:
        if not isinstance(item, Mapping):
            continue
        published_dt = _parse_published_at(item.get("published_at"))
        if published_dt is None:
            continue
        if not (window_start <= published_dt < window_end):
            continue
        filtered_items.append(dict(item))

    return {
        **(dict(announcements_payload) if isinstance(announcements_payload, Mapping) else {}),
        "lookback_days": requested_lookback_days,
        "summary": {
            **(
                dict(announcements_payload.get("summary") or {})
                if isinstance(announcements_payload, Mapping) and isinstance(announcements_payload.get("summary"), Mapping)
                else {}
            ),
            "covered_symbol_count": len({str(item.get("symbol") or "").strip() for item in filtered_items if str(item.get("symbol") or "").strip()}),
            "announcement_count": len(filtered_items),
        },
        "items": filtered_items,
    }


def _parse_published_at(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


def _build_short_risks(row: Mapping[str, Any], announcements: List[Mapping[str, Any]]) -> List[str]:
    risks: list[str] = []
    if float(row.get("volatility_3m") or 0.0) > 40:
        risks.append("近3个月波动率偏高，短线容错率较低")
    if float(row.get("max_drawdown_3m") or 0.0) < -20:
        risks.append("近3个月最大回撤较深，可能已进入高波动阶段")
    if _has_negative_announcement_signal(announcements):
        risks.append("最近公告摘要中存在监管/诉讼/减持/经营异常等风险线索")
    if not risks:
        risks.append("主题或板块确认若减弱，短期交易逻辑容易钝化")
    return risks


def _build_long_risks(row: Mapping[str, Any], announcements: List[Mapping[str, Any]]) -> List[str]:
    risks: list[str] = []
    if float(row.get("pe_3_5y_percentile") or 0.0) > 0.8:
        risks.append("估值处于近三年半高分位，长期赔率可能受限")
    if float(row.get("net_income_growth_yoy") or 0.0) < 0:
        risks.append("净利润同比为负，盈利稳定性仍需验证")
    if _has_negative_announcement_signal(announcements):
        risks.append("最近公告摘要中存在治理、监管或经营异常线索，需要重点复核")
    if not risks:
        risks.append("长期 thesis 仍需继续跟踪下一次财报或经营数据验证")
    return risks


def _announcement_matches(item: Mapping[str, Any], *, positive: bool) -> bool:
    haystack = str(item.get("summary") or "").strip()
    if not haystack:
        return False
    keywords = POSITIVE_ANNOUNCEMENT_KEYWORDS if positive else NEGATIVE_ANNOUNCEMENT_KEYWORDS
    return any(keyword in haystack for keyword in keywords)


def _has_negative_announcement_signal(items: Iterable[Mapping[str, Any]]) -> bool:
    return any(
        isinstance(item, Mapping) and _announcement_matches(item, positive=False)
        for item in items
    )


def _one_line_text(value: str, *, limit: int = 120) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _resolution_reason(
    short_item: Mapping[str, Any] | None,
    long_item: Mapping[str, Any] | None,
    final_mandate: str,
) -> str:
    if short_item and long_item:
        return f"同时进入短期池和长期池，最终按 selection_score 归入 {final_mandate}。"
    if short_item:
        return "仅进入短期池，按短期 mandate 深研。"
    return "仅进入长期池，按长期 mandate 深研。"


def _entry_reasons(item: Mapping[str, Any]) -> List[str]:
    reasons: list[str] = []
    for key in ("selected_reason", "why_short_book", "why_long_book"):
        value = str(item.get(key) or "").strip()
        if value:
            reasons.append(value)
    return reasons[:3] or ["本轮候选池评分靠前。"]


def _questions_to_verify(item: Mapping[str, Any], mandate: str) -> List[str]:
    questions = [
        "最近1-3天是否有需要联网补证的新公告、新闻或经营事件？",
        "当前 thesis 是否仍与主题/板块/公告/估值事实一致？",
    ]
    if mandate == "short_book":
        questions.append("短期催化是否仍在强化，量价是否继续支持？")
    else:
        questions.append("长期 thesis 的质量、增长和估值假设是否仍成立？")
    risks = item.get("main_risks") or []
    if isinstance(risks, list):
        for risk in risks[:2]:
            questions.append(f"如何验证风险：{risk}")
    return questions[:5]


def _join_sentences(parts: Iterable[str]) -> str:
    cleaned = [str(part).strip() for part in parts if str(part).strip()]
    return " ".join(cleaned)


def _ensure_universe_snapshot_coverage(
    run_date: str,
    universe_symbols: List[str],
    *,
    base_dir: str | Path,
) -> Dict[str, Any]:
    """【兜底路径】确保 master_universe 每只股票都有可用的 basic snapshot 缓存。

    设计归属：**每日 fresh 数据的主刷新责任在 `services/data_refresh/refresh_orchestrator.py`**
    ——它每天把 TRACKED_A_STOCKS ∪ master_universe 全部传给 manage_daily_data 强刷价格与
    basic_info。本函数只作为"安全网"：正常情况下缓存应当全量命中，missing_symbols 列表
    应为空。若触发补齐分支（有 missing），说明上游 refresh 有遗漏或 universe 被动态扩展
    但 orchestrator 尚未跟上，需要排查。
    """
    snapshot_payload = load_basic_snapshot_from_cache(
        universe_symbols,
        run_date,
        base_dir=base_dir,
    )
    cached_symbols = set((snapshot_payload.get("stocks") or {}).keys())
    missing_symbols = [symbol for symbol in universe_symbols if symbol not in cached_symbols]
    if not missing_symbols:
        LOGGER.info("master_universe snapshot 已全量命中缓存: symbol_count=%d", len(universe_symbols))
        return snapshot_payload

    LOGGER.warning(
        "[兜底触发] master_universe snapshot 覆盖不足，开始按选股系统模式补齐: "
        "universe=%d cached=%d missing=%d。"
        "正常每日刷新流程应由 refresh_orchestrator 负责强刷 universe 所有股票；"
        "此处触发通常意味着上游 refresh_all_for_date.py 未运行或 universe 刚被扩展。",
        len(universe_symbols),
        len(cached_symbols),
        len(missing_symbols),
    )
    _prepare_missing_snapshot_inputs(
        run_date,
        missing_symbols,
        base_dir=base_dir,
    )
    basic_info(
        missing_symbols,
        base_dir=base_dir,
        today_time=run_date,
        use_cache=True,
        max_workers=4,
        price_lookback_days=SNAPSHOT_PRICE_LOOKBACK_DAYS,
    )
    reloaded = load_basic_snapshot_from_cache(
        universe_symbols,
        run_date,
        base_dir=base_dir,
    )
    reloaded_symbols = set((reloaded.get("stocks") or {}).keys())
    still_missing = [symbol for symbol in universe_symbols if symbol not in reloaded_symbols]
    if still_missing:
        preview = ", ".join(still_missing[:15])
        raise RuntimeError(
            "master_universe snapshot 仍不完整，无法继续选股。"
            f" missing_count={len(still_missing)} missing_preview={preview}"
        )
    LOGGER.info("master_universe snapshot 补齐完成: symbol_count=%d", len(universe_symbols))
    return reloaded


def _prepare_missing_snapshot_inputs(
    run_date: str,
    missing_symbols: List[str],
    *,
    base_dir: str | Path,
) -> None:
    LOGGER.info("开始补齐缺失 snapshot 依赖数据: missing_symbols=%d", len(missing_symbols))
    sda = SharedDataAccess(
        base_dir=base_dir,
        logger=LOGGER,
        price_lookback_days=SNAPSHOT_PRICE_LOOKBACK_DAYS,
    )
    for symbol in missing_symbols:
        info = parse_symbol(symbol)
        sda.prepare_dataset(
            symbolInfo=info,
            as_of_date=run_date,
            force_refresh=False,
            force_refresh_price=False,
            force_refresh_financials=False,
            skip_financial_refresh=False,
        )
    LOGGER.info("缺失 snapshot 依赖数据补齐完成: symbol_count=%d", len(missing_symbols))


def _load_shared_context_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


__all__ = [
    "build_candidate_pools",
    "collect_candidate_pool_inputs",
    "collect_candidate_pools",
    "merge_candidate_pools",
    "prepare_candidate_agent_inputs",
]
