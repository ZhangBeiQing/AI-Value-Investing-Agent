"""Candidate selection for the stage-1 selection system."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Sequence

from core.logging import get_logger
from services.trading.trade_summary import get_portfolio_historical_context, load_yesterday_daily_summary

from .models import MasterUniverseDocument


LOGGER = get_logger("CandidateSelector")


def build_candidate_bundles(
    universe: MasterUniverseDocument,
    snapshots_by_symbol: Dict[str, Dict[str, Any]],
    symbol_hot_state: Dict[str, Any],
    run_date: str,
    *,
    top_hot: int = 12,
    top_core: int = 12,
    signature: str = "",
) -> Dict[str, Dict[str, Any]]:
    hot_index = {item["symbol"]: item for item in symbol_hot_state.get("symbols", []) if isinstance(item, dict)}
    code_to_symbol = {stock.symbol.split(".", 1)[0]: stock.symbol for stock in universe.stocks}
    holdings_context = _load_holdings_context(signature, run_date, code_to_symbol)
    holding_symbols = set(holdings_context.get("holding_symbols", []))

    hot_candidates: List[Dict[str, Any]] = []
    core_candidates: List[Dict[str, Any]] = []
    for stock in universe.stocks:
        snapshot = snapshots_by_symbol.get(stock.symbol, {})
        hot_state = hot_index.get(stock.symbol, {})
        hot_score, hot_reasons = _score_hot_candidate(stock.symbol, snapshot, hot_state)
        core_score, core_reasons = _score_core_candidate(stock.symbol, snapshot, hot_state)
        base_payload = {
            "symbol": stock.symbol,
            "name": stock.name,
            "sector": stock.sector,
            "industry": stock.industry,
            "snapshot": snapshot,
            "hot_state": hot_state,
            "is_current_holding": stock.symbol in holding_symbols,
        }
        hot_candidates.append(
            {
                **base_payload,
                "strategy": "hot",
                "score": hot_score,
                "reasons": hot_reasons,
            }
        )
        core_candidates.append(
            {
                **base_payload,
                "strategy": "core",
                "score": core_score,
                "reasons": core_reasons,
            }
        )

    hot_candidates.sort(key=lambda item: (item["is_current_holding"], item["score"]), reverse=True)
    core_candidates.sort(key=lambda item: (item["is_current_holding"], item["score"]), reverse=True)

    result = {
        "hot": {
            "schema_version": 1,
            "run_date": run_date,
            "updated_at": datetime.now().isoformat(),
            "candidates": hot_candidates[:top_hot],
            "holding_guardrail": sorted(holding_symbols),
        },
        "core": {
            "schema_version": 1,
            "run_date": run_date,
            "updated_at": datetime.now().isoformat(),
            "candidates": core_candidates[:top_core],
            "holding_guardrail": sorted(holding_symbols),
        },
    }
    LOGGER.info("候选筛选完成: hot=%d core=%d", len(result["hot"]["candidates"]), len(result["core"]["candidates"]))
    return result


def _score_hot_candidate(symbol: str, snapshot: Dict[str, Any], hot_state: Dict[str, Any]) -> tuple[float, List[str]]:
    hotness = _as_float(hot_state.get("hotness_score"))
    actionability = _as_float(hot_state.get("actionability_score"))
    leader = _as_float(hot_state.get("leader_score"))
    liquidity = _as_float(snapshot.get("liquidity_score")) * 100
    turnover_rate = _as_float(snapshot.get("turnover_rate"))
    turnover_bonus = min(turnover_rate, 8.0) * 1.2
    momentum = min(max(_as_float(snapshot.get("return_3m")), -20), 40)
    momentum_bonus = max(min(momentum * 0.35, 10.0), -8.0)
    score = round(
        min(
            100.0,
            hotness * 0.4
            + actionability * 0.25
            + leader * 0.15
            + liquidity * 0.1
            + turnover_bonus
            + momentum_bonus,
        ),
        2,
    )
    reasons = []
    theme_names = [theme.get("theme_name") for theme in hot_state.get("hot_themes", []) if isinstance(theme, dict)]
    if theme_names:
        reasons.append(f"热点主题: {', '.join(theme_names[:3])}")
    if hot_state.get("today_news_delta"):
        reasons.append(f"今日新增事件 {len(hot_state['today_news_delta'])} 条")
    if snapshot.get("turnover_rate") is not None:
        reasons.append(f"换手率 {snapshot.get('turnover_rate')}")
    if snapshot.get("return_3m") is not None:
        reasons.append(f"近3个月收益 {snapshot.get('return_3m')}")
    if not reasons:
        reasons.append("暂无显著热点，但保留在观察范围")
    return score, reasons[:4]


def _score_core_candidate(symbol: str, snapshot: Dict[str, Any], hot_state: Dict[str, Any]) -> tuple[float, List[str]]:
    quality = sum(
        [
            max(_as_float(snapshot.get("roe")), 0) * 1.5,
            max(_as_float(snapshot.get("revenue_growth_yoy")), 0) * 0.6,
            max(_as_float(snapshot.get("net_income_growth_yoy")), 0) * 0.7,
        ]
    )
    stability = max(_as_float(snapshot.get("return_1y")), 0) * 0.25 + max(0, 30 - abs(_as_float(snapshot.get("max_drawdown_3m")))) * 0.8
    valuation = 0.0
    if snapshot.get("pe_2y_percentile") is not None:
        valuation += max(0.0, 1.0 - _as_float(snapshot.get("pe_2y_percentile"))) * 40
    if snapshot.get("pb") is not None:
        valuation += max(0.0, 12 - _as_float(snapshot.get("pb"))) * 2
    liquidity = _as_float(snapshot.get("liquidity_score")) * 30
    hot_support = _as_float(hot_state.get("hotness_score")) * 0.15
    score = round(min(100.0, quality * 0.35 + stability * 0.25 + valuation * 0.2 + liquidity + hot_support), 2)
    reasons = []
    if snapshot.get("roe") is not None:
        reasons.append(f"ROE {snapshot.get('roe')}")
    if snapshot.get("net_income_growth_yoy") is not None:
        reasons.append(f"利润增速 {snapshot.get('net_income_growth_yoy')}")
    if snapshot.get("pe_2y_percentile") is not None:
        reasons.append(f"PE 历史分位 {snapshot.get('pe_2y_percentile')}")
    if hot_state.get("hot_themes"):
        theme_names = [theme.get("theme_name") for theme in hot_state.get("hot_themes", []) if isinstance(theme, dict)]
        reasons.append(f"主题支撑: {', '.join(theme_names[:2])}")
    if not reasons:
        reasons.append("基础面与流动性数据不足，暂以观察为主")
    return score, reasons[:4]


def _load_holdings_context(signature: str, run_date: str, code_to_symbol: Dict[str, str]) -> Dict[str, Any]:
    if not signature:
        return {}
    result: Dict[str, Any] = {"holding_symbols": []}
    try:
        yesterday = load_yesterday_daily_summary(signature, run_date)
    except Exception as exc:
        LOGGER.warning("读取昨日持仓总结失败: %s", exc)
        yesterday = None
    if yesterday:
        symbols = []
        for op in yesterday.get("stock_operations", []):
            action = str(op.get("action") or "").upper()
            if action in {"HOLD", "BUY"}:
                raw_code = str(op.get("stock_code") or "")
                if raw_code:
                    symbols.append(code_to_symbol.get(raw_code, raw_code))
        result["holding_symbols"] = symbols

    try:
        history = get_portfolio_historical_context(
            signature,
            [symbol.split(".", 1)[0] for symbol in result["holding_symbols"]],
            n=3,
        )
    except Exception as exc:
        LOGGER.warning("读取历史持仓上下文失败: %s", exc)
        history = {}
    result["history"] = history
    return result


def _as_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(str(value).replace("%", ""))
    except Exception:
        return 0.0
