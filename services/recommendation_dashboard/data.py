"""Build recommendation performance from existing decision and price caches."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.recommendation_dashboard.research import research_packages
from services.recommendation_dashboard.jobs import latest_jobs
from services.selection_system.master_universe import load_master_universe
from services.selection_system.paths import SelectionSystemPaths
from shared_data_access.historical_prices import load_price_history
from commons.stock_utils import SymbolInfo


LOGGER = get_logger("RecommendationDashboard")
BOOK = "fixed_tracked"


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def _decisions(data_dir: Path) -> tuple[dict[str, dict[str, Any]], int]:
    path = data_dir / "agent_data" / "book-fixed_tracked" / "stock_decisions.json"
    history = _read_json(path)
    if not isinstance(history, list):
        raise ValueError(f"历史决策文件不是数组: {path}")
    latest: dict[str, dict[str, Any]] = {}
    for decision in history:
        if not isinstance(decision, dict):
            continue
        symbol = decision.get("symbol")
        operation_date = decision.get("operation_date")
        if not isinstance(symbol, str) or not isinstance(operation_date, str):
            continue
        previous = latest.get(symbol)
        if previous is None or operation_date >= previous["operation_date"]:
            latest[symbol] = decision
    return latest, len(history)


def _buy_events(data_dir: Path) -> dict[str, list[dict[str, Any]]]:
    path = data_dir / "agent_data" / "book-fixed_tracked" / "position" / "position.jsonl"
    events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                LOGGER.warning("跳过损坏的账本行 %s:%d: %s", path, line_number, exc)
                continue
            action = entry.get("this_action") or {}
            if action.get("action") != "buy":
                continue
            trade_date = entry.get("date")
            trades = action.get("trades") or {}
            if not isinstance(trade_date, str) or not isinstance(trades, dict):
                continue
            for symbol, quantity in trades.items():
                if isinstance(symbol, str) and isinstance(quantity, (int, float)) and quantity > 0:
                    events[symbol].append({"date": trade_date, "quantity": quantity})
    for symbol in events:
        events[symbol].sort(key=lambda event: event["date"])
    return events


def _symbol_info(symbol: str, stock_name: str) -> SymbolInfo:
    code, suffix = symbol.rsplit(".", 1)
    market = {"HK": "HK", "US": "US"}.get(suffix, "CN_A")
    return SymbolInfo(
        symbol=symbol,
        code=code,
        suffix=suffix,
        market=market,
        calendar=suffix if suffix in {"HK", "US"} else "CN",
        stock_name=stock_name,
        description="",
    )


def _price_points(data_dir: Path, symbol: str, stock_name: str) -> tuple[dict[str, float], str | None, float | None]:
    """Read canonical cached closes; historical anchors require an exact date."""
    frame = load_price_history(
        _symbol_info(symbol, stock_name),
        base_dir=data_dir,
        as_of_date=date.today(),
    )
    if frame.empty or "收盘" not in frame.columns:
        return {}, None, None
    valid = frame.loc[frame["收盘"] > 0, ["日期", "收盘"]]
    valid = valid.replace([float("inf"), float("-inf")], float("nan")).dropna(subset=["收盘"])
    closes = dict(zip(valid["日期"].dt.strftime("%Y-%m-%d"), valid["收盘"].astype(float)))
    if not closes:
        return {}, None, None
    latest_date = max(closes)
    return closes, latest_date, closes[latest_date]


def _return_pct(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start <= 0:
        return None
    return round((end / start - 1) * 100, 2)


def build_dashboard(data_dir: Path) -> dict[str, Any]:
    """Return all analyzed stocks and first-BUY-to-latest price performance."""
    latest_decisions, decision_count = _decisions(data_dir)
    buy_events = _buy_events(data_dir)
    rows: list[dict[str, Any]] = []
    market_dates: dict[str, str] = {}
    for symbol, decision in latest_decisions.items():
        stock_name = str(decision.get("stock_name") or symbol)
        try:
            closes, latest_price_date, latest_close = _price_points(data_dir, symbol, stock_name)
        except (OSError, ValueError, KeyError) as exc:
            LOGGER.warning("读取 %s 行情缓存失败: %s", symbol, exc)
            closes, latest_price_date, latest_close = {}, None, None
        if latest_price_date:
            market = symbol.rsplit(".", 1)[-1]
            market_dates[market] = max(market_dates.get(market, ""), latest_price_date)
        events = []
        for event in buy_events.get(symbol, []):
            anchor_close = closes.get(event["date"])
            events.append({
                **event,
                "anchor_close": anchor_close,
                "return_pct": _return_pct(anchor_close, latest_close),
            })
        first = events[0] if events else None
        rows.append({
            "symbol": symbol,
            "stock_name": stock_name,
            "latest_analysis_date": decision["operation_date"],
            "latest_action": decision.get("action_type"),
            "price_impression": decision.get("price_impression"),
            "confidence_score": decision.get("confidence_score"),
            "has_buy": bool(events),
            "first_buy_date": first["date"] if first else None,
            "buy_count": len(events),
            "buy_close": first["anchor_close"] if first else None,
            "latest_close": latest_close,
            "price_date": latest_price_date,
            "return_pct": first["return_pct"] if first else None,
            "buy_events": events,
        })
    # Completed on-demand research appears in the stock list, but never becomes a
    # historical BUY signal or a formal trading decision without the normal approval flow.
    try:
        universe_names = {item.symbol: item.name for item in load_master_universe(SelectionSystemPaths.from_base_dir(data_dir)).stocks}
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        universe_names = {}
    listed_symbols = {row["symbol"] for row in rows}
    for job in latest_jobs(data_dir):
        symbol = job["symbol"]
        if job["status"] != "complete" or symbol in listed_symbols:
            continue
        listed_symbols.add(symbol)
        stock_name = universe_names.get(symbol, symbol)
        verdict_path = data_dir / "web_research_runs" / job["id"] / "skill_runs" / job["date"] / "fixed_tracked" / "debate"
        verdict = None
        for stock_dir in verdict_path.glob(f"*_{symbol}"):
            candidate = stock_dir / "final" / "stock_verdict.json"
            if candidate.is_file():
                verdict = _read_json(candidate)
                break
        try:
            _, latest_price_date, latest_close = _price_points(data_dir, symbol, stock_name)
        except (OSError, ValueError, KeyError):
            latest_price_date, latest_close = None, None
        if latest_price_date:
            market = symbol.rsplit(".", 1)[-1]
            market_dates[market] = max(market_dates.get(market, ""), latest_price_date)
        rows.append({
            "symbol": symbol, "stock_name": stock_name, "source": "on_demand", "job_id": job["id"],
            "latest_analysis_date": job["date"], "latest_action": (verdict or {}).get("action_type"),
            "price_impression": (verdict or {}).get("price_impression"), "confidence_score": (verdict or {}).get("confidence_score"),
            "has_buy": False, "first_buy_date": None, "buy_count": 0, "buy_close": None,
            "latest_close": latest_close, "price_date": latest_price_date, "return_pct": None, "buy_events": [],
        })
    for row in rows:
        market = row["symbol"].rsplit(".", 1)[-1]
        row["price_is_latest_for_market"] = bool(
            row["price_date"] and row["price_date"] == market_dates.get(market)
        )
    rows.sort(key=lambda row: (not row["has_buy"], row["symbol"]))
    tracked_returns = [
        row["return_pct"]
        for row in rows
        if row["has_buy"] and row["return_pct"] is not None and row["price_is_latest_for_market"]
    ]
    return {
        "as_of_date": date.today().isoformat(),
        "book": BOOK,
        "analyzed_count": len(rows),
        "decision_count": decision_count,
        "buy_stock_count": sum(row["has_buy"] for row in rows),
        "buy_event_count": sum(row["buy_count"] for row in rows),
        "priced_buy_stock_count": len(tracked_returns),
        "equal_weight_return_pct": round(sum(tracked_returns) / len(tracked_returns), 2) if tracked_returns else None,
        "winning_stock_count": sum(value > 0 for value in tracked_returns),
        "losing_stock_count": sum(value < 0 for value in tracked_returns),
        "market_price_dates": market_dates,
        "stocks": rows,
    }


def build_holdings(data_dir: Path) -> dict[str, Any]:
    """Value the manually maintained real positions in each security's currency."""
    path = data_dir / "agent_data" / "book-fixed_tracked" / "position" / "manual_position_override.json"
    payload = _read_json(path)
    if not isinstance(payload, dict) or not isinstance(payload.get("positions"), dict):
        raise ValueError(f"人工持仓文件缺少 positions 对象: {path}")
    latest_decisions, _ = _decisions(data_dir)
    symbols_by_name = {
        str(decision.get("stock_name")): symbol
        for symbol, decision in latest_decisions.items()
        if decision.get("stock_name")
    }
    rows: list[dict[str, Any]] = []
    for position_name, entry in payload["positions"].items():
        if not isinstance(entry, dict):
            LOGGER.warning("跳过格式不正确的人工持仓: %s", position_name)
            continue
        try:
            shares = float(entry["shares"])
            avg_cost = float(entry["avg_cost"])
        except (KeyError, TypeError, ValueError):
            LOGGER.warning("跳过股数或成本不正确的人工持仓: %s", position_name)
            continue
        if not math.isfinite(shares) or not math.isfinite(avg_cost) or shares <= 0 or avg_cost < 0:
            LOGGER.warning("跳过股数或成本异常的人工持仓: %s", position_name)
            continue
        symbol = position_name if "." in position_name else symbols_by_name.get(position_name)
        stock_name = position_name if position_name in symbols_by_name else str(
            latest_decisions.get(symbol, {}).get("stock_name") or position_name
        )
        latest_close = None
        price_date = None
        if symbol:
            try:
                _, price_date, latest_close = _price_points(data_dir, symbol, stock_name)
            except (OSError, ValueError, KeyError) as exc:
                LOGGER.warning("读取实际持仓 %s 行情缓存失败: %s", symbol, exc)
        currency = "HKD" if symbol and symbol.endswith(".HK") else "CNY"
        cost_value = shares * avg_cost
        market_value = shares * latest_close if latest_close is not None else None
        unrealized_pnl = market_value - cost_value if market_value is not None else None
        rows.append({
            "symbol": symbol,
            "stock_name": stock_name,
            "shares": shares,
            "avg_cost": avg_cost,
            "latest_close": latest_close,
            "price_date": price_date,
            "currency": currency,
            "cost_value": round(cost_value, 2),
            "market_value": round(market_value, 2) if market_value is not None else None,
            "unrealized_pnl": round(unrealized_pnl, 2) if unrealized_pnl is not None else None,
            "unrealized_pct": _return_pct(avg_cost, latest_close),
        })
    totals: dict[str, dict[str, float | int]] = {}
    for currency in ("CNY", "HKD"):
        priced = [row for row in rows if row["currency"] == currency and row["market_value"] is not None]
        totals[currency] = {
            "market_value": round(sum(row["market_value"] for row in priced), 2),
            "unrealized_pnl": round(sum(row["unrealized_pnl"] for row in priced), 2),
            "priced_count": len(priced),
            "position_count": sum(row["currency"] == currency for row in rows),
        }
    cash = payload.get("cash")
    cash_cny = float(cash) if isinstance(cash, (int, float)) and math.isfinite(cash) else None
    return {
        "source_as_of_date": payload.get("as_of_date"),
        "cash_cny": cash_cny,
        "position_count": len(rows),
        "priced_count": sum(row["market_value"] is not None for row in rows),
        "totals": totals,
        "positions": rows,
    }


def _latest_debate_dir(data_dir: Path, symbol: str) -> tuple[str | None, Path | None]:
    runs_dir = data_dir / "skill_runs"
    if not runs_dir.exists():
        return None, None
    for run_dir in sorted((item for item in runs_dir.iterdir() if item.is_dir()), reverse=True):
        debate_root = run_dir / BOOK / "debate"
        if not debate_root.is_dir():
            continue
        for stock_dir in debate_root.iterdir():
            if stock_dir.is_dir() and stock_dir.name.endswith(f"_{symbol}"):
                return run_dir.name, stock_dir
    return None, None


def stock_detail(data_dir: Path, symbol: str) -> dict[str, Any] | None:
    """Return the latest archived decision and the latest available full debate."""
    latest, _ = _decisions(data_dir)
    decision = latest.get(symbol)
    if decision is None:
        return None
    debate_date, debate_dir = _latest_debate_dir(data_dir, symbol)
    for job in latest_jobs(data_dir):
        if job["status"] != "complete" or job["symbol"] != symbol or job["date"] != decision["operation_date"]:
            continue
        candidate_root = data_dir / "web_research_runs" / job["id"] / "skill_runs" / job["date"] / BOOK / "debate"
        matched_job = False
        for candidate in candidate_root.glob(f"*_{symbol}"):
            verdict_path = candidate / "final" / "stock_verdict.json"
            if not verdict_path.is_file():
                continue
            try:
                verdict = _read_json(verdict_path)
            except (OSError, json.JSONDecodeError):
                continue
            if verdict == {key: value for key, value in decision.items() if key != "operation_date"}:
                debate_date, debate_dir = job["date"], candidate
                matched_job = True
                break
        if matched_job:
            break
    stages: dict[str, Any] = {}
    if debate_dir:
        paths = {
            "bull": "advocates/bull/opening.json",
            "bear": "advocates/bear/opening.json",
            "bull_rebuttal": "advocates/bull/rebuttal.json",
            "bear_rebuttal": "advocates/bear/rebuttal.json",
            "juror_01": "jury/juror_01/ballot.json",
            "juror_02": "jury/juror_02/ballot.json",
            "juror_03": "jury/juror_03/ballot.json",
            "vote_summary": "final/vote_summary.json",
            "final": "final/stock_verdict.json",
        }
        for key, relative_path in paths.items():
            path = debate_dir / relative_path
            if path.is_file():
                try:
                    stages[key] = _read_json(path)
                except (OSError, json.JSONDecodeError) as exc:
                    LOGGER.warning("读取辩论文件失败 %s: %s", path, exc)
    return {
        "symbol": symbol,
        "stock_name": decision.get("stock_name") or symbol,
        "latest_decision_date": decision["operation_date"],
        "decision": decision,
        "debate_date": debate_date,
        "stages": stages,
        "research_packages": research_packages(data_dir, symbol),
    }
