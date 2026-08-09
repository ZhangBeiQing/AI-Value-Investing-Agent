"""Isolated append-only portfolio ledger for fixed-tracked backtests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.logging import get_logger
from shared_data_access.historical_prices import close_on_or_before
from utlity import parse_symbol


LOGGER = get_logger("BacktestLedger")


@dataclass(frozen=True)
class LedgerState:
    record_id: int
    date: str
    positions: dict[str, float]
    total_value: float | None


class BacktestLedger:
    def __init__(
        self,
        *,
        agent_data_root: str | Path,
        signature: str,
        source_data_root: str | Path,
    ) -> None:
        self.agent_data_root = Path(agent_data_root).resolve()
        self.signature = signature
        self.source_data_root = Path(source_data_root).resolve()
        self.book_root = self.agent_data_root / signature
        self.position_file = self.book_root / "position" / "position.jsonl"
        self.orders_file = self.book_root / "orders.jsonl"

    def initialize(
        self,
        *,
        start_date: str,
        initial_cash: float,
        symbols: list[str],
    ) -> Path:
        if self.position_file.exists():
            return self.position_file
        positions = {symbol: 0 for symbol in symbols}
        positions["CASH"] = round(float(initial_cash), 4)
        self._append_position(
            {
                "id": 0,
                "date": start_date,
                "decision_date": None,
                "execution_date": start_date,
                "positions": positions,
                "this_action": {"action": "init"},
                "total_value": round(float(initial_cash), 4),
            }
        )
        return self.position_file

    def records(self) -> list[dict[str, Any]]:
        if not self.position_file.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in self.position_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                records.append(payload)
        return records

    def latest(self, on_or_before: str | None = None) -> LedgerState:
        records = self.records()
        if on_or_before:
            records = [
                record
                for record in records
                if isinstance(record.get("date"), str)
                and record["date"] <= on_or_before
            ]
        if not records:
            raise FileNotFoundError(f"回测仓位记录为空: {self.position_file}")
        record = max(
            records,
            key=lambda item: (str(item.get("date") or ""), int(item.get("id") or 0)),
        )
        return LedgerState(
            record_id=int(record.get("id") or 0),
            date=str(record.get("date") or ""),
            positions=dict(record.get("positions") or {}),
            total_value=(
                float(record["total_value"])
                if isinstance(record.get("total_value"), (int, float))
                else None
            ),
        )

    def append_execution(
        self,
        *,
        decision_date: str,
        execution_date: str,
        positions: dict[str, float],
        actions: list[dict[str, Any]],
        order_ids: list[str],
    ) -> LedgerState:
        existing_ids = self.executed_order_ids()
        duplicates = sorted(set(order_ids).intersection(existing_ids))
        if duplicates:
            raise RuntimeError(f"订单已执行，拒绝重复写入: {duplicates}")
        latest = self.latest(on_or_before=execution_date)
        total_value, stale_symbols = self.mark_to_market(
            execution_date,
            positions,
        )
        record = {
            "id": latest.record_id + 1,
            "date": execution_date,
            "decision_date": decision_date,
            "execution_date": execution_date,
            "positions": positions,
            "this_action": {
                "action": "backtest_execution" if actions else "no_trade",
                "actions": actions,
                "order_ids": order_ids,
            },
            "total_value": total_value,
            "stale_price_symbols": stale_symbols,
        }
        self._append_position(record)
        return LedgerState(
            record_id=record["id"],
            date=execution_date,
            positions=dict(positions),
            total_value=total_value,
        )

    def append_order_records(self, orders: list[dict[str, Any]]) -> None:
        if not orders:
            return
        self.orders_file.parent.mkdir(parents=True, exist_ok=True)
        with self.orders_file.open("a", encoding="utf-8") as handle:
            for order in orders:
                handle.write(json.dumps(order, ensure_ascii=False) + "\n")

    def executed_order_ids(self) -> set[str]:
        ids: set[str] = set()
        for record in self.records():
            action = record.get("this_action") or {}
            for order_id in action.get("order_ids") or []:
                if isinstance(order_id, str):
                    ids.add(order_id)
        return ids

    def held_symbols(self, on_or_before: str | None = None) -> list[str]:
        state = self.latest(on_or_before=on_or_before)
        return sorted(
            symbol
            for symbol, shares in state.positions.items()
            if symbol != "CASH" and float(shares or 0) > 0
        )

    def average_costs(self, on_or_before: str | None = None) -> dict[str, float]:
        """Replay the append-only execution ledger and return current average costs."""

        records = self.records()
        if on_or_before:
            records = [
                record
                for record in records
                if isinstance(record.get("date"), str)
                and record["date"] <= on_or_before
            ]
        records.sort(
            key=lambda item: (str(item.get("date") or ""), int(item.get("id") or 0))
        )

        tracked_shares: dict[str, float] = {}
        average_costs: dict[str, float] = {}
        for record in records:
            action_payload = record.get("this_action") or {}
            for action in action_payload.get("actions") or []:
                if not isinstance(action, dict):
                    continue
                symbol = str(action.get("symbol") or "").strip()
                action_type = str(action.get("action") or "").upper()
                try:
                    shares = float(action.get("shares") or 0)
                    price = float(action.get("price") or 0)
                except (TypeError, ValueError):
                    continue
                if not symbol or symbol == "CASH" or shares <= 0:
                    continue

                previous_shares = tracked_shares.get(symbol, 0.0)
                if action_type == "BUY" and price > 0:
                    previous_cost = average_costs.get(symbol, 0.0)
                    new_shares = previous_shares + shares
                    average_costs[symbol] = (
                        previous_cost * previous_shares + price * shares
                    ) / new_shares
                    tracked_shares[symbol] = new_shares
                elif action_type == "SELL":
                    remaining_shares = max(0.0, previous_shares - shares)
                    if remaining_shares == 0:
                        tracked_shares.pop(symbol, None)
                        average_costs.pop(symbol, None)
                    else:
                        tracked_shares[symbol] = remaining_shares

        if not records:
            return {}
        latest_positions = dict(records[-1].get("positions") or {})
        return {
            symbol: cost
            for symbol, cost in average_costs.items()
            if float(latest_positions.get(symbol, 0.0) or 0.0) > 0
        }

    def mark_to_market(
        self,
        target_date: str,
        positions: dict[str, float],
    ) -> tuple[float, list[str]]:
        total = float(positions.get("CASH", 0.0) or 0.0)
        stale: list[str] = []
        for symbol, raw_shares in positions.items():
            if symbol == "CASH":
                continue
            shares = float(raw_shares or 0)
            if shares <= 0:
                continue
            price = close_on_or_before(
                parse_symbol(symbol),
                target_date,
                base_dir=self.source_data_root,
            )
            if price is None:
                stale.append(symbol)
                continue
            price_date, close = price
            if price_date != target_date:
                stale.append(symbol)
            total += close * shares
        return round(total, 4), stale

    def _append_position(self, record: dict[str, Any]) -> None:
        self.position_file.parent.mkdir(parents=True, exist_ok=True)
        with self.position_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        LOGGER.info(
            "回测仓位已追加: signature=%s date=%s id=%s",
            self.signature,
            record.get("date"),
            record.get("id"),
        )


__all__ = ["BacktestLedger", "LedgerState"]
