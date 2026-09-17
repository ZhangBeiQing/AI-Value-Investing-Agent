"""Focused checks for the BUY signal performance shown by the web dashboard."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.recommendation_dashboard.data import build_dashboard, build_holdings, stock_detail


class RecommendationDashboardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        agent_dir = self.data_dir / "agent_data" / "book-fixed_tracked"
        (agent_dir / "position").mkdir(parents=True)
        decisions = [
            {"symbol": "600001.SH", "stock_name": "甲公司", "operation_date": "2026-09-10", "action_type": "BUY", "price_impression": "低估"},
            {"symbol": "600001.SH", "stock_name": "甲公司", "operation_date": "2026-09-15", "action_type": "HOLD", "price_impression": "合理"},
            {"symbol": "600002.SH", "stock_name": "乙公司", "operation_date": "2026-09-15", "action_type": "BUY"},
            {"symbol": "600003.SH", "stock_name": "丙公司", "operation_date": "2026-09-15", "action_type": "FLAT"},
        ]
        (agent_dir / "stock_decisions.json").write_text(json.dumps(decisions, ensure_ascii=False), encoding="utf-8")
        events = [
            {"date": "2026-09-10", "this_action": {"action": "buy", "trades": {"600001.SH": 100}}},
            {"date": "2026-09-15", "this_action": {"action": "buy", "trades": {"600001.SH": 200, "600002.SH": 100}}},
        ]
        (agent_dir / "position" / "position.jsonl").write_text(
            "\n".join(json.dumps(event, ensure_ascii=False) for event in events), encoding="utf-8"
        )
        self._price("甲公司", "600001.SH", [("2026-09-10", 10), ("2026-09-15", 12), ("2026-09-16", 11)])
        self._price("乙公司", "600002.SH", [("2026-09-15", 20)])
        self._price("丙公司", "600003.SH", [("2026-09-16", 30)])

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _price(self, name: str, symbol: str, rows: list[tuple[str, int]]) -> None:
        path = self.data_dir / "stock_info" / f"{name}_{symbol}" / "prices" / "price.csv"
        path.parent.mkdir(parents=True)
        path.write_text("日期,收盘\n" + "".join(f"{day},{close}\n" for day, close in rows), encoding="utf-8")

    def test_repeated_buy_uses_first_exact_close_and_excludes_stale_from_total(self) -> None:
        dashboard = build_dashboard(self.data_dir)
        rows = {row["symbol"]: row for row in dashboard["stocks"]}
        self.assertEqual(dashboard["analyzed_count"], 3)
        self.assertEqual(dashboard["buy_stock_count"], 2)
        self.assertEqual(dashboard["buy_event_count"], 3)
        self.assertEqual(rows["600001.SH"]["return_pct"], 10.0)
        self.assertEqual(rows["600001.SH"]["buy_events"][1]["return_pct"], -8.33)
        self.assertFalse(rows["600002.SH"]["price_is_latest_for_market"])
        self.assertEqual(dashboard["priced_buy_stock_count"], 1)
        self.assertEqual(dashboard["equal_weight_return_pct"], 10.0)
        self.assertEqual(rows["600001.SH"]["latest_action"], "HOLD")
        self.assertEqual(rows["600001.SH"]["latest_analysis_date"], "2026-09-15")
        self.assertEqual(rows["600001.SH"]["price_impression"], "合理")

    def test_detail_uses_latest_available_complete_debate(self) -> None:
        path = self.data_dir / "skill_runs" / "2026-09-10" / "fixed_tracked" / "debate" / "甲公司_600001.SH" / "jury" / "juror_01" / "ballot.json"
        path.parent.mkdir(parents=True)
        path.write_text('{"action_type":"BUY","reason":"test"}', encoding="utf-8")
        detail = stock_detail(self.data_dir, "600001.SH")
        self.assertEqual(detail["latest_decision_date"], "2026-09-15")
        self.assertEqual(detail["debate_date"], "2026-09-10")
        self.assertEqual(detail["stages"]["juror_01"]["action_type"], "BUY")
        self.assertIsNone(stock_detail(self.data_dir, "600999.SH"))

    def test_manual_holdings_keep_cny_and_hkd_totals_separate(self) -> None:
        agent_dir = self.data_dir / "agent_data" / "book-fixed_tracked"
        decisions_path = agent_dir / "stock_decisions.json"
        decisions = json.loads(decisions_path.read_text(encoding="utf-8"))
        decisions.append({"symbol": "00700.HK", "stock_name": "港股公司", "operation_date": "2026-09-15", "action_type": "HOLD"})
        decisions_path.write_text(json.dumps(decisions, ensure_ascii=False), encoding="utf-8")
        self._price("港股公司", "00700.HK", [("2026-09-16", 60)])
        holdings = {
            "as_of_date": "2026-09-15",
            "cash": 1000,
            "positions": {
                "甲公司": {"shares": 100, "avg_cost": 9},
                "港股公司": {"shares": 200, "avg_cost": 50},
                "未知股票": {"shares": 10, "avg_cost": 3},
            },
        }
        (agent_dir / "position" / "manual_position_override.json").write_text(
            json.dumps(holdings, ensure_ascii=False), encoding="utf-8"
        )
        result = build_holdings(self.data_dir)
        self.assertEqual(result["position_count"], 3)
        self.assertEqual(result["priced_count"], 2)
        self.assertEqual(result["cash_cny"], 1000)
        self.assertEqual(result["totals"]["CNY"]["market_value"], 1100)
        self.assertEqual(result["totals"]["CNY"]["unrealized_pnl"], 200)
        self.assertEqual(result["totals"]["HKD"]["market_value"], 12000)
        self.assertEqual(result["totals"]["HKD"]["unrealized_pnl"], 2000)
        self.assertIsNone(result["positions"][2]["market_value"])


if __name__ == "__main__":
    unittest.main()
