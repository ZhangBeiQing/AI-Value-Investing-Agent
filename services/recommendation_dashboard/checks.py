"""Focused checks for the BUY signal performance shown by the web dashboard."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from services.recommendation_dashboard.data import build_dashboard, build_holdings, stock_detail
from services.recommendation_dashboard.jobs import _analysis_date, _write_job, add_stock_to_universe, job_result, read_job, resolve_stock_name, run_job, start_job
from services.recommendation_dashboard.research import read_research_package, research_packages
from services.trading.dashboard_decision_publisher import publish_dashboard_verdict
from services.trading.trade_summary import use_agent_data_root


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

    def test_research_package_is_selected_by_symbol_and_date(self) -> None:
        for day in ("2026-09-15", "2026-09-16"):
            path = self.data_dir / "skill_runs" / day / "fixed_tracked" / "04_stock_research" / f"甲公司_600001.SH_{day}_research.md"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"{day} 的研究包", encoding="utf-8")
        self.assertEqual(research_packages(self.data_dir, "600001.SH")[0]["date"], "2026-09-16")
        self.assertIn("2026-09-15", read_research_package(self.data_dir, "600001.SH", "2026-09-15")["content"])
        self.assertIsNone(read_research_package(self.data_dir, "600001.SH", "2026-09-14"))
        with self.assertRaises(ValueError):
            research_packages(self.data_dir, "../../etc/passwd")

    def test_new_stock_name_is_verified_before_universe_write(self) -> None:
        universe_dir = self.data_dir / "universe"
        universe_dir.mkdir()
        (universe_dir / "master_universe.json").write_text(
            json.dumps({"stocks": [{"symbol": "600001.SH", "name": "甲公司"}]}, ensure_ascii=False), encoding="utf-8"
        )
        mapping = self.data_dir / "global_cache" / "symbol_stock_name_mapping.csv"
        mapping.parent.mkdir()
        mapping.write_text("symbol,stock_name\n600050.SH,中国联通\n", encoding="utf-8")
        self.assertEqual(resolve_stock_name(self.data_dir, "中国联通")[0]["symbol"], "600050.SH")
        with self.assertRaises(ValueError):
            add_stock_to_universe(self.data_dir, "600051.SH", "中国联通")
        added = add_stock_to_universe(self.data_dir, "600050.SH", "中国联通")
        self.assertTrue(added["in_universe"])
        self.assertEqual(len(json.loads((universe_dir / "master_universe.json").read_text(encoding="utf-8"))["stocks"]), 2)

    def test_name_resolution_uses_existing_market_and_financial_caches(self) -> None:
        universe = self.data_dir / "universe" / "master_universe.json"
        universe.parent.mkdir(parents=True)
        universe.write_text('{"stocks": []}', encoding="utf-8")
        board = self.data_dir / "global_cache" / "board_metrics_ths" / "latest_market_snapshot.json"
        board.parent.mkdir(parents=True)
        board.write_text(json.dumps({"boards": [{"related_stock_hints": [
            {"symbol": "001965.SZ", "name": "招商公路"},
        ]}]}), encoding="utf-8")
        raw = self.data_dir / "global_cache" / "industry_financial_panel" / "raw" / "20260331" / "2026-07-14.csv"
        raw.parent.mkdir(parents=True)
        raw.write_text("stock_code,stock_name\n001965,招商公路\n600123,其他公司\n", encoding="utf-8")
        self.assertEqual(resolve_stock_name(self.data_dir, "招商公路"), [
            {"symbol": "001965.SZ", "name": "招商公路", "in_universe": False},
        ])
        board.unlink()
        self.assertEqual(resolve_stock_name(self.data_dir, "招商公路")[0]["symbol"], "001965.SZ")
        self.assertEqual(resolve_stock_name(self.data_dir, "不存在的公司"), [])

    def test_completed_independent_research_is_visible_but_not_counted_as_buy(self) -> None:
        universe_dir = self.data_dir / "universe"
        universe_dir.mkdir()
        (universe_dir / "master_universe.json").write_text(
            json.dumps({"stocks": [{"symbol": "600050.SH", "name": "中国联通"}]}, ensure_ascii=False), encoding="utf-8"
        )
        job_id = str(uuid.uuid4())
        _write_job(self.data_dir, {
            "id": job_id, "symbol": "600050.SH", "date": "2026-09-16", "status": "complete",
            "message": "完成", "created_at": "2026-09-16T21:00:00+08:00", "pid": None,
        })
        verdict_path = self.data_dir / "web_research_runs" / job_id / "skill_runs" / "2026-09-16" / "fixed_tracked" / "debate" / "中国联通_600050.SH" / "final" / "stock_verdict.json"
        verdict_path.parent.mkdir(parents=True)
        verdict_path.write_text('{"action_type":"BUY","price_impression":"偏低估"}', encoding="utf-8")
        result = build_dashboard(self.data_dir)
        independent = next(item for item in result["stocks"] if item["symbol"] == "600050.SH")
        self.assertEqual(independent["source"], "on_demand")
        self.assertEqual(independent["latest_action"], "BUY")
        self.assertFalse(independent["has_buy"])
        self.assertEqual(result["buy_stock_count"], 2)
        self.assertEqual(job_result(self.data_dir, job_id)["verdict"]["price_impression"], "偏低估")

    def test_detail_uses_published_web_debate_when_it_matches_latest_decision(self) -> None:
        verdict = self._verdict(symbol="600001.SH")
        verdict["stock_name"] = "甲公司"
        operations = self.data_dir / "agent_data" / "book-fixed_tracked" / "stock_decisions.json"
        history = json.loads(operations.read_text(encoding="utf-8"))
        history.append({**verdict, "operation_date": "2026-09-17"})
        operations.write_text(json.dumps(history, ensure_ascii=False), encoding="utf-8")
        job_id = str(uuid.uuid4())
        _write_job(self.data_dir, {
            "id": job_id, "symbol": "600001.SH", "date": "2026-09-17", "status": "complete",
            "message": "完成", "created_at": "2026-09-17T21:00:00+08:00", "pid": None,
        })
        debate = self.data_dir / "web_research_runs" / job_id / "skill_runs" / "2026-09-17" / "fixed_tracked" / "debate" / "甲公司_600001.SH"
        final = debate / "final" / "stock_verdict.json"
        final.parent.mkdir(parents=True)
        final.write_text(json.dumps(verdict, ensure_ascii=False), encoding="utf-8")
        bull = debate / "advocates" / "bull" / "opening.json"
        bull.parent.mkdir(parents=True)
        bull.write_text('{"arguments":["新辩论"]}', encoding="utf-8")
        detail = stock_detail(self.data_dir, "600001.SH")
        self.assertEqual(detail["debate_date"], "2026-09-17")
        self.assertEqual(detail["stages"]["bull"]["arguments"], ["新辩论"])

    def test_job_requires_completed_daily_inputs(self) -> None:
        with patch("services.recommendation_dashboard.jobs._analysis_date", return_value="2026-09-16"):
            with self.assertRaisesRegex(ValueError, "每日公共研究输入尚未就绪"):
                start_job(self.data_dir, "600001.SH")

    def test_job_uses_last_prepared_trading_day_before_today_is_ready(self) -> None:
        official = self.data_dir / "skill_runs" / "2026-09-17" / "fixed_tracked"
        official.mkdir(parents=True)
        for name in ("01_global_context.md", "03_stock_analysis_input.md"):
            (official / name).write_text("ready", encoding="utf-8")
        with patch("services.recommendation_dashboard.jobs.date") as clock, \
             patch("services.recommendation_dashboard.jobs.inspect_market_session", return_value=SimpleNamespace(is_trading_day=True, previous_trading_day="2026-09-17")):
            clock.today.return_value.isoformat.return_value = "2026-09-18"
            self.assertEqual(_analysis_date(self.data_dir), "2026-09-17")

    def test_worker_records_missing_daily_inputs_as_failure(self) -> None:
        job_id = str(uuid.uuid4())
        _write_job(self.data_dir, {
            "id": job_id, "symbol": "600001.SH", "date": "2026-09-16", "status": "queued",
            "message": "等待", "created_at": "2026-09-16T21:00:00+08:00", "pid": None,
        })
        run_job(self.data_dir, job_id)
        self.assertEqual(read_job(self.data_dir, job_id)["status"], "failed")

    def test_duplicate_active_job_reuses_existing_job(self) -> None:
        official_dir = self.data_dir / "skill_runs" / "2026-09-16" / "fixed_tracked"
        official_dir.mkdir(parents=True)
        for name in ("01_global_context.md", "03_stock_analysis_input.md"):
            (official_dir / name).write_text("ready", encoding="utf-8")
        with patch("services.recommendation_dashboard.jobs._analysis_date", return_value="2026-09-16"), \
             patch("services.recommendation_dashboard.jobs.subprocess.Popen", return_value=SimpleNamespace(pid=os.getpid())) as spawn:
            first = start_job(self.data_dir, "600001.SH")
            second = start_job(self.data_dir, "600001.SH")
        self.assertEqual(first["id"], second["id"])
        spawn.assert_called_once()

    @staticmethod
    def _verdict(symbol: str = "600050.SH", action: str = "HOLD", quantity: int = 0) -> dict:
        return {
            "symbol": symbol, "stock_name": "中国联通", "scan": "测试扫描",
            "delta_summary": "测试变化", "key_facts": [], "inferences": [],
            "court": {"pro": [], "con": [], "verdict": "测试裁决"},
            "price_impression": "合理", "recommended_action": "测试动作",
            "action_type": action, "action_num": quantity,
            "key_risks": [], "next_day_watchlist": [], "confidence_score": 0.5,
        }

    def test_dashboard_publication_merges_hold_without_touching_real_holdings(self) -> None:
        workspace = self.data_dir / "web_research_runs" / "job" / "skill_runs" / "2026-09-17" / "fixed_tracked"
        workspace.mkdir(parents=True)
        manual = self.data_dir / "agent_data" / "book-fixed_tracked" / "position" / "manual_position_override.json"
        manual.write_text('{"as_of_date":"2026-09-17","positions":{}}', encoding="utf-8")
        original = manual.read_bytes()
        with use_agent_data_root(self.data_dir / "agent_data"), \
             patch("services.trading.dashboard_decision_publisher.PROJECT_DATA", self.data_dir), \
             patch("services.trading.dashboard_decision_publisher.execute_trade_from_decision") as execute:
            publish_dashboard_verdict(self.data_dir, workspace, symbol="600050.SH", run_date="2026-09-17", verdict=self._verdict())
        self.assertEqual(json.loads((workspace / "05_decision.json").read_text(encoding="utf-8"))["stock_decisions"][0]["symbol"], "600050.SH")
        self.assertFalse(json.loads((workspace / "06_execution_log.json").read_text(encoding="utf-8"))["position_record_written"])
        self.assertEqual(manual.read_bytes(), original)
        execute.assert_not_called()
        agent_dir = self.data_dir / "agent_data" / "book-fixed_tracked"
        operations = json.loads((agent_dir / "stock_decisions.json").read_text(encoding="utf-8"))
        self.assertTrue(any(item.get("symbol") == "600050.SH" and item.get("operation_date") == "2026-09-17" for item in operations))
        self.assertEqual(json.loads((agent_dir / "latest_decision_snapshot.json").read_text(encoding="utf-8"))["stock_count"], 1)
        self.assertTrue((workspace / "07_daily_summary.json").is_file())
        self.assertTrue((workspace / "08_history_merge.json").is_file())

    def test_dashboard_publication_buy_uses_existing_executor_once(self) -> None:
        workspace = self.data_dir / "web_research_runs" / "job" / "skill_runs" / "2026-09-17" / "fixed_tracked"
        workspace.mkdir(parents=True)
        verdict = self._verdict(action="BUY", quantity=100)
        with patch("services.trading.dashboard_decision_publisher.PROJECT_DATA", self.data_dir), \
             patch("services.trading.dashboard_decision_publisher.merge_trade_summary") as merge, \
             patch("services.trading.dashboard_decision_publisher.execute_trade_from_decision") as execute:
            publish_dashboard_verdict(self.data_dir, workspace, symbol="600050.SH", run_date="2026-09-17", verdict=verdict)
        execute.assert_called_once()
        merge.assert_called_once()

    def test_dashboard_buy_publishes_summary_and_updates_only_virtual_ledger(self) -> None:
        data_root = self.data_dir / "data"
        workspace = data_root / "web_research_runs" / "job" / "skill_runs" / "2026-09-17" / "fixed_tracked"
        workspace.mkdir(parents=True)
        position_dir = data_root / "agent_data" / "book-fixed_tracked" / "position"
        position_dir.mkdir(parents=True)
        position = position_dir / "position.jsonl"
        position.write_text(json.dumps({
            "date": "2026-09-16", "id": 7, "positions": {"CASH": 1000.0},
            "this_action": {"action": "init"}, "total_value": 1000.0,
        }) + "\n", encoding="utf-8")
        manual = position_dir / "manual_position_override.json"
        manual.write_text('{"as_of_date":"2026-09-17","cash":5000,"positions":{"中国联通":{"shares":500,"avg_cost":10}}}', encoding="utf-8")
        manual_before = manual.read_bytes()
        fake_module_path = self.data_dir / "services" / "trading" / "price_tools.py"
        with use_agent_data_root(data_root / "agent_data"), \
             patch("services.trading.dashboard_decision_publisher.PROJECT_DATA", data_root), \
             patch("services.trading.post_trade_pipeline.PROJECT_ROOT", self.data_dir), \
             patch("services.trading.trade_executor.PROJECT_ROOT", self.data_dir), \
             patch("services.trading.price_tools.__file__", str(fake_module_path)), \
             patch("services.trading.trade_executor.get_prev_close_prices", return_value={"600050.SH_price": 10.0}), \
             patch("services.trading.trade_executor.compute_total_value", return_value=1000.0), \
             patch.dict(os.environ, {"RUNTIME_ENV_PATH": str(workspace / "runtime_env.json")}):
            publish_dashboard_verdict(
                data_root, workspace, symbol="600050.SH", run_date="2026-09-17",
                verdict=self._verdict(action="BUY", quantity=100),
            )
        records = [json.loads(line) for line in position.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[-1]["positions"]["600050.SH"], 100)
        self.assertEqual(records[-1]["positions"]["CASH"], 0)
        self.assertEqual(manual.read_bytes(), manual_before)
        self.assertEqual(json.loads((workspace / "06_execution_log.json").read_text(encoding="utf-8"))["actions"][0]["tool"], "buy")
        self.assertTrue((data_root / "agent_data" / "book-fixed_tracked" / "decision_summary.json").is_file())

    def test_dashboard_publication_rejects_existing_same_day_decision(self) -> None:
        workspace = self.data_dir / "web_research_runs" / "job" / "skill_runs" / "2026-09-17" / "fixed_tracked"
        workspace.mkdir(parents=True)
        path = self.data_dir / "agent_data" / "book-fixed_tracked" / "stock_decisions.json"
        path.write_text(json.dumps([{**self._verdict(), "operation_date": "2026-09-17"}]), encoding="utf-8")
        with patch("services.trading.dashboard_decision_publisher.PROJECT_DATA", self.data_dir), \
             patch("services.trading.dashboard_decision_publisher.merge_trade_summary") as merge:
            with self.assertRaisesRegex(RuntimeError, "不同的正式决策"):
                publish_dashboard_verdict(self.data_dir, workspace, symbol="600050.SH", run_date="2026-09-17", verdict=self._verdict(action="BUY", quantity=100))
        merge.assert_not_called()

    def test_virtual_position_lookup_ignores_manual_override(self) -> None:
        from datetime import datetime
        from services.trading.price_tools import get_latest_virtual_position

        record = {
            "date": "2026-09-16", "_parsed_date": datetime(2026, 9, 16).date(),
            "id": 7, "positions": {"CASH": 900, "600050.SH": 100},
        }
        with patch("services.trading.price_tools._load_position_records", return_value=[record]), \
             patch("services.trading.price_tools._load_manual_position_override", side_effect=AssertionError("不得读取人工持仓")):
            positions, record_id = get_latest_virtual_position("2026-09-17", "book-fixed_tracked")
        self.assertEqual(positions["600050.SH"], 100)
        self.assertEqual(record_id, 7)


if __name__ == "__main__":
    unittest.main()
