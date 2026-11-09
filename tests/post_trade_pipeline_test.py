from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading import post_trade_pipeline, trade_summary


def test_second_same_day_decision_source_is_rejected(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(post_trade_pipeline, "PROJECT_ROOT", tmp_path)
    history = tmp_path / "data" / "agent_data" / "book-fixed_tracked" / "stock_decisions.json"
    history.parent.mkdir(parents=True)
    history.write_text(json.dumps([{
        "symbol": "600050.SH", "operation_date": "2026-09-17", "action_type": "BUY",
    }]), encoding="utf-8")
    decision = {"stock_decisions": [{"symbol": "600050.SH", "action_type": "SELL"}]}
    with pytest.raises(RuntimeError, match="已有正式入账"):
        post_trade_pipeline._check_previously_published_symbols(
            decision, "book-fixed_tracked", "2026-09-17", tmp_path / "another" / "06_execution_log.json"
        )


def test_matching_execution_log_prevents_duplicate_trade(tmp_path: Path) -> None:
    log_path = tmp_path / "06_execution_log.json"
    log_path.write_text(
        json.dumps(
            {
                "summary_date": "2026-06-12",
                "signature": "book-fixed_tracked",
                "actions": [
                    {
                        "tool": "buy",
                        "trades": {"600150.SH": 500},
                        "result": {"is_error": False},
                    }
                ],
                "no_trade": False,
            }
        ),
        encoding="utf-8",
    )

    payload = post_trade_pipeline._load_matching_execution_log(
        log_path,
        summary_date="2026-06-12",
        signature="book-fixed_tracked",
        buys={"600150.SH": 500},
        sells={},
    )

    assert payload is not None


def test_dated_decision_entries_uses_run_date_without_mutating_input() -> None:
    decision = {
        "summary_date": "",
        "stock_decisions": [
            {
                "stock_code": "600150.SH",
                "action_type": "BUY",
                "action_num": 500,
            }
        ],
    }

    entries = post_trade_pipeline._dated_decision_entries(
        decision,
        "2026-06-12",
    )

    assert entries[0]["operation_date"] == "2026-06-12"
    assert entries[0]["symbol"] == "600150.SH"
    assert "operation_date" not in decision["stock_decisions"][0]


def test_save_daily_operations_validates_date_and_deduplicates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_summary,
        "_data_dir",
        lambda signature: str(tmp_path / signature),
    )
    trade_summary.initialize_data_files("book-test")
    decision = {
        "summary_date": "2026-06-12",
        "stock_decisions": [
            {
                "symbol": "600150.SH",
                "action_type": "BUY",
                "action_num": 500,
            }
        ],
    }

    first_saved = trade_summary.save_daily_operations("book-test", decision)
    second_saved = trade_summary.save_daily_operations("book-test", decision)

    assert len(first_saved) == 1
    assert first_saved[0]["operation_date"] == "2026-06-12"
    assert second_saved == []
    operations = trade_summary.read_json_file(
        trade_summary._stock_operations_file("book-test")
    )
    assert len(operations) == 1


def test_rebuild_skips_invalid_operation_dates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_summary,
        "_data_dir",
        lambda signature: str(tmp_path / signature),
    )
    trade_summary.initialize_data_files("book-test")
    trade_summary.write_json_file(
        trade_summary._stock_operations_file("book-test"),
        [
            {
                "symbol": "600150.SH",
                "action_type": "HOLD",
                "operation_date": "",
            },
            {
                "symbol": "600150.SH",
                "action_type": "HOLD",
                "operation_date": "2026-06-12",
            },
        ],
    )

    trade_summary._rebuild_operation_summary("book-test")

    summary = trade_summary.read_json_file(
        trade_summary._operation_summary_file("book-test")
    )
    assert len(summary) == 1
    assert summary[0]["start_date"] == "2026-06-12"


def test_process_creates_latest_decision_snapshot_per_stock(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        trade_summary,
        "_data_dir",
        lambda signature: str(tmp_path / signature),
    )
    signature = "book-test"
    trade_summary.initialize_data_files(signature)
    trade_summary.write_json_file(
        trade_summary._operation_summary_file(signature),
        [
            {
                "stock_code": "600150.SH",
                "end_date": "2026-06-12",
                "action_type": "BUY",
                "inferences": ["旧结论"],
            },
            {
                "stock_code": "600150.SH",
                "end_date": "2026-06-13",
                "action_type": "HOLD",
                "inferences": ["最新结论"],
                "price_impression": "合理",
                "next_day_watchlist": ["核验订单兑现"],
            },
            {
                "stock_code": "300750.SZ",
                "end_date": "2026-06-12",
                "action_type": "FLAT",
            },
        ],
    )

    trade_summary.process_and_merge_operations(
        signature, [], snapshot_symbols=["600150.SH", "300750.SZ"],
    )

    snapshot = trade_summary.read_json_file(
        trade_summary._latest_decision_snapshot_file(signature)
    )
    assert snapshot["source_file"] == "decision_summary.json"
    assert snapshot["latest_analysis_date"] == "2026-06-13"
    assert snapshot["stock_count"] == 2
    latest_by_symbol = {
        entry["stock_code"]: entry for entry in snapshot["stocks"]
    }
    assert latest_by_symbol["600150.SH"] == {
        "stock_code": "600150.SH", "stock_name": "中国船舶",
        "analysis_date": "2026-06-13", "price_impression": "合理",
        "next_day_watchlist": ["核验订单兑现"],
    }


def test_p0_snapshot_filters_scope_and_preserves_watchlist(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from services.research import position_symbols

    monkeypatch.setattr(trade_summary, "_data_dir", lambda signature: str(tmp_path / signature))
    monkeypatch.setattr(trade_summary, "TRACKED_A_STOCKS", [SimpleNamespace(symbol="600150.SH")])
    monkeypatch.setattr(position_symbols, "load_position_symbols", lambda **kwargs: ["300750.SZ"])
    signature = "book-fixed_tracked"
    trade_summary.initialize_data_files(signature)
    watchlist = ["核验实际订单及其收入确认期间。" * 100, "核验现金流与利润的差异。"]
    history = [
        {"stock_code": symbol, "end_date": "2026-11-09", "next_day_watchlist": watchlist,
         "court": {"verdict": "完整裁决"}, "recommended_action": "历史执行计划"}
        for symbol in ("600150.SH", "300750.SZ", "000001.SZ")
    ]
    source_path = trade_summary._operation_summary_file(signature)
    trade_summary.write_json_file(source_path, history)
    original = Path(source_path).read_bytes()
    path = trade_summary.refresh_latest_decision_snapshot(signature)
    snapshot = trade_summary.read_json_file(str(path))
    assert {entry["stock_code"] for entry in snapshot["stocks"]} == {"600150.SH", "300750.SZ"}
    for entry in snapshot["stocks"]:
        assert entry["next_day_watchlist"] == watchlist
        assert set(entry) == {"stock_code", "stock_name", "analysis_date", "price_impression", "next_day_watchlist"}
    assert Path(source_path).read_bytes() == original
    # An explicit historical universe does not inherit today's pool or manual holdings.
    path = trade_summary.refresh_latest_decision_snapshot(signature, symbols=["000001.SZ"])
    assert [entry["stock_code"] for entry in trade_summary.read_json_file(str(path))["stocks"]] == ["000001.SZ"]
