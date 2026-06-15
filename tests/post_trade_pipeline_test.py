from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading import post_trade_pipeline, trade_summary


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
