from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.merge_subagent_decisions import (
    find_debate_verdict_files,
    merge_decisions,
)
from services.trading.debate_pipeline import (
    JUROR_IDS,
    aggregate_jury_votes,
    atomic_write_json,
    debate_directory_name,
    debate_symbol_dir,
    prepare_debate_directories,
    validate_debate_artifacts,
)
from services.trading.post_trade_pipeline import validate_decision_json
from services.trading.decision_contract import (
    validate_stock_decision_entry,
)


SYMBOL = "300613.SZ"


def _write_ballots(
    book_dir: Path,
    actions: tuple[str, str, str],
    price_impressions: tuple[str, str, str] = (
        "偏低估",
        "合理",
        "合理偏贵",
    ),
) -> None:
    for juror_id, action, price_impression in zip(
        JUROR_IDS,
        actions,
        price_impressions,
    ):
        atomic_write_json(
            debate_symbol_dir(book_dir, SYMBOL)
            / "jury"
            / juror_id
            / "ballot.json",
            {
                "action_type": action,
                "price_impression": price_impression,
                "reason": f"{juror_id} reason",
            },
        )


def _valid_verdict(action_type: str = "HOLD") -> dict:
    return {
        "symbol": SYMBOL,
        "stock_name": "富瀚微",
        "scan": "今日量价扫描。",
        "delta_summary": "今日变化。",
        "key_facts": ["事实一"],
        "inferences": ["推断一"],
        "court": {
            "pro": ["正方理由"],
            "con": ["反方理由"],
            "verdict": f"{action_type}，服从投票结果。",
        },
        "price_impression": "合理",
        "recommended_action": "下一交易日不交易。",
        "action_type": action_type,
        "action_num": 0,
        "key_risks": ["风险一"],
        "next_day_watchlist": ["观察一"],
        "confidence_score": 0.7,
    }


def test_decision_validation_rejects_urls() -> None:
    decision = {
        "summary_date": "2026-08-19",
        "system_risk_notes": ["详情 https://example.com"],
        "system_focus_items": [],
        "stock_decisions": [_valid_verdict()],
    }
    errors = validate_decision_json(
        decision,
        book_type="fixed_tracked",
    )
    assert any("禁止包含原始 URL" in error for error in errors)


def test_prepare_debate_directories_assigns_unique_role_paths(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()

    symbol_dir = prepare_debate_directories(book_dir, SYMBOL)

    assert symbol_dir.name == "富瀚微_300613.SZ"
    assert debate_directory_name(SYMBOL) == symbol_dir.name

    expected = {
        symbol_dir / "advocates" / "bull",
        symbol_dir / "advocates" / "bear",
        *(symbol_dir / "jury" / juror_id for juror_id in JUROR_IDS),
        symbol_dir / "final",
    }
    assert all(path.is_dir() for path in expected)
    assert not list(symbol_dir.rglob("*.json"))


def test_aggregate_jury_votes_two_votes_produce_majority(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()
    prepare_debate_directories(book_dir, SYMBOL)
    _write_ballots(book_dir, ("BUY", "BUY", "HOLD"))

    summary_path, summary = aggregate_jury_votes(
        book_dir,
        SYMBOL,
        position_shares=100,
    )

    assert summary_path.exists()
    assert summary["status"] == "majority"
    assert summary["majority_action"] == "BUY"
    assert summary["resolved_action"] == "BUY"
    assert summary["counts"]["BUY"] == 2
    assert summary["price_impression_votes"] == [
        {
            "juror": "juror_01",
            "price_impression": "偏低估",
        },
        {
            "juror": "juror_02",
            "price_impression": "合理",
        },
            {
                "juror": "juror_03",
                "price_impression": "合理偏贵",
            },
    ]
    assert "resolved_price_impression" not in summary


def test_aggregate_jury_votes_three_different_actions_has_no_majority(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()
    prepare_debate_directories(book_dir, SYMBOL)
    _write_ballots(book_dir, ("BUY", "SELL", "HOLD"))

    _, summary = aggregate_jury_votes(
        book_dir,
        SYMBOL,
        position_shares=100,
    )

    assert summary["status"] == "no_majority"
    assert summary["majority_action"] is None
    assert summary["resolved_action"] == "HOLD"


def test_aggregate_jury_votes_rejects_invalid_ballot(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()
    prepare_debate_directories(book_dir, SYMBOL)
    _write_ballots(book_dir, ("BUY", "INVALID", "HOLD"))

    with pytest.raises(ValueError, match="action_type 非法"):
        aggregate_jury_votes(
            book_dir,
            SYMBOL,
            position_shares=100,
        )


def test_aggregate_jury_votes_rejects_invalid_price_impression(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()
    prepare_debate_directories(book_dir, SYMBOL)
    _write_ballots(
        book_dir,
        ("BUY", "BUY", "HOLD"),
        ("偏低估", "无法判断", "合理"),
    )

    with pytest.raises(ValueError, match="price_impression 非法"):
        aggregate_jury_votes(
            book_dir,
            SYMBOL,
            position_shares=100,
        )


def test_stock_decision_schema_rejects_extra_fields() -> None:
    verdict = _valid_verdict()
    verdict["strongest_arguments"] = []

    errors = validate_stock_decision_entry(verdict)

    assert "不允许额外字段: strongest_arguments" in errors


def test_stock_decision_example_matches_schema() -> None:
    example_path = (
        PROJECT_ROOT
        / "configs"
        / "prompt_flow"
        / "fixed_tracked"
        / "stock_decision.example.json"
    )
    example = json.loads(example_path.read_text(encoding="utf-8"))

    assert validate_stock_decision_entry(example) == []


def test_validate_debate_artifacts_rejects_verdict_overriding_majority(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()
    symbol_dir = prepare_debate_directories(book_dir, SYMBOL)
    for side in ["bull", "bear"]:
        atomic_write_json(
            symbol_dir / "advocates" / side / "opening.json",
            {"arguments": [f"{side} argument"]},
        )
        atomic_write_json(
            symbol_dir / "advocates" / side / "rebuttal.json",
            {"rebuttals": []},
        )
    _write_ballots(book_dir, ("BUY", "BUY", "HOLD"))
    aggregate_jury_votes(
        book_dir,
        SYMBOL,
        position_shares=100,
    )
    atomic_write_json(
        symbol_dir / "final" / "stock_verdict.json",
        _valid_verdict("HOLD"),
    )

    errors = validate_debate_artifacts(
        book_dir,
        SYMBOL,
        require_verdict=True,
    )

    assert any(
        "action_type 必须服从多数票或回退结果 BUY" in error
        for error in errors
    )


def test_aggregate_rejects_hold_vote_for_unheld_stock(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()
    prepare_debate_directories(book_dir, SYMBOL)
    _write_ballots(book_dir, ("BUY", "FLAT", "HOLD"))

    with pytest.raises(ValueError, match="与当前持仓状态不兼容"):
        aggregate_jury_votes(
            book_dir,
            SYMBOL,
            position_shares=0,
        )


def test_validate_rejects_tampered_vote_summary(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "fixed_tracked"
    book_dir.mkdir()
    symbol_dir = prepare_debate_directories(book_dir, SYMBOL)
    for side in ["bull", "bear"]:
        atomic_write_json(
            symbol_dir / "advocates" / side / "opening.json",
            {"arguments": [f"{side} argument"]},
        )
        atomic_write_json(
            symbol_dir / "advocates" / side / "rebuttal.json",
            {"rebuttals": []},
        )
    _write_ballots(book_dir, ("BUY", "BUY", "HOLD"))
    summary_path, summary = aggregate_jury_votes(
        book_dir,
        SYMBOL,
        position_shares=100,
    )
    summary["counts"] = {"BUY": 0, "SELL": 2, "HOLD": 1, "FLAT": 0}
    summary["majority_action"] = "SELL"
    summary["resolved_action"] = "SELL"
    summary["price_impression_votes"][0]["price_impression"] = "泡沫"
    atomic_write_json(summary_path, summary)

    errors = validate_debate_artifacts(book_dir, SYMBOL)

    assert any("counts 与实际 ballot 不一致" in error for error in errors)
    assert any(
        "majority_action 与实际 ballot 不一致" in error
        for error in errors
    )
    assert any(
        "price_impression_votes 与实际 ballot 不一致" in error
        for error in errors
    )


def test_debate_verdict_merges_without_breaking_legacy_merge_contract(
    tmp_path: Path,
) -> None:
    book_dir = tmp_path / "2026-07-31" / "fixed_tracked"
    verdict_path = (
        debate_symbol_dir(book_dir, SYMBOL, allow_legacy=False)
        / "final"
        / "stock_verdict.json"
    )
    atomic_write_json(verdict_path, _valid_verdict())

    files = find_debate_verdict_files(book_dir / "debate")
    merged, replaced, appended, errors = merge_decisions(
        {
            "summary_date": "",
            "stock_decisions": [],
            "system_risk_notes": [],
            "system_focus_items": [],
        },
        files,
        validate_debate=True,
    )

    assert errors == []
    assert replaced == []
    assert appended == [SYMBOL]
    assert merged["stock_decisions"][0]["symbol"] == SYMBOL

    legacy_path = (
        book_dir
        / "subagent_result"
        / f"富瀚微_{SYMBOL}_2026-07-31_decision.json"
    )
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(
        json.dumps({"symbol": SYMBOL, "action_type": "HOLD"}),
        encoding="utf-8",
    )
    legacy_merged, _, legacy_appended, legacy_errors = merge_decisions(
        {"stock_decisions": []},
        [legacy_path],
    )
    assert legacy_errors == []
    assert legacy_appended == [SYMBOL]
    assert legacy_merged["stock_decisions"][0]["action_type"] == "HOLD"


def test_update_analysis_index_records_last_deep_analysis_price(
    tmp_path: Path,
) -> None:
    """索引须记录分析当天收盘价，并对历史缺失条目按 deep_analysis_date 回填。"""
    from scripts.merge_subagent_decisions import update_analysis_index

    base_dir = tmp_path / "skill_runs"
    book_dir = base_dir / "2026-07-31" / "fixed_tracked"
    book_dir.mkdir(parents=True)

    snapshot = {
        "timestamp": "2026-07-31",
        "stocks": {
            SYMBOL: {"latest_price": 88.88},
            "000001.SZ": {"latest_price": 11.22},
        },
    }
    (book_dir / "02_basic_snapshot_payload.json").write_text(
        json.dumps(snapshot, ensure_ascii=False),
        encoding="utf-8",
    )

    merged = {
        "stock_decisions": [
            {
                "symbol": SYMBOL,
                "price_impression": "略贵",
                "confidence_score": 0.8,
            }
        ]
    }
    update_analysis_index(merged, "fixed_tracked", "2026-07-31", base_dir)

    index = json.loads(
        (base_dir / "_analysis_index.json").read_text(encoding="utf-8")
    )
    entry = index["fixed_tracked"][SYMBOL]
    assert entry["deep_analysis_date"] == "2026-07-31"
    assert entry["last_deep_analysis_price"] == 88.88
    assert entry["price_impression"] == "略贵"
    assert entry["confidence_score"] == 0.8

    # 历史缺失条目回填：新增一日记录，旧条目无价格，应从其分析日快照回填
    snapshot_old = {
        "stocks": {"000001.SZ": {"latest_price": 12.34}},
    }
    old_book_dir = base_dir / "2026-07-30" / "fixed_tracked"
    old_book_dir.mkdir(parents=True)
    (old_book_dir / "02_basic_snapshot_payload.json").write_text(
        json.dumps(snapshot_old, ensure_ascii=False),
        encoding="utf-8",
    )
    index["fixed_tracked"]["000001.SZ"] = {
        "deep_analysis_date": "2026-07-30",
        "price_impression": "合理",
        "confidence_score": 0.7,
    }
    (base_dir / "_analysis_index.json").write_text(
        json.dumps(index, ensure_ascii=False), encoding="utf-8"
    )

    update_analysis_index(
        {
            "stock_decisions": [
                {
                    "symbol": SYMBOL,
                    "price_impression": "略贵",
                    "confidence_score": 0.8,
                }
            ]
        },
        "fixed_tracked",
        "2026-08-01",
        base_dir,
    )
    index = json.loads(
        (base_dir / "_analysis_index.json").read_text(encoding="utf-8")
    )
    assert index["fixed_tracked"]["000001.SZ"]["last_deep_analysis_price"] == 12.34
    assert index["fixed_tracked"][SYMBOL]["deep_analysis_date"] == "2026-08-01"

    # 当日快照缺失该股时 price 为 None，且不覆盖已有历史值
    update_analysis_index(
        {
            "stock_decisions": [
                {
                    "symbol": "999999.SZ",
                    "price_impression": "明显高估",
                    "confidence_score": 0.9,
                }
            ]
        },
        "fixed_tracked",
        "2026-08-01",
        base_dir,
    )
    index = json.loads(
        (base_dir / "_analysis_index.json").read_text(encoding="utf-8")
    )
    assert index["fixed_tracked"]["999999.SZ"]["last_deep_analysis_price"] is None
