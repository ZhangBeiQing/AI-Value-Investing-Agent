from __future__ import annotations
import json
from pathlib import Path
from services.trading.post_trade_pipeline import validate_decision_json
from services.trading.decision_contract import validate_stock_decision_entry
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SYMBOL = "300613.SZ"

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
