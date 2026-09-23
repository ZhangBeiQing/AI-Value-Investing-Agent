from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading import trade_summary
from services.pipeline.steps import build_stock_research


def test_memory_context_keeps_only_latest_summary_and_hides_expired_plans(
    tmp_path: Path,
    monkeypatch,
) -> None:
    operations_path = tmp_path / "stock_decisions.json"
    legacy_buy = {
        "symbol": "301536.SZ",
        "operation_date": "2026-07-30",
        "action_type": "BUY",
        "action_num": 200,
        "history_anchor": "首次分析",
        "motion": "BUY 候选",
        "delta_summary": "业绩预告确认增长。",
        "key_facts": ["事实一"],
        "inferences": ["推断一"],
        "court": {
            "pro": ["正方理由"],
            "con": ["反方理由"],
            "verdict": "BUY，跌到某价格继续加仓。",
        },
        "recommended_action": "下一交易日按指定价格买入。",
        "price_target": "某价格执行",
        "key_risks": ["风险一"],
        "next_day_watchlist": ["核验正式财报是否达到预告中值"],
    }
    current_hold = {
        "symbol": "301536.SZ",
        "operation_date": "2026-07-31",
        "action_type": "HOLD",
        "action_num": 0,
        "delta_summary": "正式财报尚未发布，原假设未被证伪。",
        "key_facts": ["事实二"],
        "inferences": ["推断二"],
        "court": {
            "pro": ["当前正方理由"],
            "con": ["当前反方理由"],
            "verdict": "HOLD。增长逻辑仍在，但现金流风险尚未解除。",
        },
        "recommended_action": "仅针对下一交易日维持持仓。",
        "sizing_reason": "当前仓位偏轻但价格已上移，安全边际不足，因此不加仓。",
        "key_risks": ["风险二"],
        "next_day_watchlist": [
            "核验公司是否发布正式财报",
            "核验行业需求是否发生变化",
        ],
    }
    operations_path.write_text(
        json.dumps([legacy_buy, current_hold], ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        trade_summary,
        "_stock_operations_file",
        lambda _signature: str(operations_path),
    )

    context = trade_summary.get_stock_memory_context(
        "book-fixed-tracked",
        "301536.SZ",
    )
    serialized = json.dumps(context, ensure_ascii=False)

    assert "position_change_events" not in context
    assert context["latest_thesis_review"]["operation_date"] == "2026-07-31"
    assert "verdict" not in context["latest_thesis_review"]["court"]
    assert context["latest_thesis_review"]["court"]["pro"] == ["当前正方理由"]
    assert context["pending_checks"] == current_hold["next_day_watchlist"]
    assert "recommended_action" not in serialized
    assert "price_target" not in serialized
    assert "sizing_reason" not in serialized
    assert "跌到某价格继续加仓" not in serialized
    assert "业绩预告确认增长" not in serialized


def test_stock_research_renders_only_memory_projection(monkeypatch) -> None:
    memory = {
        "latest_thesis_review": {
            "operation_date": "2026-07-31",
            "action_type": "HOLD",
            "action_num": 0,
            "key_facts": ["最后一次总结依据"],
        },
        "pending_checks": ["核验正式财报"],
    }
    monkeypatch.setattr(
        build_stock_research,
        "get_stock_memory_context",
        lambda _signature, _symbol: memory,
    )
    monkeypatch.setattr(
        build_stock_research,
        "_load_or_build_base_artifact",
        lambda _symbol, _date: {
            "price_payload": {
                "price_report": {},
                "valuation_report": "",
            },
            "news_payload": {
                "news_items": [],
                "diagnostics": [],
            },
            "financial_payload": {
                "content": "",
                "metadata": {},
            },
        },
    )

    markdown = build_stock_research.build_research_markdown(
        "301536.SZ",
        "2026-07-31",
        signature="book-fixed-tracked",
    )

    assert "## 4. 持仓与投资逻辑记忆" in markdown
    assert "### 4.1 最近一次投资逻辑总结" in markdown
    assert "最后一次总结依据" in markdown
    assert "核验正式财报" in markdown
    assert "不是今天应重复执行的指令" in markdown
    assert "recommended_action" in markdown
    assert "过期的 recommended_action" in markdown
