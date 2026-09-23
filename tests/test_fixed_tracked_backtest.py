from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from core.run_context import RunContext
from services.backtest.execution import simulate_post_trade
from services.backtest import announcements as backtest_announcements
from services.backtest import day_inputs as backtest_day_inputs
from services.backtest.day_inputs import write_no_trade_decision
from services.backtest.coverage import (
    ensure_experiment_decision_date,
    resolve_experiment_trading_dates,
)
from services.backtest.experiment import (
    create_backtest_experiment,
    extend_backtest_experiment,
)
from services.backtest.ledger import BacktestLedger
from services.backtest.metrics import finalize_backtest
from services.backtest.universe import build_daily_universe
from services.data_refresh.refresh_orchestrator import run_refresh_pipeline
from services.pipeline.steps import build_stock_research
from services.pipeline.steps.build_global_context import build_global_context
from services.pipeline.daily_pipeline import build_run_manifest, run_daily_pipeline
from services.research.news_summary import filter_news_before_today
from services.research.financial_report_skill import _classify_report_title
from services.research.financial_report_context import (
    _render_current_consensus,
    _select_prior_summary,
)
from scripts.manage_daily_data import refresh_shared_data
from shared_data_access.data_access import SharedDataAccess
from shared_data_access.exceptions import (
    DataUnavailableError,
    SymbolNotListedAsOfDateError,
)
from shared_data_access.historical_prices import exact_price
from shared_data_access.market_calendar import (
    NonTradingDayError,
    inspect_market_session,
    market_sessions_between,
)
from shared_data_access.paths import price_cache_dir
from commons import parse_symbol


DECISION_DATE = "2026-01-05"
EXECUTION_DATE = "2026-01-06"
SYMBOL = "600150.SH"


def _write_price_coverage_meta(target: Path) -> None:
    (target / ".cache_registry_meta.json").write_text(
        json.dumps({"requested_start_date": "20210101"}),
        encoding="utf-8",
    )


def _write_prices(source_root: Path) -> None:
    target = price_cache_dir(parse_symbol(SYMBOL), base_dir=source_root)
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "日期": DECISION_DATE,
                "开盘": 9.0,
                "最高": 10.5,
                "最低": 8.8,
                "收盘": 10.0,
                "成交量": 1000,
            },
            {
                "日期": EXECUTION_DATE,
                "开盘": 10.0,
                "最高": 11.5,
                "最低": 9.8,
                "收盘": 11.0,
                "成交量": 1200,
            },
        ]
    ).to_csv(target / "price.csv", index=False)


def _write_cn_calendar_prices(source_root: Path) -> None:
    target = price_cache_dir(parse_symbol("000001.IDX"), base_dir=source_root)
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"日期": "2026-01-02", "开盘": 1, "收盘": 1},
            {"日期": "2026-01-05", "开盘": 1, "收盘": 1},
            {"日期": "2026-01-06", "开盘": 1, "收盘": 1},
            {"日期": "2026-01-07", "开盘": 1, "收盘": 1},
            {"日期": "2026-01-08", "开盘": 1, "收盘": 1},
            {"日期": "2026-01-09", "开盘": 1, "收盘": 1},
            {"日期": "2026-01-12", "开盘": 1, "收盘": 1},
        ]
    ).to_csv(target / "price.csv", index=False)


def _decision() -> dict:
    return {
        "summary_date": DECISION_DATE,
        "system_risk_notes": [],
        "system_focus_items": [],
        "stock_decisions": [
            {
                "symbol": SYMBOL,
                "stock_name": "中国船舶",
                "scan": "测试输入",
                "delta_summary": "测试变化",
                "key_facts": ["事实"],
                "inferences": ["推论"],
                "court": {
                    "pro": ["正方"],
                    "con": ["反方"],
                    "verdict": "可以买入一手进行回测。",
                },
                "price_impression": "合理",
                "recommended_action": "按下一交易日开盘买入一手。",
                "action_type": "BUY",
                "action_num": 100,
                "key_risks": ["风险"],
                "next_day_watchlist": ["观察项"],
                "confidence_score": 0.7,
            }
        ],
    }


def test_run_context_rejects_non_experiment_root(tmp_path: Path) -> None:
    allowed = tmp_path / "backtests"
    with pytest.raises(ValueError):
        RunContext.backtest(
            tmp_path / "outside" / "case",
            source_data_root=tmp_path / "source",
            allowed_backtests_root=allowed,
        )


def test_market_calendar_uses_actual_reference_sessions(tmp_path: Path) -> None:
    _write_cn_calendar_prices(tmp_path)
    sessions = market_sessions_between(
        "2026-01-05",
        "2026-01-12",
        base_dir=tmp_path,
    )
    assert sessions.source == "reference_price_cache:000001.IDX"
    assert sessions.dates == (
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
        "2026-01-12",
    )
    saturday = inspect_market_session("2026-01-10", base_dir=tmp_path)
    assert saturday.is_trading_day is False
    assert saturday.previous_trading_day == "2026-01-09"
    assert saturday.next_trading_day == "2026-01-12"


def test_non_trading_day_is_skipped_before_daily_mutation(
    tmp_path: Path,
) -> None:
    refresh_result = run_refresh_pipeline(
        "2026-01-10",
        base_dir=str(tmp_path),
    )
    assert refresh_result.succeeded
    assert refresh_result.skipped_non_trading_date
    assert refresh_result.steps[0].name == "trading_day_guard"

    with pytest.raises(NonTradingDayError):
        run_daily_pipeline(
            "2026-01-10",
            base_dir=str(tmp_path),
        )
    assert not (tmp_path / "skill_runs" / "2026-01-10").exists()


def test_backtest_dates_skip_holidays_and_weekends(tmp_path: Path) -> None:
    source_root = tmp_path / "source_data"
    _write_cn_calendar_prices(source_root)
    experiment = create_backtest_experiment(
        start_date="2026-01-02",
        end_date="2026-01-12",
        experiment_id="trading-calendar",
        source_data_root=source_root,
        backtests_root=tmp_path / "backtests" / "fixed_tracked",
        network_mode="disabled",
    )
    assert resolve_experiment_trading_dates(experiment) == [
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
        "2026-01-12",
    ]
    with pytest.raises(ValueError, match="不是本实验的有效决策交易日"):
        write_no_trade_decision(
            experiment,
            "2026-01-10",
            reason="周末不得生成决策",
        )


def test_backtest_experiment_can_extend_end_date_without_changing_identity(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source_data"
    _write_cn_calendar_prices(source_root)
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        initial_cash=500000.0,
        experiment_id="extend-in-place",
        source_data_root=source_root,
        backtests_root=tmp_path / "backtests" / "fixed_tracked",
        network_mode="disabled",
    )
    ledger_path = (
        experiment.context.agent_data_root
        / experiment.signature
        / "position"
        / "position.jsonl"
    )
    original_ledger = ledger_path.read_text(encoding="utf-8")

    extended, coverage = extend_backtest_experiment(
        experiment,
        new_end_date="2026-01-12",
    )

    assert extended.experiment_id == experiment.experiment_id
    assert extended.root == experiment.root
    assert extended.start_date == DECISION_DATE
    assert extended.end_date == "2026-01-12"
    assert ledger_path.read_text(encoding="utf-8") == original_ledger
    assert coverage["end_date"] == "2026-01-12"
    assert extended.payload["extension_history"][-1] == {
        "extended_at": extended.payload["extension_history"][-1]["extended_at"],
        "old_end_date": EXECUTION_DATE,
        "new_end_date": "2026-01-12",
    }
    ensure_experiment_decision_date(extended, EXECUTION_DATE)


def test_default_backtest_experiment_id_does_not_embed_end_date(
    tmp_path: Path,
) -> None:
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        source_data_root=tmp_path / "source_data",
        backtests_root=tmp_path / "backtests" / "fixed_tracked",
        network_mode="disabled",
    )

    assert experiment.experiment_id.startswith("fixed_20260105_")
    assert "20260106" not in experiment.experiment_id


def test_isolated_next_open_execution_and_idempotency(tmp_path: Path) -> None:
    source_root = tmp_path / "source_data"
    backtests_root = tmp_path / "backtests" / "fixed_tracked"
    live_sentinel = source_root / "agent_data" / "live-sentinel.txt"
    live_sentinel.parent.mkdir(parents=True, exist_ok=True)
    live_sentinel.write_text("unchanged", encoding="utf-8")
    _write_prices(source_root)

    selection = source_root / "selection_runs" / DECISION_DATE
    selection.mkdir(parents=True, exist_ok=True)
    (selection / "12_quant_prefilter_long.csv").write_text(
        "symbol,score\n601179.SH,1\n",
        encoding="utf-8",
    )

    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        initial_cash=500000.0,
        experiment_id="isolation-test",
        source_data_root=source_root,
        backtests_root=backtests_root,
        network_mode="disabled",
    )
    ledger = BacktestLedger(
        agent_data_root=experiment.context.agent_data_root,
        signature=experiment.signature,
        source_data_root=source_root,
    )
    universe, metadata = build_daily_universe(experiment, DECISION_DATE, ledger)
    assert "601179.SH" in universe
    assert metadata["note"].startswith("12_quant_prefilter_long.csv 只扩展研究范围")

    book_dir = (
        experiment.context.skill_runs_root / DECISION_DATE / "fixed_tracked"
    )
    book_dir.mkdir(parents=True, exist_ok=True)
    (book_dir / "05_decision.json").write_text(
        json.dumps(_decision(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="必须是下一交易日"):
        simulate_post_trade(
            experiment,
            decision_date=DECISION_DATE,
            execution_date="2026-01-07",
        )

    paths = simulate_post_trade(
        experiment,
        decision_date=DECISION_DATE,
        execution_date=EXECUTION_DATE,
    )
    assert all(path.exists() for path in paths)
    state = ledger.latest()
    assert state.positions[SYMBOL] == 100
    assert state.positions["CASH"] == 499000.0
    _, _, return_pcts = backtest_day_inputs._portfolio_position_summary(
        experiment,
        EXECUTION_DATE,
        state.positions,
    )
    assert return_pcts["中国船舶_600150.SH"] == "10.00%"
    assert live_sentinel.read_text(encoding="utf-8") == "unchanged"
    assert not (source_root / "skill_runs" / DECISION_DATE).exists()
    snapshot_path = (
        experiment.context.agent_data_root
        / experiment.signature
        / "latest_decision_snapshot.json"
    )
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["stock_count"] == 1
    assert snapshot["stocks"][0]["stock_code"] == SYMBOL
    assert snapshot["stocks"][0]["inferences"] == ["推论"]

    record_count = len(ledger.records())
    second_paths = simulate_post_trade(
        experiment,
        decision_date=DECISION_DATE,
        execution_date=EXECUTION_DATE,
    )
    assert second_paths == paths
    assert len(ledger.records()) == record_count

    summary_path, curve_path = finalize_backtest(experiment)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert curve_path.exists()
    assert summary["final_value"] == 500100.0
    assert summary["trade_count"] == 1


def test_backtest_average_costs_replay_all_held_positions_without_orders_file(
    tmp_path: Path,
) -> None:
    ledger = BacktestLedger(
        agent_data_root=tmp_path / "agent_data",
        signature="cost-replay",
        source_data_root=tmp_path / "source_data",
    )
    ledger.initialize(
        start_date="2026-01-05",
        initial_cash=100000.0,
        symbols=["600150.SH", "002415.SZ"],
    )
    ledger.append_execution(
        decision_date="2026-01-05",
        execution_date="2026-01-06",
        positions={"CASH": 93000.0, "600150.SH": 100, "002415.SZ": 200},
        actions=[
            {"action": "buy", "symbol": "600150.SH", "shares": 100, "price": 30.0},
            {"action": "buy", "symbol": "002415.SZ", "shares": 200, "price": 20.0},
        ],
        order_ids=["ship-buy", "hik-buy"],
    )
    ledger.append_execution(
        decision_date="2026-01-06",
        execution_date="2026-01-07",
        positions={"CASH": 89500.0, "600150.SH": 200, "002415.SZ": 200},
        actions=[
            {"action": "buy", "symbol": "600150.SH", "shares": 100, "price": 35.0},
        ],
        order_ids=["ship-add"],
    )

    assert not ledger.orders_file.exists()
    assert ledger.average_costs(on_or_before="2026-01-07") == {
        "600150.SH": 32.5,
        "002415.SZ": 20.0,
    }


def test_backtest_average_costs_keep_partial_sell_and_reset_after_full_exit(
    tmp_path: Path,
) -> None:
    ledger = BacktestLedger(
        agent_data_root=tmp_path / "agent_data",
        signature="cost-reset",
        source_data_root=tmp_path / "source_data",
    )
    ledger.initialize(
        start_date="2026-01-05",
        initial_cash=100000.0,
        symbols=[SYMBOL],
    )
    ledger.append_execution(
        decision_date="2026-01-05",
        execution_date="2026-01-06",
        positions={"CASH": 96000.0, SYMBOL: 200},
        actions=[{"action": "buy", "symbol": SYMBOL, "shares": 200, "price": 20.0}],
        order_ids=["buy-1"],
    )
    ledger.append_execution(
        decision_date="2026-01-06",
        execution_date="2026-01-07",
        positions={"CASH": 98000.0, SYMBOL: 100},
        actions=[{"action": "sell", "symbol": SYMBOL, "shares": 100, "price": 20.0}],
        order_ids=["partial-sell"],
    )
    assert ledger.average_costs(on_or_before="2026-01-07") == {SYMBOL: 20.0}

    ledger.append_execution(
        decision_date="2026-01-07",
        execution_date="2026-01-08",
        positions={"CASH": 100000.0},
        actions=[{"action": "sell", "symbol": SYMBOL, "shares": 100, "price": 20.0}],
        order_ids=["full-sell"],
    )
    assert ledger.average_costs(on_or_before="2026-01-08") == {}

    ledger.append_execution(
        decision_date="2026-01-08",
        execution_date="2026-01-09",
        positions={"CASH": 97000.0, SYMBOL: 100},
        actions=[{"action": "buy", "symbol": SYMBOL, "shares": 100, "price": 30.0}],
        order_ids=["buy-2"],
    )
    assert ledger.average_costs(on_or_before="2026-01-09") == {SYMBOL: 30.0}


def test_backtest_position_summary_rejects_held_symbol_without_cost_history(
    tmp_path: Path,
) -> None:
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        experiment_id="missing-cost-history",
        source_data_root=tmp_path / "source_data",
        backtests_root=tmp_path / "backtests" / "fixed_tracked",
        network_mode="disabled",
    )

    with pytest.raises(RuntimeError, match="拒绝生成不完整的 03_agent_input.md"):
        backtest_day_inputs._portfolio_position_summary(
            experiment,
            DECISION_DATE,
            {"CASH": 97000.0, SYMBOL: 100},
        )


def test_exact_open_price_never_uses_previous_session(tmp_path: Path) -> None:
    _write_prices(tmp_path)
    assert (
        exact_price(
            parse_symbol(SYMBOL),
            "2026-01-07",
            "开盘",
            base_dir=tmp_path,
        )
        is None
    )


def test_backtest_research_rebuilds_historical_price_and_valuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        build_stock_research,
        "search_stock_news",
        lambda *_args, **_kwargs: json.dumps({"news_items": []}),
    )
    monkeypatch.setattr(
        build_stock_research,
        "get_financial_report_summary",
        lambda *_args, **_kwargs: {"content": "历史财报"},
    )
    calls: list[tuple[str, str]] = []

    def fake_analysis(symbol: str, run_date: str) -> dict:
        calls.append((symbol, run_date))
        return {
            "price_report": {
                "meta": {
                    "symbol": symbol,
                    "analysis_date": run_date,
                },
                "technical_section": {"latest_focus": {"收盘价": 10.0}},
            },
            "valuation_report": (
                "# 历史日期重建估值\n\n"
                f"- 分析日期: {run_date}\n"
                "- 扣非PE(TTM): 10.0"
            ),
        }

    monkeypatch.setattr(
        build_stock_research,
        "analyze_stock_dynamics_and_valuation",
        fake_analysis,
    )
    markdown = build_stock_research.build_research_markdown(
        SYMBOL,
        DECISION_DATE,
        snapshot_payload={"stocks": {SYMBOL: {"latest_price": 10.0}}},
        research_cache_root=tmp_path / "experiment" / "research_cache",
        backtest_read_only=True,
    )
    assert calls == [(SYMBOL, DECISION_DATE)]
    assert '"analysis_date": "2026-01-05"' in markdown
    assert "# 历史日期重建估值" in markdown
    assert "扣非PE(TTM): 10.0" in markdown


def test_backtest_announcements_prepare_each_symbol_once_per_experiment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        experiment_id="announcement-once",
        source_data_root=tmp_path / "source_data",
        backtests_root=tmp_path / "backtests" / "fixed_tracked",
        network_mode="guarded_web",
    )
    calls: list[str] = []

    def fake_prepare_one(_experiment, symbol: str, *, lookback_days: int) -> dict:
        calls.append(symbol)
        return {
            "status": "success",
            "prepared_at": "2026-08-04T12:00:00",
            "lookback_days": lookback_days,
            "added_summary_count": 0,
            "raw_news_item_count": 0,
            "audited_news_item_count": 0,
        }

    monkeypatch.setattr(
        backtest_announcements,
        "_prepare_one_symbol",
        fake_prepare_one,
    )
    first = backtest_announcements.prepare_backtest_announcements(
        experiment,
        [SYMBOL],
        max_workers=1,
    )
    second = backtest_announcements.prepare_backtest_announcements(
        experiment,
        [SYMBOL],
        max_workers=1,
    )

    assert calls == [SYMBOL]
    assert first["prepared_symbol_count"] == 1
    assert second["prepared_symbol_count"] == 0
    assert second["cached_symbol_count"] == 1
    assert Path(second["checkpoint"]).exists()


def test_backtest_prepare_day_stops_before_04_when_financial_research_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "source_data"
    _write_cn_calendar_prices(source_root)
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        experiment_id="financial-gate",
        source_data_root=source_root,
        backtests_root=tmp_path / "backtests" / "fixed_tracked",
        network_mode="disabled",
    )
    monkeypatch.setattr(
        backtest_day_inputs,
        "build_daily_universe",
        lambda *_args, **_kwargs: ([SYMBOL], {"symbols": [SYMBOL]}),
    )
    monkeypatch.setattr(
        backtest_day_inputs,
        "prepare_backtest_announcements",
        lambda *_args, **_kwargs: {"status": "skipped"},
    )
    monkeypatch.setattr(
        backtest_day_inputs,
        "prepare_backtest_financial_disclosures",
        lambda *_args, **_kwargs: {"status": "skipped"},
    )
    monkeypatch.setattr(
        backtest_day_inputs,
        "inspect_backtest_financial_research",
        lambda *_args, **_kwargs: {
            "status": "needs_financial_research",
            "required_items": [{"symbol": SYMBOL}],
        },
    )
    pipeline_called = False

    def fail_if_pipeline_runs(*_args, **_kwargs) -> None:
        nonlocal pipeline_called
        pipeline_called = True

    monkeypatch.setattr(
        backtest_day_inputs,
        "run_book_pipeline",
        fail_if_pipeline_runs,
    )

    result = backtest_day_inputs.prepare_backtest_day(
        experiment,
        DECISION_DATE,
        build_missing_inputs=True,
        max_workers=1,
    )

    assert result["status"] == "needs_financial_research"
    assert pipeline_called is False
    assert not (
        experiment.context.skill_runs_root
        / DECISION_DATE
        / "fixed_tracked"
        / "04_stock_research"
    ).exists()


def test_financial_report_classifier_prefers_q3_and_excludes_wrappers() -> None:
    assert _classify_report_title(
        "300476.SZ",
        "2025年三季度报告",
    )[:2] == ("q3", 3)
    assert _classify_report_title(
        "300476.SZ",
        "2025年半年度报告",
    )[:2] == ("interim", 2)
    assert _classify_report_title(
        "300476.SZ",
        "向特定对象发行股票募集说明书（2025年半年报更新稿）",
    )[3] == 99
    assert _classify_report_title(
        "300476.SZ",
        "关于2025年半年度报告披露提示性公告",
    )[3] == 99


def test_historical_financial_context_rejects_future_memory_and_current_forecast(
    tmp_path: Path,
) -> None:
    old_report = tmp_path / "old.md"
    future_report = tmp_path / "future.md"
    old_report.write_text("# 旧报告", encoding="utf-8")
    future_report.write_text("# 未来报告", encoding="utf-8")
    selected = _select_prior_summary(
        {
            "history": [
                {
                    "announcement_id": "future",
                    "report_date": "2026-03-13",
                    "output_path": str(future_report),
                },
                {
                    "announcement_id": "old",
                    "report_date": "2025-08-27",
                    "output_path": str(old_report),
                },
            ]
        },
        current_announcement_id="current",
        current_report_date="2025-10-28",
    )
    assert selected == old_report

    forecast_dir = tmp_path / "profit_forecast"
    forecast_dir.mkdir()
    (forecast_dir / "profit_forecast.csv").write_text(
        "预测指标,预测2026-平均\n净利润(元),100\n",
        encoding="utf-8",
    )
    markdown, source = _render_current_consensus(
        tmp_path,
        "2026-01-05",
        historical_mode=True,
    )
    assert source is None
    assert "禁止回退到当前缓存" in markdown


def test_news_filter_includes_decision_day_and_excludes_future() -> None:
    diagnostics: list[str] = []
    filtered = filter_news_before_today(
        [
            {"title": "前一日公告", "datetime": "2026-01-04 18:00:00"},
            {"title": "当日公告", "datetime": "2026-01-05 20:00:00"},
            {"title": "未来公告", "datetime": "2026-01-06 08:00:00"},
        ],
        datetime.strptime(DECISION_DATE, "%Y-%m-%d"),
        diagnostics,
    )

    assert [item["title"] for item in filtered] == ["当日公告", "前一日公告"]


def test_backtest_global_context_uses_read_only_index_cache(tmp_path: Path) -> None:
    index_dir = price_cache_dir(parse_symbol("000001.IDX"), base_dir=tmp_path)
    index_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"日期": "2026-01-02", "开盘": 9, "最高": 10, "最低": 8, "收盘": 9},
            {"日期": DECISION_DATE, "开盘": 10, "最高": 11, "最低": 9, "收盘": 10},
            {"日期": "2026-01-07", "开盘": 99, "最高": 100, "最低": 98, "收盘": 99},
        ]
    ).to_csv(index_dir / "price.csv", index=False)
    text = build_global_context(
        DECISION_DATE,
        backtest_read_only=True,
        source_data_root=tmp_path,
    )
    assert '"close": 10.0' in text
    assert "99" not in text


def test_empty_p0_day_is_valid_only_in_backtest_path(tmp_path: Path) -> None:
    source_root = tmp_path / "source_data"
    backtests_root = tmp_path / "backtests" / "fixed_tracked"
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        experiment_id="empty-p0",
        source_data_root=source_root,
        backtests_root=backtests_root,
        network_mode="disabled",
    )
    decision_path = write_no_trade_decision(
        experiment,
        DECISION_DATE,
        reason="P0 为空",
    )
    assert json.loads(decision_path.read_text(encoding="utf-8"))[
        "stock_decisions"
    ] == []
    simulate_post_trade(
        experiment,
        decision_date=DECISION_DATE,
        execution_date=EXECUTION_DATE,
    )
    ledger = BacktestLedger(
        agent_data_root=experiment.context.agent_data_root,
        signature=experiment.signature,
        source_data_root=source_root,
    )
    assert ledger.latest().positions["CASH"] == 500000.0


def test_missing_long_prefilter_is_built_from_shared_factor_snapshot(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source_data"
    factor_dir = source_root / "factor_store" / "by_date"
    factor_dir.mkdir(parents=True, exist_ok=True)
    factor_path = factor_dir / f"{DECISION_DATE}.csv"
    pd.DataFrame(
        [
            {
                "symbol": "601179.SH",
                "stock_name": "中国西电",
                "long_score": 0.9,
                "short_score": 0.1,
                "latest_volume": 1.0,
                "liquidity_score": 0.8,
                "basic_info_asof_date": DECISION_DATE,
            }
        ]
    ).to_csv(factor_path, index=False)
    original = factor_path.read_bytes()
    backtests_root = tmp_path / "backtests" / "fixed_tracked"
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        experiment_id="quant-build",
        source_data_root=source_root,
        backtests_root=backtests_root,
        network_mode="disabled",
    )
    ledger = BacktestLedger(
        agent_data_root=experiment.context.agent_data_root,
        signature=experiment.signature,
        source_data_root=source_root,
    )
    universe, _ = build_daily_universe(experiment, DECISION_DATE, ledger)
    assert "601179.SH" in universe
    assert (
        experiment.context.selection_runs_root
        / DECISION_DATE
        / "12_quant_prefilter_long.csv"
    ).exists()
    assert factor_path.read_bytes() == original
    assert not (source_root / "selection_runs" / DECISION_DATE).exists()


def test_shared_data_access_classifies_pre_listing_date(tmp_path: Path) -> None:
    target = price_cache_dir(parse_symbol("00100.HK"), base_dir=tmp_path)
    target.mkdir(parents=True, exist_ok=True)
    _write_price_coverage_meta(target)
    pd.DataFrame(
        [{"日期": "2026-01-09", "开盘": 235.4, "收盘": 345.0}]
    ).to_csv(target / "price.csv", index=False)
    access = SharedDataAccess(
        logger=logging.getLogger("test-pre-listing"),
        base_dir=tmp_path,
    )
    with pytest.raises(SymbolNotListedAsOfDateError) as captured:
        access._load_price_bundle(  # type: ignore[attr-defined]
            parse_symbol("00100.HK"),
            datetime(2026, 1, 5),
            allow_stale_cache=True,
        )
    assert captured.value.first_trading_date == "2026-01-09"
    assert captured.value.as_of_date == DECISION_DATE


def test_historical_manifest_excludes_known_not_listed_symbols(
    tmp_path: Path,
) -> None:
    for symbol, first_date in (
        ("00100.HK", "2026-01-09"),
        ("02513.HK", "2026-01-08"),
    ):
        target = price_cache_dir(parse_symbol(symbol), base_dir=tmp_path)
        target.mkdir(parents=True, exist_ok=True)
        _write_price_coverage_meta(target)
        pd.DataFrame(
            [{"日期": first_date, "开盘": 100, "收盘": 100}]
        ).to_csv(target / "price.csv", index=False)
    manifest = build_run_manifest(DECISION_DATE, base_dir=str(tmp_path))
    fixed = next(
        book
        for book in manifest["books"]
        if book["book_type"] == "fixed_tracked"
    )
    assert "00100.HK" not in fixed["symbols"]
    assert "02513.HK" not in fixed["symbols"]
    excluded = fixed["symbol_sources"]["not_listed_as_of_date"]
    assert {item["symbol"] for item in excluded} == {
        "00100.HK",
        "02513.HK",
    }


def test_price_history_without_coverage_meta_is_not_mislabeled(
    tmp_path: Path,
) -> None:
    target = price_cache_dir(parse_symbol("00100.HK"), base_dir=tmp_path)
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"日期": "2026-01-09", "开盘": 235.4, "收盘": 345.0}]
    ).to_csv(target / "price.csv", index=False)
    access = SharedDataAccess(
        logger=logging.getLogger("test-unknown-coverage"),
        base_dir=tmp_path,
    )
    with pytest.raises(DataUnavailableError) as captured:
        access._load_price_bundle(  # type: ignore[attr-defined]
            parse_symbol("00100.HK"),
            datetime(2026, 1, 5),
            allow_stale_cache=True,
        )
    assert not isinstance(captured.value, SymbolNotListedAsOfDateError)


def test_backtest_universe_excludes_pre_listing_symbols(tmp_path: Path) -> None:
    source_root = tmp_path / "source_data"
    target = price_cache_dir(parse_symbol("00100.HK"), base_dir=source_root)
    target.mkdir(parents=True, exist_ok=True)
    _write_price_coverage_meta(target)
    pd.DataFrame(
        [{"日期": "2026-01-09", "开盘": 235.4, "收盘": 345.0}]
    ).to_csv(target / "price.csv", index=False)
    experiment = create_backtest_experiment(
        start_date=DECISION_DATE,
        end_date=EXECUTION_DATE,
        experiment_id="pre-listing-filter",
        source_data_root=source_root,
        backtests_root=tmp_path / "backtests" / "fixed_tracked",
        network_mode="disabled",
    )
    ledger = BacktestLedger(
        agent_data_root=experiment.context.agent_data_root,
        signature=experiment.signature,
        source_data_root=source_root,
    )
    universe, metadata = build_daily_universe(
        experiment,
        DECISION_DATE,
        ledger,
    )
    assert "00100.HK" not in universe
    assert metadata["not_listed_as_of_date"] == [
        {"symbol": "00100.HK", "first_trading_date": "2026-01-09"}
    ]


def test_daily_refresh_treats_pre_listing_as_skip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        SharedDataAccess,
        "build_macro_objective_panel",
        lambda *_args, **_kwargs: {"indicators": {}, "central_banks": {}},
    )

    def fake_prepare(_self, *, symbolInfo, as_of_date, **_kwargs):
        if symbolInfo.symbol == "00100.HK":
            raise SymbolNotListedAsOfDateError(
                symbol=symbolInfo.symbol,
                as_of_date=as_of_date,
                first_trading_date="2026-01-09",
            )
        return object()

    monkeypatch.setattr(SharedDataAccess, "prepare_dataset", fake_prepare)
    result = refresh_shared_data(
        DECISION_DATE,
        ["600150.SH", "00100.HK"],
        force_refresh_prices=False,
        force_refresh_financials=False,
        log_file=tmp_path / "refresh.log",
        max_workers=2,
    )
    assert result["status"] == "success"
    assert result["skipped_not_listed_symbols"] == ["00100.HK"]
