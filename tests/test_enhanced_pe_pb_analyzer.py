from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from commons import parse_symbol
from services.research.enhanced_pe_pb_analyzer import EnhancedPEPBAnalyzer


def _build_dataset(balance_sheet: pd.DataFrame) -> SimpleNamespace:
    return SimpleNamespace(financials=SimpleNamespace(balance_sheet=balance_sheet))


def _build_analyzer(analysis_datetime: datetime) -> EnhancedPEPBAnalyzer:
    return EnhancedPEPBAnalyzer(analysis_datetime=analysis_datetime)


def test_compute_ps_preserves_fiscal_report_type_for_irregular_us_quarters():
    analyzer = _build_analyzer(datetime(2026, 7, 1))
    frame = pd.DataFrame({
        "REPORT_DATE": ["2025-04-27", "2025-07-27", "2025-10-26", "2026-01-25"],
        "REPORT_TYPE": ["单季报", "累计季报", "累计季报", "年报"],
        "TOTAL_OPERATE_INCOME": [10.0, 30.0, 60.0, 100.0],
    })
    assert analyzer._compute_ps(frame, 1000.0) == 10.0


def test_compute_pb_prefers_parent_equity_when_minority_present() -> None:
    # Arrange: 少数股东权益显著（模拟中国平安口径），归母权益远小于股东权益合计
    market_cap = 956_626_726_595.85
    total_parent_equity = 1_028_084_000_000.0
    total_equity = 1_454_080_000_000.0
    balance_sheet = pd.DataFrame(
        [
            {
                "REPORT_DATE": "2026-06-30",
                "NOTICE_DATE": "2026-08-21",
                "TOTAL_EQUITY": total_equity,
                "TOTAL_PARENT_EQUITY": total_parent_equity,
            }
        ]
    )
    analyzer = _build_analyzer(datetime(2026, 10, 9))
    symbol = parse_symbol("601318.SH")

    # Act
    pb = analyzer._compute_pb(_build_dataset(balance_sheet), market_cap, symbol)

    # Assert: 必须用归母权益，不能用含少数股东的股东权益合计
    assert pb is not None
    assert abs(pb - market_cap / total_parent_equity) < 1e-9
    assert abs(pb - 0.9305) < 1e-3


def test_compute_pb_falls_back_to_total_equity_when_parent_missing() -> None:
    # Arrange: 缺少归母权益列时，回退到股东权益合计
    market_cap = 1_000.0
    total_equity = 2_000.0
    balance_sheet = pd.DataFrame(
        [
            {
                "REPORT_DATE": "2026-06-30",
                "NOTICE_DATE": "2026-08-21",
                "TOTAL_EQUITY": total_equity,
            }
        ]
    )
    analyzer = _build_analyzer(datetime(2026, 10, 9))
    symbol = parse_symbol("601318.SH")

    # Act
    pb = analyzer._compute_pb(_build_dataset(balance_sheet), market_cap, symbol)

    # Assert
    assert pb is not None
    assert abs(pb - market_cap / total_equity) < 1e-9


def test_compute_pb_uses_latest_row_before_analysis_datetime() -> None:
    # Arrange: 分析日之前有多期报表，应取最近一期
    market_cap = 1_000.0
    balance_sheet = pd.DataFrame(
        [
            {
                "REPORT_DATE": "2026-03-31",
                "NOTICE_DATE": "2026-04-29",
                "TOTAL_EQUITY": 5_000.0,
                "TOTAL_PARENT_EQUITY": 4_000.0,
            },
            {
                "REPORT_DATE": "2026-06-30",
                "NOTICE_DATE": "2026-08-21",
                "TOTAL_EQUITY": 2_000.0,
                "TOTAL_PARENT_EQUITY": 1_000.0,
            },
        ]
    )
    analyzer = _build_analyzer(datetime(2026, 10, 9))
    symbol = parse_symbol("601318.SH")

    # Act
    pb = analyzer._compute_pb(_build_dataset(balance_sheet), market_cap, symbol)

    # Assert: 取 2026-06-30，归母 1,000
    assert pb is not None
    assert abs(pb - 1.0) < 1e-9


def test_history_and_extremes_only_use_disclosed_profit_and_equity() -> None:
    analyzer = _build_analyzer(datetime(2026, 11, 9))
    symbol = parse_symbol('002594.SZ')
    profit = pd.DataFrame([
        {'REPORT_DATE': '2026-06-30', 'NOTICE_DATE': '2026-08-28'},
        {'REPORT_DATE': '2026-09-30', 'NOTICE_DATE': '2026-10-30'},
    ])
    ttm = pd.DataFrame([
        {'REPORT_DATE': '2026-06-30', 'TTM_NET_PROFIT_RAW': 100.0},
        {'REPORT_DATE': '2026-09-30', 'TTM_NET_PROFIT_RAW': 200.0},
        {'REPORT_DATE': '2026-12-31', 'TTM_NET_PROFIT_RAW': 999.0},
    ])
    events = analyzer._build_profit_events(symbol=symbol, profit_sheet=profit,
        deduct_ttm_df=ttm, fallback_ttm_df=pd.DataFrame())
    prices = pd.Series([20., 10., 12., 14.], index=pd.to_datetime([
        '2026-08-31', '2026-10-20', '2026-10-30', '2026-11-02']))
    balance = profit.assign(TOTAL_PARENT_EQUITY=[500., 1000.])
    equity = analyzer._build_equity_value_map(_build_dataset(balance), symbol)
    rows = analyzer._build_historical_pe_records(events, prices, 100., equity)
    assert [r['估值日期'] for r in rows] == ['2026-08-31', '2026-11-02']
    assert [r['PE'] for r in rows] == [20., 7.]
    assert [r['PB'] for r in rows] == [4., 1.4]
    extreme = analyzer._build_extreme_price_metrics(price_series=prices,
        deduct_ttm_df=events, fallback_ttm_df=pd.DataFrame(), total_shares=100.)
    low = next(r for r in extreme if '最低价' in r['类别'])
    assert low['参考财报日期'] == '2026-06-30'
    assert low['扣非PE'] == 10.
    # Unknown announcement dates cannot be replaced by fiscal period ends.
    assert len(events) == 2
    assert analyzer._get_price_at(prices, datetime(2026, 1, 1)) is None
    daily = analyzer._build_daily_valuation_frame(price_series=prices,
        total_shares=100., symbol=symbol, profit_sheet=profit,
        deduct_ttm_df=ttm, fallback_ttm_df=pd.DataFrame(), balance_sheet=balance)
    assert daily['PE'].tolist() == [20., 10., 12., 7.]


def test_historical_peg_uses_first_trading_price_after_disclosure() -> None:
    analyzer = _build_analyzer(datetime(2026, 11, 9))
    periods = pd.to_datetime(['2025-09-30','2025-12-31','2026-03-31','2026-06-30','2026-09-30'])
    events = pd.DataFrame({'REPORT_DATE': periods,
        'effective_date': pd.to_datetime(['2025-11-01','2026-04-01','2026-05-01','2026-09-01','2026-10-31']),
        'TTM_NET_PROFIT_RAW': [100.,110.,120.,130.,200.]})
    prices = pd.Series([20.,14.], index=pd.to_datetime(['2026-09-30','2026-11-02']))
    rows = analyzer._build_historical_peg_records(events, prices, 100.)
    assert len(rows) == 1
    assert rows[0]['PE'] == 7.
    assert rows[0]['PEG'] == .07
