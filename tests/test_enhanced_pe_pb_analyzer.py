from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from commons import parse_symbol
from services.research.enhanced_pe_pb_analyzer import EnhancedPEPBAnalyzer


def _build_dataset(balance_sheet: pd.DataFrame) -> SimpleNamespace:
    return SimpleNamespace(financials=SimpleNamespace(balance_sheet=balance_sheet))


def _build_analyzer(analysis_datetime: datetime) -> EnhancedPEPBAnalyzer:
    return EnhancedPEPBAnalyzer(analysis_datetime=analysis_datetime)


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
