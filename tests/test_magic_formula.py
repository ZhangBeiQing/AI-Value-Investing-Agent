"""神奇公式核心口径的回归测试。"""

import numpy as np
import pandas as pd

from services.research.magic_formula import (
    balance_metrics_frame,
    ebit_ttm_history,
    rank_cross_section,
)


def _profit_frame(cumulative_profits):
    return pd.DataFrame(
        {
            "REPORT_DATE": [d for d, _ in cumulative_profits],
            "NOTICE_DATE": [d + pd.Timedelta(days=30) for d, _ in cumulative_profits],
            "TOTAL_PROFIT": [v for _, v in cumulative_profits],
            "FE_INTEREST_EXPENSE": [0.0 for _ in cumulative_profits],
        }
    )


def test_ebit_ttm_from_cumulative_quarters_sums_four_quarters():
    # Arrange: 每季 25，累计口径 Q1..Q4 = 25/50/75/100
    profit = _profit_frame(
        [
            (pd.Timestamp("2023-03-31"), 25.0),
            (pd.Timestamp("2023-06-30"), 50.0),
            (pd.Timestamp("2023-09-30"), 75.0),
            (pd.Timestamp("2023-12-31"), 100.0),
        ]
    )

    # Act
    ttm = ebit_ttm_history(profit)

    # Assert
    assert not ttm.empty
    assert ttm["EBIT_TTM_RAW"].iloc[-1] == 100.0


def test_balance_metrics_follows_greenblatt_definition():
    # Arrange: 单位元，验证后换算为亿元
    balance = pd.DataFrame(
        [
            {
                "REPORT_DATE": pd.Timestamp("2024-12-31"),
                "NOTICE_DATE": pd.Timestamp("2025-04-15"),
                "TOTAL_CURRENT_LIAB": 400e8,
                "NOTE_ACCOUNTS_RECE": 300e8,
                "INVENTORY": 200e8,
                "PREPAYMENT": 50e8,
                "OTHER_CURRENT_ASSET": 50e8,
                "FIXED_ASSET": 500e8,
                "CIP": 100e8,
                "USERIGHT_ASSET": 50e8,
                "GOODWILL": 20e8,
                "INTANGIBLE_ASSET": 30e8,
                "SHORT_LOAN": 50e8,
                "NONCURRENT_LIAB_1YEAR": 20e8,
                "LONG_LOAN": 100e8,
                "LEASE_LIAB": 30e8,
                "MONETARYFUNDS": 300e8,
            }
        ]
    )

    # Act
    metrics = balance_metrics_frame(balance)

    # Assert
    row = metrics.iloc[-1]
    assert row["net_working_capital_yi"] == 270.0  # (300+200+50+50) - (400-70)
    assert row["net_fixed_assets_yi"] == 600.0  # 500+100+50-20-30
    assert row["net_debt_yi"] == -100.0  # (50+100+20+30) - 300


def test_rank_cross_section_combines_both_ranks_and_drops_invalid():
    # Arrange: B 在两项都最好；C 收益率最高但 ROC 极差；X 因 ROC 缺失被剔除
    cross_section = pd.DataFrame(
        [
            {"symbol": "A", "name": "a", "earnings_yield": 0.10, "return_on_capital": 0.20, "market_cap_yi": 1, "ev_yi": 1},
            {"symbol": "B", "name": "b", "earnings_yield": 0.12, "return_on_capital": 0.30, "market_cap_yi": 1, "ev_yi": 1},
            {"symbol": "C", "name": "c", "earnings_yield": 0.20, "return_on_capital": -0.05, "market_cap_yi": 1, "ev_yi": 1},
            {"symbol": "X", "name": "x", "earnings_yield": 0.15, "return_on_capital": np.nan, "market_cap_yi": 1, "ev_yi": 1},
        ]
    )

    # Act
    ranked = rank_cross_section(cross_section, top_n=3)

    # Assert
    assert "X" not in ranked["symbol"].tolist()
    # B 综合排名应最靠前
    assert ranked.iloc[0]["symbol"] == "B"
