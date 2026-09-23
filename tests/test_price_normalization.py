from __future__ import annotations

import pandas as pd

from shared_data_access.price_fetch import (
    CANONICAL_PRICE_COLUMNS,
    _finalize_price_frame,
)


def test_eastmoney_frame_is_converted_to_canonical_units() -> None:
    frame = pd.DataFrame(
        [
            {
                "日期": "2026-09-21",
                "股票代码": 601989,
                "开盘": 5.01,
                "收盘": 5.10,
                "最高": 5.15,
                "最低": 4.90,
                "成交量": 5_832_456,  # 手
                "成交额": 2_941_450_109.0,
                "振幅": 4.99,
                "涨跌幅": 1.8,
                "涨跌额": 0.09,
                "换手率": 2.56,  # 百分数
            }
        ]
    )

    result = _finalize_price_frame(
        frame, volume_in_lots=True, turnover_in_percent=True
    )

    assert list(result.columns) == list(CANONICAL_PRICE_COLUMNS)
    assert result.loc[0, "成交量"] == 583_245_600
    assert result.loc[0, "换手率"] == 0.0256
    assert result.loc[0, "流通股本"] != result.loc[0, "流通股本"]  # NaN


def test_sina_frame_keeps_units_and_drops_extra_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "date": "2026-09-21",
                "open": 51.7,
                "high": 51.75,
                "low": 50.57,
                "close": 50.93,
                "volume": 5_940_820,
                "amount": 303_672_875.0,
                "turnover": 0.0522,
                "outstanding_share": 113_830_324.0,
                "extra": "drop-me",
            }
        ]
    )

    result = _finalize_price_frame(frame)

    assert list(result.columns) == list(CANONICAL_PRICE_COLUMNS)
    assert result.loc[0, "成交量"] == 5_940_820
    assert result.loc[0, "换手率"] == 0.0522
    assert result.loc[0, "流通股本"] == 113_830_324.0


def test_missing_columns_are_filled_and_sorted() -> None:
    frame = pd.DataFrame(
        [
            {"date": "2026-09-21", "close": 2.0},
            {"date": "2026-09-18", "close": 1.0},
        ]
    )

    result = _finalize_price_frame(frame)

    assert list(result.columns) == list(CANONICAL_PRICE_COLUMNS)
    assert result["日期"].dt.strftime("%Y-%m-%d").tolist() == [
        "2026-09-18",
        "2026-09-21",
    ]
    assert result["换手率"].isna().all()
    assert result["成交额"].isna().all()


def test_empty_frame_returns_canonical_columns() -> None:
    result = _finalize_price_frame(pd.DataFrame())
    assert list(result.columns) == list(CANONICAL_PRICE_COLUMNS)
    assert result.empty
