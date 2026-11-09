from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from commons import is_etf_symbol, parse_symbol
from shared_data_access.us_disclosures import (
    build_us_filing_news_items,
    filing_notice_date_map,
    normalize_filings,
)
from shared_data_access.us_financials import (
    build_quarterly_wide_frame,
    compute_implied_fx,
    normalize_us_financials,
)
from shared_data_access.us_market import latest_completed_us_session


def _long_frame(report_date: str, items: dict[str, float], indicator: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "REPORT_DATE": report_date,
                "REPORT_TYPE": indicator,
                "STD_ITEM_CODE": code,
                "ITEM_NAME": name,
                "AMOUNT": amount,
                "CURRENCY": "TWD",
            }
            for code, (name, amount) in enumerate(items.items())
        ]
    )


def test_parse_us_symbol_resolves_metadata() -> None:
    info = parse_symbol("TSM.US")

    assert info.symbol == "TSM.US"
    assert info.code == "TSM"
    assert info.market == "US"
    assert info.stock_name == "台积电"
    assert info.to_us_symbol() == "TSM"


def test_us_benchmark_is_treated_as_etf() -> None:
    info = parse_symbol("SPY.US")

    assert info.stock_name == "标普500ETF"
    assert is_etf_symbol(info)


def test_quarterly_wide_frame_picks_correct_indicator_per_month() -> None:
    frames = {
        "单季报": _long_frame("2026-03-31", {"主营收入": 100.0}, "单季报"),
        "累计季报": _long_frame("2026-06-30", {"主营收入": 220.0}, "累计季报"),
        "年报": _long_frame("2025-12-31", {"主营收入": 400.0}, "年报"),
    }

    wide = build_quarterly_wide_frame(frames, {"主营收入": "TOTAL_OPERATE_INCOME"})

    assert wide["REPORT_DATE"].dt.strftime("%Y-%m-%d").tolist() == [
        "2025-12-31",
        "2026-03-31",
        "2026-06-30",
    ]
    assert wide.set_index("REPORT_DATE").loc["2026-06-30", "TOTAL_OPERATE_INCOME"] == 220.0
    assert wide.set_index("REPORT_DATE").loc["2026-06-30", "REPORT_TYPE"] == "累计季报"


def test_compute_implied_fx_uses_usd_or_implied_rate() -> None:
    assert compute_implied_fx(currency="USD", quote=None, ttm_profit_local=None) == (
        1.0,
        "currency_is_usd",
    )

    fx, source = compute_implied_fx(
        currency="TWD",
        quote={"market_cap": 1000.0, "pe_ttm": 20.0},
        ttm_profit_local=400.0,
    )

    assert source == "implied_from_eastmoney_pe"
    assert fx == 8.0


def test_compute_implied_fx_refuses_to_guess_without_pe() -> None:
    fx, source = compute_implied_fx(
        currency="TWD",
        quote={"market_cap": 1000.0, "pe_ttm": None},
        ttm_profit_local=400.0,
    )

    assert fx is None
    assert source == "missing_pe_for_implied_fx"


def test_normalize_us_financials_converts_currency_and_uses_filing_date() -> None:
    frames = {
        "综合损益表": {
            "单季报": pd.concat(
                [
                    _long_frame("2025-03-31", {"主营收入": 100.0, "归属于母公司股东净利润": 10.0}, "单季报"),
                    _long_frame("2026-03-31", {"主营收入": 100.0, "归属于母公司股东净利润": 10.0}, "单季报"),
                ],
                ignore_index=True,
            ),
            "累计季报": pd.concat(
                [
                    _long_frame("2025-06-30", {"主营收入": 210.0, "归属于母公司股东净利润": 20.0}, "累计季报"),
                    _long_frame("2025-09-30", {"主营收入": 330.0, "归属于母公司股东净利润": 30.0}, "累计季报"),
                ],
                ignore_index=True,
            ),
            "年报": _long_frame("2025-12-31", {"主营收入": 460.0, "归属于母公司股东净利润": 40.0}, "年报"),
        },
        "资产负债表": {
            "单季报": _long_frame(
                "2026-03-31",
                {"总资产": 500.0, "归属于母公司股东权益": 200.0},
                "单季报",
            )
        },
        "现金流量表": {
            "单季报": _long_frame(
                "2026-03-31",
                {"经营活动产生的现金流量净额": 30.0},
                "单季报",
            )
        },
    }
    indicator = pd.DataFrame(
        [
            {
                "REPORT_DATE": "2026-03-31",
                "NOTICE_DATE": "2026-05-01",
                "CURRENCY_ABBR": "TWD",
                "PARENT_HOLDER_NETPROFIT": 40.0,
                "GROSS_PROFIT_RATIO": 50.0,
                "NET_PROFIT_RATIO": 20.0,
                "ROE_AVG": 10.0,
                "DEBT_ASSET_RATIO": 30.0,
                "CURRENT_RATIO": 2.0,
                "SPEED_RATIO": 1.5,
                "OPERATE_INCOME_YOY": 20.0,
                "PARENT_HOLDER_NETPROFIT_YOY": 25.0,
            }
        ]
    )
    quote = {
        "market_cap": 800.0,
        "pe_ttm": 20.0,
        "price": 40.0,
        "total_shares": 20.0,
    }

    normalized = normalize_us_financials(
        frames=frames,
        indicator_frame=indicator,
        quote=quote,
        filing_notice_map={pd.Timestamp("2026-03-31"): pd.Timestamp("2026-04-15")},
    )

    profit = normalized["profit_sheet"]
    latest = profit[profit["REPORT_DATE"] == "2026-03-31"].iloc[0]
    assert latest["CURRENCY"] == "TWD"
    assert latest["NOTICE_DATE"] == "2026-04-15"
    # TTM 本币利润 = 10+10+10+10 = 40；隐含汇率 = 40 / (800/20) = 1 -> 金额保持原值
    assert normalized["meta"]["fx_local_per_usd"] == 1.0
    assert latest["TOTAL_OPERATE_INCOME"] == 100.0
    assert latest["DEDUCT_PARENT_NETPROFIT"] == latest["PARENT_NETPROFIT"]

    abstract = normalized["financial_abstract"]
    assert "营业总收入" in abstract["指标"].tolist()
    assert "每股净资产" in abstract["指标"].tolist()


def _sec_payload(rows: list[dict[str, object]]) -> dict[str, object]:
    recent: dict[str, list[object]] = {
        "accessionNumber": [],
        "filingDate": [],
        "reportDate": [],
        "form": [],
        "primaryDocument": [],
        "primaryDocDescription": [],
        "items": [],
    }
    for row in rows:
        for key in recent:
            recent[key].append(row.get(key, ""))
    return {
        "cik": "0001046179",
        "tickers": ["TSM"],
        "name": "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD",
        "filings": {"recent": recent},
        "_cik": "0001046179",
    }


def test_normalize_filings_filters_insider_forms_and_builds_urls() -> None:
    payload = _sec_payload(
        [
            {
                "accessionNumber": "0001046179-26-000726",
                "filingDate": "2026-10-15",
                "reportDate": "2026-09-30",
                "form": "6-K",
                "primaryDocument": "tsm-20261015x6k.htm",
            },
            {
                "accessionNumber": "0001046179-26-000732",
                "filingDate": "2026-11-03",
                "reportDate": "2026-11-01",
                "form": "4",
                "primaryDocument": "wk-form4.xml",
            },
            {
                "accessionNumber": "0001046179-26-000001",
                "filingDate": "2026-04-10",
                "reportDate": "2025-12-31",
                "form": "20-F",
                "primaryDocument": "tsm-20f.htm",
            },
        ]
    )

    frame = normalize_filings(payload, lookback_days=1000, today=date(2026, 11, 6))

    assert set(frame["公告类型"]) == {"6-K", "20-F"}
    six_k = frame[frame["公告类型"] == "6-K"].iloc[0]
    assert six_k["impact_level"] == "Medium"
    assert "000104617926000726" in six_k["公告链接"]
    twenty_f = frame[frame["公告类型"] == "20-F"].iloc[0]
    assert twenty_f["impact_level"] == "High"


def test_filing_notice_date_prefers_quarterly_release_and_annual_report() -> None:
    payload = _sec_payload(
        [
            {
                "accessionNumber": "a1",
                "filingDate": "2026-10-08",
                "reportDate": "2026-09-30",
                "form": "6-K",
                "primaryDocument": "monthly.htm",
            },
            {
                "accessionNumber": "a2",
                "filingDate": "2026-10-15",
                "reportDate": "2026-09-30",
                "form": "6-K",
                "primaryDocument": "earnings.htm",
            },
            {
                "accessionNumber": "a3",
                "filingDate": "2026-10-22",
                "reportDate": "2026-09-30",
                "form": "6-K",
                "primaryDocument": "insider.htm",
            },
            {
                "accessionNumber": "a4",
                "filingDate": "2026-04-10",
                "reportDate": "2025-12-31",
                "form": "20-F",
                "primaryDocument": "annual.htm",
            },
        ]
    )
    frame = normalize_filings(payload, lookback_days=1000, today=date(2026, 11, 6))

    mapping = filing_notice_date_map(frame)

    assert mapping[pd.Timestamp("2026-09-30")] == pd.Timestamp("2026-10-22")
    assert mapping[pd.Timestamp("2025-12-31")] == pd.Timestamp("2026-04-10")


def test_latest_completed_us_session_waits_for_close() -> None:
    before_close = datetime(2026, 11, 6, 19, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    after_close = datetime(2026, 11, 7, 6, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    weekend = datetime(2026, 11, 8, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    assert latest_completed_us_session(before_close) == date(2026, 11, 5)
    assert latest_completed_us_session(after_close) == date(2026, 11, 6)
    assert latest_completed_us_session(weekend) == date(2026, 11, 6)


def test_build_us_filing_news_items_keeps_one_insider_filing(tmp_path: Path) -> None:
    symbol_info = parse_symbol("TSM.US")
    cache_dir = tmp_path / "stock_info" / f"{symbol_info.stock_name}_{symbol_info.symbol}" / "disclosures"
    cache_dir.mkdir(parents=True)
    frame = pd.DataFrame(
        [
            {
                "公告时间": "2026-10-22",
                "公告类型": "6-K",
                "公告标题": "临时公告（6-K）（报告期 2026-09-30）",
                "报告期": "2026-09-30",
                "摘要": "The changes in the shareholdings of TSMC board of directors",
                "文档要点": "",
                "accession": "a1",
                "impact_level": "medium",
                "公告链接": "https://www.sec.gov/a1",
            },
            {
                "公告时间": "2026-10-15",
                "公告类型": "6-K",
                "公告标题": "临时公告（6-K）（报告期 2026-09-30）",
                "报告期": "2026-09-30",
                "摘要": "TSMC Reports Third Quarter EPS of NT$29.43",
                "文档要点": "第三季度营收同比增长 50.9%",
                "accession": "a2",
                "impact_level": "medium",
                "公告链接": "https://www.sec.gov/a2",
            },
            {
                "公告时间": "2026-09-24",
                "公告类型": "6-K",
                "公告标题": "临时公告（6-K）（报告期 2026-08-31）",
                "报告期": "2026-08-31",
                "摘要": "The changes in the shareholdings of TSMC board of directors",
                "文档要点": "",
                "accession": "a3",
                "impact_level": "medium",
                "公告链接": "https://www.sec.gov/a3",
            },
        ]
    )
    frame.to_csv(cache_dir / "cninfo_list.csv", index=False, encoding="utf-8")

    items, diagnostics = build_us_filing_news_items(
        symbol_info,
        base_dir=tmp_path,
        lookback_days=180,
        today=date(2026, 11, 5),
    )

    assert len(items) == 2
    titles = [item["title"] for item in items]
    assert any("TSMC Reports Third Quarter EPS" in title for title in titles)
    assert sum("shareholdings of" in title for title in titles) == 1
    assert diagnostics


def test_us_financial_summary_uses_structured_cache(tmp_path: Path, monkeypatch) -> None:
    from services.research import us_financial_summary

    symbol_info = parse_symbol("TSM.US")
    cache_dir = tmp_path / "financials_cache"
    cache_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "REPORT_DATE": "2026-09-30",
                "NOTICE_DATE": "2026-10-15",
                "TOTAL_OPERATE_INCOME": 1e10,
                "PARENT_NETPROFIT": 2e9,
                "GROSS_PROFIT": 5e9,
                "CURRENCY": "USD",
            }
        ]
    ).to_csv(cache_dir / "profit_sheet.csv", index=False)
    pd.DataFrame(
        [
            {
                "REPORT_DATE": "2026-09-30",
                "TOTAL_ASSETS": 4e10,
                "TOTAL_LIABILITIES": 1e10,
                "TOTAL_PARENT_EQUITY": 3e10,
                "MONETARYFUNDS": 5e9,
            }
        ]
    ).to_csv(cache_dir / "balance_sheet.csv", index=False)
    pd.DataFrame(
        [
            {
                "REPORT_DATE": "2026-09-30",
                "NETCASH_OPERATE": 6e9,
                "NETCASH_INVEST": -2e9,
                "NETCASH_FINANCE": -1e9,
                "CONSTRUCT_LONG_ASSET": -1.5e9,
            }
        ]
    ).to_csv(cache_dir / "cash_flow_sheet.csv", index=False)
    (cache_dir / "us_financial_meta.json").write_text(
        '{"currency": "USD", "fx_local_per_usd": 1.0, "deduct_proxy": true}',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        us_financial_summary,
        "build_cache_dir",
        lambda *args, **kwargs: cache_dir,
    )

    content = us_financial_summary.build_us_financial_summary(
        symbol_info,
        datetime(2026, 11, 5),
    )

    assert content is not None
    assert "美股结构化财报摘要" in content
    assert "2026-09-30" in content
    assert "资产负债率" in content


def test_quarterly_wide_frame_handles_non_calendar_fiscal_year() -> None:
    """微软/英伟达这类非自然财年，月份规则对不上时必须按实际所在口径兜底。

    之前整张表会拼空，导致 prepare_dataset 抛 CacheIntegrityError。
    """

    # Arrange: 微软式财年，9 月在单季报、12/3 月在累计季报、6 月在年报
    frames = {
        "单季报": pd.concat(
            [
                _long_frame("2026-09-30", {"总资产": 1.0}, "单季报"),
                _long_frame("2025-09-30", {"总资产": 0.9}, "单季报"),
            ],
            ignore_index=True,
        ),
        "累计季报": pd.concat(
            [
                _long_frame("2026-03-31", {"总资产": 0.95}, "累计季报"),
                _long_frame("2025-12-31", {"总资产": 0.93}, "累计季报"),
            ],
            ignore_index=True,
        ),
        "年报": pd.concat(
            [
                _long_frame("2026-06-30", {"总资产": 0.98}, "年报"),
                _long_frame("2025-06-30", {"总资产": 0.88}, "年报"),
            ],
            ignore_index=True,
        ),
    }

    # Act
    wide = build_quarterly_wide_frame(frames, {"总资产": "TOTAL_ASSETS"})

    # Assert: 六个报告期一个都不能丢（月份规则会把 9 月错配给累计季报而整行丢掉）
    assert len(wide) == 6
    assert sorted(wide["REPORT_DATE"].astype(str).tolist()) == [
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
        "2026-03-31",
        "2026-06-30",
        "2026-09-30",
    ]
    assert wide["TOTAL_ASSETS"].notna().all()


def test_derive_quarterly_series_respects_non_calendar_fiscal_year() -> None:
    """MSFT 式财年：Q3 是单季报、Q4 是年报，必须按年内累计链推，不能减上一行。"""

    from indicator_library.calculators.fundamental import (
        derive_quarterly_series_by_report_type,
    )

    sheet = pd.DataFrame(
        [
            ("2025-06-30", "年报", 281.7),
            ("2025-09-30", "单季报", 77.7),
            ("2025-12-31", "累计季报", 158.9),
            ("2026-03-31", "单季报", 82.9),
            ("2026-06-30", "年报", 331.8),
            ("2026-09-30", "单季报", 92.3),
        ],
        columns=["REPORT_DATE", "REPORT_TYPE", "TOTAL_OPERATE_INCOME"],
    )

    series = derive_quarterly_series_by_report_type(sheet, "TOTAL_OPERATE_INCOME")

    assert round(series["2025-12-31"], 1) == 81.2
    assert round(series["2026-03-31"], 1) == 82.9
    # 年报减的是九个月累计（77.7+81.2+82.9），不是上一行单季
    assert round(series["2026-06-30"], 1) == 90.0
    assert round(series["2026-09-30"], 1) == 92.3
