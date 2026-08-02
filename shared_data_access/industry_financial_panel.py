"""Cached full-market financial breadth panel grouped by Eastmoney industry."""

from __future__ import annotations

from datetime import date, datetime
import json
import math
from numbers import Real
from pathlib import Path
from typing import Any, Dict, Mapping

import akshare as ak
import pandas as pd

from core.logging import get_logger

from .cache_registry import (
    CacheKind,
    build_global_cache_dir,
    record_cache_refresh,
    should_refresh,
)


LOGGER = get_logger("IndustryFinancialPanel")
SOURCE_NAME = "akshare.stock_yjbb_em"

RAW_COLUMN_MAP = {
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "营业总收入-营业总收入": "revenue",
    "营业总收入-同比增长": "revenue_growth_yoy",
    "营业总收入-季度环比增长": "revenue_growth_qoq",
    "净利润-净利润": "net_income",
    "净利润-同比增长": "net_income_growth_yoy",
    "净利润-季度环比增长": "net_income_growth_qoq",
    "净资产收益率": "roe",
    "销售毛利率": "gross_margin",
    "所处行业": "em_industry",
    "最新公告日期": "announcement_date",
}

NUMERIC_COLUMNS = (
    "revenue",
    "revenue_growth_yoy",
    "revenue_growth_qoq",
    "net_income",
    "net_income_growth_yoy",
    "net_income_growth_qoq",
    "roe",
    "gross_margin",
)


def completed_report_periods(as_of_date: str) -> tuple[str, str]:
    """Return the latest substantially complete A-share period and its predecessor."""

    as_of = _parse_date(as_of_date)
    year = as_of.year
    month_day = (as_of.month, as_of.day)
    if month_day >= (11, 1):
        return f"{year}0930", f"{year}0630"
    if month_day >= (9, 1):
        return f"{year}0630", f"{year}0331"
    if month_day >= (5, 1):
        return f"{year}0331", f"{year - 1}1231"
    return f"{year - 1}0930", f"{year - 1}0630"


def update_industry_financial_panel_cached(
    as_of_date: str,
    *,
    base_dir: str | Path = "data",
    force_refresh: bool = False,
) -> Dict[str, Path]:
    """Refresh complete report-period snapshots, then build an as-of industry panel."""

    current_period, previous_period = completed_report_periods(as_of_date)
    cache_dir = build_global_cache_dir(
        CacheKind.INDUSTRY_FINANCIAL_PANEL,
        base_dir=base_dir,
        ensure=True,
    )
    refresh_needed = should_refresh(
        cache_dir,
        CacheKind.INDUSTRY_FINANCIAL_PANEL,
        force=force_refresh,
    )
    raw_paths: dict[str, Path] = {}
    for report_period in (current_period, previous_period):
        existing = _latest_raw_path(cache_dir, report_period)
        if existing is not None and not refresh_needed:
            raw_paths[report_period] = existing
            continue
        try:
            raw_paths[report_period] = _fetch_report_period(
                report_period,
                cache_dir=cache_dir,
            )
        except Exception as exc:
            if existing is None:
                raise
            LOGGER.warning(
                "行业财务横截面刷新失败，回退已有缓存: report_period=%s path=%s error=%s",
                report_period,
                existing,
                exc,
            )
            raw_paths[report_period] = existing

    snapshot_path = build_industry_financial_snapshot(
        as_of_date,
        base_dir=base_dir,
        current_period=current_period,
        previous_period=previous_period,
    )
    record_cache_refresh(
        cache_dir,
        latest_as_of_date=as_of_date,
        current_report_period=current_period,
        previous_report_period=previous_period,
        current_raw_path=str(raw_paths[current_period]),
        previous_raw_path=str(raw_paths[previous_period]),
    )
    LOGGER.info(
        "全A行业财务面板已刷新: as_of_date=%s current=%s previous=%s output=%s",
        as_of_date,
        current_period,
        previous_period,
        snapshot_path,
    )
    return {
        "current_period_raw": raw_paths[current_period],
        "previous_period_raw": raw_paths[previous_period],
        "industry_financial_snapshot": snapshot_path,
        "latest": cache_dir / "latest.json",
    }


def build_industry_financial_snapshot(
    as_of_date: str,
    *,
    base_dir: str | Path = "data",
    current_period: str | None = None,
    previous_period: str | None = None,
) -> Path:
    """Aggregate cached full-market reports after applying announcement-date cutoff."""

    _parse_date(as_of_date)
    default_current, default_previous = completed_report_periods(as_of_date)
    current_period = current_period or default_current
    previous_period = previous_period or default_previous
    cache_dir = build_global_cache_dir(
        CacheKind.INDUSTRY_FINANCIAL_PANEL,
        base_dir=base_dir,
        ensure=True,
    )
    current_path = _latest_raw_path(cache_dir, current_period)
    previous_path = _latest_raw_path(cache_dir, previous_period)
    if current_path is None or previous_path is None:
        raise FileNotFoundError(
            f"行业财务原始缓存不完整: current={current_path} previous={previous_path}"
        )

    current = _load_raw_frame(current_path, as_of_date=as_of_date)
    previous = _load_raw_frame(previous_path, as_of_date=as_of_date)
    snapshot = aggregate_industry_financials(
        current,
        previous,
        as_of_date=as_of_date,
        current_period=current_period,
        previous_period=previous_period,
        current_source_path=current_path,
        previous_source_path=previous_path,
    )
    snapshot_dir = cache_dir / "snapshots"
    snapshot_path = snapshot_dir / f"{as_of_date}.json"
    _save_json(snapshot_path, snapshot)
    _save_json(cache_dir / "latest.json", snapshot)
    return snapshot_path


def load_industry_financial_snapshot_cached(
    as_of_date: str,
    *,
    base_dir: str | Path = "data",
    allow_previous: bool = True,
) -> tuple[Dict[str, Any], Path | None]:
    """Load an existing industry panel without triggering any external refresh."""

    cutoff = pd.Timestamp(as_of_date)
    cache_dir = build_global_cache_dir(
        CacheKind.INDUSTRY_FINANCIAL_PANEL,
        base_dir=base_dir,
        ensure=False,
    )
    snapshot_dir = cache_dir / "snapshots"
    exact_path = snapshot_dir / f"{as_of_date}.json"
    if exact_path.exists():
        return _load_json(exact_path), exact_path
    if not allow_previous or not snapshot_dir.exists():
        return {}, None

    candidates: list[tuple[pd.Timestamp, Path]] = []
    for path in snapshot_dir.iterdir():
        if not path.is_file() or path.suffix != ".json":
            continue
        try:
            snapshot_date = pd.Timestamp(path.stem)
        except (TypeError, ValueError):
            continue
        if snapshot_date <= cutoff:
            candidates.append((snapshot_date, path))
    if not candidates:
        return {}, None
    candidates.sort(key=lambda item: item[0])
    selected_path = candidates[-1][1]
    return _load_json(selected_path), selected_path


def aggregate_industry_financials(
    current: pd.DataFrame,
    previous: pd.DataFrame,
    *,
    as_of_date: str,
    current_period: str,
    previous_period: str,
    current_source_path: Path | None = None,
    previous_source_path: Path | None = None,
) -> Dict[str, Any]:
    """Build full-market industry breadth and sequential growth-acceleration metrics."""

    work = current.copy()
    previous_growth = previous[
        ["stock_code", "revenue_growth_yoy", "net_income_growth_yoy"]
    ].rename(
        columns={
            "revenue_growth_yoy": "previous_revenue_growth_yoy",
            "net_income_growth_yoy": "previous_net_income_growth_yoy",
        }
    )
    work = work.merge(previous_growth, on="stock_code", how="left", validate="many_to_one")
    work["revenue_growth_acceleration"] = (
        work["revenue_growth_yoy"] - work["previous_revenue_growth_yoy"]
    )
    work["net_income_growth_acceleration"] = (
        work["net_income_growth_yoy"] - work["previous_net_income_growth_yoy"]
    )
    work = work.loc[work["em_industry"].notna() & work["em_industry"].astype(str).str.strip().ne("")]

    industries = [
        _aggregate_one_industry(industry_name, group)
        for industry_name, group in work.groupby("em_industry", sort=True)
    ]
    industries.sort(
        key=lambda item: (
            -(item.get("both_accelerating_ratio") or 0.0),
            -(item.get("median_revenue_growth_yoy_pct") or -math.inf),
            item["industry_name"],
        )
    )
    return {
        "schema_version": 1,
        "as_of_date": as_of_date,
        "generated_at": datetime.now().isoformat(),
        "source": SOURCE_NAME,
        "current_report_period": current_period,
        "previous_report_period": previous_period,
        "source_paths": {
            "current": str(current_source_path) if current_source_path else None,
            "previous": str(previous_source_path) if previous_source_path else None,
        },
        "summary": {
            "current_company_count": int(len(current.index)),
            "previous_company_count": int(len(previous.index)),
            "classified_company_count": int(len(work.index)),
            "industry_count": len(industries),
        },
        "industries": industries,
    }


def _aggregate_one_industry(industry_name: Any, frame: pd.DataFrame) -> Dict[str, Any]:
    revenue_growth = frame["revenue_growth_yoy"]
    profit_growth = frame["net_income_growth_yoy"]
    revenue_acceleration = frame["revenue_growth_acceleration"]
    profit_acceleration = frame["net_income_growth_acceleration"]
    positive_comparable = revenue_acceleration.notna() & profit_acceleration.notna()
    both_positive = (revenue_growth > 0) & (profit_growth > 0)
    both_accelerating = (revenue_acceleration > 0) & (profit_acceleration > 0)
    comparable_count = int(positive_comparable.sum())
    return {
        "industry_name": str(industry_name),
        "company_count": int(len(frame.index)),
        "comparable_company_count": comparable_count,
        "median_revenue_growth_yoy_pct": _median(revenue_growth),
        "median_net_income_growth_yoy_pct": _median(profit_growth),
        "positive_revenue_growth_count": int((revenue_growth > 0).sum()),
        "positive_revenue_growth_ratio": _ratio(revenue_growth > 0, revenue_growth.notna()),
        "positive_net_income_growth_count": int((profit_growth > 0).sum()),
        "positive_net_income_growth_ratio": _ratio(profit_growth > 0, profit_growth.notna()),
        "both_positive_count": int(both_positive.sum()),
        "both_positive_ratio": _ratio(both_positive, revenue_growth.notna() & profit_growth.notna()),
        "median_revenue_growth_acceleration_pct": _median(revenue_acceleration),
        "median_net_income_growth_acceleration_pct": _median(profit_acceleration),
        "revenue_accelerating_count": int((revenue_acceleration > 0).sum()),
        "revenue_accelerating_ratio": _ratio(revenue_acceleration > 0, revenue_acceleration.notna()),
        "net_income_accelerating_count": int((profit_acceleration > 0).sum()),
        "net_income_accelerating_ratio": _ratio(profit_acceleration > 0, profit_acceleration.notna()),
        "both_accelerating_count": int(both_accelerating.sum()),
        "both_accelerating_ratio": _ratio(both_accelerating, positive_comparable),
        "median_gross_margin_pct": _median(frame["gross_margin"]),
        "median_roe_pct": _median(frame["roe"]),
        "leaders_by_revenue": _top_companies(frame, "revenue"),
        "leaders_by_net_income": _top_companies(frame, "net_income"),
        "leaders_by_growth_acceleration": _top_accelerating_companies(frame),
    }


def _fetch_report_period(report_period: str, *, cache_dir: Path) -> Path:
    LOGGER.info("开始抓取全A业绩横截面: report_period=%s source=%s", report_period, SOURCE_NAME)
    raw = ak.stock_yjbb_em(date=report_period)
    normalized = _normalize_raw_frame(raw, report_period=report_period)
    fetch_date = datetime.now().date().isoformat()
    output_path = cache_dir / "raw" / report_period / f"{fetch_date}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output_path, index=False, encoding="utf-8")
    LOGGER.info(
        "全A业绩横截面已缓存: report_period=%s rows=%d output=%s",
        report_period,
        len(normalized.index),
        output_path,
    )
    return output_path


def _normalize_raw_frame(raw: pd.DataFrame, *, report_period: str) -> pd.DataFrame:
    missing = [column for column in RAW_COLUMN_MAP if column not in raw.columns]
    if missing:
        raise ValueError(f"stock_yjbb_em 缺少预期字段: {', '.join(missing)}")
    work = raw[list(RAW_COLUMN_MAP)].rename(columns=RAW_COLUMN_MAP).copy()
    work["stock_code"] = work["stock_code"].astype(str).str.strip().str.zfill(6)
    work["stock_name"] = work["stock_name"].astype(str).str.strip()
    work["em_industry"] = work["em_industry"].where(work["em_industry"].notna(), None)
    for column in NUMERIC_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work["announcement_date"] = pd.to_datetime(work["announcement_date"], errors="coerce")
    work["report_period"] = _format_report_period(report_period)
    work["fetched_at"] = datetime.now().isoformat()
    work["source"] = SOURCE_NAME
    work = work.sort_values(["stock_code", "announcement_date"]).drop_duplicates(
        "stock_code",
        keep="last",
    )
    work["announcement_date"] = work["announcement_date"].dt.strftime("%Y-%m-%d")
    return work.reset_index(drop=True)


def _load_raw_frame(path: Path, *, as_of_date: str) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype={"stock_code": str})
    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["announcement_date"] = pd.to_datetime(frame["announcement_date"], errors="coerce")
    cutoff = pd.Timestamp(as_of_date)
    frame = frame.loc[frame["announcement_date"].notna() & (frame["announcement_date"] <= cutoff)]
    frame = frame.loc[
        frame["em_industry"].notna()
        & frame["em_industry"].astype(str).str.strip().ne("")
    ]
    return frame.sort_values(["stock_code", "announcement_date"]).drop_duplicates(
        "stock_code",
        keep="last",
    )


def _latest_raw_path(cache_dir: Path, report_period: str) -> Path | None:
    period_dir = cache_dir / "raw" / report_period
    if not period_dir.exists():
        return None
    candidates = sorted(path for path in period_dir.iterdir() if path.is_file() and path.suffix == ".csv")
    return candidates[-1] if candidates else None


def _top_companies(frame: pd.DataFrame, column: str, limit: int = 3) -> list[Dict[str, Any]]:
    valid = frame.loc[frame[column].notna()].sort_values(column, ascending=False).head(limit)
    columns = [
        "stock_code",
        "stock_name",
        "revenue",
        "net_income",
        "revenue_growth_yoy",
        "net_income_growth_yoy",
    ]
    return _records(valid[columns])


def _top_accelerating_companies(frame: pd.DataFrame, limit: int = 3) -> list[Dict[str, Any]]:
    valid = frame.loc[
        (frame["revenue_growth_acceleration"] > 0)
        & (frame["net_income_growth_acceleration"] > 0)
    ].copy()
    valid["combined_growth_acceleration"] = (
        valid["revenue_growth_acceleration"] + valid["net_income_growth_acceleration"]
    )
    valid = valid.sort_values("combined_growth_acceleration", ascending=False).head(limit)
    columns = [
        "stock_code",
        "stock_name",
        "revenue_growth_yoy",
        "net_income_growth_yoy",
        "revenue_growth_acceleration",
        "net_income_growth_acceleration",
    ]
    return _records(valid[columns])


def _records(frame: pd.DataFrame) -> list[Dict[str, Any]]:
    return frame.where(pd.notna(frame), None).to_dict(orient="records")


def _median(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return round(float(values.median()), 4) if not values.empty else None


def _ratio(numerator_mask: pd.Series, denominator_mask: pd.Series) -> float:
    denominator = int(denominator_mask.sum())
    if denominator == 0:
        return 0.0
    return round(float((numerator_mask & denominator_mask).sum() / denominator), 4)


def _format_report_period(value: str) -> str:
    if len(value) != 8 or not value.isdigit():
        raise ValueError(f"报告期格式必须为 YYYYMMDD: {value}")
    return f"{value[:4]}-{value[4:6]}-{value[6:]}"


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _load_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _save_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_json_numbers(payload)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_json_numbers(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: _normalize_json_numbers(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_normalize_json_numbers(item) for item in payload]
    if isinstance(payload, tuple):
        return [_normalize_json_numbers(item) for item in payload]
    if isinstance(payload, bool) or payload is None or isinstance(payload, int):
        return payload
    if isinstance(payload, Real):
        numeric = float(payload)
        if not math.isfinite(numeric):
            return None
        return round(numeric, 4)
    return payload


__all__ = [
    "aggregate_industry_financials",
    "build_industry_financial_snapshot",
    "completed_report_periods",
    "load_industry_financial_snapshot_cached",
    "update_industry_financial_panel_cached",
]
