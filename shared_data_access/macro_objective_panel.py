"""Global macro objective data panel cache and formatting helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping
import json

import akshare as ak
import pandas as pd

from core.logging import get_logger

from .cache_registry import CacheKind, build_global_cache_dir, record_cache_refresh

try:  # pragma: no cover - optional fallback
    import yfinance as yf
except Exception:  # pragma: no cover - optional fallback
    yf = None


LOGGER = get_logger("MacroObjectivePanel")

INDICATOR_ORDER = (
    "US02Y",
    "US10Y",
    "CN10Y",
    "DXY",
    "USD_CNH",
    "COMEX_GOLD",
    "LONDON_GOLD",
    "BRENT",
    "WTI",
)

CENTRAL_BANK_ORDER = (
    "FED",
    "PBOC_LPR_1Y",
    "PBOC_LPR_5Y",
    "BOJ",
    "ECB",
)


def load_or_build_macro_objective_panel(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """Load cached macro panel or fetch a new snapshot for today's run date."""

    cache_dir = build_global_cache_dir(CacheKind.MACRO_OBJECTIVE_PANEL, base_dir=base_dir, ensure=True)
    snapshot_dir = cache_dir / "daily_snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"{run_date}.json"

    if snapshot_path.exists() and not force_refresh:
        return _load_json(snapshot_path, default=_warning_payload(run_date, "snapshot_exists_but_unreadable"))

    today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
    if run_date != today_str:
        payload = _warning_payload(
            run_date,
            f"缺少 {run_date} 的宏观客观数据面板缓存，且该日期不是今天，跳过实时抓取。",
        )
        if not snapshot_path.exists():
            _save_json(snapshot_path, payload)
        return payload

    payload = _build_macro_objective_panel(run_date)
    _save_json(snapshot_path, payload)
    _save_json(cache_dir / "latest.json", payload)
    record_cache_refresh(
        cache_dir,
        latest_run_date=run_date,
        indicator_count=len(payload.get("indicators", {})),
        central_bank_count=len(payload.get("central_banks", {})),
    )
    return payload


def load_macro_objective_panel(
    run_date: str | None = None,
    *,
    base_dir: str | Path = "data",
) -> Dict[str, Any]:
    """Read cached macro panel from daily snapshot or latest snapshot."""

    cache_dir = build_global_cache_dir(CacheKind.MACRO_OBJECTIVE_PANEL, base_dir=base_dir, ensure=True)
    if run_date:
        snapshot_path = cache_dir / "daily_snapshots" / f"{run_date}.json"
        if snapshot_path.exists():
            return _load_json(snapshot_path, default={})
    latest_path = cache_dir / "latest.json"
    return _load_json(latest_path, default={})


def render_macro_objective_panel_markdown(payload: Mapping[str, Any]) -> str:
    """Render a compact markdown appendix for the macro objective panel."""

    if not payload:
        return "> 宏观客观数据面板为空。"

    lines = [
        "## 附录：宏观客观数据面板",
        "",
        "| 类别 | 指标 | 最新值 | 时间 | 来源 | 状态 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    indicators = payload.get("indicators", {}) if isinstance(payload, Mapping) else {}
    for key in INDICATOR_ORDER:
        metric = indicators.get(key)
        if not isinstance(metric, Mapping):
            continue
        lines.append(_render_metric_row("市场", metric))

    central_banks = payload.get("central_banks", {}) if isinstance(payload, Mapping) else {}
    for key in CENTRAL_BANK_ORDER:
        metric = central_banks.get(key)
        if not isinstance(metric, Mapping):
            continue
        lines.append(_render_metric_row("央行", metric))

    notes = [
        _coerce_str(item.get("warning") or item.get("message"))
        for item in payload.get("source_status", [])
        if isinstance(item, Mapping) and (item.get("warning") or item.get("message"))
    ]
    notes = [item for item in notes if item]
    if notes:
        lines.extend(["", "### 数据源提示", ""])
        lines.extend([f"- {note}" for note in notes])

    return "\n".join(lines)


def _build_macro_objective_panel(run_date: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema_version": 1,
        "run_date": run_date,
        "collected_at": datetime.now().isoformat(),
        "indicators": {},
        "central_banks": {},
        "source_status": [],
    }
    run_dt = pd.Timestamp(run_date).normalize()

    _attach_bond_metrics(payload, run_dt)
    _attach_usd_cnh(payload, run_dt)
    _attach_dxy(payload, run_dt)
    _attach_futures_metrics(payload, run_dt)
    _attach_central_bank_metrics(payload, run_dt)

    return payload


def _attach_bond_metrics(payload: Dict[str, Any], run_dt: pd.Timestamp) -> None:
    try:
        start_date = (run_dt - pd.Timedelta(days=10)).strftime("%Y%m%d")
        df = ak.bond_zh_us_rate(start_date=start_date)
        df = df.copy()
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df = df.dropna(subset=["日期"])
        row = df[df["日期"] <= run_dt].sort_values("日期").tail(1)
        if row.empty:
            raise ValueError("bond_zh_us_rate 未返回有效记录")
        latest = row.iloc[0]
        as_of_date = latest["日期"].strftime("%Y-%m-%d")
        payload["indicators"]["US02Y"] = _metric(
            label="US02Y",
            value=latest.get("美国国债收益率2年"),
            unit="%",
            as_of_date=as_of_date,
            source="akshare.bond_zh_us_rate",
        )
        payload["indicators"]["US10Y"] = _metric(
            label="US10Y",
            value=latest.get("美国国债收益率10年"),
            unit="%",
            as_of_date=as_of_date,
            source="akshare.bond_zh_us_rate",
        )
        payload["indicators"]["CN10Y"] = _metric(
            label="中国10Y国债",
            value=latest.get("中国国债收益率10年"),
            unit="%",
            as_of_date=as_of_date,
            source="akshare.bond_zh_us_rate",
        )
        payload["source_status"].append(
            {
                "source": "akshare.bond_zh_us_rate",
                "status": "ok",
                "as_of_date": as_of_date,
            }
        )
    except Exception as exc:
        payload["indicators"]["US02Y"] = _metric("US02Y", None, "%", None, "akshare.bond_zh_us_rate", status="warning")
        payload["indicators"]["US10Y"] = _metric("US10Y", None, "%", None, "akshare.bond_zh_us_rate", status="warning")
        payload["indicators"]["CN10Y"] = _metric("中国10Y国债", None, "%", None, "akshare.bond_zh_us_rate", status="warning")
        payload["source_status"].append(
            {
                "source": "akshare.bond_zh_us_rate",
                "status": "warning",
                "warning": f"美债/中债收益率抓取失败: {exc}",
            }
        )


def _attach_usd_cnh(payload: Dict[str, Any], run_dt: pd.Timestamp) -> None:
    try:
        df = ak.fx_quote_baidu(symbol="美元")
        row = df[df["代码"].astype(str) == "USDCNH"].tail(1)
        if row.empty:
            raise ValueError("fx_quote_baidu 未返回 USDCNH")
        latest = row.iloc[0]
        payload["indicators"]["USD_CNH"] = _metric(
            label="USD/CNH",
            value=latest.get("最新价"),
            unit="",
            as_of_date=run_dt.strftime("%Y-%m-%d"),
            source="akshare.fx_quote_baidu",
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        payload["source_status"].append(
            {
                "source": "akshare.fx_quote_baidu",
                "status": "ok",
                "symbol": "USDCNH",
            }
        )
    except Exception as exc:
        payload["indicators"]["USD_CNH"] = _metric("USD/CNH", None, "", None, "akshare.fx_quote_baidu", status="warning")
        payload["source_status"].append(
            {
                "source": "akshare.fx_quote_baidu",
                "status": "warning",
                "warning": f"USD/CNH 抓取失败: {exc}",
            }
        )


def _attach_dxy(payload: Dict[str, Any], run_dt: pd.Timestamp) -> None:
    errors: list[str] = []
    if yf is not None:
        try:
            ticker = yf.Ticker("DX-Y.NYB")
            hist = ticker.history(period="10d", interval="1d", auto_adjust=False)
            if not hist.empty:
                hist = hist.copy()
                hist.index = pd.to_datetime(hist.index).tz_localize(None)
                row = hist[hist.index.normalize() <= run_dt].tail(1)
                if not row.empty:
                    ts = row.index[-1]
                    value = row.iloc[-1].get("Close")
                    payload["indicators"]["DXY"] = _metric(
                        label="DXY",
                        value=value,
                        unit="",
                        as_of_date=ts.strftime("%Y-%m-%d"),
                        source="yfinance.DX-Y.NYB",
                    )
                    payload["source_status"].append(
                        {
                            "source": "yfinance.DX-Y.NYB",
                            "status": "ok",
                            "as_of_date": ts.strftime("%Y-%m-%d"),
                        }
                    )
                    return
            errors.append("history 返回空数据")
        except Exception as exc:  # pragma: no cover - network defensive
            errors.append(str(exc))
    else:
        errors.append("yfinance 不可用")

    payload["indicators"]["DXY"] = _metric("DXY", None, "", None, "yfinance.DX-Y.NYB", status="warning")
    payload["source_status"].append(
        {
            "source": "yfinance.DX-Y.NYB",
            "status": "warning",
            "warning": f"DXY 抓取失败: {'; '.join(errors)}",
        }
    )


def _attach_futures_metrics(payload: Dict[str, Any], run_dt: pd.Timestamp) -> None:
    symbol_map = {
        "GC": ("COMEX_GOLD", "COMEX黄金"),
        "XAU": ("LONDON_GOLD", "伦敦金"),
        "OIL": ("BRENT", "Brent"),
        "CL": ("WTI", "WTI"),
    }
    for source_symbol, (metric_key, label) in symbol_map.items():
        try:
            df = ak.futures_foreign_commodity_realtime(symbol=source_symbol)
            if df.empty:
                raise ValueError("返回空数据")
            latest = df.iloc[-1]
            as_of_date = _coerce_str(latest.get("日期")) or run_dt.strftime("%Y-%m-%d")
            updated_at = f"{as_of_date} {_coerce_str(latest.get('行情时间'))}".strip()
            payload["indicators"][metric_key] = _metric(
                label=label,
                value=latest.get("最新价"),
                unit="",
                as_of_date=as_of_date,
                source=f"akshare.futures_foreign_commodity_realtime:{source_symbol}",
                updated_at=updated_at,
                extra={
                    "change": _round_float(latest.get("涨跌额")),
                    "change_pct": _round_float(latest.get("涨跌幅")),
                    "prev_close": _round_float(latest.get("昨日结算价")),
                },
            )
            payload["source_status"].append(
                {
                    "source": f"akshare.futures_foreign_commodity_realtime:{source_symbol}",
                    "status": "ok",
                }
            )
        except Exception as exc:
            payload["indicators"][metric_key] = _metric(
                label=label,
                value=None,
                unit="",
                as_of_date=None,
                source=f"akshare.futures_foreign_commodity_realtime:{source_symbol}",
                status="warning",
            )
            payload["source_status"].append(
                {
                    "source": f"akshare.futures_foreign_commodity_realtime:{source_symbol}",
                    "status": "warning",
                    "warning": f"{label} 抓取失败: {exc}",
                }
            )


def _attach_central_bank_metrics(payload: Dict[str, Any], run_dt: pd.Timestamp) -> None:
    _attach_lpr_metrics(payload, run_dt)
    _attach_policy_metric(
        payload,
        metric_key="FED",
        label="Fed利率",
        fetcher=ak.macro_bank_usa_interest_rate,
        source="akshare.macro_bank_usa_interest_rate",
        max_age_days=120,
        run_dt=run_dt,
    )
    _attach_policy_metric(
        payload,
        metric_key="BOJ",
        label="BOJ利率",
        fetcher=ak.macro_bank_japan_interest_rate,
        source="akshare.macro_bank_japan_interest_rate",
        max_age_days=120,
        run_dt=run_dt,
    )
    _attach_policy_metric(
        payload,
        metric_key="ECB",
        label="ECB利率",
        fetcher=ak.macro_bank_euro_interest_rate,
        source="akshare.macro_bank_euro_interest_rate",
        max_age_days=120,
        run_dt=run_dt,
    )


def _attach_lpr_metrics(payload: Dict[str, Any], run_dt: pd.Timestamp) -> None:
    source = "akshare.macro_china_lpr"
    try:
        df = ak.macro_china_lpr()
        df = df.copy()
        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
        df = df.dropna(subset=["TRADE_DATE"])
        row = df[df["TRADE_DATE"] <= run_dt].sort_values("TRADE_DATE").tail(1)
        if row.empty:
            raise ValueError("macro_china_lpr 未返回有效记录")
        latest = row.iloc[0]
        as_of_date = latest["TRADE_DATE"].strftime("%Y-%m-%d")
        payload["central_banks"]["PBOC_LPR_1Y"] = _metric(
            label="PBOC 1Y LPR",
            value=latest.get("LPR1Y"),
            unit="%",
            as_of_date=as_of_date,
            source=source,
        )
        payload["central_banks"]["PBOC_LPR_5Y"] = _metric(
            label="PBOC 5Y LPR",
            value=latest.get("LPR5Y"),
            unit="%",
            as_of_date=as_of_date,
            source=source,
        )
        payload["source_status"].append(
            {
                "source": source,
                "status": "ok",
                "as_of_date": as_of_date,
            }
        )
    except Exception as exc:
        payload["central_banks"]["PBOC_LPR_1Y"] = _metric("PBOC 1Y LPR", None, "%", None, source, status="warning")
        payload["central_banks"]["PBOC_LPR_5Y"] = _metric("PBOC 5Y LPR", None, "%", None, source, status="warning")
        payload["source_status"].append(
            {
                "source": source,
                "status": "warning",
                "warning": f"LPR 抓取失败: {exc}",
            }
        )


def _attach_policy_metric(
    payload: Dict[str, Any],
    *,
    metric_key: str,
    label: str,
    fetcher: Any,
    source: str,
    max_age_days: int,
    run_dt: pd.Timestamp,
) -> None:
    try:
        df = fetcher()
        df = df.copy()
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df["今值"] = pd.to_numeric(df["今值"], errors="coerce")
        df = df.dropna(subset=["日期", "今值"])
        if df.empty:
            raise ValueError("返回空数据")
        latest = df[df["日期"] <= run_dt].sort_values("日期").iloc[-1]
        as_of_dt = pd.Timestamp(latest["日期"])
        age_days = int((pd.Timestamp.now().normalize() - as_of_dt.normalize()).days)
        status = "ok" if age_days <= max_age_days else "stale"
        note = None
        if status == "stale":
            note = f"最新记录距今 {age_days} 天，当前不宜作为唯一实时政策依据。"
        payload["central_banks"][metric_key] = _metric(
            label=label,
            value=latest.get("今值"),
            unit="%",
            as_of_date=as_of_dt.strftime("%Y-%m-%d"),
            source=source,
            status=status,
            note=note,
            extra={
                "forecast": _round_float(latest.get("预测值")),
                "previous": _round_float(latest.get("前值")),
            },
        )
        status_payload = {
            "source": source,
            "status": status,
            "as_of_date": as_of_dt.strftime("%Y-%m-%d"),
        }
        if note:
            status_payload["warning"] = note
        payload["source_status"].append(status_payload)
    except Exception as exc:
        payload["central_banks"][metric_key] = _metric(label, None, "%", None, source, status="warning")
        payload["source_status"].append(
            {
                "source": source,
                "status": "warning",
                "warning": f"{label} 抓取失败: {exc}",
            }
        )


def _metric(
    label: str,
    value: Any,
    unit: str,
    as_of_date: str | None,
    source: str,
    *,
    status: str = "ok",
    updated_at: str | None = None,
    note: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "label": label,
        "value": _round_float(value),
        "unit": unit,
        "as_of_date": as_of_date,
        "source": source,
        "status": status,
    }
    if updated_at:
        payload["updated_at"] = updated_at
    if note:
        payload["note"] = note
    if extra:
        payload["extra"] = dict(extra)
    return payload


def _render_metric_row(category: str, metric: Mapping[str, Any]) -> str:
    value = metric.get("value")
    unit = _coerce_str(metric.get("unit"))
    if value is None:
        display_value = "N/A"
    else:
        display_value = f"{value}{unit}"
    as_of = _coerce_str(metric.get("updated_at")) or _coerce_str(metric.get("as_of_date")) or "N/A"
    source = _coerce_str(metric.get("source")) or "N/A"
    status = _coerce_str(metric.get("status")) or "unknown"
    label = _coerce_str(metric.get("label")) or "N/A"
    return f"| {category} | {label} | {display_value} | {as_of} | {source} | {status} |"


def _warning_payload(run_date: str, message: str) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "run_date": run_date,
        "collected_at": "",
        "indicators": {},
        "central_banks": {},
        "source_status": [
            {
                "source": "macro_objective_panel",
                "status": "warning",
                "warning": message,
            }
        ],
    }


def _save_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_json(path: Path, *, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _round_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


__all__ = [
    "load_or_build_macro_objective_panel",
    "load_macro_objective_panel",
    "render_macro_objective_panel_markdown",
]
