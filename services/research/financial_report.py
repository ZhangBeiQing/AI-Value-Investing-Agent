"""Financial report service implementation for the skill-only architecture."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from core.logging import init_tool_logger
from configs.stock_pool import TRACKED_A_STOCKS
from shared_data_access import SharedDataAccess
from shared_data_access.cache_registry import (
    CacheKind,
    build_cache_dir,
    update_cn_profit_forecast_cached,
    update_hk_profit_forecast_cached,
)
from utlity import SymbolInfo, get_stock_data_dir, is_cn_etf_symbol, parse_symbol


load_dotenv()

logger = init_tool_logger("financial_report")

CUSTOM_STOCK_DIR = os.getenv("STOCK_DATA_DIR")
STOCK_BASE_DIR = Path(CUSTOM_STOCK_DIR).resolve() if CUSTOM_STOCK_DIR else Path("data/stock_info").resolve()
SYMBOL_NAME_MAP = {entry.symbol: entry.name for entry in TRACKED_A_STOCKS}
PRICE_CLOSE_COLUMNS = ("收盘", "close", "Close", "收盘价", "CLOSE")
FORECAST_DIR_NAME = "forecast"
PROFIT_FORECAST_FILE = "profit_forecast.csv"


@dataclass(frozen=True)
class ReportMeta:
    path: Path
    release_date: date
    fiscal_year: int
    fiscal_quarter: int

    @property
    def stem(self) -> str:
        return self.path.stem


def _stock_root_dir(symbol_info: SymbolInfo) -> Path:
    stock_name = symbol_info.stock_name or SYMBOL_NAME_MAP.get(symbol_info.symbol, symbol_info.symbol)
    if CUSTOM_STOCK_DIR:
        return STOCK_BASE_DIR / f"{stock_name}_{symbol_info.symbol}"
    return get_stock_data_dir(symbol_info)


def _list_report_files(symbol_info: SymbolInfo) -> List[Path]:
    target_dir = _stock_root_dir(symbol_info) / "financial_reports"
    if not target_dir.is_dir():
        logger.warning("财报目录不存在: %s", target_dir)
        return []
    files = [p for p in target_dir.iterdir() if p.suffix.lower() == ".md"]
    files.sort()
    return files


def _list_forecast_files(symbol_info: SymbolInfo) -> List[Path]:
    target_dir = _stock_root_dir(symbol_info) / FORECAST_DIR_NAME
    if not target_dir.is_dir():
        return []
    files = [p for p in target_dir.iterdir() if p.suffix.lower() == ".md"]
    files.sort()
    return files


DATE_PATTERN = re.compile(r"(\d{8})")
YEAR_PATTERN = re.compile(r"(20\d{2}|\d{2})")
PERIOD_KEYWORDS: List[Tuple[str, int]] = [
    ("一季报", 1),
    ("1季报", 1),
    ("一季度", 1),
    ("Q1", 1),
    ("二季报", 2),
    ("半年报", 2),
    ("中报", 2),
    ("Q2", 2),
    ("三季报", 3),
    ("三季度", 3),
    ("Q3", 3),
    ("四季报", 4),
    ("年报", 4),
    ("Q4", 4),
]


def _infer_quarter_from_month(release_month: int) -> int:
    if release_month <= 4:
        return 1
    if release_month <= 8:
        return 2
    if release_month <= 10:
        return 3
    return 4


def _parse_report_meta(path: Path) -> Optional[ReportMeta]:
    match = DATE_PATTERN.search(path.stem)
    if not match:
        return None
    try:
        release_dt = datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None

    suffix = path.stem[match.end() :]
    if suffix.startswith("_"):
        suffix = suffix[1:]

    year_match = YEAR_PATTERN.search(suffix)
    if year_match:
        year_val = year_match.group()
        fiscal_year = int(year_val) if len(year_val) == 4 else 2000 + int(year_val)
    else:
        fiscal_year = release_dt.year

    fiscal_quarter = _infer_quarter_from_month(release_dt.month)
    for keyword, quarter in PERIOD_KEYWORDS:
        if keyword in suffix:
            fiscal_quarter = quarter
            break

    return ReportMeta(path=path, release_date=release_dt, fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter)


def _select_reports(
    files: List[Path],
    today_dt: datetime,
    *,
    available_cutoff_date: Optional[date] = None,
) -> Tuple[ReportMeta, Optional[ReportMeta]]:
    metas = [meta for meta in (_parse_report_meta(p) for p in files) if meta]
    if not metas:
        raise FileNotFoundError("未能解析任何财报文件名。")

    selection_cutoff = available_cutoff_date or today_dt.date()

    metas.sort(
        key=lambda m: (m.release_date, m.fiscal_year, m.fiscal_quarter, m.stem),
        reverse=True,
    )
    latest_available = next((m for m in metas if m.release_date <= selection_cutoff), None)
    if latest_available is None:
        raise FileNotFoundError(f"没有早于或等于 cutoff:{selection_cutoff.isoformat()} 的财报文件。")

    future_candidates = sorted(
        [m for m in metas if m.release_date > latest_available.release_date],
        key=lambda m: m.release_date,
    )
    next_future = future_candidates[0] if future_candidates else None
    return latest_available, next_future


def _load_latest_forecast_markdown(symbol_info: SymbolInfo, today_dt: datetime) -> tuple[str, Optional[Path]]:
    files = _list_forecast_files(symbol_info)
    if not files:
        return "", None
    try:
        latest_meta, _ = _select_reports(files, today_dt)
    except Exception:
        return "", None
    try:
        return latest_meta.path.read_text(encoding="utf-8").strip(), latest_meta.path
    except OSError:
        return "", None


def _repair_mojibake(text: str) -> str:
    if not text or not isinstance(text, str):
        return text
    if any("\u4e00" <= ch <= "\u9fff" for ch in text):
        return text
    try:
        fixed = text.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return text
    if any("\u4e00" <= ch <= "\u9fff" for ch in fixed):
        return fixed
    return text


def _repair_mojibake_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work.columns = [_repair_mojibake(str(col)) for col in work.columns]
    for col in work.columns:
        if work[col].dtype == object:
            work[col] = work[col].apply(lambda value: _repair_mojibake(value) if isinstance(value, str) else value)
    return work


def _extract_year(value: object) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(20\d{2})", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _read_profit_forecast_cache(symbol_info: SymbolInfo, kind: CacheKind) -> pd.DataFrame:
    cache_dir = build_cache_dir(symbol_info, kind, base_dir="data", ensure=True)
    csv_path = cache_dir / PROFIT_FORECAST_FILE
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()


def _load_cn_profit_forecast(symbol_info: SymbolInfo) -> pd.DataFrame:
    if not symbol_info.is_cn_market():
        return pd.DataFrame()
    is_index = symbol_info.market == "CN_INDEX"
    is_etf = symbol_info.code.startswith(("51", "58", "15", "16", "50", "53"))
    if is_index or is_etf:
        return pd.DataFrame()

    df = update_cn_profit_forecast_cached(
        symbol_info,
        base_data_dir="data",
        force_refresh=False,
        logger=logger,
    )
    if df is not None and not df.empty:
        return df
    return _read_profit_forecast_cache(symbol_info, CacheKind.CN_PROFIT_FORECAST)


def _load_hk_profit_forecast(symbol_info: SymbolInfo) -> pd.DataFrame:
    if not symbol_info.is_hk_market():
        return pd.DataFrame()

    df = update_hk_profit_forecast_cached(
        symbol_info,
        base_data_dir="data",
        force_refresh=False,
        logger=logger,
    )
    if df is not None and not df.empty:
        return df
    return _read_profit_forecast_cache(symbol_info, CacheKind.HK_PROFIT_FORECAST)


def _render_markdown_table(rows: List[dict], columns: List[str]) -> str:
    if not rows:
        return "> 未找到符合条件的记录。"
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, sep]
    for row in rows:
        vals: List[str] = []
        for col in columns:
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                vals.append("")
            else:
                vals.append(str(val))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def _format_cn_profit_forecast_sections(symbol_info: SymbolInfo) -> str:
    if not symbol_info.is_cn_market():
        return ""
    is_index = symbol_info.market == "CN_INDEX"
    is_etf = symbol_info.code.startswith(("51", "58", "15", "16", "50", "53"))
    if is_index or is_etf:
        return ""

    df = _load_cn_profit_forecast(symbol_info)
    if df.empty or "预测指标" not in df.columns:
        return ""

    work = df.copy()
    work["预测指标"] = work["预测指标"].astype(str)
    work = work.loc[work["预测指标"] != "市盈率(动态)"].copy()

    preferred_rows = [
        "营业收入(元)",
        "利润总额(元)",
        "净利润(元)",
        "净利润增长率",
    ]
    filtered = work.loc[work["预测指标"].isin(preferred_rows)].copy()
    if filtered.empty:
        filtered = work.head(12).copy()

    columns = [col for col in filtered.columns if col != "预测指标"][:8]
    display_cols = ["预测指标", *columns]
    rows = filtered[display_cols].to_dict(orient="records")

    lines: List[str] = []
    lines.append("### A股机构一致预期（同花顺汇总）")
    lines.append(_render_markdown_table(rows, display_cols))
    lines.append("")
    lines.append(
        "> 说明：同花顺表格中除“营业收入/利润总额/净利润”等核心预测外，"
        "部分指标为同花顺基于“当前股价/市值保持不变”的假设，用预测净利润等推算的派生指标，"
        "具有口径假设，仅供参考。"
    )
    lines.append(
        "> 风险提示：机构预测仅供参考，可能受模型假设、样本偏差或机构自身立场与利益影响，"
        "请勿完全依赖，建议结合财报与公告信息独立判断。"
    )
    return "\n".join(lines).strip()


def _format_hk_profit_forecast_sections(symbol_info: SymbolInfo, today_time: str) -> str:
    if not symbol_info.is_hk_market():
        return ""

    df = _load_hk_profit_forecast(symbol_info)
    if df.empty:
        return ""

    work = _repair_mojibake_dataframe(df)
    required_cols = {"财政年度", "目标价", "更新日期"}
    if not required_cols.issubset(set(work.columns)):
        return ""

    work["更新日期"] = pd.to_datetime(work["更新日期"], errors="coerce")
    work = work.dropna(subset=["更新日期"])
    try:
        cutoff_dt = datetime.strptime(today_time, "%Y-%m-%d")
        work = work.loc[work["更新日期"] <= cutoff_dt]
    except ValueError:
        pass

    work["财政年度"] = work["财政年度"].apply(_extract_year)
    work["目标价"] = pd.to_numeric(work["目标价"], errors="coerce")

    years = sorted({year for year in work["财政年度"].dropna().tolist() if isinstance(year, int)})
    if not years:
        return ""

    try:
        today_year = datetime.strptime(today_time, "%Y-%m-%d").year
    except Exception:
        today_year = datetime.now().year
    future_years = [year for year in years if year >= today_year]
    target_year = min(future_years) if future_years else max(years)

    filtered = work.loc[(work["财政年度"] == target_year) & work["目标价"].notna()].copy()
    filtered = filtered.sort_values(by="更新日期", ascending=False)
    top = filtered.head(10).copy()
    if top.empty:
        return ""

    top["更新日期"] = top["更新日期"].dt.strftime("%Y-%m-%d")
    cols = [col for col in ("更新日期", "证券商", "评级", "目标价") if col in top.columns]
    rows = top[cols].to_dict(orient="records")

    targets = pd.to_numeric(top["目标价"], errors="coerce").dropna()
    if targets.empty:
        return ""

    avg_val = float(targets.mean())
    median_val = float(targets.median())
    max_val = float(targets.max())
    min_val = float(targets.min())

    def _first_broker(value: float) -> str:
        if "证券商" not in top.columns:
            return ""
        hits = top.loc[pd.to_numeric(top["目标价"], errors="coerce") == value]
        if hits.empty:
            return ""
        return str(hits.iloc[0].get("证券商") or "")

    max_broker = _first_broker(max_val)
    min_broker = _first_broker(min_val)

    rating_dist_lines: List[str] = []
    if "评级" in top.columns:
        counts = top["评级"].astype(str).value_counts()
        rating_dist_lines = [f"- {key}: {int(val)}" for key, val in counts.items()]

    lines: List[str] = []
    lines.append(f"### 港股机构一致预期（经济通，{target_year}财年）")
    lines.append(_render_markdown_table(rows, cols))
    lines.append("")
    lines.append("### 港股目标价统计")
    lines.append(f"- 样本数量: {len(top)} 家机构")
    lines.append(f"- 平均目标价: {avg_val:.2f} 港元")
    lines.append(f"- 中位数目标价: {median_val:.2f} 港元")
    lines.append(f"- 最高目标价: {max_val:.2f} 港元" + (f" ({max_broker})" if max_broker else ""))
    lines.append(f"- 最低目标价: {min_val:.2f} 港元" + (f" ({min_broker})" if min_broker else ""))
    lines.append(f"- 目标价区间: {min_val:.2f} - {max_val:.2f} 港元")
    if rating_dist_lines:
        lines.append("- 评级分布:")
        lines.extend(rating_dist_lines)
    lines.append("")
    lines.append(
        "> 风险提示：机构预测/评级/目标价仅供参考，可能受模型假设、样本偏差或机构自身立场与利益影响，"
        "请结合公司基本面与财报原始数据独立判断。"
    )
    return "\n".join(lines).strip()


def _extract_close(series: pd.Series) -> Optional[float]:
    for col in PRICE_CLOSE_COLUMNS:
        if col in series:
            value = series[col]
            if pd.notna(value):
                return float(value)
    return None


def _get_price_on_or_after(frame: pd.DataFrame, target: date) -> Optional[float]:
    subset = frame.loc[frame.index >= pd.Timestamp(target)]
    if subset.empty:
        return None
    return _extract_close(subset.iloc[0])


def _get_price_on_or_before(frame: pd.DataFrame, target: date) -> Optional[float]:
    subset = frame.loc[frame.index <= pd.Timestamp(target)]
    if subset.empty:
        return None
    return _extract_close(subset.iloc[-1])


def _compute_price_drift(symbol_info: SymbolInfo, today_time: str, release_day: date) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    today_date = datetime.strptime(today_time, "%Y-%m-%d").date()
    if release_day > today_date:
        return None, None, None

    accessor = SharedDataAccess(logger=logger)
    dataset = accessor.prepare_dataset(symbolInfo=symbol_info, as_of_date=today_time)
    frame = dataset.prices.frame
    if frame.empty:
        return None, None, None
    release_price = _get_price_on_or_after(frame, release_day)
    today_price = _get_price_on_or_before(frame, today_date)
    if release_price is None or today_price is None or release_price == 0:
        return release_price, today_price, None
    pct = (today_price - release_price) / release_price * 100
    return release_price, today_price, pct


def _decorate_content(
    content: str,
    stock_name: str,
    today_time: str,
    latest_meta: ReportMeta,
    next_meta: Optional[ReportMeta],
    release_price: Optional[float],
    today_price: Optional[float],
    change_pct: Optional[float],
) -> Tuple[str, dict]:
    today_dt = datetime.strptime(today_time, "%Y-%m-%d").date()
    days_since = (today_dt - latest_meta.release_date).days
    report_is_after_today = days_since < 0
    if report_is_after_today:
        price_sentence = (
            f"该研究包按 next-day 宽限窗口引用了 {latest_meta.release_date.isoformat()} 发布的财报总结，"
            f"它相对当前请求日期晚 {abs(days_since)} 天发布，因此暂不计算“财报发布日至今”的股价变动。"
        )
    elif change_pct is None:
        price_sentence = "由于缺少有效的价格数据，暂时无法计算财报发布日至今的股价变动，请结合行情自行评估市场是否已经消化该信息。"
    else:
        direction = "上涨" if change_pct >= 0 else "下跌"
        price_sentence = f"当前股价距离财报发行日已经{direction}了{abs(change_pct):.2f}%，请自己评估当前市场是否已充分定价该财报带来的影响。"

    if next_meta:
        next_gap = (next_meta.release_date - today_dt).days
        next_sentence = f"距离下一季度财报发行日预告还有{next_gap}天（参考文件日期 {next_meta.release_date.isoformat()}）。"
        next_gap_val: Optional[int] = next_gap
        next_date_str = next_meta.release_date.isoformat()
    else:
        next_sentence = "距离下一季度财报发行日预告还有未知。"
        next_gap_val = None
        next_date_str = None

    if report_is_after_today:
        report_timing_text = (
            f"当前日期是{today_time}，{stock_name}最近季度财报对应的总结文件日期为"
            f"{latest_meta.release_date.isoformat()}，相对当前请求日期晚 {abs(days_since)} 天。"
        )
    else:
        report_timing_text = (
            f"当前日期是{today_time}，{stock_name}于{latest_meta.release_date.isoformat()}发布了最近季度的财报，"
            f"现在{today_time}距离财报发布时间已经过去了{days_since}天。"
        )

    appendix = f"\n\n---\n{report_timing_text}{price_sentence}\n{next_sentence}\n"
    metadata_text = report_timing_text + price_sentence
    if next_meta:
        metadata_text += f" 距离下一季度正式财报发行日预告还有{next_gap_val}天（参考 {next_date_str} ）, 公司也可能在这个正式日期之前提前发布财报预告，请注意查看公告新闻系统"
    else:
        metadata_text += " 距离下一季度财报发行日预告还有未知天数。"

    return content.rstrip() + appendix, metadata_text


def get_financial_report_summary(symbol: str, today_time: str, *, report_release_slack_days: int = 0) -> dict:
    stock_code = symbol.strip()
    if is_cn_etf_symbol(stock_code):
        message = {"error": "ETF/基金类标的没有季度财报摘要数据，请选择股票标的。", "stock": stock_code}
        logger.info("get_financial_report_summary ETF 预检测: %s", stock_code)
        return message
    try:
        symbol_info = parse_symbol(stock_code)
    except Exception as exc:
        message = {"error": f"无法解析股票代码: {symbol}", "details": str(exc)}
        logger.exception("parse_symbol 失败: %s", symbol)
        return message

    stock_name = symbol_info.stock_name
    logger.info("get_financial_report_summary 请求: %s (%s), today=%s", stock_name, symbol_info.symbol, today_time)

    try:
        today_dt = datetime.strptime(today_time, "%Y-%m-%d")
    except ValueError:
        message = {"error": f"无法解析 today_time: {today_time}", "hint": "格式需为 YYYY-MM-DD"}
        logger.error("解析 today_time 失败: %s", today_time)
        return message

    cn_profit_forecast_md = _format_cn_profit_forecast_sections(symbol_info)
    hk_profit_forecast_md = _format_hk_profit_forecast_sections(symbol_info, today_time)
    consensus_blocks = [part for part in (cn_profit_forecast_md, hk_profit_forecast_md) if part]
    consensus_md = "\n\n---\n\n".join(consensus_blocks).strip()
    forecast_text, forecast_path = _load_latest_forecast_markdown(symbol_info, today_dt)

    files = _list_report_files(symbol_info)
    if not files:
        message = {"error": "未找到财报目录或文件", "stock": f"{stock_name} ({stock_code})"}
        if consensus_md or forecast_text:
            path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
            parts: List[str] = []
            if consensus_md:
                parts.append(consensus_md)
            if forecast_text:
                parts.append(
                    "## 未来预期（forecast）\n"
                    + (path_hint + "\n" if path_hint else "")
                    + "\n"
                    + forecast_text
                )
            message["content"] = "\n\n---\n\n".join(parts) + "\n\n> 未找到财报目录或文件。\n"
            message["forecast_path"] = str(forecast_path) if forecast_path else None
        logger.error("财报目录为空: %s", STOCK_BASE_DIR / f"{stock_name}_{stock_code}" / "financial_reports")
        return message

    report_selection_cutoff = today_dt.date() + timedelta(days=max(0, int(report_release_slack_days or 0)))

    try:
        latest_meta, next_meta = _select_reports(
            files,
            today_dt,
            available_cutoff_date=report_selection_cutoff,
        )
    except FileNotFoundError as exc:
        message = {"error": str(exc), "stock": f"{stock_name} ({stock_code})"}
        if consensus_md or forecast_text:
            path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
            parts: List[str] = []
            if consensus_md:
                parts.append(consensus_md)
            if forecast_text:
                parts.append(
                    "## 未来预期（forecast）\n"
                    + (path_hint + "\n" if path_hint else "")
                    + "\n"
                    + forecast_text
                )
            message["content"] = "\n\n---\n\n".join(parts) + f"\n\n> {exc}\n"
            message["forecast_path"] = str(forecast_path) if forecast_path else None
        logger.warning("财报选择失败: %s", exc)
        return message

    try:
        content = latest_meta.path.read_text(encoding="utf-8")
    except OSError as exc:
        message = {"error": f"读取财报失败: {exc}", "path": str(latest_meta.path)}
        if consensus_md or forecast_text:
            path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
            parts: List[str] = []
            if consensus_md:
                parts.append(consensus_md)
            if forecast_text:
                parts.append(
                    "## 未来预期（forecast）\n"
                    + (path_hint + "\n" if path_hint else "")
                    + "\n"
                    + forecast_text
                )
            message["content"] = "\n\n---\n\n".join(parts) + f"\n\n> 读取财报失败：{exc}\n"
            message["forecast_path"] = str(forecast_path) if forecast_path else None
        logger.exception("读取财报失败: %s", latest_meta.path)
        return message

    if consensus_md:
        content = consensus_md + "\n\n---\n\n" + content.lstrip()
    if forecast_text:
        path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
        forecast_section = (
            "\n\n---\n\n## 未来预期（forecast）\n"
            + (path_hint + "\n" if path_hint else "")
            + "\n"
            + forecast_text
            + "\n"
        )
        content = content.rstrip() + forecast_section

    release_price, today_price, change_pct = _compute_price_drift(symbol_info, today_time, latest_meta.release_date)
    enriched_content, metadata = _decorate_content(
        content,
        stock_name,
        today_time,
        latest_meta,
        next_meta,
        release_price,
        today_price,
        change_pct,
    )

    result = {
        "stock": f"{stock_name} ({stock_code})",
        "today": today_time,
        "report_selection_cutoff": report_selection_cutoff.isoformat(),
        "report_release_slack_days": max(0, int(report_release_slack_days or 0)),
        "report_path": str(latest_meta.path),
        "forecast_path": str(forecast_path) if forecast_path else None,
        "content": enriched_content,
        "metadata": metadata,
    }
    logger.info(
        "get_financial_report_summary 成功: report=%s, days_since=%s",
        latest_meta.path.name,
        (datetime.strptime(today_time, "%Y-%m-%d").date() - latest_meta.release_date).days,
    )
    return result


__all__ = ["ReportMeta", "get_financial_report_summary"]
