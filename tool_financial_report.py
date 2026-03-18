#!/usr/bin/env python
"""财报 Markdown 摘要读取工具
==============================

该 MCP 工具读取 `data/stock_info/{stock_name}_{stock_code}/financial_reports/`
目录下由研究员人工整理的 Markdown 财报摘要，并返回最接近（且早于）
`today_time` 的最新一份报告，同时追加股价变化提示及下一次财报的预估时间差。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from fastmcp import FastMCP

project_root = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(project_root))

from agent_tools.logging_utils import init_tool_logger
from configs.stock_pool import TRACKED_A_STOCKS
from shared_data_access import SharedDataAccess
from shared_data_access.cache_registry import (
    CacheKind,
    build_cache_dir,
    update_cn_profit_forecast_cached,
    update_hk_profit_forecast_cached,
)
from utlity import SymbolInfo, parse_symbol, get_stock_data_dir, is_cn_etf, is_cn_etf_symbol

load_dotenv()

mcp = FastMCP("FinancialReportSummary")
logger = init_tool_logger("financial_report")

CUSTOM_STOCK_DIR = os.getenv("STOCK_DATA_DIR")
STOCK_BASE_DIR = (
    Path(CUSTOM_STOCK_DIR).resolve() if CUSTOM_STOCK_DIR else Path("data/stock_info").resolve()
)
SYMBOL_NAME_MAP = {entry.symbol: entry.name for entry in TRACKED_A_STOCKS}
PRICE_CLOSE_COLUMNS = ("收盘", "close", "Close", "收盘价", "CLOSE")
HK_FORECAST_FILE = "profit_forecast.csv"
CN_FORECAST_FILE = "profit_forecast.csv"
FORECAST_DIR_NAME = "forecast"


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
        base = STOCK_BASE_DIR
        return base / f"{stock_name}_{symbol_info.symbol}"
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
YEAR_IN_TEXT_PATTERN = re.compile(r"(20\d{2})")

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

def _repair_mojibake(text: str) -> str:
    """
    修复常见的“UTF-8 被按 latin-1 误解码”导致的中文乱码。
    例如：'è´¢æ”¿å¹´åº¦' -> '财政年度'
    """
    if not text or not isinstance(text, str):
        return text
    # 快速过滤：如果已经包含中文，认为正常。
    if any("\u4e00" <= ch <= "\u9fff" for ch in text):
        return text
    try:
        fixed = text.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return text
    # 仅在修复后出现中文时采用，避免误伤正常英文/数字字段
    if any("\u4e00" <= ch <= "\u9fff" for ch in fixed):
        return fixed
    return text


def _repair_mojibake_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """对 DataFrame 的列名与字符串单元格做中文乱码修复（轻量）。"""
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work.columns = [_repair_mojibake(str(c)) for c in work.columns]
    for col in work.columns:
        if work[col].dtype == object:
            work[col] = work[col].apply(lambda v: _repair_mojibake(v) if isinstance(v, str) else v)
    return work


def _extract_year(value: object) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = YEAR_IN_TEXT_PATTERN.search(text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


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

    return ReportMeta(
        path=path,
        release_date=release_dt,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
    )


def _select_reports(files: List[Path], today_dt: datetime) -> Tuple[ReportMeta, Optional[ReportMeta]]:
    metas = [meta for meta in (_parse_report_meta(p) for p in files) if meta]
    if not metas:
        raise FileNotFoundError("未能解析任何财报文件名。")

    metas.sort(
        key=lambda m: (
            m.release_date,
            m.fiscal_year,
            m.fiscal_quarter,
            m.stem,
        ),
        reverse=True,
    )

    latest_past = next((m for m in metas if m.release_date < today_dt.date()), None)
    if latest_past is None:
        raise FileNotFoundError(f"没有早于 today_time:{today_dt} 的财报文件。")

    future_candidates = sorted(
        [m for m in metas if m.release_date > today_dt.date()],
        key=lambda m: m.release_date,
    )
    next_future = future_candidates[0] if future_candidates else None
    return latest_past, next_future


def _load_latest_forecast_markdown(symbol_info: SymbolInfo, today_dt: datetime) -> tuple[str, Optional[Path]]:
    """读取 forecast/ 下 latest_past 的 Markdown（早于 today_time 的最近一份）。"""

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
    accessor = SharedDataAccess(logger=logger)
    dataset = accessor.prepare_dataset(symbolInfo=symbol_info, as_of_date=today_time)
    frame = dataset.prices.frame
    if frame.empty:
        return None, None, None
    release_price = _get_price_on_or_after(frame, release_day)
    today_price = _get_price_on_or_before(frame, datetime.strptime(today_time, "%Y-%m-%d").date())
    if release_price is None or today_price is None or release_price == 0:
        return release_price, today_price, None
    pct = (today_price - release_price) / release_price * 100
    return release_price, today_price, pct


def _load_hk_profit_forecast(symbol_info: SymbolInfo) -> pd.DataFrame:
    """读取港股机构盈利预测缓存；若缺失则尝试触发一次刷新（通过 shared_data_access 链路）。"""

    if not symbol_info.is_hk_market():
        return pd.DataFrame()

    # 统一入口：由缓存函数自行判断 TTL/是否需要刷新
    df = update_hk_profit_forecast_cached(
        symbol_info,
        base_data_dir="data",
        force_refresh=False,
        logger=logger,
    )
    if df is not None and not df.empty:
        return df

    cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.HK_PROFIT_FORECAST,
        base_dir=None,
        ensure=True,
    )
    csv_path = cache_dir / HK_FORECAST_FILE
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()


def _load_cn_profit_forecast(symbol_info: SymbolInfo) -> pd.DataFrame:
    """读取A股机构盈利预测缓存；若缺失则尝试触发一次刷新（通过 shared_data_access 链路）。"""

    if not symbol_info.is_cn_market():
        return pd.DataFrame()
    if symbol_info.market == "CN_INDEX" or is_cn_etf(symbol_info):
        return pd.DataFrame()

    df = update_cn_profit_forecast_cached(
        symbol_info,
        base_data_dir="data",
        force_refresh=False,
        logger=logger,
    )
    if df is not None and not df.empty:
        return df

    cache_dir = build_cache_dir(
        symbol_info,
        CacheKind.CN_PROFIT_FORECAST,
        base_dir=None,
        ensure=True,
    )
    csv_path = cache_dir / CN_FORECAST_FILE
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()


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


def _format_hk_profit_forecast_sections(symbol_info: SymbolInfo, today_time: str) -> str:
    """生成港股机构预测的 Task2/Task3 Markdown 片段（仅最新10条）。"""

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

    years = sorted({y for y in work["财政年度"].dropna().tolist() if isinstance(y, int)})
    if not years:
        return ""

    # 优先遵循原始需求：筛选 2025 财年；若没有则自动回退到“最接近当前的未来财年”。
    preferred_year = 2025
    if preferred_year in years:
        target_year = preferred_year
    else:
        try:
            today_year = datetime.strptime(today_time, "%Y-%m-%d").year
        except Exception:
            today_year = datetime.now().year
        future_years = [y for y in years if y >= today_year]
        target_year = min(future_years) if future_years else max(years)

    filtered = work.loc[(work["财政年度"] == target_year) & work["目标价"].notna()].copy()
    filtered = filtered.sort_values(by="更新日期", ascending=False)
    top = filtered.head(10).copy()
    if top.empty:
        return ""

    top["更新日期"] = top["更新日期"].dt.strftime("%Y-%m-%d")

    cols = [c for c in ("更新日期", "证券商", "评级", "目标价") if c in top.columns]
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
        rating_dist_lines = [f"- {k}: {int(v)}" for k, v in counts.items()]

    lines: List[str] = []
    lines.append(f"### 【任务2】筛选{target_year}财年且有目标价的最新10条预测：")
    lines.append(_render_markdown_table(rows, cols))
    lines.append("")
    lines.append("### 【任务3】目标价统计分析：")
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


def _format_cn_profit_forecast_sections(symbol_info: SymbolInfo) -> str:
    """生成A股机构一致预期（同花顺汇总）的Markdown片段。"""

    if not symbol_info.is_cn_market():
        return ""
    if symbol_info.market == "CN_INDEX" or is_cn_etf(symbol_info):
        return ""

    df = _load_cn_profit_forecast(symbol_info)
    if df.empty:
        return ""

    # 常见列：预测指标 + 若干年度实际值/预测均值
    if "预测指标" not in df.columns:
        return ""

    work = df.copy()
    work["预测指标"] = work["预测指标"].astype(str)
    # “市盈率(动态)”属于同花顺基于“股价/市值不变”假设推算的派生值，容易误导，默认不输出。
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

    columns = [col for col in filtered.columns if col != "预测指标"]
    # 保持原始列顺序，最多输出到 2027（避免过长）
    columns = columns[:8]
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
    if change_pct is None:
        price_sentence = (
            "由于缺少有效的价格数据，暂时无法计算财报发布日至今的股价变动，"
            "请结合行情自行评估市场是否已经消化该信息。"
        )
    else:
        direction = "上涨" if change_pct >= 0 else "下跌"
        price_sentence = (
            f"当前股价距离财报发行日已经{direction}了{abs(change_pct):.2f}%，"
            "请自己评估当前市场是否已充分定价该财报带来的影响。"
        )

    if next_meta:
        next_gap = (next_meta.release_date - today_dt).days
        next_sentence = (
            f"距离下一季度财报发行日预告还有{next_gap}天（参考文件日期 {next_meta.release_date.isoformat()}）。"
        )
        next_gap_val: Optional[int] = next_gap
        next_date_str = next_meta.release_date.isoformat()
    else:
        next_sentence = "距离下一季度财报发行日预告还有未知。"
        next_gap_val = None
        next_date_str = None

    appendix = (
        f"\n\n---\n"
        f"当前日期是{today_time}，{stock_name}于{latest_meta.release_date.isoformat()}发布了最近季度的财报，"
        f"现在{today_time}距离财报发布时间已经过去了{days_since}天，{price_sentence}\n"
        f"{next_sentence}\n"
    )

    metadata_text = (
        f"当前日期是{today_time}，{stock_name}于{latest_meta.release_date.isoformat()}发布了最近季度的财报，"
        f"现在{today_time}距离财报发布时间已经过去了{days_since}天，{price_sentence}"
    )
    if next_meta:
        metadata_text += f" 距离下一季度正式财报发行日预告还有{next_gap_val}天（参考 {next_date_str} ）, 公司也可能在这个正式日期之前提前发布财报预告，请注意查看公告新闻系统"
    else:
        metadata_text += " 距离下一季度财报发行日预告还有未知天数。"

    return content.rstrip() + appendix, metadata_text


@mcp.tool()
def get_financial_report_summary(symbol: str, today_time: str) -> dict:
    """
    返回指定A股和港股股票在 today_time 之前最近一期财报 Markdown 及附加指标。
    ETF/基金类标的不具备财报摘要，不适用本工具。
    另外，如果存在 `data/stock_info/<name_symbol>/forecast/YYYYMMDD.md`，
    将在财报摘要后拼接最近一期（早于 today_time）的“未来预期/行业预测”内容。

    Args:
        symbol: 股票代码（CODE.SUFFIX），如 "601877.SH"。
        today_time: 当前日期，格式 YYYY-MM-DD。

    Returns:
        dict: {
            "stock": "...",
            "today": "YYYY-MM-DD",
            "report_path": ".../financial_reports/xxx.md",
            "content": "<Markdown + 附加提示>",
            "metadata": {...}
        }
    """
    stock_code = symbol.strip()
    if is_cn_etf_symbol(stock_code):
        message = {
            "error": "ETF/基金类标的没有季度财报摘要数据，请选择股票标的。",
            "stock": stock_code,
        }
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

    hk_forecast_md = _format_hk_profit_forecast_sections(symbol_info, today_time)
    cn_forecast_md = _format_cn_profit_forecast_sections(symbol_info)
    forecast_blocks = [part for part in (cn_forecast_md, hk_forecast_md) if part]
    forecast_md = "\n\n---\n\n".join(forecast_blocks).strip()

    files = _list_report_files(symbol_info)
    if not files:
        message = {
            "error": "未找到财报目录或文件",
            "stock": f"{stock_name} ({stock_code})",
            "today": today_time,
        }
        # 即使找不到财报，也尽量把 forecast/ 的内容输出出去（如果有）
        forecast_text, forecast_path = _load_latest_forecast_markdown(symbol_info, today_dt)
        if forecast_text:
            path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
            message["content"] = (
                (forecast_md + "\n\n---\n\n" if forecast_md else "")
                + "## 未来预期（forecast）\n"
                + (path_hint + "\n" if path_hint else "")
                + "\n"
                + forecast_text
                + "\n\n> 未找到财报目录或文件。\n"
            )
            message["forecast_path"] = str(forecast_path) if forecast_path else None
        elif forecast_md:
            message["content"] = forecast_md + "\n\n> 未找到财报目录或文件。\n"
        logger.error("财报目录为空: %s", STOCK_BASE_DIR / f"{stock_name}_{stock_code}" / "financial_reports")
        return message

    try:
        latest_meta, next_meta = _select_reports(files, today_dt)
    except FileNotFoundError as exc:
        message = {
            "error": str(exc),
            "stock": f"{stock_name} ({stock_code})",
            "today": today_time,
        }
        forecast_text, forecast_path = _load_latest_forecast_markdown(symbol_info, today_dt)
        if forecast_text:
            path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
            message["content"] = (
                (forecast_md + "\n\n---\n\n" if forecast_md else "")
                + "## 未来预期（forecast）\n"
                + (path_hint + "\n" if path_hint else "")
                + "\n"
                + forecast_text
                + f"\n\n> {exc}\n"
            )
            message["forecast_path"] = str(forecast_path) if forecast_path else None
        elif forecast_md:
            message["content"] = forecast_md + f"\n\n> {exc}\n"
        logger.warning("财报选择失败: %s", exc)
        return message

    try:
        content = latest_meta.path.read_text(encoding="utf-8")
    except OSError as exc:
        message = {
            "error": f"读取财报失败: {exc}",
            "path": str(latest_meta.path),
            "stock": f"{stock_name} ({stock_code})",
            "today": today_time,
        }
        forecast_text, forecast_path = _load_latest_forecast_markdown(symbol_info, today_dt)
        if forecast_text:
            path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
            message["content"] = (
                (forecast_md + "\n\n---\n\n" if forecast_md else "")
                + "## 未来预期（forecast）\n"
                + (path_hint + "\n" if path_hint else "")
                + "\n"
                + forecast_text
                + f"\n\n> 读取财报失败：{exc}\n"
            )
            message["forecast_path"] = str(forecast_path) if forecast_path else None
        elif forecast_md:
            message["content"] = forecast_md + f"\n\n> 读取财报失败：{exc}\n"
        logger.exception("读取财报失败: %s", latest_meta.path)
        return message

    forecast_text, forecast_path = _load_latest_forecast_markdown(symbol_info, today_dt)
    if forecast_text:
        path_hint = f"- 文件: {forecast_path}" if forecast_path else ""
        forecast_section = (
            "\n\n---\n\n## 未来预期（forecast）\n"
            + (path_hint + "\n" if path_hint else "")
            + "\n"
            + forecast_text.strip()
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
    if forecast_md:
        enriched_content = forecast_md + "\n\n---\n\n" + enriched_content

    result = {
        "stock": f"{stock_name} ({stock_code})",
        "today": today_time,
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


if __name__ == "__main__":
    port = int(os.getenv("FIN_REPORT_HTTP_PORT", "8008"))
    mcp.run(transport="streamable-http", port=port)

    # result = get_financial_report_summary("09988.HK", "2026-01-13")
    # print(result)
