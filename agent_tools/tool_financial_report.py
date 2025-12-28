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
    返回指定股票在 today_time 之前最近一期财报 Markdown 及附加指标。
    ETF/基金类标的不具备财报摘要，不适用本工具。

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

    files = _list_report_files(symbol_info)
    if not files:
        message = {"error": "未找到财报目录或文件", "stock": f"{stock_name} ({stock_code})"}
        logger.error("财报目录为空: %s", STOCK_BASE_DIR / f"{stock_name}_{stock_code}" / "financial_reports")
        return message

    try:
        latest_meta, next_meta = _select_reports(files, today_dt)
    except FileNotFoundError as exc:
        message = {"error": str(exc), "stock": f"{stock_name} ({stock_code})"}
        logger.warning("财报选择失败: %s", exc)
        return message

    try:
        content = latest_meta.path.read_text(encoding="utf-8")
    except OSError as exc:
        message = {"error": f"读取财报失败: {exc}", "path": str(latest_meta.path)}
        logger.exception("读取财报失败: %s", latest_meta.path)
        return message

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
        "report_path": str(latest_meta.path),
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
