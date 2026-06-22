"""Stock analysis service implementation for the skill-only architecture."""

from __future__ import annotations

import json
import os
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.logging import init_tool_logger


warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"pkg_resources")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"py_mini_racer")

from enhanced_pe_pb_analyzer import EnhancedPEPBAnalyzer
from stock_price_dynamics_summarizer import stock_price_dynamics_summarizer
from utlity import SymbolInfo, get_latest_trading_day, is_etf_symbol, parse_symbol, resolve_base_dir


logger = init_tool_logger("stock_analysis")

DEFAULT_INDEX_SYMBOL = os.getenv("PRICE_DYNAMICS_INDEX", "000001.IDX")
ANNOUNCEMENT_KEYWORDS: Tuple[str, ...] = ("质押", "回购")
ANNOUNCEMENT_TIME_FIELDS: Tuple[str, ...] = (
    "datetime",
    "date",
    "公告时间",
    "公告日期",
    "timestamp",
)


def _resolve_stock_name(symbol_info: SymbolInfo) -> str:
    return (symbol_info.stock_name or symbol_info.symbol).strip() or symbol_info.symbol


def _is_index_symbol(symbol_info: SymbolInfo) -> bool:
    return symbol_info.market == "CN_INDEX" or symbol_info.symbol.endswith(".IDX")


def _parse_datetime(value: str) -> pd.Timestamp:
    if not value or not isinstance(value, str):
        raise ValueError("today_time 必须为非空字符串")
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"无法解析 today_time: {value}")
    if isinstance(parsed, pd.Timestamp) and parsed.tzinfo is not None:
        parsed = parsed.tz_convert(None)
    return parsed


def _resolve_effective_trade_datetime(symbol_info: SymbolInfo, today_time: str) -> datetime:
    requested = _parse_datetime(today_time)
    last_trade_date = get_latest_trading_day(requested.date(), symbol_info.calendar)
    return datetime.combine(last_trade_date, datetime.min.time())


def _parse_announcement_datetime(entry: Dict[str, Any]) -> Optional[datetime]:
    for field in ANNOUNCEMENT_TIME_FIELDS:
        raw_value = entry.get(field)
        if not raw_value:
            continue
        if isinstance(raw_value, datetime):
            return raw_value
        parsed = pd.to_datetime(raw_value, errors="coerce")
        if parsed is None or pd.isna(parsed):
            continue
        if isinstance(parsed, pd.Timestamp):
            if parsed.tzinfo is not None:
                parsed = parsed.tz_convert(None)
            return parsed.to_pydatetime()
        if isinstance(parsed, datetime):
            return parsed
    return None


def _retain_latest_for_keyword(items: List[Dict[str, Any]], keyword: str) -> Tuple[List[Dict[str, Any]], int]:
    matched_indices: List[int] = []
    latest_idx: Optional[int] = None
    latest_dt: Optional[datetime] = None
    fallback_idx: Optional[int] = None

    for idx, item in enumerate(items):
        title = str(item.get("title") or "")
        if keyword not in title:
            continue
        matched_indices.append(idx)
        if fallback_idx is None:
            fallback_idx = idx
        parsed_dt = _parse_announcement_datetime(item)
        if parsed_dt is None:
            continue
        if latest_dt is None or parsed_dt > latest_dt:
            latest_idx = idx
            latest_dt = parsed_dt

    if len(matched_indices) <= 1:
        return items, 0

    keep_idx = latest_idx if latest_idx is not None else fallback_idx
    if keep_idx is None:
        return items, 0

    filtered: List[Dict[str, Any]] = []
    removed = 0
    for idx, item in enumerate(items):
        if idx in matched_indices and idx != keep_idx:
            removed += 1
            continue
        filtered.append(item)
    return filtered, removed


def _apply_announcement_keyword_filters(payload: Any, trace: str = "root") -> None:
    if isinstance(payload, dict):
        for key, value in list(payload.items()):
            if key == "news_items" and isinstance(value, list):
                filtered = value
                total_removed = 0
                for keyword in ANNOUNCEMENT_KEYWORDS:
                    filtered, removed = _retain_latest_for_keyword(filtered, keyword)
                    total_removed += removed
                if total_removed:
                    logger.info("公告关键字过滤完成: trace=%s, removed=%d", trace, total_removed)
                payload[key] = filtered
                continue
            next_trace = f"{trace}.{key}" if trace else key
            _apply_announcement_keyword_filters(value, next_trace)
        return

    if isinstance(payload, list):
        if payload and all(isinstance(item, dict) for item in payload):
            for idx, item in enumerate(payload):
                next_trace = f"{trace}[{idx}]" if trace else f"[{idx}]"
                _apply_announcement_keyword_filters(item, next_trace)
            return
        for idx, item in enumerate(payload):
            next_trace = f"{trace}[{idx}]" if trace else f"[{idx}]"
            _apply_announcement_keyword_filters(item, next_trace)


def run_enhanced_pe_pb_analysis(symbol: str, today_time: str) -> Dict[str, Any]:
    if not symbol:
        raise ValueError("symbol 参数不能为空")
    symbol_info = parse_symbol(symbol.strip())
    if is_etf_symbol(symbol_info.symbol) or _is_index_symbol(symbol_info):
        logger.info("run_enhanced_pe_pb_analysis 检测到ETF/指数标的，直接返回空结果: %s", symbol_info.symbol)
        return {"error": "ETF/指数标的不支持 PE/PB 分析，请选择股票标的。", "symbol": symbol_info.symbol}

    stock_name = _resolve_stock_name(symbol_info)
    analysis_timestamp = _resolve_effective_trade_datetime(symbol_info, today_time)
    logger.info(
        "run_enhanced_pe_pb_analysis 请求: symbol=%s, stock_name=%s, today=%s (effective=%s)",
        symbol_info.symbol,
        stock_name,
        today_time,
        analysis_timestamp.strftime("%Y-%m-%d"),
    )

    analyzer = EnhancedPEPBAnalyzer(base_dir=None, analysis_datetime=analysis_timestamp)
    analyzer.analyze_stock(symbol_info, force_refresh=False, force_refresh_financials=False)

    paths = analyzer._prepare_paths(symbol_info)  # type: ignore[attr-defined]
    stock_root = paths["root"]
    analysis_dir = paths["analysis"]
    date_suffix = analysis_timestamp.strftime("%Y%m%d")

    json_file = analysis_dir / f"{stock_root.name}_{date_suffix}_enhanced_pe_analysis.json"
    md_file = analysis_dir / f"{stock_root.name}_{date_suffix}_enhanced_pe_analysis.md"

    if not json_file.exists():
        raise FileNotFoundError(f"未找到生成的JSON报告: {json_file}")
    if not md_file.exists():
        raise FileNotFoundError(f"未找到生成的Markdown报告: {md_file}")

    report_json = json.loads(json_file.read_text(encoding="utf-8"))
    meta = report_json.get("meta", {})
    analysis_date = meta.get("analysis_date") or meta.get("analysis_time")

    logger.info("run_enhanced_pe_pb_analysis 完成: json=%s, markdown=%s", json_file, md_file)
    return {
        "target_symbols": symbol_info.symbol,
        "stock_name": stock_name,
        "analysis_date": analysis_date,
        "report": report_json,
        "analysis_markdown": md_file.read_text(encoding="utf-8"),
    }


def summarize_stock_price_dynamics(
    symbol: str,
    today_time: str,
    start_date: Optional[str] = None,
    long_term_start_date: Optional[str] = None,
) -> Dict[str, Any]:
    if not symbol:
        raise ValueError("symbol 参数不能为空")

    symbol_info = parse_symbol(symbol.strip())
    is_index = _is_index_symbol(symbol_info)
    is_etf = is_etf_symbol(symbol_info.symbol)
    similar_enabled = not (is_index or is_etf)
    stock_name = _resolve_stock_name(symbol_info)

    effective_timestamp = _resolve_effective_trade_datetime(symbol_info, today_time)
    reference_date = effective_timestamp.date()
    end_date_str = reference_date.strftime("%Y-%m-%d")

    logger.info(
        "summarize_stock_price_dynamics 请求: symbol=%s, start=%s, today=%s (effective=%s)",
        symbol_info.symbol,
        start_date,
        today_time,
        end_date_str,
    )

    if start_date:
        start_dt = pd.to_datetime(start_date, errors="coerce")
        if pd.isna(start_dt):
            raise ValueError("start_date 必须为 YYYY-MM-DD 格式（允许省略前导0）")
        if getattr(start_dt, "tz", None):
            start_dt = start_dt.tz_convert(None)
        if start_dt.date() > reference_date:
            logger.info("start_date %s 晚于最新可用交易日 %s，已自动对齐", start_dt.strftime("%Y-%m-%d"), reference_date)
            start_dt = pd.Timestamp(reference_date)
        if (reference_date - start_dt.date()).days > 31:
            logger.warning(
                "start_date (%s) 距 today_time (%s) 超过 31 天，建议缩短窗口以节省上下文",
                start_dt.strftime("%Y-%m-%d"),
                reference_date,
            )
        start_str = start_dt.strftime("%Y-%m-%d")
    else:
        start_str = (reference_date - timedelta(days=14)).strftime("%Y-%m-%d")

    if long_term_start_date:
        long_term_dt = pd.to_datetime(long_term_start_date, errors="coerce")
        if pd.isna(long_term_dt):
            raise ValueError("long_term_start_date 必须为 YYYY-MM-DD 格式（允许省略前导0）")
        if getattr(long_term_dt, "tz", None):
            long_term_dt = long_term_dt.tz_convert(None)
        if long_term_dt.date() > reference_date:
            logger.info(
                "long_term_start_date %s 晚于最新可用交易日 %s，已自动对齐",
                long_term_dt.strftime("%Y-%m-%d"),
                reference_date,
            )
            long_term_dt = pd.Timestamp(reference_date)
        long_term = long_term_dt.strftime("%Y-%m-%d")
    else:
        long_term = (reference_date - timedelta(days=365 * 3)).strftime("%Y-%m-%d")

    index_symbol_info = parse_symbol(DEFAULT_INDEX_SYMBOL.strip())
    data_root = resolve_base_dir(None)
    results = stock_price_dynamics_summarizer(
        symbolsInfo=[symbol_info],
        index_symbolInfo=index_symbol_info,
        start_date=start_str,
        end_date=end_date_str,
        long_term_start_date=long_term,
        top_n_similar=2 if similar_enabled else 0,
        base_dir=str(data_root),
        force_refresh=False,
        only_find_similar=False,
        force_refresh_financials=False,
    )

    target_key = symbol_info.symbol
    if not results or target_key not in results:
        raise RuntimeError(f"未能生成 {target_key} 的价格动态分析")

    entry = results[target_key]
    json_path_val = entry.get("json_path")
    json_path = Path(json_path_val) if json_path_val else None
    if not json_path or not json_path.exists():
        raise FileNotFoundError(f"未找到生成的价格动态 JSON 报告: {json_path}")

    report_json = json.loads(json_path.read_text(encoding="utf-8"))
    logger.info("summarize_stock_price_dynamics 完成: json=%s", json_path)
    return {
        "symbol": target_key,
        "stock_name": stock_name,
        "analysis_date": entry.get("analysis_date"),
        "similar_stocks": entry.get("similar_stocks") if similar_enabled else [],
        "report": report_json,
    }


def analyze_stock_dynamics_and_valuation(symbol: str, today_time: str) -> Dict[str, Any]:
    if not symbol:
        raise ValueError("symbol 参数不能为空")

    symbol_info = parse_symbol(symbol.strip())
    stock_name = _resolve_stock_name(symbol_info)
    logger.info("analyze_stock_dynamics_and_valuation 请求: symbol=%s, today=%s", symbol_info.symbol, today_time)

    price_analysis = summarize_stock_price_dynamics(symbol=symbol_info.symbol, today_time=today_time)

    valuation_analysis: Optional[Dict[str, Any]] = None
    valuation_reason: Optional[str] = None
    if is_etf_symbol(symbol_info.symbol) or _is_index_symbol(symbol_info):
        valuation_reason = "ETF/指数标的不支持估值分析"
    else:
        valuation_analysis = run_enhanced_pe_pb_analysis(symbol=symbol_info.symbol, today_time=today_time)

    response: Dict[str, Any] = {
        "symbol": price_analysis.get("symbol", symbol_info.symbol),
        "stock_name": price_analysis.get("stock_name", stock_name),
        "analysis_date": price_analysis.get("analysis_date"),
        "price_report": price_analysis.get("report"),
        "valuation_report": (valuation_analysis or {}).get("analysis_markdown"),
    }
    if valuation_reason:
        response["valuation_report"] = None
        response["valuation_unavailable_reason"] = valuation_reason

    _apply_announcement_keyword_filters(response, trace="analysis_response")
    logger.info(
        "analyze_stock_dynamics_and_valuation 完成: symbol=%s, has_valuation=%s",
        response["symbol"],
        valuation_analysis is not None,
    )
    return response


__all__ = [
    "run_enhanced_pe_pb_analysis",
    "summarize_stock_price_dynamics",
    "analyze_stock_dynamics_and_valuation",
]
