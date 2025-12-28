import warnings

# 过滤 py_mini_racer / pkg_resources 在 Python 3.12+ 启动时的弃用告警，避免刷屏
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"pkg_resources")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"py_mini_racer")

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from fastmcp import FastMCP

# fastmcp 在导入时会调用 simplefilter("default"), 需要再次插入忽略规则
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"pkg_resources")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"py_mini_racer")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from basic_stock_info import basic_info  # type: ignore
from enhanced_pe_pb_analyzer import EnhancedPEPBAnalyzer  # type: ignore
from logging_utils import init_tool_logger
from configs.stock_pool import TRACKED_A_STOCKS
from stock_price_dynamics_summarizer import stock_price_dynamics_summarizer
from utlity import (
    resolve_base_dir,
    parse_symbol,
    SymbolInfo,
    is_cn_etf_symbol,
    get_last_trading_day,
)  # type: ignore

mcp = FastMCP("StockAnalysis")
logger = init_tool_logger("stock_analysis")

SYMBOL_NAME_MAP = {entry.symbol: entry.name for entry in TRACKED_A_STOCKS}
DEFAULT_INDEX_SYMBOL = os.getenv("PRICE_DYNAMICS_INDEX", "000001.IDX")
ANNOUNCEMENT_KEYWORDS: Tuple[str, ...] = ("质押", "回购")
ANNOUNCEMENT_TIME_FIELDS: Tuple[str, ...] = (
    "datetime",
    "date",
    "公告时间",
    "公告日期",
    "timestamp",
)


def _resolve_stock_name(symbol: str) -> str:
    name = SYMBOL_NAME_MAP.get(symbol)
    return (name if isinstance(name, str) else symbol).strip() or symbol


def _is_index_symbol(symbol_info: SymbolInfo) -> bool:
    return symbol_info.market == "CN_INDEX" or symbol_info.symbol.endswith(".IDX")


def _parse_symbols(symbols: Union[str, List[str]]) -> List[str]:
    if isinstance(symbols, str):
        cleaned = [s.strip() for s in symbols.replace("\n", ",").split(",") if s.strip()]
    else:
        cleaned = [s.strip() for s in symbols if isinstance(s, str) and s.strip()]
    if not cleaned:
        raise ValueError("symbols 参数不能为空")
    return cleaned


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
    """Return the previous trading day's midnight timestamp for the symbol's calendar."""
    requested = _parse_datetime(today_time)
    last_trade_date = get_last_trading_day(requested.date(), symbol_info.calendar)
    return datetime.combine(last_trade_date, datetime.min.time())


def _parse_announcement_datetime(entry: Dict[str, Any]) -> Optional[datetime]:
    """尝试解析公告/新闻条目的日期字段，支持多种键名。"""
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


def _retain_latest_for_keyword(
    items: List[Dict[str, Any]],
    keyword: str,
) -> Tuple[List[Dict[str, Any]], int]:
    """保留指定关键字的最新公告，返回过滤后的列表及移除数量。"""
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
        dt_value = _parse_announcement_datetime(item)
        if dt_value is None:
            continue
        if latest_dt is None or dt_value > latest_dt:
            latest_dt = dt_value
            latest_idx = idx

    if len(matched_indices) <= 1:
        return items, 0

    keep_idx = latest_idx if latest_idx is not None else fallback_idx
    if keep_idx is None:
        keep_idx = matched_indices[0]
    filtered: List[Dict[str, Any]] = []
    removed = 0
    for idx, item in enumerate(items):
        title = str(item.get("title") or "")
        if keyword in title and idx != keep_idx:
            removed += 1
            continue
        filtered.append(item)
    return filtered, removed


def _apply_announcement_keyword_filters(payload: Any, trace: str = "root") -> None:
    """
    遍历任意嵌套结构，针对包含 title 的列表执行关键词去重，只保留最新一条。
    """
    if payload is None:
        return
    if isinstance(payload, dict):
        for key, value in payload.items():
            next_trace = f"{trace}.{key}" if trace else key
            _apply_announcement_keyword_filters(value, next_trace)
        return
    if isinstance(payload, list):
        if not payload:
            return
        if all(isinstance(item, dict) and "title" in item for item in payload):
            working = list(payload)
            mutated = False
            for keyword in ANNOUNCEMENT_KEYWORDS:
                working, removed = _retain_latest_for_keyword(working, keyword)
                if removed:
                    logger.info(
                        "公告关键词去重: path=%s, keyword=%s, 移除%d条，仅保留最近披露",
                        trace or "<list>",
                        keyword,
                        removed,
                    )
                    mutated = True
            if mutated:
                payload[:] = working
            for idx, item in enumerate(payload):
                next_trace = f"{trace}[{idx}]" if trace else f"[{idx}]"
                _apply_announcement_keyword_filters(item, next_trace)
            return
        for idx, item in enumerate(payload):
            next_trace = f"{trace}[{idx}]" if trace else f"[{idx}]"
            _apply_announcement_keyword_filters(item, next_trace)


"""暂时下线 get_basic_stock_info MCP tool，保留函数以便后续恢复。
@mcp.tool()
def get_basic_stock_info(
    symbols: Union[str, List[str]],
    today_time: str,
) -> Dict[str, Any]:

    parsed_symbols = _parse_symbols(symbols)
    parsed_today = _parse_datetime(today_time)
    price_lookback_days = 420
    force_refresh = False
    force_refresh_financials = False
    base_dir: Optional[str] = None

    logger.info(
        "get_basic_stock_info 请求: count=%d, today=%s",
        len(parsed_symbols),
        parsed_today.date().isoformat(),
    )
    try:
        result = basic_info(
            parsed_symbols,
            base_dir=base_dir,
            today_time=parsed_today.to_pydatetime(),
            price_lookback_days=price_lookback_days,
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
        )
    except Exception:
        logger.exception("get_basic_stock_info 执行失败")
        raise
    logger.info(
        "get_basic_stock_info 完成: stocks=%d, errors=%d",
        len(result.get("stocks", {})),
        len(result.get("errors", {})),
    )
    return result
"""


# 保留函数以供内部及调试脚本调用；如需重新开放 MCP 接口，可恢复下方注解。
# @mcp.tool()
def run_enhanced_pe_pb_analysis(
    symbol: str,
    today_time: str,
) -> Dict[str, Any]:
    """
    返回指定股票的PE/PB/PS/PEG和历史PE信息，及相似股票PE/PB估值比较分析数据，
    用于指定公司的当前估值分析。为避免泄露回测日当天尚未收盘的数据，工具内部会自动返回离today_time之前最近一个交易日的数据。

    **注意**：ETF/基金及指数（IDX）类标的无财报估值指标，不适用本工具；
    针对此类资产仅可使用 `summarize_stock_price_dynamics`。

    Args:
        symbol (str): 目标股票代码，必须包含市场后缀。
            格式要求: CODE.SUFFIX
            示例: "600276.SH"

        today_time: 当前日期（YYYY-MM-DD）。工具会回退到该日期之前最近的一个
            交易日生成分析，以避免使用当日未收盘价格。

    Returns:
        Dict[str, Any]: 结构化的分析结果，包含：
            - symbol: 股票代码
            - stock_name: 股票名称  
            - analysis_date: 分析执行日期
            - report: 完整的JSON估值报告内容，包括：
                * 基础信息（代码、名称、行业）
                * 当前估值指标（PE、PB、PS、PEG）
                * 历史估值区间（最高/最低/分位数）
                * 净利润增长率数据
                * 相似股票对比分析
    """
    if not symbol:
        raise ValueError("symbol 参数不能为空")
    symbol = symbol.strip()
    symbol_info = parse_symbol(symbol)
    if is_cn_etf_symbol(symbol) or _is_index_symbol(symbol_info):
        logger.info("run_enhanced_pe_pb_analysis 检测到ETF/指数标的，直接返回空结果: %s", symbol)
        return {
            "error": "ETF/指数标的不支持 PE/PB 分析，请选择股票标的。",
            "symbol": symbol,
        }
    stock_name = symbol_info.stock_name or _resolve_stock_name(symbol_info.symbol)

    base_dir: Optional[str] = None
    force_refresh = False
    force_refresh_financials = False

    analysis_timestamp = _resolve_effective_trade_datetime(symbol_info, today_time)
    logger.info(
        "run_enhanced_pe_pb_analysis 请求: symbol=%s, stock_name=%s, today=%s (effective=%s)",
        symbol,
        stock_name,
        today_time,
        analysis_timestamp.strftime("%Y-%m-%d"),
    )

    try:
        analyzer = EnhancedPEPBAnalyzer(
            base_dir=base_dir,
            analysis_datetime=analysis_timestamp,
        )
        analyzer.analyze_stock(
            symbol_info,
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
        )

        paths = analyzer._prepare_paths(symbol_info)  # type: ignore[attr-defined]
        stock_root = paths["root"]
        analysis_dir = paths["analysis"]
        date_suffix = analysis_timestamp.strftime("%Y%m%d")

        json_file = analysis_dir / f"{stock_root.name}_{date_suffix}_enhanced_pe_analysis.json"
        csv_file = analysis_dir / f"{stock_root.name}_{date_suffix}_pe_comparison_table.csv"
        md_file = analysis_dir / f"{stock_root.name}_{date_suffix}_enhanced_pe_analysis.md"

        if not json_file.exists():
            raise FileNotFoundError(f"未找到生成的JSON报告: {json_file}")
        if not md_file.exists():
            raise FileNotFoundError(f"未找到生成的Markdown报告: {md_file}")

        report_json = json.loads(json_file.read_text(encoding="utf-8"))
        meta = report_json.get("meta", {})
        analysis_date = meta.get("analysis_date") or meta.get("analysis_time")

        result = {
            "symbol": symbol_info.symbol,
            "stock_name": stock_name,
            "analysis_date": analysis_date,
            "report": report_json,
            "analysis_markdown": md_file.read_text(encoding="utf-8"),
            "json_path": str(json_file),
        }
    except Exception:
        logger.exception("run_enhanced_pe_pb_analysis 执行失败")
        raise

    logger.info(
        "run_enhanced_pe_pb_analysis 完成: json=%s, markdown=%s",
        result["json_path"],
        md_file,
    )
    # 调用方历史依赖 report-only 响应，这里回落到兼容字段
    return {
        "target_symbols": result["symbol"],
        "stock_name": result["stock_name"],
        "analysis_date": result["analysis_date"],
        "report": result["report"],
        "analysis_markdown": result["analysis_markdown"],
    }


# 保留函数以供内部及 CLI 调用；默认不直接暴露为 MCP 工具，防止 AI 直接调可调参数。
# @mcp.tool()
def summarize_stock_price_dynamics(
    symbol: str,
    today_time: str,
    start_date: Optional[str] = None,
    long_term_start_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    返回指定symbol的最新价格动态（支持普通股票、ETF 以及 IDX 指数；其它 MCP
    工具均 **不支持** ETF/指数，仅此工具可用）。为避免泄露回测日当天尚未收盘的数据，工具内部会自动返回离today_time之前最近一个交易日的数据。

    具体指标包括：
    1、目标股票和相似股票价格动态总结(3、6、12个月的累计回报率、夏普比率、波动率、最大回撤)
    2、目标股票和相似股票相关性矩阵
    3、目标股票技术指标(收盘价、MACD、RSI(14)、涨跌幅、换手率、成交量)和行业ETF收盘价(返回从start_date开始到today_time的每日技术指标)
    4、目标股票最新移动平均线指标
    5、目标股票最近的月度均值收盘价(统计从long_term_start_date开始算起，到today_time中间每个月的平均收盘价)

    Args:：
        symbol (str): 目标股票代码，必须包含市场后缀。
            格式要求: CODE.SUFFIX
            示例: "600276.SH"
        start_date: 短期分析起始日，格式（YYYY-MM-DD），建议与 today_time 相差 ≤20 天，缺省时默认取 today_time 向前 14 天。
        today_time: 当前日期，格式（YYYY-MM-DD），必填，工具会回退到该日期之前最近的交易日并将其作为 end_date。
        long_term_start_date: 长期分析起始日（YYYY-MM-DD），缺省时取 today_time 向前 3 年（约 1095 天）。
    """
    if not symbol:
        raise ValueError("symbol 参数不能为空")
    symbol = symbol.strip()
    symbol_info = parse_symbol(symbol)
    is_index = _is_index_symbol(symbol_info)
    is_etf = is_cn_etf_symbol(symbol)
    similar_enabled = not (is_index or is_etf)
    stock_name = symbol_info.stock_name or _resolve_stock_name(symbol_info.symbol)

    effective_timestamp = _resolve_effective_trade_datetime(symbol_info, today_time)
    reference_date = effective_timestamp.date()
    end_date_str = reference_date.strftime("%Y-%m-%d")

    logger.info(
        "summarize_stock_price_dynamics 请求: symbol=%s, start=%s, today=%s (effective=%s)",
        symbol,
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
            logger.info(
                "start_date %s 晚于最新可用交易日 %s，已自动对齐",
                start_dt.strftime("%Y-%m-%d"),
                reference_date,
            )
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

    index_symbol_str = DEFAULT_INDEX_SYMBOL.strip()
    try:
        index_symbol_info: SymbolInfo = parse_symbol(index_symbol_str)
    except Exception as exc:
        raise ValueError(f"指数代码 {index_symbol_str} 无法解析: {exc}") from exc
    force_refresh = False
    data_root = resolve_base_dir(None)
    try:
        results = stock_price_dynamics_summarizer(
            symbolsInfo=[symbol_info],
            index_symbolInfo=index_symbol_info,
            start_date=start_str,
            end_date=end_date_str,
            long_term_start_date=long_term,
            top_n_similar=2 if similar_enabled else 0,
            base_dir=str(data_root),
            force_refresh=force_refresh,
            only_find_similar=False,
            force_refresh_financials=False,
        )
    except Exception:
        logger.exception("summarize_stock_price_dynamics 执行失败")
        raise
    target_key = symbol_info.symbol
    if not results or target_key not in results:
        raise RuntimeError(f"未能生成 {target_key} 的价格动态分析")
    entry = results[target_key]
    json_path_val = entry.get("json_path")
    json_path = Path(json_path_val) if json_path_val else None
    if not json_path or not json_path.exists():
        raise FileNotFoundError(f"未找到生成的价格动态 JSON 报告: {json_path}")
    report_json = json.loads(json_path.read_text(encoding="utf-8"))
    markdown_path_val = entry.get("markdown_path")
    markdown_path = str(markdown_path_val) if markdown_path_val else None

    summary = {
        "symbol": target_key,
        "stock_name": stock_name,
        "analysis_date": entry.get("analysis_date"),
        "similar_stocks": entry.get("similar_stocks") if similar_enabled else [],
        "report": report_json,
    }
    logger.info(
        "summarize_stock_price_dynamics 完成: json=%s",
        json_path,
    )
    return summary


@mcp.tool()
def analyze_stock_dynamics_and_valuation(symbol: str, today_time: str) -> Dict[str, Any]:
    """
    
    返回指定symbol的最新价格动态指标、PE及历史PE,PB,PS估值等指标，用于查看symbol对应股票近期股价情况和长期估值情况
    注：上证指数的股票代码symbol是：000001.IDX
    具体指标包括：
    1、目标股票和相似股票价格动态总结(3、6、12个月的累计回报率、夏普比率、波动率、最大回撤)
    2、目标股票和相似股票相关性矩阵
    3、目标股票技术指标(收盘价、MACD、RSI(14)、涨跌幅、换手率、成交量)和行业ETF收盘价(返回today_time前14日的每日技术指标)
    4、目标股票最新移动平均线指标
    5、目标股票最近的月度均值收盘价(统计从today_time开始前一年间每个月的平均收盘价)
    6、PE及历史PE,PB,PS估值指标
    7、(仅指数) 市场环境定量诊断得分 (Level 1 技术面 + Level 2 位置)

    Args:
    symbol (str): 目标股票代码，必须包含市场后缀。
        格式要求: CODE.SUFFIX
        示例: "600276.SH"

    today_time: 当前日期（YYYY-MM-DD）。工具会回退到该日期之前最近的一个
        交易日生成分析，以避免使用当日未收盘价格。
    """

    if not symbol:
        raise ValueError("symbol 参数不能为空")

    normalized_symbol = symbol.strip()
    symbol_info = parse_symbol(normalized_symbol)
    stock_name = symbol_info.stock_name or _resolve_stock_name(symbol_info.symbol)
    logger.info(
        "analyze_stock_dynamics_and_valuation 请求: symbol=%s, today=%s",
        symbol_info.symbol,
        today_time,
    )

    price_analysis = summarize_stock_price_dynamics(
        symbol=symbol_info.symbol,
        today_time=today_time,
    )

    valuation_analysis: Optional[Dict[str, Any]] = None
    valuation_reason: Optional[str] = None
    if is_cn_etf_symbol(symbol_info.symbol) or _is_index_symbol(symbol_info):
        valuation_reason = "ETF/指数标的不支持估值分析"
    else:
        valuation_analysis = run_enhanced_pe_pb_analysis(
            symbol=symbol_info.symbol,
            today_time=today_time,
        )

    response: Dict[str, Any] = {
        "symbol": price_analysis.get("symbol", symbol_info.symbol),
        "stock_name": price_analysis.get("stock_name", stock_name),
        "analysis_date": price_analysis.get("analysis_date"),
        #"price_analysis": price_analysis,
        "price_report": price_analysis.get("report"),
        # "valuation_analysis": valuation_analysis,
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


if __name__ == "__main__":
    # Bind HTTP server so the service health check can succeed
    port = int(os.getenv("ANALYSIS_HTTP_PORT", "8004"))
    mcp.run(transport="streamable-http", port=port)

    # summarize_stock_price_dynamics(symbol="000001.IDX", today_time="2025-12-15")
    # analyze_stock_dynamics_and_valuation(symbol="01810.HK", today_time="2025-12-18")
    # analyze_stock_dynamics_and_valuation(symbol="000001.IDX", today_time="2025-12-12")
