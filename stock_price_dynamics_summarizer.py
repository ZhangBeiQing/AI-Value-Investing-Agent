f"""Stock Price Dynamics Summarizer
===================================

本文件是整个项目用于“价格动态分析”的主脚本，其定位与 MCP 工具不同：

* **核心用途**：给研究员 / 回测脚本批量生成指定时间段内的行情、相关性、技术指标、
  Markdown 报告等本地缓存文件。它以 `start_date` 为起点，以 `end_date` 为终点，当`end_date`
  不指定时，自动使用当前日期datetime.datetime.now()作为end_date
  并生成完整的 `CSV + Markdown` 资料库供人工/AI进行股票分析。
* **数据范围**：短期股票数据由 `start_date`/`end_date` 控制，长期价格（用于月度平均、
  技术指标等）由 `long_term_start_date` 到“end_date”覆盖；当调用者未指定 start_date 时，
  默认取最近7个交易日的短期数据，并默认拉取最近3年的收盘价序列
* **缓存策略**：所有数据写入 `data/stock_name_symbol/analysis/` 等目录，并依照end_date
  命名文件（例如 `.../price_dynamics_summary_20251031.csv`）。缓存系统采用智能日期处理机制：
  - **交易日识别**：使用 `get_latest_trading_day()` 函数自动识别最近的交易日，确保缓存文件
    命名基于实际交易日而非日历日期
  - **节假日处理**：当 `end_date` 落在周末或节假日时，系统自动使用前一个交易日的缓存数据
  - **缓存验证**：通过 `manage_cache_with_cleanup()` 函数验证缓存数据的时间范围，支持容错天数
    （默认3天）来处理日期边界情况
  - **过期清理**：自动清理非当日的历史缓存文件，保持存储空间高效
  - **智能刷新**：当请求日期与缓存日期不匹配时，系统会检查日期差异：如果缓存日期是周五或节假日前
    一天，而当前 `end_date` 是周末或节假日，则沿用现有缓存；否则重新获取数据
* **主要入口**：`stock_price_dynamics_summarizer(...)`。该函数接受目标股票、指数配置、
  日期范围、相似股票数量等参数，返回一个以目标股票为 key 的结果字典，其中包含：
  - `summary`：累计收益/夏普/波动率/最大回撤等汇总表
  - `correlation`：与指数、相似股票的相关系数矩阵
  - `price_data`：合并后的时序数据（收盘价、成交量等）
  - `technical_indicators`：MACD、RSI、波动率等指标
  - `markdown_path`：自动合成的 Markdown 报告路径（存档于 `analysis/` 与
    `data/0_transaction_package/`）
  - `analysis_date`：生成报告时的真实日期（YYYYMMDD）
* **设计规则 / 限制**：
  1. **数据完整性优先**：若缓存不足，会自动下发 API 请求，并为不同市场自动选取正确的
     Akshare 接口。失败会记录日志，但仍返回已有内容。
  2. **无“历史 today” 概念**：start/end/long_term 参数完全由调用者控制。若 end_date 超出
     当前atetime.datetime.now()真实日期，则自动截断为今天并记录警告。
  3. **Markdown 合并**：`merge_csv_to_markdown` 会聚合 CSV 成单份报告，内容包括价格动态
     总结、相关性矩阵、技术指标表等；命名规则为
     `stock_name_symbol_股票分析报告_end_date.md`。
  4. **相似股票**：基于 `get_similar_stocks` 的配置/规则，若无可用记录则返回空列表。
* **扩展指引**：
  - 如需新增指标，只需在 `_PRICE_MODULE.calculate_metrics` 或
    `calculate_technical_indicators` 中扩展列，再在 Markdown 汇总逻辑里追加。
  - 若想在 MCP 工具中施加“today_time”或“start_date 限制”等回测规则，应在工具层实现，
    不在本脚本中引入，以便该脚本继续服务于真实时间的人工分析。

以下代码部分保持原有结构：先封装 API 抓取/缓存工具，再实现批量分析、Markdown 生成、
以及命令行入口。"""

import json
import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
from datetime import datetime
from pathlib import Path
import os
import argparse
from typing import List, Dict, Any, Optional, Tuple, Iterable
import shutil  # 导入shutil模块用于文件复制
import logging
import sys

from indicator_library import IndicatorBatchRequest, IndicatorLibrary, IndicatorSpec
from indicator_library.calculators.risk import return_metrics_indicator
from indicator_library.gateways import DataFrameGateway

# 从utlity包导入缓存与股票工具
from utlity import (
    ensure_stock_subdir,
    get_latest_trading_day,
    get_next_trading_day,
    is_trading_day,
    parse_symbol,
    resolve_base_dir,
    SymbolInfo,
    is_cn_etf,
)
from utlity.get_similar_stocks import get_similar_stocks

# 设置pandas选项以避免FutureWarning
pd.set_option('future.no_silent_downcasting', True)

# API调用延迟配置（秒）
API_DELAY = 0.5  # 建议1秒延迟，既能避免频率限制又不会过度影响性能
# 配置独立的日志系统（非MCP工具）
LOG_ENV_KEY = "STOCK_ANALYZER_LOG_FILE"


def setup_main_logger(log_level=logging.INFO):
    """配置主脚本日志系统，保证整个进程复用同一个日志文件。"""

    logger = logging.getLogger('StockAnalyzer')
    if logger.handlers:
        logger.setLevel(log_level)
        return logger

    log_dir = Path('logs') / 'main_scripts' / 'StockAnalyzer'
    log_dir.mkdir(parents=True, exist_ok=True)

    existing_path = os.environ.get(LOG_ENV_KEY)
    if existing_path:
        log_filename = Path(existing_path)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = (log_dir / f'stock_analyzer_{timestamp}.log').resolve()
        os.environ[LOG_ENV_KEY] = str(log_filename)

    logger.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"主脚本日志系统初始化完成，日志文件: {log_filename}")
    return logger

# 初始化全局logger
logger = setup_main_logger()

class DataValidationError(RuntimeError):
    """关键行情数据缺失或无效时抛出，避免产生错误结论。"""
    pass


RESERVED_CACHE_FILES = {".cache_registry_meta.json"}


def cleanup_output_directory(directory: Path, keep: Optional[Iterable[str]] = None) -> None:
    """清理输出目录，只保留缓存元数据文件。"""
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    keep_names = set(RESERVED_CACHE_FILES)
    if keep:
        keep_names.update(keep)
    for entry in target.iterdir():
        if entry.name in keep_names:
            continue
        try:
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink(missing_ok=True)
        except Exception as exc:  # pragma: no cover - 容错日志
            logger.warning("清理目录 %s 时跳过 %s: %s", target, entry, exc)




def calculate_technical_indicators(
    data: pd.DataFrame,
    symbolInfo: SymbolInfo,
    similar_names: List[str] | None = None,
    etf_name: str | None = None,
    period: int | None = None,
    long_term_period: int | None = None,
    short_term_start_date: str | None = None,
) -> pd.DataFrame:
    """生成目标股票的所有技术指标和统计数据（由共享指标库驱动）。"""
    _ = similar_names, period, long_term_period
    if data.empty:
        return pd.DataFrame()
    working = data.copy()
    if not isinstance(working.index, pd.DatetimeIndex):
        working.index = pd.to_datetime(working.index)
    working = working.sort_index()
    target_prefix = (symbolInfo.stock_name or symbolInfo.symbol).strip() or symbolInfo.symbol
    target_close_col = f"{target_prefix}_收盘"
    if target_close_col not in working.columns:
        logger.error("目标列 %s 不存在，无法计算技术指标", target_close_col)
        return pd.DataFrame()

    gateway = DataFrameGateway(working)
    indicator_lib = IndicatorLibrary(gateway=gateway, logger=logger)
    start_date = working.index.min().date()
    end_date = working.index.max().date()

    try:
        batch = indicator_lib.calculate(
            IndicatorBatchRequest(
                symbolInfo=symbolInfo,
                start_date=start_date,
                end_date=end_date,
                specs=[
                    IndicatorSpec(
                        name="price_snapshot",
                        params={"symbolInfo": symbolInfo, "include_turnover": True},
                        alias="price_snapshot",
                    ),
                    IndicatorSpec(
                        name="pct_change",
                        params={"column_name": "涨跌幅(%)"},
                        alias="pct_change",
                    ),
                    IndicatorSpec(
                        name="macd",
                        params={"column_name": "MACD"},
                        alias="macd",
                    ),
                    IndicatorSpec(
                        name="rsi",
                        params={"period": 14, "column_name": "RSI(14)"},
                        alias="rsi",
                    ),
                ],
                price_fields=["收盘", "成交量", "成交额", "换手率"],
            )
        )
    except Exception as exc:
        logger.error("调用 IndicatorLibrary 计算技术指标失败: %s", exc)
        return pd.DataFrame()

    # 合并技术指标表格
    tables: List[pd.DataFrame] = []
    for key in ("price_snapshot", "pct_change", "macd", "rsi"):
        frame = batch.tabular.get(key)
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            tables.append(frame)
    technical_indicators = pd.concat(tables, axis=1) if tables else pd.DataFrame(index=working.index)

    # 添加ETF收盘价（如果存在）
    if etf_name:
        etf_col = f"{etf_name}_收盘"
        if etf_col in working.columns:
            technical_indicators[f"ETF({etf_name})收盘价"] = working[etf_col]
        else:
            matches = [col for col in working.columns if col.endswith('_收盘') and etf_name in col]
            if matches:
                technical_indicators[f"ETF({etf_name})收盘价"] = working[matches[0]]

    # 处理技术指标数据
    for col in technical_indicators.columns:
        if col != "收盘价" and (not etf_name or col != f"ETF({etf_name})收盘价"):
            technical_indicators[col] = technical_indicators[col].ffill().infer_objects(copy=False)

    # 根据短期开始日期过滤数据
    if short_term_start_date:
        mask = technical_indicators.index >= pd.to_datetime(short_term_start_date)
        technical_indicators = technical_indicators.loc[mask]

    return technical_indicators

# 相似股票管理辅助函数
LEGACY_SIMILAR_STOCKS_PATH = Path(__file__).resolve().with_name("similar_stocks.csv")


# 主函数：股票价格动态总结
def stock_price_dynamics_summarizer(
                                   symbolsInfo: List[SymbolInfo],
                                   index_symbolInfo: SymbolInfo,
                                   start_date: str = None, end_date: str = None,
                                   long_term_start_date: str = None,
                                   top_n_similar: int = 2,
                                   base_dir: str = 'data',
                                   force_refresh: bool = False,
                                   only_find_similar: bool = False,
                                   force_refresh_financials: bool = False) -> Dict[str, Any]:
    """生成目标股票的价格动态总结
    
    参数:
        symbolsInfo: 股票信息列表，每个元素为 SymbolInfo 类型
        index_info: 指数信息字典，键为指数代码，值为指数名称
        start_date: 开始日期，格式为YYYY-MM-DD，默认取 end_date 向前7个交易日
        end_date: 结束日期，格式为YYYY-MM-DD，默认为今天（自动截断到最近交易日）
        long_term_start_date: 长期收盘价的开始日期，格式为YYYY-MM-DD，默认为None（使用 end_date 向前三年）
        index_code: 指数代码，默认为上证指数
        top_n_similar: 每只股票获取的相似股票数量
        base_dir: 数据存储的基础目录
        force_refresh: 是否强制刷新数据，不使用缓存
        only_find_similar: 如果为True，则只执行相似股票查找，不获取数据和计算指标
        
    返回:
        包含各种分析结果的字典，键为股票代码
    """
    # 处理日期参数
    today = pd.Timestamp(datetime.now().date().strftime("%Y-%m-%d"))
    if end_date is None:
        end_dt = today
    else:
        end_dt = pd.to_datetime(end_date, format="%Y-%m-%d", errors="coerce")
    
    if start_date is None:
        start_dt = end_dt - BDay(14)
    else:
        start_dt = pd.to_datetime(start_date, format="%Y-%m-%d", errors="coerce")

    if start_dt > end_dt:
        raise ValueError("start_date 需要早于或等于 end_date")
    
    # 处理长期日期参数
    if long_term_start_date is None:
        long_term_start_dt = end_dt - pd.DateOffset(years=3)
    else:
        long_term_start_dt = pd.to_datetime(
            long_term_start_date, format="%Y-%m-%d", errors="coerce"
        )
    if pd.isna(long_term_start_dt):
        raise ValueError("long_term_start_date 无法解析为有效日期")
    if getattr(long_term_start_dt, "tzinfo", None) is not None:
        long_term_start_dt = long_term_start_dt.tz_convert(None)
    long_term_start_dt = pd.Timestamp(long_term_start_dt)

    end_date = end_dt.strftime("%Y-%m-%d")
    start_date = start_dt.strftime("%Y-%m-%d")
    long_term_start_date = long_term_start_dt.strftime("%Y-%m-%d")
    analysis_date_str = datetime.now().strftime("%Y-%m-%d")
    
    # 计算长期和短期的时间跨度（以天为单位）
    short_term_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
    long_term_days = (pd.to_datetime(end_date) - pd.to_datetime(long_term_start_date)).days
    
    # 转换为交易日数量（粗略估计：一年约250个交易日）
    short_term_period = int(short_term_days * 250 / 365)
    long_term_period = int(long_term_days * 250 / 365)
    
    
    # 存储每只股票的分析结果
    results = {}
    
    # 一个字典来存储所有股票的名称信息，包括目标股票、指数和相似股票
    all_stock_names = {}
    all_stock_names[index_symbolInfo.symbol] = index_symbolInfo.stock_name

    # 对每只目标股票进行分析
    for symbolInfo in symbolsInfo:
        logger.info(f"\n分析股票: {symbolInfo.stock_name} ({symbolInfo.symbol})")
        etf_mode = is_cn_etf(symbolInfo) or symbolInfo.market == "CN_INDEX"
        if etf_mode:
            logger.info("检测到 ETF/指数标的，跳过相似股票与相关性分析，仅输出自身指标。")
        
        all_stock_names[symbolInfo.symbol] = symbolInfo.stock_name
        effective_trading_day = get_latest_trading_day(
            end_dt.date(), symbolInfo.calendar, logger=logger
        )
        report_date = effective_trading_day.strftime("%Y%m%d")

        similar_stocks_symbolInfo: List[SymbolInfo] = []
        similar_names: List[str] = []
        similar_symbols: List[str] = []
        if not etf_mode:
            similar_stocks_info = get_similar_stocks(symbolInfo, base_dir)[:top_n_similar]
            structured_similars: List[Tuple[SymbolInfo, str]] = []
            for similar_stock_info in similar_stocks_info:
                code = similar_stock_info.get("code")
                name = (similar_stock_info.get("name") or "").strip()
                if not code:
                    logger.warning("相似股票[%s]缺少代码，跳过: %s", name or "Unknown", similar_stock_info)
                    continue
                try:
                    similar_info = parse_symbol(code)
                except Exception as exc:
                    logger.warning("解析相似股票代码 %s 失败: %s", code, exc)
                    continue
                structured_similars.append((similar_info, name or similar_info.symbol))
            similar_stocks_symbolInfo = [item[0] for item in structured_similars]
            similar_names = [item[1] for item in structured_similars]
            similar_symbols = [item[0].symbol for item in structured_similars]

            # 更新股票名称字典
            for info, name in zip(similar_stocks_symbolInfo, similar_names):
                all_stock_names[info.symbol] = name

            logger.info(f"相似股票: {[f'{name}({info.symbol})' for name, info in zip(similar_names, similar_stocks_symbolInfo)]}")
        
        # 如果只需要查找相似股票，则跳过后续步骤
        if only_find_similar:
            results[symbolInfo.symbol] = {
                'similar_stocks': similar_symbols,
                'similar_names': similar_names,
            }
            continue

        # 使用SharedDataAccess获取数据
        from shared_data_access.data_access import SharedDataAccess
        
        data_access = SharedDataAccess(
            base_dir=base_dir,
            logger=logger
        )
        
        # 获取目标股票/指数数据
        logger.info(f"正在获取{symbolInfo.stock_name} {symbolInfo.symbol}数据...")
        target_dataset = data_access.prepare_dataset(
            symbolInfo=symbolInfo,
            as_of_date=end_date,
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
        )
        target_df = target_dataset.prices.frame.copy()
        target_df = target_df.sort_index()
        
        # 获取指数数据（如果目标不是指数）
        index_df = pd.DataFrame()
        if symbolInfo.market != "CN_INDEX":
            logger.info(f"正在获取指数{index_symbolInfo.stock_name} {index_symbolInfo.symbol}数据...")
            index_dataset = data_access.prepare_dataset(
                symbolInfo=index_symbolInfo,
                as_of_date=end_date,
                force_refresh=force_refresh,
                force_refresh_financials=force_refresh_financials,
            )
            index_df = index_dataset.prices.frame.copy()
            index_df = index_df.sort_index()
        
        # 获取相似股票数据
        prefixed_similar_dfs: List[pd.DataFrame] = []
        column_symbol_map: Dict[str, str] = {}
        etf_display_name: Optional[str] = None
        if similar_stocks_symbolInfo:
            logger.info("正在获取相似股票数据...")
            for similar_info, similar_name in zip(similar_stocks_symbolInfo, similar_names):
                try:
                    similar_dataset = data_access.prepare_dataset(
                        symbolInfo=similar_info,
                        as_of_date=end_date,
                        force_refresh=force_refresh,
                        force_refresh_financials=force_refresh_financials,
                    )
                    similar_df = similar_dataset.prices.frame.copy().sort_index()
                except Exception as e:
                    logger.warning(
                        "获取相似股票%s的完整数据失败，尝试仅加载价格: %s",
                        similar_info.symbol,
                        e,
                    )
                    try:
                        price_bundle = data_access._load_price_bundle(  # type: ignore[attr-defined]
                            similar_info,
                            pd.to_datetime(end_date),
                        )
                        similar_df = price_bundle.frame.copy().sort_index()
                    except Exception as price_exc:
                        logger.warning(
                            "相似股票%s价格数据加载失败，跳过: %s",
                            similar_info.symbol,
                            price_exc,
                        )
                        continue
                if similar_df.empty:
                    continue
                similar_df.index = pd.to_datetime(similar_df.index)
                similar_df = similar_df[similar_df.index >= long_term_start_dt]
                safe_name = similar_name.strip()
                prefixed_df = similar_df.add_prefix(f"{safe_name}_")
                prefixed_similar_dfs.append(prefixed_df)
                for col in prefixed_df.columns:
                    column_symbol_map[col] = similar_info.symbol
                if etf_display_name is None and "ETF" in safe_name.upper():
                    etf_display_name = safe_name
        
        # 为每个数据源的列添加唯一前缀，避免列名冲突
        # 目标股票数据
        if not target_df.empty:
            target_df.index = pd.to_datetime(target_df.index)
            target_df = target_df[target_df.index >= long_term_start_dt]
            target_prefix = symbolInfo.stock_name.strip()
            target_df = target_df.add_prefix(f"{target_prefix}_")
            for col in target_df.columns:
                column_symbol_map[col] = symbolInfo.symbol
        
        # 指数数据
        if not index_df.empty:
            index_df.index = pd.to_datetime(index_df.index)
            index_df = index_df[index_df.index >= long_term_start_dt]
            index_prefix = index_symbolInfo.stock_name.strip()
            index_df = index_df.add_prefix(f"{index_prefix}_")
            for col in index_df.columns:
                column_symbol_map[col] = index_symbolInfo.symbol
        
        # 相似股票数据
        similar_dfs = pd.concat(prefixed_similar_dfs, axis=1) if prefixed_similar_dfs else pd.DataFrame()
        
        # 合并所有数据
        all_data = pd.concat([target_df, index_df, similar_dfs], axis=1)
        
        # 计算相关性矩阵 - 仅使用收盘价列
        close_cols = [
            col for col in all_data.columns
            if col.endswith('_收盘')
        ]
        close_df = all_data[close_cols].copy() if close_cols else pd.DataFrame()
        
        daily_returns = close_df.pct_change(fill_method=None) if not close_df.empty else pd.DataFrame()
        correlation_matrix = (
            daily_returns.corr(method='pearson', min_periods=30)
            if not daily_returns.empty else pd.DataFrame()
        )
        if etf_mode:
            correlation_matrix = pd.DataFrame()
        
        # 计算技术指标
        technical_indicators = calculate_technical_indicators(
            all_data,
            symbolInfo=symbolInfo,
            similar_names=similar_names,
            etf_name=etf_display_name,
            period=short_term_period,
            long_term_period=long_term_period,
            short_term_start_date=start_date
        )
        
        # 计算每个股票和指数的指标 - 仅使用收盘价列
        metrics: Dict[str, Dict[int, Dict[str, float]]] = {}
        symbol_close_col: Dict[str, str] = {}
        for column in close_cols:
            symbol_key = column_symbol_map.get(column)
            if not symbol_key:
                continue
            symbol_close_col.setdefault(symbol_key, column)
            series = close_df[column].dropna()
            if series.empty:
                continue
            metric_result = return_metrics_indicator(
                pd.DataFrame({"收盘": series}),
                windows=(63, 126, 252),
            )
            if metric_result:
                metrics[symbol_key] = metric_result
        
        # 所有股票代码（包括目标股票和指数）
        index_symbol_key = f'{index_symbolInfo.symbol}'
        if etf_mode:
            all_symbols = [symbolInfo.symbol]
        elif symbolInfo.market == "CN_INDEX" and symbolInfo.symbol == index_symbolInfo.symbol:
            all_symbols = [symbolInfo.symbol] + similar_symbols
        else:
            all_symbols = [symbolInfo.symbol, index_symbol_key] + similar_symbols
        
        # 为不同时间段创建结果DataFrame
        period_results = {}
        
        for period in [63, 126, 252]:
            period_months = period // 21
            
            # 创建带有股票名称和代码的索引
            index_with_names = [
                f"{all_stock_names.get(s, s.split('.')[0])}({s})" 
                for s in all_symbols
            ]
            
            # 初始化该时间段的DataFrame
            period_df = pd.DataFrame(index=index_with_names)
            
            metric_column_map = {
                "累计收益率(%)": f"{period_months}个月累计收益率(%)",
                "夏普比率": f"{period_months}个月夏普比率",
                "年化波动率(%)": f"{period_months}个月年化波动率(%)",
                "最大回撤(%)": f"{period_months}个月最大回撤(%)",
            }
            for metric_key, column_name in metric_column_map.items():
                period_df[column_name] = [
                    metrics.get(s, {}).get(period, {}).get(metric_key, 0.0)
                    for s in all_symbols
                ]
            
            period_results[f"{period_months}m"] = period_df
        
        # 合并所有时间段的结果
        summary_df = pd.concat(period_results.values(), axis=1)
        
        # 添加相关性指标
        correlations = []
        target_close_col = symbol_close_col.get(symbolInfo.symbol)
        for s in all_symbols:
            compare_col = symbol_close_col.get(s)
            if s == symbolInfo.symbol:
                correlations.append(1.0)
            elif (
                target_close_col
                and compare_col
                and target_close_col in correlation_matrix.columns
                and compare_col in correlation_matrix.columns
            ):
                correlations.append(correlation_matrix.loc[target_close_col, compare_col])
            else:
                correlations.append(np.nan)
        if not etf_mode:
            summary_df['与目标股票相关系数'] = correlations
        
        # 格式化
        for col in summary_df.columns:
            if any(keyword in col for keyword in ('收益率', '波动率', '回撤')):
                summary_df[col] = summary_df[col].round(1)
            elif '夏普比率' in col or '相关系数' in col:
                # 保持原有的数值列处理逻辑
                summary_df[col] = summary_df[col].round(3)
        
        # 保存分析结果
        analysis_dir = ensure_stock_subdir(symbolInfo, "analysis", base_dir)
        cleanup_output_directory(analysis_dir)

        summary_file = analysis_dir / f"price_dynamics_summary_{report_date}.csv"
        summary_df.to_csv(summary_file)

        corr_file = analysis_dir / f"correlation_matrix_{report_date}.csv"
        if not correlation_matrix.empty:
            correlation_matrix.to_csv(corr_file)
        elif corr_file.exists():
            corr_file.unlink(missing_ok=True)

        target_close_col = symbol_close_col.get(symbolInfo.symbol)
        long_term_price_data = pd.DataFrame()
        price_file = None
        if target_close_col and target_close_col in target_df.columns:
            long_term_price_data = target_df[[target_close_col]].copy()
            long_term_price_data.rename(columns={target_close_col: '收盘价'}, inplace=True)
            price_file = analysis_dir / f"close_price_{report_date}.csv"
            if not long_term_price_data.empty:
                long_term_price_data['收盘价'] = long_term_price_data['收盘价'].apply(
                    lambda x: round(x, 3) if pd.notnull(x) else x
                )
                long_term_price_data.to_csv(price_file, float_format='%.2f')
                logger.info(f"长期收盘价数据已保存到: {price_file}")
        else:
            logger.warning("未找到 %s 的收盘价列，无法导出长期收盘价数据", symbolInfo.symbol)

        tech_file = None
        if not technical_indicators.empty:
            tech_indicators = technical_indicators.copy()
            if '收盘价' in tech_indicators.columns and len(tech_indicators.columns) > 1:
                tech_indicators = tech_indicators.drop(columns=['收盘价'])

            if not tech_indicators.empty:
                tech_file = analysis_dir / f"technical_indicators_{report_date}.csv"
                tech_indicators.index = pd.to_datetime(tech_indicators.index).strftime('%Y-%m-%d')
                valid_indicators = tech_indicators.dropna(axis=1, how='all')

                if not valid_indicators.empty:
                    for col in valid_indicators.columns:
                        valid_indicators.loc[:, col] = valid_indicators[col].apply(
                            lambda x: round(x, 3) if pd.notnull(x) else x
                        )
                    valid_indicators.to_csv(tech_file, na_rep='', float_format='%.2f')
                    logger.info(f"短期技术指标已保存到: {tech_file} (从{start_date}开始)")
                else:
                    logger.warning("没有有效的技术指标可以保存")
                    
        logger.info(f"分析结果已保存到: {analysis_dir}")

        markdown_path = merge_csv_to_markdown(
            symbolInfo.symbol,
            symbolInfo.stock_name,
            analysis_dir,
            report_date,
            index_symbolInfo.symbol,
            index_symbolInfo.stock_name,
            start_date,
            long_term_start_date,
            is_etf=etf_mode,
        )

        # 将结果添加到字典
        results[symbolInfo.symbol] = {
            'summary': summary_df,
            'correlation': correlation_matrix,
            'price_data': all_data,
            'similar_stocks': similar_symbols,
            'similar_names': similar_names,
            'technical_indicators': technical_indicators,
            'markdown_path': markdown_path,
            'json_path': analysis_dir / f"{symbolInfo.stock_name}_{symbolInfo.symbol}_股票分析报告_{report_date}.json",
            'analysis_date': analysis_date_str,
            'report_date': report_date,
        }
    
    return results


# 将多个CSV文件合并为一个Markdown文件
def merge_csv_to_markdown(
    target_symbol: str,
    stock_name: str,
    analysis_dir: str,
    report_date: str,
    index_code: str = '000300',
    index_name: str = '沪深300',
    start_date: str = None,
    long_term_start_date: str | None = None,
    is_etf: bool = False,
) -> str:
    """将分析结果的所有CSV文件合并到一个Markdown文件中"""
    analysis_dir_path = Path(analysis_dir)
    base_data_dir = analysis_dir_path.parent.parent

    # 检查相关文件是否存在
    summary_file = analysis_dir_path / f"price_dynamics_summary_{report_date}.csv"
    corr_file = analysis_dir_path / f"correlation_matrix_{report_date}.csv"
    price_file = analysis_dir_path / f"close_price_{report_date}.csv"
    tech_file = analysis_dir_path / f"technical_indicators_{report_date}.csv"

    iso_report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:]}"
    json_payload: Dict[str, Any] = {
        "meta": {
            "stock_name": stock_name,
            "symbol": target_symbol,
            "analysis_date": iso_report_date,
            "report_date": report_date,
            "short_term_start_date": start_date,
            "long_term_start_date": long_term_start_date,
            "index": {"code": index_code, "name": index_name},
        },
        "summary_table": [],
        "correlation_matrix": [],
        "technical_section": {
            "table": [],
            "latest_focus": None,
            "ma_indicators": {},
            "start_date": start_date,
        },
        "monthly_section": {},
    }
    extreme_price_section: Dict[str, Any] = {}
    
    # 创建Markdown文件内容
    md_content = f"# {stock_name}({target_symbol}) 股票分析报告\n\n"
    md_content += f"分析日期: {iso_report_date}\n\n"
    
    # 添加价格动态总结
    if summary_file.exists():
        try:
            summary_df = pd.read_csv(summary_file, index_col=0)
            summary_df.index.name = "股票"
            if is_etf:
                md_content += "## 目标股票价格动态总结(3、6、12个月的累计回报率、夏普比率、波动率、最大回撤)\n\n"
            else:
                md_content += "## 目标股票和相似股票价格动态总结(3、6、12个月的累计回报率、夏普比率、波动率、最大回撤)\n\n"
            md_content += summary_df.to_markdown() + "\n\n"
            json_payload["summary_table"] = dataframe_to_table(
                summary_df, include_index=True, index_name="股票"
            )
        except Exception as e:
            md_content += f"读取价格动态总结文件时出错: {e}\n\n"
    
    # 添加相关性矩阵
    if (not is_etf) and corr_file.exists():
        try:
            corr_df = pd.read_csv(corr_file, index_col=0)
            md_content += "## 目标股票和相似股票相关性矩阵\n\n"
            md_content += corr_df.round(3).to_markdown() + "\n\n"
            json_payload["correlation_matrix"] = dataframe_to_table(
                corr_df, include_index=True, index_name="symbol"
            )
        except Exception as e:
            md_content += f"读取相关性矩阵文件时出错: {e}\n\n"
    
    tech_section = json_payload["technical_section"]
    # 添加技术指标 - 显示全部数据，并添加收盘价作为第一列
    if tech_file.exists() and price_file.exists():
        try:
            # 读取收盘价数据
            price_df = pd.read_csv(price_file, index_col=0)
            price_df.index = pd.to_datetime(price_df.index)
            min_price_date = price_df.index.min() if not price_df.empty else None
            
            # 读取技术指标数据
            tech_df = pd.read_csv(tech_file, index_col=0)
            tech_df.index = pd.to_datetime(tech_df.index)
            
            # 转换为标准日期格式
            price_df.index = price_df.index.strftime('%Y-%m-%d')
            tech_df.index = tech_df.index.strftime('%Y-%m-%d')
            
            # 只保留有收盘价的日期（即实际交易日）
            valid_dates = [idx for idx in price_df.index if not pd.isna(price_df.loc[idx, '收盘价'])]
            
            # 合并数据 - 使用更安全的方式，只使用有效交易日
            combined_df = pd.DataFrame(index=valid_dates) if valid_dates else pd.DataFrame()
            display_df = pd.DataFrame()
            if valid_dates:
                
                # 添加收盘价
                if '收盘价' in price_df.columns:
                    combined_df['收盘价'] = None
                    for idx in valid_dates:
                        combined_df.loc[idx, '收盘价'] = price_df.loc[idx, '收盘价']
                
                # 添加所有技术指标
                for col in tech_df.columns:
                    if col != '收盘价':  # 避免重复添加收盘价
                        combined_df[col] = None
                        for idx in tech_df.index:
                            if idx in combined_df.index:
                                combined_df.loc[idx, col] = tech_df.loc[idx, col]
                
                # 过滤只显示start_date之后的数据
                if start_date:
                    start_date_formatted = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                    # 确保索引是字符串类型，与start_date_formatted可比较
                    if combined_df.index.dtype != 'object':
                        combined_df.index = combined_df.index.astype(str)
                    combined_df = combined_df[combined_df.index >= start_date_formatted]
                
                # 确保所有列都存在
                expected_columns = ['收盘价', 'MACD', 'RSI(14)', '涨跌幅(%)', '成交额(亿元)', '成交量(万手)', '换手率(%)']
                
                for col in expected_columns:
                    if col not in combined_df.columns:
                        combined_df[col] = None
                
                # 添加ETF收盘价列（如果存在）
                etf_cols = [col for col in tech_df.columns if 'ETF' in col and '收盘价' in col]
                for col in etf_cols:
                    if col not in combined_df.columns:
                        combined_df[col] = None
                        for idx in tech_df.index:
                            if idx in combined_df.index:
                                combined_df.loc[idx, col] = tech_df.loc[idx, col]
                
                # 整理列的顺序
                ordered_columns = []
                if '收盘价' in combined_df.columns:
                    ordered_columns.append('收盘价')
                
                # 添加MACD主指标（不包括Signal和Hist）
                if 'MACD' in combined_df.columns:
                    ordered_columns.append('MACD')
                
                # 添加RSI
                if 'RSI(14)' in combined_df.columns:
                    ordered_columns.append('RSI(14)')
                
                # 添加涨跌幅、成交额/成交量和换手率
                if '涨跌幅(%)' in combined_df.columns:
                    ordered_columns.append('涨跌幅(%)')
                if '成交额(亿元)' in combined_df.columns:
                    ordered_columns.append('成交额(亿元)')
                if '换手率(%)' in combined_df.columns:
                    ordered_columns.append('换手率(%)')
                if '成交量(万手)' in combined_df.columns:
                    ordered_columns.append('成交量(万手)')
                
                # 添加ETF收盘价
                for col in etf_cols:
                    if col in combined_df.columns:
                        ordered_columns.append(col)
                
                # 添加任何可能遗漏的列
                for col in combined_df.columns:
                    if col not in ordered_columns:
                        ordered_columns.append(col)
                
                # 按新的列顺序重排
                if ordered_columns:
                    combined_df = combined_df[ordered_columns]
                
                # 按日期排序
                combined_df = combined_df.sort_index()
                
                # 过滤只显示start_date之后的数据
                if start_date:
                    start_date_formatted = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                    # 确保索引是字符串类型，与start_date_formatted可比较
                    if combined_df.index.dtype != 'object':
                        combined_df.index = combined_df.index.astype(str)
                    combined_df = combined_df[combined_df.index >= start_date_formatted]
                else:
                    start_date_formatted = None
                
                display_df = combined_df.drop(columns=['成交量(万手)'], errors='ignore')
                
                # 显示结果
                md_content += "## 技术指标和行业ETF收盘价\n\n"
                if combined_df.empty or display_df.empty:
                    if start_date:
                        md_content += f"*没有从 {start_date_formatted} 开始的技术指标数据*\n\n"
                        tech_section["note"] = f"没有从 {start_date_formatted} 开始的技术指标数据"
                    else:
                        md_content += "*没有技术指标数据*\n\n"
                        tech_section["note"] = "没有技术指标数据"
                else:
                    tech_section.pop("note", None)
                    # 获取最新日期（今天的数据）
                    latest_date = combined_df.index[-1] if not combined_df.empty else None
                    
                    # 创建自定义的Markdown表格，突出显示今天的数据
                    if latest_date and not display_df.empty:
                        # 构建表格头
                        headers = ['日期'] + list(display_df.columns)
                        md_content += '| ' + ' | '.join(headers) + ' |\n'
                        md_content += '|' + '|'.join(['---' for _ in headers]) + '|\n'
                        
                        table_df = display_df.reset_index().rename(columns={'index': '日期'})
                        tech_section['table'] = dataframe_to_table(table_df)
                        
                        # 构建表格行
                        for idx in display_df.index:
                            row_data = [str(idx)]
                            for col in display_df.columns:
                                value = display_df.loc[idx, col]
                                if pd.isna(value):
                                    row_data.append('')
                                elif isinstance(value, (int, float)):
                                    row_data.append(f'{value:.2f}')
                                else:
                                    row_data.append(str(value))
                            
                            # 如果是最新日期，添加粗体标记突出显示
                            if idx == latest_date:
                                row_data = [f'**{data}**' for data in row_data]
                                md_content += '| ' + ' | '.join(row_data) + ' | ← **今日数据** |\n'
                            else:
                                md_content += '| ' + ' | '.join(row_data) + ' |\n'
                        
                        md_content += '\n'
                        
                        # 添加今日数据说明
                        md_content += f"📈 **今日重点关注** ({latest_date}):\n\n"
                        latest_row = combined_df.loc[latest_date]
                        latest_focus: Dict[str, Any] = {}
                        for col in combined_df.columns:
                            value = latest_row[col]
                            if pd.notna(value):
                                if isinstance(value, (int, float)):
                                    md_content += f"- **{col}**: {value:.2f}\n"
                                else:
                                    md_content += f"- **{col}**: {value}\n"
                                latest_focus[col] = _normalize_cell_value(value)
                        tech_section["latest_focus"] = latest_focus
                        md_content += '\n'
                    else:
                        # 如果没有数据，使用默认表格
                        md_content += display_df.to_markdown() + "\n\n"
                        tech_section['table'] = dataframe_to_table(
                            display_df.reset_index().rename(columns={'index': '日期'})
                        )
                    
                    # 添加最新MA指标展示 - 使用长周期收盘价数据计算
                    ma_source = price_df['收盘价'].dropna() if '收盘价' in price_df.columns else pd.Series(dtype=float)
                    if ma_source.empty and not combined_df.empty and '收盘价' in combined_df.columns:
                        ma_source = combined_df['收盘价'].dropna()

                    if not ma_source.empty:
                        latest_date_ts = ma_source.index[-1]
                        latest_date = latest_date_ts.strftime('%Y-%m-%d') if isinstance(latest_date_ts, pd.Timestamp) else latest_date_ts
                        ma_indicators = {}

                        for period in [5, 10, 20, 60]:
                            window_series = ma_source.tail(period)
                            if len(window_series) < 2:
                                continue
                            ma_value = window_series.mean()
                            if pd.notna(ma_value):
                                ma_indicators[f'MA({period})'] = ma_value

                        if ma_indicators:
                            md_content += "### 最新移动平均线指标\n\n"
                            md_content += f"**日期**: {latest_date}\n\n"
                            for ma_name, ma_value in ma_indicators.items():
                                md_content += f"- **{ma_name}**: {ma_value:.2f}元\n"
                            md_content += "\n"
                            tech_section["ma_indicators"] = {
                                ma_name: round(float(ma_value), 4)
                                for ma_name, ma_value in ma_indicators.items()
                            }
            else:
                md_content += "## 技术指标和行业ETF收盘价\n\n"
                md_content += "*没有有效的交易日数据*\n\n"
                tech_section["note"] = "没有有效的交易日数据"
                
        except Exception as e:
            md_content += f"读取技术指标文件时出错: {e}\n\n"
            tech_section["note"] = f"读取技术指标文件时出错: {e}"
            
            # 如果合并失败，尝试只显示技术指标
            try:
                tech_df = pd.read_csv(tech_file, index_col=0)
                tech_df.index = pd.to_datetime(tech_df.index).strftime('%Y-%m-%d')
                
                # 过滤只显示start_date之后的数据
                if start_date:
                    start_date_formatted = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                    tech_df = tech_df[tech_df.index >= start_date_formatted]
                
                md_content += "## 技术指标和行业ETF收盘价\n\n"
                if tech_df.empty:
                    md_content += f"*没有从 {start_date_formatted} 开始的技术指标数据*\n\n"
                    tech_section["note"] = f"没有从 {start_date_formatted} 开始的技术指标数据"
                else:
                    md_content += tech_df.to_markdown() + "\n\n"
                    tech_section["table"] = dataframe_to_table(
                        tech_df.reset_index().rename(columns={'index': '日期'})
                    )
            except Exception as e2:
                md_content += f"再次尝试读取技术指标文件时出错: {e2}\n\n"
                tech_section["note"] = f"再次尝试读取技术指标文件时出错: {e2}"
    
    monthly_section: Dict[str, Any] = {}
    same_as_index = (
        (target_symbol or "").strip().upper()
        == (index_code or "").strip().upper()
    )
    # 添加收盘价数据 - 将每日收盘价转换为每月均值，并加入指数数据
    if price_file.exists():
        try:
            # 读取收盘价数据
            price_df = pd.read_csv(price_file, index_col=0)
            price_df.index = pd.to_datetime(price_df.index)

            # 获取最新的收盘价（用于支撑线/阻力线分析）
            latest_price = price_df['收盘价'].iloc[-1] if not price_df.empty else None
            monthly_section["latest_price"] = float(latest_price) if latest_price is not None else None

            # 计算股票每月均值
            # 计算月度均值 (resample按月结束日，这里简单取mean)
            monthly_price_df = price_df.resample('ME').mean().round(3)
            monthly_price_df.index = monthly_price_df.index.strftime('%Y-%m')
            monthly_price_display = monthly_price_df.tail(12)
            monthly_section["stock_monthly"] = dataframe_to_table(
                monthly_price_display.reset_index().rename(columns={'index': '日期'})
            )
            min_price_date = price_df.index.min() if not price_df.empty else None
            max_price_date = price_df.index.max() if not price_df.empty else None
            
            if same_as_index:
                md_content += "## 收盘价数据（月度均值）\n\n"
                md_content += monthly_price_display.to_markdown() + "\n\n"
            else:
                # 获取指数数据的文件路径
                clean_index_name = (index_name or "").strip() or index_code
                index_dir_name = f"{clean_index_name}_{index_code}"
                if not index_dir_name.endswith(".IDX"):
                    index_dir_name = f"{index_dir_name}.IDX"
                index_prices_dir = base_data_dir / index_dir_name / "prices"
                index_file = index_prices_dir / f"{report_date}.csv"
                if not index_file.exists():
                    csv_candidates = sorted(index_prices_dir.glob("*.csv"))
                    index_file = csv_candidates[-1] if csv_candidates else None

                rendered_monthly_table = False

                # 如果找到了指数数据文件
                if index_file and index_file.exists():
                    try:
                        # 读取指数收盘价数据
                        index_df = pd.read_csv(index_file, index_col=0)
                        index_df.index = pd.to_datetime(index_df.index)
                        if min_price_date is not None:
                            index_df = index_df[index_df.index >= min_price_date]
                        if max_price_date is not None:
                            index_df = index_df[index_df.index <= max_price_date]

                        index_close_col = _detect_close_column(index_df.columns)

                        if index_close_col:
                            index_df[index_close_col] = pd.to_numeric(
                                index_df[index_close_col], errors='coerce'
                            )
                            monthly_index_df = (
                                index_df[[index_close_col]].resample('ME').mean().round(3)
                            )
                            monthly_index_df.index = monthly_index_df.index.strftime('%Y-%m')
                            display_name = (index_name or "").strip() or index_code
                            monthly_index_df.rename(
                                columns={index_close_col: f'{display_name}指数'}, inplace=True
                            )

                            monthly_index_display = monthly_index_df.reindex(monthly_price_display.index)
                            merged_monthly_df = pd.concat([monthly_price_display, monthly_index_display], axis=1)
                            monthly_section["stock_monthly"] = dataframe_to_table(
                                monthly_price_display.reset_index().rename(columns={'index': '日期'})
                            )
                            monthly_section["index_monthly"] = dataframe_to_table(
                                monthly_index_display.reset_index().rename(columns={'index': '日期'})
                            )
                            monthly_section["index_name"] = index_name
                            monthly_section["index_code"] = index_code

                            md_content += "## 收盘价数据（月度均值）\n\n"
                            md_content += merged_monthly_df.to_markdown() + "\n\n"
                            rendered_monthly_table = True
                        else:
                            logger.warning("指数数据缺少可识别的收盘价列: %s", index_file)
                            monthly_section.setdefault("notes", []).append("未找到指数收盘价列")
                    except Exception as exc:
                        logger.warning("指数月度数据处理失败: %s", exc)
                        monthly_section.setdefault("notes", []).append(str(exc))

                if not rendered_monthly_table:
                    # 如果没有可用的指数数据或渲染失败，只显示股票数据
                    md_content += "## 收盘价数据（月度均值）\n\n"
                    md_content += monthly_price_display.to_markdown() + "\n\n"
                    if not index_file or not index_file.exists():
                        monthly_section["note"] = f"未找到{index_name}({index_code})指数数据"
                        md_content += f"*注: 未找到{index_name}({index_code})指数数据*\n\n"
                    elif "notes" in monthly_section:
                        md_content += "*注: 指数月度数据处理失败，已仅展示目标标的数据*\n\n"
                        monthly_section["note"] = "; ".join(monthly_section.pop("notes"))

            if (same_as_index or is_etf) and '收盘价' in price_df.columns:
                simple_extremes = _build_simple_price_extremes(price_df['收盘价'].dropna(), reference_date=price_df.index.max())
                if simple_extremes:
                    md_content += "## 最近三年价格极值\n\n"
                    extremes_df = pd.DataFrame(simple_extremes)
                    md_content += extremes_df.to_markdown(index=False) + "\n\n"
                    extreme_price_section["table"] = dataframe_to_table(extremes_df)
                    extreme_price_section["note"] = "基于最近三年收盘价计算的最高/最低记录"
        except Exception as e:
            md_content += f"读取并处理收盘价文件时出错: {e}\n\n"
            monthly_section["note"] = f"读取并处理收盘价文件时出错: {e}"

    json_payload["monthly_section"] = monthly_section
    if extreme_price_section:
        json_payload["extreme_price_section"] = extreme_price_section
    
    # 保存Markdown文件
    md_file = analysis_dir_path / f"{stock_name}_{target_symbol}_股票分析报告_{report_date}.md"
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    json_file = analysis_dir_path / f"{stock_name}_{target_symbol}_股票分析报告_{report_date}.json"
    try:
        with open(json_file, 'w', encoding='utf-8') as jf:
            json.dump(json_payload, jf, ensure_ascii=False, separators=(',', ':'))
        logger.info(f"结构化JSON已保存到: {json_file}")
    except Exception as exc:
        logger.error(f"写入JSON报告失败: {exc}")
    
    logger.info(f"合并报告已保存到: {md_file}")
    
    # 创建集中存放分析报告的目录
    transaction_package_dir = base_data_dir / "0_transaction_package"
    transaction_package_dir.mkdir(parents=True, exist_ok=True)
    
    # 复制报告到集中目录
    transaction_md_file = transaction_package_dir / f"{stock_name}_{target_symbol}_股票分析报告_{report_date}.md"
    try:
        shutil.copy2(md_file, transaction_md_file)
        logger.info(f"报告已复制到: {transaction_md_file}")
    except Exception as e:
        logger.error(f"复制报告时出错: {e}")
    
    transaction_json_file = transaction_package_dir / f"{stock_name}_{target_symbol}_股票分析报告_{report_date}.json"
    try:
        shutil.copy2(json_file, transaction_json_file)
        logger.info(f"JSON报告已复制到: {transaction_json_file}")
    except Exception as e:
        logger.error(f"复制JSON报告时出错: {e}")
    
    return str(md_file)


def _detect_close_column(columns: Iterable[Any]) -> Optional[str]:
    """在多种命名规则中寻找收盘价列，兼容中文/英文/带前缀的列名。"""
    prioritized: List[str] = []
    for col in columns:
        if not isinstance(col, str):
            continue
        cleaned = col.strip()
        lowered = cleaned.lower()
        if lowered.endswith('_close') or lowered == 'close':
            return col
        if 'close' in lowered:
            prioritized.append(col)
        if '收盘' in cleaned:
            return col
    return prioritized[0] if prioritized else None


def _normalize_cell_value(value: Any) -> Any:
    """将DataFrame单元格转换为可序列化且紧凑的类型。"""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return round(float(value), 6)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    return value


def dataframe_to_table(df: pd.DataFrame, include_index: bool = False, index_name: str | None = None) -> Dict[str, Any]:
    """将DataFrame转换为列+二维数组格式，便于节省token。"""
    if df is None or df.empty:
        return {"columns": [], "data": []}
    working = df.copy()
    if include_index:
        working = working.reset_index()
        if index_name:
            working = working.rename(columns={"index": index_name})
    columns = list(working.columns)
    data: List[List[Any]] = []
    for _, row in working.iterrows():
        data.append([_normalize_cell_value(row[col]) for col in columns])
    return {"columns": columns, "data": data}


def _build_simple_price_extremes(
    price_series: pd.Series,
    *,
    reference_date: Optional[pd.Timestamp] = None,
    years: int = 3,
    high_count: int = 3,
) -> List[Dict[str, Any]]:
    """计算最近三年内最高的三个价格和最低价。"""
    if price_series is None or price_series.empty:
        return []
    if reference_date is None:
        reference_date = price_series.index.max()
    if reference_date is None:
        return []
    window_start = pd.Timestamp(reference_date) - pd.Timedelta(days=365 * years)
    window = price_series[price_series.index >= window_start]
    if window.empty:
        return []

    def _single_point(series: pd.Series, *, largest: bool) -> Optional[Tuple[pd.Timestamp, float]]:
        if series is None or series.empty:
            return None
        ordered = series.sort_values(ascending=not largest)
        for ts, value in ordered.items():
            if pd.isna(value):
                continue
            return pd.Timestamp(ts), float(value)
        return None

    windows = [
        ("三年最高价", 365 * 3),
        ("一年最高价", 365),
        ("三个月最高价", 90),
    ]

    highs: List[Tuple[str, pd.Timestamp, float]] = []
    for label, days in windows:
        subset = price_series[price_series.index >= (reference_date - pd.Timedelta(days=days))]
        point = _single_point(subset, largest=True)
        if point:
            highs.append((label, point[0], point[1]))

    lows_point = _single_point(window, largest=False)
    records: List[Dict[str, Any]] = []
    for label, ts, price in highs:
        records.append(
            {
                "类别": label,
                "日期": ts.strftime("%Y-%m-%d"),
                "价格(元)": price,
            }
        )
    if lows_point:
        ts, price = lows_point
        records.append(
            {
                "类别": "三年最低价",
                "日期": ts.strftime("%Y-%m-%d"),
                "价格(元)": price,
            }
        )
    return records


# 命令行接口
def main():
    parser = argparse.ArgumentParser(description='股票价格动态分析工具')
    parser.add_argument('--symbols', nargs='+', help='股票代码列表，带市场后缀，如 002415.SZ')
    parser.add_argument('--start-date', type=str, help='短期分析开始日期，格式为YYYY-MM-DD（可省略前导0），默认取结束日期前最近7个交易日')
    parser.add_argument('--end-date', type=str, help='结束日期，格式为YYYY-MM-DD（可省略前导0），默认为今天（自动截断到最近交易日）')
    parser.add_argument('--long-term-start-date', type=str, help='收盘价数据的长期开始日期，格式为YYYY-MM-DD（可省略前导0），默认取结束日期前一年')
    parser.add_argument('--index', type=str, default='000001.IDX', help='指数代码，默认为上证指数')
    parser.add_argument('--similar', type=int, default=5, help='相似股票数量，默认为5')
    parser.add_argument('--data-dir', type=str, default='data', help='数据存储目录，默认为data')
    parser.add_argument('--force-refresh', action='store_true', help='强制刷新数据，不使用缓存')
    parser.add_argument('--only-find-similar', action='store_true', help='只执行相似股票查找，不获取数据和计算指标')
    parser.add_argument('--force_refresh_financials', action='store_true', help='只执行相似股票查找，不获取数据和计算指标')
    
    args = parser.parse_args()
    
    if not isinstance(args.symbols, list):
        args.symbols = [args.symbols]

    if not args.symbols:
        # 默认使用海康威视作为示例
        args.symbols = ["300274.SZ"]
        args.names = ["阳光电源"]
    
    symbolsInfo = [parse_symbol(symbol) for symbol in args.symbols]
    index_symbolInfo = parse_symbol(args.index)
    
    # 创建集中存放分析报告的目录
    data_dir_path = resolve_base_dir(args.data_dir)
    transaction_package_dir = data_dir_path / "0_transaction_package"
    transaction_package_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"集中分析报告将保存到: {transaction_package_dir}")
    
    try:
        results = stock_price_dynamics_summarizer(
            symbolsInfo=symbolsInfo,
            index_symbolInfo=index_symbolInfo,
            start_date=args.start_date,
            end_date=args.end_date,
            long_term_start_date=args.long_term_start_date,
            top_n_similar=args.similar,
            base_dir=data_dir_path,
            force_refresh=args.force_refresh,
            only_find_similar=args.only_find_similar,
            force_refresh_financials=args.force_refresh_financials
        )
    except DataValidationError as exc:
        logger.error("数据校验失败: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("分析过程中出现未预期错误: %s", exc)
        sys.exit(1)
    
    # 打印简要结果
    for symbol, result in results.items():
        logger.info(f"\n股票 {parse_symbol(symbol).stock_name} ({symbol}) 的分析结果概览:")
        
        if args.only_find_similar:
            logger.info(f"相似股票: {result['similar_stocks']}")
            logger.info(f"相似股票名称: {result['similar_names']}")
        else:
            logger.info(result['summary'].head())
            logger.info(f"相似股票: {result['similar_stocks']}")
    
    if not args.only_find_similar:
        logger.info(f"\n所有分析报告已复制到: {transaction_package_dir}")
    else:
        logger.info("\n只执行了相似股票查找，未生成分析报告。")


if __name__ == "__main__":
    main()


    # --symbols 002236.SZ --names 大华股份 --start-date 20250701 --long-term-start-date 20210101 --force-refresh
    # --symbols 003816.SZ --names 中国广核 --start-date 20250701 --long-term-start-date 20210101 --force-refresh
    # --symbols 513050.SH --names 中概互联网ETF --start-date 20250701 --long-term-start-date 20220101 --force-refresh
    # --symbols 01810.HK --names 小米集团-W --start-date 20250701 --long-term-start-date 20220101 --force-refresh
    # --symbols 00700.HK --names 腾讯控股 --start-date 20250701 --long-term-start-date 20220101 --force-refresh
    # --symbols 09988.HK --names 阿里巴巴-W --start-date 20250810 --long-term-start-date 20220101 --force-refresh
    # --symbols 03690.HK --names 美团-W --start-date 20250701 --long-term-start-date 20220101 --force-refresh
    # --symbols PDD.US --names 拼多多 --start-date 20250701 --long-term-start-date 20220101 --force-refresh
    # --symbols 518800.SH --names 黄金ETF --start-date 20250901 --long-term-start-date 20230101 --force-refresh
    # --symbols 605117.SH --names 德业股份 --start-date 20250701 --long-term-start-date 20220101 --force-refresh
    # --symbols 01810.HK 00700.HK 09988.HK 03690.HK 513050.SH --names 小米集团-W 腾讯控股 阿里巴巴-W 美团-W 中概互联网ETF --start-date 20250801 --long-term-start-date 20240301 --force-refresh
    # --symbols 002714 600598 002311 002415 000792 600989 600426 600019 601899 603993 002371 603501 002049 000895 603195 600276 300760 600900 601888 600258 300750 002594 600760 600941 601398 601100 600938 000063 601318 600030 002352 300274 002027 003816 518800 000895 000300 600460 600584 688017 002475 688981 601877 --names 牧原股份 北大荒 海大集团 海康威视 盐湖股份 宝丰能源 华鲁恒升 宝钢股份 紫金矿业 洛阳钼业 北方华创 韦尔股份 紫光国微 双汇发展 公牛集团 恒瑞医药 迈瑞医疗 长江电力 中国中免 首旅酒店 宁德时代 比亚迪 中航沈飞 中国移动 工商银行 恒立液压 中国海油 中兴通讯 中国平安 中信证券 顺丰控股 阳光电源 分众传媒 中国广核 黄金ETF 双汇集团 沪深300ETF 士兰微 长电科技 绿的谐波 立讯精密 中芯国际 正泰电器 --start-date 20250901 --long-term-start-date 20220101 --force-refresh
    # --symbols 518800.SH 09988.HK 513050.SH 601877.SH 603501.SH 002352.SZ 002714.SZ --names 黄金ETF 阿里巴巴-W 中概互联网ETF 正泰电器 豪威集团 顺丰控股 牧原股份 --start-date 20250901 --long-term-start-date 20230101 --force-refresh
