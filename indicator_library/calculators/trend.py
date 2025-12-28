"""Trend/price snapshot related indicators."""

from __future__ import annotations

import numpy as np
import pandas as pd
from utlity import SymbolInfo

def price_snapshot_indicator(
    price_df: pd.DataFrame,
    *,
    symbolInfo:SymbolInfo,
    include_turnover: bool = True,
) -> pd.DataFrame:
    """
    生成股票价格快照指标
    
    Args:
        price_df: 包含价格数据的DataFrame，必须包含相关价格列
        symbol: 股票代码，用于区分指数和普通股票
        include_turnover: 是否包含换手率指标，默认为True
        
    Returns:
        包含价格快照指标的DataFrame
    """
    
    # 如果价格数据为空，返回空DataFrame
    if price_df.empty:
        return pd.DataFrame()
    
    # 初始化结果DataFrame，使用与输入数据相同的索引
    result = pd.DataFrame(index=price_df.index)
    
    # 如果包含收盘价列，添加到结果中
    if "收盘" in price_df.columns:
        result["收盘价"] = price_df["收盘"]
    
    # 获取成交量序列
    volume_series = price_df.get("成交量")
    turnover_series = price_df.get("成交额")
    if volume_series is not None:
        # 将成交量转换为数值类型，无法转换的值设为NaN
        volume_series = pd.to_numeric(volume_series, errors="coerce")
        
        # 处理指数数据（代码以"IDX_"开头）
        if symbolInfo.symbol.endswith("IDX"):
            # 指数的成交量通常是成交额，直接转换为亿元
            result["成交额(亿元)"] = (volume_series / 1e8).round(2)
            # 指数没有成交量（万手）数据，设为NaN
            result["成交量(万手)"] = np.nan
        else:
            result["成交量(万手)"] = (volume_series / 1e6).round(2)
            if price_df["成交额"].notna().any():
                result["成交额(亿元)"] = (turnover_series / 1e8).round(2)
            else:
                result["成交额(亿元)"] = (
                    (volume_series * result["收盘价"]) / 1e8
                ).round(2)

    if include_turnover:
        turnover_col = None
        if "换手率" in price_df.columns:
            turnover_col = "换手率"
        if turnover_col:
            turnover_series = pd.to_numeric(price_df[turnover_col], errors="coerce")
            result["换手率(%)"] = turnover_series * 100

    return result
