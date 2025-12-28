"""Liquidity related helper functions."""

from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd


def liquidity_profile_indicator(
    price_df: pd.DataFrame,
    *,
    market_cap: float | None = None,
    turnover_column: str = "换手率",
    volume_column: str = "成交量",
) -> Dict[str, Any]:
    """
    计算股票的流动性概况指标
    
    Args:
        price_df: 包含价格数据的DataFrame，必须包含换手率和成交量列
        market_cap: 股票的市值，可选
        turnover_column: 换手率列名，默认为"换手率"
        volume_column: 成交量列名，默认为"成交量"
        
    Returns:
        包含流动性指标的字典
    """
    # 获取最新的换手率值
    turnover_rate = _latest_value(price_df.get(turnover_column))
    # 计算30天平均换手率
    avg_turnover = _rolling_avg(price_df.get(turnover_column), window=30)
    # 计算30天平均成交量（单位：万手）
    avg_volume_wan = _rolling_avg(price_df.get(volume_column), window=30, divider=10000)
    
    # 计算流动性评分
    # 处理换手率值：如果换手率≤1，说明是小数形式（如0.02表示2%），需要乘以100转换为百分比
    score = liquidity_score(
        turnover_rate * 100 if turnover_rate and turnover_rate <= 1 else turnover_rate,
        avg_turnover * 100 if avg_turnover and avg_turnover <= 1 else avg_turnover,
        market_cap,
        avg_volume_wan,
    )
    
    # 返回流动性指标结果
    return {
        "最新换手率(%)": _format_optional(turnover_rate, scale=100),  # 最新换手率（百分比）
        "30天平均换手率(%)": _format_optional(avg_turnover, scale=100),  # 30天平均换手率（百分比）
        "30天平均成交量(万手)": avg_volume_wan,  # 30天平均成交量（万手）
        "流动性评分": score,  # 流动性评分
    }



def liquidity_score(
    turnover_rate: float | None,
    average_turnover: float | None,
    market_cap: float | None,
    avg_volume_wan: float | None,
) -> float | None:
    """
    计算股票的流动性评分
    
    Args:
        turnover_rate: 最新换手率（百分比）
        average_turnover: 平均换手率（百分比）
        market_cap: 市值
        avg_volume_wan: 平均成交量（万手）
        
    Returns:
        流动性评分（0-1之间），如果所有指标都为0或None则返回None
    """
    # 收集所有流动性指标组件
    components = [turnover_rate, average_turnover, market_cap, avg_volume_wan]
    # 如果所有组件都为None或0，返回None
    if all(value in (None, 0) for value in components):
        return None

    # 内部函数：将指标值归一化到0-1之间
    def _norm(value: float | None, scale: float) -> float:
        if value is None or value <= 0:
            return 0.0
        # 归一化公式：value / scale，最大值为1.0
        return min(value / scale, 1.0)

    # 计算各组件的归一化值
    turnover_component = _norm(turnover_rate, 5.0)  # 换手率组件，目标值5%
    avg_component = _norm(average_turnover, 4.0)  # 平均换手率组件，目标值4%
    volume_component = _norm(avg_volume_wan, 6000.0)  # 成交量组件，目标值6000万手
    # 市值组件：使用对数归一化，处理市值差异大的问题
    size_component = (
        np.log1p(market_cap) / np.log1p(1000)  # 对数归一化，目标市值1000亿
        if market_cap and market_cap > 0
        else 0
    )
    
    # 计算最终流动性评分，各组件权重：
    # 换手率（35%）+ 平均换手率（25%）+ 成交量（20%）+ 市值（20%）
    score = (
        0.35 * turnover_component
        + 0.25 * avg_component
        + 0.2 * volume_component
        + 0.2 * size_component
    )
    
    # 返回四舍五入后的评分
    return round(score, 4)



def _latest_value(series: pd.Series | None) -> float | None:
    """
    获取序列的最新值
    
    Args:
        series: 输入序列
        
    Returns:
        序列的最新值，若序列为空或无法转换为数值则返回None
    """
    # 如果序列为None或为空，返回None
    if series is None or series.empty:
        return None
    
    # 获取序列的最后一个非空值，并转换为数值类型
    last = pd.to_numeric(series.dropna().tail(1), errors="coerce")
    
    # 如果转换后为空，返回None
    if last.empty:
        return None
    
    # 返回转换后的最新值
    return float(last.iloc[0])



def _rolling_avg(series: pd.Series | None, window: int, divider: float | None = None) -> float | None:
    """
    计算序列的滚动平均值
    
    Args:
        series: 输入序列
        window: 滚动窗口大小
        divider: 可选的除数，用于单位转换
        
    Returns:
        滚动平均值，若序列为空或无法计算则返回None
    """
    # 如果序列为None或为空，返回None
    if series is None or series.empty:
        return None
    
    # 获取序列的最后window个非空值，并转换为数值类型
    window_series = pd.to_numeric(series.dropna().tail(window), errors="coerce")
    
    # 如果转换后为空，返回None
    if window_series.empty:
        return None
    
    # 计算平均值
    value = float(window_series.mean())
    
    # 如果提供了除数，进行单位转换
    if divider:
        value /= divider
    
    # 返回计算结果
    return value



def _format_optional(value: float | None, scale: float = 1.0, decimals: int = 2) -> float | None:
    """
    格式化可选的数值
    
    Args:
        value: 输入数值，可能为None
        scale: 缩放因子，默认为1.0
        decimals: 保留的小数位数，默认为2
        
    Returns:
        格式化后的数值，若输入为None则返回None
    """
    # 如果输入为None，返回None
    if value is None:
        return None
    
    # 应用缩放因子
    scaled = value * scale if scale != 1.0 else value
    
    # 四舍五入到指定小数位数
    return round(scaled, decimals)
