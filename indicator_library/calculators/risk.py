"""Risk related indicators (return metrics, correlation)."""

from __future__ import annotations

from typing import Dict, Iterable

import numpy as np
import pandas as pd


def return_metrics_indicator(
    price_df: pd.DataFrame,
    *,
    windows: Iterable[int] = (63, 126, 252),
) -> Dict[int, Dict[str, float]]:
    """
    计算不同时间窗口的收益指标
    
    Args:
        price_df: 包含价格数据的DataFrame，必须包含"收盘"列
        windows: 时间窗口列表，默认值为(63, 126, 252)，分别对应约3个月、6个月、12个月
        
    Returns:
        字典，键为时间窗口，值为包含各项收益指标的字典
    """
    # 检查价格数据是否为空或缺少"收盘"列
    if price_df.empty or "收盘" not in price_df.columns:
        return {}
    
    # 获取收盘价序列，删除缺失值
    prices = price_df["收盘"].dropna()
    
    # 如果收盘价序列为空，返回空字典
    if prices.empty:
        return {}
    
    # 计算日收益率，使用pct_change方法，不填充缺失值，然后删除缺失值
    returns = prices.pct_change(fill_method=None).dropna()
    
    # 如果收益率序列为空，返回空字典
    if returns.empty:
        return {}
    
    # 初始化结果字典，用于存储不同时间窗口的指标
    results: Dict[int, Dict[str, float]] = {}
    
    # 遍历每个时间窗口
    for period in windows:
        # 获取最近period天的收益率
        window_returns = returns.tail(period)
        
        # 如果该时间窗口的收益率为空，跳过当前循环
        if window_returns.empty:
            continue
        
        # 获取对应时间窗口的价格数据，包括收益率起始日期的前一天价格
        window_prices = prices.loc[window_returns.index.union([window_returns.index[0]])]
        
        # 计算累计收益率，公式：(期末价格/期初价格 - 1) * 100
        cumulative = (window_prices.iloc[-1] / window_prices.iloc[0] - 1) * 100
        
        # 计算日平均收益率
        mean_return = window_returns.mean()
        
        # 计算日收益率的标准差（无偏估计，自由度为0）
        std_return = window_returns.std(ddof=0)
        
        # 初始化夏普比率为0
        sharpe = 0.0
        
        # 如果标准差大于0，计算夏普比率
        # 公式：(日平均收益率 / 日标准差) * 根号(252)，假设一年252个交易日
        if std_return > 0:
            sharpe = (mean_return / std_return) * np.sqrt(252)
        
        # 计算年化波动率，公式：日标准差 * 根号(252) * 100
        annual_vol = std_return * np.sqrt(252) * 100
        
        # 计算价格序列的累计最大值
        running_max = window_prices.cummax()
        
        # 计算回撤，公式：(当前价格 / 累计最大值) - 1
        drawdowns = (window_prices / running_max) - 1
        
        # 计算最大回撤，公式：最小回撤值 * 100
        max_drawdown = drawdowns.min() * 100
        
        # 将当前时间窗口的指标存储到结果字典中
        results[period] = {
            "累计收益率(%)": float(cumulative),  # 累计收益率（百分比）
            "夏普比率": float(sharpe),  # 夏普比率
            "年化波动率(%)": float(annual_vol),  # 年化波动率（百分比）
            "最大回撤(%)": float(max_drawdown),  # 最大回撤（百分比）
        }
    
    # 返回包含所有时间窗口指标的结果字典
    return results



def correlation_matrix_indicator(
    prices: pd.DataFrame,
    *,
    min_periods: int = 30,
) -> pd.DataFrame:
    """
    计算不同股票之间的相关性矩阵
    
    Args:
        prices: 包含多个股票价格数据的DataFrame，列名格式为"股票名称_收盘"
        min_periods: 计算相关性所需的最小观测值数量，默认值为30
        
    Returns:
        相关性矩阵DataFrame
    """
    # 筛选出所有以"_收盘"结尾的列，即所有股票的收盘价列
    closes = prices.filter(like="_收盘")
    
    # 如果没有收盘价列，返回空DataFrame
    if closes.empty:
        return pd.DataFrame()
    
    # 计算所有股票的日收益率
    returns = closes.pct_change(fill_method=None)
    
    # 计算相关性矩阵，使用皮尔逊相关系数，最小观测值数量为min_periods
    return returns.corr(method="pearson", min_periods=min_periods)
