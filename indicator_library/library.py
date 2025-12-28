"""High-level indicator orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable

import numpy as np
import pandas as pd

from indicator_library.gateways import PriceSeriesGateway
from indicator_library.schemas import IndicatorBatchRequest, IndicatorBatchResult, IndicatorSpec
from indicator_library.calculators import (
    macd_indicator,
    pct_change_indicator,
    price_snapshot_indicator,
    return_metrics_indicator,
    rsi_indicator,
    correlation_matrix_indicator,
    fundamental_ttm_indicator,
)
from indicator_library.calculators.liquidity import liquidity_profile_indicator, liquidity_score


@dataclass(frozen=True)
class ReturnMetrics:
    cumulative: Dict[str, float]
    volatility: Dict[str, float]
    sharpe: Dict[str, float]
    max_drawdown: Dict[str, float]


class IndicatorCalculationError(RuntimeError):
    """Raised when indicator computation fails."""


class IndicatorLibrary:
    """Entry point for consolidated indicator computations."""

    def __init__(
        self,
        gateway: PriceSeriesGateway | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.gateway = gateway
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._registry = self._build_registry()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def calculate(self, request: IndicatorBatchRequest) -> IndicatorBatchResult:
        """
        批量计算指标的核心方法
        
        Args:
            request: 指标计算请求对象，包含要计算的指标规格、股票代码、时间范围等信息
            
        Returns:
            IndicatorBatchResult: 包含计算结果的对象，包括价格数据、指标计算结果和元数据
        """
        # 初始化metadata变量，避免未定义错误
        metadata = {
            "start_date": request.start_date.isoformat(),  # 开始日期的ISO格式字符串
            "end_date": request.end_date.isoformat(),      # 结束日期的ISO格式字符串
            "frequency": request.frequency,               # 数据频率（如日线、周线等）
            "benchmark_count": len(request.benchmarks),   # 基准数量
        }
        
        # 检查指标库的网关是否已配置，若未配置则抛出异常
        if self.gateway is None:
            raise IndicatorCalculationError("IndicatorLibrary gateway 未配置")
        
        # 通过网关获取指定股票在指定时间范围内的价格序列数据
        price_df = self.gateway.get_price_series(
            request.symbolInfo.stock_name,  # 股票名称
            request.start_date,             # 开始日期
            request.end_date,               # 结束日期
            request.price_fields,           # 要获取的价格字段列表
        )
        
        # 按索引（日期）对价格数据进行排序，确保时间顺序正确
        price_df = price_df.sort_index()
        
        # 创建字典用于存储指标计算结果，键为指标别名或名称，值为计算结果
        outputs: Dict[str, Any] = {}
        
        # 遍历请求中的每个指标规格，逐个计算指标
        for spec in request.specs:
            # 确定指标的别名，若未指定别名则使用指标名称
            alias = spec.alias or spec.name
            
            # 从注册表中获取指标计算处理器函数
            handler = self._registry.get(spec.name)
            
            # 若未找到对应的指标处理器，则抛出异常
            if handler is None:
                raise IndicatorCalculationError(f"未注册的指标: {spec.name}")
            
            # 调用指标处理器计算指标，并将结果存入outputs字典
            outputs[alias] = handler(price_df, spec, self.logger, request)
        
        # 返回包含计算结果的IndicatorBatchResult对象
        return IndicatorBatchResult(
            symbolInfo=request.symbolInfo,# 股票信息
            prices=price_df,          # 价格数据
            tabular=outputs,          # 指标计算结果字典
            metadata=metadata,        # 元数据
        )

    # Backwards-compatible helpers ------------------------------------
    @staticmethod
    def compute_return_metrics(
        close_series: pd.Series,
        *,
        windows: Iterable[int] = (63, 126, 252),
        risk_free_rate: float = 0.02,
    ) -> ReturnMetrics:
        """
        计算收益率指标
        
        Args:
            close_series: 收盘价序列
            windows: 时间窗口列表，默认值为(63, 126, 252)，分别对应约3个月、6个月、12个月
            risk_free_rate: 无风险利率，默认值为0.02（2%）
            
        Returns:
            ReturnMetrics对象，包含累计收益率、波动率、夏普比率和最大回撤
        """
        # 检查输入是否为pandas.Series类型
        if not isinstance(close_series, pd.Series):
            raise TypeError("收盘价序列必须是 pandas.Series")
        
        # 检查收盘价序列是否为空
        if close_series.empty:
            raise ValueError("收盘价序列不能为空")
        
        # 计算对数收益率：ln(今日收盘价/昨日收盘价)，并删除第一个NaN值
        log_returns = np.log(close_series).diff().dropna()
        
        # 检查是否有足够的价格数据计算收益率
        if log_returns.empty:
            raise ValueError("无法计算收益：价格序列不足")
        
        # 初始化存储各类指标的字典
        cumulative: Dict[str, float] = {}  # 累计收益率
        volatility: Dict[str, float] = {}  # 波动率
        sharpe: Dict[str, float] = {}  # 夏普比率
        max_drawdown: Dict[str, float] = {}  # 最大回撤
        
        # 遍历每个时间窗口
        for window in windows:
            # 生成窗口键名，格式为"天数d"，如"63d"
            key = f"{window}d"
            
            # 获取最近window天的对数收益率
            window_returns = log_returns.tail(window)
            
            # 如果该时间窗口的收益率为空，将所有指标设为NaN并继续下一个窗口
            if window_returns.empty:
                cumulative[key] = volatility[key] = sharpe[key] = max_drawdown[key] = float("nan")
                continue
            
            # 计算累计收益率：(乘积(对数收益率+1) - 1) * 100
            # 注：这里使用的是近似计算，正确的累计收益率计算应为np.exp(window_returns.sum()) - 1
            cumulative_ret = (window_returns.add(1).prod() - 1) * 100
            
            # 计算年化波动率：日波动率 * sqrt(252)
            vol = window_returns.std() * (252 ** 0.5)
            
            # 计算夏普比率：(年化收益率 - 无风险利率) / 年化波动率
            # 年化收益率 = 日平均收益率 * 252
            sharpe_ratio = (
                (window_returns.mean() * 252 - risk_free_rate) / vol
                if vol and vol != 0  # 避免除以零
                else float("nan")  # 如果波动率为0，夏普比率设为NaN
            )
            
            # 计算最大回撤：使用私有方法_max_drawdown，结果乘以100转换为百分比
            drawdown = IndicatorLibrary._max_drawdown(close_series.tail(window)) * 100
            
            # 将计算结果存入对应字典
            cumulative[key] = cumulative_ret
            volatility[key] = vol * 100  # 波动率转换为百分比
            sharpe[key] = sharpe_ratio
            max_drawdown[key] = drawdown
        
        # 返回包含所有指标的ReturnMetrics对象
        return ReturnMetrics(
            cumulative=cumulative,
            volatility=volatility,
            sharpe=sharpe,
            max_drawdown=max_drawdown,
        )

    @staticmethod
    def liquidity_score(
        turnover_rate: float | None,
        average_turnover: float | None,
        market_cap: float | None,
        avg_volume_wan: float | None,
    ) -> float | None:
        return liquidity_score(turnover_rate, average_turnover, market_cap, avg_volume_wan)

    @staticmethod
    def valuation_ratio(numerator: float | None, denominator: float | None) -> float | None:
        if numerator is None or denominator is None or denominator == 0:
            return None
        return numerator / denominator

    # ------------------------------------------------------------------
    def _build_registry(self) -> Dict[str, Callable[[pd.DataFrame, IndicatorSpec, logging.Logger, IndicatorBatchRequest], Any]]:
        return {
            "price_snapshot": lambda df, spec, _logger, request: price_snapshot_indicator(
                df,
                symbolInfo=spec.params.get("symbolInfo"),
                include_turnover=spec.params.get("include_turnover", True),
            ),
            "pct_change": lambda df, spec, _logger, _request: pct_change_indicator(
                df,
                column_name=spec.params.get("column_name", "涨跌幅(%)"),
            ),
            "macd": lambda df, spec, logger, _request: macd_indicator(
                df,
                fastperiod=spec.params.get("fastperiod", 12),
                slowperiod=spec.params.get("slowperiod", 26),
                signalperiod=spec.params.get("signalperiod", 9),
                column_name=spec.params.get("column_name", "MACD"),
                logger=logger,
            ),
            "rsi": lambda df, spec, logger, _request: rsi_indicator(
                df,
                period=spec.params.get("period", 14),
                column_name=spec.params.get("column_name", "RSI(14)"),
                logger=logger,
            ),
            "return_metrics": lambda df, spec, _logger, _request: return_metrics_indicator(
                df,
                windows=spec.params.get("windows", (63, 126, 252)),
            ),
            "liquidity_profile": lambda df, spec, _logger, request: liquidity_profile_indicator(
                df,
                market_cap=spec.params.get("market_cap"),
            ),
            "correlation_matrix": lambda df, spec, _logger, request: correlation_matrix_indicator(
                _resolve_frame_from_context(
                    request.context,
                    spec.params.get("frame_key"),
                    fallback_key="correlation_frame",
                )
                or df,
                min_periods=spec.params.get("min_periods", 30),
            ),
            "fundamental_ttm": lambda df, spec, _logger, request: fundamental_ttm_indicator(
                df,
                frame=_resolve_frame_from_context(request.context, spec.params.get("frame_key")),
                value_column=spec.params.get("value_column", ""),
                date_column=spec.params.get("date_column", "REPORT_DATE"),
                window=spec.params.get("window", 4),
            ),
        }

    @staticmethod
    def _max_drawdown(price_series: pd.Series) -> float:
        cummax = price_series.cummax()
        drawdown = price_series / cummax - 1
        return float(drawdown.min()) if not drawdown.empty else 0.0


def _resolve_frame_from_context(
    context: Dict[str, Any] | None,
    key: str | None,
    *,
    fallback_key: str | None = None,
) -> pd.DataFrame | None:
    if not isinstance(context, dict):
        return None
    keys = [key] if key else []
    if fallback_key:
        keys.append(fallback_key)
    for candidate in keys:
        if candidate and candidate in context:
            frame = context[candidate]
            if isinstance(frame, pd.DataFrame):
                return frame
    return None


__all__ = [
    "IndicatorLibrary",
    "IndicatorCalculationError",
    "IndicatorBatchRequest",
    "IndicatorBatchResult",
    "IndicatorSpec",
    "ReturnMetrics",
]
