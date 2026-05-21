"""Factor metadata registry for the selection factor store."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


FactorDirection = Literal["higher_better", "lower_better", "neutral", "range_best"]
FactorGroup = Literal[
    "valuation",
    "fundamental",
    "trend",
    "technical",
    "liquidity",
    "volatility",
    "event",
    "board",
    "money_flow",
    "chip",
    "risk",
]
FactorHorizon = Literal["short", "long", "both"]
SourceType = Literal["raw", "derived"]
CostTier = Literal["light", "medium", "heavy"]
MissingPolicy = Literal["neutral", "penalty", "drop", "forward_fill"]


@dataclass(frozen=True)
class FactorDefinition:
    factor_name: str
    group: FactorGroup
    direction: FactorDirection
    horizon: FactorHorizon
    source: str
    source_type: SourceType = "derived"
    cost_tier: CostTier = "light"
    point_in_time: bool = True
    transform: str = "cross_section_rank"
    missing_policy: MissingPolicy = "neutral"
    default_weight_short: float = 0.0
    default_weight_long: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


FACTOR_DEFINITIONS: tuple[FactorDefinition, ...] = (
    FactorDefinition("pe_ttm", "valuation", "lower_better", "long", "basic_info_cache", default_weight_long=0.06),
    FactorDefinition("pb", "valuation", "lower_better", "long", "basic_info_cache", default_weight_long=0.05),
    FactorDefinition("ps", "valuation", "lower_better", "long", "basic_info_cache", default_weight_long=0.04),
    FactorDefinition("peg", "valuation", "lower_better", "long", "pe_pb_analysis_json", default_weight_long=0.04),
    FactorDefinition("pe_3_5y_percentile", "valuation", "lower_better", "long", "basic_info_cache", default_weight_long=0.04),
    FactorDefinition("pb_3_5y_percentile", "valuation", "lower_better", "long", "pe_pb_analysis_json", default_weight_long=0.03),
    FactorDefinition("price_percentile_3_5y", "valuation", "lower_better", "long", "pe_pb_analysis_json", default_weight_long=0.02),
    FactorDefinition("roe", "fundamental", "higher_better", "long", "basic_info_cache", default_weight_long=0.08),
    FactorDefinition("gross_margin", "fundamental", "higher_better", "long", "basic_info_cache", default_weight_long=0.05),
    FactorDefinition("net_profit_margin", "fundamental", "higher_better", "long", "basic_info_cache", default_weight_long=0.05),
    FactorDefinition("revenue_growth_yoy", "fundamental", "higher_better", "long", "basic_info_cache", default_weight_long=0.06),
    FactorDefinition("net_income_growth_yoy", "fundamental", "higher_better", "long", "basic_info_cache", default_weight_long=0.06),
    FactorDefinition("deduct_net_income_growth_yoy", "fundamental", "higher_better", "long", "pe_pb_analysis_json", default_weight_long=0.04),
    FactorDefinition("return_3m", "trend", "higher_better", "both", "basic_info_cache", default_weight_short=0.08, default_weight_long=0.02),
    FactorDefinition("return_6m", "trend", "higher_better", "both", "basic_info_cache", default_weight_short=0.04, default_weight_long=0.03),
    FactorDefinition("return_1y", "trend", "higher_better", "long", "basic_info_cache", default_weight_long=0.03),
    FactorDefinition("sharpe_3m", "trend", "higher_better", "short", "basic_info_cache", default_weight_short=0.06),
    FactorDefinition("sharpe_6m", "trend", "higher_better", "both", "basic_info_cache", default_weight_short=0.03, default_weight_long=0.02),
    FactorDefinition("max_drawdown_3m", "risk", "higher_better", "short", "basic_info_cache", default_weight_short=0.05),
    FactorDefinition("max_drawdown_1y", "risk", "higher_better", "long", "basic_info_cache", default_weight_long=0.05),
    FactorDefinition("distance_to_52w_high", "trend", "higher_better", "short", "price_csv", default_weight_short=0.04),
    FactorDefinition("macd", "technical", "higher_better", "short", "technical_indicators_csv", default_weight_short=0.05),
    FactorDefinition("rsi_14", "technical", "range_best", "short", "technical_indicators_csv", transform="range_score", default_weight_short=0.04),
    FactorDefinition("ma_bullish_score", "technical", "higher_better", "short", "price_csv", default_weight_short=0.06),
    FactorDefinition("close_vs_ma20", "technical", "higher_better", "short", "price_csv", default_weight_short=0.03),
    FactorDefinition("close_vs_ma60", "technical", "higher_better", "both", "price_csv", default_weight_short=0.02, default_weight_long=0.02),
    FactorDefinition("amount", "liquidity", "higher_better", "short", "price_or_technical_cache", default_weight_short=0.05),
    FactorDefinition("turnover_rate", "liquidity", "higher_better", "short", "basic_info_or_technical_cache", default_weight_short=0.04),
    FactorDefinition("avg_turnover_30d", "liquidity", "higher_better", "short", "basic_info_cache", default_weight_short=0.03),
    FactorDefinition("liquidity_score", "liquidity", "higher_better", "both", "basic_info_cache", default_weight_short=0.05, default_weight_long=0.01),
    FactorDefinition("volatility_20d", "volatility", "lower_better", "short", "price_csv", default_weight_short=0.03),
    FactorDefinition("volatility_60d", "volatility", "lower_better", "both", "price_csv", default_weight_short=0.02, default_weight_long=0.02),
    FactorDefinition("volume_ratio_5d_20d", "technical", "higher_better", "short", "price_csv", default_weight_short=0.05),
    FactorDefinition("chip_profit_ratio", "chip", "higher_better", "short", "chip_distribution_csv", default_weight_short=0.03),
    FactorDefinition("chip_concentration_70", "chip", "lower_better", "short", "chip_distribution_csv", default_weight_short=0.03),
    FactorDefinition("chip_concentration_90", "chip", "lower_better", "short", "chip_distribution_csv", default_weight_short=0.02),
    FactorDefinition("price_vs_chip_avg_cost", "chip", "higher_better", "short", "chip_distribution_csv", default_weight_short=0.03),
    FactorDefinition("chip_support_distance_70", "chip", "higher_better", "short", "chip_distribution_csv", default_weight_short=0.02),
    FactorDefinition("overhead_pressure_70", "chip", "lower_better", "short", "chip_distribution_csv", default_weight_short=0.02),
    FactorDefinition("overhead_pressure_90", "chip", "lower_better", "short", "chip_distribution_csv"),
    FactorDefinition("event_board_score", "event", "higher_better", "both", "phase3_placeholder", default_weight_short=0.10, default_weight_long=0.05),
    FactorDefinition("money_flow_score", "money_flow", "higher_better", "short", "phase3_placeholder", default_weight_short=0.09),
    FactorDefinition("chip_score", "chip", "higher_better", "short", "chip_distribution_csv", default_weight_short=0.06),
    FactorDefinition("money_chip_score", "money_flow", "higher_better", "short", "money_flow_and_chip_composite", default_weight_short=0.15),
)


FACTOR_REGISTRY: dict[str, FactorDefinition] = {
    definition.factor_name: definition
    for definition in FACTOR_DEFINITIONS
}


def factor_registry_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "factor_count": len(FACTOR_DEFINITIONS),
        "items": [definition.to_dict() for definition in FACTOR_DEFINITIONS],
    }


__all__ = [
    "FACTOR_DEFINITIONS",
    "FACTOR_REGISTRY",
    "FactorDefinition",
    "factor_registry_payload",
]
