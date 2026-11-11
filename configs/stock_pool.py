"""Stock pool configuration for the A-share focused agent.

This module centralizes the list of tracked symbols so that data ingestion,
prompt构建、交易工具等可以共享统一配置，避免各处维护多份名单。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class StockEntry:
    symbol: str
    name: str
    description: str


@dataclass(frozen=True)
class ForcedShortBookEntry:
    symbol: str
    name: str
    reason: str

TRACKED_A_STOCKS: List[StockEntry] = [
    StockEntry("002714.SZ", "牧原股份", "生猪养殖"),
    StockEntry("603501.SH", "豪威集团", "半导体设计"),
    StockEntry("600406.SH", "国电南瑞", "智能电网"),
    StockEntry("01810.HK", "小米集团-W", "手机与智能家居与汽车"),
    StockEntry("002594.SZ", "比亚迪", "汽车电子"),
    StockEntry("600150.SH", "中国船舶", "船舶制造"),
    StockEntry("518800.SH", "黄金基金ETF", "黄金"),
    StockEntry("688213.SH", "思特威-W", "sensor传感器"),
    StockEntry("002415.SZ", "海康威视", "安防监控"),
    StockEntry("02513.HK", "智谱", "人工智能大模型"),
    StockEntry("603283.SH", "赛腾股份", "消费电子自动化设备"),
    StockEntry("301536.SZ", "星宸科技", "AI视觉芯片"),
    StockEntry("300760.SZ", "迈瑞医疗", "医疗器械"),
    StockEntry("600989.SH", "宝丰能源", "煤化工与烯烃"),
    StockEntry("601138.SH", "工业富联", "AI服务器组装"),
    StockEntry("605117.SH", "德业股份", "光储逆变器"),
    StockEntry("601688.SH", "华泰证券", "证券"),
    StockEntry("512880.SH", "证券ETF", "券商板块"),
    StockEntry("512800.SH", "银行ETF", "银行板块"),
    StockEntry("512400.SH", "有色金属ETF", "有色金属板块"),
    StockEntry("513050.SH", "中概互联网ETF", "中概互联网"),
    StockEntry("512660.SH", "军工ETF", "国防军工"),
    StockEntry("159611.SZ", "电力ETF", "电力公用事业"),
]


US_TRACKED_STOCKS: List[StockEntry] = [
    StockEntry("TSM.US", "台积电", "全球最大晶圆代工厂"),
    StockEntry("NVDA.US", "英伟达", "AI 加速卡与数据中心 GPU"),
    StockEntry("AVGO.US", "博通", "定制 ASIC、交换芯片与基础软件"),
    StockEntry("GOOGL.US", "谷歌", "搜索广告、云服务与 AI 模型"),
    StockEntry("MSFT.US", "微软", "操作系统、Azure 云与企业软件"),
    StockEntry("AMZN.US", "亚马逊", "电商、AWS 云与广告"),
]

# 美股没有 A 股口径的指数缓存，用 SPY 作为基准（ETF，走仅价格模式）。
US_BENCHMARK: StockEntry = StockEntry("SPY.US", "标普500ETF", "美股基准")


FORCED_SHORT_BOOK_STOCKS: List[ForcedShortBookEntry] = [
]

TRACKED_SYMBOLS: List[str] = [entry.symbol for entry in TRACKED_A_STOCKS]
US_TRACKED_SYMBOLS: List[str] = [entry.symbol for entry in US_TRACKED_STOCKS]

# ---------------------------------------------------------------------------
# fixed_tracked 账本纳入的美股标的
# ---------------------------------------------------------------------------
# 这些标的与 TRACKED_A_STOCKS 一起进入每日 fixed_tracked 研究包与账本，
# 研究包结构、辩论流程与 05 决策完全一致（不再单独走 us_tracked 账本）。
FIXED_TRACKED_US_SYMBOLS: List[str] = [
    "TSM.US",
    "NVDA.US",
    "AVGO.US",
    "GOOGL.US",
    "MSFT.US",
    "AMZN.US",
]


def fixed_tracked_symbols() -> List[str]:
    """Current fixed book scope shared by daily preparation and historical replay."""
    return list(dict.fromkeys(
        [entry.symbol for entry in TRACKED_A_STOCKS] + FIXED_TRACKED_US_SYMBOLS
    ))

# 美元兑人民币默认换算比例：仅用于把美元计价持仓折算为人民币口径
# （总资产、浮动盈亏、仓位比例），不改变标的自身的美元报价。
USD_CNY_FX_RATE: float = 6.65


def is_usd_symbol(symbol: str) -> bool:
    """判断标的是否以美元计价。"""
    return isinstance(symbol, str) and symbol.upper().endswith(".US")


def to_cny(amount_usd: float, fx_rate: float = USD_CNY_FX_RATE) -> float:
    """把美元金额折算成人民币。"""
    return float(amount_usd) * float(fx_rate)


def symbol_to_akshare(symbol: str) -> str:
    """Convert `000001.SZ` style symbol into akshare preferred format."""

    if "." not in symbol:
        raise ValueError(f"符号{symbol}缺少交易所后缀，无法转换为 akshare 格式")

    code, exchange = symbol.split(".")
    exchange = exchange.upper()
    prefix_map = {
        "SZ": "sz",
        "SH": "sh",
        "HK": "hk",
        "BJ": "bj",
    }
    prefix = prefix_map.get(exchange)
    if prefix is None:
        raise ValueError(f"暂不支持交易所 {exchange}，请更新 prefix_map")
    return f"{prefix}{code}"


AKSHARE_SYMBOL_MAP: Dict[str, str] = {
    entry.symbol: symbol_to_akshare(entry.symbol) for entry in TRACKED_A_STOCKS
}
