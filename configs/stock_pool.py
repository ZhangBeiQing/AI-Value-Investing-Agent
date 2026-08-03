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
    StockEntry("300476.SZ", "胜宏科技", "GB300 HDI板独家份额50%"),
    StockEntry("00100.HK", "MINIMAX-WP", "人工智能"),
    StockEntry("002714.SZ", "牧原股份", "生猪养殖"),
    StockEntry("603501.SH", "豪威集团", "半导体设计"),
    StockEntry("600276.SH", "恒瑞医药", "创新医药"),
    StockEntry("300274.SZ", "阳光电源", "光伏逆变器"),
    StockEntry("600406.SH", "国电南瑞", "智能电网"),
    StockEntry("01810.HK", "小米集团-W", "手机与智能家居与汽车"),
    StockEntry("002594.SZ", "比亚迪", "汽车电子"),
    StockEntry("300750.SZ", "宁德时代", "新能源电池"),
    StockEntry("600150.SH", "中国船舶", "船舶制造"),
    StockEntry("518800.SH", "黄金基金ETF", "黄金"),
    StockEntry("688213.SH", "思特威-W", "sensor传感器"),
    StockEntry("002415.SZ", "海康威视", "安防监控"),
    StockEntry("02513.HK", "智谱", "人工智能大模型"),
    StockEntry("603283.SH", "赛腾股份", "消费电子自动化设备"),
    StockEntry("301536.SZ", "星宸科技", "AI视觉芯片")
]


FORCED_SHORT_BOOK_STOCKS: List[ForcedShortBookEntry] = [
    ForcedShortBookEntry("02513.HK", "智谱", "人工强制纳入短线候选"),
    ForcedShortBookEntry("07709.HK", "南方东英海力士2倍做多", "人工强制纳入短线候选"),
]

TRACKED_SYMBOLS: List[str] = [entry.symbol for entry in TRACKED_A_STOCKS]


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
