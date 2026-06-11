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

TRACKED_A_STOCKS: List[StockEntry] = [
    StockEntry("600685.SH", "中船防务", "国防海军舰船"),
    StockEntry("600584.SH", "长电科技", "封测龙头"),
    StockEntry("002600.SZ", "领益智造", "Rubin冷接头大陆唯一"),
    StockEntry("002484.SZ", "江海股份", "MLPC超级电容独家方案"),
    StockEntry("300395.SZ", "菲利华", "石英布全球3家认证国内唯一"),
    StockEntry("601208.SH", "东材科技", "树脂全球双寡头M9认证"),
    StockEntry("688498.SH", "源杰科技", "EML光芯片国产替代"),
    StockEntry("688661.SH", "和林微纳", "H100/B100测试探针独家"),
    StockEntry("300709.SZ", "精研科技", "大陆唯一精密散热"),
    StockEntry("300308.SZ", "中际旭创", "800G/1.6T光模块全球第一"),
    StockEntry("300394.SZ", "天孚通信", "光引擎90%+份额CPO独占"),
    StockEntry("601138.SH", "工业富联", "英伟达40%DC订单代工商"),
    StockEntry("300502.SZ", "新易盛", "800G光模块全球第二大"),
    StockEntry("002463.SZ", "沪电股份", "AI主板北美市占率80%+"),
    StockEntry("300476.SZ", "胜宏科技", "GB300 HDI板独家份额50%"),
    StockEntry("600183.SH", "生益科技", "国内唯一M9覆铜板认证"),
    StockEntry("002837.SZ", "英维克", "GB300冷板+CDU认证"),
    StockEntry("002851.SZ", "麦格米特", "A股唯一英伟达官宣电源"),
    StockEntry("300811.SZ", "铂科新材", "GPU电感全球独家"),
]

TRACKED_A_STOCKS: List[StockEntry] = [
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
    StockEntry("002463.SZ", "沪电股份", "AI主板北美市占率80%+"),
    StockEntry("601138.SH", "工业富联", "英伟达40%DC订单代工商"),
    StockEntry("600584.SH", "长电科技", "封测龙头"),
    StockEntry("300476.SZ", "胜宏科技", "GB300 HDI板独家份额50%"),
    StockEntry("688213.SH", "思特威-W", "sensor传感器"),
    StockEntry("002415.SZ", "海康威视", "安防监控")
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
