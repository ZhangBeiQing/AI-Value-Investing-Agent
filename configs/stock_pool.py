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
    StockEntry("601877.SH", "正泰电器", "低压电器与户用光伏"),
    StockEntry("01810.HK", "小米集团-W", "手机与智能家居与汽车"),
    StockEntry("09988.HK", "阿里巴巴-W", "电商、云计算与人工智能"),
    StockEntry("00700.HK", "腾讯控股", "社交、游戏与云服务"),
    StockEntry("002594.SZ", "比亚迪", "汽车电子"),
    StockEntry("300750.SZ", "宁德时代", "新能源电池"),
    StockEntry("600150.SH", "中国船舶", "船舶制造"),
    StockEntry("518800.SH", "黄金基金ETF", "黄金"),
    StockEntry("688213.SH", "思特威-W", "sensor传感器"),
    StockEntry("002415.SZ", "海康威视", "安防监控"),
    StockEntry("02513.HK", "智谱", "人工智能大模型"),
    StockEntry("603283.SH", "赛腾股份", "消费电子自动化设备"),
    StockEntry("301536.SZ", "星宸科技", "AI视觉芯片"),
    StockEntry("688235.SH", "百济神州-U", "创新药"),
    StockEntry("600036.SH", "招商银行", "零售银行"),
    StockEntry("601398.SH", "工商银行", "国有大行"),
    StockEntry("603195.SH", "公牛集团", "民用电工"),
    StockEntry("603288.SH", "海天味业", "调味品龙头"),
    StockEntry("300014.SZ", "亿纬锂能", "锂电池"),
    StockEntry("002463.SZ", "沪电股份", "PCB"),
    StockEntry("002466.SZ", "天齐锂业", "锂矿"),
    StockEntry("600584.SH", "长电科技", "半导体封测"),
    StockEntry("600183.SH", "生益科技", "覆铜板CCL"),
    StockEntry("300458.SZ", "全志科技", "芯片设计"),
    StockEntry("300394.SZ", "天孚通信", "光通信器件"),
    StockEntry("300613.SZ", "富瀚微", "视频监控芯片"),
    StockEntry("002851.SZ", "麦格米特", "电力电子与电气自动化"),
    StockEntry("00175.HK", "吉利汽车", "汽车制造"),
    StockEntry("300760.SZ", "迈瑞医疗", "医疗器械"),
    StockEntry("600362.SH", "江西铜业", "铜矿采选与冶炼"),
    StockEntry("000969.SZ", "安泰科技", "钨铜偏滤器、第一壁、屏蔽材料"),
    StockEntry("688122.SH", "西部超导", "NbTi / Nb3Sn低温超导线材、磁体"),
    StockEntry("601399.SH", "国机重装", "TF线圈盒、大型结构件"),
    StockEntry("600363.SH", "联创光电", "高温超导磁体"),
    StockEntry("688776.SH", "国光电气", "真空泵、阀门、氚系统相关设备"),
    StockEntry("603011.SH", "合锻智能", "真空室/大型结构件"),
    StockEntry("600105.SH", "永鼎股份", "REBCO高温超导带材"),
    StockEntry("002318.SZ", "久立特材", "特种合金管材/核能材料"),
]


FORCED_SHORT_BOOK_STOCKS: List[ForcedShortBookEntry] = [
   #  ForcedShortBookEntry("02513.HK", "智谱", "人工强制纳入短线候选"),
   #  ForcedShortBookEntry("07709.HK", "南方东英海力士2倍做多", "人工强制纳入短线候选"),
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
