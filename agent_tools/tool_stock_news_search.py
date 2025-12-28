#!/usr/bin/env python
"""
本地股票新闻检索 MCP 工具
===========================

设计思路
--------
1. 依托 AI-TRADER 既有的数据目录约定（`data/{stock_name}_{stock_code}/news/`）直接读取本地 JSON，
   解决外部搜索效果不佳的问题，并避免重复请求网络接口。
2. 通过 FastMCP 暴露 `search_stock_news` 工具，供 LangChain Agent 在回测或实盘推理时按需加载新闻。
3. 调用侧只需提供股票名称、股票代码与当前 `today_time`，工具负责过滤掉未来新闻，保证回测一致性。

设计细节
--------
* 文件定位：根据 `NEWS_BASE_DIR`（默认 `data/`）拼接成 `data/{stock_name}_{stock_code}/news/`，遍历目录下全部
  `.json`/`.md`/`.txt` 文件，只要内容中能解析出合法 JSON，就会被纳入候选。
* 时间过滤：同时支持 `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, ISO8601 等常见格式；只有 `datetime < today_time`
  的新闻才会返回，并按照时间倒序排序，保证最近的历史新闻优先。
* 结果输出：返回一个 JSON 字符串，包含 `stock`, `today`, `news_items`（过滤后的数组）以及
  `diagnostics`（记录跳过文件、解析失败等信息），便于调用端进一步处理。
* 容错策略：当找不到目录或解析失败时，及时返回友好提示，避免 Agent 阻塞；同时保留诊断信息，方便排查。

工具作用
--------
* 替代网络检索，直接消费研究团队整理的高置信度新闻。
* 支持 LangChain Agent / MCP 工作流：在推理链中插入 `search_stock_news` 即可获得结构化 JSON，
  无需额外编写文件读取逻辑。
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
from fastmcp import FastMCP
load_dotenv()
from agent_tools.logging_utils import init_tool_logger
from configs.stock_pool import TRACKED_A_STOCKS
from news.disclosures_builder import update_disclosures_for_stock
from utlity import parse_symbol, is_cn_etf_symbol

# === 请将此代码块放在脚本的最开头 ===

mcp = FastMCP("StockNewsSearch")
logger = init_tool_logger("stock_news_search")

SYMBOL_NAME_MAP = {entry.symbol: entry.name for entry in TRACKED_A_STOCKS}

NEWS_BASE_DIR = Path(os.getenv("NEWS_BASE_DIR", "data/stock_info")).resolve()
SUPPORTED_SUFFIXES = {".json", ".md", ".txt"}


def parse_datetime(value: str | None) -> Optional[datetime]:
    """尝试将字符串解析为 datetime，支持常见的日期/时间格式。"""
    if not value:
        return None

    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_today_date(value: str) -> Optional[datetime]:
    """仅支持 YYYY-MM-DD 格式的 today_time。"""
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def load_json_payload(text: str, source: Path) -> Optional[Dict[str, Any]]:
    """从字符串中提取 JSON；若原文包含 Markdown 包裹则自动裁剪。"""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and start < end:
            snippet = text[start : end + 1]
            try:
                return json.loads(snippet)
            except json.JSONDecodeError:
                return None
    return None


def collect_news_items(stock_name: str, stock_code: str, diagnostics: List[str]) -> List[Dict[str, Any]]:
    """读取指定股票目录下的所有 news JSON，并拼接成统一列表。"""
    target_dir = NEWS_BASE_DIR / f"{stock_name}_{stock_code}" / "news"
    if not target_dir.is_dir():
        diagnostics.append(f"未找到目录: {target_dir}")
        logger.warning("新闻目录不存在: %s", target_dir)
        return []

    aggregated: List[Dict[str, Any]] = []
    audited_json = target_dir / "news_audited.json"
    if audited_json.exists():
        try:
            content = audited_json.read_text(encoding="utf-8")
            payload = json.loads(content)
            items = payload.get("news_items") or []
            if isinstance(items, list):
                return items
            diagnostics.append(f"主新闻文件结构异常: {audited_json}")
        except Exception as exc:
            diagnostics.append(f"读取主新闻文件失败 {audited_json.name}: {exc}")
    return aggregated


def filter_news_before_today(
    items: List[Dict[str, Any]],
    today_time: datetime,
    diagnostics: List[str],
    days: int = 95,
) -> List[Dict[str, Any]]:
    """
    筛选在指定日期之前且位于最近 N 天窗口内的新闻，并按时间倒序返回。

    Summary:
        从 `today_time` 开始向前 `days` 天构造时间窗口，保留满足
        `start_time <= datetime < today_time` 的新闻项（排除当天及未来）。
        无法解析日期的项会记录到 `diagnostics` 并被忽略。

    Args:
        items (List[Dict[str, Any]]): 原始新闻项列表，每项需包含可解析的 `datetime` 字段。
        today_time (datetime): 当前基准日期，过滤严格小于该日期的新闻。
        diagnostics (List[str]): 诊断信息输出容器，用于记录被跳过条目原因。
        days (int): 回溯天数窗口，默认 85；窗口下界为 `today_time - days`。

    Returns:
        List[Dict[str, Any]]: 过滤后的新闻项列表，按时间倒序（最近在前）。
    """
    start_time = today_time - timedelta(days=days)
    filtered: List[Dict[str, Any]] = []
    for item in items:
        dt_value = parse_datetime(item.get("datetime"))
        if dt_value is None:
            # 对于这些同质化极高的公告，如果日期缺失，通常是因为爬虫或源数据质量问题
            # 只要不是唯一的（通常会有多次），我们就可以忽略它，而不必向用户报错
            title = str(item.get('title', ''))
            if any(kw in title for kw in ["质押", "回购", "担保"]):
                # 仅记录到后台日志，不作为面向用户的诊断信息
                # logger.debug(f"跳过无效日期的新闻(同质化忽略): {title}") # Logger needs to be available or passed
                continue
                
            diagnostics.append(f"跳过新闻（无法解析日期）: {item.get('title', '未知标题')}")
            continue
        if dt_value >= today_time or dt_value < start_time:
            continue
        item["_datetime_obj"] = dt_value
        filtered.append(item)

    filtered.sort(key=lambda x: x["_datetime_obj"], reverse=True)
    return filtered


def _deduplicate_recurring_events(
    items: List[Dict[str, Any]],
    diagnostics: List[str],
) -> List[Dict[str, Any]]:
    """
    针对特定类型的同质化公告（质押、回购、担保），仅保留最近的一条。
    
    逻辑：
    1. 识别包含特定关键词的公告。
    2. 对每一类关键词，找出时间最近的那一条。
    3. 将该类中稍早的公告全部标记为删除。
    """
    # 定义需要去重的关键词列表
    # 用户特别指出："担保"需要匹配（原有需求提到 title中有担保开头，但在实际公告标题中通常包含股票名/日期，使用 in 更稳健）
    keywords = ["质押", "回购", "担保"]
    
    to_remove_indices = set()
    
    for kw in keywords:
        # 找出所有包含当前关键词的索引
        match_indices = []
        for idx, item in enumerate(items):
            title = str(item.get("title") or "")
            if kw in title:
                match_indices.append(idx)
        
        if not match_indices:
            continue
            
        # 找出该组中时间最近的由
        latest_idx = None
        latest_dt = None
        
        for idx in match_indices:
            item = items[idx]
            dt_value = item.get("_datetime_obj")
            if not isinstance(dt_value, datetime):
                 dt_value = parse_datetime(item.get("datetime"))
            
            if dt_value is None:
                continue
                
            if latest_dt is None or dt_value > latest_dt:
                latest_dt = dt_value
                latest_idx = idx
                
        # 如果找不到有效时间的项，默认保留第一个，其余删除
        if latest_idx is None:
            latest_idx = match_indices[0]
            
        # 标记除 latest_idx 以外的所有匹配项
        for idx in match_indices:
            if idx != latest_idx:
                to_remove_indices.add(idx)
                
    if not to_remove_indices:
        return items

    trimmed = []
    removed_count = 0
    for idx, item in enumerate(items):
        if idx in to_remove_indices:
            removed_count += 1
            continue
        trimmed.append(item)

    diagnostics.append(
        f"同质化公告过滤(质押/回购/担保): 移除 {removed_count} 条冗余信息"
    )
    return trimmed


def _strip_internal_datetime(items: List[Dict[str, Any]]) -> None:
    for entry in items:
        entry.pop("_datetime_obj", None)


def _filter_low_impact_noise(
    items: List[Dict[str, Any]], diagnostics: List[str]
) -> List[Dict[str, Any]]:
    """移除 impact=Low 且 sentiment=Neutral 的低价值新闻，减少 token 开销。"""

    cleaned: List[Dict[str, Any]] = []
    dropped = 0
    for item in items:
        impact = str(item.get("impact_level", "")).strip().lower()
        sentiment = str(item.get("sentiment", "")).strip().lower()
        if impact == "low" and sentiment == "neutral":
            dropped += 1
            continue
        cleaned.append(item)

    if dropped:
        diagnostics.append(f"过滤低影响/中性新闻 {dropped} 条，保留 {len(cleaned)} 条")
    return cleaned


@mcp.tool()
def search_stock_news(symbol: str, today_time: str) -> str:
    """
    搜索指定股票在给定日期之前3个月的新闻信息,作为AI投资分析参考。
    ETF/基金类标的（如 51/58/15/16/50/53 开头的 A 股基金）不适用本工具。

    Args:
        symbol: 标准化股票代码（CODE.SUFFIX），例如 "300274.SZ"。
        today_time: 当前日期（YYYY-MM-DD），例如 "2025-09-01"，返回该日期之前3个月的所有新闻/公告摘要。

    Returns:
        str: JSON 字符串，结构为 {stock, today, news_items, diagnostics}。

        特殊返回情况：
        - 日期格式错误：返回错误信息及格式提示
        - 目录不存在：返回空 news_items 与诊断信息
        - 解析失败：包含详细诊断信息
    """

    diagnostics: List[str] = []
    stock_code = symbol.strip()
    stock_name = SYMBOL_NAME_MAP.get(stock_code, stock_code)
    if is_cn_etf_symbol(stock_code):
        payload = {
            "stock": f"{stock_name} ({stock_code})",
            "today": today_time,
            "news_items": [],
            "diagnostics": ["ETF/基金类标的不提供公告/新闻摘要，请选择股票标的。"],
        }
        logger.info("search_stock_news ETF 检测: %s", stock_code)
        return json.dumps(payload, ensure_ascii=False, indent=2)
    symbol_info = None
    try:
        symbol_info = parse_symbol(stock_code)
        stock_name = symbol_info.stock_name or stock_name
    except Exception:
        symbol_info = None

    logger.info(
        "search_stock_news 请求: stock=%s(%s), today=%s",
        stock_name,
        stock_code,
        today_time,
    )

    today_dt = parse_today_date(today_time)
    if today_dt is None:
        payload = {
            "error": f"无法解析 today_time: {today_time}",
            "hint": "只支持 YYYY-MM-DD 格式，例如 2025-11-07。",
        }
        logger.error("search_stock_news 日期解析失败: %s", today_time)
        return json.dumps(payload, ensure_ascii=False, indent=2)

    items = collect_news_items(stock_name, stock_code, diagnostics)
    if not items:
        try:
            update_disclosures_for_stock(stock_name, stock_code, lookback_days=365)
            items = collect_news_items(stock_name, stock_code, diagnostics)
        except Exception as exc:
            diagnostics.append(f"公告构建失败: {exc}")
        payload = {
            "stock": f"{stock_name} ({stock_code})",
            "today": today_time,
            "news_items": [],
            "diagnostics": diagnostics or ["未找到任何可用的新闻文件。"],
        }
        logger.warning(
            "search_stock_news 未找到新闻: stock=%s(%s)",
            stock_name,
            stock_code,
        )
        return json.dumps(payload, ensure_ascii=False, indent=2)

    filtered = filter_news_before_today(items, today_dt, diagnostics)
    filtered = _deduplicate_recurring_events(filtered, diagnostics)
    filtered = _filter_low_impact_noise(filtered, diagnostics)
    _strip_internal_datetime(filtered)

    payload = {
        "stock": f"{stock_name} ({stock_code})",
        "today": today_time,
        "news_items": filtered,
        "diagnostics": diagnostics,
    }
    logger.info(
        "search_stock_news 完成: items=%d, diagnostics=%d",
        len(filtered),
        len(diagnostics),
    )
    return json.dumps(payload, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    port = int(os.getenv("NEWS_HTTP_PORT", "8006"))
    mcp.run(transport="streamable-http", port=port)

    # search_stock_news(symbol="002352.SZ", today_time="2025-12-15")
