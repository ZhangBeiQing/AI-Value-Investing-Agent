import os
from dotenv import load_dotenv
load_dotenv()
import json
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys

# 将项目根目录加入 Python 路径，便于从子目录直接运行本文件
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from tools.general_tools import get_config_value
from configs.stock_pool import TRACKED_SYMBOLS
from utlity.stock_utils import get_latest_trading_day

TRACKED_SYMBOLS_LIST = TRACKED_SYMBOLS

def get_yesterday_date(today_date: str, calendar_market: str = "CN") -> str:
    """
    获取“上一个交易日”的日期（严格早于 today_date），使用交易日历精确判断。

    - 通过 `utlity.stock_utils.get_latest_trading_day` 在 `today_date - 1天` 的参考点向前寻找最近交易日
    - 默认市场为中国A股（CN）
    """
    ref = datetime.strptime(today_date, "%Y-%m-%d").date() - timedelta(days=1)
    try:
        prev_trading_day: date = get_latest_trading_day(ref, calendar_market)
    except Exception:
        # 兜底：若交易日历不可用，则回退到工作日判断
        while ref.weekday() >= 5:
            ref -= timedelta(days=1)
        prev_trading_day = ref
    return prev_trading_day.strftime("%Y-%m-%d")

def get_open_prices(today_date: str, symbols: List[str], merged_path: Optional[str] = None) -> Dict[str, Optional[float]]:
    """从 data/merged.jsonl 中读取指定日期与标的的开盘价。

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD。
        symbols: 需要查询的股票代码列表。
        merged_path: 可选，自定义 merged.jsonl 路径；默认读取项目根目录下 data/merged.jsonl。

    Returns:
        {symbol_price: open_price 或 None} 的字典；若未找到对应日期或标的，则值为 None。
    """
    wanted = set(symbols)
    results: Dict[str, Optional[float]] = {}

    if merged_path is None:
        base_dir = Path(__file__).resolve().parents[1]
        merged_file = base_dir / "data" / "merged.jsonl"
    else:
        merged_file = Path(merged_path)

    if not merged_file.exists():
        return results

    with merged_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            meta = doc.get("Meta Data", {}) if isinstance(doc, dict) else {}
            sym = meta.get("2. Symbol")
            if sym not in wanted:
                continue
            series = doc.get("Time Series (Daily)", {})
            if not isinstance(series, dict):
                continue
            bar = series.get(today_date)
            if isinstance(bar, dict):
                open_val = bar.get("1. buy price")
                try:
                    results[f'{sym}_price'] = float(open_val) if open_val is not None else None
                except Exception:
                    results[f'{sym}_price'] = None

    return results

def get_prev_close_prices(today_date: str, symbols: List[str], merged_path: Optional[str] = None) -> Dict[str, Optional[float]]:
    """获取相对于 today_date 的上一交易日收盘价。"""
    _, close_prices = get_yesterday_open_and_close_price(today_date, symbols, merged_path)
    return close_prices

def get_yesterday_open_and_close_price(today_date: str, symbols: List[str], merged_path: Optional[str] = None) -> tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """从 data/merged.jsonl 中读取指定日期与股票的昨日买入价和卖出价。

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        symbols: 需要查询的股票代码列表。
        merged_path: 可选，自定义 merged.jsonl 路径；默认读取项目根目录下 data/merged.jsonl。

    Returns:
        (买入价字典, 卖出价字典) 的元组；若未找到对应日期或标的，则值为 None。
    """
    wanted = set(symbols)
    buy_results: Dict[str, Optional[float]] = {}
    sell_results: Dict[str, Optional[float]] = {}

    if merged_path is None:
        base_dir = Path(__file__).resolve().parents[1]
        merged_file = base_dir / "data" / "merged.jsonl"
    else:
        merged_file = Path(merged_path)

    if not merged_file.exists():
        return buy_results, sell_results

    yesterday_date = get_yesterday_date(today_date)

    with merged_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            meta = doc.get("Meta Data", {}) if isinstance(doc, dict) else {}
            sym = meta.get("2. Symbol")
            if sym not in wanted:
                continue
            series = doc.get("Time Series (Daily)", {})
            if not isinstance(series, dict):
                continue
            
            # 尝试获取昨日买入价和卖出价
            bar = series.get(yesterday_date)
            if isinstance(bar, dict):
                buy_val = bar.get("1. buy price")  # 买入价字段
                sell_val = bar.get("4. sell price")  # 卖出价字段
                
                try:
                    buy_price = float(buy_val) if buy_val is not None else None
                    sell_price = float(sell_val) if sell_val is not None else None
                    buy_results[f'{sym}_price'] = buy_price
                    sell_results[f'{sym}_price'] = sell_price
                except Exception:
                    buy_results[f'{sym}_price'] = None
                    sell_results[f'{sym}_price'] = None
            else:
                # 如果昨日没有数据，使用交易日历向前查找最近的交易日（最多5次）
                found_data = False
                # 从昨日参考点继续向前
                current_ref = datetime.strptime(yesterday_date, "%Y-%m-%d").date() - timedelta(days=1)
                for _ in range(5):
                    try:
                        prev_trade = get_latest_trading_day(current_ref, "CN")
                    except Exception:
                        # 回退工作日近似
                        while current_ref.weekday() >= 5:
                            current_ref -= timedelta(days=1)
                        prev_trade = current_ref
                    check_date = prev_trade.strftime("%Y-%m-%d")
                    bar = series.get(check_date)
                    if isinstance(bar, dict):
                        buy_val = bar.get("1. buy price")
                        sell_val = bar.get("4. sell price")
                        try:
                            buy_price = float(buy_val) if buy_val is not None else None
                            sell_price = float(sell_val) if sell_val is not None else None
                            buy_results[f'{sym}_price'] = buy_price
                            sell_results[f'{sym}_price'] = sell_price
                            found_data = True
                            break
                        except Exception:
                            pass
                    # 继续向前一天
                    current_ref = prev_trade - timedelta(days=1)
                if not found_data:
                    buy_results[f'{sym}_price'] = None
                    sell_results[f'{sym}_price'] = None

    return buy_results, sell_results

def get_today_init_position(today_date: str, modelname: str) -> Dict[str, float]:
    """
    获取今日开盘时的初始持仓（即文件中上一个交易日代表的持仓）。从../data/agent_data/{modelname}/position/position.jsonl中读取。
    如果同一日期有多条记录，选择id最大的记录作为初始持仓。
    
    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        modelname: 模型名称，用于构建文件路径。

    Returns:
        {symbol: weight} 的字典；若未找到对应日期，则返回空字典。
    """
    base_dir = Path(__file__).resolve().parents[1]
    position_file = base_dir / "data" / "agent_data" / modelname / "position" / "position.jsonl"

    if not position_file.exists():
        print(f"Position file {position_file} does not exist")
        return {}
    
    yesterday_date = get_yesterday_date(today_date)
    max_id = -1
    latest_positions = {}
  
    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                if doc.get("date") == yesterday_date:
                    current_id = doc.get("id", 0)
                    if current_id > max_id:
                        max_id = current_id
                        latest_positions = doc.get("positions", {})
            except Exception:
                continue
    
    return latest_positions


def compute_total_value(today_date: str, positions: Dict[str, float]) -> float:
    """
    计算当日持仓总价值（股票持仓市值 + 现金）。

    - 股票市值 = 当日开盘价 * 持股数量（若价格缺失则按0计）
    - 现金直接取 `positions['CASH']`

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD
        positions: 当前持仓字典 {symbol: shares, 'CASH': cash}

    Returns:
        总价值的浮点数
    """
    if not isinstance(positions, dict) or not positions:
        return 0.0

    # 提取所有非现金的股票代码
    symbols = [s for s in positions.keys() if s != "CASH"]
    prices: Dict[str, Optional[float]] = {}
    if symbols:
        try:
            price_map = get_open_prices(today_date, symbols)
            # 将 {"CODE.SUFFIX_price": value} 转回按 symbol 索引
            for sym in symbols:
                prices[sym] = price_map.get(f"{sym}_price")
        except Exception:
            prices = {sym: None for sym in symbols}
    total_stock_value = 0.0
    for sym in symbols:
        shares = positions.get(sym, 0.0) or 0.0
        price = prices.get(sym)
        if isinstance(price, (int, float)):
            total_stock_value += float(price) * float(shares)

    cash_value = float(positions.get("CASH", 0.0) or 0.0)
    return round(total_stock_value + cash_value, 4)


def get_prev_trading_day_total_value(today_date: str, modelname: str) -> Optional[float]:
    """读取 position.jsonl 中上一交易日记录的组合总资产。

    优先返回 position.jsonl 中上一交易日 id 最大记录的 ``total_value`` 字段，
    若缺失则基于该记录的持仓重新计算。

    Args:
        today_date: 今天日期 (YYYY-MM-DD)。
        modelname: 签名/模型名，用于拼接 position 文件路径。

    Returns:
        如果找到记录则返回浮点值，否则返回 None。
    """

    base_dir = Path(__file__).resolve().parents[1]
    position_file = base_dir / "data" / "agent_data" / modelname / "position" / "position.jsonl"
    if not position_file.exists():
        return None

    prev_date = get_yesterday_date(today_date)
    max_id = -1
    latest_record: Optional[Dict[str, object]] = None

    max_any_id = -1
    fallback_record: Optional[Dict[str, object]] = None

    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            current_id = doc.get("id", -1)
            if current_id > max_any_id:
                max_any_id = current_id
                fallback_record = doc
            if doc.get("date") != prev_date:
                continue
            if current_id > max_id:
                max_id = current_id
                latest_record = doc

    if latest_record is None:
        latest_record = fallback_record
    if latest_record is None:
        return None

    total_value = latest_record.get("total_value")
    if isinstance(total_value, (int, float)):
        return float(total_value)

    positions = latest_record.get("positions")
    record_date = latest_record.get("date") or prev_date
    if isinstance(positions, dict) and positions:
        try:
            when = record_date if isinstance(record_date, str) else prev_date
            return compute_total_value(when, positions)
        except Exception:
            return None
    return None

def get_latest_position(today_date: str, modelname: str) -> Dict[str, float]:
    """
    获取最新持仓。从 ../data/agent_data/{modelname}/position/position.jsonl 中读取。
    优先选择当天 (today_date) 中 id 最大的记录；
    若当天无记录，则回退到上一个交易日，选择该日中 id 最大的记录。

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        modelname: 模型名称，用于构建文件路径。

    Returns:
        (positions, max_id):
          - positions: {symbol: weight} 的字典；若未找到任何记录，则为空字典。
          - max_id: 选中记录的最大 id；若未找到任何记录，则为 -1。
    """
    base_dir = Path(__file__).resolve().parents[1]
    position_file = base_dir / "data" / "agent_data" / modelname / "position" / "position.jsonl"

    if not position_file.exists():
        return {}, -1
    
    # 先尝试读取当天记录
    max_id_today = -1
    latest_positions_today: Dict[str, float] = {}
    
    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                if doc.get("date") == today_date:
                    current_id = doc.get("id", -1)
                    if current_id > max_id_today:
                        max_id_today = current_id
                        latest_positions_today = doc.get("positions", {})
            except Exception:
                continue
    
    if max_id_today >= 0:
        return latest_positions_today, max_id_today

    # 当天没有记录，则回退到上一个交易日
    prev_date = get_yesterday_date(today_date)
    max_id_prev = -1
    latest_positions_prev: Dict[str, float] = {}

    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                if doc.get("date") == prev_date:
                    current_id = doc.get("id", -1)
                    if current_id > max_id_prev:
                        max_id_prev = current_id
                        latest_positions_prev = doc.get("positions", {})
            except Exception:
                continue

    return latest_positions_prev, max_id_prev

def add_no_trade_record(today_date: str, modelname: str):
    """
    添加不交易记录。从 ../data/agent_data/{modelname}/position/position.jsonl 中前一日最后一条持仓，并更新在今日的position.jsonl文件中。
    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        modelname: 模型名称，用于构建文件路径。

    Returns:
        None
    """
    save_item = {}
    current_position, current_action_id = get_latest_position(today_date, modelname)
    print(current_position, current_action_id)
    save_item["date"] = today_date
    save_item["id"] = current_action_id+1
    save_item["this_action"] = {"action":"no_trade","symbol":"","amount":0}
    
    save_item["positions"] = current_position
    base_dir = Path(__file__).resolve().parents[1]
    position_file = base_dir / "data" / "agent_data" / modelname / "position" / "position.jsonl"

    # 计算 total_value 并写入
    try:
        save_item["total_value"] = compute_total_value(today_date, current_position)
    except Exception:
        save_item["total_value"] = None
        save_item["total_value"] = None
    
    # Check if the file ends with a newline
    needs_newline = False
    if os.path.exists(position_file) and os.path.getsize(position_file) > 0:
        with open(position_file, "rb") as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                needs_newline = True

    with position_file.open("a", encoding="utf-8") as f:
        if needs_newline:
            f.write("\n")
        f.write(json.dumps(save_item, ensure_ascii=False) + "\n")
    return 

def compute_position_costs_and_profit(
    today_date: str,
    modelname: str,
) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    计算当前（today_date 当日盘前）每只持仓股票的加权平均成本与浮动盈亏。
    - 加权成本通过回放 position.jsonl 中截至昨日的所有买卖操作得到，采用买入额加权、卖出优先用现有成本法（与主流券商APP一致）。
    - 浮动盈亏 = （今日开盘价 - 平均成本） * 当前持股数量；若无持仓或价格缺失则记 0。
    """
    base_dir = Path(__file__).resolve().parents[1]
    position_file = base_dir / "data" / "agent_data" / modelname / "position" / "position.jsonl"
    if not position_file.exists():
        return {}, {}

    today_positions, _ = get_latest_position(today_date, modelname)
    if not today_positions:
        return {}, {}
    tracked_symbols = [
        symbol for symbol, shares in today_positions.items() if symbol != "CASH" and (shares or 0) > 0
    ]
    if not tracked_symbols:
        return {}, {}

    cutoff_date_str = today_date
    cutoff_date = datetime.strptime(cutoff_date_str, "%Y-%m-%d").date()

    holdings: Dict[str, float] = {}
    avg_costs: Dict[str, float] = {}
    price_cache: Dict[str, Dict[str, Optional[float]]] = {}

    def _get_price_map(date_str: str, symbols: List[str]) -> Dict[str, Optional[float]]:
        if not symbols:
            return {}
        cache = price_cache.setdefault(date_str, {})
        
        # Identify symbols not yet in cache
        target_missing = [sym for sym in symbols if f"{sym}_price" not in cache]
        
        if target_missing:
            close_prices = get_prev_close_prices(date_str, target_missing)
            cache.update(close_prices)
            
        for sym in symbols:
            cache.setdefault(f"{sym}_price", None)
        return cache

    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            date_str = doc.get("date")
            if not date_str:
                continue
            try:
                record_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if record_date > cutoff_date:
                continue

            action_info = doc.get("this_action") or {}
            action_type = action_info.get("action")
            trades = action_info.get("trades") or {}
            if not isinstance(trades, dict) or action_type not in {"buy", "sell"}:
                continue

            symbols = [sym for sym, qty in trades.items() if isinstance(qty, int) and qty > 0]
            if not symbols:
                continue

            if action_type == "buy":
                price_map = _get_price_map(date_str, symbols)
                for sym in symbols:
                    qty = trades.get(sym, 0)
                    if qty <= 0:
                        continue
                    price = price_map.get(f"{sym}_price")
                    if price is None:
                        continue
                    prev_shares = holdings.get(sym, 0.0)
                    prev_cost = avg_costs.get(sym, 0.0)
                    total_cost = prev_shares * prev_cost + price * qty
                    new_shares = prev_shares + qty
                    holdings[sym] = new_shares
                    if new_shares > 0:
                        avg_costs[sym] = total_cost / new_shares
            elif action_type == "sell":
                for sym in symbols:
                    qty = trades.get(sym, 0)
                    if qty <= 0:
                        continue
                    prev_shares = holdings.get(sym, 0.0)
                    sell_qty = min(prev_shares, qty)
                    new_shares = prev_shares - sell_qty
                    if new_shares <= 0:
                        holdings.pop(sym, None)
                        avg_costs.pop(sym, None)
                    else:
                        holdings[sym] = new_shares

    # 仅保留当前仍有持仓的股票成本
    active_costs: Dict[str, float] = {}
    for sym in tracked_symbols:
        cost = avg_costs.get(sym)
        if cost is not None and cost > 0:
            active_costs[sym] = round(cost, 4)

    if not active_costs:
        return {}, {}

    today_price_map = _get_price_map(today_date, list(active_costs.keys()))
    profits: Dict[str, float] = {}
    for sym, cost in active_costs.items():
        shares = float(today_positions.get(sym, 0.0) or 0.0)
        if shares <= 0:
            continue
        price = today_price_map.get(f"{sym}_price")
        if price is None:
            profits[sym] = 0.0
        else:
            profits[sym] = round((price - cost) * shares, 4)

    return active_costs, profits


if __name__ == "__main__":
    today_date = get_config_value("TODAY_DATE")
    today_date = datetime.strptime("20251106", "%Y%m%d").strftime("%Y-%m-%d")
    signature = get_config_value("SIGNATURE")
    signature = "deepseek-chat"
    if signature is None:
        raise ValueError("SIGNATURE environment variable is not set")
    print(today_date, signature)
    yesterday_date = get_yesterday_date(today_date)
    # print(yesterday_date)
    today_buy_price = get_open_prices(today_date, TRACKED_SYMBOLS_LIST)
    # print(today_buy_price)
    yesterday_buy_prices, yesterday_sell_prices = get_yesterday_open_and_close_price(today_date, TRACKED_SYMBOLS_LIST)
    today_init_position = get_today_init_position(today_date, signature)
    latest_position, latest_action_id = get_latest_position(today_date, signature)
    print(latest_position, latest_action_id)
    avg_costs, profits = compute_position_costs_and_profit(today_date, signature)
    print(avg_costs, profits)
    add_no_trade_record(today_date, signature)
