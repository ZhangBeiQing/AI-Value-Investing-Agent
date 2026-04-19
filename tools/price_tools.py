import os
from dotenv import load_dotenv
load_dotenv()
import json
from datetime import datetime, timedelta, date
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys

import pandas as pd

# 将项目根目录加入 Python 路径，便于从子目录直接运行本文件
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from core.logging import get_logger
from shared_data_access.paths import price_cache_dir
from tools.general_tools import get_config_value
from configs.stock_pool import TRACKED_SYMBOLS
from utlity.stock_utils import get_latest_trading_day, parse_symbol

TRACKED_SYMBOLS_LIST = TRACKED_SYMBOLS
LOGGER = get_logger("PriceTools")
MANUAL_POSITION_OVERRIDE_FILENAME = "manual_position_override.json"


def _load_position_records(position_file: Path) -> List[Dict]:
    records: List[Dict] = []
    if not position_file.exists():
        return records

    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            if not isinstance(doc, dict):
                continue
            date_str = doc.get("date")
            positions = doc.get("positions")
            if not isinstance(date_str, str) or not isinstance(positions, dict):
                continue
            try:
                record_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            record_id = doc.get("id", -1)
            try:
                record_id = int(record_id)
            except Exception:
                record_id = -1
            records.append(
                {
                    "date": date_str,
                    "_parsed_date": record_date,
                    "id": record_id,
                    "positions": positions,
                    "raw": doc,
                }
            )
    return records


def _manual_position_override_file(modelname: str) -> Path:
    base_dir = Path(__file__).resolve().parents[1]
    return (
        base_dir
        / "data"
        / "agent_data"
        / modelname
        / "position"
        / MANUAL_POSITION_OVERRIDE_FILENAME
    )


def _safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _safe_int(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            parsed = float(stripped)
        except ValueError:
            return None
        if parsed.is_integer():
            return int(parsed)
    return None


def _load_manual_position_override(modelname: str) -> Optional[Dict[str, object]]:
    override_file = _manual_position_override_file(modelname)
    if not override_file.exists():
        return None
    try:
        payload = json.loads(override_file.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("manual_position_override 读取失败: %s (%s)", override_file, exc)
        return None

    if not isinstance(payload, dict):
        LOGGER.warning("manual_position_override 结构非法(非对象): %s", override_file)
        return None

    as_of_date_raw = payload.get("as_of_date")
    if not isinstance(as_of_date_raw, str):
        LOGGER.warning("manual_position_override 缺少 as_of_date: %s", override_file)
        return None
    try:
        as_of_date = datetime.strptime(as_of_date_raw, "%Y-%m-%d").date()
    except ValueError:
        LOGGER.warning("manual_position_override as_of_date 非法: %s", as_of_date_raw)
        return None

    positions_raw = payload.get("positions")
    if not isinstance(positions_raw, dict):
        LOGGER.warning("manual_position_override 缺少 positions 对象: %s", override_file)
        return None

    flat_positions: Dict[str, float] = {}
    avg_costs: Dict[str, float] = {}

    cash_value = _safe_float(payload.get("cash"))
    if cash_value is None and isinstance(positions_raw.get("CASH"), dict):
        cash_value = _safe_float((positions_raw.get("CASH") or {}).get("shares"))
    if cash_value is None and not isinstance(positions_raw.get("CASH"), dict):
        cash_value = _safe_float(positions_raw.get("CASH"))
    flat_positions["CASH"] = round(float(cash_value or 0.0), 4)

    for symbol, raw_entry in positions_raw.items():
        if symbol == "CASH":
            continue
        shares: Optional[int] = None
        avg_cost: Optional[float] = None

        if isinstance(raw_entry, dict):
            shares = _safe_int(raw_entry.get("shares"))
            avg_cost = _safe_float(raw_entry.get("avg_cost"))
        else:
            shares = _safe_int(raw_entry)

        if shares is None:
            continue
        flat_positions[symbol] = float(shares)
        if avg_cost is not None:
            avg_costs[symbol] = round(float(avg_cost), 4)

    return {
        "as_of_date": as_of_date,
        "positions": flat_positions,
        "avg_costs": avg_costs,
        "path": str(override_file),
    }


def _pick_latest_record_on_or_before(
    records: List[Dict],
    target_date: str,
    *,
    preferred_dates: Optional[List[str]] = None,
) -> Optional[Dict]:
    preferred_dates = preferred_dates or []
    for preferred_date in preferred_dates:
        exact_matches = [record for record in records if record["date"] == preferred_date]
        if exact_matches:
            return max(exact_matches, key=lambda record: (record["_parsed_date"], record["id"]))

    try:
        target = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        return None

    candidates = [record for record in records if record["_parsed_date"] <= target]
    if not candidates:
        return None
    return max(candidates, key=lambda record: (record["_parsed_date"], record["id"]))


def _resolve_position_state_on_or_before(
    target_date: str,
    modelname: str,
    *,
    preferred_dates: Optional[List[str]] = None,
) -> Tuple[Dict[str, float], int, Optional[date], str]:
    base_dir = Path(__file__).resolve().parents[1]
    position_file = base_dir / "data" / "agent_data" / modelname / "position" / "position.jsonl"

    records = _load_position_records(position_file)
    latest_record = _pick_latest_record_on_or_before(
        records,
        target_date,
        preferred_dates=preferred_dates,
    )
    latest_positions = latest_record["positions"] if latest_record is not None else {}
    latest_id = latest_record["id"] if latest_record is not None else -1
    latest_date = latest_record["_parsed_date"] if latest_record is not None else None

    override = _load_manual_position_override(modelname)
    if override is None:
        return latest_positions, latest_id, latest_date, "position_jsonl"

    override_date = override["as_of_date"]
    try:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        return latest_positions, latest_id, latest_date, "position_jsonl"

    if not isinstance(override_date, date) or override_date > target_dt:
        return latest_positions, latest_id, latest_date, "position_jsonl"

    if latest_date is not None and latest_date > override_date:
        return latest_positions, latest_id, latest_date, "position_jsonl"

    return (
        dict(override.get("positions") or {}),
        latest_id,
        override_date,
        "manual_position_override",
    )


def _resolve_manual_avg_costs(
    today_date: str,
    modelname: str,
) -> Optional[Tuple[Dict[str, float], Dict[str, float]]]:
    override = _load_manual_position_override(modelname)
    if override is None:
        return None

    try:
        target_dt = datetime.strptime(today_date, "%Y-%m-%d").date()
    except ValueError:
        return None

    override_date = override.get("as_of_date")
    if not isinstance(override_date, date) or override_date > target_dt:
        return None

    _, _, resolved_date, source = _resolve_position_state_on_or_before(today_date, modelname)
    if source != "manual_position_override" or resolved_date != override_date:
        return None

    flat_positions = dict(override.get("positions") or {})
    avg_costs = dict(override.get("avg_costs") or {})
    active_costs: Dict[str, float] = {}
    for symbol, shares in flat_positions.items():
        if symbol == "CASH":
            continue
        share_count = float(shares or 0.0)
        if share_count <= 0:
            continue
        avg_cost = _safe_float(avg_costs.get(symbol))
        if avg_cost is None or avg_cost <= 0:
            continue
        active_costs[symbol] = round(float(avg_cost), 4)

    if not active_costs:
        return {}, {}

    today_price_map = get_prev_close_prices(today_date, list(active_costs.keys()))
    profits: Dict[str, float] = {}
    for symbol, cost in active_costs.items():
        shares = float(flat_positions.get(symbol, 0.0) or 0.0)
        if shares <= 0:
            continue
        price = today_price_map.get(f"{symbol}_price")
        if price is None:
            profits[symbol] = 0.0
        else:
            profits[symbol] = round((float(price) - float(cost)) * shares, 4)

    return active_costs, profits


@lru_cache(maxsize=256)
def _load_price_frame(symbol: str) -> pd.DataFrame:
    symbol_info = parse_symbol(symbol)
    price_dir = price_cache_dir(symbol_info, base_dir=Path(project_root) / "data")
    price_file = price_dir / "price.csv"
    if not price_file.exists():
        LOGGER.warning("Price cache file %s does not exist", price_file)
        return pd.DataFrame()

    frame = pd.read_csv(price_file)
    if "日期" not in frame.columns:
        LOGGER.warning("Price cache file missing 日期 column: %s", price_file)
        return pd.DataFrame()

    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame = frame.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)
    for column in ("开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _price_row_on_or_before(symbol: str, target_date: str) -> Optional[pd.Series]:
    try:
        target_dt = pd.Timestamp(target_date)
    except Exception:
        return None

    frame = _load_price_frame(symbol)
    if frame.empty:
        return None
    subset = frame.loc[frame["日期"] <= target_dt]
    if subset.empty:
        return None
    return subset.iloc[-1]

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

def get_open_prices(today_date: str, symbols: List[str]) -> Dict[str, Optional[float]]:
    """从标准价格缓存读取指定日期与标的的开盘价。

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD。
        symbols: 需要查询的股票代码列表。

    Returns:
        {symbol_price: open_price 或 None} 的字典；若未找到对应日期或标的，则值为 None。
    """
    results: Dict[str, Optional[float]] = {}

    for sym in symbols:
        row = _price_row_on_or_before(sym, today_date)
        if row is None:
            results[f"{sym}_price"] = None
            continue
        open_val = row.get("开盘")
        results[f"{sym}_price"] = float(open_val) if pd.notna(open_val) else None

    return results

def get_prev_close_prices(today_date: str, symbols: List[str]) -> Dict[str, Optional[float]]:
    """获取相对于 today_date 的上一交易日收盘价。"""
    _, close_prices = get_yesterday_open_and_close_price(today_date, symbols)
    return close_prices

def get_yesterday_open_and_close_price(today_date: str, symbols: List[str]) -> tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """从标准价格缓存读取指定日期与股票的上一交易日开盘价和收盘价。

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        symbols: 需要查询的股票代码列表。

    Returns:
        (买入价字典, 卖出价字典) 的元组；若未找到对应日期或标的，则值为 None。
    """
    buy_results: Dict[str, Optional[float]] = {}
    sell_results: Dict[str, Optional[float]] = {}

    yesterday_date = get_yesterday_date(today_date)
    for sym in symbols:
        row = _price_row_on_or_before(sym, yesterday_date)
        if row is None:
            buy_results[f"{sym}_price"] = None
            sell_results[f"{sym}_price"] = None
            continue
        open_val = row.get("开盘")
        close_val = row.get("收盘")
        buy_results[f"{sym}_price"] = float(open_val) if pd.notna(open_val) else None
        sell_results[f"{sym}_price"] = float(close_val) if pd.notna(close_val) else None

    return buy_results, sell_results

def get_today_init_position(today_date: str, modelname: str) -> Dict[str, float]:
    """
    获取今日开盘时的初始持仓（即文件中上一个交易日代表的持仓）。
    若上一个交易日没有记录，则回退到更早的最近一条持仓记录。
    
    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        modelname: 模型名称，用于构建文件路径。

    Returns:
        {symbol: weight} 的字典；若未找到对应日期，则返回空字典。
    """
    yesterday_date = get_yesterday_date(today_date)
    positions, _, _, _ = _resolve_position_state_on_or_before(
        yesterday_date,
        modelname,
        preferred_dates=[yesterday_date],
    )
    return positions or {}


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
    prev_date = get_yesterday_date(today_date)
    positions, _, resolved_date, source = _resolve_position_state_on_or_before(
        prev_date,
        modelname,
        preferred_dates=[prev_date],
    )
    if not positions:
        return None
    if source == "manual_position_override":
        try:
            when = resolved_date.strftime("%Y-%m-%d") if isinstance(resolved_date, date) else prev_date
            return compute_total_value(when, positions)
        except Exception:
            return None

    base_dir = Path(__file__).resolve().parents[1]
    position_file = base_dir / "data" / "agent_data" / modelname / "position" / "position.jsonl"
    if not position_file.exists():
        return None
    records = _load_position_records(position_file)
    latest_record = _pick_latest_record_on_or_before(records, prev_date, preferred_dates=[prev_date])
    if latest_record is None:
        return None

    raw_record = latest_record["raw"]
    total_value = raw_record.get("total_value")
    if isinstance(total_value, (int, float)):
        return float(total_value)

    record_date = latest_record.get("date") or prev_date
    try:
        when = record_date if isinstance(record_date, str) else prev_date
        return compute_total_value(when, positions)
    except Exception:
        return None

def get_latest_position(today_date: str, modelname: str) -> Dict[str, float]:
    """
    获取最新持仓。从 ../data/agent_data/{modelname}/position/position.jsonl 中读取。
    优先选择当天 (today_date) 中 id 最大的记录；
    若当天无记录，则回退到上一个交易日；
    若上一个交易日也无记录，则继续回退到早于 today_date 的最近一条记录。

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        modelname: 模型名称，用于构建文件路径。

    Returns:
        (positions, max_id):
          - positions: {symbol: weight} 的字典；若未找到任何记录，则为空字典。
          - max_id: 选中记录的最大 id；若未找到任何记录，则为 -1。
    """
    prev_date = get_yesterday_date(today_date)
    positions, record_id, _, source = _resolve_position_state_on_or_before(
        today_date,
        modelname,
        preferred_dates=[today_date, prev_date],
    )
    if source == "manual_position_override":
        LOGGER.info("get_latest_position 使用 manual_position_override: signature=%s, today=%s", modelname, today_date)
    return positions or {}, record_id

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
    LOGGER.info("add_no_trade_record 使用上一条仓位: position=%s, action_id=%s", current_position, current_action_id)
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
    manual_override_result = _resolve_manual_avg_costs(today_date, modelname)
    if manual_override_result is not None:
        LOGGER.info("compute_position_costs_and_profit 使用 manual_position_override: signature=%s, today=%s", modelname, today_date)
        return manual_override_result

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


