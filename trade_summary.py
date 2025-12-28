import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Optional
from configs.stock_pool import TRACKED_A_STOCKS
from utlity import get_last_trading_day

# --- 1. 文件路径定义 (模拟数据库) ---
# 路径改造为按 signature 分目录，避免多 Agent 数据串扰

def _data_dir(signature: str) -> str:
    """根据 agent 的 signature 返回专属数据目录路径。"""
    return os.path.join('data', 'agent_data', signature)

def _stock_operations_file(signature: str) -> str:
    """原始每日逐股操作记录文件路径。"""
    return os.path.join(_data_dir(signature), 'stock_operations.json')

def _operation_summary_file(signature: str) -> str:
    """合并后的操作摘要文件路径。"""
    return os.path.join(_data_dir(signature), 'operation_summary.json')

def _portfolio_summary_file(signature: str) -> str:
    """组合级别每日系统信息文件路径。"""
    return os.path.join(_data_dir(signature), 'portfolio_daily_summary.json')

# 股票代码到名称的映射
NAME_BY_SYMBOL = {entry.symbol: entry.name for entry in TRACKED_A_STOCKS}
SUMMARY_DETAIL_FIELDS = [
    "action_num",
    "reason",
    "confidence_score",
    "key_observations",
    "position_size",
    "price_target",
    "action_price",
    "stop_loss",
    "individual_risk_notes",
    "individual_focus",
    "last_analysis_date",
]


def _normalize_relative_reason(text: Optional[str], reference_date: Optional[str]) -> Optional[str]:
    """Convert relative words like 今天/昨日 into absolute dates based on reference date."""

    if not text or not reference_date or not isinstance(text, str):
        return text
    try:
        ref_dt = datetime.strptime(reference_date, "%Y-%m-%d").date()
    except ValueError:
        return text

    today_str = ref_dt.strftime("%Y-%m-%d")
    yesterday_str = (ref_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    replacements = (
        ("今天", f'"{today_str}"'),
        ("今日", f'"{today_str}"'),
        ("当日", f'"{today_str}"'),
        ("昨天", f'"{yesterday_str}"'),
        ("昨日", f'"{yesterday_str}"'),
    )
    result = text
    for needle, repl in replacements:
        result = result.replace(needle, repl)
    return result


def _apply_reason_normalization(entry: dict, reference_date: Optional[str]) -> None:
    entry["reason"] = _normalize_relative_reason(entry.get("reason"), reference_date)


def _normalize_summary_reasons(entries: List[dict]) -> None:
    for entry in entries:
        ref_date = entry.get("end_date") or entry.get("start_date") or entry.get("operation_date")
        if ref_date:
            _apply_reason_normalization(entry, ref_date)


def _sort_summary_entries(entries: List[dict]) -> None:
    entries.sort(
        key=lambda item: (
            item.get("stock_code") or "",
            item.get("start_date") or "",
            item.get("end_date") or "",
        )
    )


# --- 2. 辅助函数 ---

def initialize_data_files(signature: str):
    """按 signature 初始化数据目录与文件。"""
    data_dir = _data_dir(signature)
    os.makedirs(data_dir, exist_ok=True)
    for file_path in [
        _stock_operations_file(signature),
        _operation_summary_file(signature),
        _portfolio_summary_file(signature),
    ]:
        if not os.path.exists(file_path):
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)

def read_json_file(file_path: str):
    """从JSON文件读取数据，异常时返回空列表。"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def write_json_file(file_path: str, data):
    """将数据写入JSON文件。"""
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _build_summary_entry(op: dict) -> dict:
    entry = {
        "stock_code": op.get("stock_code"),
        "stock_name": op.get("stock_name") or NAME_BY_SYMBOL.get(op.get("stock_code")),
        "start_date": op.get("operation_date"),
        "end_date": op.get("operation_date"),
        "duration_days": 1,
        "action_type": op.get("action_type"),
    }
    for field in SUMMARY_DETAIL_FIELDS:
        if field == "reason":
            entry[field] = _normalize_relative_reason(op.get(field), op.get("operation_date"))
        else:
            entry[field] = op.get(field)
    return entry


def _incremental_update_summary(summary_entries: List[dict], new_operations: List[dict]) -> bool:
    if not new_operations:
        return False
    latest_entry_by_stock: dict[str, dict] = {}
    for entry in summary_entries:
        stock_code = entry.get("stock_code")
        if stock_code:
            latest_entry_by_stock[stock_code] = entry

    changed = False
    new_operations_sorted = sorted(new_operations, key=lambda op: op.get("operation_date", ""))

    for op in new_operations_sorted:
        stock_code = op.get("stock_code")
        action_type = op.get("action_type")
        if not stock_code or not action_type or not op.get("operation_date"):
            continue

        current_date = datetime.strptime(op["operation_date"], "%Y-%m-%d").date()

        if action_type in {"BUY", "SELL"}:
            entry = _build_summary_entry(op)
            summary_entries.append(entry)
            latest_entry_by_stock[stock_code] = entry
            changed = True
            continue

        if action_type in {"HOLD", "FLAT"}:
            last_entry = latest_entry_by_stock.get(stock_code)
            extended = False
            if last_entry and last_entry.get("action_type") == action_type:
                prev_end_date = datetime.strptime(last_entry["end_date"], "%Y-%m-%d").date()
                if current_date == prev_end_date + timedelta(days=1) or get_last_trading_day(current_date) == prev_end_date:
                    last_entry["end_date"] = op["operation_date"]
                    last_entry["duration_days"] = (
                        current_date - datetime.strptime(last_entry["start_date"], "%Y-%m-%d").date()
                    ).days + 1
                    for field in SUMMARY_DETAIL_FIELDS:
                        if field == "reason":
                            last_entry[field] = _normalize_relative_reason(op.get(field), op.get("operation_date"))
                        else:
                            last_entry[field] = op.get(field)
                    extended = True
                    changed = True
            if extended:
                continue
            entry = _build_summary_entry(op)
            summary_entries.append(entry)
            latest_entry_by_stock[stock_code] = entry
            changed = True

    return changed


# --- 3. 核心功能函数 ---

def save_daily_operations(signature: str, ai_output_json: dict) -> List[dict]:
    """
    步骤1: 保存AI每日操盘总结到该 agent 的专属目录。
    - 将股票操作记录追加到 `stock_operations.json`。
    - 将系统级信息追加到 `portfolio_daily_summary.json`。
    """
    # 加载现有数据（按 signature 路径）
    operations_file = _stock_operations_file(signature)
    portfolio_file = _portfolio_summary_file(signature)

    all_operations = read_json_file(operations_file)
    all_portfolio_summaries = read_json_file(portfolio_file)

    # 提取并处理股票操作
    summary_date = ai_output_json.get("summary_date")
    new_operations = ai_output_json.get("stock_operations", [])

    # 为每条新操作记录添加操作日期，并检查是否重复
    saved_operations: list[dict] = []
    for op in new_operations:
        op['operation_date'] = summary_date
        # 允许同一天同一只股票有多条记录（追加模式），支持 Force Run 的重新决策
        all_operations.append(op)
        saved_operations.append(op)

    # 提取并处理系统信息
    portfolio_summary = {
        "summary_date": summary_date,
        "system_risk_notes": ai_output_json.get("system_risk_notes", []),
        "system_focus_items": ai_output_json.get("system_focus_items", []),
    }
    # 检查日期是否重复
    # 更新系统信息（覆盖同日期记录）
    existing_p_idx = next((i for i, p in enumerate(all_portfolio_summaries) if p.get('summary_date') == summary_date), -1)
    if existing_p_idx != -1:
        all_portfolio_summaries[existing_p_idx] = portfolio_summary
    else:
        all_portfolio_summaries.append(portfolio_summary)

    # 保存更新后的数据
    write_json_file(operations_file, all_operations)
    write_json_file(portfolio_file, all_portfolio_summaries)

    print(f"成功保存 {summary_date} 的 {len(saved_operations)} 条股票操作记录和1条系统总结。")
    return saved_operations


def _rebuild_operation_summary(signature: str, summary_file: str | None = None) -> None:
    summary_file = summary_file or _operation_summary_file(signature)
    operations_file = _stock_operations_file(signature)
    all_operations = read_json_file(operations_file)
    if not all_operations:
        print("没有原始操作记录可供处理。")
        return

    operations_by_stock = defaultdict(list)
    for op in all_operations:
        operations_by_stock[op.get('stock_code')].append(op)

    rebuilt_summary: List[dict] = []
    for ops in operations_by_stock.values():
        ops.sort(key=lambda x: x.get('operation_date'))
        i = 0
        while i < len(ops):
            current_op = ops[i]
            action_type = current_op.get('action_type')
            if action_type in ['BUY', 'SELL']:
                rebuilt_summary.append(_build_summary_entry(current_op))
                i += 1
                continue
            if action_type in ['HOLD', 'FLAT']:
                start_date = current_op.get('operation_date')
                last_op_in_sequence = current_op
                j = i + 1
                while j < len(ops) and ops[j].get('action_type') == action_type:
                    prev_date = datetime.strptime(ops[j-1].get('operation_date'), '%Y-%m-%d')
                    curr_date = datetime.strptime(ops[j].get('operation_date'), '%Y-%m-%d')
                    if (curr_date - prev_date).days == 1 or get_last_trading_day(curr_date) == prev_date:
                        last_op_in_sequence = ops[j]
                        j += 1
                    else:
                        break
                summary_entry = _build_summary_entry(current_op)
                summary_entry['start_date'] = start_date
                summary_entry['end_date'] = last_op_in_sequence.get('operation_date')
                start_dt = datetime.strptime(summary_entry['start_date'], '%Y-%m-%d')
                end_dt = datetime.strptime(summary_entry['end_date'], '%Y-%m-%d')
                summary_entry['duration_days'] = (end_dt - start_dt).days + 1
                for field in SUMMARY_DETAIL_FIELDS:
                    if field == "reason":
                        summary_entry[field] = _normalize_relative_reason(
                            last_op_in_sequence.get(field),
                            last_op_in_sequence.get('operation_date'),
                        )
                    else:
                        summary_entry[field] = last_op_in_sequence.get(field)
                rebuilt_summary.append(summary_entry)
                i = j
                continue
            i += 1

    _sort_summary_entries(rebuilt_summary)
    _normalize_summary_reasons(rebuilt_summary)
    write_json_file(summary_file, rebuilt_summary)
    print(f"全量重建 operation_summary 完成，共 {len(rebuilt_summary)} 条记录。")


def process_and_merge_operations(signature: str, new_operations: List[dict] | None = None):
    summary_file = _operation_summary_file(signature)
    if new_operations:
        summary_entries = read_json_file(summary_file)
        if _incremental_update_summary(summary_entries, new_operations):
            _sort_summary_entries(summary_entries)
            _normalize_summary_reasons(summary_entries)
            write_json_file(summary_file, summary_entries)
            print(f"增量更新 operation_summary，新增/合并 {len(new_operations)} 条记录。")
            return
        print("增量更新 operation_summary 未产生变化，触发全量重建以确保一致性。")
    _rebuild_operation_summary(signature, summary_file)


def get_historical_context(signature: str, stock_code: str, n: int):
    """
    步骤3: 获取指定股票最近N次的操作历史（按 signature）。
    查询 `operation_summary.json` 文件并返回结果数组。
    """
    summary_file = _operation_summary_file(signature)
    operation_summary = read_json_file(summary_file)

    # 筛选出目标股票的所有操作
    stock_history = [op for op in operation_summary if op.get('stock_code') == stock_code]

    if not stock_history:
        return []

    # 按结束日期降序排序，以获取最近的操作
    stock_history.sort(key=lambda x: x.get('end_date'), reverse=True)

    # 返回最近的 N 条记录
    return stock_history[:n]


def get_portfolio_historical_context(signature: str, stock_codes: list, n: int = 3):
    """
    获取股票池中每只股票最近N次的合并操作历史。

    返回结构示例：
    {
      "n": 3,
      "stocks": [
        {
          "stock_code": "002714",
          "stock_name": "牧原股份",
          "last_operations": [ {operation_summary记录...}, ... ]
        },
        ...
      ]
    }
    """
    summary_file = _operation_summary_file(signature)
    operation_summary = read_json_file(summary_file)

    # 建立按代码索引的记录列表
    records_by_code = defaultdict(list)
    for op in operation_summary:
        code = op.get('stock_code')
        records_by_code[code].append(op)

    result = {"n": int(n or 3), "stocks": []}
    for code in stock_codes or []:
        recs = records_by_code.get(code, [])
        # 按结束日期降序排序，选择最近N条
        recs.sort(key=lambda x: x.get('end_date'), reverse=True)
        latest = recs[:n]
        # 输出到提示词时采用时间正序（旧在前，新在后），便于阅读时间线
        latest.sort(key=lambda x: x.get('end_date') or x.get('start_date') or "")
        stock_name = latest[0].get('stock_name') if latest else NAME_BY_SYMBOL.get(code)
        result["stocks"].append({
            "stock_code": code,
            "stock_name": stock_name,
            "last_operations": latest,
        })

    # 追加最新的系统级别提示（仅取最新日期的一条）
    portfolios = read_json_file(_portfolio_summary_file(signature))
    latest_portfolio_summary = {}
    try:
        # 使用排序选择最新日期的系统提示，避免手写遍历逻辑
        valid_entries = []
        for p in portfolios:
            date_str = p.get('summary_date')
            if not isinstance(date_str, str):
                continue
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                continue
            valid_entries.append((dt, p))

        if valid_entries:
            # 以日期降序排序后取第一条
            valid_entries.sort(key=lambda t: t[0], reverse=True)
            latest_entry = valid_entries[0][1]
            latest_portfolio_summary = {
                "summary_date": latest_entry.get('summary_date'),
                "system_risk_notes": latest_entry.get('system_risk_notes', []),
                "system_focus_items": latest_entry.get('system_focus_items', []),
            }
    except Exception:
        latest_portfolio_summary = {}

    result["latest_portfolio_summary"] = latest_portfolio_summary
    return result

def load_yesterday_daily_summary(signature: str, today_date: str):
    """
    根据 today_date 读取并重构“昨日”的完整每日操盘总结 JSON。

    返回格式：
    {
      "summary_date": "YYYY-MM-DD",
      "stock_operations": [...],
      "system_risk_notes": [...],
      "system_focus_items": [...]
    }

    若不存在昨日数据，返回 None。
    """
    try:
        today_dt = datetime.strptime(today_date, '%Y-%m-%d')
    except ValueError:
        return None

    yesterday = (today_dt - timedelta(days=1)).strftime('%Y-%m-%d')

    # 读取原始逐股操作并筛选昨日
    operations = read_json_file(_stock_operations_file(signature))
    yesterday_ops = [op for op in operations if op.get('operation_date') == yesterday]

    # 读取组合级别信息并筛选昨日
    portfolios = read_json_file(_portfolio_summary_file(signature))
    portfolio_yesterday = next((p for p in portfolios if p.get('summary_date') == yesterday), None)

    if not yesterday_ops and not portfolio_yesterday:
        return None

    return {
        "summary_date": yesterday,
        "stock_operations": yesterday_ops,
        "system_risk_notes": (portfolio_yesterday or {}).get('system_risk_notes', []),
        "system_focus_items": (portfolio_yesterday or {}).get('system_focus_items', []),
    }


# --- 4. 主执行逻辑 (示例) ---
if __name__ == '__main__':
    # 示例：使用固定 signature 进行本地演示
    demo_signature = 'deepseek-chat'
    initialize_data_files(demo_signature)

    # --- 模拟AI每日输出 ---
    # Day 1
    ai_output_day1 = {
      "summary_date": "2024-01-15",
      "stock_operations": [
        {"stock_code": "002714", "stock_name": "牧原股份", "action_type": "HOLD", "reason": "持有，因估值处于历史低位...", "position_size": 0.12},
        {"stock_code": "300750", "stock_name": "宁德时代", "action_type": "BUY", "reason": "首次买入，看好技术优势和行业复苏...", "position_size": 0.15},
        {"stock_code": "600519", "stock_name": "贵州茅台", "action_type": "FLAT", "reason": "空仓，等待更好的价格...", "position_size": 0.0}
      ],
      "system_risk_notes": ["整体仓位控制在70%以内"], "system_focus_items": ["明日CPI数据发布"]
    }
    
    # Day 2
    ai_output_day2 = {
      "summary_date": "2024-01-16",
      "stock_operations": [
        {"stock_code": "002714", "stock_name": "牧原股份", "action_type": "HOLD", "reason": "继续持有，猪价出现企稳迹象...", "position_size": 0.13},
        {"stock_code": "300750", "stock_name": "宁德时代", "action_type": "SELL", "reason": "卖出，短期涨幅过大，获利了结...", "position_size": 0.0},
        {"stock_code": "600519", "stock_name": "贵州茅台", "action_type": "FLAT", "reason": "继续空仓，消费数据仍显疲弱...", "position_size": 0.0}
      ],
      "system_risk_notes": ["仓位下降至55%"], "system_focus_items": ["关注美联储会议纪要"]
    }
    
    # Day 3
    ai_output_day3 = {
      "summary_date": "2024-01-17",
      "stock_operations": [
        {"stock_code": "002714", "stock_name": "牧原股份", "action_type": "BUY", "reason": "增持，行业去产能数据超预期...", "position_size": 0.18},
        {"stock_code": "600519", "stock_name": "贵州茅台", "action_type": "FLAT", "reason": "空仓观察，批价未有明显起色...", "position_size": 0.0}
      ],
      "system_risk_notes": ["仓位提升至60%"], "system_focus_items": ["跟踪行业政策动向"]
    }

    # --- 模拟每日流程 ---
    print("="*20 + " 处理第一天数据 " + "="*20)
    ops_day1 = save_daily_operations(demo_signature, ai_output_day1)
    process_and_merge_operations(demo_signature, ops_day1)
    
    print("\n" + "="*20 + " 处理第二天数据 " + "="*20)
    ops_day2 = save_daily_operations(demo_signature, ai_output_day2)
    process_and_merge_operations(demo_signature, ops_day2)

    print("\n" + "="*20 + " 处理第三天数据 " + "="*20)
    ops_day3 = save_daily_operations(demo_signature, ai_output_day3)
    process_and_merge_operations(demo_signature, ops_day3)

    # --- 每日结束后，执行一次合并任务 ---
    print("\n" + "="*20 + " 执行智能合并任务 " + "="*20)


    process_and_merge_operations(demo_signature)

    # --- 模拟调用GET接口查询历史上下文 ---
    print("\n" + "="*20 + " 查询 '牧原股份' 最近3次操作总结 " + "="*20)
    history_muyuan = get_historical_context(demo_signature, "002714", 3)
    print(json.dumps(history_muyuan, indent=2, ensure_ascii=False))

    print("\n" + "="*20 + " 查询 '贵州茅台' 最近5次操作总结 " + "="*20)
    history_maotai = get_historical_context(demo_signature, "600519", 5)
    print(json.dumps(history_maotai, indent=2, ensure_ascii=False))
    
    print("\n" + "="*20 + " 查询 '宁德时代' 最近2次操作总结 " + "="*20)
    history_ningde = get_historical_context(demo_signature, "300750", 2)
    print(json.dumps(history_ningde, indent=2, ensure_ascii=False))
