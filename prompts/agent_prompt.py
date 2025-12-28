import os
from dotenv import load_dotenv

load_dotenv()

import json
import re
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import sys

from pydantic import BaseModel, Field, field_validator

# Add project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from tools.price_tools import (
    get_open_prices,
    get_today_init_position,
    get_yesterday_open_and_close_price,
    compute_position_costs_and_profit,
    get_prev_trading_day_total_value,
    get_latest_position,
)
from tools.general_tools import get_config_value
from configs.stock_pool import TRACKED_SYMBOLS, TRACKED_A_STOCKS

all_stock_pool_symbols = TRACKED_SYMBOLS

stock_pool_block = "\n".join(
    f"{idx}. {entry.symbol} {entry.name}（{entry.description}）"
    for idx, entry in enumerate(TRACKED_A_STOCKS, start=1)
)

NAME_BY_SYMBOL = {entry.symbol: entry.name for entry in TRACKED_A_STOCKS}

PROMPT_CONFIG_ENV = "PROMPT_FLOW_CONFIG"
DEFAULT_PROMPT_CONFIG = (
    Path(project_root) / "configs" / "prompt_flow" / "default_flow.json"
)
SUMMARY_PLACEHOLDER = "（暂无历史总结，请在今日结束后补充。）"
SUMMARY_FILENAME = "daily_summary.json"
SUMMARY_PATTERN = re.compile(r"<summary>(.*?)</summary>", re.IGNORECASE | re.DOTALL)


class PromptConfig(BaseModel):
    template_lines: List[str] = Field(min_length=1)
    role_intro: str
    role_traits: List[str] = Field(min_length=1)
    workflow_steps: List[str] = Field(min_length=1)
    tool_rules: List[str] = Field(min_length=1)
    decision_rules: List[str] = Field(min_length=1)
    data_rules: List[str] = Field(min_length=1)
    input_blocks: List[str] = Field(min_length=1)
    final_summary_rules: str

    @field_validator(
        "role_traits",
        "workflow_steps",
        "tool_rules",
        "decision_rules",
        "data_rules",
        "input_blocks",
    )
    @classmethod
    def _strip_items(cls, value: List[str]) -> List[str]:
        cleaned = [item.strip() for item in value]
        if not all(cleaned):
            raise ValueError("配置列表项不能为空字符串")
        return cleaned

    @field_validator("role_intro", "final_summary_rules", mode="before")
    @classmethod
    def _strip_text(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("配置字段不能为空")
        return value.strip()


STOP_SIGNAL = "<FINISH_SIGNAL>"


def _agent_data_dir(signature: str) -> Path:
    return Path(project_root) / "data" / "agent_data" / signature


def _summary_file(signature: str) -> Path:
    return _agent_data_dir(signature) / SUMMARY_FILENAME


def _load_summary_entries(signature: str) -> List[Dict[str, str]]:
    path = _summary_file(signature)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []

    entries: List[Dict[str, str]] = []
    if isinstance(data, dict):
        raw_entries = data.get("entries")
        if isinstance(raw_entries, list):
            source = raw_entries
        else:
            # 兼容旧格式 {"summary": "...", "updated_at": "..."}
            summary = data.get("summary")
            date = data.get("updated_at") or data.get("date")
            if isinstance(summary, str) and isinstance(date, str):
                source = [{"summary": summary, "date": date}]
            else:
                source = []
    else:
        source = []

    for item in source:
        summary = (item or {}).get("summary")
        date = (item or {}).get("date")
        if isinstance(summary, str) and isinstance(date, str):
            summary_clean = summary.strip()
            if summary_clean:
                entries.append({"summary": summary_clean, "date": date})
    return entries


def _write_summary_entries(signature: str, entries: List[Dict[str, str]]) -> None:
    target_dir = _agent_data_dir(signature)
    target_dir.mkdir(parents=True, exist_ok=True)
    payload = {"entries": entries}
    _summary_file(signature).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_latest_summary(signature: str, reference_date: str) -> str:
    entries = _load_summary_entries(signature)
    if not entries:
        return SUMMARY_PLACEHOLDER
    try:
        ref_dt = datetime.strptime(reference_date, "%Y-%m-%d")
    except ValueError:
        ref_dt = None

    latest_summary = None
    latest_dt = None
    for item in entries:
        try:
            entry_dt = datetime.strptime(item["date"], "%Y-%m-%d")
        except (ValueError, KeyError):
            continue
        if ref_dt and entry_dt >= ref_dt:
            # 只取参考日期之前的记录
            continue
        if latest_dt is None or entry_dt > latest_dt:
            latest_dt = entry_dt
            latest_summary = item["summary"]
    if latest_summary:
        return latest_summary
    # 如果没有早于 reference_date 的记录，fallback 为最近一条
    if entries:
        return entries[-1]["summary"]
    return SUMMARY_PLACEHOLDER


def save_daily_summary(signature: str, summary_text: str, summary_date: str) -> None:
    entries = _load_summary_entries(signature)
    # 移除同日期旧记录
    entries = [entry for entry in entries if entry.get("date") != summary_date]
    entries.append({"summary": summary_text.strip(), "date": summary_date})
    entries.sort(key=lambda item: item.get("date", ""))
    _write_summary_entries(signature, entries)


def extract_summary_text(response: str) -> Optional[str]:
    match = SUMMARY_PATTERN.search(response)
    if match:
        return match.group(1).strip()
    return None

def extract_json_from_ai_output(raw_response_text: str) -> str | None:
    """
    从AI的原始文本输出中稳健地提取JSON字符串。
    
    该函数首先会移除停止信号，然后尝试匹配Markdown格式的JSON代码块 (```json ... ```)，
    如果失败，则回退到查找第一个 '{' 和最后一个 '}' 之间内容的方法。

    Args:
        raw_response_text: AI返回的完整字符串。
        stop_signal: 用于判断任务结束的停止信号字符串。

    Returns:
        如果成功提取，则返回纯净的JSON字符串；否则返回 None。
    """
    # 步骤 0 (新增): 预处理，移除停止信号，得到一个干净的文本
    # .strip() 也能顺便移除两端的空白字符
    cleaned_text = raw_response_text.strip()

    # 步骤 1: 首选方法：在干净的文本上使用正则表达式匹配Markdown代码块
    # re.DOTALL 标志让 '.' 可以匹配换行符
    match = re.search(r'```json\s*(\{.*\})\s*```', cleaned_text, re.DOTALL)
    if match:
        return match.group(1).strip()

    # 步骤 2: 备用方法：在干净的文本上查找第一个 '{' 和最后一个 '}'
    try:
        start_index = cleaned_text.find('{')
        end_index = cleaned_text.rfind('}')
        if start_index != -1 and end_index != -1 and end_index > start_index:
            return cleaned_text[start_index : end_index + 1].strip()
    except Exception:
        pass

    # 步骤 3: 如果两种方法都失败
    return None

def _format_numbered(items: List[str], context: Dict[str, str]) -> str:
    def _safe_substitute(text: str) -> str:
        pattern = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
        return pattern.sub(lambda m: str(context.get(m.group(1), m.group(0))), text)
    lines = []
    for idx, item in enumerate(items, start=1):
        resolved = _safe_substitute(item)
        lines.append(f"{idx}. {resolved}")
    return "\n".join(lines)


def _format_bullets(
    items: List[str],
    context: Dict[str, str],
    *,
    bullet: str = "- ",
) -> str:
    def _safe_substitute(text: str) -> str:
        pattern = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
        return pattern.sub(lambda m: str(context.get(m.group(1), m.group(0))), text)
    return "\n".join(f"{bullet}{_safe_substitute(item)}" for item in items)


def _format_blocks(items: List[str], context: Dict[str, str]) -> str:
    def _safe_substitute(text: str) -> str:
        pattern = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
        return pattern.sub(lambda m: str(context.get(m.group(1), m.group(0))), text)
    return "\n\n".join(_safe_substitute(item) for item in items)


def _stringify_payload(payload: Optional[Dict[str, object]]) -> str:
    return json.dumps(payload or {}, ensure_ascii=False, indent=2, sort_keys=True)


def _resolve_config_path(config_path: Optional[str]) -> Path:
    candidate = (
        Path(config_path).expanduser()
        if config_path
        else Path(os.environ.get(PROMPT_CONFIG_ENV, DEFAULT_PROMPT_CONFIG))
    )
    candidate = candidate if isinstance(candidate, Path) else Path(candidate)
    return candidate.resolve()


@lru_cache(maxsize=4)
def _load_config_from_path(path_str: str) -> PromptConfig:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"未找到 Prompt 配置文件: {path}")
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    return PromptConfig(**data)


def load_prompt_config(config_path: Optional[str] = None) -> PromptConfig:
    resolved = _resolve_config_path(config_path)
    return _load_config_from_path(str(resolved))


def build_prompt(
    config: PromptConfig,
    *,
    stock_pool_block: str,
    context: Dict[str, str],
) -> str:
    resolved_context = dict(context)
    resolved_context.setdefault("stock_pool_block", stock_pool_block)

    def _safe_substitute(text: str, mapping: Dict[str, str]) -> str:
        pattern = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
        def _repl(m: re.Match):
            key = m.group(1)
            return str(mapping.get(key, m.group(0)))
        return pattern.sub(_repl, text)

    sections = {
        "role_intro": config.role_intro.format(**resolved_context),
        "role_traits": _format_bullets(config.role_traits, resolved_context),
        "workflow_steps": _format_numbered(config.workflow_steps, resolved_context),
        "tool_rules": _format_bullets(config.tool_rules, resolved_context),
        "decision_rules": _format_bullets(config.decision_rules, resolved_context),
        "data_rules": _format_bullets(config.data_rules, resolved_context),
        "input_blocks": _format_blocks(config.input_blocks, resolved_context),
        "final_summary_rules": _safe_substitute(config.final_summary_rules, resolved_context),
        "historical_summary": resolved_context.get("historical_summary", SUMMARY_PLACEHOLDER),
    }

    template = "\n".join(config.template_lines)
    final_mapping = {**resolved_context, **sections}
    return template.format(**final_mapping)

def _format_price_dict(raw: Dict[str, float]) -> Dict[str, float]:
    formatted: Dict[str, float] = {}
    for key, value in raw.items():
        symbol = key
        if isinstance(key, str) and key.endswith("_price"):
            symbol = key[:-6]
        name = NAME_BY_SYMBOL.get(symbol, symbol)
        formatted[f"{name}_{symbol}_price"] = value
    return formatted


def _format_metric_dict(raw: Dict[str, float], suffix: str) -> Dict[str, float]:
    formatted: Dict[str, float] = {}
    for symbol, value in raw.items():
        name = NAME_BY_SYMBOL.get(symbol, symbol)
        formatted[f"{name}_{symbol}_{suffix}"] = value
    return formatted


def get_agent_system_prompt(today_date: str, signature: str) -> str:
    print(f"signature: {signature}")
    print(f"today_date: {today_date}")
    
    # Calculate yesterday's date for search restriction
    today_dt = datetime.strptime(today_date, "%Y-%m-%d")
    yesterday_dt = today_dt - timedelta(days=1)
    yesterday_date = yesterday_dt.strftime("%Y-%m-%d")
    
    # Get yesterday's buy and sell prices
    yesterday_buy_prices, yesterday_sell_prices = get_yesterday_open_and_close_price(today_date, all_stock_pool_symbols)
    today_buy_price = get_open_prices(today_date, all_stock_pool_symbols)
    today_init_position, _ = get_latest_position(today_date, signature)
    position_costs, position_profit = compute_position_costs_and_profit(today_date, signature)

    formatted_yesterday_close = _format_price_dict(yesterday_sell_prices)
    formatted_today_buy = _format_price_dict(today_buy_price)
    formatted_position_profit = _format_metric_dict(position_profit, "profit")
    formatted_position_costs = _format_metric_dict(position_costs, "avg_cost")

    position_return_pct: Dict[str, str] = {}
    if today_init_position:
        for symbol, shares in today_init_position.items():
            if symbol == "CASH":
                continue
            shares_val = float(shares or 0)
            if shares_val <= 0:
                continue
            avg_cost_val = position_costs.get(symbol)
            profit_val = position_profit.get(symbol)
            if avg_cost_val in (None, 0) or profit_val is None:
                continue
            invested = avg_cost_val * shares_val
            if invested == 0:
                continue
            pct = (profit_val / invested) * 100
            position_return_pct[symbol] = f"{pct:+.2f}%"
    formatted_position_return_pct = _format_metric_dict(position_return_pct, "return_pct")

    # 历史总结改为读取昨日JSON，如无则回退占位文本
    try:
        # 延迟导入，避免循环依赖
        from trade_summary import get_portfolio_historical_context
        # 读取股票池最近N次（默认3）合并后的操作摘要
        portfolio_hist = get_portfolio_historical_context(signature, [s.symbol for s in TRACKED_A_STOCKS], n=2)
        historical_summary_value = json.dumps(portfolio_hist, ensure_ascii=False, indent=2)
    except Exception:
        historical_summary_value = SUMMARY_PLACEHOLDER

    prev_total_value = get_prev_trading_day_total_value(today_date, signature)
    if prev_total_value is None:
        fallback_cash = get_config_value("INITIAL_CASH") or get_config_value("INIT_CASH") or 500000.0
        try:
            fallback_cash = float(fallback_cash)
        except (TypeError, ValueError):
            fallback_cash = 500000.0
        portfolio_value_text = f"{fallback_cash:,.2f} 元（默认初始资产），"
    else:
        portfolio_value_text = f"{prev_total_value:,.2f} 元，"

    context = {
        "date": today_date,
        "date_1": yesterday_date,
        "positions": _stringify_payload(today_init_position or {}),
        # "today_buy_price": _stringify_payload(formatted_today_buy),
        "position_costs": _stringify_payload(formatted_position_costs),
        "position_profit": _stringify_payload(formatted_position_profit),
        "position_return_pct": _stringify_payload(formatted_position_return_pct),
        "STOP_SIGNAL": STOP_SIGNAL,
        "historical_summary": historical_summary_value,
        "portfolio_value": portfolio_value_text,
    }

    config = load_prompt_config()
    return build_prompt(
        config,
        stock_pool_block=stock_pool_block,
        context=context,
    )


if __name__ == "__main__":
    # today_date = get_config_value("TODAY_DATE") or datetime.now().strftime("%Y-%m-%d")
    # date_str = "2025-11-06"
    # # 先转为 datetime，再提取 date 部分
    # date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    # signature = get_config_value("SIGNATURE") or "debug-signature"
    # print(get_agent_system_prompt(str(date_obj), signature))

    prompt = ""
    output = extract_json_from_ai_output(prompt)
    print(output)
