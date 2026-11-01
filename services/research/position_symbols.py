"""把账本持仓解析成 symbol，供财报链路把「当前持仓」并入扫描范围。

背景：``prepare_financial_report_skill.py`` 默认扫描 ``TRACKED_A_STOCKS ∪ 量化初筛``，
持仓里一旦有不在 ``TRACKED_A_STOCKS`` 的股票（如 605117.SH、601138.SH、601688.SH），
它们的公告索引不会被同步，财报季会整批漏掉 —— 2026-10-29 的德业股份三季报就是这样漏的。

本模块只负责“持仓 → symbol”的解析，不做任何抓取。名称解析顺序：

1. ``data/universe/master_universe.json`` 的 ``name → symbol``；
2. ``data/stock_info/{名称}_{代码}`` 目录名反推；
3. 都匹配不到时原样返回名称（交由下游判定是否可用）。

``CASH`` 等非证券键会被跳过。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from core.logging import get_logger

LOGGER = get_logger("PositionSymbols")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SIGNATURES: Sequence[str] = ("book-fixed_tracked",)
_NON_SECURITY_KEYS = {"CASH", "cash"}


def _load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("读取失败，忽略: %s (%s)", path, exc)
        return None


def build_name_symbol_map(
    *,
    universe_path: Path,
    stock_info_dir: Path,
) -> Dict[str, str]:
    """构建 名称 → symbol 映射；stock_info 目录名只做兜底，不覆盖 universe。"""
    mapping: Dict[str, str] = {}

    payload = _load_json(universe_path)
    if isinstance(payload, dict):
        for entry in payload.get("stocks") or []:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name") or "").strip()
            symbol = str(entry.get("symbol") or "").strip()
            if name and symbol:
                mapping[name] = symbol

    if stock_info_dir.is_dir():
        for child in stock_info_dir.iterdir():
            if not child.is_dir():
                continue
            name, sep, symbol = child.name.rpartition("_")
            if sep and name and symbol and name not in mapping:
                mapping[name] = symbol

    return mapping


def resolve_position_name(name: str, mapping: Dict[str, str]) -> str:
    """把持仓里的名称换成 symbol；匹配不到时原样返回。"""
    cleaned = name.strip()
    if not cleaned:
        return cleaned
    if cleaned in mapping:
        return mapping[cleaned]
    # 已经是代码形态则直接返回
    upper = cleaned.upper()
    if upper.endswith((".SH", ".SZ", ".HK", ".BJ")):
        return upper
    return cleaned


def load_position_symbols(
    *,
    base_dir: Path | str = PROJECT_ROOT / "data",
    signatures: Iterable[str] = DEFAULT_SIGNATURES,
    universe_path: Path | None = None,
    stock_info_dir: Path | None = None,
) -> List[str]:
    """返回指定账本的持仓 symbol 列表（去重、保持出现顺序）。"""
    base = Path(base_dir)
    stock_info = Path(stock_info_dir) if stock_info_dir else base / "stock_info"
    universe = Path(universe_path) if universe_path else base / "universe" / "master_universe.json"
    mapping = build_name_symbol_map(universe_path=universe, stock_info_dir=stock_info)

    symbols: List[str] = []
    for signature in signatures:
        override_path = base / "agent_data" / signature / "position" / "manual_position_override.json"
        payload = _load_json(override_path)
        if not isinstance(payload, dict):
            continue
        positions = payload.get("positions") or {}
        if not isinstance(positions, dict):
            continue
        for raw_name in positions:
            if not isinstance(raw_name, str) or raw_name in _NON_SECURITY_KEYS:
                continue
            symbol = resolve_position_name(raw_name, mapping)
            if not symbol:
                continue
            if symbol != raw_name:
                LOGGER.info("持仓解析: %s -> %s (%s)", raw_name, symbol, signature)
            else:
                LOGGER.warning("持仓名称未能解析为代码，原样使用: %s (%s)", raw_name, signature)
            if symbol not in symbols:
                symbols.append(symbol)
    return symbols
