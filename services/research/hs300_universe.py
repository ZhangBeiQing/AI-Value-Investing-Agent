"""沪深300 历史成分股（动态 universe）辅助模块。

用 baostock 的 query_hs300_stocks(date) 取任意历史交易日的沪深300成分，
缓存到本地后供回测按调仓日重建 PIT（point-in-time）股票池，避免幸存者偏差。

baostock 是免费开源库，仅用于获取成分名单；价格/财报仍走 shared_data_access。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

from commons.stock_utils import SymbolInfo
from core.logging import get_logger
from services.selection_system.master_universe import load_master_universe
from services.selection_system.models import MasterUniverseStock


LOGGER = get_logger("Hs300Universe")

MEMBERSHIP_FILENAME = "hs300_membership.json"


def _membership_path(base_dir: str | Path) -> Path:
    return Path(base_dir) / "magic_formula" / MEMBERSHIP_FILENAME


def _baostock_symbol_to_std(code: str) -> str:
    """sh.600000 -> 600000.SH"""
    market, ticker = code.split(".")
    suffix = "SH" if market.lower() == "sh" else "SZ"
    return f"{ticker}.{suffix}"


def _sanitize_name(name: str) -> str:
    return (name or "").replace("/", "_").replace(" ", "").strip()


def fetch_membership_snapshots(years: Iterable[int]) -> dict[str, dict[str, str]]:
    """用 baostock 抓取每个年度起点对应的沪深300成分，返回 {年份: {symbol: name}}。"""
    import baostock as bs

    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {login.error_msg}")
    snapshots: dict[str, dict[str, str]] = {}
    try:
        for year in years:
            rs = bs.query_hs300_stocks(date=f"{year}-01-01")
            if rs.error_code != "0":
                LOGGER.warning("查询 %s 年沪深300成分失败: %s", year, rs.error_msg)
                continue
            members: dict[str, str] = {}
            while rs.next():
                row = rs.get_row_data()  # updateDate, code, code_name
                members[_baostock_symbol_to_std(row[1])] = _sanitize_name(row[2])
            snapshots[str(year)] = members
            LOGGER.info("沪深300 %s 年成分: %d 只 (快照日 %s)", year, len(members), row[0] if members else "")
    finally:
        bs.logout()
    return snapshots


def load_or_fetch_membership(base_dir: str | Path, years: Sequence[int], *, force: bool = False) -> dict[str, dict[str, str]]:
    path = _membership_path(base_dir)
    if path.exists() and not force:
        payload = json.loads(path.read_text(encoding="utf-8"))
        snapshots = payload.get("years", {})
        if all(str(y) in snapshots for y in years):
            return snapshots
    snapshots = fetch_membership_snapshots(years)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"updated_at": datetime.now().isoformat(timespec="seconds"), "years": snapshots}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return snapshots


def membership_for_year(snapshots: dict[str, dict[str, str]], year: int) -> dict[str, str]:
    return snapshots.get(str(year), {})


def union_symbols(snapshots: dict[str, dict[str, str]]) -> dict[str, str]:
    union: dict[str, str] = {}
    for members in snapshots.values():
        union.update(members)
    return union


def build_master_stock(symbol: str, name: str) -> MasterUniverseStock:
    """构造 build_cross_section 需要的轻量股票对象。sector/industry 未知留空。"""
    return MasterUniverseStock(symbol=symbol, name=name or symbol, sector="", industry="", stock_type="growth")


def union_stocks(snapshots: dict[str, dict[str, str]]) -> list[MasterUniverseStock]:
    return [build_master_stock(sym, name) for sym, name in sorted(union_symbols(snapshots).items())]


def membership_stocks_for_year(snapshots: dict[str, dict[str, str]], year: int) -> list[MasterUniverseStock]:
    return [build_master_stock(sym, name) for sym, name in sorted(membership_for_year(snapshots, year).items())]


def build_symbol_info(symbol: str, name: str) -> SymbolInfo:
    code, suffix = symbol.split(".")
    return SymbolInfo(
        symbol=symbol,
        code=code,
        suffix=suffix,
        market="CN_A",
        calendar="CN",
        stock_name=_sanitize_name(name) or symbol,
        description="",
    )


def ensure_symbols_data(
    base_dir: str | Path,
    stocks: Sequence[MasterUniverseStock],
    *,
    lookback_price_days: int = 7600,
) -> dict[str, int]:
    """批量抓取长期价格+财报缓存，返回成功/失败计数。"""
    from shared_data_access.cache_registry import ensure_symbol_data

    ok = 0
    failed: list[str] = []
    for idx, stock in enumerate(stocks, 1):
        symbol_info = build_symbol_info(stock.symbol, stock.name)
        try:
            ensure_symbol_data(
                base_dir,
                symbol_info,
                LOGGER,
                lookback_price_days=lookback_price_days,
                force_refresh_price=False,
            )
            ok += 1
        except Exception as exc:  # pragma: no cover - 网络/退市等
            failed.append(stock.symbol)
            LOGGER.warning("抓取 %s %s 失败: %s", stock.symbol, stock.name, exc)
        if idx % 25 == 0:
            LOGGER.info("抓取进度 %d/%d (成功 %d, 失败 %d)", idx, len(stocks), ok, len(failed))
    if failed:
        LOGGER.warning("抓取失败 %d 只: %s", len(failed), failed[:20])
    return {"ok": ok, "failed": len(failed)}


__all__ = [
    "fetch_membership_snapshots",
    "load_or_fetch_membership",
    "membership_for_year",
    "membership_stocks_for_year",
    "union_symbols",
    "union_stocks",
    "build_master_stock",
    "build_symbol_info",
    "ensure_symbols_data",
]
