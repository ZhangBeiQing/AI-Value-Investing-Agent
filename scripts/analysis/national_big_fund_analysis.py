"""国家大基金历史持仓变动跟踪与盈亏回测分析。

数据源：
  - akshare.stock_gdfx_holding_analyse_em(date) 拉取指定季度全 A 股十大股东
  - akshare.stock_zh_a_hist(symbol, period, adjust) 拉取个股日线
  - akshare.stock_zh_a_spot_em() 拉取最新行情作为"当前价"参考

核心逻辑：
  1. 按报告期遍历过去 3 年的所有季度，从全 A 股十大股东中筛出"国家集成电路产业投资基金"
     相关股东条目（大基金一期、二期及其资管计划）。
  2. 同一只股票同一股东做时序 diff，得到"新进 / 加仓 / 减仓 / 退出"动作。
  3. 用报告期后 5 个交易日均价估算建仓/减仓价，结合最新收盘价估算盈亏。
  4. 输出 CSV + JSON + Markdown 报告到 data/analysis/national_big_fund/。

使用：
  python scripts/analysis/national_big_fund_analysis.py
  python scripts/analysis/national_big_fund_analysis.py --start 2023-06-30 --end 2026-03-31
  python scripts/analysis/national_big_fund_analysis.py --no-price   跳过行情，仅出动作表
  python scripts/analysis/national_big_fund_analysis.py --fresh        强制重抓不读缓存
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import akshare as ak
import requests
from requests.adapters import HTTPAdapter

_QUARTER_FETCH_TIMEOUT_SEC = 480  # 单季度抓取硬上限
_DEFAULT_HTTP_TIMEOUT = 15
_orig_request = requests.Session.request


def _patched_request(self, method, url, **kwargs):  # noqa: ANN001
    kwargs.setdefault("timeout", _DEFAULT_HTTP_TIMEOUT)
    return _orig_request(self, method, url, **kwargs)


requests.Session.request = _patched_request

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger


CACHE_DIR = PROJECT_ROOT / "data" / "analysis" / "national_big_fund" / "cache"
OUT_DIR = PROJECT_ROOT / "data" / "analysis" / "national_big_fund"

NATIONAL_TEAM_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("大基金一期", re.compile(r"^国家集成电路产业投资基金股份有限公司$")),
    ("大基金二期-资管", re.compile(r"华芯投资管理有限责任公司-国家集成电路产业投资基金二期股份有限公司$")),
    ("大基金二期-直接", re.compile(r"^国家集成电路产业投资基金二期股份有限公司$")),
    ("中央汇金", re.compile(r"^中央汇金(?!.*易方达)(?!.*华夏)")),
    ("中央汇金-资管计划", re.compile(r"(易方达基金|华夏基金)-中央汇金资产管理")),
    ("证金", re.compile(r"^中国证券金融")),
    ("社保", re.compile(r"^全国社保")),
    ("基本养老", re.compile(r"^基本养老保险")),
    ("国新投资", re.compile(r"^国新投资")),
    ("诚通金控", re.compile(r"^北京诚通金控")),
    ("央企军工", re.compile(r"华北计算技术研究所|中国电子科技集团")),
]

# 兼容旧代码引用
NATIONAL_FUND_PATTERNS = {
    "一期": NATIONAL_TEAM_PATTERNS[0][1],
    "二期-直接": NATIONAL_TEAM_PATTERNS[2][1],
    "二期-资管": NATIONAL_TEAM_PATTERNS[1][1],
}

QUARTER_END_DATES = [
    "03-31", "06-30", "09-30", "12-31",
]

PRICE_BUFFER_DAYS = 5  # 用报告期后 5 个交易日均价估算建仓/减仓价


@dataclass
class Holding:
    holder: str           # 股东名称
    role: str             # 国家队角色：中央汇金/证金/社保/大基金一期/...
    symbol: str
    name: str
    report_date: str      # 报告期 YYYY-MM-DD
    end_shares: float
    share_change: float
    change_ratio: float
    change_type: str      # 期末持股-持股变动（增加/减少/不变/新进）
    market_cap: float
    announce_date: Optional[str]


def list_quarters(start: date, end: date) -> list[date]:
    """枚举 start..end 之间的所有季度报告期。"""
    quarters: list[date] = []
    cursor = date(start.year, 1, 1)
    while cursor <= end:
        for md in QUARTER_END_DATES:
            m, d = md.split("-")
            q = date(cursor.year, int(m), int(d))
            if start <= q <= end:
                quarters.append(q)
        cursor = date(cursor.year + 1, 1, 1)
    return quarters


def fetch_quarter_top10(report_date: date, logger, *, fresh: bool = False) -> pd.DataFrame:
    """从 akshare 拉指定季度全 A 股十大股东，带本地缓存。"""
    date_str = report_date.strftime("%Y%m%d")
    cache_path = CACHE_DIR / f"quarter_top10_{date_str}.csv"
    if cache_path.exists() and not fresh:
        logger.info("[缓存命中] 季度十大股东: %s", cache_path.name)
        return pd.read_csv(cache_path, dtype={"股票代码": str})

    logger.info("[抓取中] 季度十大股东: %s ...", date_str)
    started = time.time()
    df = ak.stock_gdfx_holding_analyse_em(date=date_str)
    elapsed = time.time() - started
    logger.info(
        "[抓取完成] %s -> %d 行, 耗时 %.1fs",
        date_str, len(df), elapsed,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return df


def filter_national_team(df: pd.DataFrame) -> list[Holding]:
    """从全量十大股东中筛出所有「国家队」相关条目。"""
    results: list[Holding] = []
    holder_col = "股东名称"
    for _, row in df.iterrows():
        holder = str(row[holder_col] or "")
        role: Optional[str] = None
        for name, pat in NATIONAL_TEAM_PATTERNS:
            if pat.search(holder):
                role = name
                break
        if not role:
            continue
        symbol = str(row["股票代码"]).strip().zfill(6)
        if not re.match(r"^\d{6}$", symbol):
            continue
        if symbol[0] not in ("0", "3", "6", "8"):
            continue
        end_shares = float(row.get("期末持股-数量") or 0)
        results.append(
            Holding(
                holder=holder,
                role=role,
                symbol=symbol,
                name=str(row["股票简称"] or ""),
                report_date=str(row["报告期"]),
                end_shares=end_shares,
                share_change=float(row.get("期末持股-数量变化") or 0),
                change_ratio=float(row.get("期末持股-数量变化比例") or 0),
                change_type=str(row.get("期末持股-持股变动") or ""),
                market_cap=float(row.get("期末持股-流通市值") or 0),
                announce_date=str(row.get("公告日")) if pd.notna(row.get("公告日")) else None,
            )
        )
    return results


# 兼容旧调用
filter_national_fund = filter_national_team


def fetch_price_window(symbol: str, start: date, end: date, logger, *, fresh: bool = False) -> pd.DataFrame:
    """拉取单只股票 [start, end] 区间的前复权日线，带本地缓存。"""
    cache_path = CACHE_DIR / f"price_{symbol}.csv"
    if cache_path.exists() and not fresh:
        try:
            cached = pd.read_csv(cache_path, dtype={"股票代码": str})
            cached["日期"] = pd.to_datetime(cached["日期"]).dt.date
            in_range = cached[(cached["日期"] >= start) & (cached["日期"] <= end)]
            if len(in_range) > 0:
                return in_range.reset_index(drop=True)
        except Exception:
            pass

    logger.info("[抓取中] 行情: %s %s ~ %s", symbol, start, end)
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust="qfq",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[抓取失败] %s: %s", symbol, exc)
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={
        "日期": "日期", "开盘": "开盘", "收盘": "收盘",
        "最高": "最高", "最低": "最低", "成交量": "成交量", "成交额": "成交额",
    })
    df["日期"] = pd.to_datetime(df["日期"]).dt.date
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    in_range = df[(df["日期"] >= start) & (df["日期"] <= end)]
    return in_range.reset_index(drop=True)


def fetch_latest_prices(symbols: Iterable[str], logger, price_cache: Optional[dict[str, pd.DataFrame]] = None) -> dict[str, float]:
    """拉取最新全 A 行情，返回 {symbol: last_close}。失败时回退到日线缓存最后一行。"""
    logger.info("[抓取中] 全 A 最新行情 (用于最新价) ...")
    result: dict[str, float] = {}
    try:
        spot = ak.stock_zh_a_spot_em()
        for sym in symbols:
            row = spot[spot["代码"] == sym]
            if not row.empty:
                try:
                    result[sym] = float(row["最新价"].iloc[0])
                except (KeyError, ValueError):
                    continue
        logger.info("[抓取完成] stock_zh_a_spot_em 命中 %d 只", len(result))
    except Exception as exc:  # noqa: BLE001
        logger.warning("[抓取失败] stock_zh_a_spot_em: %s；回退到日线缓存", exc)
    if price_cache:
        for sym in symbols:
            if sym in result or sym not in price_cache or price_cache[sym].empty:
                continue
            try:
                result[sym] = float(price_cache[sym]["收盘"].iloc[-1])
            except (KeyError, ValueError):
                continue
        if price_cache:
            logger.info("[兜底完成] 累计获取最新价 %d 只", len(result))
    return result


def avg_price_after(price_df: pd.DataFrame, ref_date: date, n_days: int) -> Optional[float]:
    """取 ref_date 之后 n 个交易日的收盘均价；返回 None 表示数据不足。"""
    if price_df.empty:
        return None
    future = price_df[price_df["日期"] > ref_date].head(n_days)
    if future.empty:
        return None
    return float(future["收盘"].mean())


def compute_actions(holdings: list[Holding]) -> pd.DataFrame:
    """同一 (holder, symbol) 按时间排序，逐季 diff 得到动作。"""
    if not holdings:
        return pd.DataFrame()
    df = pd.DataFrame([asdict(h) for h in holdings])
    df = df.rename(columns={
        "holder": "股东名称",
        "role": "角色",
        "symbol": "股票代码",
        "name": "股票简称",
        "report_date": "报告期",
        "end_shares": "期末持股-数量",
        "share_change": "期末持股-数量变化",
        "change_ratio": "期末持股-数量变化比例",
        "change_type": "期末持股-持股变动",
        "market_cap": "期末持股-流通市值",
        "announce_date": "公告日",
    })
    df["report_date_dt"] = pd.to_datetime(df["报告期"])
    df = df.sort_values(["股东名称", "股票代码", "report_date_dt"]).reset_index(drop=True)

    actions = []
    for (holder, symbol), sub in df.groupby(["股东名称", "股票代码"]):
        sub = sub.sort_values("report_date_dt").reset_index(drop=True)
        prev_shares: Optional[float] = None
        first_seen = True
        for _, r in sub.iterrows():
            cur = float(r["期末持股-数量"])
            if first_seen:
                action = "新进"
                delta = cur
                first_seen = False
            else:
                if cur == 0 and (prev_shares or 0) > 0:
                    action = "退出"
                elif cur > (prev_shares or 0):
                    action = "加仓"
                elif cur < (prev_shares or 0):
                    action = "减仓"
                else:
                    action = "持有"
                delta = cur - (prev_shares or 0)
            actions.append({
                "股东名称": holder,
                "角色": r["角色"],
                "股票代码": symbol,
                "股票简称": r["股票简称"],
                "报告期": r["报告期"],
                "动作": action,
                "持股数变化(股)": delta,
                "本次期末持股(股)": cur,
                "上期期末持股(股)": prev_shares if prev_shares is not None else 0,
                "数量变化比例(%)": r["期末持股-数量变化比例"],
                "原报告期-持股变动": r["期末持股-持股变动"],
                "期末流通市值(元)": r["期末持股-流通市值"],
                "公告日": r["公告日"],
            })
            prev_shares = cur
    return pd.DataFrame(actions)


def compute_pnl(
    actions: pd.DataFrame,
    prices: dict[str, pd.DataFrame],
    latest_prices: dict[str, float],
    logger,
) -> pd.DataFrame:
    """对每只股票估算：建仓均价、减仓兑现均价、最新价、已实现盈亏、浮动盈亏、总盈亏。"""
    if actions.empty:
        return actions
    rows = []
    for (holder, symbol), sub in actions.groupby(["股东名称", "股票代码"]):
        sub = sub.sort_values("报告期").reset_index(drop=True)
        first_action = sub.iloc[0]
        if first_action["动作"] != "新进":
            continue
        report_date = datetime.strptime(first_action["报告期"], "%Y-%m-%d").date()
        entry_avg = avg_price_after(prices.get(symbol, pd.DataFrame()), report_date, PRICE_BUFFER_DAYS)
        current_holding = float(sub.iloc[-1]["本次期末持股(股)"])
        is_holding_now = current_holding > 0

        realized = 0.0
        realized_qty = 0.0
        for _, r in sub.iloc[1:].iterrows():
            delta = float(r["持股数变化(股)"])
            if delta >= 0:
                continue
            rpt = datetime.strptime(r["报告期"], "%Y-%m-%d").date()
            exit_avg = avg_price_after(prices.get(symbol, pd.DataFrame()), rpt, PRICE_BUFFER_DAYS)
            if exit_avg is None or entry_avg is None:
                continue
            realized += (exit_avg - entry_avg) * abs(delta)
            realized_qty += abs(delta)
        if is_holding_now and entry_avg is not None:
            latest = latest_prices.get(symbol)
            unrealized: Optional[float] = None
            unrealized_pct: Optional[float] = None
            if latest is not None:
                unrealized = (latest - entry_avg) * current_holding
                unrealized_pct = (latest / entry_avg - 1) * 100 if entry_avg > 0 else None
        else:
            latest: Optional[float] = None
            unrealized = None
            unrealized_pct = None
        latest_str = f"{latest:.2f}" if latest is not None else "N/A"
        entry_str = f"{entry_avg:.2f}" if entry_avg is not None else "N/A"
        total = (realized or 0) + (unrealized or 0)
        rows.append({
            "股东名称": holder,
            "角色": first_action["角色"],
            "股票代码": symbol,
            "股票简称": first_action["股票简称"],
            "首次进入报告期": first_action["报告期"],
            "首次进入时持股(股)": first_action["本次期末持股(股)"],
            "建仓均价估算(元)": entry_str,
            "是否仍持有": "是" if is_holding_now else "否",
            "当前持股(股)": current_holding,
            "最新收盘价(元)": latest_str,
            "已实现盈亏(元)": round(realized, 2),
            "累计已减仓股数": realized_qty,
            "浮动盈亏(元)": round(unrealized, 2) if unrealized is not None else None,
            "浮动盈亏比例(%)": round(unrealized_pct, 2) if unrealized_pct is not None else None,
            "总盈亏(元)": round(total, 2),
            "参与季度数": len(sub),
        })
    pnl = pd.DataFrame(rows)
    if pnl.empty:
        return pnl
    pnl = pnl.sort_values("总盈亏(元)", ascending=False).reset_index(drop=True)
    return pnl


def format_money(yuan: float) -> str:
    if yuan is None:
        return "N/A"
    if abs(yuan) < 1:
        return "持平"
    if yuan > 0:
        prefix = "盈"
    else:
        prefix = "亏"
        yuan = -yuan
    if yuan >= 1e8:
        return f"{prefix}{yuan / 1e8:.2f}亿"
    if yuan >= 1e4:
        return f"{prefix}{yuan / 1e4:.2f}万"
    return f"{prefix}{yuan:.0f}元"


def build_markdown_report(
    holdings: list[Holding],
    actions: pd.DataFrame,
    pnl: pd.DataFrame,
    quarters: list[date],
    start: date,
    end: date,
    focus_role: Optional[str] = None,
) -> str:
    today = date.today().strftime("%Y-%m-%d")
    n_symbols = len({h.symbol for h in holdings})
    n_holders = len({h.holder for h in holdings})

    tier_count: dict[str, set[str]] = {}
    for h in holdings:
        latest_in_tier = max(
            (x for x in holdings if x.holder == h.holder),
            key=lambda x: x.report_date,
        )
        tier_count.setdefault(latest_in_tier.role, set()).add(h.symbol)
    tier_summary = ", ".join(f"{k}={len(v)}只" for k, v in tier_count.items())

    lines: list[str] = []
    lines.append(f"# 「国家队」全谱历史持仓变动报告\n")
    lines.append(f"- 生成日期：{today}")
    lines.append(f"- 回测区间：{start} ~ {end}（共 {len(quarters)} 个季度）")
    lines.append(f"- 涉及标的：{n_symbols} 只")
    lines.append(f"- 涉及股东条目：{n_holders} 个（按角色聚合：{tier_summary}）\n")

    # 角色聚合表
    lines.append("## 一、角色分布（按 2025-12-31 最新可见快照）\n")
    lines.append("| 角色 | 持仓只数 | 估算总流通市值 | 涉及组合数 |")
    lines.append("|---|---|---|---|")
    hdf = pd.DataFrame([asdict(h) for h in holdings])
    hdf = hdf.rename(columns={
        "holder": "股东名称", "role": "角色", "symbol": "股票代码",
        "name": "股票简称", "report_date": "报告期",
        "end_shares": "期末持股-数量", "share_change": "期末持股-数量变化",
        "change_ratio": "期末持股-数量变化比例", "change_type": "期末持股-持股变动",
        "market_cap": "期末持股-流通市值", "announce_date": "公告日",
    })
    for role in sorted(tier_count.keys()):
        sub = hdf[(hdf["角色"] == role) & (hdf["报告期"] == str(end))]
        if sub.empty:
            sub = hdf[hdf["角色"] == role]
            sub = sub[sub["报告期"] == sub["报告期"].max()]
        if sub.empty:
            continue
        mcap = sub["期末持股-流通市值"].sum() / 1e8
        n_combo = sub["股东名称"].nunique()
        lines.append(f"| {role} | {sub['股票代码'].nunique()} | {mcap:.0f}亿 | {n_combo} |")
    lines.append("")

    lines.append("## 二、口径说明\n")
    lines.append(
        "1. **数据来源**：akshare 东方财富 `stock_gdfx_holding_analyse_em(date)` 拉取每季度全 A 股十大股东，"
        "再按股东名匹配「国家队」9 类角色（中央汇金 / 证金 / 社保 / 基本养老 / 国新 / 诚通 / 大基金一期 / 大基金二期-直接 / 大基金二期-资管 / 央企军工）。"
    )
    lines.append(
        "2. **建仓均价估算**：以「首次进入十大股东」季度报告期之后 5 个交易日的收盘均价作为参考建仓成本。"
        "由于十大股东披露有 1~2 个月滞后，且只能看到期末快照，**这是粗略估算**，实际建仓/减仓时点不可知。"
    )
    lines.append(
        "3. **减仓兑现估算**：以减仓动作所在季度报告期之后 5 个交易日的均价 × 减仓股数，得到估算兑现资金。"
    )
    lines.append(
        "4. **当前浮动**：以 akshare 全 A 最新行情的收盘价 × 当前仍持有股数计算。"
    )
    lines.append(
        "5. **总盈亏 = 已实现盈亏 + 浮动盈亏**。未实现收益按当前价估值，未考虑税费、印花税、资金成本。\n"
    )
    lines.append(
        "2. **建仓均价估算**：以「首次进入十大股东」季度报告期之后 5 个交易日的收盘均价作为参考建仓成本。"
        "由于十大股东披露有 1~2 个月滞后，且只能看到期末快照，**这是粗略估算**，实际建仓/减仓时点不可知。"
    )
    lines.append(
        "3. **减仓兑现估算**：以减仓动作所在季度报告期之后 5 个交易日的均价 × 减仓股数，得到估算兑现资金。"
    )
    lines.append(
        "4. **当前浮动**：以 akshare 全 A 最新行情的收盘价 × 当前仍持有股数计算。"
    )
    lines.append(
        "5. **总盈亏 = 已实现盈亏 + 浮动盈亏**。未实现收益按当前价估值，未考虑税费、印花税、资金成本。\n"
    )

    if not pnl.empty:
        win = pnl[pnl["总盈亏(元)"] > 0]
        lose = pnl[pnl["总盈亏(元)"] < 0]
        flat = pnl[pnl["总盈亏(元)"] == 0]
        total_pnl = pnl["总盈亏(元)"].sum()
        decisive = len(win) + len(lose)
        win_rate = (len(win) / decisive * 100) if decisive else 0.0
        lines.append("## 二、整体盈亏\n")
        lines.append(f"- 跟踪过的标的数：{len(pnl)}")
        lines.append(f"- 盈利标的数：{len(win)}，亏损标的数：{len(lose)}，持平：{len(flat)}")
        lines.append(f"- **胜率（盈利/有胜负）：{win_rate:.1f}%**")
        lines.append(f"- **累计估算总盈亏：{format_money(total_pnl)}**\n")
    else:
        lines.append("## 三、整体盈亏\n")
        lines.append("（无足够数据计算盈亏，可能需要更多季度或行情数据）\n")

    if not pnl.empty:
        lines.append("## 四、逐股盈亏明细（按总盈亏降序）\n")
        lines.append(
            "| 股票 | 股东 | 首次进入 | 建仓均价 | 当前价 | 当前持股 | 已实现 | 浮动 | 总盈亏 | 胜 |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for _, r in pnl.iterrows():
            if r["总盈亏(元)"] > 0:
                tag = "✓"
            elif r["总盈亏(元)"] < 0:
                tag = "✗"
            else:
                tag = "—"
            entry = r["建仓均价估算(元)"]
            latest = r["最新收盘价(元)"]
            unrealized = r["浮动盈亏(元)"]
            lines.append(
                f"| {r['股票简称']}({r['股票代码']}) | {r['股东名称']} | {r['首次进入报告期']} | {entry} | {latest} | "
                f"{r['当前持股(股)']} | {format_money(r['已实现盈亏(元)'])} | "
                f"{format_money(unrealized)} | **{format_money(r['总盈亏(元)'])}** | {tag} |"
            )
        lines.append("")

    if not actions.empty:
        lines.append("## 五、逐季增减仓动作（仅展示增减仓/退出）\n")
        sub = actions[actions["动作"].isin(["加仓", "减仓", "退出"])].copy()
        sub = sub.sort_values(["报告期", "股东名称", "股票代码"]).reset_index(drop=True)
        lines.append(
            "| 报告期 | 股票 | 股东 | 动作 | 变化股数 | 期末持股 | 公告日 |"
        )
        lines.append("|---|---|---|---|---|---|---|")
        for _, r in sub.head(120).iterrows():
            lines.append(
                f"| {r['报告期']} | {r['股票简称']}({r['股票代码']}) | {r['股东名称']} | "
                f"**{r['动作']}** | {int(r['持股数变化(股)']):+,} | {int(r['本次期末持股(股)']):,} | {r['公告日']} |"
            )
        if len(sub) > 120:
            lines.append(f"\n（仅展示前 120 条，共 {len(sub)} 条；全量见 `actions.csv`）\n")
        else:
            lines.append("")

    lines.append("## 六、风险与结论\n")
    lines.append(
        "- **数据滞后**：季报披露日往往在报告期后 1~2 个月，「看到」国家队持仓时已经晚了；且实际调仓时点不一定是报告期末。"
    )
    lines.append(
        "- **十大股东盲区**：只有进入前 10 大股东的持仓才会被记录；国家队如因股价上涨导致持股跌出前 10，本脚本会误判为「退出」。"
    )
    lines.append(
        "- **角色定位差异**：中央汇金/证金主要做金融压舱石，社保/养老做长期配置，国新/诚通投央企整合，"
        "大基金专注半导体。**不同角色的策略和持仓周期完全不一样**，不能简单加总比较。"
    )
    lines.append(
        "- **跟单限制**：国家队持仓周期通常 5~10 年，普通人资金成本、流动性需求、税务安排都不同，"
        "**「跟着国家队买」属于幸存者偏差**。建议把本表当作「国家队偏好的细分方向与公司清单」的参考，而不是「明日买卖信号」。"
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="国家大基金历史持仓盈亏回测")
    parser.add_argument("--start", default="2023-06-30", help="起始季度报告期 YYYY-MM-DD")
    parser.add_argument("--end", default="2026-03-31", help="结束季度报告期 YYYY-MM-DD")
    parser.add_argument("--no-price", action="store_true", help="跳过行情抓取，仅输出动作表")
    parser.add_argument("--fresh", action="store_true", help="强制重抓不读缓存")
    parser.add_argument("--step", choices=["all", "fetch-top10", "analyze"], default="all",
                        help="分阶段执行：fetch-top10 只抓十大股东；analyze 只跑分析（要求已抓完）")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger = init_component_logger(
        "NationalBigFund",
        group="analysis",
        filename_prefix="national_big_fund",
    )
    logger.info("回测区间: %s ~ %s", start, end)

    quarters = list_quarters(start, end)
    logger.info("待抓取季度: %s", [q.strftime("%Y%m%d") for q in quarters])
    print(f"[+] 待抓取季度: {[q.strftime('%Y%m%d') for q in quarters]}")

    holdings: list[Holding] = []
    for idx, q in enumerate(quarters, 1):
        print(f"[+] [{idx}/{len(quarters)}] 抓取季度 {q.strftime('%Y%m%d')} ...", flush=True)
        try:
            df = fetch_quarter_top10(q, logger, fresh=args.fresh)
        except Exception as exc:  # noqa: BLE001
            logger.error("[抓取失败] 季度 %s: %s", q, exc)
            print(f"[!] 季度 {q} 抓取失败: {exc}", flush=True)
            continue
        matched = filter_national_fund(df)
        logger.info(
            "[过滤] %s 大基金条目: %d 条",
            q.strftime("%Y%m%d"), len(matched),
        )
        print(f"    -> {len(matched)} 条大基金持仓", flush=True)
        holdings.extend(matched)

    if args.step == "fetch-top10":
        print("[+] --step=fetch-top10 模式，抓取完毕退出")
        return 0

    if args.step == "analyze":
        print("[+] --step=analyze 模式，从缓存读十大股东")
        for q in quarters:
            cache_path = CACHE_DIR / f"quarter_top10_{q.strftime('%Y%m%d')}.csv"
            if not cache_path.exists():
                logger.error("[缺失] 缓存不存在: %s；请先跑 fetch-top10 阶段", cache_path)
                continue
            df = pd.read_csv(cache_path, dtype={"股票代码": str})
            holdings.extend(filter_national_fund(df))

    if not holdings:
        logger.error("未抓取到任何大基金条目，请检查网络/接口")
        return 1

    holdings_df = pd.DataFrame([asdict(h) for h in holdings])
    holdings_df = holdings_df.rename(columns={
        "holder": "股东名称",
        "role": "角色",
        "symbol": "股票代码",
        "name": "股票简称",
        "report_date": "报告期",
        "end_shares": "期末持股-数量",
        "share_change": "期末持股-数量变化",
        "change_ratio": "期末持股-数量变化比例",
        "change_type": "期末持股-持股变动",
        "market_cap": "期末持股-流通市值",
        "announce_date": "公告日",
    })
    holdings_df = holdings_df.sort_values(["报告期", "股东名称", "股票代码"]).reset_index(drop=True)
    holdings_path = OUT_DIR / "holdings_quarterly.csv"
    holdings_df.to_csv(holdings_path, index=False, encoding="utf-8-sig")
    logger.info("[写入] %s (%d 行)", holdings_path, len(holdings_df))

    actions = compute_actions(holdings)
    if not actions.empty:
        actions_path = OUT_DIR / "actions.csv"
        actions.to_csv(actions_path, index=False, encoding="utf-8-sig")
        logger.info("[写入] %s (%d 行)", actions_path, len(actions))

    pnl = pd.DataFrame()
    if not args.no_price and not actions.empty:
        symbols = sorted({h.symbol for h in holdings})
        price_start = start - timedelta(days=15)
        price_end = date.today()
        logger.info("[准备] 拉取 %d 只标的的行情 (%s ~ %s)", len(symbols), price_start, price_end)
        print(f"[+] 拉取 {len(symbols)} 只标的的日线行情 (起点 {price_start}) ...", flush=True)
        prices: dict[str, pd.DataFrame] = {}
        for idx, sym in enumerate(symbols, 1):
            print(f"    [{idx}/{len(symbols)}] {sym} ...", end="", flush=True)
            df = fetch_price_window(sym, price_start, price_end, logger, fresh=args.fresh)
            print(f" {len(df)} 条" if not df.empty else " 失败/空")
            if not df.empty:
                prices[sym] = df
        latest_prices = fetch_latest_prices(symbols, logger, price_cache=prices)
        pnl = compute_pnl(actions, prices, latest_prices, logger)
        pnl_path = OUT_DIR / "pnl_per_stock.csv"
        pnl.to_csv(pnl_path, index=False, encoding="utf-8-sig")
        logger.info("[写入] %s (%d 行)", pnl_path, len(pnl))

    md = build_markdown_report(holdings, actions, pnl, quarters, start, end)
    md_path = OUT_DIR / "report.md"
    md_path.write_text(md, encoding="utf-8")
    logger.info("[写入] %s", md_path)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start": str(start),
        "end": str(end),
        "quarters": [q.strftime("%Y-%m-%d") for q in quarters],
        "n_holding_rows": len(holdings),
        "n_actions": int(len(actions)) if not actions.empty else 0,
        "n_pnl_stocks": int(len(pnl)) if not pnl.empty else 0,
        "latest_holdings": [],
    }
    if not actions.empty:
        last = actions[actions["动作"].isin(["加仓", "新进", "持有"])]
        last = last.sort_values("报告期").groupby("股票代码").tail(1)
        summary["latest_holdings"] = last[
            ["股票代码", "股票简称", "股东名称", "报告期", "本次期末持股(股)", "期末流通市值(元)"]
        ].to_dict(orient="records")
    summary_path = OUT_DIR / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[写入] %s", summary_path)

    print()
    print(md.split("## 二、口径说明")[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
