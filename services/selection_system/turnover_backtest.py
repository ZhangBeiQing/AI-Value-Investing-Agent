"""Turnover-constrained backtest with daily replacement limit and realistic costs.

Architecture inspired by qlib but simplified for our factor_store ecosystem:

    ConstrainedStrategy   →  generates daily buy/sell decisions from factor scores
    SimulatedExchange     →  executes trades (price lookup, commission, trade unit)
    Account               →  tracks cash, holdings, NAV, trade history
    run_turnover_backtest →  orchestrates the daily loop
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from core.logging import get_logger
from services.selection_system.paths import SelectionSystemPaths
from utlity.stock_utils import get_stock_data_dir, parse_symbol

LOGGER = get_logger("TurnoverBacktest")

# ── constants ──────────────────────────────────────────────────────────

DEFAULT_POOL_SIZE = 20
DEFAULT_LONG_HOLD_SIZE = 10
DEFAULT_SHORT_HOLD_SIZE = 7
DEFAULT_MAX_DAILY_REPLACE = 2
DEFAULT_INITIAL_CAPITAL = 500_000.0
DEFAULT_COMMISSION_RATE = 0.0003       # 万三
DEFAULT_MIN_COMMISSION = 5.0           # 最低5元
TRADE_UNIT = 100                       # A股 100 股
CSI300_BENCHMARK = "000300.SH"         # 沪深300基准

# ── dataclasses ───────────────────────────────────────────────────────


@dataclass
class TurnoverBacktestConfig:
    start_date: str
    end_date: str
    score_column: str = "long_score"
    pool_size: int = DEFAULT_POOL_SIZE
    hold_size: int = DEFAULT_LONG_HOLD_SIZE
    max_daily_replace: int = DEFAULT_MAX_DAILY_REPLACE
    forced_exit_days: int | None = None       # None = 不强制出池
    initial_capital: float = DEFAULT_INITIAL_CAPITAL
    commission_rate: float = DEFAULT_COMMISSION_RATE
    min_commission: float = DEFAULT_MIN_COMMISSION
    base_dir: str | Path = "data"
    output_date: str | None = None
    use_market_timing: bool = False       # 沪深300均线仓位控制


class MarketTimingSignal:
    """沪深300均线信号 + 3天确认状态机."""

    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"

    POSITION_MAP = {BULLISH: 0.95, NEUTRAL: 0.60, BEARISH: 0.25}

    def __init__(self, csi300_prices):
        self._prices = csi300_prices
        self._current_signal: str = self.BULLISH  # start optimistic
        self._pending_signal: str | None = None
        self._pending_days: int = 0

    def target_position(self) -> float:
        return self.POSITION_MAP[self._current_signal]

    def update(self, date: str) -> None:
        asof = pd.Timestamp(date).normalize()
        eligible = self._prices[self._prices.index <= asof]
        if len(eligible) < 120:
            return

        close = float(eligible.iloc[-1])
        ma60 = float(eligible.tail(60).mean())
        ma120 = float(eligible.tail(120).mean())

        if close > ma60 and close > ma120:
            raw = self.BULLISH
        elif close > ma120:
            raw = self.NEUTRAL
        else:
            raw = self.BEARISH

        if raw == self._current_signal:
            self._pending_signal = None
            self._pending_days = 0
            return

        if raw == self._pending_signal:
            self._pending_days += 1
        else:
            self._pending_signal = raw
            self._pending_days = 1

        if self._pending_days >= 3:
            self._current_signal = self._pending_signal
            self._pending_signal = None
            self._pending_days = 0


@dataclass
class Holding:
    symbol: str
    shares: int
    avg_cost: float
    entry_date: str
    days_held: int = 0
    stock_name: str = ""     # 股票名称

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price if self.current_price else 0.0

    current_price: float = 0.0  # set externally each day


# ── Account ───────────────────────────────────────────────────────────


class Account:
    def __init__(self, initial_capital: float):
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.holdings: dict[str, Holding] = {}
        self._trade_log: list[dict[str, Any]] = []
        self._daily_snapshots: list[dict[str, Any]] = []
        self.benchmark_start_price: float | None = None
        self.benchmark_price: float = 0.0

    def total_market_value(self) -> float:
        return sum(h.market_value for h in self.holdings.values())

    def total_asset(self) -> float:
        return self.cash + self.total_market_value()

    def benchmark_value(self) -> float:
        if self.benchmark_start_price and self.benchmark_start_price > 0:
            return self.initial_capital * self.benchmark_price / self.benchmark_start_price
        return self.initial_capital

    def benchmark_return(self) -> float:
        return self.benchmark_value() / self.initial_capital - 1.0

    def update_prices(self, prices: dict[str, float]) -> None:
        for sym, price in prices.items():
            if sym in self.holdings:
                self.holdings[sym].current_price = price

    def can_buy(self, price: float, shares: int, commission: float) -> bool:
        return self.cash >= price * shares + commission

    def snapshot(self, date: str) -> dict[str, Any]:
        bv = self.benchmark_value()
        tr = self.total_asset() / self.initial_capital - 1.0
        br = self.benchmark_return()
        record = {
            "date": date,
            "cash": round(self.cash, 2),
            "market_value": round(self.total_market_value(), 2),
            "total_asset": round(self.total_asset(), 2),
            "total_return": round(tr, 6),
            "benchmark_value": round(bv, 2),
            "benchmark_return": round(br, 6),
            "excess_return": round(tr - br, 6),
            "holding_count": len(self.holdings),
            "market_signal": getattr(self, "_signal", ""),
            "holdings": ",".join(f"{h.stock_name or h.symbol}({h.shares}股)" for h in self.holdings.values()),
        }
        self._daily_snapshots.append(record)
        return record

    def increment_holding_days(self) -> None:
        for h in self.holdings.values():
            h.days_held += 1


# ── SimulatedExchange ─────────────────────────────────────────────────


class SimulatedExchange:
    """Handles price lookup, trade execution, and commission calculation."""

    def __init__(
        self,
        base_dir: Path,
        commission_rate: float = DEFAULT_COMMISSION_RATE,
        min_commission: float = DEFAULT_MIN_COMMISSION,
    ):
        self._base_dir = base_dir
        self._commission_rate = commission_rate
        self._min_commission = min_commission
        self._close_cache: dict[str, pd.Series] = {}
        self._open_cache: dict[str, pd.Series] = {}

    def get_close(self, symbol: str, date: str) -> float | None:
        """收盘价，用于估值 snapshot."""
        return self._lookup(symbol, date, "收盘", self._close_cache)

    get_price = get_close  # backward compat alias

    def get_open(self, symbol: str, date: str) -> float | None:
        """开盘价，用于交易执行."""
        return self._lookup(symbol, date, "开盘", self._open_cache)

    def _lookup(self, symbol: str, date: str, column: str, cache: dict) -> float | None:
        series = cache.get(symbol)
        if series is None:
            series = self._load_price_column(symbol, column)
            cache[symbol] = series
        asof = pd.Timestamp(date).normalize()
        eligible = series[series.index == asof]
        if eligible.empty:
            # fallback to latest close <= asof
            eligible = series[series.index <= asof]
        if eligible.empty:
            return None
        return float(eligible.iloc[-1])

    def _load_price_column(self, symbol: str, column: str) -> pd.Series:
        try:
            info = parse_symbol(symbol)
        except Exception:
            return pd.Series(dtype=float)
        path = get_stock_data_dir(info, base_dir=self._base_dir) / "prices" / "price.csv"
        if not path.exists():
            return pd.Series(dtype=float)
        try:
            frame = pd.read_csv(path)
        except Exception:
            return pd.Series(dtype=float)
        if frame.empty or "日期" not in frame.columns or column not in frame.columns:
            return pd.Series(dtype=float)
        dates = pd.to_datetime(frame["日期"], errors="coerce")
        values = pd.to_numeric(frame[column], errors="coerce")
        series = pd.Series(values.values, index=dates).dropna()
        return series[~series.index.isna()].sort_index()

    def calc_commission(self, trade_value: float) -> float:
        if trade_value <= 0:
            return 0.0
        return max(trade_value * self._commission_rate, self._min_commission)

    def calc_max_shares(self, cash: float, price: float) -> int:
        """Calculate max shares affordable given cash, price, commission, and trade unit."""
        if price <= 0 or cash <= 0:
            return 0
        max_affordable = int(cash / (price * (1 + self._commission_rate)) / TRADE_UNIT) * TRADE_UNIT
        if max_affordable < TRADE_UNIT and cash >= price * TRADE_UNIT + self._min_commission:
            max_affordable = int((cash - self._min_commission) / price / TRADE_UNIT) * TRADE_UNIT
        return max(0, max_affordable)


# ── ConstrainedStrategy ───────────────────────────────────────────────


class ConstrainedStrategy:
    """Generates daily buy/sell decisions with turnover constraints."""

    def __init__(
        self,
        score_column: str,
        pool_size: int,
        hold_size: int,
        max_daily_replace: int,
        forced_exit_days: int | None,
    ):
        self.score_column = score_column
        self.pool_size = pool_size
        self.hold_size = hold_size
        self.max_daily_replace = max_daily_replace
        self.forced_exit_days = forced_exit_days

    def generate_decisions(
        self,
        date: str,
        account: Account,
        factor_frame: pd.DataFrame,
    ) -> tuple[list[str], list[str]]:
        """Return (sells, buys) lists of symbols.

        Rules:
          1. Force-exit holdings that have reached forced_exit_days (if set).
          2. Get today's top pool_size stocks by score.
          3. Candidates = top pool stocks NOT already in portfolio.
          4. Rank portfolio by today's score (worst first).
          5. For each candidate (best first), if its score > worst portfolio stock's score,
             replace up to max_daily_replace.
        """
        if factor_frame.empty or self.score_column not in factor_frame.columns:
            return [], []

        scorable = factor_frame[self.score_column].notna()
        work = factor_frame.loc[scorable].copy()
        if work.empty:
            return [], []

        sells: list[str] = []
        buys: list[str] = []

        # ── 0. force-sell holdings with NAN score (priority over everything) ──
        for sym in list(account.holdings.keys()):
            if sym not in work.index:
                sells.append(sym)
                if len(sells) >= self.max_daily_replace:
                    return sells, buys

        # ── 1. forced exits ──
        remaining = self.max_daily_replace - len(sells)
        if self.forced_exit_days is not None and remaining > 0:
            for sym in list(account.holdings.keys()):
                if sym not in sells and account.holdings[sym].days_held >= self.forced_exit_days:
                    sells.append(sym)
                    remaining -= 1
                    if remaining <= 0:
                        break

        if remaining <= 0:
            return sells, buys

        # ── 2. top pool ──
        top_symbols = work.nlargest(self.pool_size, self.score_column).index.tolist()

        # ── 3. candidates = in top pool, not in portfolio ──
        portfolio_set = set(account.holdings.keys())
        candidate_scores = [
            (sym, float(work.loc[sym, self.score_column]))
            for sym in top_symbols
            if sym not in portfolio_set
        ]
        if not candidate_scores:
            return sells, buys

        # ── 4. rank portfolio by today's score (worst first) ──
        portfolio_ranked: list[tuple[str, float]] = []
        for sym in account.holdings:
            if sym in work.index:
                portfolio_ranked.append((sym, float(work.loc[sym, self.score_column])))
        portfolio_ranked.sort(key=lambda x: x[1])  # ascending = worst first

        # ── 5. replace ──
        sold_set = set(sells)
        replace_count = 0
        for cand_sym, cand_score in candidate_scores:
            if replace_count >= remaining:
                break
            # find worst stock still in portfolio
            worst = None
            for psym, pscore in portfolio_ranked:
                if psym not in sold_set:
                    worst = (psym, pscore)
                    break
            if worst is None:
                break
            if cand_score > worst[1]:
                sells.append(worst[0])
                buys.append(cand_sym)
                sold_set.add(worst[0])
                replace_count += 1
            else:
                break   # candidates sorted descending, rest won't be better

        return sells, buys


# ── orchestrator ──────────────────────────────────────────────────────


def run_turnover_backtest(config: TurnoverBacktestConfig) -> dict[str, Any]:
    base_dir = Path(config.base_dir)
    paths = SelectionSystemPaths.from_base_dir(base_dir)

    # ── load available dates from selection_runs ──
    selection_runs_dir = base_dir / "selection_runs"
    available_dates = sorted(
        d.name
        for d in selection_runs_dir.iterdir()
        if d.is_dir()
        and (d / "13_factor_scores.csv").exists()
        and d.name >= (config.start_date or "0000")
        and d.name <= (config.end_date or "9999")
    )
    if not available_dates:
        raise FileNotFoundError(
            f"selection_runs 中没有 {config.start_date} ~ {config.end_date} 的 13_factor_scores.csv"
        )

    start_idx = 0
    end_idx = len(available_dates) - 1
    for i, d in enumerate(available_dates):
        if d >= config.start_date:
            start_idx = i
            break
    for i in range(len(available_dates) - 1, -1, -1):
        if available_dates[i] <= config.end_date:
            end_idx = i
            break
    trading_dates = available_dates[start_idx : end_idx + 1]

    LOGGER.info(
        "回测区间: %s ~ %s (%d 个交易日)",
        trading_dates[0],
        trading_dates[-1],
        len(trading_dates),
    )

    # ── initialize ──
    account = Account(config.initial_capital)
    exchange = SimulatedExchange(
        base_dir=base_dir,
        commission_rate=config.commission_rate,
        min_commission=config.min_commission,
    )
    strategy = ConstrainedStrategy(
        score_column=config.score_column,
        pool_size=config.pool_size,
        hold_size=config.hold_size,
        max_daily_replace=config.max_daily_replace,
        forced_exit_days=config.forced_exit_days,
    )

    trade_records: list[dict[str, Any]] = []
    symbol_names: dict[str, str] = {}

    # ── load benchmark (CSI 300) prices ──
    benchmark_prices = _load_benchmark_prices(base_dir / "stock_info" / "沪深300_000300.SH" / "prices" / "price.csv")
    if benchmark_prices.empty:
        LOGGER.warning("无法加载沪深300基准数据，将不输出基准对比")

    # ── market timing signal ──
    market_timing = None
    if config.use_market_timing and not benchmark_prices.empty:
        market_timing = MarketTimingSignal(benchmark_prices)

    # ── daily loop ──
    for i, date in enumerate(trading_dates):
        # 1. load factor scores from previous trading day (trade on D using D-1 scores)
        factor_date = trading_dates[i - 1] if i > 0 else date
        score_path = selection_runs_dir / factor_date / "13_factor_scores.csv"
        if not score_path.exists():
            continue
        factor_frame = pd.read_csv(score_path)
        if "symbol" in factor_frame.columns:
            factor_frame = factor_frame.set_index("symbol")
        # build name mapping (accumulate across all dates)
        if "stock_name" in factor_frame.columns:
            for sym, row in factor_frame.iterrows():
                name = row.get("stock_name")
                if name and pd.notna(name) and str(sym) not in symbol_names:
                    symbol_names[str(sym)] = str(name)

        # 2. update prices: close for mark-to-market
        close_prices: dict[str, float] = {}
        for sym in account.holdings:
            p = exchange.get_close(sym, date)
            if p is not None:
                close_prices[sym] = p
        account.update_prices(close_prices)

        # 2.2. force-sell any holding with NAN score (PE turned negative, etc.)
        scorable_mask = factor_frame[config.score_column].notna() if config.score_column in factor_frame.columns else pd.Series(False, index=factor_frame.index)
        for sym in list(account.holdings.keys()):
            if sym not in factor_frame.index or not scorable_mask.get(sym, False):
                h = account.holdings[sym]
                price = exchange.get_open(sym, date)
                if price is None or price <= 0:
                    continue
                proceeds = price * h.shares
                commission = exchange.calc_commission(proceeds)
                account.cash += proceeds - commission
                trade_records.append({
                    "date": date, "action": "sell", "symbol": sym,
                    "shares": h.shares, "price": price, "amount": proceeds,
                    "commission": commission,
                    "hold_days": h.days_held,
                    "return_pct": round(price / h.avg_cost - 1.0, 6) if h.avg_cost and price else None,
                    "reason": "score_invalid",
                })
                del account.holdings[sym]

        # 2.5. market timing: update signal, adjust effective hold_size
        effective_hold_size = config.hold_size
        if config.use_market_timing and market_timing:
            market_timing.update(date)
            account._signal = market_timing._current_signal
            target_pct = market_timing.target_position()
            effective_hold_size = max(1, int(config.hold_size * target_pct + 0.5))

        # 3. generate decisions (skip normal trading when market timing needs to reduce)
        if len(account.holdings) < effective_hold_size:
            # ── building phase: buy up to max_daily_replace per day until full ──
            scorable = factor_frame[factor_frame[config.score_column].notna()]
            if not scorable.empty:
                existing = set(account.holdings.keys())
                targets = scorable.nlargest(max(effective_hold_size * 2, config.pool_size), config.score_column)
                to_buy = [s for s in targets.index if s not in existing]
                slots = effective_hold_size - len(account.holdings)
                batch = min(slots, config.max_daily_replace, len(to_buy))
                total_asset = account.cash + account.total_market_value()
                position_pct = market_timing.target_position() if market_timing else 1.0
                target_per_stock = total_asset * position_pct / max(effective_hold_size, 1)
                cash_per_batch = min(account.cash, batch * target_per_stock)
                cash_per_stock = cash_per_batch / max(batch, 1) if batch > 0 else 0.0
                bought = 0
                for sym in to_buy[:effective_hold_size]:
                    if bought >= batch:
                        break
                    price = exchange.get_open(sym, date)
                    if price is None or price <= 0:
                        continue
                    shares = exchange.calc_max_shares(cash_per_stock, price)
                    if shares == 0:
                        continue
                    cost = price * shares
                    commission = exchange.calc_commission(cost)
                    if not account.can_buy(price, shares, commission):
                        continue
                    account.cash -= cost + commission
                    account.holdings[sym] = Holding(
                        symbol=sym, shares=shares, avg_cost=price,
                        entry_date=date, days_held=0, current_price=price,
                    )
                    trade_records.append({
                        "date": date, "action": "buy", "symbol": sym,
                        "shares": shares, "price": price, "amount": cost,
                        "commission": commission, "reason": "init",
                    })
                    bought += 1
        else:
            # ── normal day: constrained turnover ──
            sells, buys = strategy.generate_decisions(date, account, factor_frame)

            # pair sells with buys: sell pool, then buy each at target_per_stock
            total_asset = account.cash + account.total_market_value()
            position_pct = market_timing.target_position() if market_timing else 1.0
            target_per_stock = total_asset * position_pct / max(effective_hold_size, 1)
            buyable_pairs: list[tuple[str, str, float, int]] = []
            pending_sells: list[str] = []
            pooled_cash = account.cash  # accumulate sell proceeds here

            for i, sell_sym in enumerate(sells):
                is_forced = (
                    config.forced_exit_days is not None
                    and sell_sym in account.holdings
                    and account.holdings[sell_sym].days_held >= config.forced_exit_days
                )
                h = account.holdings.get(sell_sym)
                sell_price = exchange.get_open(sell_sym, date) if h else None
                if sell_price is None or sell_price <= 0:
                    if is_forced:
                        LOGGER.debug("强制出池跳过(无价格): %s %s", sell_sym, date)
                    continue
                sell_proceeds = sell_price * h.shares if h else 0.0

                if i < len(buys):
                    buy_sym = buys[i]
                    buy_price = exchange.get_open(buy_sym, date)
                    if buy_price and buy_price > 0:
                        pool_after_sell = pooled_cash + sell_proceeds
                        cash_for_this = min(target_per_stock, pool_after_sell / max(len(buys) - i, 1))
                        buy_shares = exchange.calc_max_shares(cash_for_this, buy_price)
                        if buy_shares > 0:
                            buyable_pairs.append((sell_sym, buy_sym, buy_price, buy_shares))
                            pooled_cash += sell_proceeds  # accumulate for remaining buys
                            continue
                if is_forced:
                    pending_sells.append(sell_sym)

            # execute paired sell+buy
            for sell_sym, buy_sym, buy_price, buy_shares in buyable_pairs:
                # sell
                h = account.holdings[sell_sym]
                sell_price = exchange.get_open(sell_sym, date)
                proceeds = sell_price * h.shares if sell_price else 0.0
                commission = exchange.calc_commission(proceeds) if proceeds else 0.0
                account.cash += proceeds - commission
                trade_records.append({
                    "date": date, "action": "sell", "symbol": sell_sym,
                    "shares": h.shares, "price": sell_price, "amount": proceeds,
                    "commission": commission,
                    "hold_days": h.days_held,
                    "return_pct": round(sell_price / h.avg_cost - 1.0, 6) if h.avg_cost and sell_price else None,
                    "reason": "replaced",
                })
                del account.holdings[sell_sym]

                # buy (safety: reduce shares if cash insufficient)
                while buy_shares >= TRADE_UNIT:
                    cost = buy_price * buy_shares
                    commission = exchange.calc_commission(cost)
                    if account.cash >= cost + commission:
                        break
                    buy_shares -= TRADE_UNIT
                if buy_shares < TRADE_UNIT:
                    continue  # can't afford even 1 lot
                cost = buy_price * buy_shares
                commission = exchange.calc_commission(cost)
                account.cash -= cost + commission
                account.holdings[buy_sym] = Holding(
                    symbol=buy_sym, shares=buy_shares, avg_cost=buy_price,
                    entry_date=date, days_held=0, current_price=buy_price,
                )
                trade_records.append({
                    "date": date, "action": "buy", "symbol": buy_sym,
                    "shares": buy_shares, "price": buy_price, "amount": cost,
                    "commission": commission, "reason": "replace",
                })

            # execute forced exits (no replacement needed)
            for sell_sym in pending_sells:
                h = account.holdings.get(sell_sym)
                if not h:
                    continue
                sell_price = exchange.get_open(sell_sym, date)
                if sell_price is None or sell_price <= 0:
                    continue
                proceeds = sell_price * h.shares
                commission = exchange.calc_commission(proceeds)
                account.cash += proceeds - commission
                trade_records.append({
                    "date": date, "action": "sell", "symbol": sell_sym,
                    "shares": h.shares, "price": sell_price, "amount": proceeds,
                    "commission": commission,
                    "hold_days": h.days_held,
                    "return_pct": round(sell_price / h.avg_cost - 1.0, 6) if h.avg_cost and sell_price else None,
                    "reason": "forced_exit",
                })
                del account.holdings[sell_sym]

        # 3.1. light rebalancing: trim positions deviating >20% from target
        target_mv = (account.cash + account.total_market_value()) * (market_timing.target_position() if market_timing else 1.0) / max(effective_hold_size, 1)
        for sym in list(account.holdings.keys()):
            h = account.holdings[sym]
            if target_mv <= 0:
                continue
            deviation = h.market_value / target_mv - 1.0
            if deviation > 0.2:
                # overweight: sell excess (>20% above target)
                excess_shares = int(h.shares * (deviation - 0.2) / (deviation + 1.0))
                excess_shares = max(excess_shares // TRADE_UNIT * TRADE_UNIT, TRADE_UNIT)
                if excess_shares >= TRADE_UNIT and excess_shares < h.shares:
                    price = exchange.get_open(sym, date)
                    if price and price > 0:
                        proceeds = price * excess_shares
                        commission = exchange.calc_commission(proceeds)
                        account.cash += proceeds - commission
                        h.shares -= excess_shares
                        trade_records.append({
                            "date": date, "action": "sell", "symbol": sym,
                            "shares": excess_shares, "price": price, "amount": proceeds,
                            "commission": commission, "reason": "rebalance",
                        })
            elif deviation < -0.2 and account.cash > 0:
                # underweight: buy more (up to target)
                shortfall = target_mv - h.market_value
                budget = min(shortfall, account.cash * 0.5)  # don't use all cash
                price = exchange.get_open(sym, date)
                if price and price > 0:
                    add_shares = exchange.calc_max_shares(budget, price)
                    if add_shares >= TRADE_UNIT:
                        cost = price * add_shares
                        commission = exchange.calc_commission(cost)
                        if account.cash >= cost + commission:
                            account.cash -= cost + commission
                            h.shares += add_shares
                            h.avg_cost = (h.avg_cost * (h.shares - add_shares) + price * add_shares) / h.shares if h.shares > 0 else price
                            trade_records.append({
                                "date": date, "action": "buy", "symbol": sym,
                                "shares": add_shares, "price": price, "amount": cost,
                                "commission": commission, "reason": "rebalance",
                            })

        # 3.5. market timing: sell excess if holdings > effective_hold_size
        excess_count = len(account.holdings) - effective_hold_size
        if excess_count > 0 and config.use_market_timing and market_timing:
            # rank current holdings by today's score (worst first)
            holding_scores = []
            for sym in account.holdings:
                sc = float(factor_frame.loc[sym, config.score_column]) if sym in factor_frame.index and pd.notna(factor_frame.loc[sym, config.score_column]) else 0.0
                holding_scores.append((sym, sc))
            holding_scores.sort(key=lambda x: x[1])
            to_sell = min(excess_count, config.max_daily_replace)
            for i in range(to_sell):
                sym = holding_scores[i][0]
                h = account.holdings.get(sym)
                if not h:
                    continue
                price = exchange.get_open(sym, date)
                if price is None or price <= 0:
                    continue
                proceeds = price * h.shares
                commission = exchange.calc_commission(proceeds)
                account.cash += proceeds - commission
                trade_records.append({
                    "date": date, "action": "sell", "symbol": sym,
                    "shares": h.shares, "price": price, "amount": proceeds,
                    "commission": commission,
                    "hold_days": h.days_held,
                    "return_pct": round(price / h.avg_cost - 1.0, 6) if h.avg_cost and price else None,
                    "reason": "timing_reduce",
                })
                del account.holdings[sym]

        # 4. end-of-day: update prices, benchmark, record snapshot
        # update benchmark
        if not benchmark_prices.empty:
            asof = pd.Timestamp(date).normalize()
            bench_eligible = benchmark_prices[benchmark_prices.index <= asof]
            if not bench_eligible.empty:
                account.benchmark_price = float(bench_eligible.iloc[-1])
                if account.benchmark_start_price is None:
                    account.benchmark_start_price = account.benchmark_price

        prices: dict[str, float] = {}
        for sym in account.holdings:
            p = exchange.get_close(sym, date)
            if p is not None:
                prices[sym] = p
        account.update_prices(prices)
        if market_timing:
            account._signal = market_timing._current_signal
        account.snapshot(date)
        account.increment_holding_days()

    # ── build results ──
    daily_df = pd.DataFrame(account._daily_snapshots)
    trade_df = pd.DataFrame(trade_records)

    if daily_df.empty:
        return {"status": "empty", "message": "无交易日数据"}

    trading_day_count = len(daily_df)
    final_asset = account.total_asset()
    total_return = final_asset / config.initial_capital - 1.0

    # benchmark metrics
    benchmark_final = account.benchmark_value()
    benchmark_return = benchmark_final / config.initial_capital - 1.0
    excess_return = total_return - benchmark_return
    if trading_day_count > 1:
        years = trading_day_count / 252.0
        benchmark_annual = (benchmark_final / config.initial_capital) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    else:
        benchmark_annual = 0.0
    info_ratio = _calc_info_ratio(daily_df) if "excess_return" in daily_df.columns else None

    # calculate annualized return
    if trading_day_count > 1:
        years = trading_day_count / 252.0
        annual_return = (final_asset / config.initial_capital) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    else:
        annual_return = 0.0

    # max drawdown
    cummax = daily_df["total_asset"].cummax()
    drawdown = (daily_df["total_asset"] - cummax) / cummax
    max_dd = float(drawdown.min()) if not drawdown.empty else 0.0

    # trade statistics
    buy_count = len(trade_df[trade_df["action"] == "buy"]) if not trade_df.empty else 0
    sell_count = len(trade_df[trade_df["action"] == "sell"]) if not trade_df.empty else 0
    total_commission = float(trade_df["commission"].sum()) if not trade_df.empty else 0.0

    # output paths
    target_date = config.output_date or config.end_date
    paths.ensure_run_dir(target_date)
    score_stem = config.score_column.removesuffix("_score")
    detail_path = paths.run_dir(target_date) / f"12_turnover_backtest_{score_stem}_detail.csv"
    trade_path = paths.run_dir(target_date) / f"12_turnover_backtest_{score_stem}_trades.csv"
    summary_path = paths.run_dir(target_date) / f"12_turnover_backtest_{score_stem}.json"

    # add stock names to daily holdings display
    if symbol_names:
        def _name_holdings(row_holdings: str) -> str:
            if not isinstance(row_holdings, str) or not row_holdings:
                return row_holdings
            parts = []
            for item in row_holdings.split(","):
                sym = item.split("(")[0] if "(" in item else item
                name = symbol_names.get(sym, "")
                display = f"{name}({item.split('(')[1]}" if "(" in item and name else item
                if not name:
                    display = item
                else:
                    display = f"{name}({item.split('(')[1]}" if "(" in item else name
                parts.append(display)
            return ",".join(parts)
        daily_df["holdings"] = daily_df["holdings"].apply(_name_holdings)

    daily_df.to_csv(detail_path, index=False)
    if not trade_df.empty:
        # add stock_name to trade records
        trade_df["stock_name"] = trade_df["symbol"].map(symbol_names).fillna("")
        # reorder: date, action, symbol, stock_name, ...
        cols = ["date", "action", "symbol", "stock_name"] + [c for c in trade_df.columns if c not in ("date", "action", "symbol", "stock_name")]
        trade_df = trade_df[cols]
        trade_df.to_csv(trade_path, index=False)

    # ── stock-level P&L summary ──
    stock_summary_path = paths.run_dir(target_date) / f"12_turnover_backtest_{score_stem}_stocks.csv"
    _write_stock_summary(trade_df, account, symbol_names, stock_summary_path)

    # ── summary ──
    summary = {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(),
        "method": "turnover_constrained_backtest",
        "score_column": config.score_column,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "initial_capital": config.initial_capital,
        "pool_size": config.pool_size,
        "hold_size": config.hold_size,
        "max_daily_replace": config.max_daily_replace,
        "forced_exit_days": config.forced_exit_days,
        "commission_rate": config.commission_rate,
        "min_commission": config.min_commission,
        "results": {
            "final_asset": round(final_asset, 2),
            "total_return_pct": round(total_return * 100, 2),
            "annual_return_pct": round(annual_return * 100, 2),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "sharpe_ratio": _calc_sharpe(daily_df),
            "benchmark_return_pct": round(benchmark_return * 100, 2),
            "benchmark_annual_pct": round(benchmark_annual * 100, 2),
            "excess_return_pct": round(excess_return * 100, 2),
            "information_ratio": info_ratio,
            "trading_days": trading_day_count,
            "total_commission": round(total_commission, 2),
            "buy_trades": int(buy_count),
            "sell_trades": int(sell_count),
        },
    }

    import json
    from services.selection_system.store import save_json_file
    save_json_file(summary_path, summary)

    LOGGER.info(
        "换仓约束回测完成: score=%s 总收益=%.2f%% 年化=%.2f%% 最大回撤=%.2f%% 基准=%.2f%% 超额=%.2f%%",
        config.score_column,
        total_return * 100,
        annual_return * 100,
        max_dd * 100,
        benchmark_return * 100,
        excess_return * 100,
    )
    return {
        "summary_json": summary_path,
        "detail_csv": detail_path,
        "trade_csv": trade_path,
        "stock_csv": stock_summary_path,
        "summary": summary,
    }


def _calc_sharpe(daily_df: pd.DataFrame, risk_free: float = 0.02) -> float:
    if len(daily_df) < 2:
        return 0.0
    daily_returns = daily_df["total_return"].diff().dropna()
    if daily_returns.empty or daily_returns.std() == 0:
        return 0.0
    excess = daily_returns.mean() * 252 - risk_free
    vol = daily_returns.std() * np.sqrt(252)
    return float(excess / vol) if vol > 0 else 0.0


def _calc_info_ratio(daily_df: pd.DataFrame) -> float | None:
    """Information ratio = mean(excess_return) / std(excess_return) * sqrt(252)."""
    if len(daily_df) < 2 or "excess_return" not in daily_df.columns:
        return None
    daily_excess = daily_df["excess_return"].diff().dropna()
    if daily_excess.empty or daily_excess.std() == 0:
        return None
    return float(daily_excess.mean() / daily_excess.std() * np.sqrt(252))


def _load_benchmark_prices(path: Path) -> pd.Series:
    if not path.exists():
        return pd.Series(dtype=float)
    try:
        frame = pd.read_csv(path)
    except Exception:
        return pd.Series(dtype=float)
    if frame.empty or "日期" not in frame.columns or "收盘" not in frame.columns:
        return pd.Series(dtype=float)
    dates = pd.to_datetime(frame["日期"], errors="coerce")
    close = pd.to_numeric(frame["收盘"], errors="coerce")
    series = pd.Series(close.values, index=dates).dropna()
    return series[~series.index.isna()].sort_index()


def _write_stock_summary(trade_df: pd.DataFrame, account: Account, symbol_names: dict[str, str], output_path: Path) -> None:
    """Generate per-stock P&L summary including unrealized gains on current holdings."""
    cols = ["symbol", "stock_name", "buy_count", "sell_count", "total_buy", "total_sell", "unrealized_value", "net_pnl", "avg_return_pct", "avg_hold_days"]
    if trade_df.empty:
        pd.DataFrame(columns=cols).to_csv(output_path, index=False)
        return

    sells = trade_df[trade_df["action"] == "sell"].copy()
    buys = trade_df[trade_df["action"] == "buy"].copy()

    sell_agg_cols: dict[str, Any] = {"sell_count": ("shares", "count"), "total_sell": ("amount", "sum")}
    if "return_pct" in sells.columns:
        sell_agg_cols["avg_return_pct"] = ("return_pct", "mean")
    if "hold_days" in sells.columns:
        sell_agg_cols["avg_hold_days"] = ("hold_days", "mean")

    sell_agg = sells.groupby("symbol").agg(**sell_agg_cols).reset_index() if not sells.empty else pd.DataFrame(columns=["symbol"] + list(sell_agg_cols.keys()))

    buy_agg = buys.groupby("symbol").agg(
        buy_count=("shares", "count"),
        total_buy=("amount", "sum"),
    ).reset_index()

    merged = buy_agg.merge(sell_agg, on="symbol", how="outer").fillna(0)

    # add unrealized value for currently held stocks
    unrealized_values: dict[str, float] = {}
    for sym, h in account.holdings.items():
        unrealized_values[sym] = h.market_value if h.current_price else 0.0

    merged["unrealized_value"] = merged["symbol"].map(unrealized_values).fillna(0.0)
    merged["net_pnl"] = merged["total_sell"] + merged["unrealized_value"] - merged["total_buy"]
    merged["stock_name"] = merged["symbol"].map(symbol_names).fillna("")
    merged = merged.sort_values("net_pnl", ascending=False)

    out_cols = ["symbol", "stock_name", "buy_count", "sell_count", "total_buy", "total_sell", "unrealized_value", "net_pnl"]
    for extra in ("avg_return_pct", "avg_hold_days"):
        if extra in merged.columns:
            out_cols.append(extra)
    merged = merged[out_cols]
    for col in ["buy_count", "sell_count"]:
        merged[col] = merged[col].astype(int)
    for col in ["total_buy", "total_sell", "unrealized_value", "net_pnl"]:
        merged[col] = merged[col].round(2)

    merged.to_csv(output_path, index=False)
    LOGGER.info("股票盈亏汇总已保存: %s (%d 只)", output_path, len(merged))


__all__ = [
    "TurnoverBacktestConfig",
    "ConstrainedStrategy",
    "Account",
    "SimulatedExchange",
    "run_turnover_backtest",
]
