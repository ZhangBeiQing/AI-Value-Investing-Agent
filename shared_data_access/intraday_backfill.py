"""用 1 分钟 K 线聚合日 K，回填 price.csv 当日缺一日的数据。

日均数据更新延迟到晚上 9 点以后，尤其是港股和 ETF 更慢。
本模块在检测到 price.csv 最新日期刚好差一天时，用盘中分钟线数据
聚合出当日 OHLCV，写回 price.csv。

安全锁：
- 今天必须是交易日
- 只缺一天（多数日缺失说明数据问题，不回填）  
- 必须收盘后（盘中分钟线数据不完整）
- 分时数据行数 >= 180（少于 3 小时不完整）
- 异常不阻断主流程
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from core.logging import get_logger
from utlity.stock_utils import get_last_trading_day, get_latest_trading_day, is_trading_day


_LOGGER = get_logger("IntradayBackfill")

# 市场收盘时间 (hour, minute)
_MARKET_CLOSE_TIME: dict[str, tuple[int, int]] = {
    "CN": (15, 0),
    "HK": (16, 0),
}

# 收盘后额外等待缓冲（分钟）
_CLOSE_BUFFER_MINUTES = 10

# 最少需要的分钟线行数（3 小时 × 60 分钟）
_MIN_BAR_COUNT = 180

# 日期列名（各市场统一）
_DATE_COL = "日期"


def _parse_date(value) -> Optional[date]:
    """将字符串或 Timestamp 转换为 date 对象。"""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


class IntradayBackfillProvider:
    """按股票调用分时接口聚合日 K 并回填 price.csv。"""

    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or _LOGGER

    # ---- 主入口 -------------------------------------------------------

    def backfill_if_needed(self, symbol_info, price_csv_path: str | Path) -> bool:
        """
        判定是否需要回填，如果需要则执行。

        返回 True 表示「成功回填」，返回 False 表示「不满足触发条件」或「失败」。
        失败不抛异常。
        """
        price_csv_path = Path(price_csv_path)
        symbol = getattr(symbol_info, "symbol", str(symbol_info))

        try:
            result = self._try_backfill(symbol_info, price_csv_path)
            return result
        except Exception as exc:
            self.logger.warning(
                "%s 分时回填异常(不阻断主流程): %s",
                symbol,
                exc,
            )
            return False

    def _try_backfill(self, symbol_info, price_csv_path: Path) -> bool:
        symbol = getattr(symbol_info, "symbol", str(symbol_info))
        market = self._market_calendar(symbol_info)
        today = date.today()

        # ▸ 锁 1: 今天必须是交易日（周末/节假日不操作）
        if not is_trading_day(today, market, logger=self.logger):
            return False

        # 最近交易日（今天如果是交易日，就是今天；今天不是交易日的话，前面已经返回了）
        latest_trading_day = get_latest_trading_day(today, market, logger=self.logger)

        # ▸ 读取 price.csv
        if not price_csv_path.exists():
            return False
        try:
            df_existing = pd.read_csv(price_csv_path)
        except Exception:
            return False
        if df_existing.empty or _DATE_COL not in df_existing.columns:
            return False

        latest_in_csv = _parse_date(df_existing[_DATE_COL].iloc[-1])
        if latest_in_csv is None:
            return False

        # ▸ 锁 2: 只差一天（price.csv 最新日期 == 最近交易日的前一个交易日）
        prev_trading_day = get_last_trading_day(latest_trading_day, market, logger=self.logger)
        if latest_in_csv != prev_trading_day:
            # 已是最新（差零天）或多天缺失 → 不触发
            return False

        # ▸ 锁 3: 必须收盘后
        close_hour, close_min = _MARKET_CLOSE_TIME.get(market, (15, 0))
        close_deadline = time(close_hour, close_min)
        buffer_delta = timedelta(minutes=_CLOSE_BUFFER_MINUTES)
        close_with_buffer = (
            datetime.combine(date.today(), close_deadline) + buffer_delta
        ).time()
        now = datetime.now().time()
        if now < close_with_buffer:
            self.logger.info(
                "%s 未到收盘时间(%s:%02d + %dmin 缓冲)，跳过分时回填",
                symbol,
                close_hour,
                close_min,
                _CLOSE_BUFFER_MINUTES,
            )
            return False

        # ▸ 拉取分时数据
        self.logger.info("%s 开始分时回填 target=%s", symbol, latest_trading_day)
        min_bars = self._fetch_min_bars(symbol_info)
        if min_bars is None or min_bars.empty:
            self.logger.warning("%s 分时数据为空，跳过回填", symbol)
            return False

        # ▸ 筛选今天 + 排除集合竞价
        min_bars["_dt"] = pd.to_datetime(min_bars["时间"])
        min_bars = min_bars[min_bars["_dt"].dt.date == latest_trading_day].copy()
        min_bars = min_bars[min_bars["_dt"].dt.time >= time(9, 30)]

        # ▸ 锁 4: 行数不够（盘中数据不完整）
        if len(min_bars) < _MIN_BAR_COUNT:
            self.logger.info(
                "%s 当日分钟线仅 %d 行 (< %d)，可能盘中数据不完整，跳过回填",
                symbol,
                len(min_bars),
                _MIN_BAR_COUNT,
            )
            return False

        # ▸ 聚合日 K
        prev_row = df_existing.iloc[-1].to_dict()
        daily = self._aggregate_daily(
            min_bars, df_existing.columns.tolist(), market, prev_row, symbol
        )
        if daily is None:
            return False

        # ▸ 写回
        self._upsert_price_csv(df_existing, daily, price_csv_path, symbol)
        return True

    # ---- 分时数据拉取 -------------------------------------------------

    def _fetch_min_bars(self, symbol_info) -> Optional[pd.DataFrame]:
        """根据市场类型调用对应的 akshare 1 分钟 K 线接口。带重试和回退。"""
        code = getattr(symbol_info, "code", "")
        import akshare as ak

        if getattr(symbol_info, "is_hk_market", None) and symbol_info.is_hk_market():
            fetch_fn = lambda: ak.stock_hk_hist_min_em(symbol=code, period="1")
        elif getattr(symbol_info, "market", "") == "CN_INDEX":
            fetch_fn = lambda: ak.index_zh_a_hist_min_em(symbol=code, period="1")
        elif code.startswith(("51", "58", "15", "16", "50", "53")):
            fetch_fn = lambda: ak.fund_etf_hist_min_em(symbol=code, period="1")
        else:
            fetch_fn = lambda: ak.stock_zh_a_hist_min_em(symbol=code, period="1")

        return _retry_fetch(fetch_fn, symbol=getattr(symbol_info, "symbol", code), logger=self.logger)

    # ---- 市场日历 ----------------------------------------------------

    def _market_calendar(self, symbol_info) -> str:
        if getattr(symbol_info, "is_hk_market", None) and symbol_info.is_hk_market():
            return "HK"
        return "CN"

    # ---- 聚合日 K ----------------------------------------------------

    def _aggregate_daily(
        self,
        min_bars: pd.DataFrame,
        existing_columns: list[str],
        market: str,
        prev_row: dict,
        symbol: str,
    ) -> Optional[dict]:
        """从 1 分钟 K 线 DataFrame 聚合出一行日 K 数据。"""
        if min_bars.empty:
            return None

        # 基础 OHLCV
        first_close = float(min_bars["收盘"].iloc[0])
        first_open = float(min_bars["开盘"].iloc[0])
        open_price = first_close if first_open == 0 else first_open
        close_price = float(min_bars["收盘"].iloc[-1])
        high_price = float(min_bars["最高"].max())
        low_series = min_bars["最低"]
        low_series_nozero = low_series[low_series > 0]
        low_price = float(low_series_nozero.min()) if not low_series_nozero.empty else float(
            min_bars["收盘"].min()
        )
        volume = int(min_bars["成交量"].sum())

        daily: dict = {}
        daily[_DATE_COL] = str(min_bars["_dt"].iloc[0].date())

        # 按存量 CSV 列顺序填充
        for col in existing_columns:
            if col == _DATE_COL:
                continue

            if col == "开盘":
                daily[col] = open_price
            elif col == "收盘":
                daily[col] = close_price
            elif col == "最高":
                daily[col] = high_price
            elif col == "最低":
                daily[col] = low_price
            elif col == "成交量":
                daily[col] = volume
            elif col == "成交额":
                if "成交额" in min_bars.columns:
                    daily[col] = int(min_bars["成交额"].sum())
                # 指数没有成交额列则跳过
            elif col == "流通股本":
                prev_val = prev_row.get("流通股本")
                daily[col] = float(prev_val) if prev_val is not None and prev_val != "" else 0.0
            elif col == "换手率":
                float_shares = daily.get("流通股本", prev_row.get("流通股本", 1))
                try:
                    float_shares = float(float_shares) if float_shares else 1.0
                except (ValueError, TypeError):
                    float_shares = 1.0
                daily[col] = round(volume / float_shares, 6) if float_shares > 0 else 0.0
            elif col == "涨跌额":
                prev_close = prev_row.get("收盘")
                try:
                    prev_close = float(prev_close) if prev_close is not None else None
                except (ValueError, TypeError):
                    prev_close = None
                if prev_close is not None and prev_close > 0:
                    daily[col] = round(close_price - prev_close, 2)
            elif col == "涨跌幅":
                chg = daily.get("涨跌额")
                prev_close = prev_row.get("收盘")
                try:
                    prev_close = float(prev_close) if prev_close is not None else None
                except (ValueError, TypeError):
                    prev_close = None
                if chg is not None and prev_close is not None and prev_close > 0:
                    daily[col] = round(chg / prev_close * 100, 2)
                else:
                    daily[col] = 0.0
            elif col == "振幅":
                prev_close = prev_row.get("收盘")
                try:
                    prev_close = float(prev_close) if prev_close is not None else None
                except (ValueError, TypeError):
                    prev_close = None
                if prev_close is not None and prev_close > 0:
                    daily[col] = round((high_price - low_price) / prev_close * 100, 2)
                else:
                    daily[col] = 0.0
            elif col == "均价":
                if "均价" in min_bars.columns:
                    daily[col] = round(daily.get("成交额", 0) / volume, 2) if volume > 0 else 0.0

        return daily

    # ---- 写回 CSV ----------------------------------------------------

    def _upsert_price_csv(
        self,
        df_existing: pd.DataFrame,
        daily: dict,
        price_csv_path: Path,
        symbol: str,
    ) -> None:
        """把聚合后的日 K 行追加/覆盖到 price.csv。"""
        new_row = pd.DataFrame([daily])

        # 列对齐：只保留 price.csv 已有的列
        new_row = new_row.reindex(columns=df_existing.columns)

        target_date = daily[_DATE_COL]
        mask = df_existing[_DATE_COL].astype(str) == str(target_date)
        if mask.any():
            # 覆盖已有的同一行
            df_existing.loc[mask, :] = new_row.values
        else:
            df_existing = pd.concat([df_existing, new_row], ignore_index=True)

        # 排序 + 去重（保留最后一条）
        df_existing["_sort_key"] = pd.to_datetime(
            df_existing[_DATE_COL], errors="coerce"
        )
        df_existing = df_existing.sort_values("_sort_key").drop_duplicates(
            subset=[_DATE_COL], keep="last"
        )
        df_existing = df_existing.drop(columns=["_sort_key"])

        df_existing.to_csv(price_csv_path, index=False)
        self.logger.info(
            "%s 分时回填完成，新增/覆盖 %s", symbol, target_date
        )


# ---- 工具函数（模块级）-----------------------------------------------

def _retry_fetch(
    fetch_fn,
    *,
    symbol: str = "",
    logger: logging.Logger | None = None,
    max_attempts: int = 3,
    base_delay: float = 2.0,
) -> Optional[pd.DataFrame]:
    """带指数退避的重试抓取。"""
    import time as _time

    last_error: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            return fetch_fn()
        except Exception as exc:
            last_error = exc
            if attempt < max_attempts - 1:
                delay = base_delay * (2**attempt)
                if logger:
                    logger.info(
                        "%s 分时抓取失败(第 %d/%d 次)，%ss 后重试: %s",
                        symbol,
                        attempt + 1,
                        max_attempts,
                        delay,
                        exc,
                    )
                _time.sleep(delay)
    raise last_error  # type: ignore[misc]
