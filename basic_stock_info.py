
from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from configs.stock_pool import TRACKED_A_STOCKS  # 添加导入
from utlity import (  # type: ignore
    SymbolFormatError,
    SymbolInfo,
    get_latest_trading_day,
    normalize_symbol,
    parse_symbol,
    resolve_base_dir,
    is_cn_etf_symbol,
)

from shared_data_access import SharedDataAccess
from shared_data_access.indicator_library import (
    IndicatorLibrary,
    IndicatorBatchRequest,
    IndicatorSpec,
    IndicatorCalculationError,
)
from shared_data_access.cache_registry import (
    CacheKind,
    check_cache,
    record_cache_refresh,
)
from shared_data_access.exceptions import CacheIntegrityError, DataUnavailableError
from shared_data_access.models import PreparedData
from indicator_library.gateways import DataFrameGateway
from indicator_library.calculators.fundamental import calculate_rolling_ttm_profit


def setup_main_logger() -> logging.Logger:
    """设置主脚本日志器，将日志输出到 logs/main_scripts/BasicStockInfo/ 目录"""
    logger = logging.getLogger("basic_stock_info")
    
    # 清除现有处理器，避免重复添加
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 设置日志格式
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器 - 统一到 logs/main_scripts/BasicStockInfo/
    main_scripts_dir = Path(__file__).resolve().parent / "logs" / "main_scripts" / "BasicStockInfo"
    main_scripts_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"basic_stock_info_{timestamp}.log"
    file_handler = logging.FileHandler(
        main_scripts_dir / log_filename, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.setLevel(logging.INFO)
    return logger


LOGGER = setup_main_logger()

DEFAULT_PRICE_LOOKBACK_DAYS = 2000  # ≈5.5年默认值，保证大多数场景命中缓存
# 保障估值/风险计算至少覆盖近2年交易日（≈504个交易日），留出节假日缓冲
MIN_PRICE_LOOKBACK_DAYS = 800

FIELD_NOTES: Dict[str, Any] = {
    "timestamp": "结果生成日期 (YYYY-MM-DD)",
    "data_source": "核心数据来源说明",
    "stocks_count": "成功返回的股票数量",
    "stocks": "股票指标字典，键为标准化代码，值为该股票的各项指标",
    "errors": "拉取失败的股票及原因 (可选)",
        "stock_fields": {
            "stock_name": "股票名称/别名 (字符串)",
            "latest_price": "最近收盘价，A股单位：元人民币，港股单位：港元",
            "daily_change_pct": "最近收盘价相对上一交易日的涨跌幅 (字符串, 如 +7%)",
            "latest_volume": "最近成交额，单位：亿元",
        "pe_ttm": "扣非市盈率 (TTM，使用扣非TTM净利润)",
        "pb": "市净率",
        "ps": "市销率",
        "market_cap": "总市值，单位：人民币亿元",
        "pe_2y_median": "近2年扣非TTM市盈率中位数（使用扣非净利润）",
        "pe_2y_percentile": "当前扣非TTM市盈率在近2年样本中的分位数 (0-1)",
        "pe_2y_std": "近2年扣非TTM市盈率标准差",
        "pe_current_vs_median": "当前扣非TTM市盈率 / 近2年中位数",
        "return_3m": "近3个月累计收益率 (%)",
        "sharpe_3m": "近3个月夏普比率",
        "volatility_3m": "近3个月年化波动率 (%)",
        "max_drawdown_3m": "近3个月最大回撤 (%)",
        "return_6m": "近6个月累计收益率 (%)",
        "sharpe_6m": "近6个月夏普比率",
        "volatility_6m": "近6个月年化波动率 (%)",
        "max_drawdown_6m": "近6个月最大回撤 (%)",
        "return_1y": "近12个月累计收益率 (%)",
        "sharpe_1y": "近12个月夏普比率",
        "volatility_1y": "近12个月年化波动率 (%)",
        "max_drawdown_1y": "近12个月最大回撤 (%)",
        "revenue_growth_yoy": "营业总收入同比增速 (%)",
        "net_income_growth_yoy": "归母净利润同比增速 (%)",
        "gross_margin": "毛利率 (%)",
        "net_profit_margin": "净利率/销售净利率 (%)",
        "roe": "净资产收益率 ROE (%)",
        "turnover_rate": "最新单日换手率 (%)",
        "avg_turnover_30d": "近30日平均换手率 (%)",
        "liquidity_score": "流动性综合评分 (0-1)",
    },
}


CACHE_FOLDER_NAME = CacheKind.BASIC_INFO.value
FIELD_NOTES_DESCRIPTION = json.dumps(FIELD_NOTES.get("stock_fields", {}), ensure_ascii=False)
META_INFORMATION_TEXT = (
    "Basic stock info snapshot. 字段说明: " + FIELD_NOTES_DESCRIPTION
)


def _normalize_stock_list(stock_codes: Iterable[str]) -> Tuple[List[str], Dict[str, str]]:
    normalized: List[str] = []
    errors: Dict[str, str] = {}
    seen: set[str] = set()
    for raw in stock_codes:
        try:
            symbol = normalize_symbol(raw)
        except SymbolFormatError as exc:
            errors[str(raw)] = str(exc)
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized, errors


def _snapshot_root(base_dir: Path) -> Path:
    root = resolve_base_dir(base_dir)
    target = root / CacheKind.BASIC_INFO.value
    target.mkdir(parents=True, exist_ok=True)
    return target


def _snapshot_file(base_dir: Path, symbol: str) -> Path:
    safe_symbol = symbol.replace("/", "_")
    return _snapshot_root(base_dir) / f"basic_info_{safe_symbol}.json"


def _load_cached_stocks(
    symbols: List[str],
    base_dir: Path,
    target_dates: Dict[str, date],
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    cached: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    snapshot_dir = _snapshot_root(base_dir)
    status = check_cache(snapshot_dir, CacheKind.BASIC_INFO)
    if not snapshot_dir.exists() or status.stale:
        if status.stale:
            LOGGER.info("basic_info_cache 过期或将刷新 (last_updated=%s)", status.last_updated)
        return cached, symbols.copy()
    for symbol in symbols:
        target_date = target_dates.get(symbol)
        if target_date is None:
            missing.append(symbol)
            continue
        date_key = target_date.strftime("%Y-%m-%d")
        file_path = _snapshot_file(base_dir, symbol)
        if not file_path.exists():
            missing.append(symbol)
            continue
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
            series = data.get("Time Series (Daily)", {})
            entry = series.get(date_key)
            if isinstance(entry, dict):
                cached[symbol] = entry
            else:
                missing.append(symbol)
        except Exception as exc:
            LOGGER.warning("读取 %s 缓存失败: %s", file_path, exc)
            missing.append(symbol)
    return cached, missing


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float, np.number)):
        try:
            if math.isnan(float(value)):
                return None
        except ValueError:
            return None
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped in {"--", "False"}:
            return None
        stripped = stripped.replace(",", "")
        try:
            if stripped.endswith("%"):
                return float(stripped[:-1])
            if stripped.endswith("亿"):
                return float(stripped[:-1]) * 1e8
            if stripped.endswith("万"):
                return float(stripped[:-1]) * 1e4
            return float(stripped)
        except ValueError:
            return None
    return None


def _round(value: Optional[float], digits: int = 4) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(value, digits)
    except (TypeError, ValueError):
        return None


def _parse_analysis_time(value: Optional[Union[str, datetime]]) -> Optional[datetime]:
    """将 today_time 参数转换为 datetime 对象（UTC-naive）。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        parsed = pd.to_datetime(stripped, errors="coerce")
        if pd.isna(parsed):
            raise ValueError(f"无法解析 today_time: {value}")
        if isinstance(parsed, pd.Timestamp):
            if parsed.tzinfo is not None:
                parsed = parsed.tz_convert(None)
            return parsed.to_pydatetime()
        if isinstance(parsed, datetime):
            return parsed
    raise ValueError(f"无法解析 today_time: {value}")


PRICE_NUMERIC_COLUMNS = (
    "开盘",
    "最高",
    "最低",
    "收盘",
    "成交量",
    "成交额",
    "换手率",
    "流通股本",
)



class BasicStockInfoService:
    """核心调度器，负责聚合单只股票的所有字段。"""

    def __init__(
        self,
        base_dir: Optional[str | Path] = None,
        max_workers: int = 1,
        price_lookback_days: int = DEFAULT_PRICE_LOOKBACK_DAYS,
        force_refresh: bool = False,
        force_refresh_financials: bool = False,
        analysis_datetime: Optional[datetime] = None,
        symbol_infos: Optional[Dict[str, SymbolInfo]] = None,
        target_dates: Optional[Dict[str, date]] = None,
    ) -> None:
        self.base_dir = resolve_base_dir(base_dir)
        self.price_lookback_days = max(price_lookback_days, MIN_PRICE_LOOKBACK_DAYS)
        self.max_workers = max_workers
        self.force_refresh = force_refresh
        self.force_refresh_financials = force_refresh_financials
        self.symbol_infos = symbol_infos or {}
        self.target_dates = target_dates or {}
        base_analysis_date = (
            analysis_datetime.date()
            if analysis_datetime is not None
            else datetime.now().date()
        )
        if self.target_dates:
            base_analysis_date = max(self.target_dates.values())
        # base_analysis_date = datetime.date
        self.analysis_date = base_analysis_date
        if (
            analysis_datetime is None
            or analysis_datetime.date() != self.analysis_date
        ):
            self.analysis_datetime = datetime.combine(
                self.analysis_date,
                datetime.min.time(),
            )
        else:
            self.analysis_datetime = analysis_datetime
        self._data_access = SharedDataAccess(
            base_dir=self.base_dir,
            price_lookback_days=self.price_lookback_days,
            logger=LOGGER,
        )
        self.snapshot_root = _snapshot_root(self.base_dir)
        self.snapshot_history_limit = max(self.price_lookback_days, 365)

    def _get_symbol_info(self, symbol: str) -> SymbolInfo:
        info = self.symbol_infos.get(symbol)
        if info is None:
            info = parse_symbol(symbol)
            self.symbol_infos[symbol] = info
        return info

    def _get_symbol_target_date(self, symbol: str) -> date:
        return self.target_dates.get(symbol, self.analysis_date)

    def _get_symbol_target_datetime(self, symbol: str) -> datetime:
        target_date = self._get_symbol_target_date(symbol)
        return datetime.combine(target_date, datetime.min.time())

    def _normalize_price_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        work = frame.copy()
        if not isinstance(work.index, pd.DatetimeIndex):
            work.index = pd.to_datetime(work.index, errors="coerce")
        work = work.sort_index()
        work.index = pd.to_datetime(work.index, errors="coerce")
        work = work.dropna(axis=0, how="all")
        for column in PRICE_NUMERIC_COLUMNS:
            if column in work.columns:
                work[column] = pd.to_numeric(work[column], errors="coerce")
        return work

    def _build_indicator_gateway_frame(
        self,
        symbol_info: SymbolInfo,
        price_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if price_df.empty:
            return pd.DataFrame()
        alias = symbol_info.stock_name or symbol_info.symbol
        gateway = pd.DataFrame(index=price_df.index)
        for column in ("收盘", "成交量", "成交额", "换手率"):
            if column in price_df.columns:
                gateway[f"{alias}_{column}"] = price_df[column]
        return gateway

    @staticmethod
    def _price_on_or_before(price_df: pd.DataFrame, when: pd.Timestamp | datetime) -> Optional[float]:
        if price_df.empty or "收盘" not in price_df.columns:
            return None
        ts = pd.Timestamp(when)
        subset = price_df.loc[price_df.index <= ts]
        if subset.empty:
            return None
        return _safe_float(subset["收盘"].iloc[-1])

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------
    def _collect_symbols(
        self,
        symbols: List[str],
        initial_errors: Optional[Dict[str, str]] = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
        stocks: Dict[str, Dict[str, Any]] = {}
        errors: Dict[str, str] = dict(initial_errors or {})
        if not symbols:
            return stocks, errors

        if self.max_workers <= 1 or len(symbols) == 1:
            for symbol in symbols:
                data, err = self._collect_single(symbol)
                if data:
                    stocks[symbol] = data
                else:
                    errors[symbol] = err or "数据缺失"
            return stocks, errors

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {
                executor.submit(self._collect_single, symbol): symbol
                for symbol in symbols
            }
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    data, err = future.result()
                except Exception as exc:  # pragma: no cover - 容错日志
                    LOGGER.exception("并发获取 %s 时出现异常", symbol)
                    data, err = None, str(exc)
                if data:
                    stocks[symbol] = data
                else:
                    errors[symbol] = err or "数据缺失"
        return stocks, errors

    def build_payload(self, stock_codes: Iterable[str]) -> Dict[str, Any]:
        normalized, errors = _normalize_stock_list(stock_codes)

        stocks, collect_errors = self._collect_symbols(normalized)
        errors.update(collect_errors)

        payload: Dict[str, Any] = {
            "timestamp": self.analysis_date.strftime("%Y-%m-%d"),
            "data_source": "akshare + 内部计算",
            "stocks_count": len(stocks),
            "stocks": stocks,
        }
        if errors:
            payload["errors"] = errors
        payload["field_notes"] = FIELD_NOTES
        self._persist_stock_snapshots(stocks)
        return payload

    def build_payload_from_normalized(
        self,
        normalized_symbols: List[str],
        initial_errors: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        stocks, errors = self._collect_symbols(
            normalized_symbols,
            initial_errors=initial_errors,
        )

        payload: Dict[str, Any] = {
            "timestamp": self.analysis_date.strftime("%Y-%m-%d"),
            "data_source": "akshare + 内部计算",
            "stocks_count": len(stocks),
            "stocks": stocks,
        }
        if errors:
            payload["errors"] = errors
        payload["field_notes"] = FIELD_NOTES
        self._persist_stock_snapshots(stocks)
        return payload

    def _persist_stock_snapshots(self, stocks: Dict[str, Dict[str, Any]]) -> None:
        if not stocks:
            return
        self.snapshot_root.mkdir(parents=True, exist_ok=True)
        for symbol, record in stocks.items():
            stock_name = record.get("stock_name") or symbol
            record.setdefault("stock_name", stock_name)
            date_key = self._get_symbol_target_date(symbol).strftime("%Y-%m-%d")
            meta = {
                "1. Information": META_INFORMATION_TEXT,
                "2. Symbol": f"{stock_name}_{symbol}",
                "3. Last Refreshed": date_key,
                "4. Output Size": "basic_info",
                "5. Time Zone": "Asia/Shanghai",
            }
            file_path = _snapshot_file(self.base_dir, symbol)
            if file_path.exists():
                try:
                    content = json.loads(file_path.read_text(encoding="utf-8"))
                except Exception:
                    content = {"Meta Data": meta, "Time Series (Daily)": {}}
            else:
                content = {"Meta Data": meta, "Time Series (Daily)": {}}

            content["Meta Data"].update(meta)
            series = content.setdefault("Time Series (Daily)", {})
            series[date_key] = record

            ordered_keys = sorted(series.keys(), reverse=True)
            if self.snapshot_history_limit and len(ordered_keys) > self.snapshot_history_limit:
                ordered_keys = ordered_keys[: self.snapshot_history_limit]
            ordered_series = {key: series[key] for key in ordered_keys}
            content["Time Series (Daily)"] = ordered_series

            file_path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
        record_cache_refresh(self.snapshot_root)

    # ------------------------------------------------------------------
    # 单只股票处理
    # ------------------------------------------------------------------
    def _collect_single(self, symbol: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        try:
            symbol_info = self._get_symbol_info(symbol)
        except SymbolFormatError as exc:
            return None, str(exc)

        try:
            dataset = self._data_access.prepare_dataset(
                symbolInfo=symbol_info,
                as_of_date=self.analysis_datetime.strftime("%Y-%m-%d"),
                force_refresh=self.force_refresh,
                force_refresh_financials=self.force_refresh_financials,
            )
            stock_name = dataset.symbolInfo.stock_name
        except (CacheIntegrityError, DataUnavailableError) as exc:
            return None, str(exc)

        price_df = self._normalize_price_frame(dataset.prices.frame)
        indicator_price_df = self._build_indicator_gateway_frame(symbol_info, price_df)

        is_etf = is_cn_etf_symbol(symbol_info.symbol)
        if is_etf:
            latest_price = _round(self._latest_close(symbol_info.symbol, price_df))
            latest_volume = _round(self._latest_volume(symbol_info.symbol, price_df))
            valuation = {
                "latest_price": latest_price,
                "latest_volume": latest_volume,
                "market_cap": None,
            }
            history_stats: Dict[str, Any] = {}
            financial_metrics: Dict[str, Any] = {}
        else:
            valuation = self._compute_valuation_fields(
                symbol_info,
                dataset,
                price_df,
            )
            reference_date = self._get_symbol_target_date(symbol_info.symbol)
            history_stats = self._compute_pe_history(
                valuation["pe_history"], reference_date, valuation.get("pe_for_stats")
            )
            financial_metrics = self._compute_financial_metrics(
                dataset.financials.financial_abstract
            )
        risk_metrics = self._compute_risk_metrics(symbol_info, indicator_price_df)
        liquidity = self._compute_liquidity_metrics(symbol_info, indicator_price_df, valuation)
        liquidity_payload = (
            {"liquidity_score": liquidity.get("liquidity_score")}
            if is_etf
            else liquidity
        )
        daily_change_pct = self._daily_change_pct(price_df)

        if is_etf:
            payload = {
                "stock_name": stock_name,
                "latest_price": valuation["latest_price"],
                "daily_change_pct": daily_change_pct,
                "latest_volume": valuation["latest_volume"],
                **risk_metrics,
                **liquidity_payload,
            }
        else:
            payload = {
                "stock_name": stock_name,
                "latest_price": valuation["latest_price"],
                "daily_change_pct": daily_change_pct,
                "latest_volume": valuation["latest_volume"],
                "pe_ttm": valuation["pe_ttm"],
                "pb": valuation["pb"],
                "ps": valuation["ps"],
                "market_cap": valuation["market_cap"],
                **history_stats,
                **risk_metrics,
                **financial_metrics,
                **liquidity_payload,
            }

        if is_etf:
            required_fields = {
                "latest_price",
                "return_3m",
            }
        else:
            required_fields = {
                "latest_price",
                "pe_ttm",
                "market_cap",
                "return_3m",
                "revenue_growth_yoy",
                "turnover_rate",
            }
        missing = [f for f in required_fields if payload.get(f) is None]
        if missing:
            LOGGER.warning("%s 缺失字段: %s", symbol, ",".join(missing))
        return payload, None

    # ------------------------------------------------------------------
    # 指标计算
    # ------------------------------------------------------------------
    def _compute_valuation_fields(
        self,
        symbol_info: SymbolInfo,
        dataset: PreparedData,
        price_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        financials = dataset.financials
        profit_sheet = financials.profit_sheet
        # TTM 净利润（全量）与扣非净利润
        ttm_profit = calculate_rolling_ttm_profit(profit_sheet, logger=LOGGER)
        deduct_ttm_profit = calculate_rolling_ttm_profit(
            profit_sheet, profit_column="DEDUCT_PARENT_NETPROFIT", logger=LOGGER
        )

        total_shares = dataset.share_info.total_shares
        latest_price = self._latest_close(symbol_info.symbol, price_df)
        latest_volume = self._latest_volume(symbol_info.symbol, price_df)

        pe_history: List[Dict[str, Any]] = []
        pe_ttm = None  # 输出字段：改为扣非PE(TTM)
        pe_for_stats = None  # 统计用扣非PE优先

        profit_for_stats = deduct_ttm_profit if not deduct_ttm_profit.empty else ttm_profit

        # 当前PE(TTM)：改为使用扣非TTM净利润（若缺失则回退普通TTM）；允许负值，0 则视为不可算
        if (
            total_shares
            and latest_price is not None
            and not profit_for_stats.empty
            and "TTM_NET_PROFIT_RAW" in profit_for_stats.columns
        ):
            latest_row = profit_for_stats.iloc[-1]
            ttm_eps = latest_row["TTM_NET_PROFIT_RAW"] / total_shares
            if ttm_eps != 0:
                pe_ttm = latest_price / ttm_eps

        # 统计用：同样基于扣非TTM净利润
        if (
            total_shares
            and latest_price is not None
            and not profit_for_stats.empty
            and "TTM_NET_PROFIT_RAW" in profit_for_stats.columns
        ):
            stats_eps = profit_for_stats.iloc[-1]["TTM_NET_PROFIT_RAW"] / total_shares
            if stats_eps != 0:
                pe_for_stats = latest_price / stats_eps

        if total_shares and "TTM_NET_PROFIT_RAW" in profit_for_stats.columns:
            for _, row in profit_for_stats.iterrows():
                quarter = pd.to_datetime(row["REPORT_DATE"]).strftime("%Y-%m-%d")
                price = self._price_on_or_before(price_df, pd.to_datetime(row["REPORT_DATE"]))
                if price is None:
                    continue
                eps = row["TTM_NET_PROFIT_RAW"] / total_shares
                if eps == 0:
                    continue
                pe_value = price / eps
                pe_history.append({"报告期": quarter, "PE": pe_value})

        pb = self._current_pb(financials.financial_abstract, latest_price)
        ps = self._current_ps(financials.financial_abstract, latest_price, total_shares)

        market_cap = None
        if latest_price is not None and total_shares:
            market_cap = latest_price * total_shares / 1e8  # 亿元

        return {
            "latest_price": _round(latest_price),
            "latest_volume": _round(latest_volume),
            "pe_ttm": _round(pe_ttm),
            "pe_for_stats": _round(pe_for_stats),
            "pb": _round(pb),
            "ps": _round(ps),
            "market_cap": _round(market_cap),
            "pe_history": pe_history,
        }

    def _compute_pe_history(
        self,
        pe_history: List[Dict[str, Any]],
        reference_date: date,
        current_pe: Optional[float],
    ) -> Dict[str, Any]:
        if not pe_history:
            return {
                "pe_2y_median": None,
                "pe_2y_percentile": None,
                "pe_2y_std": None,
                "pe_current_vs_median": None,
            }
        reference_dt = datetime.combine(reference_date, datetime.min.time())
        two_years_ago = reference_dt - timedelta(days=2 * 365)
        values = [
            item["PE"]
            for item in pe_history
            if _safe_float(item["PE"]) and pd.to_datetime(item["报告期"]) >= two_years_ago
        ]
        if current_pe:
            values.append(current_pe)
        if not values:
            return {
                "pe_2y_median": None,
                "pe_2y_percentile": None,
                "pe_2y_std": None,
                "pe_current_vs_median": None,
            }
        median_val = float(np.median(values))
        std_val = float(np.std(values))
        current_value = current_pe if current_pe is not None else values[-1]
        percentile = sum(v <= current_value for v in values) / len(values)
        ratio = current_value / median_val if median_val > 0 else None
        return {
            "pe_2y_median": _round(median_val),
            "pe_2y_percentile": _round(percentile),
            "pe_2y_std": _round(std_val),
            "pe_current_vs_median": _round(ratio),
        }

    def _compute_risk_metrics(self, symbol_info: SymbolInfo, indicator_frame: pd.DataFrame) -> Dict[str, Any]:
        result = {
            "return_3m": None,
            "sharpe_3m": None,
            "volatility_3m": None,
            "max_drawdown_3m": None,
            "return_6m": None,
            "sharpe_6m": None,
            "volatility_6m": None,
            "max_drawdown_6m": None,
            "return_1y": None,
            "sharpe_1y": None,
            "volatility_1y": None,
            "max_drawdown_1y": None,
        }
        alias = symbol_info.stock_name or symbol_info.symbol
        close_col = f"{alias}_收盘"
        if indicator_frame.empty or close_col not in indicator_frame.columns:
            return result
        indicator_frame = indicator_frame.sort_index()
        indicator = IndicatorLibrary(gateway=DataFrameGateway(indicator_frame))
        try:
            batch = indicator.calculate(
                IndicatorBatchRequest(
                    symbolInfo=symbol_info,
                    start_date=indicator_frame.index.min().date(),
                    end_date=indicator_frame.index.max().date(),
                    specs=[
                        IndicatorSpec(
                            name="return_metrics",
                            params={"windows": (63, 126, 252)},
                            alias="return_metrics",
                        )
                    ],
                    price_fields=["收盘"],
                )
            )
        except IndicatorCalculationError:
            return result

        metrics = batch.tabular.get("return_metrics", {})
        if not metrics:
            return result

        mapping = {
            63: ("return_3m", "sharpe_3m", "volatility_3m", "max_drawdown_3m"),
            126: ("return_6m", "sharpe_6m", "volatility_6m", "max_drawdown_6m"),
            252: ("return_1y", "sharpe_1y", "volatility_1y", "max_drawdown_1y"),
        }
        for window, keys in mapping.items():
            key = f"{window}d"
            window_metrics = metrics.get(window, {})
            result[keys[0]] = _round(window_metrics.get("累计收益率(%)"))
            result[keys[1]] = _round(window_metrics.get("夏普比率"))
            result[keys[2]] = _round(window_metrics.get("年化波动率(%)"))
            result[keys[3]] = _round(window_metrics.get("最大回撤(%)"))
        return result

    def _compute_financial_metrics(self, financial_abstract: pd.DataFrame) -> Dict[str, Any]:
        default_payload = {
            "revenue_growth_yoy": None,
            "net_income_growth_yoy": None,
            "gross_margin": None,
            "net_profit_margin": None,
            "roe": None,
        }
        if financial_abstract.empty:
            return default_payload
        if "指标" not in financial_abstract.columns:
            return self._compute_financial_metrics_columnar(financial_abstract)

        def row(keyword: str) -> Optional[pd.Series]:
            matches = financial_abstract[financial_abstract["指标"].str.contains(keyword, regex=False, na=False)]
            return matches.iloc[0] if not matches.empty else None

        revenue_row = row("营业总收入")
        net_row = row("归母净利润")
        if net_row is None:
            net_row = row("净利润")
        gross_row = row("毛利率")
        net_margin_row = row("销售净利率")
        if net_margin_row is None:
            net_margin_row = row("净利率")
        roe_row = row("ROE")

        revenue_growth = self._yoy_from_row(revenue_row)
        net_growth = self._yoy_from_row(net_row)
        gross_margin = self._latest_metric(gross_row)
        net_margin = self._latest_metric(net_margin_row)
        roe = self._latest_metric(roe_row)

        return {
            "revenue_growth_yoy": _round(revenue_growth),
            "net_income_growth_yoy": _round(net_growth),
            "gross_margin": _round(gross_margin),
            "net_profit_margin": _round(net_margin),
            "roe": _round(roe),
        }

    def _compute_financial_metrics_columnar(self, financial_abstract: pd.DataFrame) -> Dict[str, Any]:
        def latest(*columns: str) -> Optional[float]:
            for col in columns:
                value = self._latest_column_numeric(financial_abstract, col)
                if value is not None:
                    return value
            return None

        revenue_growth = latest("OPERATE_INCOME_YOY", "TOTAL_OPERATE_INCOME_YOY")
        net_growth = latest("HOLDER_PROFIT_YOY", "NETPROFIT_YOY")
        gross_margin = latest("GROSS_PROFIT_RATIO")
        net_margin = latest("NET_PROFIT_RATIO")
        roe = latest("ROE_AVG", "ROE_YEARLY")

        return {
            "revenue_growth_yoy": _round(revenue_growth),
            "net_income_growth_yoy": _round(net_growth),
            "gross_margin": _round(gross_margin),
            "net_profit_margin": _round(net_margin),
            "roe": _round(roe),
        }

    def _compute_liquidity_metrics(
        self,
        symbol_info: SymbolInfo,
        indicator_frame: pd.DataFrame,
        valuation: Dict[str, Any],
    ) -> Dict[str, Any]:
        alias = symbol_info.stock_name or symbol_info.symbol
        turnover_col = f"{alias}_换手率"
        if indicator_frame.empty or indicator_frame.index.empty or turnover_col not in indicator_frame.columns:
            return {
                "turnover_rate": None,
                "avg_turnover_30d": None,
                "liquidity_score": None,
            }
        indicator = IndicatorLibrary(gateway=DataFrameGateway(indicator_frame))
        params = {
            "market_cap": valuation.get("market_cap"),
        }
        try:
            batch = indicator.calculate(
                IndicatorBatchRequest(
                    symbolInfo=symbol_info,
                    start_date=indicator_frame.index.min().date(),
                    end_date=indicator_frame.index.max().date(),
                    specs=[
                        IndicatorSpec(
                            name="liquidity_profile",
                            params=params,
                            alias="liquidity_profile",
                        )
                    ],
                    price_fields=["收盘", "成交量", "换手率"],
                )
            )
        except IndicatorCalculationError:
            return {
                "turnover_rate": None,
                "avg_turnover_30d": None,
                "liquidity_score": None,
            }

        profile = batch.tabular.get("liquidity_profile", {})
        score = profile.get("流动性评分")
        return {
            "turnover_rate": _round(profile.get("最新换手率(%)")),
            "avg_turnover_30d": _round(profile.get("30天平均换手率(%)")),
            "liquidity_score": _round(score, digits=4) if score is not None else None,
        }

    # ------------------------------------------------------------------
    # 细粒度工具
    # ------------------------------------------------------------------
    def _daily_change_pct(self, price_df: pd.DataFrame) -> Optional[str]:
        if price_df.empty or "收盘" not in price_df.columns:
            return None
        series = price_df["收盘"].dropna()
        if series.shape[0] < 2:
            return None
        prev_close = _safe_float(series.iloc[-2])
        latest_close = _safe_float(series.iloc[-1])
        if prev_close in (None, 0) or latest_close is None:
            return None
        change = (latest_close - prev_close) / prev_close * 100
        if abs(change) < 1e-8:
            return "0%"
        formatted = f"{change:+.2f}"
        formatted = formatted.rstrip("0").rstrip(".")
        return f"{formatted}%"

    def _latest_close(self, symbol: str, price_df: pd.DataFrame) -> Optional[float]:
        if price_df.empty or "收盘" not in price_df.columns:
            return None
        series = price_df["收盘"].dropna()
        if series.empty:
            return None
        return _safe_float(series.iloc[-1])

    def _latest_volume(self, symbol: str, price_df: pd.DataFrame) -> Optional[float]:
        if price_df.empty:
            return None
        if "成交额" in price_df.columns:
            series = pd.to_numeric(price_df["成交额"], errors="coerce").dropna()
            if not series.empty:
                return series.iloc[-1] / 1e8
        if "成交量" in price_df.columns:
            series = pd.to_numeric(price_df["成交量"], errors="coerce").dropna()
            if not series.empty:
                return series.iloc[-1] / 1e8
        return None

    def _current_pb(self, financial_abstract: pd.DataFrame, latest_price: Optional[float]) -> Optional[float]:
        if latest_price is None or financial_abstract.empty:
            return None
        if "指标" in financial_abstract.columns:
            rows = financial_abstract[financial_abstract["指标"].str.contains("每股净资产", na=False)]
            if not rows.empty:
                row = rows.iloc[0]
                latest_value = self._latest_numeric_value(row)
                if latest_value and latest_value > 0:
                    return latest_price / latest_value
        latest_bps = self._latest_column_numeric(financial_abstract, "BPS")
        if latest_bps and latest_bps > 0:
            return latest_price / latest_bps
        return None

    def _current_ps(
        self,
        financial_abstract: pd.DataFrame,
        latest_price: Optional[float],
        total_shares: Optional[float],
    ) -> Optional[float]:
        if latest_price is None or not total_shares or financial_abstract.empty:
            return None
        if "指标" in financial_abstract.columns:
            rows = financial_abstract[financial_abstract["指标"].str.contains("营业总收入", na=False)]
            if rows.empty:
                return None
            row = rows.iloc[0]
            ttm_revenue = self._ttm_from_cumulative_row(row)
            if not ttm_revenue or ttm_revenue <= 0:
                return None
            market_cap = latest_price * total_shares
            return market_cap / ttm_revenue
        revenue_candidates = [
            "OPERATE_INCOME_TTM",
            "TOTAL_OPERATE_INCOME_TTM",
            "OPERATE_INCOME",
            "TOTAL_OPERATE_INCOME",
        ]
        for col in revenue_candidates:
            ttm_revenue = self._latest_column_numeric(financial_abstract, col)
            if ttm_revenue and ttm_revenue > 0:
                market_cap = latest_price * total_shares
                return market_cap / ttm_revenue
        return None

    def _latest_metric(self, row: Optional[pd.Series]) -> Optional[float]:
        if row is None:
            return None
        return self._latest_numeric_value(row)

    def _latest_numeric_value(self, row: pd.Series) -> Optional[float]:
        date_cols = [col for col in row.index if isinstance(col, str) and col.isdigit()]
        if not date_cols:
            return None
        latest_col = max(date_cols)
        return _safe_float(row[latest_col])

    def _latest_column_numeric(self, frame: pd.DataFrame, column: str) -> Optional[float]:
        if column not in frame.columns:
            return None
        work = frame[[column]].copy()
        if "REPORT_DATE" in frame.columns:
            work["REPORT_DATE"] = pd.to_datetime(frame["REPORT_DATE"], errors="coerce")
            work = work.dropna(subset=["REPORT_DATE"])
            work = work.sort_values("REPORT_DATE")
        series = pd.to_numeric(work[column], errors="coerce").dropna()
        if series.empty:
            return None
        return float(series.iloc[-1])

    def _yoy_from_row(self, row: Optional[pd.Series]) -> Optional[float]:
        if row is None:
            return None
        series = {col: _safe_float(val) for col, val in row.items() if isinstance(col, str) and col.isdigit()}
        if not series:
            return None
        latest_key = max(series)
        latest_val = series[latest_key]
        if latest_val is None:
            return None
        prev_key = str(int(latest_key[:4]) - 1) + latest_key[4:]
        prev_val = series.get(prev_key)
        if prev_val in (None, 0):
            return None
        return (latest_val - prev_val) / abs(prev_val) * 100

    def _ttm_from_cumulative_row(self, row: pd.Series) -> Optional[float]:
        date_cols = sorted([col for col in row.index if isinstance(col, str) and col.isdigit()], reverse=True)
        if len(date_cols) < 4:
            return None
        quarters = date_cols[:4]
        quarterly_values: List[float] = []
        for idx, col in enumerate(quarters):
            cumulative = _safe_float(row[col])
            if cumulative is None:
                continue
            if col.endswith("0331"):
                quarterly = cumulative
            elif col.endswith("0630"):
                q1 = row.get(col[:4] + "0331")
                quarterly = cumulative - _safe_float(q1) if _safe_float(q1) else cumulative
            elif col.endswith("0930"):
                h1 = row.get(col[:4] + "0630")
                quarterly = cumulative - _safe_float(h1) if _safe_float(h1) else cumulative / 2
            elif col.endswith("1231"):
                q3 = row.get(col[:4] + "0930")
                quarterly = cumulative - _safe_float(q3) if _safe_float(q3) else cumulative / 3
            else:
                quarterly = cumulative
            quarterly_values.append(quarterly)
        if len(quarterly_values) < 3:
            return None
        return sum(q for q in quarterly_values if q)

# ----------------------------------------------------------------------
# 对外 API
# ----------------------------------------------------------------------
def basic_info(
    stock_codes: Iterable[str],
    *,
    base_dir: Optional[str | Path] = None,
    max_workers: int = 1,
    price_lookback_days: int = DEFAULT_PRICE_LOOKBACK_DAYS,
    force_refresh: bool = False,
    force_refresh_financials: bool = False,
    today_time: Optional[Union[str, datetime]] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    analysis_dt = _parse_analysis_time(today_time)
    analysis_date = (
        analysis_dt.date() if analysis_dt is not None else datetime.now().date()
    )
    normalized_symbols, normalization_errors = _normalize_stock_list(stock_codes)
    price_lookback_days = max(price_lookback_days, MIN_PRICE_LOOKBACK_DAYS)
    resolved_base_dir = resolve_base_dir(base_dir)

    symbol_infos: Dict[str, SymbolInfo] = {}
    target_dates: Dict[str, date] = {}
    valid_symbols: List[str] = []
    for symbol in normalized_symbols:
        try:
            info = parse_symbol(symbol)
        except SymbolFormatError as exc:
            normalization_errors[symbol] = str(exc)
            continue
        try:
            trade_date = get_latest_trading_day(analysis_date, info.calendar, LOGGER)
        except Exception as exc:
            normalization_errors[symbol] = f"无法确定交易日: {exc}"
            continue
        symbol_infos[symbol] = info
        target_dates[symbol] = trade_date
        valid_symbols.append(symbol)

    cached_stocks: Dict[str, Dict[str, Any]] = {}
    missing_symbols: List[str] = list(valid_symbols)
    if use_cache and not force_refresh and valid_symbols:
        cached_stocks, missing_symbols = _load_cached_stocks(
            valid_symbols,
            resolved_base_dir,
            target_dates,
        )
    else:
        cached_stocks = {}
        missing_symbols = list(valid_symbols)

    computed_data: Dict[str, Dict[str, Any]] = {}
    service_errors: Dict[str, str] = {}
    if missing_symbols:
        service = BasicStockInfoService(
            base_dir=resolved_base_dir,
            max_workers=max_workers,
            price_lookback_days=price_lookback_days,
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
            analysis_datetime=analysis_dt,
            symbol_infos=symbol_infos,
            target_dates=target_dates,
        )
        payload = service.build_payload_from_normalized(missing_symbols)
        computed_data = payload.get("stocks", {})
        service_errors = payload.get("errors", {})

    combined_stocks: Dict[str, Dict[str, Any]] = {}
    combined_stocks.update(cached_stocks)
    combined_stocks.update(computed_data)

    errors: Dict[str, str] = {}
    errors.update(normalization_errors)
    errors.update(service_errors)
    for symbol in normalized_symbols:
        if symbol not in combined_stocks and symbol not in errors:
            errors[symbol] = "数据缺失"

    timestamp_date = (
        max(target_dates.values()).strftime("%Y-%m-%d")
        if target_dates
        else analysis_date.strftime("%Y-%m-%d")
    )
    payload: Dict[str, Any] = {
        "timestamp": timestamp_date,
        "data_source": "akshare + 内部计算（缓存优先）",
        "stocks_count": len(combined_stocks),
        "stocks": combined_stocks,
        "field_notes": FIELD_NOTES,
    }
    if errors:
        payload["errors"] = errors
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="basic_info 工具")
    parser.add_argument(
        "--symbols",
        "-s",
        dest="symbols_option",
        nargs="+",
        default=[entry.symbol for entry in TRACKED_A_STOCKS],
        help="也可通过参数传入多只股票代码，例如 --symbols 000001.SZ 000002.SZ",
    )
    parser.add_argument("--output", "-o", help="结果写入 JSON 文件")
    parser.add_argument("--force-refresh", action="store_true", help="强制刷新行情+财务缓存")
    parser.add_argument(
        "--force-refresh-financials",
        action="store_true",
        help="仅强制刷新财务相关缓存",
    )
    parser.add_argument(
        "--price_lookback",
        type=int,
        default=DEFAULT_PRICE_LOOKBACK_DAYS,
        help=f"价格指标回溯天数，默认 {DEFAULT_PRICE_LOOKBACK_DAYS}，最低 {MIN_PRICE_LOOKBACK_DAYS}（保证两年估值所需）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=11,
        help="并发线程数，默认 1 (串行)",
    )
    parser.add_argument(
        "--today-time",
        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="估值基准时间，例如 2024-12-31 或 2024-12-31 15:00:00，默认为昨天",
    )
    parser.add_argument(
        "--get-look-back-days",
        dest="get_look_back_days",
        type=int,
        default=0,
        help="若大于0，则从 --today-time 指定的日期开始，向前回溯 N 天（含当日）批量生成 basic_info",
    )
    return parser.parse_args()


def _resolve_cli_symbols(args: argparse.Namespace) -> List[str]:
    cli_symbols: List[str] = []
    if args.symbols_option:
        cli_symbols.extend(args.symbols_option)
    if not cli_symbols:
        return [entry.symbol for entry in TRACKED_A_STOCKS]

    ordered: List[str] = []
    seen: set[str] = set()
    for symbol in cli_symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        ordered.append(symbol)
    return ordered


def _combine_payloads(payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not payloads:
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d"),
            "data_source": "akshare + 内部计算（缓存优先）",
            "stocks_count": 0,
            "stocks": {},
            "field_notes": FIELD_NOTES,
        }

    combined_stocks: Dict[str, Dict[str, Any]] = {}
    combined_errors: Dict[str, str] = {}
    timestamp = None
    data_source = None

    for payload in payloads:
        timestamp = payload.get("timestamp", timestamp)
        data_source = payload.get("data_source", data_source)
        combined_stocks.update(payload.get("stocks", {}))
        if payload.get("errors"):
            combined_errors.update(payload["errors"])

    result: Dict[str, Any] = {
        "timestamp": timestamp or datetime.now().strftime("%Y-%m-%d"),
        "data_source": data_source or "akshare + 内部计算（缓存优先）",
        "stocks_count": len(combined_stocks),
        "stocks": combined_stocks,
        "field_notes": FIELD_NOTES,
    }
    if combined_errors:
        result["errors"] = combined_errors
    return result


def _main() -> None:
    args = _parse_args()
    symbols = _resolve_cli_symbols(args)
    lookback = max(0, args.get_look_back_days or 0)
    if lookback:
        anchor_dt = _parse_analysis_time(args.today_time) or datetime.now()
        anchor_date = anchor_dt.date()
        for offset in range(lookback):
            target_date = anchor_date - timedelta(days=offset)
            target_str = target_date.strftime("%Y-%m-%d")
            LOGGER.info("批量生成 %s 的 basic_info（共 %d 只股票）", target_str, len(symbols))
            basic_info(
                symbols,
                max_workers=args.max_workers,
                price_lookback_days=args.price_lookback,
                force_refresh=args.force_refresh,
                force_refresh_financials=args.force_refresh_financials,
                today_time=target_str,
                use_cache=False,
            )
        return

    result = basic_info(
        symbols,
        max_workers=args.max_workers,
        price_lookback_days=args.price_lookback,
        force_refresh=args.force_refresh,
        force_refresh_financials=args.force_refresh_financials,
        today_time=args.today_time,
        use_cache=False,
    )
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
        print(f"已写入 {args.output}")
    else:
        print(text)


if __name__ == "__main__":
    _main()
