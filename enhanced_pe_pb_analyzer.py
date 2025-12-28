#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增强版 PE/PB/PS 历史分析器（重构版）

核心约束：
1. 所有需要股票信息的函数都通过 SymbolInfo 传参，避免散落的代码/名称不一致。
2. 所有外部数据（价格、财报、股本等）一律通过 SharedDataAccess.prepare_dataset 获取，
   不再在脚本层直接调用 akshare。
3. 股本信息全部来自 PreparedData.share_info，删除历史遗留的手工估算逻辑。
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from numbers import Integral, Real
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import pandas as pd

from indicator_library.calculators.fundamental import calculate_rolling_ttm_profit
from shared_data_access.data_access import SharedDataAccess
from shared_data_access.models import PreparedData
from utlity import (
    SymbolInfo,
    ensure_stock_subdir,
    get_stock_data_dir,
    parse_symbol,
    resolve_base_dir,
)
from utlity.get_similar_stocks import get_similar_stocks

LOG_DIR = Path("logs") / "main_scripts" / "EnhancedPEPBAnalyzer"
LOG_DIR.mkdir(parents=True, exist_ok=True)
HK_PROFIT_NOTE = "港股的净利润增速没有扣除非经营损益，仅供参考，详细数据请查看财报接口返回的财报分析结果"


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("enhanced_pe_pb_analyzer")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_handler = logging.FileHandler(
        LOG_DIR / f"enhanced_pe_pb_analyzer_{timestamp}.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


LOGGER = setup_logger()


class DataQualityError(RuntimeError):
    """Raised when关键数据缺失或异常。"""


@dataclass
class StockSnapshot:
    symbol: SymbolInfo
    display_name: str
    analysis_time: datetime
    price: float
    total_shares: float
    float_shares: Optional[float]
    market_cap: float
    float_market_cap: Optional[float]
    pe_ttm: Optional[float]
    pe_dynamic: Optional[float]
    pb: Optional[float]
    ps: Optional[float]
    peg: Optional[float]
    net_profit_growth: Optional[float]
    pe_ttm_note: str
    pe_dynamic_note: str
    pe_deduct: Optional[float] = None
    pe_deduct_note: str = ""
    peg_note: str = ""
    historical_pe: List[Dict[str, Any]] = field(default_factory=list)
    historical_peg: List[Dict[str, Any]] = field(default_factory=list)
    extreme_price_metrics: List[Dict[str, Any]] = field(default_factory=list)
    reason: str = ""
    ttm_profit_raw: float = 0.0
    use_plain_pe_label: bool = False


RESERVED_CACHE_FILES = {".cache_registry_meta.json"}


def cleanup_output_directory(directory: Path, keep_names: Iterable[str] | None = None) -> None:
    """清理分析输出目录，仅保留缓存元数据文件。"""
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    allowed = set(RESERVED_CACHE_FILES)
    if keep_names:
        allowed.update(keep_names)
    for entry in target.iterdir():
        if entry.name in allowed:
            continue
        try:
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink(missing_ok=True)
        except Exception as exc:
            LOGGER.warning("清理目录 %s 时跳过 %s: %s", target, entry, exc)


class EnhancedPEPBAnalyzer:
    """主分析器：聚焦估值指标 + Markdown 报告生成。"""

    NOTICE_DATE_COLUMNS = ("NOTICE_DATE", "公告日期")

    def __init__(
        self,
        *,
        base_dir: Path | str | None = None,
        analysis_datetime: datetime | None = None,
        price_lookback_days: int = 900,
        similar_limit: int = 2,
    ) -> None:
        self.base_dir = resolve_base_dir(base_dir)
        self.analysis_datetime = min(
            analysis_datetime, datetime.now()
        ) if analysis_datetime else datetime.now()
        self.similar_limit = max(0, similar_limit)
        self.transaction_package_dir = self.base_dir / "0_transaction_package"
        self.transaction_package_dir.mkdir(parents=True, exist_ok=True)
        self.data_access = SharedDataAccess(
            base_dir=self.base_dir,
            price_lookback_days=price_lookback_days,
            logger=LOGGER,
        )

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------
    def analyze_stock(
        self,
        symbol: SymbolInfo,
        *,
        force_refresh: bool = False,
        force_refresh_financials: bool = False,
    ) -> None:
        """执行主流程：目标股票 + 相似股票 + 报告输出。"""
        LOGGER.info("=" * 70)
        LOGGER.info("启动增强版PE/PB分析: %s (%s)", symbol.stock_name, symbol.symbol)

        target_snapshot = self._build_snapshot(
            symbol,
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
        )

        similar_snapshots = self._build_similar_snapshots(
            symbol,
            limit=self.similar_limit,
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
        )

        comparison_df = self._build_comparison_table(target_snapshot, similar_snapshots)
        self._save_results(target_snapshot, similar_snapshots, comparison_df)

        LOGGER.info("完成增强版PE/PB分析: %s (%s)", target_snapshot.display_name, symbol.symbol)
        LOGGER.info("=" * 70)

    # ------------------------------------------------------------------
    # 数据准备
    # ------------------------------------------------------------------
    def _analysis_date_str(self) -> str:
        return self.analysis_datetime.strftime("%Y-%m-%d")

    def _build_snapshot(
        self,
        symbol: SymbolInfo,
        *,
        force_refresh: bool,
        force_refresh_financials: bool,
    ) -> StockSnapshot:
        use_plain_labels = symbol.is_hk_market()
        dataset = self.data_access.prepare_dataset(
            symbolInfo=symbol,
            as_of_date=self._analysis_date_str(),
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
        )

        price_series = self._extract_price_series(dataset)
        latest_price = self._get_price_at(price_series, self.analysis_datetime)
        if latest_price is None or latest_price <= 0:
            raise DataQualityError(f"{symbol.symbol} 缺少有效的收盘价数据")

        total_shares = self._resolve_total_shares(dataset)
        float_shares = dataset.share_info.float_shares or None
        market_cap = latest_price * total_shares
        float_market_cap = (
            latest_price * float_shares if float_shares and float_shares > 0 else None
        )

        hk_abstract_metrics: Optional[Dict[str, Any]] = None
        if symbol.is_hk_market():
            hk_abstract_metrics = self._prepare_hk_financial_abstract_metrics(
                dataset,
                total_shares,
            )

        profit_sheet = dataset.financials.profit_sheet
        if profit_sheet.empty:
            raise DataQualityError(f"{symbol.symbol} 缺少利润表数据，无法计算PE")
        notice_col = self._extract_notice_column(profit_sheet, "利润表", symbol)
        profit_with_notice = profit_sheet.copy()
        profit_with_notice[notice_col] = pd.to_datetime(
            profit_with_notice[notice_col], errors="coerce"
        )
        profit_with_notice = profit_with_notice.dropna(subset=[notice_col])
        if profit_with_notice.empty:
            raise DataQualityError(f"{symbol.symbol} 缺少有效公告日期的利润表数据")
        sort_keys = [notice_col]
        if "REPORT_DATE" in profit_with_notice.columns:
            profit_with_notice["REPORT_DATE"] = pd.to_datetime(
                profit_with_notice["REPORT_DATE"], errors="coerce"
            )
            sort_keys.append("REPORT_DATE")
        profit_with_notice = profit_with_notice.sort_values(sort_keys)
        latest_profit_row = profit_with_notice.iloc[-1]
        latest_notice_date = pd.to_datetime(latest_profit_row[notice_col])
        if pd.isna(latest_notice_date):
            raise DataQualityError(f"{symbol.symbol} 最新利润表缺少公告日期")
        report_date = pd.to_datetime(
            latest_profit_row.get("REPORT_DATE"), errors="coerce"
        )
        if pd.isna(report_date):
            raise DataQualityError(f"{symbol.symbol} 最新利润表缺少 REPORT_DATE")

        if symbol.is_hk_market() and "NETPROFIT" in profit_sheet.columns:
            profit_sheet = profit_sheet.copy()
            profit_sheet["HOLDER_PROFIT"] = profit_sheet["NETPROFIT"]

        ttm_profit_df = calculate_rolling_ttm_profit(
            profit_sheet,
            logger=LOGGER,
        )
        if hk_abstract_metrics and not hk_abstract_metrics["ttm_df"].empty:
            ttm_profit_df = hk_abstract_metrics["ttm_df"]
        if ttm_profit_df.empty:
            raise DataQualityError(f"{symbol.symbol} 无法构建TTM净利润序列")

        ttm_profit_row = self._pick_ttm_row(ttm_profit_df, report_date)
        ttm_profit = float(ttm_profit_row["TTM_NET_PROFIT_RAW"])
        quarter_profit = float(ttm_profit_row.get("QUARTERLY_NET_PROFIT_RAW") or 0)

        cumulative_profit_raw = None
        cumulative_source = None
        preferred_cols = [
            "PARENT_NETPROFIT",
            "DEDUCT_PARENT_NETPROFIT",
            "NETPROFIT",
        ]
        for col in preferred_cols:
            val = latest_profit_row.get(col)
            if pd.notna(val):
                try:
                    cumulative_profit_raw = float(val)
                    cumulative_source = col
                    break
                except (TypeError, ValueError):
                    continue

        def _annualization_multiplier(date_val: Optional[pd.Timestamp]) -> float:
            if date_val is None or pd.isna(date_val):
                return 4.0
            month = date_val.month
            if month <= 3:
                return 4.0
            if month <= 6:
                return 2.0
            if month <= 9:
                return 4.0 / 3.0
            return 1.0

        multiplier = _annualization_multiplier(report_date)
        dynamic_profit = 0.0
        if report_date is not None and not pd.isna(report_date):
            month = report_date.month
            if month == 12:
                dynamic_profit = ttm_profit
            elif cumulative_profit_raw and cumulative_profit_raw != 0:
                dynamic_profit = cumulative_profit_raw * multiplier
            elif quarter_profit:
                dynamic_profit = quarter_profit * multiplier
        else:
            dynamic_profit = quarter_profit * multiplier if quarter_profit else 0.0

        if dynamic_profit == 0 and cumulative_profit_raw:
            dynamic_profit = cumulative_profit_raw

        yoy_percent_map = self._extract_net_profit_yoy_map(profit_sheet)

        if symbol.is_hk_market():
            deduct_ttm_df = ttm_profit_df.copy()
            deduct_profit = float(deduct_ttm_df.iloc[-1].get("TTM_NET_PROFIT_RAW") or 0)
        else:
            deduct_ttm_df = calculate_rolling_ttm_profit(
                profit_sheet,
                profit_column="DEDUCT_PARENT_NETPROFIT",
                logger=LOGGER,
            )
            deduct_profit = None
            if not deduct_ttm_df.empty:
                deduct_profit = float(
                    deduct_ttm_df.iloc[-1].get("TTM_NET_PROFIT_RAW") or 0
                )

        pe_ttm, pe_ttm_note = self._safe_ratio(market_cap, ttm_profit, "TTM净利润")
        pe_dynamic, pe_dynamic_note = self._safe_ratio(
            market_cap, dynamic_profit, "动态净利润"
        )
        if use_plain_labels:
            pe_deduct = pe_ttm
            pe_deduct_note = "港股财报无扣非口径，直接使用 PE(TTM)"
        else:
            pe_deduct, pe_deduct_note = (None, "缺少扣非TTM净利润")
            if deduct_profit:
                pe_deduct, pe_deduct_note = self._safe_ratio(
                    market_cap, deduct_profit, "扣非TTM净利润"
                )

        net_profit_growth, growth_note = self._calculate_ttm_yoy_growth_rate(
            deduct_ttm_df if not deduct_ttm_df.empty else ttm_profit_df,
            self.analysis_datetime,
            yoy_percent_map=yoy_percent_map,
        )
        peg = None
        base_pe_for_peg = pe_deduct or pe_ttm
        peg_note = growth_note
        if base_pe_for_peg and net_profit_growth:
            growth_percent = net_profit_growth * 100
            if base_pe_for_peg > 0 and growth_percent > 0 and abs(growth_percent) >= 1e-6:
                peg = base_pe_for_peg / growth_percent
                if pe_deduct:
                    suffix_note = "PE(TTM)" if use_plain_labels else "扣非PE(TTM)"
                    peg_note = (growth_note or "") + f"；使用{suffix_note}"
            else:
                peg_note = (growth_note or "") + "；PEG因PE或增速为非正值而置为None"

        if symbol.is_hk_market() and hk_abstract_metrics:
            latest_bps = hk_abstract_metrics.get("latest_bps")
            pb = None
            if latest_bps and latest_bps != 0:
                pb = latest_price / latest_bps
            ps = None
        else:
            pb = self._compute_pb(dataset, market_cap, symbol)
            ps = self._compute_ps(profit_sheet, market_cap)

        if symbol.is_hk_market() and hk_abstract_metrics:
            latest_eps = hk_abstract_metrics.get("latest_eps")
            if latest_eps and latest_eps != 0:
                pe_ttm = latest_price / latest_eps
                pe_ttm_note = f"基于财务摘要 EPS_TTM={latest_eps:.3f}"
                pe_deduct = pe_ttm
                pe_deduct_note = "港股基于 EPS_TTM 估算 PE"
                ttm_profit = latest_eps * total_shares
                deduct_profit = ttm_profit

        if symbol.is_hk_market() and hk_abstract_metrics:
            equity_value_map_override = hk_abstract_metrics.get("equity_map") or {}
        else:
            equity_value_map_override = None

        historical_source = deduct_ttm_df if not deduct_ttm_df.empty else ttm_profit_df
        if equity_value_map_override:
            equity_value_map = equity_value_map_override
        else:
            equity_value_map = self._build_equity_value_map(dataset, symbol)
        historical_pe = self._build_historical_pe_records(
            historical_source, price_series, total_shares, equity_value_map
        )
        historical_peg = self._build_historical_peg_records(
            historical_source, price_series, total_shares, yoy_percent_map
        )
        extreme_price_metrics = self._build_extreme_price_metrics(
            price_series=price_series,
            deduct_ttm_df=deduct_ttm_df,
            fallback_ttm_df=ttm_profit_df,
            total_shares=total_shares,
            yoy_percent_map=yoy_percent_map,
            use_plain_labels=use_plain_labels,
        )

        snapshot = StockSnapshot(
            symbol=symbol,
            display_name=symbol.stock_name,
            analysis_time=self.analysis_datetime,
            price=latest_price,
            total_shares=total_shares,
            float_shares=float_shares,
            market_cap=market_cap,
            float_market_cap=float_market_cap,
            pe_ttm=pe_ttm,
            pe_dynamic=pe_dynamic,
            pe_deduct=pe_deduct,
            pb=pb,
            ps=ps,
            peg=peg,
            net_profit_growth=net_profit_growth,
            pe_ttm_note=pe_ttm_note,
            pe_dynamic_note=pe_dynamic_note,
            pe_deduct_note=pe_deduct_note,
            peg_note=peg_note,
            historical_pe=historical_pe,
            historical_peg=historical_peg,
            extreme_price_metrics=extreme_price_metrics,
            reason="",
            ttm_profit_raw=ttm_profit,
            use_plain_pe_label=use_plain_labels,
        )
        return snapshot

    def _build_similar_snapshots(
        self,
        symbol: SymbolInfo,
        *,
        limit: int,
        force_refresh: bool,
        force_refresh_financials: bool,
    ) -> List[StockSnapshot]:
        if limit <= 0:
            return []
        similar_entries = get_similar_stocks(symbol, self.base_dir)[:limit]
        snapshots: List[StockSnapshot] = []
        for entry in similar_entries:
            code = entry.get("code")
            name = entry.get("name")
            if not code:
                continue
            try:
                similar_info = parse_symbol(code)
            except Exception as exc:
                LOGGER.warning("解析相似股票代码 %s 失败: %s", code, exc)
                continue
            try:
                snap = self._build_snapshot(
                    similar_info,
                    force_refresh=force_refresh,
                    force_refresh_financials=force_refresh_financials,
                )
                snap.reason = entry.get("reason", "")
                snapshots.append(snap)
            except DataQualityError as exc:
                LOGGER.warning("相似股票 %s 数据不足: %s", similar_info.symbol, exc)
        return snapshots

    def _extract_net_profit_yoy_map(
        self, profit_sheet: pd.DataFrame
    ) -> Dict[str, Tuple[float, str]]:
        yoy_map: Dict[str, Tuple[float, str]] = {}
        if profit_sheet.empty or "REPORT_DATE" not in profit_sheet.columns:
            return yoy_map

        columns_priority = [
            "DEDUCT_PARENT_NETPROFIT_YOY",
            "PARENT_NETPROFIT_YOY",
            "NETPROFIT_YOY",
        ]
        available_cols = [col for col in columns_priority if col in profit_sheet.columns]
        if not available_cols:
            return yoy_map

        work = profit_sheet.copy()
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE"])

        for _, row in work.iterrows():
            date_str = row["REPORT_DATE"].strftime("%Y-%m-%d")
            for col in available_cols:
                value = row.get(col)
                if value in (None, "", "nan"):
                    continue
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(numeric_value):
                    continue
                yoy_map[date_str] = (numeric_value, col)
                break
        return yoy_map

    # ------------------------------------------------------------------
    # 计算细节
    # ------------------------------------------------------------------
    def _extract_price_series(self, dataset: PreparedData) -> pd.Series:
        frame = dataset.prices.frame.copy()
        if frame.index.name != "日期":
            frame.index = pd.to_datetime(frame.index, errors="coerce")
        else:
            frame.index = pd.to_datetime(frame.index, errors="coerce")
        frame = frame.sort_index()
        price_series = frame.get("收盘")
        if price_series is None:
            raise DataQualityError("价格数据缺少 '收盘' 列")
        price_series = pd.to_numeric(price_series, errors="coerce").dropna()
        if price_series.empty:
            raise DataQualityError("收盘价序列为空")
        price_series.index = pd.to_datetime(price_series.index)
        return price_series

    @staticmethod
    def _get_price_at(series: pd.Series, when: datetime) -> Optional[float]:
        filtered = series[series.index <= pd.Timestamp(when)]
        if filtered.empty:
            return float(series.iloc[-1])
        return float(filtered.iloc[-1])

    @staticmethod
    def _resolve_total_shares(dataset: PreparedData) -> float:
        shares = dataset.share_info.total_shares
        if shares and shares > 0:
            return float(shares)
        frame = dataset.prices.frame
        if "流通股本" in frame.columns:
            candidate = pd.to_numeric(frame["流通股本"], errors="coerce").dropna()
            if not candidate.empty:
                return float(candidate.iloc[-1])
        raise DataQualityError("缺少有效总股本数据，无法计算市值")

    @staticmethod
    def _safe_ratio(
        numerator: float, denominator: float, label: str
    ) -> Tuple[Optional[float], str]:
        if denominator is None or denominator == 0:
            return None, f"{label}为0，无法计算"
        ratio = numerator / denominator
        if not math.isfinite(ratio):
            return None, f"{label}异常，无法计算"
        return ratio, ""

    def _compute_pb(
        self, dataset: PreparedData, market_cap: float, symbol: SymbolInfo
    ) -> Optional[float]:
        sheet = dataset.financials.balance_sheet
        if sheet.empty:
            return None
        work = sheet.copy()
        notice_col = self._extract_notice_column(sheet, "资产负债表", symbol)
        work[notice_col] = pd.to_datetime(work[notice_col], errors="coerce")
        work = work.dropna(subset=[notice_col]).sort_values(notice_col)
        work = work[work[notice_col] <= self.analysis_datetime]
        if work.empty:
            return None
        row = work.iloc[-1]
        equity_value = None
        for key in ("TOTAL_EQUITY", "TOTAL_PARENT_EQUITY"):
            if key in row and pd.notna(row[key]) and row[key] > 0:
                equity_value = float(row[key])
                break
        if not equity_value:
            return None
        pb, _ = self._safe_ratio(market_cap, equity_value, "净资产")
        return pb

    def _build_equity_value_map(
        self, dataset: PreparedData, symbol: SymbolInfo
    ) -> Dict[str, float]:
        """构建公告期 -> 归母净资产的映射，供历史PB计算使用。"""
        sheet = dataset.financials.balance_sheet
        if sheet.empty:
            return {}
        work = sheet.copy()
        notice_col = self._extract_notice_column(sheet, "资产负债表", symbol)
        work[notice_col] = pd.to_datetime(work[notice_col], errors="coerce")
        if "REPORT_DATE" in work.columns:
            work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=[notice_col]).sort_values(notice_col)
        work = work[work[notice_col] <= self.analysis_datetime]
        equity_map: Dict[str, float] = {}
        if work.empty:
            return equity_map
        for _, row in work.iterrows():
            equity_value = None
            for key in ("TOTAL_PARENT_EQUITY", "TOTAL_EQUITY"):
                if key in row and pd.notna(row[key]) and row[key] > 0:
                    equity_value = float(row[key])
                    break
            if not equity_value:
                continue
            report_dt = row.get("REPORT_DATE")
            if pd.isna(report_dt):
                report_dt = row.get(notice_col)
            if pd.isna(report_dt):
                continue
            key_str = pd.Timestamp(report_dt).strftime("%Y-%m-%d")
            equity_map[key_str] = equity_value
        return equity_map

    @classmethod
    def _extract_notice_column(
        cls, frame: pd.DataFrame, label: str, symbol: SymbolInfo
    ) -> str:
        for candidate in cls.NOTICE_DATE_COLUMNS:
            if candidate in frame.columns:
                return candidate
        raise DataQualityError(f"{symbol.symbol} 的{label}缺少公告日期列")

    @staticmethod
    def _pick_ttm_row(ttm_df: pd.DataFrame, report_date: pd.Timestamp | None) -> pd.Series:
        if ttm_df.empty:
            raise DataQualityError("无法构建TTM净利润序列")
        if report_date is not None and "REPORT_DATE" in ttm_df.columns:
            matches = ttm_df[
                pd.to_datetime(ttm_df["REPORT_DATE"], errors="coerce") == report_date
            ]
            if not matches.empty:
                return matches.iloc[-1]
        return ttm_df.iloc[-1]

    def _compute_ps(
        self, profit_sheet: pd.DataFrame, market_cap: float
    ) -> Optional[float]:
        revenue_col = None
        for key in ("TOTAL_OPERATE_INCOME", "OPERATE_INCOME"):
            if key in profit_sheet.columns:
                revenue_col = key
                break
        if not revenue_col:
            return None
        revenue_frame = profit_sheet[["REPORT_DATE", revenue_col]].copy()
        revenue_frame = revenue_frame.rename(columns={revenue_col: "PARENT_NETPROFIT"})
        revenue_ttm = calculate_rolling_ttm_profit(revenue_frame, logger=LOGGER)
        if revenue_ttm.empty:
            return None
        latest = revenue_ttm.iloc[-1]
        revenue_value = float(latest.get("TTM_NET_PROFIT_RAW") or 0)
        if revenue_value <= 0:
            return None
        ps, _ = self._safe_ratio(market_cap, revenue_value, "TTM营收")
        return ps

    def _prepare_hk_financial_abstract_metrics(
        self,
        dataset: PreparedData,
        total_shares: float,
    ) -> Optional[Dict[str, Any]]:
        abstract = dataset.financials.financial_abstract
        if abstract is None or abstract.empty or "REPORT_DATE" not in abstract.columns:
            return None
        work = abstract.copy()
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE"])
        work = work[work["REPORT_DATE"] <= self.analysis_datetime]
        if work.empty:
            return None
        for col in ("EPS_TTM", "BPS"):
            if col in work.columns:
                work[col] = pd.to_numeric(work[col], errors="coerce")
        work = work.dropna(subset=["EPS_TTM"])
        if work.empty:
            return None
        work = work.sort_values("REPORT_DATE").drop_duplicates(
            subset=["REPORT_DATE"], keep="last"
        )
        work["TTM_NET_PROFIT_RAW"] = work["EPS_TTM"] * total_shares
        work = work.dropna(subset=["TTM_NET_PROFIT_RAW"])
        if work.empty:
            return None

        ttm_df = work[["REPORT_DATE", "TTM_NET_PROFIT_RAW"]].copy()
        equity_map: Dict[str, float] = {}
        if "BPS" in work.columns:
            bps_rows = work.dropna(subset=["BPS"])
            for _, row in bps_rows.iterrows():
                bps_value = float(row["BPS"])
                if not math.isfinite(bps_value) or bps_value == 0:
                    continue
                equity_map[row["REPORT_DATE"].strftime("%Y-%m-%d")] = (
                    bps_value * total_shares
                )

        latest_row = work.iloc[-1]
        latest_eps = (
            float(latest_row["EPS_TTM"])
            if pd.notna(latest_row["EPS_TTM"])
            else None
        )
        latest_bps = None
        if "BPS" in latest_row and pd.notna(latest_row["BPS"]) and latest_row["BPS"] != 0:
            latest_bps = float(latest_row["BPS"])

        return {
            "frame": work,
            "ttm_df": ttm_df,
            "equity_map": equity_map,
            "latest_eps": latest_eps,
            "latest_bps": latest_bps,
        }

    def _calculate_ttm_yoy_growth_rate(
        self,
        ttm_profit_data: pd.DataFrame,
        analysis_cutoff: datetime | None,
        *,
        yoy_percent_map: Optional[Dict[str, Tuple[float, str]]] = None,
    ) -> Tuple[Optional[float], str]:
        if ttm_profit_data is None or ttm_profit_data.empty:
            return None, "TTM净利润数据为空"
        work = ttm_profit_data.copy()
        if "REPORT_DATE" not in work.columns or "TTM_NET_PROFIT_RAW" not in work.columns:
            return None, "TTM净利润缺少必要字段"
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE", "TTM_NET_PROFIT_RAW"])
        work = work.sort_values("REPORT_DATE").reset_index(drop=True)
        if analysis_cutoff is not None:
            work = work[work["REPORT_DATE"] <= analysis_cutoff]
        if work.empty:
            return None, "TTM净利润序列不足以计算同比"

        latest_row = work.iloc[-1]
        latest_key = latest_row["REPORT_DATE"].strftime("%Y-%m-%d")
        if yoy_percent_map:
            entry = yoy_percent_map.get(latest_key)
            if entry:
                yoy_percent, source_col = entry
                note_parts = []
                if len(work) >= 5:
                    prev_row = work.iloc[-5]
                    note_parts.append(
                        f"比较 {prev_row['REPORT_DATE'].date()} vs {latest_row['REPORT_DATE'].date()}"
                    )
                note_parts.append(f"利润表字段 {source_col}")
                note = "；".join(note_parts)
                return yoy_percent / 100, note

        if len(work) < 5:
            return None, "TTM净利润序列不足以计算同比"
        for idx in range(len(work) - 1, 3, -1):
            latest_row = work.iloc[idx]
            prev_row = work.iloc[idx - 4]
            current_profit = float(latest_row["TTM_NET_PROFIT_RAW"])
            prev_profit = float(prev_row["TTM_NET_PROFIT_RAW"])
            if prev_profit == 0:
                continue
            growth_rate = (current_profit - prev_profit) / abs(prev_profit)
            note = (
                f"比较 {prev_row['REPORT_DATE'].date()} vs {latest_row['REPORT_DATE'].date()}"
            )
            return growth_rate, note
        return None, "TTM净利润同比不可用"

    def _build_historical_pe_records(
        self,
        ttm_profit_df: pd.DataFrame,
        price_series: pd.Series,
        total_shares: float,
        equity_value_map: Optional[Dict[str, float]] = None,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        if total_shares <= 0:
            return records
        window_start = self.analysis_datetime - timedelta(days=365 * 2)
        for _, row in ttm_profit_df.iterrows():
            report_date = pd.to_datetime(row["REPORT_DATE"])
            if pd.isna(report_date) or report_date < window_start:
                continue
            ttm_profit = float(row.get("TTM_NET_PROFIT_RAW") or 0)
            price = self._get_price_at(price_series, report_date.to_pydatetime())
            if price is None or price <= 0:
                continue
            eps = ttm_profit / total_shares
            pe_value = price * total_shares / ttm_profit
            pb_value = None
            if equity_value_map:
                equity_val = equity_value_map.get(report_date.strftime("%Y-%m-%d"))
                if equity_val and equity_val > 0:
                    pb_value = price * total_shares / equity_val
            records.append(
                {
                    "报告期": report_date.strftime("%Y-%m-%d"),
                    "TTM净利润(亿元)": ttm_profit / 1e8,
                    "TTM每股收益(元)": eps,
                    "股价(元)": price,
                    "PE": pe_value,
                    "PB": pb_value,
                }
            )
        return records[-12:]

    def _build_historical_peg_records(
        self,
        ttm_profit_df: pd.DataFrame,
        price_series: pd.Series,
        total_shares: float,
        yoy_percent_map: Optional[Dict[str, Tuple[float, str]]] = None,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        if len(ttm_profit_df) < 5 or total_shares <= 0:
            return records
        ttm_profit_df = ttm_profit_df.sort_values("REPORT_DATE").reset_index(drop=True)
        window_start = self.analysis_datetime - timedelta(days=365 * 2)
        for idx in range(4, len(ttm_profit_df)):
            current = ttm_profit_df.iloc[idx]
            prev = ttm_profit_df.iloc[idx - 4]
            current_profit = float(current.get("TTM_NET_PROFIT_RAW") or 0)
            prev_profit = float(prev.get("TTM_NET_PROFIT_RAW") or 0)
            if prev_profit == 0:
                continue
            growth = (current_profit - prev_profit) / abs(prev_profit)
            report_date = pd.to_datetime(current["REPORT_DATE"])
            if pd.isna(report_date) or report_date < window_start:
                continue
            price = self._get_price_at(price_series, report_date.to_pydatetime())
            if price is None or price <= 0:
                continue
            pe_value = price * total_shares / current_profit if current_profit != 0 else None
            date_key = report_date.strftime("%Y-%m-%d")
            yoy_percent = None
            if yoy_percent_map and date_key in yoy_percent_map:
                yoy_percent = yoy_percent_map[date_key][0]
            if yoy_percent is None:
                yoy_percent = growth * 100
            denominator = yoy_percent if yoy_percent else None
            # 仅当 PE>0 且 增速>0 时计算 PEG，否则置 None
            peg = None
            if pe_value and pe_value > 0 and denominator and denominator > 0:
                peg = pe_value / denominator
            records.append(
                {
                    "报告期": report_date.strftime("%Y-%m-%d"),
                    "PE": pe_value,
                    "净利润同比增长率(%)": yoy_percent,
                    "PEG": peg,
                }
            )
        return records[-12:]

    def _build_extreme_price_metrics(
        self,
        price_series: pd.Series,
        deduct_ttm_df: pd.DataFrame,
        fallback_ttm_df: pd.DataFrame,
        total_shares: float,
        yoy_percent_map: Optional[Dict[str, Tuple[float, str]]] = None,
        *,
        years: int = 3,
        use_plain_labels: bool = False,
    ) -> List[Dict[str, Any]]:
        if total_shares <= 0 or price_series.empty:
            return []
        window_start = self.analysis_datetime - timedelta(days=365 * years)
        window_prices = price_series[price_series.index >= window_start]
        if window_prices.empty:
            return []

        deduct_df = self._normalize_report_df(deduct_ttm_df)
        fallback_df = self._normalize_report_df(fallback_ttm_df)
        report_df = deduct_df if not deduct_df.empty else fallback_df
        if report_df.empty:
            return []

        high_windows = [
            ("三年最高价", timedelta(days=365 * 3)),
            ("一年最高价", timedelta(days=365)),
            ("三个月最高价", timedelta(days=90)),
        ]
        highs: List[Tuple[str, pd.Timestamp, float]] = []
        for label, delta in high_windows:
            slice_series = price_series[price_series.index >= (self.analysis_datetime - delta)]
            point = self._select_extreme_points(slice_series, count=1, largest=True)
            if point:
                ts, value = point[0]
                highs.append((label, ts, value))

        lows = self._select_extreme_points(window_prices, count=1, largest=False)

        records: List[Dict[str, Any]] = []
        for label, ts, price in highs:
            records.append(
                self._compose_extreme_record(
                    label=label,
                    price=float(price),
                    price_date=ts,
                    total_shares=total_shares,
                    base_df=report_df,
                    yoy_percent_map=yoy_percent_map,
                    use_plain_labels=use_plain_labels,
                )
            )
        if lows:
            ts, price = lows[0]
            records.append(
                self._compose_extreme_record(
                    label="三年最低价",
                    price=float(price),
                    price_date=ts,
                    total_shares=total_shares,
                    base_df=report_df,
                    yoy_percent_map=yoy_percent_map,
                    use_plain_labels=use_plain_labels,
                )
            )
        return [record for record in records if record]

    @staticmethod
    def _normalize_report_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        work = df.copy()
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE"]).sort_values("REPORT_DATE").reset_index(drop=True)
        return work

    @staticmethod
    def _select_extreme_points(
        series: pd.Series,
        count: int,
        *,
        largest: bool,
    ) -> List[Tuple[pd.Timestamp, float]]:
        ordered = series.sort_values(ascending=not largest)
        points: List[Tuple[pd.Timestamp, float]] = []
        seen_dates: set = set()
        for idx, value in ordered.items():
            if pd.isna(value):
                continue
            date_key = pd.Timestamp(idx).date()
            if date_key in seen_dates:
                continue
            seen_dates.add(date_key)
            points.append((pd.Timestamp(idx), float(value)))
            if len(points) >= count:
                break
        return points

    def _compose_extreme_record(
        self,
        *,
        label: str,
        price: float,
        price_date: pd.Timestamp,
        total_shares: float,
        base_df: pd.DataFrame,
        yoy_percent_map: Optional[Dict[str, Tuple[float, str]]] = None,
        use_plain_labels: bool = False,
    ) -> Optional[Dict[str, Any]]:
        row = self._find_latest_report_row(base_df, price_date)
        if row is None:
            return None
        report_date = row["REPORT_DATE"]
        ttm_profit = float(row.get("TTM_NET_PROFIT_RAW") or 0)
        pe_value = None
        if ttm_profit not in (None, 0):
            pe_value = price * total_shares / ttm_profit
        yoy_percent = self._resolve_yoy_percent_for_report(
            report_date, yoy_percent_map, base_df
        )
        peg = None
        if (
            pe_value is not None
            and yoy_percent is not None
            and yoy_percent > 0
            and abs(yoy_percent) >= 1e-6
        ):
            peg = pe_value / yoy_percent
        pe_field = "PE" if use_plain_labels else "扣非PE"
        growth_field = "净利润同比增长率(%)" if use_plain_labels else "扣非净利润同比增长率(%)"
        return {
            "类别": label,
            "价格日期": price_date.strftime("%Y-%m-%d"),
            "价格(元)": price,
            "参考财报日期": report_date.strftime("%Y-%m-%d"),
            "TTM净利润(亿元)": ttm_profit / 1e8 if ttm_profit else None,
            pe_field: pe_value,
            growth_field: yoy_percent,
            "PEG": peg,
        }

    @staticmethod
    def _find_latest_report_row(df: pd.DataFrame, cutoff: pd.Timestamp) -> Optional[pd.Series]:
        if df.empty:
            return None
        mask = df["REPORT_DATE"] <= cutoff.to_pydatetime()
        filtered = df.loc[mask]
        if filtered.empty:
            return None
        return filtered.iloc[-1]

    def _resolve_yoy_percent_for_report(
        self,
        report_date: pd.Timestamp,
        yoy_percent_map: Optional[Dict[str, Tuple[float, str]]],
        df: pd.DataFrame,
    ) -> Optional[float]:
        key = report_date.strftime("%Y-%m-%d")
        if yoy_percent_map and key in yoy_percent_map:
            return float(yoy_percent_map[key][0])
        # fallback: self-calculated
        if df.empty:
            return None
        matches = df.index[df["REPORT_DATE"] == report_date]
        if len(matches) == 0:
            return None
        idx = int(matches[0])
        if idx < 4:
            return None
        current = float(df.iloc[idx].get("TTM_NET_PROFIT_RAW") or 0)
        prev = float(df.iloc[idx - 4].get("TTM_NET_PROFIT_RAW") or 0)
        if prev == 0:
            return None
        return (current - prev) / abs(prev) * 100

    # ------------------------------------------------------------------
    # 报告 & 输出
    # ------------------------------------------------------------------
    def _build_comparison_table(
        self,
        target: StockSnapshot,
        similars: Sequence[StockSnapshot],
    ) -> pd.DataFrame:
        current_pe_label = self._pe_label(target, prefix="当前", suffix="(TTM)")
        rows = [
            self._snapshot_to_row(
                target,
                role="目标股票",
                remark=target.peg_note,
                current_pe_label=current_pe_label,
            ),
        ]
        for snap in similars:
            rows.append(
                self._snapshot_to_row(
                    snap,
                    role="相似股票",
                    remark="",
                    current_pe_label=current_pe_label,
                )
            )
        columns = [
            "股票代码",
            "股票名称",
            "角色",
            "当前价格",
            current_pe_label,
            "动态PE",
            "当前PEG",
            "净利润增长率",
            "当前PB",
            "当前PS",
            "备注",
        ]
        return pd.DataFrame(rows, columns=columns)

    def _snapshot_to_row(
        self,
        snapshot: StockSnapshot,
        *,
        role: str,
        remark: str,
        current_pe_label: str,
    ) -> Dict[str, Any]:
        def _fmt(value: Optional[float]) -> Any:
            if value is None or not math.isfinite(value):
                return None
            return round(value, 3)

        growth = (
            f"{snapshot.net_profit_growth * 100:.3f}%"
            if snapshot.net_profit_growth is not None
            else "--"
        )
        display_pe = self._get_display_pe_value(snapshot)
        return {
            "股票代码": snapshot.symbol.symbol,
            "股票名称": snapshot.display_name,
            "角色": role,
            "当前价格": round(snapshot.price, 3),
            current_pe_label: _fmt(display_pe),
            "动态PE": _fmt(snapshot.pe_dynamic),
            "当前PEG": _fmt(snapshot.peg),
            "净利润增长率": growth,
            "当前PB": _fmt(snapshot.pb),
            "当前PS": _fmt(snapshot.ps),
            "备注": remark or snapshot.reason or "",
        }

    @staticmethod
    def _pe_label(snapshot: StockSnapshot, *, prefix: str = "", suffix: str = "") -> str:
        base = "PE" if snapshot.use_plain_pe_label else "扣非PE"
        return f"{prefix}{base}{suffix}"

    @staticmethod
    def _growth_label(snapshot: StockSnapshot) -> str:
        return "净利润同比增长率(%)" if snapshot.use_plain_pe_label else "扣非净利润同比增长率(%)"

    @staticmethod
    def _get_display_pe_value(snapshot: StockSnapshot) -> Optional[float]:
        if snapshot.use_plain_pe_label:
            return snapshot.pe_ttm if snapshot.pe_ttm is not None else snapshot.pe_deduct
        return snapshot.pe_deduct

    def _save_results(
        self,
        target_snapshot: StockSnapshot,
        similar_snapshots: Sequence[StockSnapshot],
        comparison_df: pd.DataFrame,
    ) -> None:
        paths = self._prepare_paths(target_snapshot.symbol)
        analysis_dir = paths["analysis"]
        stock_root = paths["root"]
        cleanup_output_directory(analysis_dir)

        stamp = target_snapshot.analysis_time.strftime("%Y%m%d")
        prefix = stock_root.name
        json_file = analysis_dir / f"{prefix}_{stamp}_enhanced_pe_analysis.json"
        csv_file = analysis_dir / f"{prefix}_{stamp}_pe_comparison_table.csv"
        md_file = analysis_dir / f"{prefix}_{stamp}_enhanced_pe_analysis.md"

        payload = self._round_float_values(
            self._build_json_payload(target_snapshot, similar_snapshots, comparison_df)
        )
        json_file.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
            encoding="utf-8",
        )
        comparison_df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        self._write_markdown(md_file, target_snapshot, similar_snapshots, comparison_df)

        try:
            dest = self.transaction_package_dir / md_file.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(md_file, dest)
            LOGGER.info("Markdown报告已复制到 %s", dest)
        except Exception as exc:
            LOGGER.warning("复制Markdown报告失败: %s", exc)

        LOGGER.info("JSON数据: %s", json_file)
        LOGGER.info("对比表格: %s", csv_file)
        LOGGER.info("Markdown报告: %s", md_file)

    def _prepare_paths(self, symbol: SymbolInfo) -> Dict[str, Path]:
        stock_root = get_stock_data_dir(symbol, base_dir=self.base_dir)
        analysis_dir = ensure_stock_subdir(symbol, "pe_pb_analysis", base_dir=self.base_dir)
        return {"root": stock_root, "analysis": analysis_dir}

    def _build_json_payload(
        self,
        target_snapshot: StockSnapshot,
        similar_snapshots: Sequence[StockSnapshot],
        comparison_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        def _snapshot_dict(snapshot: StockSnapshot) -> Dict[str, Any]:
            pe_label = self._pe_label(snapshot, suffix="(TTM)")
            pe_note_label = f"{pe_label}备注"
            display_pe = self._get_display_pe_value(snapshot)
            return {
                "股票代码": snapshot.symbol.symbol,
                "股票名称": snapshot.display_name,
                "当前价格": snapshot.price,
                "总市值": snapshot.market_cap,
                "流通市值": snapshot.float_market_cap,
                "总股本": snapshot.total_shares,
                "流通股本": snapshot.float_shares,
                "分析时间": snapshot.analysis_time.strftime("%Y-%m-%d %H:%M:%S"),
                "PE统计": {
                    pe_label: display_pe,
                    pe_note_label: snapshot.pe_deduct_note,
                    "动态PE": snapshot.pe_dynamic,
                    "动态PE备注": snapshot.pe_dynamic_note,
                    "当前PEG": snapshot.peg,
                    "PEG备注": snapshot.peg_note,
                    "净利润增长率": snapshot.net_profit_growth,
                },
                "PB统计": {"当前PB": snapshot.pb} if snapshot.pb else {},
                "PS统计": {"当前PS": snapshot.ps} if snapshot.ps else {},
                "历史PE数据": snapshot.historical_pe,
                "历史PEG数据": snapshot.historical_peg,
                "价格极值回顾": snapshot.extreme_price_metrics,
                **({"估值口径提示": HK_PROFIT_NOTE} if snapshot.use_plain_pe_label else {}),
            }

        return {
            "target": _snapshot_dict(target_snapshot),
            "similar": [_snapshot_dict(snap) for snap in similar_snapshots],
            "comparison_table": comparison_df.to_dict(orient="records"),
        }

    @staticmethod
    def _round_float_values(obj: Any, digits: int = 3) -> Any:
        if isinstance(obj, dict):
            return {k: EnhancedPEPBAnalyzer._round_float_values(v, digits) for k, v in obj.items()}
        if isinstance(obj, list):
            return [EnhancedPEPBAnalyzer._round_float_values(item, digits) for item in obj]
        if isinstance(obj, Real) and not isinstance(obj, (bool, Integral)):
            value = float(obj)
            if math.isfinite(value):
                return round(value, digits)
        return obj

    def _write_markdown(
        self,
        md_path: Path,
        target_snapshot: StockSnapshot,
        similar_snapshots: Sequence[StockSnapshot],
        comparison_df: pd.DataFrame,
    ) -> None:
        info = target_snapshot
        fmt_percent = (
            f"{info.net_profit_growth * 100:.3f}%"
            if info.net_profit_growth is not None
            else "--"
        )
        pe_label = self._pe_label(info, suffix="(TTM)")
        peg_base_label = self._pe_label(info)
        lines = [
            f"# {info.display_name}({info.symbol.symbol}) 增强版PE/PB/PS分析报告",
            "",
            f"- **分析日期**: {info.analysis_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"- **当前价格**: {info.price:.3f} 元",
            f"- **总市值**: {info.market_cap/1e8:.3f} 亿元",
            f"- **流通市值**: {info.float_market_cap/1e8:.3f} 亿元" if info.float_market_cap else "",
            "",
            "## 估值摘要",
            "",
            f"- {pe_label}: {self._format_metric(self._get_display_pe_value(info), info.pe_deduct_note)}",
            f"- 动态PE: {self._format_metric(info.pe_dynamic, info.pe_dynamic_note)}",
            f"- 当前PEG（基于{peg_base_label}）: {self._format_metric(info.peg, info.peg_note)}",
            f"- 净利润同比增长率: {fmt_percent}",
            f"- PB: {self._format_metric(info.pb)}",
            f"- PS: {self._format_metric(info.ps)}",
        ]
        if info.use_plain_pe_label:
            lines.append("")
            lines.append(f"> ⚠️ {HK_PROFIT_NOTE}")
        lines.append(f"## {info.display_name}与相似股票的PE/PB/PS 对比")
        lines.append("")
        lines.append(comparison_df.to_markdown(index=False))
        lines.append("")

        history_table = self._build_history_table(info)
        if history_table is not None and not history_table.empty:
            lines.append("## 最近两年净利润增速与PE/PEG/PB")
            lines.append("")
            lines.append(history_table.to_markdown(index=False, floatfmt=".3f").replace("nan", ""))
            lines.append("")
            lines.append("_* 表示估值基准日数据_")
            lines.append("")

        if info.extreme_price_metrics:
            extreme_df = pd.DataFrame(info.extreme_price_metrics)
            lines.append("## 最近三年最高/最低股价及对应估值指标")
            lines.append("")
            lines.append(extreme_df.to_markdown(index=False, floatfmt=".3f").replace("nan", ""))
            lines.append("")

        md_path.write_text("\n".join(filter(None, lines)), encoding="utf-8")
        LOGGER.info("Markdown报告已生成: %s", md_path)

    @staticmethod
    def _format_metric(value: Optional[float], note: str = "") -> str:
        if value is None or not math.isfinite(value):
            return f"数据缺失 {note}".strip()
        base = f"{value:.3f}"
        return f"{base}（{note}）" if note else base

    def _build_history_table(self, snapshot: StockSnapshot) -> Optional[pd.DataFrame]:
        pe_col_name = self._pe_label(snapshot)
        growth_col_name = self._growth_label(snapshot)
        desired_order = [
            "报告期",
            "TTM净利润(亿元)",
            "股价(元)",
            pe_col_name,
            "PB",
            growth_col_name,
            "PEG",
        ]

        record_map: Dict[str, Dict[str, Any]] = {}
        for entry in snapshot.historical_pe:
            report_key = entry.get("报告期")
            if not report_key:
                continue
            record_map[report_key] = {
                "报告期": report_key,
                "TTM净利润(亿元)": entry.get("TTM净利润(亿元)"),
                "股价(元)": entry.get("股价(元)"),
                pe_col_name: entry.get("PE"),
                "PB": entry.get("PB"),
                growth_col_name: None,
                "PEG": None,
            }

        for entry in snapshot.historical_peg:
            report_key = entry.get("报告期")
            if not report_key:
                continue
            record = record_map.setdefault(
                report_key,
                {
                    "报告期": report_key,
                    "TTM净利润(亿元)": None,
                    "股价(元)": None,
                    pe_col_name: entry.get("PE"),
                    "PB": entry.get("PB"),
                    growth_col_name: None,
                    "PEG": None,
                },
            )
            if entry.get("净利润同比增长率(%)") is not None:
                record[growth_col_name] = entry["净利润同比增长率(%)"]
            if entry.get("PEG") is not None:
                record["PEG"] = entry["PEG"]

        current_row = {
            "报告期": snapshot.analysis_time.strftime("%Y-%m-%d") + "*",
            "TTM净利润(亿元)": snapshot.ttm_profit_raw / 1e8 if snapshot.ttm_profit_raw else None,
            "股价(元)": snapshot.price,
            pe_col_name: self._get_display_pe_value(snapshot) or snapshot.pe_ttm,
            "PB": snapshot.pb,
            growth_col_name: snapshot.net_profit_growth * 100 if snapshot.net_profit_growth is not None else None,
            "PEG": snapshot.peg,
        }
        record_map[current_row["报告期"]] = current_row

        combined = pd.DataFrame(list(record_map.values()), columns=desired_order)
        report_str = combined["报告期"].astype(str)
        combined["__is_current"] = report_str.str.endswith("*").astype(int)
        combined["__sort"] = pd.to_datetime(report_str.str.replace("*", ""), errors="coerce")
        combined = combined.sort_values(
            ["__is_current", "__sort"], ascending=[False, False]
        ).drop(columns=["__is_current", "__sort"])

        for col in ("TTM净利润(亿元)", "股价(元)", pe_col_name, "PB", growth_col_name, "PEG"):
            combined[col] = combined[col].apply(
                lambda x: round(x, 3) if isinstance(x, (int, float)) and math.isfinite(x) else x
            )
        return combined


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="增强版PE/PB历史数据分析工具（重构版）")
    parser.add_argument("stock_code", help="股票代码（如 002028.SZ）")
    parser.add_argument("--refresh", "-r", action="store_true", help="强制刷新行情+财报缓存")
    parser.add_argument(
        "--refresh-financials",
        action="store_true",
        help="仅刷新财报缓存",
    )
    parser.add_argument(
        "--today_time",
        help="估值基准日期，YYYY-MM-DD（默认当前日期）",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    try:
        analysis_dt = None
        if args.today_time:
            analysis_dt = datetime.strptime(args.today_time, "%Y-%m-%d")
        symbol = parse_symbol(args.stock_code)
        analyzer = EnhancedPEPBAnalyzer(analysis_datetime=analysis_dt)
        analyzer.analyze_stock(
            symbol,
            force_refresh=args.refresh,
            force_refresh_financials=args.refresh_financials,
        )
    except DataQualityError as exc:
        LOGGER.error("数据质量错误: %s", exc)
        sys.exit(1)
    except Exception as exc:
        LOGGER.exception("执行失败: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
