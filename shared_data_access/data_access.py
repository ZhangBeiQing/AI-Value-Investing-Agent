"""Centralized service for loading cached financial/price/share datasets."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, TYPE_CHECKING

import pandas as pd

from shared_financial_utils import (
    ShareInfoProvider,
    apply_dataframe_cutoff,
    filter_financial_abstract_by_cutoff,
)
from utlity import resolve_base_dir, SymbolInfo

from .cache_registry import CacheKind, build_cache_dir, check_cache, ensure_symbol_data
from .exceptions import CacheIntegrityError, DataUnavailableError
from .models import (
    DisclosureBundle,
    FinancialDataBundle,
    PreparedData,
    PriceDataBundle,
    ShareInfo,
)
from .paths import financial_cache_dir, global_cache_dir, price_cache_dir, stock_root
from .validation import normalize_date, normalize_optional_date, normalize_stock_name, normalize_symbol


LOOKBACK_PRICE_DAYS = 1800
DISCLOSURE_LOOKBACK_DAYS = 730


class SharedDataAccess:
    """Primary entrypoint for downstream tools to access canonical datasets."""

    REQUIRED_FINANCIAL_FILES = {
        "profit_sheet": "profit_sheet.csv",
        "balance_sheet": "balance_sheet.csv",
    }
    OPTIONAL_FINANCIAL_FILES = {
        "cash_flow_sheet": "cash_flow_sheet.csv",
        "financial_abstract": "financial_abstract.csv",
    }

    # Price data is handled separately from financial data
    PRICE_FILES = {
        "stock_price": "price.csv",
    }

    def __init__(
        self,
        *,
        logger: logging.Logger,
        base_dir: Optional[str | Path] = None,
        price_lookback_days: int = LOOKBACK_PRICE_DAYS,
        share_cache_ttl_days: int = 90,
        disclosure_lookback_days: int = DISCLOSURE_LOOKBACK_DAYS,
    ) -> None:
        """
        初始化SharedDataAccess实例
        
        Args:
            logger: 日志记录器实例
            base_dir: 数据根目录路径，默认为None，使用默认路径
            price_lookback_days: 价格数据回溯天数，默认为1800天
            share_cache_ttl_days: 股本信息缓存有效期天数，默认为90天
            disclosure_lookback_days: 公告数据默认回溯天数，默认为730天
        """
        self.base_dir = resolve_base_dir(base_dir)
        self.price_lookback_days = price_lookback_days
        global_cache = global_cache_dir(self.base_dir)
        global_cache.mkdir(parents=True, exist_ok=True)
        self.share_provider = ShareInfoProvider(
            cache_path=global_cache / "share_info_cache.json",
            cache_ttl=timedelta(days=share_cache_ttl_days),
            base_data_dir=self.base_dir,
        )
        self.logger = logger
        self.disclosure_lookback_days = disclosure_lookback_days

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def prepare_dataset(
        self,
        *,
        symbolInfo: SymbolInfo,
        as_of_date: str,
        force_refresh: bool = False,
        force_refresh_price: bool = False,
        force_refresh_financials: bool = False,
        skip_financial_refresh: bool = False,
        include_disclosures: bool = False,
        disclosure_lookback_days: Optional[int] = None,
        force_refresh_disclosures: bool = False,
        
    ) -> PreparedData:
        """
        准备指定股票的数据集，包括财务数据、价格数据和股本数据
        
        Args:
            symbolInfo: 股票信息对象，包含股票代码和名称等信息
            as_of_date: 截至日期，格式为YYYY-MM-DD
            force_refresh: 是否强制刷新所有缓存数据
            force_refresh_financials: 是否仅强制刷新财务数据
            include_disclosures: 是否加载公告列表数据
            disclosure_lookback_days: 公告回溯天数，默认使用初始化配置
            force_refresh_disclosures: 是否强制刷新公告缓存
        
        Returns:
            PreparedData: 包含财务数据、价格数据和股本数据的数据集对象
        
        Raises:
            CacheIntegrityError: 当缓存数据不完整或已过期时
            DataUnavailableError: 当所需数据无法获取时
        """
        as_of_dt = normalize_date(as_of_date)

        disclosure_window = disclosure_lookback_days or self.disclosure_lookback_days

        ensure_symbol_data(
            self.base_dir,
            symbolInfo,
            logger=self.logger,
            lookback_price_days=self.price_lookback_days,
            force_refresh=force_refresh,
            force_refresh_price=force_refresh_price,
            force_refresh_financials=force_refresh_financials,
            skip_financial_refresh=skip_financial_refresh,
            include_disclosures=include_disclosures,
            disclosure_lookback_days=disclosure_window,
            force_refresh_disclosures=force_refresh_disclosures,
        )

        # 判断是否为指数或ETF
        is_index = symbolInfo.market == "CN_INDEX"
        is_etf = symbolInfo.is_cn_market() and symbolInfo.code.startswith(('51', '58', '15', '16', '50', '53'))
        
        # 对于指数和ETF，只加载价格数据，跳过财务和股本数据
        if is_index or is_etf:
            self.logger.info(f"{symbolInfo.stock_name} {symbolInfo.symbol} 为指数或ETF，仅加载价格数据")
            
            # 只加载价格数据
            prices = self._load_price_bundle(
                symbolInfo,
                as_of_dt,
            )
            
            # 为财务数据提供空值
            financials = FinancialDataBundle(
                profit_sheet=pd.DataFrame(),
                balance_sheet=pd.DataFrame(),
                cash_flow_sheet=pd.DataFrame(),
                stock_price=pd.DataFrame(),
                financial_abstract=pd.DataFrame(),
            )
            
            # 为股本数据提供默认值
            share_info = ShareInfo(
                total_shares=0.0,
                float_shares=0.0,
                source="default",
            )
        else:
            # 对于普通股票，加载所有数据
            financials = self._load_financial_bundle(symbolInfo, as_of_dt)
            prices = self._load_price_bundle(symbolInfo, as_of_dt)
            share_info = self._load_share_info(symbolInfo, as_of_dt)

        disclosures = None
        if include_disclosures and (symbolInfo.is_cn_market() or symbolInfo.is_hk_market()):
            disclosures = self._load_disclosure_bundle(
                symbolInfo,
                as_of_dt,
                disclosure_window,
            )

        return PreparedData(
            symbolInfo=symbolInfo,
            as_of=as_of_dt,
            financials=financials,
            prices=prices,
            share_info=share_info,
            disclosures=disclosures,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_financial_bundle(
        self,
        symbolInfo: SymbolInfo,
        as_of_dt: datetime,
    ) -> FinancialDataBundle:
        """
        加载并返回指定股票的财务数据包
        
        Args:
            symbolInfo: 股票信息对象，包含股票代码和名称等信息
            as_of_dt: 截至日期，用于过滤财务数据
        
        Returns:
            FinancialDataBundle: 包含利润表、资产负债表、现金流量表等财务数据的对象
        
        Raises:
            CacheIntegrityError: 当财务数据缓存不完整、缺失或已过期时（仅普通股票）
        """
        # 判断是否为指数或ETF，直接返回空的财务数据
        is_index = symbolInfo.market == "CN_INDEX"
        is_etf = symbolInfo.is_cn_market() and symbolInfo.code.startswith(('51', '58', '15', '16', '50', '53'))
        if is_index or is_etf:
            self.logger.info(f"{symbolInfo.stock_name} {symbolInfo.symbol} 为指数或ETF，返回空财务数据")
            return FinancialDataBundle(
                profit_sheet=pd.DataFrame(),
                balance_sheet=pd.DataFrame(),
                cash_flow_sheet=pd.DataFrame(),
                stock_price=pd.DataFrame(),
                financial_abstract=pd.DataFrame(),
            )
        
        # 对于普通股票，继续加载财务数据
        cache_dir = financial_cache_dir(symbolInfo, base_dir=self.base_dir)
        status = check_cache(cache_dir, CacheKind.FINANCIALS)
        if not cache_dir.exists():
            raise CacheIntegrityError(f"缺少财报缓存目录: {cache_dir}")
        if status.missing_files:
            raise CacheIntegrityError(
                f"财报缓存缺失文件: {', '.join(status.missing_files)}"
            )
        if status.stale:
            raise CacheIntegrityError(
                f"财报缓存已过期 (last_updated={status.last_updated})"
            )

        frames: Dict[str, pd.DataFrame] = {}

        notice_candidates = ("NOTICE_DATE", "公告日期")
        is_hk_stock = symbolInfo.is_hk_market()

        for key, filename in self.REQUIRED_FINANCIAL_FILES.items():
            frame = self._read_csv(cache_dir / filename)
            if frame is None:
                raise CacheIntegrityError(f"缺少必要财报文件: {filename}")
            if not any(col in frame.columns for col in notice_candidates):
                if is_hk_stock and "REPORT_DATE" in frame.columns:
                    frame = frame.copy()
                    frame["NOTICE_DATE"] = frame["REPORT_DATE"]
                else:
                    raise CacheIntegrityError(
                        f"{symbolInfo.symbol} 的 {filename} 缺少 NOTICE_DATE/公告日期 列"
                    )
            frames[key] = frame

        for key, filename in self.OPTIONAL_FINANCIAL_FILES.items():
            frame = self._read_csv(cache_dir / filename)
            if frame is not None and not frame.empty:
                if key != "financial_abstract" and not any(
                    col in frame.columns for col in notice_candidates
                ):
                    if is_hk_stock and "REPORT_DATE" in frame.columns:
                        frame = frame.copy()
                        frame["NOTICE_DATE"] = frame["REPORT_DATE"]
                    else:
                        raise CacheIntegrityError(
                            f"{symbolInfo.symbol} 的 {filename} 缺少 NOTICE_DATE/公告日期 列"
                        )
                frames[key] = frame
            else:
                frames[key] = pd.DataFrame()

        notice_map = self._build_notice_map(frames.get("profit_sheet"))

        cutoff_config = {
            "profit_sheet": ["NOTICE_DATE", "公告日期"],
            "balance_sheet": ["NOTICE_DATE", "公告日期"],
            "cash_flow_sheet": ["NOTICE_DATE", "公告日期"],
            "stock_price": ["日期", "TRADE_DATE", "trade_date"],
        }

        filtered = apply_dataframe_cutoff(frames, cutoff=as_of_dt, column_config=cutoff_config)

        # 财务摘要单独处理（csv，可选）
        abstract_path = cache_dir / "financial_abstract.csv"
        if abstract_path.exists():
            try:
                abstract_df = self._read_csv(abstract_path)
                if abstract_df is not None:
                    filtered["financial_abstract"] = filter_financial_abstract_by_cutoff(
                        abstract_df,
                        as_of_dt,
                        release_map=notice_map,
                    )
                else:
                    filtered["financial_abstract"] = pd.DataFrame()
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warning("读取财务摘要失败 %s: %s", abstract_path, exc)
                filtered["financial_abstract"] = pd.DataFrame()
        else:
            filtered["financial_abstract"] = pd.DataFrame()

        return FinancialDataBundle(
            profit_sheet=filtered["profit_sheet"],
            balance_sheet=filtered["balance_sheet"],
            cash_flow_sheet=filtered.get("cash_flow_sheet", pd.DataFrame()),
            stock_price=pd.DataFrame(),  # Price data handled separately
            financial_abstract=filtered.get("financial_abstract", pd.DataFrame()),
        )

    @staticmethod
    def _build_notice_map(profit_sheet: Optional[pd.DataFrame]) -> Dict[str, datetime]:
        mapping: Dict[str, datetime] = {}
        if profit_sheet is None or profit_sheet.empty:
            return mapping
        if "REPORT_DATE" not in profit_sheet.columns:
            return mapping
        notice_col = next(
            (col for col in ("NOTICE_DATE", "公告日期") if col in profit_sheet.columns),
            None,
        )
        if notice_col is None:
            return mapping
        frame = profit_sheet.copy()
        frame["REPORT_DATE"] = pd.to_datetime(frame["REPORT_DATE"], errors="coerce")
        frame[notice_col] = pd.to_datetime(frame[notice_col], errors="coerce")
        frame = frame.dropna(subset=["REPORT_DATE", notice_col])
        if frame.empty:
            return mapping
        frame = frame.sort_values(notice_col)
        for _, row in frame.iterrows():
            report_dt = row["REPORT_DATE"]
            notice_dt = row[notice_col]
            if pd.isna(report_dt) or pd.isna(notice_dt):
                continue
            key = pd.Timestamp(report_dt).strftime("%Y%m%d")
            if key not in mapping or notice_dt < mapping[key]:
                mapping[key] = notice_dt
        return mapping

    def _load_price_bundle(
        self,
        symbolInfo: SymbolInfo,
        as_of_dt: datetime,
    ) -> PriceDataBundle:
        """
        加载并返回指定股票的价格数据包
        
        Args:
            symbolInfo: 股票信息对象
            as_of_dt: 截至日期，用于过滤价格数据
        
        Returns:
            PriceDataBundle: 包含价格数据的对象，包括起始日期、结束日期和数据来源
        
        Raises:
            CacheIntegrityError: 当价格数据缓存不完整、缺失或已过期时
            DataUnavailableError: 当指定区间内的价格数据为空时
        """
        prices_dir = price_cache_dir(symbolInfo, base_dir=self.base_dir)
        status = check_cache(prices_dir, CacheKind.PRICE_SERIES)
        if not prices_dir.exists():
            raise CacheIntegrityError(f"缺少价格缓存目录: {prices_dir}")
        if status.stale:
            raise CacheIntegrityError(
                f"价格缓存已过期 (last_updated={status.last_updated})"
            )

        csv_files = sorted(prices_dir.glob("*.csv"))
        if not csv_files:
            raise CacheIntegrityError(f"价格缓存目录为空: {prices_dir}")

        latest_file = csv_files[-1]
        frame = pd.read_csv(latest_file, index_col=0, parse_dates=True)
        frame.index = pd.to_datetime(frame.index, errors="coerce")
        frame = frame[~frame.index.isna()]
        frame = frame.sort_index()
        start_dt = datetime.now() - timedelta(days=LOOKBACK_PRICE_DAYS)
        mask = (frame.index >= start_dt) & (frame.index <= as_of_dt)
        sliced = frame.loc[mask]
        if sliced.empty:
            raise DataUnavailableError(
                f"价格数据在区间 {start_dt.date()} 至 {as_of_dt.date()} 内为空"
            )
        return PriceDataBundle(
            frame=sliced,
            start=start_dt,
            end=as_of_dt,
            source_path=latest_file,
        )

    def _load_share_info(
        self,
        symbolInfo: SymbolInfo,
        as_of_dt: datetime,
    ) -> ShareInfo:
        """
        加载并返回指定股票的股本信息
        
        Args:
            symbolInfo: 股票信息对象，包含股票代码和名称等信息
            as_of_dt: 截至日期，用于获取该日期之前的最新股本数据
            force_refresh: 是否强制刷新股本数据
        
        Returns:
            ShareInfo: 包含总股本、流通股本和数据来源的股本信息对象
        
        Raises:
            DataQualityError: 当无法获取有效股本数据时（仅普通股票）
        """
        # 判断是否为指数或ETF，直接返回默认的股本数据
        is_index = symbolInfo.market == "CN_INDEX"
        is_etf = symbolInfo.is_cn_market() and symbolInfo.code.startswith(('51', '58', '15', '16', '50', '53'))
        if is_index or is_etf:
            self.logger.info(f"{symbolInfo.stock_name} {symbolInfo.symbol} 为指数或ETF，返回默认股本数据")
            return ShareInfo(
                total_shares=0.0,
                float_shares=0.0,
                source="default",
            )
        
        # 对于普通股票，继续加载股本数据
        result = self.share_provider.get_share_info(
            symbolInfo,
            cutoff=as_of_dt
        )
        return ShareInfo(
            total_shares=float(result.total),
            float_shares=float(result.float_shares),
            source=result.source,
        )

    def _load_disclosure_bundle(
        self,
        symbolInfo: SymbolInfo,
        as_of_dt: datetime,
        lookback_days: int,
    ) -> DisclosureBundle:
        cache_dir = build_cache_dir(
            symbolInfo,
            CacheKind.DISCLOSURES,
            base_dir=self.base_dir,
            ensure=True,
        )
        csv_path = cache_dir / "cninfo_list.csv"
        frame = self._read_csv(csv_path) if csv_path.exists() else pd.DataFrame()
        if frame is None:
            frame = pd.DataFrame()
        start_dt = as_of_dt - timedelta(days=lookback_days)
        if not frame.empty:
            normalized = frame.copy()
            time_col = None
            for candidate in ("公告时间", "date", "datetime"):
                if candidate in normalized.columns:
                    time_col = candidate
                    break
            if time_col:
                normalized[time_col] = pd.to_datetime(normalized[time_col], errors="coerce")
                normalized = normalized.dropna(subset=[time_col])
                mask = (normalized[time_col] >= start_dt) & (normalized[time_col] <= as_of_dt)
                normalized = normalized.loc[mask]
                normalized = normalized.sort_values(time_col, ascending=False)
            frame = normalized.reset_index(drop=True)
        return DisclosureBundle(
            frame=frame,
            start=start_dt,
            end=as_of_dt,
            source_path=csv_path if csv_path.exists() else None,
        )

    @staticmethod
    def _read_csv(path: Path) -> Optional[pd.DataFrame]:
        """
        安全地读取CSV文件，处理文件不存在和读取错误的情况
        
        Args:
            path: 要读取的CSV文件路径
        
        Returns:
            Optional[pd.DataFrame]: 读取成功返回DataFrame，失败返回None
        """
        if not path.exists():
            return None
        try:
            return pd.read_csv(path)
        except Exception as exc:  # pragma: no cover - defensive
            print("读取CSV失败 %s: %s", path, exc)
            return None


__all__ = ["SharedDataAccess"]
