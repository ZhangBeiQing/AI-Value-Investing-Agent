"""Central registry for cache layout and TTL policies.

This module formalizes the directory structure used by multiple tools to store
financial statements, price series, and derived analysis artifacts.  By
exposing a single registry we can gradually migrate legacy scripts without
duplicating hard-coded paths.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, Optional
import pandas as pd
import akshare as ak
import pickle
from utlity import *
import logging
import numpy as np
import requests
from urllib.parse import quote

# 导入ETF数据获取函数
from utlity.stock_utils import fetch_cn_etf_daily, fetch_cn_index_daily


HK_METADATA_COLUMNS: tuple[str, ...] = (
    "SECUCODE",
    "SECURITY_CODE",
    "SECURITY_NAME_ABBR",
    "ORG_CODE",
    "DATE_TYPE_CODE",
    "FISCAL_YEAR",
    "START_DATE",
    "STD_REPORT_DATE",
)

HK_PROFIT_ALIASES: Dict[str, str] = {
    "股东应占溢利": "HOLDER_PROFIT",
    "除税后溢利": "NETPROFIT",
    "除税前溢利": "TOTAL_PROFIT",
    "持续经营业务税后利润": "CONTINUED_NETPROFIT",
    "溢利其他项目": "NETPROFIT_OTHER",
    "营业额": "OPERATE_INCOME",
    "营运收入": "OPERATE_INCOME",
    "营运支出": "OPERATE_COST",
    "经营溢利": "OPERATE_PROFIT",
    "毛利": "GROSS_PROFIT",
    "其他收益": "OTHER_INCOME",
    "研发费用": "RESEARCH_EXPENSE",
    "销售及分销费用": "SALE_EXPENSE",
    "行政开支": "MANAGE_EXPENSE",
    "融资成本": "FINANCE_EXPENSE",
    "税项": "INCOME_TAX",
    "少数股东损益": "MINORITY_INTEREST",
    "利息收入": "INTEREST_INCOME",
    "减值及拨备": "ASSET_IMPAIRMENT_LOSS",
    "每股基本盈利": "BASIC_EPS",
    "每股摊薄盈利": "DILUTED_EPS",
    "非运算项目": "NON_OPERATING_ITEMS",
    "本公司拥有人应占全面收益总额": "PARENT_TCI",
}

HK_BALANCE_ALIASES: Dict[str, str] = {
    "总资产": "TOTAL_ASSETS",
    "非流动资产合计": "TOTAL_NONCURRENT_ASSETS",
    "流动资产合计": "TOTAL_CURRENT_ASSETS",
    "现金及等价物": "MONETARYFUNDS",
    "短期投资": "OTHER_CURRENT_ASSET",
    "短期贷款": "SHORT_LOAN",
    "长期贷款": "LONG_LOAN",
    "递延收入(流动)": "DEFER_INCOME_1YEAR",
    "递延收入(非流动)": "DEFER_INCOME",
    "非流动负债合计": "TOTAL_NONCURRENT_LIAB",
    "流动负债合计": "TOTAL_CURRENT_LIAB",
    "总负债": "TOTAL_LIABILITIES",
    "净流动资产": "NET_CURRENT_ASSETS",
    "少数股东权益": "MINORITY_EQUITY",
    "净资产": "TOTAL_PARENT_EQUITY",
    "股东权益": "TOTAL_EQUITY",
    "总权益": "TOTAL_EQUITY",
    "总权益及总负债": "TOTAL_LIAB_EQUITY",
    "股本": "SHARE_CAPITAL",
    "公积金": "CAPITAL_RESERVE",
    "保留溢利(累计亏损)": "UNASSIGN_RPOFIT",
    "应付帐款": "ACCOUNTS_PAYABLE",
    "预付款项": "PREPAYMENT",
    "预付款按金及其他应收款": "PREPAYMENT",
    "应付税项": "TAX_PAYABLE",
    "受限制存款及现金": "RESTRICTED_CASH",
    "总资产减流动负债": "TOTAL_ASSETS_LESS_CURRENT_LIAB",
}

HK_CASHFLOW_ALIASES: Dict[str, str] = {
    "经营业务现金净额": "NETCASH_OPERATE",
    "投资业务现金净额": "NETCASH_INVEST",
    "融资业务现金净额": "NETCASH_FINANCE",
    "现金净额": "CCE_ADD",
    "融资前现金净额": "NETCASH_OPERATE_BEFORE_FINANCE",
    "期初现金": "BEGIN_CASH",
    "期末现金": "END_CASH",
    "经营产生现金": "OPERATE_INFLOW_BALANCE",
    "投资业务其他项目": "INVEST_OUTFLOW_OTHER",
    "融资业务其他项目": "FINANCE_OUTFLOW_OTHER",
    "已付税项": "PAY_ALL_TAX",
    "处置固定资产": "DISPOSAL_LONG_ASSET",
    "购建固定资产": "CONSTRUCT_LONG_ASSET",
    "加:折旧及摊销": "FA_IR_DEPR",
    "加:经营调整其他项目": "OPERATE_NETCASH_OTHER",
    "营运资金变动前经营溢利": "OPERATE_PROFIT",
    "经营业务其他项目": "OPERATE_OUTFLOW_OTHER",
    "新增借款": "RECEIVE_LOAN_CASH",
    "偿还借款": "PAY_DEBT_CASH",
    "已付股息(融资)": "ASSIGN_DIVIDEND_PORFIT",
    "发行股份": "ACCEPT_INVEST_CASH",
    "发行债券": "ISSUE_BOND",
    "回购股份": "BUY_SUBSIDIARY_EQUITY",
    "收购附属公司": "OBTAIN_SUBSIDIARY_OTHER",
    "出售附属公司": "SUBSIDIARY_ACCEPT_INVEST",
}

CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_HK_STOCK_LIST_URL = "https://www.cninfo.com.cn/new/data/hke_stock.json"
CNINFO_JSON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.cninfo.com.cn",
    "Referer": "https://www.cninfo.com.cn/new/disclosure/list/notice",
}
HK_STOCK_MAP_CACHE: Dict[str, Dict[str, str]] = {}
HK_STOCK_MAP_LOADED_AT: Optional[datetime] = None
HK_STOCK_MAP_TTL = timedelta(hours=12)


def _normalize_hk_financial_report(
    df: pd.DataFrame,
    *,
    alias_map: Dict[str, str],
) -> pd.DataFrame:
    """Pivot港股报表为按报告期展开的列结构，并做字段别名映射。"""

    if df is None or df.empty:
        return pd.DataFrame()
    required_cols = {"REPORT_DATE", "STD_ITEM_NAME", "AMOUNT"}
    if not required_cols.issubset(df.columns):
        return df

    work = df.copy()
    work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
    work = work.dropna(subset=["REPORT_DATE"])
    if work.empty:
        return pd.DataFrame()

    work["AMOUNT"] = pd.to_numeric(work["AMOUNT"], errors="coerce")
    work = work.dropna(subset=["AMOUNT"])

    alias_series = work["STD_ITEM_NAME"].map(alias_map)
    work["__HK_FIELD__"] = alias_series.fillna(work["STD_ITEM_NAME"])

    pivot = (
        work.pivot_table(
            index="REPORT_DATE",
            columns="__HK_FIELD__",
            values="AMOUNT",
            aggfunc="sum",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    metadata_cols = [col for col in HK_METADATA_COLUMNS if col in work.columns]
    if metadata_cols:
        metadata = (
            work[["REPORT_DATE", *metadata_cols]]
            .sort_values("REPORT_DATE")
            .drop_duplicates(subset=["REPORT_DATE"], keep="last")
        )
        merged = metadata.merge(pivot, on="REPORT_DATE", how="left")
    else:
        merged = pivot

    merged = merged.sort_values("REPORT_DATE").reset_index(drop=True)
    return merged


def _normalize_hk_financial_abstract(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    if "REPORT_DATE" in work.columns:
        work["REPORT_DATE"] = pd.to_datetime(work["REPORT_DATE"], errors="coerce")
        work = work.dropna(subset=["REPORT_DATE"])
        work = work.sort_values("REPORT_DATE")
    return work.reset_index(drop=True)
META_FILENAME = ".cache_registry_meta.json"
UTC = datetime.now  # simple hook for testing
POLICY_PATH = Path(__file__).resolve().parents[1] / "configs" / "cache_policy.json"


class CacheKind(str, Enum):
    FINANCIALS = "financials_cache"
    PRICE_SERIES = "prices"
    ANALYSIS = "analysis"
    PE_ANALYSIS = "pe_pb_analysis"
    BASIC_INFO = "basic_info_cache"
    DISCLOSURES = "disclosures"
    SHARE_INFO = "share_info"


@dataclass(frozen=True)
class CacheSpec:
    kind: CacheKind
    subdir: str
    description: str
    ttl_days: Optional[int] = None
    required_files: tuple[str, ...] = ()
    optional_files: tuple[str, ...] = ()
    per_stock: bool = True


BASE_REGISTRY: Dict[CacheKind, CacheSpec] = {
    CacheKind.FINANCIALS: CacheSpec(
        kind=CacheKind.FINANCIALS,
        subdir="financials_cache",
        description="利润表/资产负债表/现金流表/财务摘要等 CSV/PKL 缓存",
        ttl_days=7,
        required_files=(
            "profit_sheet.csv",
            "balance_sheet.csv",
            "cash_flow_sheet.csv",
        ),
        optional_files=(
            "financial_abstract.csv",
        ),
    ),
    CacheKind.PRICE_SERIES: CacheSpec(
        kind=CacheKind.PRICE_SERIES,
        subdir="prices",
        description="以交易日命名的行情 CSV，含 _close/_volume/_turnover 列",
        ttl_days=1,
        required_files=(
            "price.csv",
        ),
    ),
    CacheKind.ANALYSIS: CacheSpec(
        kind=CacheKind.ANALYSIS,
        subdir="analysis",
        description="价格动态/Markdown 报告等派生结果",
        ttl_days=1,
    ),
    CacheKind.PE_ANALYSIS: CacheSpec(
        kind=CacheKind.PE_ANALYSIS,
        subdir="pe_pb_analysis",
        description="增强版 PE/PB 分析过程中生成的 JSON/Markdown/CSV",
        ttl_days=1,
    ),
    CacheKind.BASIC_INFO: CacheSpec(
        kind=CacheKind.BASIC_INFO,
        subdir="basic_info_cache",
        description="basic_info 快照（AlphaVantage 风格 JSON）",
        ttl_days=0,
        per_stock=False,
    ),
    CacheKind.DISCLOSURES: CacheSpec(
        kind=CacheKind.DISCLOSURES,
        subdir="disclosures",
        description="上市公司公告 PDF/Markdown/索引缓存",
        ttl_days=1,
        required_files=(
            "index.json",
            "cninfo_list.csv"
        ),
    ),
    CacheKind.SHARE_INFO: CacheSpec(
        kind=CacheKind.SHARE_INFO,
        subdir="share_info",
        description="股本和流通股本数据 CSV 缓存",
        ttl_days=7,
    ),
}


def _load_policy() -> dict:
    if not POLICY_PATH.exists():
        return {}
    try:
        return json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _apply_policy() -> Dict[CacheKind, CacheSpec]:
    policy = _load_policy()
    registry: Dict[CacheKind, CacheSpec] = {}
    for kind, spec in BASE_REGISTRY.items():
        override = policy.get(kind.value, {}) if policy else {}
        ttl_days = override.get("ttl_days", spec.ttl_days)
        subdir = override.get("subdir", spec.subdir)
        registry[kind] = CacheSpec(
            kind=kind,
            subdir=subdir,
            description=spec.description,
            ttl_days=ttl_days,
            required_files=spec.required_files,
            per_stock=spec.per_stock,
        )
    return registry


REGISTRY = _apply_policy()


def get_cache_spec(kind: CacheKind) -> CacheSpec:
    return REGISTRY[kind]


def build_cache_dir(
    symbolInfo: SymbolInfo,
    kind: CacheKind,
    *,
    base_dir: str | Path | None = None,
    ensure: bool = True,
) -> Path:
    base = resolve_base_dir(base_dir)
    spec = get_cache_spec(kind)
    if spec.per_stock:
        root = get_stock_data_dir(symbolInfo, base_dir=base)
        target = root / spec.subdir
    else:
        target = base / spec.subdir
    if ensure:
        target.mkdir(parents=True, exist_ok=True)
    return target

def prepare_directories(symbolInfo: SymbolInfo, base_data_dir: str | Path | None = None) -> Dict[str, Path]:
    """
    为指定股票准备所有相关目录并返回它们的路径
    
    参数:
        symbolInfo: 股票信息对象，包含股票代码和名称等信息
        base_data_dir: 基础数据目录路径，如果为None则使用默认路径
        
    返回:
        包含以下目录路径的字典:
        - root: 股票数据根目录
        - analysis: PE分析缓存目录
        - financial_cache: 财务数据缓存目录
        - market_cache: 市场数据缓存目录
    """

    root = get_stock_data_dir(symbolInfo, base_dir=base_data_dir)
    analysis_dir = build_cache_dir(
        symbolInfo,
        CacheKind.PE_ANALYSIS,
        base_dir=base_data_dir,
        ensure=True,
    )
    financial_cache_dir = build_cache_dir(
        symbolInfo,
        CacheKind.FINANCIALS,
        base_dir=base_data_dir,
        ensure=True,
    )
    market_cache_dir = ensure_stock_subdir(symbolInfo, subdir="market_cache", base_dir=base_data_dir)
    return {
        "root": root,
        "analysis": analysis_dir,
        "financial_cache": financial_cache_dir,
        "market_cache": market_cache_dir,
    }

@dataclass
class CacheCheckResult:
    path: Path
    missing_files: list[str]
    last_updated: Optional[datetime]
    stale: bool


def _meta_path(cache_dir: Path) -> Path:
    return cache_dir / META_FILENAME


def _load_meta(cache_dir: Path) -> Optional[Dict[str, Any]]:
    meta_file = _meta_path(cache_dir)
    if not meta_file.exists():
        return None
    try:
        return json.loads(meta_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        return None


def record_cache_refresh(cache_dir: Path, **kwargs: Any) -> None:
    meta = {"last_updated": UTC().isoformat()}
    meta.update(kwargs)
    cache_dir.mkdir(parents=True, exist_ok=True)
    _meta_path(cache_dir).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def check_cache(cache_dir: Path, kind: CacheKind) -> CacheCheckResult:
    """
    检查指定缓存目录的状态，包括文件完整性、最后更新时间、是否过期等
    
    参数:
        cache_dir: 缓存目录路径
        kind: 缓存类型枚举值（CacheKind）
        
    返回:
        CacheCheckResult: 包含缓存检查结果的数据类
    """
    # 获取对应缓存类型的配置规格
    spec = get_cache_spec(kind)

    if spec.ttl_days is None:
        raise ValueError(f"Cache kind {kind} has no TTL specified")
    
    # 检查必需文件是否缺失
    missing: list[str] = []
    for rel in spec.required_files:
        if not (cache_dir / rel).exists():
            missing.append(rel)

    # 尝试从元数据文件加载最后更新时间
    meta = _load_meta(cache_dir)
    last_updated = None
    if meta:
        ts = meta.get("last_updated")
        if ts:
            try:
                last_updated = datetime.fromisoformat(ts)
            except ValueError:
                pass

    stale = False
    # 如果没有元数据文件，检查目录中是否有实际文件
    if last_updated is None:
        stale = True
    else:
        stale = UTC() - last_updated > timedelta(days=spec.ttl_days)

    # 返回包含所有检查结果的数据对象
    return CacheCheckResult(
        path=cache_dir,           # 缓存目录路径
        missing_files=missing,     # 缺失的必要文件列表
        last_updated=last_updated, # 最后更新时间（可能为None）
        stale=stale,              # 是否过期的布尔值
    )


def iter_cache_dirs(
    stock_name: str,
    symbol: str,
    kinds: Iterable[CacheKind],
    *,
    base_dir: str | Path | None = None,
) -> Dict[CacheKind, CacheCheckResult]:
    results: Dict[CacheKind, CacheCheckResult] = {}
    for kind in kinds:
        cache_dir = build_cache_dir(stock_name, symbol, kind, base_dir=base_dir, ensure=False)
        results[kind] = check_cache(cache_dir, kind)
    return results


def should_refresh(cache_dir: Path, kind: CacheKind, force: bool = False) -> bool:
    if force:
        return True
    if not cache_dir.exists():
        return True
    status = check_cache(cache_dir, kind)
    return bool(status.missing_files) or status.stale

def update_financial_data_cached(
    symbolInfo: SymbolInfo,
    base_data_dir: str | Path = 'data',
    force_refresh: bool = False,
    force_refresh_financials: bool | None = None,
    logger: logging.Logger | None = None,
) -> Dict[str, pd.DataFrame]:
    """获取财务数据（带缓存功能），包括利润表、资产负债表、现金流量表和财务摘要"""
    
    # 如果没有提供logger，使用默认logger
    if logger is None:
        logger = logging.getLogger(__name__)

    # 判断是否为指数或ETF，跳过财务数据获取
    is_index = symbolInfo.market == "CN_INDEX"
    is_etf = symbolInfo.is_cn_market() and symbolInfo.code.startswith(('51', '58', '15', '16', '50', '53'))
    if is_index or is_etf:
        logger.info(f"{symbolInfo.stock_name} {symbolInfo.symbol} 为指数或ETF，跳过财务数据获取")
        return {}

    # 自动转换股票代码格式
    if symbolInfo.is_hk_market():
        formatted_code = symbolInfo.to_hk_symbol()
    elif symbolInfo.is_cn_market():
        formatted_code = symbolInfo.to_xueqiu_symbol()
    else:
        formatted_code = symbolInfo.symbol

    paths = prepare_directories(symbolInfo, base_data_dir=base_data_dir)
    cache_dir = paths["financial_cache"]

    cache_files = {
        "profit_sheet": cache_dir / "profit_sheet.csv",
        "balance_sheet": cache_dir / "balance_sheet.csv",
        "cash_flow_sheet": cache_dir / "cash_flow_sheet.csv",
        "financial_abstract": cache_dir / "financial_abstract.csv"
    }

    # 使用should_refresh方法判断整个财务缓存目录是否需要刷新
    need_refresh = should_refresh(cache_dir, CacheKind.FINANCIALS, force=force_refresh or force_refresh_financials)

    if not need_refresh:
        return
    
    refreshed = False
    for data_type, cache_path in cache_files.items():
        try:
            logger.info(f"{symbolInfo.stock_name} {data_type}需要重新获取财报数据")
            df = pd.DataFrame()
            if symbolInfo.is_hk_market():
                if data_type == "profit_sheet":
                    logger.info(f"正在获取港股{symbolInfo.stock_name}利润表数据...")
                    try:
                        df = api_call_with_delay(
                            ak.stock_financial_hk_report_em,
                            stock=formatted_code,
                            symbol="利润表",
                            indicator="报告期",
                            logger=logger,
                        )
                    except Exception as exc:  # pragma: no cover - network defensive
                        logger.error(f"获取港股{symbolInfo.stock_name}利润表失败: {exc}")
                elif data_type == "balance_sheet":
                    logger.info(f"正在获取港股{symbolInfo.stock_name}资产负债表数据...")
                    try:
                        df = api_call_with_delay(
                            ak.stock_financial_hk_report_em,
                            stock=formatted_code,
                            symbol="资产负债表",
                            indicator="报告期",
                            logger=logger,
                        )
                    except Exception as exc:
                        logger.error(f"获取港股{symbolInfo.stock_name}资产负债表失败: {exc}")
                        
                elif data_type == "cash_flow_sheet":
                    logger.info(f"正在获取港股{symbolInfo.stock_name}现金流量表数据...")
                    try:
                        df = api_call_with_delay(
                            ak.stock_financial_hk_report_em,
                            stock=formatted_code,
                            symbol="现金流量表",
                            indicator="报告期",
                            logger=logger,
                        )
                    except Exception as exc:
                        logger.error(f"获取港股{symbolInfo.stock_name}现金流量表失败: {exc}")
                elif data_type == "financial_abstract":
                    logger.info(f"正在获取港股{symbolInfo.stock_name}财务指标摘要数据...")
                    try:
                        df = api_call_with_delay(
                            ak.stock_financial_hk_analysis_indicator_em,
                            symbol=formatted_code,
                            indicator="报告期",
                            logger=logger,
                        )
                    except Exception as exc:
                        logger.error(f"获取港股{symbolInfo.stock_name}财务指标摘要失败: {exc}")
            elif symbolInfo.is_cn_market():
                if data_type == "profit_sheet":
                    logger.info(f"正在获取{symbolInfo.stock_name}利润表数据...")
                    df = api_call_with_delay(
                        ak.stock_profit_sheet_by_report_em,
                        symbol=formatted_code,
                        logger=logger,
                    )
                elif data_type == "balance_sheet":
                    logger.info(f"正在获取{symbolInfo.stock_name}资产负债表数据...")
                    df = api_call_with_delay(
                        ak.stock_balance_sheet_by_report_em,
                        symbol=formatted_code,
                        logger=logger,
                    )
                elif data_type == "cash_flow_sheet":
                    logger.info(f"正在获取{symbolInfo.stock_name}现金流量表数据...")
                    df = api_call_with_delay(
                        ak.stock_cash_flow_sheet_by_report_em,
                        symbol=formatted_code,
                        logger=logger,
                    )
                elif data_type == "financial_abstract":
                    logger.info(f"正在获取{symbolInfo.stock_name}财务摘要数据...")
                    df = api_call_with_delay(
                        ak.stock_financial_abstract,
                        symbol=formatted_code,
                        logger=logger,
                    )
            if symbolInfo.is_hk_market() and not df.empty:
                if data_type == "profit_sheet":
                    df = _normalize_hk_financial_report(df, alias_map=HK_PROFIT_ALIASES)
                elif data_type == "balance_sheet":
                    df = _normalize_hk_financial_report(df, alias_map=HK_BALANCE_ALIASES)
                elif data_type == "cash_flow_sheet":
                    df = _normalize_hk_financial_report(df, alias_map=HK_CASHFLOW_ALIASES)
                elif data_type == "financial_abstract":
                    df = _normalize_hk_financial_abstract(df)

            if not df.empty:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(cache_path, index=False)
                logger.info(f"已缓存{symbolInfo.stock_name} {data_type}数据到 {cache_path}")
                refreshed = True
            else:
                logger.warning(f"获取{symbolInfo.stock_name} {data_type}数据为空")

        except Exception as exc:
            logger.error(f"获取{symbolInfo.stock_name} {data_type}数据时出错: {exc}")

    # 如果有数据刷新，记录缓存刷新时间
    if refreshed:
        record_cache_refresh(cache_dir)

    return


def update_price_data_cached(
    symbolInfo: SymbolInfo,
    lookback_days: int, 
    force_refresh: bool = False,
    base_data_dir: str | Path = 'data',
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """获取价格数据（使用PRICE_SERIES缓存）"""

    # 构建价格缓存目录
    price_cache_dir = build_cache_dir(
        symbolInfo,
        CacheKind.PRICE_SERIES,
        base_dir=base_data_dir,
        ensure=True,
    )
    price_file = price_cache_dir / "price.csv"

    # 检查缓存是否需要刷新
    need_refresh = should_refresh(price_cache_dir, CacheKind.PRICE_SERIES, force_refresh)
    if not need_refresh and price_file.exists():
        try:
            existing = pd.read_csv(price_file, usecols=["日期"])
            existing["日期"] = pd.to_datetime(existing["日期"], errors="coerce")
            existing = existing.dropna(subset=["日期"])
            required_start = datetime.now() - timedelta(days=lookback_days - 7)
            
            # 检查是否有数据覆盖不足的情况
            if existing.empty or existing["日期"].min() > required_start:
                # 检查是否是因为历史请求记录表明我们已经尽力请求了更早的数据
                # 如果上次请求的 start_date 已经早于或等于我们需要的时间，说明数据源本身就没有更早的数据
                meta = _load_meta(price_cache_dir)
                bypass_refresh = False
                if meta and "requested_start_date" in meta:
                    try:
                        # requested_start_date 格式为 YYYYMMDD
                        prev_req_str = str(meta["requested_start_date"])
                        # 转换为 datetime
                        prev_req_dt = datetime.strptime(prev_req_str, "%Y%m%d")
                        if prev_req_dt <= required_start:
                            bypass_refresh = True
                            if logger:
                                logger.info(f"{symbolInfo.symbol} 数据覆盖不足但上次已请求至 {prev_req_str}，跳过刷新")
                    except Exception:
                        pass
                
                if not bypass_refresh:
                    need_refresh = True
        except Exception:
            need_refresh = True

    if need_refresh:
        try:
            logger.info(f"正在获取{symbolInfo.stock_name} {symbolInfo.symbol}历史股价数据...")
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")

            if symbolInfo.is_hk_market():
                df = fetch_hk_a_daily_with_fallback(
                    symbol_info=symbolInfo,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                    logger=logger,
                )
            elif symbolInfo.is_cn_market() or symbolInfo.market == "CN_INDEX":
                # 判断是否为指数
                if symbolInfo.market == "CN_INDEX":
                    # 使用指数专用函数获取数据
                    df = fetch_cn_index_daily(
                        symbol_info=symbolInfo,
                        logger=logger,
                    )
                else:
                    # 判断是否为ETF（A股ETF代码通常以51、58、15、16、50、53等开头）
                    is_etf = symbolInfo.code.startswith(('51', '58', '15', '16', '50', '53'))
                    
                    if is_etf:
                        # 使用ETF专用函数获取数据
                        df = fetch_cn_etf_daily(
                            symbol_info=symbolInfo,
                            start_date=start_date,
                            end_date=end_date,
                            logger=logger,
                        )
                    else:
                        # 使用普通A股函数获取数据
                        df = fetch_cn_a_daily_with_fallback(
                            symbol_info=symbolInfo,
                            start_date=start_date,
                            end_date=end_date,
                            adjust="qfq",
                            logger=logger,
                        )
            else:
                logger.warning(f"暂不支持{symbolInfo.market}市场的价格数据获取")
                return pd.DataFrame()

            if not df.empty:  
                # 对"换手率"列进行四舍五入，保留小数点后4位
                df["换手率"] = df["换手率"].round(4)
                
                # 按日期保存价格文件（YYYYMMDD.csv）
                price_file = price_cache_dir / f"price.csv"
                df.to_csv(price_file, index=False)
                record_cache_refresh(price_cache_dir, requested_start_date=start_date)
                logger.info(f"已缓存{symbolInfo.stock_name} {symbolInfo.symbol}价格数据到 {price_file}")
                return df
            else:
                logger.warning(f"获取{symbolInfo.stock_name} {symbolInfo.symbol}价格数据为空")
                return pd.DataFrame()

        except Exception as exc:
            logger.error(f"获取{symbolInfo.stock_name} {symbolInfo.symbol}价格数据失败: {exc}")
            return pd.DataFrame()
    return pd.DataFrame()


def update_share_info_cached(
    symbolInfo: SymbolInfo,
    force_refresh: bool = False,
    base_data_dir: str | Path = 'data',
    logger: logging.Logger | None = None,
) -> None:
    """获取股本数据（使用SHARE_INFO缓存）"""
    
    # 如果没有提供logger，使用默认logger
    if logger is None:
        logger = logging.getLogger(__name__)

    # 判断是否为index和ETF（A股ETF代码通常以51、58、15、16、50、53等开头）
    is_index = symbolInfo.market == "CN_INDEX"
    is_etf = symbolInfo.is_cn_market() and symbolInfo.code.startswith(('51', '58', '15', '16', '50', '53'))
    if is_etf or is_index:
        logger.info(f"{symbolInfo.stock_name} {symbolInfo.symbol} 为ETF或指数，跳过股本数据获取")
        return

    # 构建股本缓存目录
    share_cache_dir = build_cache_dir(
        symbolInfo,
        CacheKind.SHARE_INFO,
        base_dir=base_data_dir,
        ensure=True,
    )

    # 检查缓存是否需要刷新
    if should_refresh(share_cache_dir, CacheKind.SHARE_INFO, force_refresh):
        refreshed = False
        try:
            if symbolInfo.is_cn_market():
                logger.info(f"正在获取{symbolInfo.stock_name} {symbolInfo.symbol}股本变动数据...")
                # 获取完整的股本变动数据
                end_date = datetime.now().strftime("%Y%m%d")
                df = api_call_with_delay(
                    ak.stock_share_change_cninfo,
                    symbol=symbolInfo.code,
                    start_date="20000101",
                    end_date=end_date,
                    logger=logger,
                )
                
                if not df.empty:
                    # 保存完整数据到缓存文件
                    share_file = share_cache_dir / "stock_share_change_cninfo.csv"
                    df.to_csv(share_file, index=False)
                    logger.info(f"已缓存{symbolInfo.stock_name} {symbolInfo.symbol}股本变动数据到 {share_file}")
                    refreshed = True
                else:
                    logger.warning(f"获取{symbolInfo.stock_name} {symbolInfo.symbol}股本变动数据为空")
            
            elif symbolInfo.is_hk_market():
                logger.info(f"正在获取{symbolInfo.stock_name} {symbolInfo.symbol}港股财务指标数据...")
                # 获取完整的港股财务指标数据
                df = api_call_with_delay(
                    ak.stock_hk_financial_indicator_em,
                    symbol=symbolInfo.code,
                    logger=logger,
                )
                
                if not df.empty:
                    # 保存完整数据到缓存文件
                    share_file = share_cache_dir / "stock_hk_financial_indicator_em.csv"
                    df.to_csv(share_file, index=False)
                    logger.info(f"已缓存{symbolInfo.stock_name} {symbolInfo.symbol}港股财务指标数据到 {share_file}")
                    refreshed = True
                else:
                    logger.warning(f"获取{symbolInfo.stock_name} {symbolInfo.symbol}港股财务指标数据为空")
            
            else:
                logger.warning(f"暂不支持{symbolInfo.market}市场的股本数据获取")
        
        except Exception as exc:
            logger.error(f"获取{symbolInfo.stock_name} {symbolInfo.symbol}股本数据失败: {exc}")
        
        # 如果有数据刷新，记录缓存刷新时间
        if refreshed:
            record_cache_refresh(share_cache_dir)


def _load_cninfo_hk_stock_map(
    *,
    force_refresh: bool = False,
    logger: logging.Logger | None = None,
) -> Dict[str, Dict[str, str]]:
    """加载巨潮港股 orgId 映射表。"""
    global HK_STOCK_MAP_CACHE, HK_STOCK_MAP_LOADED_AT
    if (
        not force_refresh
        and HK_STOCK_MAP_CACHE
        and HK_STOCK_MAP_LOADED_AT
        and datetime.now() - HK_STOCK_MAP_LOADED_AT < HK_STOCK_MAP_TTL
    ):
        return HK_STOCK_MAP_CACHE

    try:
        resp = requests.get(
            CNINFO_HK_STOCK_LIST_URL,
            headers=CNINFO_JSON_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        mapping: Dict[str, Dict[str, str]] = {}
        for item in payload.get("stockList", []):
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            normalized = code.zfill(5)
            mapping[normalized] = {
                "code": normalized,
                "orgId": str(item.get("orgId") or "").strip(),
                "name": str(item.get("zwjc") or "").strip(),
            }
        if mapping:
            HK_STOCK_MAP_CACHE = mapping
            HK_STOCK_MAP_LOADED_AT = datetime.now()
            if logger:
                logger.info("已刷新巨潮港股 orgId 映射，共 %d 条", len(mapping))
    except Exception as exc:
        if logger:
            logger.warning("获取巨潮港股 orgId 映射失败: %s", exc)
    return HK_STOCK_MAP_CACHE


def _fetch_cninfo_hk_announcements(
    symbolInfo: SymbolInfo,
    *,
    lookback_days: int,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """调用巨潮接口抓取港股公告列表。"""
    mapping = _load_cninfo_hk_stock_map(logger=logger)
    entry = mapping.get(symbolInfo.code) or mapping.get(symbolInfo.code.zfill(5))
    if entry is None:
        if logger:
            logger.warning("未找到 %s 的巨潮 orgId，跳过公告抓取", symbolInfo.symbol)
        return pd.DataFrame()

    now = datetime.now()
    start = now - timedelta(days=lookback_days)
    se_date = f"{start:%Y-%m-%d}~{now:%Y-%m-%d}"

    rows: list[Dict[str, str]] = []
    page = 1
    while True:
        payload = {
            "pageNum": page,
            "pageSize": 30,
            "tabName": "fulltext",
            "column": "hke",
            "stock": f"{entry['code']},{entry['orgId']}",
            "searchkey": "",
            "secid": "",
            "plate": "hke",
            "category": "",
            "trade": "",
            "seDate": se_date,
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        try:
            resp = requests.post(
                CNINFO_QUERY_URL,
                data=payload,
                headers=CNINFO_JSON_HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            if logger:
                logger.error("获取港股公告失败: %s", exc)
            break

        announcements = data.get("announcements") or []
        if not announcements:
            break

        for ann in announcements:
            ann_id = str(ann.get("announcementId") or "").strip()
            title = str(ann.get("announcementTitle") or "").strip()
            dt_raw = ann.get("announcementTime")
            ts = None
            if isinstance(dt_raw, (int, float)):
                ts = datetime.fromtimestamp(dt_raw / 1000)
            elif isinstance(dt_raw, str):
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                    try:
                        ts = datetime.strptime(dt_raw[: len(fmt)], fmt)
                        break
                    except Exception:
                        continue
            if ts is None:
                ts = now
            dt_str = ts.strftime("%Y-%m-%d %H:%M")
            encoded_time = quote(dt_str, safe="")
            detail_url = (
                "https://www.cninfo.com.cn/new/disclosure/detail"
                f"?plate=hke&orgId={entry['orgId']}&stockCode={entry['code']}"
                f"&announcementId={ann_id}&announcementTime={encoded_time}"
            )
            rows.append(
                {
                    "代码": symbolInfo.symbol,
                    "stockCode": symbolInfo.symbol,
                    "公告标题": title,
                    "公告时间": dt_str,
                    "公告链接": detail_url,
                    "url": detail_url,
                    "announcementId": ann_id,
                    "orgId": entry["orgId"],
                    "adjunctUrl": ann.get("adjunctUrl"),
                    "adjunctType": ann.get("adjunctType"),
                    "stockName": ann.get("secName") or symbolInfo.stock_name,
                }
            )

        if not data.get("hasMore"):
            break
        page += 1
        if page > 200:
            break

    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    frame = frame.drop_duplicates(subset=["announcementId"]).reset_index(drop=True)
    return frame


def update_disclosures_cached(
    symbolInfo: SymbolInfo,
    *,
    lookback_days: int,
    base_data_dir: str | Path = 'data',
    logger: logging.Logger | None = None,
    force_refresh: bool = False,
) -> None:
    """获取公告列表并缓存csv（使用DISCLOSURES缓存）"""
    if logger is None:
        logger = logging.getLogger(__name__)

    cache_dir = build_cache_dir(
        symbolInfo,
        CacheKind.DISCLOSURES,
        base_dir=base_data_dir,
        ensure=True,
    )
    csv_path = cache_dir / "cninfo_list.csv"

    if not should_refresh(cache_dir, CacheKind.DISCLOSURES, force_refresh):
        logger.info(f"{symbolInfo.stock_name} 公告缓存仍在 TTL 内，跳过刷新")
        return

    now = datetime.now()
    start_date = (now - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_date = now.strftime("%Y%m%d")

    if symbolInfo.is_cn_market():
        try:
            logger.info(f"正在获取{symbolInfo.stock_name} {symbolInfo.symbol}公告列表...")
            fetched = api_call_with_delay(
                ak.stock_zh_a_disclosure_report_cninfo,
                symbol=symbolInfo.code,
                market="沪深京",
                start_date=start_date,
                end_date=end_date,
                logger=logger,
            )
        except Exception as exc:
            logger.error(f"获取{symbolInfo.stock_name}公告列表失败: {exc}")
            return
    elif symbolInfo.is_hk_market():
        logger.info(f"正在获取{symbolInfo.stock_name} {symbolInfo.symbol}港股公告列表...")
        fetched = _fetch_cninfo_hk_announcements(
            symbolInfo,
            lookback_days=lookback_days,
            logger=logger,
        )
    else:
        logger.info(f"{symbolInfo.stock_name} {symbolInfo.symbol} 暂不支持公告抓取")
        return

    if fetched is None or fetched.empty:
        logger.warning(f"{symbolInfo.stock_name} 公告列表为空")
        return

    frame = fetched.copy()
    dedup_candidates = [col for col in ("公告链接", "url") if col in frame.columns]
    if dedup_candidates:
        frame = frame.drop_duplicates(subset=dedup_candidates[0], keep="first")
    else:
        frame = frame.drop_duplicates()

    time_col = None
    for candidate in ("公告时间", "date", "datetime"):
        if candidate in frame.columns:
            time_col = candidate
            break
    if time_col:
        frame[time_col] = pd.to_datetime(frame[time_col], errors="coerce")
        frame = frame.dropna(subset=[time_col])
        frame = frame.sort_values(time_col, ascending=False)

    frame.to_csv(csv_path, index=False, encoding="utf-8")
    record_cache_refresh(cache_dir)
        
def ensure_symbol_data(
    base_data_dir: str | Path,
    symbolInfo: SymbolInfo,
    logger: logging.Logger | None ,
    lookback_price_days: int,
    force_refresh: bool = False,
    force_refresh_price: bool = False,
    force_refresh_financials: bool = False,
    skip_financial_refresh: bool = False,
    include_disclosures: bool = False,
    disclosure_lookback_days: int = 900,
    force_refresh_disclosures: bool = False,
) -> str:
    """
    确保指定股票代码的所有基础数据都已缓存并可用
    
    该函数是数据预加载的核心入口，负责协调多个数据源的获取和缓存：
    1. 财务数据（利润表、资产负债表、现金流量表、财务摘要信息）- 仅普通股票
    2. 价格历史数据 - 所有证券类型
    3. 股本历史数据 - 仅普通股票
    
    参数:
        symbol: 股票代码（如 "600276.SH", "300750.SZ"）
        stock_name: 股票名称（必填）
        price_lookback_days: 价格数据回溯天数，默认730天（约2年）
        force_refresh: 是否强制刷新所有缓存数据
        force_refresh_financials: 是否强制刷新财务数据（比force_refresh更细粒度）
        reference_date: 参考日期，用于确定数据的时间范围，默认使用当前日期
        
    返回:
        str: 解析后的股票名称，用于后续的目录构建和数据访问
    """

    # 判断是否为指数或ETF
    is_index = symbolInfo.market == "CN_INDEX"
    is_etf = symbolInfo.is_cn_market() and symbolInfo.code.startswith(('51', '58', '15', '16', '50', '53'))
    
    # 对于普通股票，获取所有数据
    if not is_index and not is_etf and not skip_financial_refresh:
        # 第一步：获取并缓存财务数据
        # 调用 EnhancedPEPBAnalyzer 获取完整的财务数据（利润表、资产负债表、现金流量表、财务摘要表）
        update_financial_data_cached(
            symbolInfo,
            base_data_dir,
            force_refresh=force_refresh,
            force_refresh_financials=force_refresh_financials,
            logger=logger,
        )

        # 第二步：获取并缓存股本数据
        update_share_info_cached(
            symbolInfo,
            base_data_dir=base_data_dir,
            force_refresh=force_refresh or force_refresh_financials,
            logger=logger,
        )
    else:
        logger.info(f"{symbolInfo.stock_name} {symbolInfo.symbol} 为指数或ETF，仅获取价格数据")

    # 第三步：获取并缓存价格数据 - 所有证券类型都需要
    update_price_data_cached(
        symbolInfo,
        lookback_days=lookback_price_days,
        force_refresh=force_refresh or force_refresh_price,
        base_data_dir=base_data_dir,
        logger=logger,
    )

    if include_disclosures and (symbolInfo.is_cn_market() or symbolInfo.is_hk_market()):
        update_disclosures_cached(
            symbolInfo,
            lookback_days=disclosure_lookback_days,
            base_data_dir=base_data_dir,
            logger=logger,
            force_refresh=force_refresh or force_refresh_disclosures,
        )



__all__ = [
    "CacheKind",
    "CacheSpec",
    "build_cache_dir",
    "check_cache",
    "iter_cache_dirs",
    "should_refresh",
    "record_cache_refresh",
    "update_financial_data_cached",
    "update_share_info_cached",
    "update_disclosures_cached",
]


# 缓存管理器说明
#
# 本项目的缓存管理可以通过根目录下的 cache_manager.py 工具进行统一管理：
#
# 主要功能：
# 1. 自动检查：每天 12:00 自动检查所有股票的缓存状态
# 2. 选择性更新：支持更新 price、financials 或全部数据
# 3. 智能清理：自动清理过期缓存文件
# 4. 强制更新：支持忽略现有缓存强制下载最新数据
# 5. 批量操作：支持对单个或多个股票进行操作
#
# 使用方法：
# - 检查所有缓存状态：python cache_manager.py check
# - 更新所有数据：python cache_manager.py update --type all
# - 仅更新价格数据：python cache_manager.py update --type price
# - 清理过期缓存：python cache_manager.py clean
# - 设置定时任务：python cache_manager.py setup-cron
#
# 详细设计文档请参考：docs/cache/cache_manager_design.md
