"""Shared helpers for financial data post-processing and share info retrieval."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type

import akshare as ak  # type: ignore
import pandas as pd  # type: ignore
from utlity import api_call_with_delay, SymbolInfo
from shared_data_access.cache_registry import build_cache_dir, CacheKind

LOGGER = logging.getLogger(__name__)


@dataclass
class ShareInfoResult:
    """Container for total shares and float shares with provenance metadata."""

    total: Optional[float]
    float_shares: Optional[float]
    source: str = "unknown"

    @property
    def is_valid(self) -> bool:
        return bool(self.total and self.total > 0 and self.float_shares and self.float_shares > 0)


class ShareInfoProvider:
    """Provide total/float share data with persistent caching and CNInfo fallback."""

    def __init__(
        self,
        cache_path: Path,
        cache_ttl: timedelta = timedelta(days=90),
        base_data_dir: Path | str = 'data',
    ) -> None:
        self.cache_path = cache_path
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_ttl = cache_ttl
        self.base_data_dir = Path(base_data_dir)
        self._cache = self._load_cache()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_share_info(
        self,
        symbolInfo: SymbolInfo,
        *,
        cutoff: Optional[datetime] = None,
    ) -> ShareInfoResult:
        """Return share info, fetching and caching when necessary."""

        if symbolInfo.is_cn_market():
            fetched = self._fetch_share_info_from_cninfo(symbolInfo, cutoff=cutoff)
            if fetched:
                result = fetched
        elif symbolInfo.is_hk_market():
            fetched = self._fetch_share_info_from_hk(symbolInfo)
            if fetched:
                result = fetched
        else:
            LOGGER.warning("%s %s 未支持的股票市场", symbolInfo.stock_name, symbolInfo.symbol)
            raise ValueError(f"{symbolInfo.stock_name} {symbolInfo.symbol} 未支持的股票市场")

        if not result or not result.is_valid:
            LOGGER.warning("%s %s 缺少有效股本数据", symbolInfo.stock_name, symbolInfo.symbol)
            raise ValueError(f"{symbolInfo.stock_name} {symbolInfo.symbol} 缺少有效股本数据")

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_cache(self) -> Dict[str, Any]:
        if not self.cache_path.exists():
            return {}
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("读取股本缓存失败: %s", exc)
        return {}

    def _save_cache(self) -> None:
        try:
            self.cache_path.write_text(
                json.dumps(self._cache, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("保存股本缓存失败: %s", exc)

    def _get_cached_share_info(self, stock_code: str) -> Optional[ShareInfoResult]:
        entry = self._cache.get(stock_code)
        if not isinstance(entry, dict):
            return None
        timestamp = entry.get("timestamp")
        if timestamp:
            try:
                cached_at = datetime.fromisoformat(timestamp)
            except ValueError:
                return None
            if datetime.now() - cached_at > self.cache_ttl:
                return None
        total = _normalize_share_value(entry.get("total"))
        float_shares = _normalize_share_value(entry.get("float"))
        if not total or not float_shares:
            return None
        return ShareInfoResult(total=total, float_shares=float_shares, source="cache")

    def _fetch_share_info_from_cninfo(self, symbolInfo:SymbolInfo, cutoff: Optional[datetime]) -> Optional[ShareInfoResult]:
        try:
            # 从缓存目录加载数据
            share_cache_dir = build_cache_dir(
                symbolInfo,
                CacheKind.SHARE_INFO,
                base_dir=self.base_data_dir,
                ensure=False,
            )
            share_file = share_cache_dir / "stock_share_change_cninfo.csv"
            
            if not share_file.exists():
                LOGGER.warning("股本缓存文件不存在: %s", share_file)
                return None
            
            # 读取缓存的完整数据
            df = pd.read_csv(share_file)
        except Exception as exc:  # pragma: no cover - file/data failure
            LOGGER.warning("读取股本缓存文件失败: %s", exc)
            return None

        if df is None or df.empty:
            return None

        working = df.copy()
        if "变动日期" in working.columns:
            working["变动日期"] = pd.to_datetime(working["变动日期"], errors="coerce")
            working = working.dropna(subset=["变动日期"])
            if cutoff is not None:
                working = working[working["变动日期"] <= cutoff]
        if working.empty:
            return None

        working = working.sort_values("变动日期")
        latest = working.iloc[-1]
        total = _cninfo_value_to_shares(latest.get("总股本"))
        float_shares = _cninfo_value_to_shares(
            latest.get("已流通股份") or latest.get("流通受限股份")
        )
        if total is None or total <= 0:
            return None
        if float_shares is None or float_shares <= 0:
            float_shares = total

        LOGGER.info("从缓存文件获取 %s %s 股本: 总股本 %.2f 亿股", symbolInfo.stock_name, symbolInfo.symbol, total / 1e8)
        return ShareInfoResult(total=total, float_shares=float_shares, source="cninfo_cache")

    def _fetch_share_info_from_hk(self, symbolInfo:SymbolInfo) -> Optional[ShareInfoResult]:
        try:
            # 从缓存目录加载数据
            share_cache_dir = build_cache_dir(
                symbolInfo,
                CacheKind.SHARE_INFO,
                base_dir=self.base_data_dir,
                ensure=False,
            )
            share_file = share_cache_dir / "stock_hk_financial_indicator_em.csv"
            
            if not share_file.exists():
                LOGGER.warning("港股股本缓存文件不存在: %s", share_file)
                return None
            
            # 读取缓存的完整数据
            df = pd.read_csv(share_file)
        except Exception as exc:  # pragma: no cover - file/data failure
            LOGGER.warning("读取港股股本缓存文件失败: %s", exc)
            return None
        
        if df is None or df.empty:
            return None
        
        latest = df.iloc[0]
        total = _normalize_share_value(latest.get("已发行股本(股)"))
        float_shares = _normalize_share_value(
            latest.get("已发行股本-H股(股)") or latest.get("已发行股本(股)")
        )
        if total is None or total <= 0:
            return None
        if float_shares is None or float_shares <= 0:
            float_shares = total
        LOGGER.info("从缓存文件获取港股 %s %s 股本: 总股本 %.2f 亿股", symbolInfo.stock_name, symbolInfo.symbol, total / 1e8)
        return ShareInfoResult(total=total, float_shares=float_shares, source="eastmoney_cache")


def _normalize_share_value(value: Optional[Any]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    try:
        text = str(value).strip()
    except Exception:
        return None
    if not text:
        return None

    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]

    try:
        numeric = float(text.replace(",", ""))
    except ValueError:
        return None
    numeric *= multiplier
    return numeric if numeric > 0 else None


def _cninfo_value_to_shares(value: Any) -> Optional[float]:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    numeric = float(numeric) * 10000  # 万股 -> 股
    return numeric if numeric > 0 else None


def _normalize_hk_symbol(stock_code: str) -> Optional[str]:
    if not stock_code:
        return None
    digits = "".join(ch for ch in stock_code if ch.isdigit())
    if not digits:
        return None
    if len(digits) > 5:
        digits = digits[-5:]
    return digits.zfill(5)


def filter_dataframe_by_date(
    df: pd.DataFrame,
    candidate_columns: Iterable[str],
    cutoff: Optional[datetime],
    *,
    keep_order: bool = False,
) -> pd.DataFrame:
    """Filter rows whose date column is greater than the cutoff."""

    if df is None or df.empty or cutoff is None:
        return df

    working = df.copy()
    target_col = next((col for col in candidate_columns if col in working.columns), None)
    if target_col is None:
        return working

    working[target_col] = pd.to_datetime(working[target_col], errors="coerce")
    working = working.dropna(subset=[target_col])
    if working.empty:
        return working

    filtered = working[working[target_col] <= cutoff]
    if not keep_order:
        filtered = filtered.sort_values(target_col, ascending=False)
    return filtered


def filter_financial_abstract_by_cutoff(
    financial_abstract: pd.DataFrame,
    cutoff: Optional[datetime],
    *,
    release_map: Optional[Dict[str, datetime]] = None,
) -> pd.DataFrame:
    if financial_abstract is None or financial_abstract.empty or cutoff is None:
        return financial_abstract

    result = financial_abstract.copy()
    cutoff_str = cutoff.strftime("%Y%m%d")
    date_cols = [col for col in result.columns if col.isdigit() and len(col) == 8]
    drop_cols: List[str] = []
    for col in date_cols:
        if col > cutoff_str:
            drop_cols.append(col)
            continue
        if release_map:
            notice_dt = release_map.get(col)
            if notice_dt and notice_dt > cutoff:
                drop_cols.append(col)
    if drop_cols:
        result = result.drop(columns=drop_cols)
    return result


def apply_dataframe_cutoff(
    frames: Dict[str, pd.DataFrame],
    cutoff: Optional[datetime],
    column_config: Dict[str, Iterable[str]],
    *,
    keep_order: bool = True,
) -> Dict[str, pd.DataFrame]:
    if cutoff is None:
        return frames

    filtered: Dict[str, pd.DataFrame] = {}
    for key, df in frames.items():
        cols = column_config.get(key)
        if cols:
            filtered[key] = filter_dataframe_by_date(df, cols, cutoff, keep_order=keep_order)
        else:
            filtered[key] = df
    return filtered


__all__ = [
    "ShareInfoProvider",
    "ShareInfoResult",
    "filter_dataframe_by_date",
    "filter_financial_abstract_by_cutoff",
    "apply_dataframe_cutoff",
]
