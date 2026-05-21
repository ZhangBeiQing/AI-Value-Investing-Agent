"""Typed data models for the selection system foundation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping

from utlity.stock_utils import normalize_symbol


def _as_clean_str(value: Any, *, field_name: str, required: bool) -> str:
    if value is None:
        value = ""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} 必须是字符串")
    cleaned = value.strip()
    if required and not cleaned:
        raise ValueError(f"{field_name} 不能为空")
    return cleaned


@dataclass(frozen=True)
class MasterUniverseStock:
    """Single stock entry inside the master universe."""

    symbol: str
    name: str
    sector: str = ""
    industry: str = ""
    stock_type: str = "growth"

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "MasterUniverseStock":
        raw_symbol = _as_clean_str(payload.get("symbol"), field_name="symbol", required=True)
        raw_name = _as_clean_str(payload.get("name"), field_name="name", required=True)
        sector = _as_clean_str(payload.get("sector"), field_name="sector", required=False)
        industry = _as_clean_str(payload.get("industry"), field_name="industry", required=False)
        stock_type = _as_clean_str(payload.get("stock_type", "growth"), field_name="stock_type", required=False) or "growth"
        return cls(
            symbol=normalize_symbol(raw_symbol),
            name=raw_name,
            sector=sector,
            industry=industry,
            stock_type=stock_type,
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "sector": self.sector,
            "industry": self.industry,
            "stock_type": self.stock_type,
        }


@dataclass(frozen=True)
class MasterUniverseDocument:
    """Top-level master universe document stored on disk."""

    stocks: List[MasterUniverseStock]
    schema_version: int = 1
    universe_name: str = "master_universe"
    description: str = "一期选股系统主股票宇宙"
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    def empty(cls) -> "MasterUniverseDocument":
        return cls(stocks=[])

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any] | List[Mapping[str, Any]]) -> "MasterUniverseDocument":
        if isinstance(payload, list):
            stocks_payload = payload
            schema_version = 1
            universe_name = "master_universe"
            description = "一期选股系统主股票宇宙"
            updated_at = datetime.now().isoformat()
        else:
            raw_schema_version = payload.get("schema_version", 1)
            if not isinstance(raw_schema_version, int) or raw_schema_version <= 0:
                raise ValueError("schema_version 必须是正整数")
            schema_version = raw_schema_version
            universe_name = _as_clean_str(payload.get("universe_name", "master_universe"), field_name="universe_name", required=True)
            description = _as_clean_str(payload.get("description", "一期选股系统主股票宇宙"), field_name="description", required=True)
            updated_at = _as_clean_str(payload.get("updated_at", datetime.now().isoformat()), field_name="updated_at", required=True)
            stocks_payload = payload.get("stocks", [])

        if not isinstance(stocks_payload, list):
            raise ValueError("stocks 必须是数组")

        stocks = [MasterUniverseStock.from_dict(item) for item in stocks_payload]
        _validate_unique_symbols(stocks)

        return cls(
            stocks=stocks,
            schema_version=schema_version,
            universe_name=universe_name,
            description=description,
            updated_at=updated_at,
        )

    def with_updated_timestamp(self) -> "MasterUniverseDocument":
        return MasterUniverseDocument(
            stocks=list(self.stocks),
            schema_version=self.schema_version,
            universe_name=self.universe_name,
            description=self.description,
            updated_at=datetime.now().isoformat(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "universe_name": self.universe_name,
            "description": self.description,
            "updated_at": self.updated_at,
            "stocks": [stock.to_dict() for stock in self.stocks],
        }

    def summary(self) -> Dict[str, Any]:
        missing_sector = sum(1 for stock in self.stocks if not stock.sector)
        missing_industry = sum(1 for stock in self.stocks if not stock.industry)
        return {
            "schema_version": self.schema_version,
            "universe_name": self.universe_name,
            "stock_count": len(self.stocks),
            "missing_sector_count": missing_sector,
            "missing_industry_count": missing_industry,
            "updated_at": self.updated_at,
        }


def _validate_unique_symbols(stocks: Iterable[MasterUniverseStock]) -> None:
    seen: set[str] = set()
    duplicates: list[str] = []
    for stock in stocks:
        if stock.symbol in seen and stock.symbol not in duplicates:
            duplicates.append(stock.symbol)
        seen.add(stock.symbol)
    if duplicates:
        duplicates_text = ", ".join(sorted(duplicates))
        raise ValueError(f"master_universe 存在重复 symbol: {duplicates_text}")
