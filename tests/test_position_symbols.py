"""持仓 → symbol 解析的回归测试。

覆盖 2026-10-29 的真实事故：持仓里不在 TRACKED_A_STOCKS 的股票（德业 605117.SH、
工业富联 601138.SH、华泰 601688.SH）原先不会进入财报扫描范围，导致三季报整批漏抓。
"""

from __future__ import annotations

import json

from scripts.prepare_financial_report_skill import (
    _apply_explicit_symbol_scope,
    build_parser,
)
from services.research.position_symbols import (
    build_name_symbol_map,
    load_position_symbols,
    resolve_position_name,
)


def _write_override(base, signature: str, positions: dict) -> None:
    path = base / "agent_data" / signature / "position" / "manual_position_override.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"as_of_date": "2026-10-29", "cash": 1000.0, "positions": positions}),
        encoding="utf-8",
    )


def _prepare_base(tmp_path):
    base = tmp_path / "data"
    universe = base / "universe" / "master_universe.json"
    universe.parent.mkdir(parents=True, exist_ok=True)
    universe.write_text(
        json.dumps(
            {
                "stocks": [
                    {"name": "德业股份", "symbol": "605117.SH"},
                    {"name": "工业富联", "symbol": "601138.SH"},
                    {"name": "黄金基金ETF", "symbol": "518800.SH"},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return base


def test_load_position_symbols_resolves_names_via_universe(tmp_path) -> None:
    base = _prepare_base(tmp_path)
    _write_override(
        base,
        "book-fixed_tracked",
        {"德业股份": {"shares": 300}, "工业富联": {"shares": 100}, "CASH": {"amount": 1}},
    )

    symbols = load_position_symbols(base_dir=base)

    assert symbols == ["605117.SH", "601138.SH"]


def test_load_position_symbols_falls_back_to_stock_info_directory(tmp_path) -> None:
    base = _prepare_base(tmp_path)
    (base / "stock_info" / "华泰证券_601688.SH").mkdir(parents=True)
    _write_override(base, "book-fixed_tracked", {"华泰证券": {"shares": 1200}})

    symbols = load_position_symbols(base_dir=base)

    assert symbols == ["601688.SH"]


def test_load_position_symbols_keeps_order_and_dedupes_across_books(tmp_path) -> None:
    base = _prepare_base(tmp_path)
    _write_override(base, "book-a", {"德业股份": {"shares": 1}})
    _write_override(base, "book-b", {"德业股份": {"shares": 2}, "工业富联": {"shares": 3}})

    symbols = load_position_symbols(base_dir=base, signatures=("book-a", "book-b"))

    assert symbols == ["605117.SH", "601138.SH"]


def test_load_position_symbols_returns_empty_when_book_missing(tmp_path) -> None:
    base = _prepare_base(tmp_path)

    assert load_position_symbols(base_dir=base, signatures=("book-absent",)) == []


def test_build_name_symbol_map_prefers_universe_over_directory(tmp_path) -> None:
    base = _prepare_base(tmp_path)
    (base / "stock_info" / "德业股份_000001.SZ").mkdir(parents=True)

    mapping = build_name_symbol_map(
        universe_path=base / "universe" / "master_universe.json",
        stock_info_dir=base / "stock_info",
    )

    assert mapping["德业股份"] == "605117.SH"


def test_resolve_position_name_passes_through_symbol_and_unknown_name() -> None:
    mapping = {"德业股份": "605117.SH"}

    assert resolve_position_name("605117.sh", mapping) == "605117.SH"
    assert resolve_position_name("不存在的公司", mapping) == "不存在的公司"


def test_parser_enables_position_scope_by_default() -> None:
    args = build_parser().parse_args([])

    assert args.include_positions is True
    assert args.position_books == "book-fixed_tracked"


def test_explicit_symbols_disable_position_scope() -> None:
    args = build_parser().parse_args(["--symbols", "600601.SH", "--include-positions"])

    symbols = _apply_explicit_symbol_scope(args)

    assert symbols == ["600601.SH"]
    assert args.include_positions is False
