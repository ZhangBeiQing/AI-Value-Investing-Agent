"""单股数据准备入口的回归测试。"""

from __future__ import annotations

import pytest

from scripts.prepare_single_stock import _normalize_symbol, build_parser


def test_normalize_symbol_keeps_suffix_when_present() -> None:
    assert _normalize_symbol("600601.SH") == "600601.SH"
    assert _normalize_symbol("600601.sh") == "600601.SH"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("600601", "600601.SH"),
        ("688981", "688981.SH"),
        ("000333", "000333.SZ"),
        ("300750", "300750.SZ"),
        ("301308", "301308.SZ"),
    ],
)
def test_normalize_symbol_infers_exchange_suffix(raw: str, expected: str) -> None:
    assert _normalize_symbol(raw) == expected


def test_normalize_symbol_rejects_empty_and_garbage() -> None:
    with pytest.raises(ValueError):
        _normalize_symbol("")
    with pytest.raises(ValueError):
        _normalize_symbol("abc")


def test_parser_requires_symbol_and_defaults_to_all_stages() -> None:
    args = build_parser().parse_args(["--symbol", "600601.SH"])
    assert args.symbol == "600601.SH"
    assert args.only == "all"
    assert args.disclosure_cutoff == "next-day"
    assert args.skip_market_context is False


def test_parser_rejects_unknown_stage() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--symbol", "600601.SH", "--only", "bogus"])
