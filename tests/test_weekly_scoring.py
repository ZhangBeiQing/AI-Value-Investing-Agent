from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.ranking.scoring_contract import load_score_file, validate_weekly_score_entry
from services.ranking.weekly_scoring import (
    REFERENCE_WEIGHTS,
    collect_weekly_scores,
    generate_scoring_tasks,
    merge_weekly_scores,
    score_filename,
)


RUN_DATE = "2026-10-13"
SYMBOLS = ["300476.SZ", "600276.SH", "00700.HK"]


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _inputs(root: Path) -> Path:
    run_dir = root / "skill_runs" / RUN_DATE
    book = run_dir / "fixed_tracked"
    book.mkdir(parents=True)
    (book / "01_global_context.md").write_text("完整宏观上下文", encoding="utf-8")
    _write(book / "02_basic_snapshot_payload.json", {
        "timestamp": RUN_DATE,
        "stocks": {symbol: {"stock_name": f"公司{index}"} for index, symbol in enumerate(SYMBOLS)},
    })
    _write(run_dir / "run_manifest.json", {
        "run_date": RUN_DATE,
        "books": [{"book_type": "fixed_tracked", "symbols": SYMBOLS}],
    })
    research = book / "04_stock_research"
    research.mkdir()
    for symbol in SYMBOLS:
        (research / f"公司_{symbol}_{RUN_DATE}_research.md").write_text("§1 估值\n§2 新闻\n§3 财报\n§4 记忆", encoding="utf-8")
    return book


def _entry(symbol: str = SYMBOLS[0], score: int = 60) -> dict:
    scores = {}
    for key in REFERENCE_WEIGHTS:
        scores[f"{key}_score"] = score
        scores[f"{key}_reason"] = "完整研究包与公告支持该判断。"
    return {"symbol": symbol, "stock_name": "公司", "scores": scores, "composite_reason": "总体判断与最大风险。"}


def test_generate_uses_day_pool_and_isolated_output(tmp_path: Path) -> None:
    inputs = tmp_path / "inputs"
    book = _inputs(inputs)
    outputs = tmp_path / "outputs"

    result = generate_scoring_tasks(RUN_DATE, base_dir=outputs, input_base_dir=inputs)

    assert [task["symbol"] for task in result["tasks"]] == SYMBOLS
    assert Path(result["output_dir"]) == outputs / "weekly_scoring" / RUN_DATE
    assert (outputs / "weekly_scoring" / RUN_DATE / "scoring_manifest.json").exists()
    assert not (inputs / "weekly_scoring").exists()
    for task in result["tasks"]:
        assert Path(task["output_path"]).name == score_filename(task["stock_name"], task["symbol"])
        assert str(book / "01_global_context.md") in task["prompt"]
        assert task["research_path"] in task["prompt"]
        assert task["output_path"] in task["prompt"]
        assert "weekly_score.schema.json" in task["prompt"]
        assert not Path(task["output_path"]).exists()


@pytest.mark.parametrize("missing", ["01_global_context.md", "02_basic_snapshot_payload.json"])
def test_generate_missing_prerequisite_fails_without_refresh(tmp_path: Path, missing: str) -> None:
    book = _inputs(tmp_path)
    (book / missing).unlink()

    with pytest.raises(FileNotFoundError):
        generate_scoring_tasks(RUN_DATE, base_dir=tmp_path)

    assert not (tmp_path / "weekly_scoring").exists()


def test_generate_stale_snapshot_rejected(tmp_path: Path) -> None:
    book = _inputs(tmp_path)
    snapshot = json.loads((book / "02_basic_snapshot_payload.json").read_text())
    snapshot["timestamp"] = "2026-10-12"
    _write(book / "02_basic_snapshot_payload.json", snapshot)

    with pytest.raises(ValueError, match="快照日期"):
        generate_scoring_tasks(RUN_DATE, base_dir=tmp_path)


def test_generate_skips_missing_and_ambiguous_research_without_previous_day_fallback(tmp_path: Path) -> None:
    book = _inputs(tmp_path)
    research = book / "04_stock_research"
    (research / f"公司_{SYMBOLS[0]}_{RUN_DATE}_research.md").rename(research / f"公司_{SYMBOLS[0]}_2026-10-12_research.md")
    (research / f"重复_{SYMBOLS[1]}_{RUN_DATE}_research.md").write_text("重复研究包")

    result = generate_scoring_tasks(RUN_DATE, base_dir=tmp_path)

    assert [task["symbol"] for task in result["tasks"]] == [SYMBOLS[2]]
    assert {item["symbol"] for item in result["skipped"]} == set(SYMBOLS[:2])


def test_generate_subset_deduplicates_and_rejects_outside_pool(tmp_path: Path) -> None:
    _inputs(tmp_path)
    result = generate_scoring_tasks(RUN_DATE, base_dir=tmp_path, symbols=[SYMBOLS[2], SYMBOLS[2]])
    assert len(result["tasks"]) == 1
    with pytest.raises(ValueError, match="不属于"):
        generate_scoring_tasks(RUN_DATE, base_dir=tmp_path, symbols=["000001.SZ"])


@pytest.mark.parametrize("value", [True, 60.0, "60", -1, 101, None])
def test_score_invalid_integer_or_range_rejected(value: object) -> None:
    entry = _entry()
    entry["scores"]["valuation_score"] = value
    assert any("valuation_score" in error for error in validate_weekly_score_entry(entry))


@pytest.mark.parametrize("change", ["missing", "extra", "nested_reason", "composite_score", "blank", "identity", "link"])
def test_score_wrong_contract_rejected(change: str) -> None:
    entry = _entry()
    if change == "missing":
        entry["scores"].pop("valuation_reason")
    elif change == "extra":
        entry["scores"]["other"] = 10
    elif change == "nested_reason":
        entry["scores"]["composite_reason"] = entry.pop("composite_reason")
    elif change == "composite_score":
        entry["composite_score"] = 100
    elif change == "blank":
        entry["scores"]["fundamental_reason"] = "  "
    elif change == "identity":
        entry["symbol"] = SYMBOLS[1]
    else:
        entry["scores"]["valuation_reason"] = "[来源](https://example.com)"
    assert validate_weekly_score_entry(entry, expected_symbol=SYMBOLS[0])


@pytest.mark.parametrize("score", [0, 100])
def test_score_valid_endpoints_accepted(score: int) -> None:
    assert validate_weekly_score_entry(_entry(score=score)) == []


def test_score_duplicate_json_keys_rejected(tmp_path: Path) -> None:
    path = tmp_path / "score.json"
    path.write_text('{"symbol":"300476.SZ","symbol":"600276.SH"}')
    _, errors = load_score_file(path)
    assert any("重复字段" in error for error in errors)


def test_merge_partial_scores_excludes_bad_missing_and_foreign_files(tmp_path: Path) -> None:
    _inputs(tmp_path)
    generate_scoring_tasks(RUN_DATE, base_dir=tmp_path)
    output = tmp_path / "weekly_scoring" / RUN_DATE
    valid = _entry()
    for key, score in zip(REFERENCE_WEIGHTS, [45, 68, 18, 78, 72]):
        valid["scores"][f"{key}_score"] = score
    _write(output / f"{SYMBOLS[0]}_score.json", valid)
    bad = _entry(SYMBOLS[1]); bad["composite_score"] = 100
    _write(output / f"{SYMBOLS[1]}_score.json", bad)
    _write(output / "000001.SZ_score.json", _entry("000001.SZ", 100))

    result = merge_weekly_scores(RUN_DATE, base_dir=tmp_path)

    assert result["status"] == "partial"
    assert len(result["items"]) == 1
    assert result["items"][0]["composite_score"] == 56.2
    assert result["items"][0]["rank"] == 1
    assert result["invalid"][0]["symbol"] == SYMBOLS[1]
    assert result["missing_symbols"] == [SYMBOLS[2]]
    assert result["ignored_files"] == ["000001.SZ_score.json"]
    assert "composite_score" not in json.loads((output / f"{SYMBOLS[0]}_score.json").read_text())
    assert (output / "weekly_ranking.md").exists()
    assert "56.2" in (output / "weekly_ranking.md").read_text()


def test_merge_ties_deterministic_and_empty_run_replaces_stale_ranking(tmp_path: Path) -> None:
    _inputs(tmp_path)
    generate_scoring_tasks(RUN_DATE, base_dir=tmp_path)
    output = tmp_path / "weekly_scoring" / RUN_DATE
    for symbol in SYMBOLS:
        _write(output / f"{symbol}_score.json", _entry(symbol))
    first = merge_weekly_scores(RUN_DATE, base_dir=tmp_path)
    assert [item["symbol"] for item in first["items"]] == sorted(SYMBOLS)
    assert first["status"] == "complete"
    saved = (output / "weekly_scoring.json").read_bytes()
    collected = collect_weekly_scores(RUN_DATE, base_dir=tmp_path)
    assert len(collected["items"]) == 3
    assert (output / "weekly_scoring.json").read_bytes() == saved
    for symbol in SYMBOLS:
        (output / f"{symbol}_score.json").unlink()
    empty = merge_weekly_scores(RUN_DATE, base_dir=tmp_path)
    assert empty["status"] == "empty"
    assert json.loads((output / "weekly_scoring.json").read_text())["items"] == []


def test_merge_wrong_date_manifest_rejected(tmp_path: Path) -> None:
    _inputs(tmp_path)
    generate_scoring_tasks(RUN_DATE, base_dir=tmp_path)
    path = tmp_path / "weekly_scoring" / RUN_DATE / "scoring_manifest.json"
    manifest = json.loads(path.read_text()); manifest["run_date"] = "2026-10-12"
    _write(path, manifest)
    with pytest.raises(ValueError, match="日期"):
        collect_weekly_scores(RUN_DATE, base_dir=tmp_path)


def test_collect_named_score_and_legacy_manifest_compatible(tmp_path: Path) -> None:
    _inputs(tmp_path)
    generated = generate_scoring_tasks(RUN_DATE, base_dir=tmp_path, symbols=[SYMBOLS[0]])
    output = Path(generated["output_dir"])
    _write(output / score_filename("中兴_通讯", SYMBOLS[0]), _entry())
    result = merge_weekly_scores(RUN_DATE, base_dir=tmp_path)
    assert len(result["items"]) == 1
    assert result["ignored_files"] == []


def test_collect_named_and_legacy_duplicates_rejected(tmp_path: Path) -> None:
    _inputs(tmp_path)
    generated = generate_scoring_tasks(RUN_DATE, base_dir=tmp_path, symbols=[SYMBOLS[0]])
    output = Path(generated["output_dir"])
    _write(output / score_filename("公司", SYMBOLS[0]), _entry())
    _write(output / f"{SYMBOLS[0]}_score.json", _entry())
    result = collect_weekly_scores(RUN_DATE, base_dir=tmp_path)
    assert result["items"] == []
    assert "多个评分文件" in result["invalid"][0]["errors"][0]


def test_score_filename_reserved_characters_stay_in_output_directory() -> None:
    filename = score_filename('中兴/通讯:*?', "000063.SZ")
    assert filename == "中兴_通讯____000063.SZ_score.json"
    assert Path(filename).name == filename


UNIVERSE_SYMBOL = "300502.SZ"


def _universe_inputs(root: Path) -> Path:
    run_dir = root / "skill_runs" / RUN_DATE
    (run_dir / "fixed_tracked").mkdir(parents=True)
    (run_dir / "fixed_tracked" / "01_global_context.md").write_text("完整宏观上下文", encoding="utf-8")
    book = run_dir / "universe"
    (book / "04_stock_research").mkdir(parents=True)
    _write(book / "02_basic_snapshot_payload.json", {
        "timestamp": RUN_DATE,
        "stocks": {UNIVERSE_SYMBOL: {"stock_name": "新易盛"}},
    })
    (book / "04_stock_research" / f"新易盛_{UNIVERSE_SYMBOL}_{RUN_DATE}_research.md").write_text(
        "§1 估值\n§2 新闻\n§3 财报\n§4 记忆", encoding="utf-8"
    )
    _write(root / "universe" / "master_universe.json", {
        "stocks": [{"symbol": UNIVERSE_SYMBOL, "name": "新易盛"}, {"symbol": "000063.SZ", "name": "中兴通讯"}],
    })
    return book


def test_universe_pool_generates_isolated_tasks_and_merges(tmp_path: Path) -> None:
    book = _universe_inputs(tmp_path)
    result = generate_scoring_tasks(
        RUN_DATE, base_dir=tmp_path, symbols=[UNIVERSE_SYMBOL], pool_source="universe"
    )

    assert result["pool_source"] == "universe"
    assert [task["symbol"] for task in result["tasks"]] == [UNIVERSE_SYMBOL]
    output = tmp_path / "weekly_scoring" / RUN_DATE / "universe"
    assert Path(result["output_dir"]) == output
    fallback_context = tmp_path / "skill_runs" / RUN_DATE / "fixed_tracked" / "01_global_context.md"
    assert str(fallback_context) in result["tasks"][0]["prompt"]

    # 缺失研究包的 universe 股票既不阻断也不落到 fixed_tracked 目录。
    full = generate_scoring_tasks(RUN_DATE, base_dir=tmp_path, pool_source="universe")
    assert len(full["tasks"]) == 1
    assert {item["symbol"] for item in full["skipped"]} == {"000063.SZ"}

    # 池外代码被拒绝。
    with pytest.raises(ValueError, match="不属于"):
        generate_scoring_tasks(RUN_DATE, base_dir=tmp_path, symbols=["600276.SH"], pool_source="universe")

    _write(output / f"{UNIVERSE_SYMBOL}_score.json", _entry(UNIVERSE_SYMBOL))
    merged = merge_weekly_scores(RUN_DATE, base_dir=tmp_path, pool_source="universe")
    assert merged["status"] == "partial"
    assert [item["symbol"] for item in merged["items"]] == [UNIVERSE_SYMBOL]
    assert "universe" in (output / "weekly_ranking.md").read_text()
    assert not (tmp_path / "weekly_scoring" / RUN_DATE / "weekly_scoring.json").exists()
