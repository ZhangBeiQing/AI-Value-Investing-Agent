"""Bootstrap helpers for multi-book trading agent_data directories."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Mapping

from core.logging import init_component_logger
from services.pipeline.daily_pipeline import build_run_manifest


LOGGER = init_component_logger(
    "BookDataBootstrap",
    group="services/trading",
    filename_prefix="book_data_bootstrap",
)

BOOK_SIGNATURES = {
    "fixed_tracked": "book-fixed_tracked",
    "short_book": "book-short_book",
    "long_book": "book-long_book",
}
DEFAULT_BOOK_BUDGETS = {
    "short_book": 200000.0,
    "long_book": 400000.0,
}


def _normalize_run_date(run_date: str | None, *, base_dir: str) -> str:
    if run_date:
        return run_date

    skill_runs_dir = Path(base_dir) / "skill_runs"
    if not skill_runs_dir.exists():
        raise FileNotFoundError(f"skill_runs 目录不存在: {skill_runs_dir}")

    candidates = sorted(
        path.name
        for path in skill_runs_dir.iterdir()
        if path.is_dir() and (path / "run_manifest.json").exists()
    )
    if not candidates:
        raise FileNotFoundError(f"未找到包含 run_manifest.json 的 skill_runs 日期目录: {skill_runs_dir}")
    return candidates[-1]


def _load_manifest(run_date: str, *, base_dir: str) -> Dict[str, Any]:
    manifest_path = Path(base_dir) / "skill_runs" / run_date / "run_manifest.json"
    if manifest_path.exists():
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    LOGGER.warning("run_manifest.json 不存在，回退实时构建: %s", manifest_path)
    return build_run_manifest(run_date, base_dir=base_dir)


def _book_map(manifest: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    result: Dict[str, Mapping[str, Any]] = {}
    for book in manifest.get("books") or []:
        book_type = book.get("book_type")
        if isinstance(book_type, str) and book_type in BOOK_SIGNATURES:
            result[book_type] = book
    return result


def _agent_data_dir(base_dir: str, signature: str) -> Path:
    return Path(base_dir) / "agent_data" / signature


def _write_json(path: Path, payload: Any, *, force: bool) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_position_init(
    path: Path,
    *,
    run_date: str,
    symbols: List[str],
    initial_cash: float,
    force: bool,
) -> None:
    if path.exists() and not force:
        return

    positions = {symbol: 0 for symbol in symbols if isinstance(symbol, str) and symbol}
    positions["CASH"] = round(float(initial_cash), 2)
    record = {
        "date": run_date,
        "id": 0,
        "positions": positions,
        "this_action": {"action": "init"},
        "total_value": round(float(initial_cash), 2),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _resolve_book_initial_cash(book_type: str, book_payload: Mapping[str, Any]) -> float:
    configured_budget = _safe_float(book_payload.get("capital_budget"), 0.0)
    if configured_budget > 0:
        return configured_budget
    return DEFAULT_BOOK_BUDGETS.get(book_type, 500000.0)


def _bootstrap_empty_book(
    target_dir: Path,
    *,
    run_date: str,
    symbols: List[str],
    initial_cash: float,
    force: bool,
) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    _write_json(target_dir / "stock_decisions.json", [], force=force)
    _write_json(target_dir / "decision_summary.json", [], force=force)
    _write_json(target_dir / "portfolio_daily_summary.json", [], force=force)
    _write_json(target_dir / "daily_summary.json", {"entries": []}, force=force)
    _write_position_init(
        target_dir / "position" / "position.jsonl",
        run_date=run_date,
        symbols=symbols,
        initial_cash=initial_cash,
        force=force,
    )


def bootstrap_book_agent_data(
    *,
    run_date: str | None = None,
    base_dir: str = "data",
    legacy_signature: str = "deepseek-reasoner",
    force: bool = False,
) -> Dict[str, Any]:
    resolved_run_date = _normalize_run_date(run_date, base_dir=base_dir)
    manifest = _load_manifest(resolved_run_date, base_dir=base_dir)
    book_payloads = _book_map(manifest)

    results: Dict[str, Any] = {
        "run_date": resolved_run_date,
        "base_dir": str(Path(base_dir).resolve()),
        "legacy_signature": legacy_signature,
        "books": {},
    }

    fixed_signature = BOOK_SIGNATURES["fixed_tracked"]
    fixed_target_dir = _agent_data_dir(base_dir, fixed_signature)
    legacy_dir = _agent_data_dir(base_dir, legacy_signature)
    fixed_book = book_payloads.get("fixed_tracked") or {}
    fixed_symbols = [
        symbol
        for symbol in fixed_book.get("symbols") or []
        if isinstance(symbol, str) and symbol
    ]

    if legacy_dir.exists():
        if fixed_target_dir.exists() and not force:
            action = "skip_existing"
            LOGGER.info("fixed_tracked 已存在，跳过复制: %s", fixed_target_dir)
        else:
            shutil.copytree(legacy_dir, fixed_target_dir, dirs_exist_ok=force)
            action = "copied_from_legacy"
            LOGGER.info("fixed_tracked 账本已复制: %s -> %s", legacy_dir, fixed_target_dir)
    else:
        initial_cash = _resolve_book_initial_cash("fixed_tracked", fixed_book)
        _bootstrap_empty_book(
            fixed_target_dir,
            run_date=resolved_run_date,
            symbols=fixed_symbols,
            initial_cash=initial_cash,
            force=force,
        )
        action = "initialized_empty"
        LOGGER.warning("legacy signature 不存在，fixed_tracked 退回空账本初始化: %s", legacy_dir)

    results["books"]["fixed_tracked"] = {
        "signature": fixed_signature,
        "path": str(fixed_target_dir),
        "action": action,
    }

    for book_type in ("short_book", "long_book"):
        signature = BOOK_SIGNATURES[book_type]
        target_dir = _agent_data_dir(base_dir, signature)
        book_payload = book_payloads.get(book_type) or {}
        symbols = [
            symbol
            for symbol in book_payload.get("symbols") or []
            if isinstance(symbol, str) and symbol
        ]
        initial_cash = _resolve_book_initial_cash(book_type, book_payload)

        if target_dir.exists() and not force:
            action = "skip_existing"
            LOGGER.info("%s 已存在，跳过初始化: %s", book_type, target_dir)
        else:
            _bootstrap_empty_book(
                target_dir,
                run_date=resolved_run_date,
                symbols=symbols,
                initial_cash=initial_cash,
                force=force,
            )
            action = "initialized_empty"
            LOGGER.info(
                "%s 初始化完成: path=%s, symbols=%d, initial_cash=%.2f",
                book_type,
                target_dir,
                len(symbols),
                initial_cash,
            )

        results["books"][book_type] = {
            "signature": signature,
            "path": str(target_dir),
            "action": action,
            "initial_cash": initial_cash,
            "symbols_count": len(symbols),
        }

    return results


__all__ = ["bootstrap_book_agent_data"]
