"""Daily skill pipeline orchestration service."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Iterable, List

from configs.stock_pool import TRACKED_A_STOCKS
from core.logging import init_component_logger
from services.pipeline.steps.build_agent_input import build_snapshot_payload
from services.pipeline.steps.build_agent_input import write_agent_input_bundle
from services.pipeline.steps.build_global_context import write_global_context
from services.pipeline.steps.build_stock_research import write_stock_research_bundle
from services.pipeline.steps.refresh_data import run_refresh_data
from services.selection_system.store import load_json_file


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SKILL_FLOW_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "skill_flow.json"
SHORT_BOOK_FLOW_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "skill_flow_short_book.json"
LONG_BOOK_FLOW_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "skill_flow_long_book.json"
LOGGER = init_component_logger(
    "DailyPipeline",
    group="services/pipeline",
    filename_prefix="daily_pipeline",
)


def _round_sec(value: float) -> float:
    return round(float(value), 2)


def _slowest_stage_label(stage_costs: Dict[str, float]) -> str:
    if not stage_costs:
        return ""
    return max(stage_costs.items(), key=lambda item: item[1])[0]


def resolve_output_dir(base_dir: str, run_date: str) -> Path:
    return Path(base_dir) / "skill_runs" / run_date


def safe_clean_dir(target_dir: Path) -> None:
    if target_dir.exists():
        if target_dir.is_dir() and target_dir.parent.name == "skill_runs":
            shutil.rmtree(target_dir)
        else:
            raise ValueError(f"Refuse to clean unexpected path: {target_dir}")
    target_dir.mkdir(parents=True, exist_ok=True)


def _book_signature(book_type: str) -> str:
    return f"book-{book_type}"


def _load_selection_symbols(path: Path) -> List[str]:
    payload = load_json_file(path, default={}) or {}
    items = payload.get("items") or []
    symbols: List[str] = []
    for item in items:
        symbol = (item or {}).get("symbol")
        if isinstance(symbol, str) and symbol.strip() and symbol not in symbols:
            symbols.append(symbol.strip())
    return symbols


def _collect_manifest_symbols(manifest: Dict[str, Any]) -> List[str]:
    symbols: List[str] = []
    for book in manifest.get("books") or []:
        for symbol in book.get("symbols") or []:
            if isinstance(symbol, str) and symbol and symbol not in symbols:
                symbols.append(symbol)
    return symbols


def build_run_manifest(run_date: str, *, base_dir: str = "data") -> Dict[str, Any]:
    base_path = Path(base_dir)
    selection_dir = base_path / "selection_runs" / run_date
    short_candidates = selection_dir / "08_short_book_candidates.json"
    long_candidates = selection_dir / "09_long_book_candidates.json"

    fixed_symbols = [entry.symbol for entry in TRACKED_A_STOCKS]
    short_symbols = _load_selection_symbols(short_candidates) if short_candidates.exists() else []
    long_symbols = _load_selection_symbols(long_candidates) if long_candidates.exists() else []

    books = [
        {
            "book_type": "fixed_tracked",
            "signature": _book_signature("fixed_tracked"),
            "prompt_config": str(SKILL_FLOW_CONFIG),
            "source_type": "tracked_stock_pool",
            "source_path": "configs/stock_pool.py",
            "capital_budget": 0,
            "symbols": fixed_symbols,
        },
        {
            "book_type": "short_book",
            "signature": _book_signature("short_book"),
            "prompt_config": str(SHORT_BOOK_FLOW_CONFIG),
            "source_type": "selection_candidates",
            "source_path": str(short_candidates),
            "capital_budget": 200000,
            "symbols": short_symbols,
        },
        {
            "book_type": "long_book",
            "signature": _book_signature("long_book"),
            "prompt_config": str(LONG_BOOK_FLOW_CONFIG),
            "source_type": "selection_candidates",
            "source_path": str(long_candidates),
            "capital_budget": 400000,
            "symbols": long_symbols,
        },
    ]
    return {"run_date": run_date, "books": books}


def _write_manifest(output_dir: Path, manifest: Dict[str, Any]) -> Path:
    path = output_dir / "run_manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _book_output_dir(base_output_dir: Path, book_type: str) -> Path:
    target = base_output_dir / book_type
    target.mkdir(parents=True, exist_ok=True)
    return target


def run_book_pipeline(
    run_date: str,
    *,
    output_dir: str | Path,
    symbols: Iterable[str],
    prompt_config: str | Path,
    signature: str,
    book_type: str,
    max_workers: int = 1,
) -> tuple[Path, Dict[str, Any]]:
    target_symbols = [symbol for symbol in symbols if symbol]
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    stage_start = perf_counter()
    snapshot_payload = build_snapshot_payload(
        run_date,
        target_symbols,
        max_workers=max_workers,
    )
    snapshot_sec = perf_counter() - stage_start

    stage_start = perf_counter()
    write_global_context(
        run_date,
        target_dir,
    )
    global_context_sec = perf_counter() - stage_start

    stage_start = perf_counter()
    write_stock_research_bundle(
        run_date,
        target_dir,
        symbols=target_symbols,
        snapshot_payload=snapshot_payload,
        signature=signature,
        book_type=book_type,
        max_workers=max_workers,
    )
    research_sec = perf_counter() - stage_start

    stage_start = perf_counter()
    write_agent_input_bundle(
        run_date,
        target_dir,
        symbols=target_symbols,
        book_type=book_type,
        signature=signature,
        prompt_config=prompt_config,
        snapshot_payload=snapshot_payload,
    )
    agent_input_sec = perf_counter() - stage_start

    stage_costs = {
        "snapshot": snapshot_sec,
        "global_context": global_context_sec,
        "stock_research": research_sec,
        "agent_input": agent_input_sec,
    }
    metrics = {
        "book_type": book_type,
        "symbol_count": len(target_symbols),
        "max_workers": max_workers,
        "stage_costs_sec": {key: _round_sec(val) for key, val in stage_costs.items()},
        "total_sec": _round_sec(sum(stage_costs.values())),
        "slowest_stage": _slowest_stage_label(stage_costs),
    }
    return target_dir, metrics


def run_daily_pipeline_from_manifest(
    run_date: str,
    *,
    base_dir: str = "data",
    manifest: Dict[str, Any],
    refresh_data: bool = True,
    max_workers: int = 1,
) -> Path:
    overall_start = perf_counter()
    output_dir = resolve_output_dir(base_dir, run_date)
    safe_clean_dir(output_dir)
    _write_manifest(output_dir, manifest)
    LOGGER.info(
        "开始执行 daily pipeline: date=%s, output_dir=%s, books=%d, max_workers=%d",
        run_date,
        output_dir,
        len(manifest.get("books") or []),
        max_workers,
    )

    refresh_sec = 0.0

    if refresh_data:
        refresh_start = perf_counter()
        run_refresh_data(
            run_date,
            signature=_book_signature("fixed_tracked"),
            symbols=_collect_manifest_symbols(manifest),
            max_workers=max_workers,
        )
        refresh_sec = perf_counter() - refresh_start
        LOGGER.info("阶段耗时 refresh_data: %.2fs", refresh_sec)

    books = manifest.get("books") or []
    book_metrics: List[Dict[str, Any]] = []
    for book in books:
        symbols = book.get("symbols") or []
        if not symbols:
            LOGGER.info(
                "跳过空账本: book_type=%s, source=%s",
                book.get("book_type"),
                book.get("source_path"),
            )
            continue
        book_start = perf_counter()
        _, metrics = run_book_pipeline(
            run_date,
            output_dir=_book_output_dir(output_dir, book["book_type"]),
            symbols=symbols,
            prompt_config=book["prompt_config"],
            signature=book["signature"],
            book_type=book["book_type"],
            max_workers=max_workers,
        )
        measured_book_sec = perf_counter() - book_start
        metrics["measured_total_sec"] = _round_sec(measured_book_sec)
        book_metrics.append(metrics)
        LOGGER.info(
            "账本耗时 book_type=%s symbols=%d total=%.2fs slowest=%s breakdown=%s",
            metrics["book_type"],
            metrics["symbol_count"],
            measured_book_sec,
            metrics["slowest_stage"],
            json.dumps(metrics["stage_costs_sec"], ensure_ascii=False, sort_keys=True),
        )

    overall_sec = perf_counter() - overall_start
    stage_totals: Dict[str, float] = {"refresh_data": refresh_sec}
    for metrics in book_metrics:
        stage_totals[metrics["book_type"]] = float(metrics.get("measured_total_sec") or 0.0)
    slowest_top_level = _slowest_stage_label(stage_totals)
    LOGGER.info(
        "daily pipeline 完成: date=%s total=%.2fs slowest_top_level=%s stage_totals=%s",
        run_date,
        overall_sec,
        slowest_top_level,
        json.dumps({key: _round_sec(val) for key, val in stage_totals.items()}, ensure_ascii=False, sort_keys=True),
    )
    return output_dir


def run_daily_pipeline(
    run_date: str,
    *,
    base_dir: str = "data",
    prompt_config: str | Path | None = None,
    signature: str = "",
    manifest_path: str | Path | None = None,
    max_workers: int = 4,
) -> Path:
    LOGGER.info(
        "run_daily_pipeline 请求: date=%s, base_dir=%s, manifest=%s, prompt_config=%s, signature=%s, max_workers=%d",
        run_date,
        base_dir,
        manifest_path or "auto",
        str(prompt_config) if prompt_config else "",
        signature,
        max_workers,
    )
    if manifest_path and str(manifest_path) != "auto":
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    else:
        manifest = build_run_manifest(run_date, base_dir=base_dir)
    if prompt_config or signature:
        # 兼容旧调用：只跑 fixed_tracked 单账本
        fixed_symbols = [entry.symbol for entry in TRACKED_A_STOCKS]
        output_dir = resolve_output_dir(base_dir, run_date)
        safe_clean_dir(output_dir)
        overall_start = perf_counter()
        refresh_start = perf_counter()
        run_refresh_data(
            run_date,
            signature=signature or _book_signature("fixed_tracked"),
            max_workers=max_workers,
        )
        refresh_sec = perf_counter() - refresh_start
        LOGGER.info("阶段耗时 refresh_data: %.2fs", refresh_sec)
        _, metrics = run_book_pipeline(
            run_date,
            output_dir=_book_output_dir(output_dir, "fixed_tracked"),
            symbols=fixed_symbols,
            prompt_config=prompt_config or SKILL_FLOW_CONFIG,
            signature=signature or _book_signature("fixed_tracked"),
            book_type="fixed_tracked",
            max_workers=max_workers,
        )
        overall_sec = perf_counter() - overall_start
        LOGGER.info(
            "账本耗时 book_type=%s symbols=%d total=%.2fs slowest=%s breakdown=%s",
            metrics["book_type"],
            metrics["symbol_count"],
            float(metrics["total_sec"]),
            metrics["slowest_stage"],
            json.dumps(metrics["stage_costs_sec"], ensure_ascii=False, sort_keys=True),
        )
        LOGGER.info(
            "daily pipeline 完成(兼容单账本模式): date=%s total=%.2fs slowest_top_level=%s stage_totals=%s",
            run_date,
            overall_sec,
            _slowest_stage_label({"refresh_data": refresh_sec, "fixed_tracked": float(metrics["total_sec"])}),
            json.dumps(
                {"refresh_data": _round_sec(refresh_sec), "fixed_tracked": float(metrics["total_sec"])},
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        return output_dir
    return run_daily_pipeline_from_manifest(
        run_date,
        base_dir=base_dir,
        manifest=manifest,
        max_workers=max_workers,
    )
