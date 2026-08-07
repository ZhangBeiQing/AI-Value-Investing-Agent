"""Step 2: build the shared global context markdown."""

from __future__ import annotations

import json
from pathlib import Path

from services.research.macro_summary import get_macro_summary
from services.research.stock_analysis import analyze_stock_dynamics_and_valuation
from shared_data_access.historical_prices import load_price_history
from utlity import parse_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[3]
INDEX_SYMBOL = "000001.IDX"


def _build_read_only_index_payload(
    run_date: str,
    source_data_root: str | Path | None,
) -> dict:
    frame = load_price_history(
        parse_symbol(INDEX_SYMBOL),
        base_dir=source_data_root or PROJECT_ROOT / "data",
        as_of_date=run_date,
    )
    if frame.empty:
        return {
            "price_report": {
                "symbol": INDEX_SYMBOL,
                "analysis_date": run_date,
                "error": "共享行情缓存中没有截止该日的上证指数数据",
            },
            "valuation_report": None,
            "valuation_unavailable_reason": "回测只读模式不生成新的指数分析缓存。",
        }
    latest = frame.iloc[-1]
    previous = frame.iloc[-2] if len(frame) > 1 else latest
    previous_close = float(previous.get("收盘") or 0.0)
    latest_close = float(latest.get("收盘") or 0.0)
    def _number(field: str) -> float | None:
        value = latest.get(field)
        return float(value) if value is not None else None

    daily_change = (
        (latest_close / previous_close - 1.0) * 100.0
        if previous_close
        else None
    )
    return {
        "price_report": {
            "symbol": INDEX_SYMBOL,
            "analysis_date": latest["日期"].strftime("%Y-%m-%d"),
            "open": _number("开盘"),
            "high": _number("最高"),
            "low": _number("最低"),
            "close": latest_close,
            "daily_change_pct": daily_change,
            "source": "shared_data_access 历史行情缓存（只读、按回测日截断）",
        },
        "valuation_report": None,
        "valuation_unavailable_reason": "指数不适用个股 PE/PB 估值；回测模式不写共享派生报告。",
    }


def build_global_context(
    run_date: str,
    *,
    backtest_read_only: bool = False,
    source_data_root: str | Path | None = None,
) -> str:
    macro_text = get_macro_summary(
        today_time=run_date,
        include_objective_panel=not backtest_read_only,
    )
    index_payload = (
        _build_read_only_index_payload(run_date, source_data_root)
        if backtest_read_only
        else analyze_stock_dynamics_and_valuation(INDEX_SYMBOL, run_date)
    )

    sections = [
        "# 全局宏观与上证指数上下文",
        "",
        "## 1. 宏观总结",
        "",
        macro_text.strip() if isinstance(macro_text, str) else str(macro_text),
        "",
        "## 2. 上证指数分析",
        "",
        "### 2.1 Price Report JSON",
        "```json",
        json.dumps(index_payload.get("price_report"), ensure_ascii=False, indent=2),
        "```",
        "",
        "### 2.2 Valuation Report (Markdown)",
        index_payload.get("valuation_report") or "> 无估值数据或标的不支持。",
    ]
    reason = index_payload.get("valuation_unavailable_reason")
    if reason:
        sections.extend(["", f"> 说明：{reason}"])
    sections.append("")
    return "\n".join(sections)


def write_global_context(
    run_date: str,
    output_dir: str | Path,
    *,
    backtest_read_only: bool = False,
    source_data_root: str | Path | None = None,
) -> Path:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / "01_global_context.md"
    output_path.write_text(
        build_global_context(
            run_date,
            backtest_read_only=backtest_read_only,
            source_data_root=source_data_root,
        ),
        encoding="utf-8",
    )
    return output_path
