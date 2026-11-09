"""US research pack builder (``us_tracked`` book).

复用 fixed_tracked 的 ``01-04`` 结构，只为美股标的生成同构研究包：

* ``01_global_context.md``：全球宏观 + 标普 500（SPY）基准；
* ``02_basic_snapshot_payload.json``：美股快照（价格/估值/风险/财务指标）；
* ``03_agent_input.md`` / ``03_stock_analysis_input.md``：沿用 fixed_tracked 策略源；
* ``04_stock_research/*.md``：完整个股研究包。

设计约束：不写 ``fixed_tracked`` 目录、不清理整个日期目录、不触发 A 股数据刷新。
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from configs.stock_pool import US_TRACKED_STOCKS
from core.logging import init_component_logger
from services.pipeline.steps.build_agent_input import (
    build_snapshot_payload,
    write_agent_input_bundle,
)
from services.pipeline.steps.build_stock_research import write_stock_research_bundle
from services.research.macro_summary import get_macro_summary
from services.research.stock_analysis import analyze_stock_dynamics_and_valuation
from shared_data_access.cache_registry import ensure_symbol_data
from shared_data_access.market_calendar import inspect_market_session
from shared_data_access.us_market import latest_completed_us_session
from commons import parse_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROMPT_CONFIG = PROJECT_ROOT / "configs" / "prompt_flow" / "fixed_tracked" / "main_policy.md"
BOOK_TYPE = "us_tracked"
SIGNATURE = "book-us_tracked"
US_BENCHMARK_SYMBOL = "SPY.US"
LOGGER = init_component_logger(
    "UsResearch",
    group="services/pipeline",
    filename_prefix="us_research",
)


def resolve_us_analysis_date(requested: str | None = None) -> str:
    """Resolve the latest US session whose close is already available.

    ``--date`` 语义与 A 股一致（要分析的交易日，收盘数据已产生）。北京时间晚上
    运行时，当天美东通常还没收盘，因此会自动回退到最近一个已收盘交易日。
    """

    completed = latest_completed_us_session().isoformat()
    if not requested:
        return completed
    text = str(requested).strip()
    session = inspect_market_session(text, market="US")
    if not session.is_trading_day:
        LOGGER.warning("%s 不是美股交易日，自动回退到最近已收盘交易日 %s", text, completed)
        return completed
    if text > completed:
        LOGGER.warning(
            "%s 的美股收盘尚未产生（最近已收盘 %s），自动回退", text, completed
        )
        return completed
    return text


def _resolve_symbols(symbols: Optional[Iterable[str]]) -> List[str]:
    if symbols:
        resolved = [str(item).strip().upper() for item in symbols if str(item).strip()]
    else:
        resolved = [entry.symbol for entry in US_TRACKED_STOCKS]
    for symbol in resolved:
        info = parse_symbol(symbol)
        if info.market != "US":
            raise ValueError(f"us_tracked 只接受美股标的（TICKER.US），收到 {symbol}")
    return resolved


def _build_us_global_context(run_date: str) -> str:
    macro_text = get_macro_summary(today_time=run_date, include_objective_panel=False)
    benchmark_payload = analyze_stock_dynamics_and_valuation(US_BENCHMARK_SYMBOL, run_date)
    sections = [
        "# 美股全局宏观与基准上下文",
        "",
        f"- 分析日期（美东已收盘交易日）：{run_date}",
        f"- 基准标的：{US_BENCHMARK_SYMBOL}（标普 500 ETF，美元计价）",
        "",
        "## 1. 全球宏观总结",
        "",
        macro_text.strip() if isinstance(macro_text, str) else str(macro_text),
        "",
        "## 2. 标普 500（SPY）价格与风险",
        "",
        "### 2.1 Price Report JSON",
        "```json",
        json.dumps(benchmark_payload.get("price_report"), ensure_ascii=False, indent=2),
        "```",
        "",
        "### 2.2 估值说明",
        benchmark_payload.get("valuation_report")
        or benchmark_payload.get("valuation_unavailable_reason")
        or "> ETF 不适用个股 PE/PB 估值。",
        "",
        "> 说明：宏观总结仍是既有中国/全球口径，美股个股结论不应直接等同于 A 股宏观结论。",
        "",
    ]
    return "\n".join(sections)


US_BOOK_NOTE = """
> **美股账本说明（us_tracked）**
> - 本账本只覆盖美股，沿用 fixed_tracked 的投资哲学与研究方法；不接入 A 股交易执行、涨跌停/T+1 规则与仓位账本。
> - 金额单位：美元；财报金额已按隐含汇率折算为美元，原始币种见 `financials_cache/us_financial_meta.json`。
> - 公告来源为 SEC EDGAR（6-K/8-K/10-Q/10-K/20-F 等），A 股/港股特有的龙虎榜、北向资金、涨跌停等字段不适用。
> - 文中若出现「默认初始资产 50 万元」等 A 股账本占位值，仅为 prompt 兼容保留，不代表美股实盘资金。
"""


def _inject_us_book_note(output_dir: Path) -> None:
    """在复用的 fixed_tracked prompt 前插入美股账本说明。"""

    agent_input = output_dir / "03_agent_input.md"
    if agent_input.exists():
        text = agent_input.read_text(encoding="utf-8")
        if "美股账本说明" not in text:
            text = text.replace("\n## SYSTEM_PROMPT", "\n" + US_BOOK_NOTE + "\n## SYSTEM_PROMPT", 1)
            agent_input.write_text(text, encoding="utf-8")

    stock_input = output_dir / "03_stock_analysis_input.md"
    if stock_input.exists():
        text = stock_input.read_text(encoding="utf-8")
        if "美股账本说明" not in text:
            text = text.replace(
                "\n# fixed_tracked 共享投资策略",
                "\n" + US_BOOK_NOTE + "\n# fixed_tracked 共享投资策略",
                1,
            )
            stock_input.write_text(text, encoding="utf-8")


def _clean_book_dir(target: Path) -> None:
    """只清理 ``skill_runs/<date>/us_tracked``，绝不碰 fixed_tracked 或整日目录。"""

    if target.exists():
        safe = target.name == BOOK_TYPE and target.parent.parent.name == "skill_runs"
        if not safe:
            raise ValueError(f"Refuse to clean unexpected path: {target}")
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)


def _write_manifest(run_date: str, output_dir: Path, symbols: List[str]) -> None:
    payload = {
        "run_date": run_date,
        "book_type": BOOK_TYPE,
        "signature": SIGNATURE,
        "prompt_config": str(PROMPT_CONFIG),
        "source_type": "us_tracked_pool",
        "source_path": "configs/stock_pool.py（US_TRACKED_STOCKS）",
        "benchmark": US_BENCHMARK_SYMBOL,
        "symbols": symbols,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    (output_dir / "run_manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _prepare_symbol_data(symbols: List[str], run_date: str) -> None:
    for symbol in symbols:
        info = parse_symbol(symbol)
        LOGGER.info("准备美股数据: %s %s", info.stock_name, info.symbol)
        ensure_symbol_data(
            "data",
            info,
            LOGGER,
            lookback_price_days=1800,
            include_disclosures=True,
            disclosure_lookback_days=730,
        )
    benchmark = parse_symbol(US_BENCHMARK_SYMBOL)
    ensure_symbol_data(
        "data",
        benchmark,
        LOGGER,
        lookback_price_days=1800,
    )


def run_us_research_pack(
    run_date: Optional[str] = None,
    *,
    symbols: Optional[Iterable[str]] = None,
    base_dir: str = "data",
    max_workers: int = 2,
    skip_refresh: bool = False,
) -> Path:
    resolved_date = resolve_us_analysis_date(run_date)
    target_symbols = _resolve_symbols(symbols)
    output_dir = Path(base_dir) / "skill_runs" / resolved_date / BOOK_TYPE
    _clean_book_dir(output_dir)

    LOGGER.info(
        "开始生成美股研究包: date=%s, symbols=%s, output=%s",
        resolved_date,
        ",".join(target_symbols),
        output_dir,
    )

    if not skip_refresh:
        _prepare_symbol_data(target_symbols, resolved_date)

    snapshot_payload = build_snapshot_payload(
        resolved_date,
        target_symbols,
        max_workers=max_workers,
    )
    (output_dir / "02_basic_snapshot_payload.json").write_text(
        json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    (output_dir / "01_global_context.md").write_text(
        _build_us_global_context(resolved_date),
        encoding="utf-8",
    )

    write_stock_research_bundle(
        resolved_date,
        output_dir,
        symbols=target_symbols,
        snapshot_payload=snapshot_payload,
        signature=SIGNATURE,
        book_type=BOOK_TYPE,
        max_workers=max_workers,
        report_release_slack_days=1,
        allow_news_refresh=True,
    )

    write_agent_input_bundle(
        resolved_date,
        output_dir,
        symbols=target_symbols,
        book_type=BOOK_TYPE,
        signature=SIGNATURE,
        prompt_config=PROMPT_CONFIG,
        snapshot_payload=snapshot_payload,
        max_workers=max_workers,
    )

    _inject_us_book_note(output_dir)
    _write_manifest(resolved_date, output_dir, target_symbols)
    LOGGER.info("美股研究包生成完成: %s", output_dir)
    return output_dir


__all__ = ["resolve_us_analysis_date", "run_us_research_pack"]
