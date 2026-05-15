"""Build shared selection-context markdown from current upstream artifacts."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from core.logging import get_logger

from .announcement_summary import load_or_build_recent_company_announcements
from .master_universe import load_master_universe
from .paths import SelectionSystemPaths
from .store import load_json_file


LOGGER = get_logger("SelectionSharedContext")


def build_shared_selection_context(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    announcement_limit: int = 24,
    announcements_payload_override: Mapping[str, Any] | None = None,
) -> dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)
    announcements = announcements_payload_override or load_or_build_recent_company_announcements(
        run_date,
        base_dir=base_dir,
        refresh_missing=False,
    )

    content = render_shared_selection_context(
        run_date,
        base_dir=base_dir,
        announcement_limit=announcement_limit,
        announcements_payload_override=announcements,
    )
    target = paths.run_shared_selection_context_path(run_date)
    target.write_text(content, encoding="utf-8")
    LOGGER.info("shared selection context 已写入: %s", target)
    return {"shared_selection_context": target}


def render_shared_selection_context(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    announcement_limit: int = 24,
    announcements_payload_override: Mapping[str, Any] | None = None,
) -> str:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    universe = load_master_universe(paths)
    hot_news_state = load_json_file(paths.run_hot_news_state_path(run_date), default={}) or {}
    board_heat_digest = load_json_file(paths.run_board_heat_digest_path(run_date), default={}) or {}
    board_heat_state = load_json_file(paths.run_board_heat_state_path(run_date), default={}) or {}
    announcements = announcements_payload_override or load_json_file(paths.run_recent_company_announcements_path(run_date), default={}) or {}
    macro_path, macro_full_text = _load_macro_context(run_date, base_dir=base_dir)
    announcement_lookback_days = max(int(announcements.get("lookback_days") or 3), 1) if isinstance(announcements, Mapping) else 3

    lines: list[str] = []
    lines.append("# Shared Selection Context")
    lines.append("")
    lines.append(f"- run_date: {run_date}")
    lines.append(f"- generated_at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("- market_scope: A股 + 港股 + ETF")
    lines.append("")

    lines.append("## 1. 股票宇宙摘要")
    lines.append("")
    lines.append(f"- universe_name: {universe.universe_name}")
    lines.append(f"- stock_count: {len(universe.stocks)}")
    lines.append(f"- updated_at: {universe.updated_at}")
    lines.append("- 使用原则：选股系统以 master_universe 为初筛母集，再从中产出短期/长期候选池。")
    preview = ", ".join(f"{stock.name}({stock.symbol})" for stock in universe.stocks[:12])
    if preview:
        lines.append(f"- preview: {preview}")
    lines.append("")

    lines.append("## 2. 宏观背景全文")
    lines.append("")
    if macro_path is None:
        lines.append("- 未找到可用宏观总结文件。")
    else:
        lines.append(f"- source: {macro_path}")
        lines.append("- full_text:")
        lines.append("```text")
        lines.append(macro_full_text)
        lines.append("```")
    lines.append("")

    lines.append("## 3. 主题主线全文")
    lines.append("")
    active_themes = hot_news_state.get("active_themes") if isinstance(hot_news_state, Mapping) else []
    if isinstance(active_themes, list) and active_themes:
        lines.append(f"- active_theme_count: {len(active_themes)}")
        lines.append("- source: 06_hot_news_state.json.active_themes")
        lines.append("```json")
        lines.append(json.dumps(active_themes, ensure_ascii=False, indent=2))
        lines.append("```")
    else:
        lines.append("- 未找到 active_themes。")
    lines.append("")

    lines.append("## 4. 板块热点全文")
    lines.append("")
    if isinstance(board_heat_digest, Mapping) and board_heat_digest:
        lines.append("- source: 05_board_heat_digest.json")
        lines.append("```json")
        lines.append(json.dumps(board_heat_digest, ensure_ascii=False, indent=2))
        lines.append("```")
    else:
        lines.append("- 未找到 05_board_heat_digest.json。")
    lines.append("")
    if isinstance(board_heat_state, Mapping) and board_heat_state:
        lines.append("- source: 05_board_heat_state.json")
        lines.append("```json")
        lines.append(json.dumps(board_heat_state, ensure_ascii=False, indent=2))
        lines.append("```")
    else:
        lines.append("- 未找到 05_board_heat_state.json。")
    lines.append("")

    lines.append(f"## 5. 最近{announcement_lookback_days}天公告摘要")
    lines.append("")
    announcement_items = (announcements.get("items") or []) if isinstance(announcements, Mapping) else []
    if announcement_items:
        lines.append(f"- announcement_count: {len(announcement_items)}")
        for item in announcement_items[: max(int(announcement_limit), 1)]:
            if not isinstance(item, Mapping):
                continue
            lines.append(
                f"- {item.get('published_at')} | {item.get('stock_name')}({item.get('symbol')}) | "
                f"{_one_line(item.get('summary'))}"
            )
    else:
        lines.append(f"- 未找到最近{announcement_lookback_days}天公告摘要。")
    lines.append("")

    lines.append("## 6. Snapshot 使用说明")
    lines.append("")
    lines.append("- snapshot 基座：services/snapshot/basic_snapshot.py")
    lines.append("- 推荐使用方式：字段说明 + 排序接口 + 过滤接口 + 单股查询接口")
    lines.append("- 短期池重点字段：daily_change_pct, return_3m, sharpe_3m, volatility_3m, max_drawdown_3m, turnover_rate, avg_turnover_30d, liquidity_score")
    lines.append("- 长期池重点字段：roe, revenue_growth_yoy, net_income_growth_yoy, gross_margin, net_profit_margin, pe_ttm, pb, ps, pe_3_5y_percentile, return_1y, max_drawdown_1y")
    lines.append("")

    lines.append("## 7. 连续性提示")
    lines.append("")
    lines.append("- 当前文件只负责汇总上游输入，不直接输出候选池。")
    lines.append("- 后续短期/长期选股头应结合昨日候选池、昨日 merge 结果与昨日深研观察点继续推进。")
    lines.append("")
    return "\n".join(lines)


def _load_macro_context(run_date: str, *, base_dir: str | Path) -> tuple[str | None, str]:
    base_path = Path(base_dir)
    macro_dir = base_path / "macro_economy"
    if not macro_dir.exists():
        return None, ""

    target = run_date.replace("-", "")
    candidates = sorted(
        (
            path for path in macro_dir.glob("*.md")
            if path.stem.isdigit() and path.stem <= target
        ),
        key=lambda path: path.stem,
    )
    if not candidates:
        return None, ""
    latest = candidates[-1]
    text = latest.read_text(encoding="utf-8")
    return str(latest), text.strip()


def _one_line(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().replace("\n", " ")
    if len(text) <= 120:
        return text
    return text[:117] + "..."


__all__ = [
    "build_shared_selection_context",
    "render_shared_selection_context",
]
