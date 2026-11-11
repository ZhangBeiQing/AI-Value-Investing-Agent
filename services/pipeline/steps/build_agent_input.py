"""Step 4: build basic snapshot and agent input markdown."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from configs.feature_flags import (
    board_heat_layer_enabled,
    gradual_news_summary_enabled,
)
from services.prompting.system_prompt import (
    get_skill_prompt_context,
    get_skill_system_prompt,
)
from services.snapshot.basic_snapshot import build_basic_snapshot
from commons.stock_utils import get_stock_data_dir, is_etf_symbol, parse_symbol
from services.trading.analysis_index import analysis_entry_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROMPT_CONFIG = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "main_policy.md"
)
FIXED_INVESTMENT_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "investment_policy.md"
)
FIXED_STOCK_ANALYSIS_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "stock_analysis_policy.md"
)
WEB_RESEARCH_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "research"
    / "web_research_policy.md"
)

DEFAULT_ANALYSIS_INDEX_REF = "data/skill_runs/_analysis_index.json"

PRICE_DELTA_REPORT_FILENAME = "price_delta_report.md"
P0_EVENT_REPORT_FILENAME = "p0_event_report.md"


def write_p0_event_report(
    run_date: str, output_dir: str | Path, symbols: Iterable[str],
    *, source_data_root: str | Path | None = None,
) -> Path:
    """Project cached announcement and news titles into the P0 input."""
    end_date = datetime.strptime(run_date, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=13)
    lines = [
        f"# P0 近期事件概览（{run_date}）", "",
        f"事件窗口：{start_date} 至 {end_date}。按发布日期判断 P0 的最近三个交易日条件。",
        "先按标题筛选可能改变盈利、估值、治理或风险的事项，需要时定向核验正文。",
        "宏观与产业政策结合 01 研判；缓存覆盖不足或待核验事项依赖最新事件时，先定向搜索补证。", "",
    ]
    for symbol in symbols:
        info = parse_symbol(symbol)
        lines.extend([f"## {info.stock_name or symbol}（{symbol}）", ""])
        if is_etf_symbol(info):
            lines.extend(["公司公告不适用；结合 01 的宏观和市场变化筛选。", ""])
            continue
        stock_root = get_stock_data_dir(info, base_dir=source_data_root)
        index_path = stock_root / "disclosures" / "index.json"
        news_path = stock_root / "news" / "news_audited.json"
        announcements = json.loads(index_path.read_text(encoding="utf-8")) if index_path.exists() else {}
        news = json.loads(news_path.read_text(encoding="utf-8")) if news_path.exists() else {}
        events: set[tuple[str, str]] = set()
        visible_dates = []
        for item in [*announcements.values(), *(news.get("news_items") or [])]:
            date_text = str(item.get("datetime") or item.get("date") or "")[:10]
            if not date_text or date_text > run_date:
                continue
            visible_dates.append(date_text)
            if date_text < start_date.isoformat():
                continue
            events.add((date_text, item["title"]))
        if not index_path.exists() and not news_path.exists():
            lines.extend(["事件资料缺失，需定向补证。", ""])
            continue
        latest = max(visible_dates) if visible_dates else "无可见日期"
        lines.append(f"缓存最近可见事件日期：{latest}；窗口内 {len(events)} 条。")
        if not events:
            lines.append("窗口内缓存未记录事件；需结合信息时效核查新增事项。")
        for event_date, title in sorted(events, reverse=True):
            lines.append(f"- {event_date}｜{title}")
        lines.append("")
    target = Path(output_dir) / P0_EVENT_REPORT_FILENAME
    target.write_text("\n".join(lines), encoding="utf-8")
    return target


def resolve_signature(raw_signature: str) -> str:
    if raw_signature:
        return raw_signature
    if os.environ.get("SIGNATURE"):
        return os.environ["SIGNATURE"]
    return os.environ.get("DEFAULT_SIGNATURE", "deepseek-reasoner")


def _build_stock_pool_block(symbols: Iterable[str]) -> str:
    lines = []
    for idx, symbol in enumerate(symbols, start=1):
        info = parse_symbol(symbol)
        lines.append(f"{idx}. {symbol} {info.stock_name or symbol}")
    return "\n".join(lines)


def build_user_query(research_files: List[Path], run_date: str, book_type: str) -> str:
    lines = [
        f"今天是 {run_date} 晚上，股市已经收盘。你需要复盘今天的股票，然后为明天的交易做决策",
        f"当前分析账本为 `{book_type}`。你只能基于当前账本的持仓、研究包和历史交易总结做决策，不能把其他账本的仓位或锚点混入本账本。",
        "请遵循 SYSTEM_PROMPT 的规范和价值投资原则，基于已提供的输入文件进行分析并决策今日的持仓调整。",
        "",
        "【强制输出（在任何分析之前）】",
        "1) 用你自己的话复述你对 configs/prompt_flow/skill_flow.json 股票分析流程与关键规则的理解。",
        "2) 从输入信息中提取并输出：当前总资金、可用现金、持仓明细（股票代码+股数）。",
        "",
        "## 外部输入文件（请按顺序打开）",
        "",
        "在完成 USER_QUERY 的两条强制输出前，不要打开任何外部文件。",
        "",
        "- 02_basic_snapshot_payload.json（《基本面数据概览》：basic_stock_info 生成的 basic snapshot；仅供主agent在 Step 0 做全组合快扫与定价基准）",
        "- 01_global_context.md（宏观/上证）",
        "",
        "",
        "## 个股研究包索引（按需逐只打开，禁止批量读取）",
        "",
    ]
    for file_path in research_files:
        lines.append(f"- {file_path.name}")
    lines.extend(["", "> 需要深度分析的股票请逐只打开对应研究包文件阅读。"])
    return "\n".join(lines)


def build_fixed_main_user_query(
    research_files: List[Path],
    run_date: str,
    analysis_index_ref: str = DEFAULT_ANALYSIS_INDEX_REF,
) -> str:
    """Build the fixed_tracked main-agent-only runtime input."""
    lines = [
        f"分析日期为 `{run_date}`，收盘数据已经产生。当前账本为 `fixed_tracked`。",
        "本文件只供主 Agent 做组合扫描、系统风险判断和 P0 队列选择。",
        "人工确认点和后续 Agent 调度只服从 auto-trading-fixed-tracked Skill；本 USER_QUERY 不另定义状态机。",
        "不要读取个股研究包，不要在派发给个股角色的 prompt 中复述或添加分析结论。",
        "",
        "## 主 Agent 外部输入",
        "",
        "- 02_basic_snapshot_payload.json",
        "- 01_global_context.md",
        f"- price_delta_report.md（当前价 vs 上次深研价变化，选 P0 参考）",
        "- p0_event_report.md（全池近期公告与新闻标题及资料覆盖情况）",
        *(
            [f"- data/selection_runs/{run_date}/06_hot_news_state.json（可选）"]
            if gradual_news_summary_enabled()
            else []
        ),
        *(
            [f"- data/selection_runs/{run_date}/05_board_heat_digest.json（可选）"]
            if board_heat_layer_enabled()
            else []
        ),
        f"- {analysis_index_ref}（不存在时按冷启动处理）",
        "",
        "## 个股研究包索引（仅用于确认文件存在，主 Agent 不打开内容）",
        "",
    ]
    for file_path in research_files:
        lines.append(f"- {file_path.name}")
    return "\n".join(lines)


def build_agent_input(
    run_date: str,
    signature: str,
    prompt_config: str | Path,
    output_dir: str | Path,
    *,
    stock_codes: Optional[List[str]] = None,
    book_type: str = "fixed_tracked",
    prompt_context: Optional[Dict[str, str]] = None,
    analysis_index_ref: Optional[str] = None,
) -> str:
    prompt_path = Path(prompt_config)
    target_symbols = stock_codes or []
    role_prompt = get_skill_system_prompt(
        run_date,
        signature,
        prompt_path,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    if book_type == "fixed_tracked":
        shared_policy = get_skill_system_prompt(
            run_date,
            signature,
            FIXED_INVESTMENT_POLICY,
            stock_codes=target_symbols,
            stock_pool_block_override=_build_stock_pool_block(target_symbols),
            prompt_context=prompt_context,
        )
        system_prompt = f"{shared_policy}\n\n---\n\n{role_prompt}"
    else:
        system_prompt = role_prompt
    research_files = sorted((Path(output_dir) / "04_stock_research").glob("*_research.md"))
    generated_at = datetime.now()

    system_prompt_section = (
        [system_prompt]
        if prompt_path.suffix.lower() == ".md"
        else ["```text", system_prompt, "```"]
    )
    sections = [
        "# Agent 输入",
        "",
        f"- 生成日期: {run_date}",
        f"- 生成时间: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- BOOK_TYPE: {book_type}",
        f"- PROMPT_FLOW_CONFIG: {prompt_path.resolve()}",
        *(
            [f"- SHARED_POLICY_SOURCE: {FIXED_INVESTMENT_POLICY.resolve()}"]
            if book_type == "fixed_tracked"
            else []
        ),
        f"- SIGNATURE: {signature}",
        "",
        "## SYSTEM_PROMPT",
        "",
        *system_prompt_section,
        "",
        "## USER_QUERY",
        "",
        (
            build_fixed_main_user_query(
                research_files,
                run_date,
                analysis_index_ref=analysis_index_ref or DEFAULT_ANALYSIS_INDEX_REF,
            )
            if book_type == "fixed_tracked"
            else build_user_query(research_files, run_date, book_type)
        ),
        "",
    ]
    return "\n".join(sections)


def build_stock_analysis_input(
    run_date: str,
    signature: str,
    *,
    stock_codes: Optional[List[str]] = None,
    prompt_context: Optional[Dict[str, str]] = None,
) -> str:
    """Build the common fixed_tracked input read by stock debate roles."""
    target_symbols = stock_codes or []
    shared_policy = get_skill_system_prompt(
        run_date,
        signature,
        FIXED_INVESTMENT_POLICY,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    research_policy = get_skill_system_prompt(
        run_date,
        signature,
        FIXED_STOCK_ANALYSIS_POLICY,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    web_research_policy = get_skill_system_prompt(
        run_date,
        signature,
        WEB_RESEARCH_POLICY,
        stock_codes=target_symbols,
        stock_pool_block_override=_build_stock_pool_block(target_symbols),
        prompt_context=prompt_context,
    )
    common_policy = (
        f"{shared_policy}\n\n---\n\n"
        f"{web_research_policy}\n\n---\n\n"
        f"{research_policy}"
    )
    generated_at = datetime.now()
    sections = [
        "# fixed_tracked 个股共同分析输入",
        "",
        f"- 生成日期: {run_date}",
        f"- 生成时间: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- SHARED_POLICY_SOURCE: {FIXED_INVESTMENT_POLICY.resolve()}",
        f"- WEB_RESEARCH_POLICY_SOURCE: {WEB_RESEARCH_POLICY.resolve()}",
        f"- RESEARCH_POLICY_SOURCE: {FIXED_STOCK_ANALYSIS_POLICY.resolve()}",
        f"- SIGNATURE: {signature}",
        "",
        common_policy,
        "",
        "## 角色规则",
        "",
        "本文件不定义 Bull、Bear、定向质询或 Adjudicator 的具体职责。",
        "每个角色必须另外直接读取主 Agent 指定的 Skill reference，不得由主 Agent复述。",
        "",
    ]
    return "\n".join(sections)


def write_price_delta_report(
    run_date: str,
    output_dir: str | Path,
    snapshot_payload: Dict[str, Any],
    *,
    book_type: str = "fixed_tracked",
    analysis_index_ref: Optional[str] = None,
) -> Optional[Path]:
    """生成「当前价 vs 上次深研价」变化报告，供主 Agent 选 P0 参考。

    读取 `_analysis_index.json` 中每只股票的上次深研价与价格印象，结合当日
    `02_basic_snapshot_payload.json` 的最新价，计算变化百分比，按绝对值降序输出
    到 `output_dir` 下的 `price_delta_report.md`。
    """
    index_path: Optional[Path] = None
    # 回测场景下 analysis_index_ref 明确指向实验目录内的冻结索引；该索引不存在时
    # 必须按冷启动处理，严禁回退到日常共享索引，否则会读到未来日期的深研记录。
    candidates = [analysis_index_ref] if analysis_index_ref else [DEFAULT_ANALYSIS_INDEX_REF]
    for candidate in candidates:
        if not candidate:
            continue
        p = Path(candidate)
        if not p.is_absolute():
            p = PROJECT_ROOT / p
        if p.exists():
            index_path = p
            break
    if index_path is None:
        index: Dict[str, Any] = {}
    else:
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                index = json.load(f)
        except (json.JSONDecodeError, OSError):
            index = {}

    book_index = index.get(book_type, {}) or {}
    stocks = snapshot_payload.get("stocks", {}) or {}

    rows: List[Dict[str, Any]] = []
    for key, entry in book_index.items():
        if not isinstance(entry, dict):
            continue
        symbol = analysis_entry_symbol(key, entry)
        last = entry.get("last_deep_analysis_price")
        if not isinstance(last, (int, float)) or float(last) <= 0:
            continue
        cur_fields = stocks.get(symbol, {}) or {}
        cur_price = cur_fields.get("latest_price")
        if not isinstance(cur_price, (int, float)):
            continue
        last_f = float(last)
        cur_f = float(cur_price)
        chg_pct = (cur_f - last_f) / last_f * 100.0
        rows.append(
            {
                "symbol": symbol,
                "stock_name": cur_fields.get("stock_name") or symbol,
                "deep_analysis_date": entry.get("deep_analysis_date", ""),
                "last_price": last_f,
                "cur_price": cur_f,
                "chg_pct": chg_pct,
                "price_impression": entry.get("price_impression", ""),
            }
        )

    rows.sort(key=lambda r: abs(r["chg_pct"]), reverse=True)

    lines = [
        "# 价格变化报告（当前价 vs 上次深研价）",
        "",
        f"- 分析日期：{run_date}",
        "- 数据源：`_analysis_index.json`（上次深研价）＋ `02_basic_snapshot_payload.json`（当日最新价）",
        "",
        "按变化绝对值降序排列。主 Agent 选 P0 时应重点复核：变化幅度超过约 4% 的股票、"
        "上次印象为「合理偏低估 / 偏低估 / 明显低估」的候选，以及持仓股。",
        "",
        "| 股票 | 上次日期 | 上次价 | 当前价 | 变化% | 上次印象 |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for r in rows:
        lines.append(
            f"| {r['stock_name']}（{r['symbol']}） | {r['deep_analysis_date']} | "
            f"{r['last_price']:.2f} | {r['cur_price']:.2f} | "
            f"{r['chg_pct']:+.1f}% | {r['price_impression']} |"
        )
    if not rows:
        lines.append("| （无可用上次深研价记录） | - | - | - | - | - |")

    # 快照里存在、但分析索引中没有有效记录的标的（新入池或从未深研）单独列出，
    # 避免它们因为「没有上次深研价」而从价格变化报告中消失、进而漏掉 P0。
    covered = {r["symbol"] for r in rows}
    uncovered: List[Dict[str, Any]] = []
    for symbol, fields in stocks.items():
        if symbol in covered or not isinstance(fields, dict):
            continue
        cur_price = fields.get("latest_price")
        if not isinstance(cur_price, (int, float)):
            continue
        uncovered.append(
            {
                "symbol": symbol,
                "stock_name": fields.get("stock_name") or symbol,
                "cur_price": float(cur_price),
            }
        )
    if uncovered:
        uncovered.sort(key=lambda r: r["symbol"])
        lines += [
            "",
            "## 无历史深研记录的标的（首次深研候选）",
            "",
            "这些标的在当前账本的分析索引中没有有效的上次深研价，按规则视作「从未深度分析」，"
            "应按 P0 规则第 1 条（新入池）或兜底条款直接纳入复核。",
            "",
            "| 股票 | 当前价 | 上次印象 |",
            "| --- | ---: | --- |",
        ]
        for r in uncovered:
            lines.append(f"| {r['stock_name']}（{r['symbol']}） | {r['cur_price']:.2f} | 无 |")

    target = Path(output_dir) / PRICE_DELTA_REPORT_FILENAME
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


def build_snapshot_payload(
    run_date: str,
    symbols: List[str],
    *,
    max_workers: int = 1,
    source_data_root: str | Path | None = None,
    backtest_read_only: bool = False,
) -> Dict[str, Any]:
    if backtest_read_only and any(symbol.endswith(".US") for symbol in symbols):
        from services.backtest.prices import quote_date_as_of

        stocks: Dict[str, Any] = {}
        errors: Dict[str, Any] = {}
        payload: Dict[str, Any] = {}
        for group in ([s for s in symbols if not s.endswith(".US")],
                      [s for s in symbols if s.endswith(".US")]):
            if not group:
                continue
            quote_date = quote_date_as_of(group[0], run_date) if group[0].endswith(".US") else run_date
            part = build_basic_snapshot(group, quote_date, base_dir=source_data_root,
                                        max_workers=max_workers, backtest_read_only=True)
            payload.update(part)
            for symbol, row in part.get("stocks", {}).items():
                row["quote_date"] = quote_date_as_of(symbol, run_date)
            stocks.update(part.get("stocks", {}))
            errors.update(part.get("errors", {}))
        payload.update(timestamp=run_date, stocks=stocks, stocks_count=len(stocks))
        if errors:
            payload["errors"] = errors
        else:
            payload.pop("errors", None)
        return payload
    return build_basic_snapshot(
        symbols,
        run_date,
        base_dir=source_data_root,
        max_workers=max_workers,
        backtest_read_only=backtest_read_only,
    )


def write_agent_input_bundle(
    run_date: str,
    output_dir: str | Path,
    *,
    symbols: List[str],
    book_type: str = "fixed_tracked",
    signature: str = "",
    prompt_config: str | Path | None = None,
    snapshot_payload: Dict[str, Any] | None = None,
    max_workers: int = 1,
    prompt_context_override: Optional[Dict[str, str]] = None,
    source_data_root: str | Path | None = None,
    backtest_read_only: bool = False,
) -> None:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    resolved_signature = resolve_signature(signature)
    resolved_prompt_config = Path(prompt_config) if prompt_config else DEFAULT_PROMPT_CONFIG
    prompt_context = prompt_context_override or get_skill_prompt_context(
        run_date,
        resolved_signature,
        stock_codes=symbols,
    )

    # 回测场景下，主 Agent 必须读取实验目录内的冻结分析索引
    # （由 merge_subagent_decisions.py 按 --base-dir 写入），而不是日常共享索引，
    # 否则会读到未来日期的记录，造成时间穿越。
    analysis_index_ref: Optional[str] = None
    if backtest_read_only:
        skill_runs_root = target_dir.parent.parent
        backtest_index = skill_runs_root / "_analysis_index.json"
        try:
            analysis_index_ref = str(backtest_index.relative_to(PROJECT_ROOT).as_posix())
        except ValueError:
            analysis_index_ref = str(backtest_index.resolve())

    snapshot_payload = snapshot_payload or build_snapshot_payload(
        run_date,
        symbols,
        max_workers=max_workers,
        source_data_root=source_data_root,
        backtest_read_only=backtest_read_only,
    )
    (target_dir / "02_basic_snapshot_payload.json").write_text(
        json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    write_price_delta_report(
        run_date,
        target_dir,
        snapshot_payload,
        book_type=book_type,
        analysis_index_ref=analysis_index_ref,
    )
    write_p0_event_report(run_date, target_dir, symbols, source_data_root=source_data_root)

    agent_input = build_agent_input(
        run_date,
        resolved_signature,
        resolved_prompt_config,
        target_dir,
        stock_codes=symbols,
        book_type=book_type,
        prompt_context=prompt_context,
        analysis_index_ref=analysis_index_ref,
    )
    # 回测场景：把 main_policy 正文中写死的共享索引路径也替换为实验目录冻结索引，
    # 避免主 Agent 按 Step 3 正文去读未来日期的日常索引造成时间穿越。
    if analysis_index_ref and analysis_index_ref != DEFAULT_ANALYSIS_INDEX_REF:
        agent_input = agent_input.replace(
            DEFAULT_ANALYSIS_INDEX_REF, analysis_index_ref
        )
    if backtest_read_only:
        snapshot_ref = target_dir.parents[2] / "agent_data" / resolved_signature / "latest_decision_snapshot.json"
        agent_input = agent_input.replace(
            "data/agent_data/book-fixed_tracked/latest_decision_snapshot.json",
            str(snapshot_ref),
        )
    (target_dir / "03_agent_input.md").write_text(agent_input, encoding="utf-8")
    if book_type in {"fixed_tracked", "us_tracked"}:
        stock_analysis_input = build_stock_analysis_input(
            run_date,
            resolved_signature,
            stock_codes=symbols,
            prompt_context=prompt_context,
        )
        (target_dir / "03_stock_analysis_input.md").write_text(
            stock_analysis_input,
            encoding="utf-8",
        )
