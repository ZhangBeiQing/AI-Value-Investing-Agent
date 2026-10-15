"""Prepare weekly scoring tasks and merge validated Agent outputs; no data fetching."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Iterable

from core.logging import get_logger
from services.ranking.scoring_contract import WEEKLY_SCORE_SCHEMA, load_score_file


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCORING_POLICY = PROJECT_ROOT / "configs/prompt_flow/fixed_tracked/weekly_scoring_policy.md"
LOGGER = get_logger("WeeklyScoring")
REFERENCE_WEIGHTS = {
    "market_sentiment": 0.20,
    "valuation": 0.20,
    "price_volume": 0.20,
    "news_expectation": 0.20,
    "fundamental": 0.20,
}
DIMENSION_LABELS = {
    "market_sentiment": "宏观市场情绪",
    "valuation": "估值",
    "price_volume": "量价",
    "news_expectation": "新闻与未来预期",
    "fundamental": "基本面",
}
SYMBOL_PATTERN = re.compile(r"(?:\d{6}\.(?:SH|SZ|BJ)|\d{5}\.HK)")
POOL_SOURCES = ("fixed_tracked", "universe")
FIXED_TRACKED_BOOK = "fixed_tracked"
UNIVERSE_BOOK = "universe"
UNIVERSE_FILE_NAME = "master_universe.json"


def _date(run_date: str) -> str:
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError("日期必须是 YYYY-MM-DD 格式")
    return run_date


def _root(base_dir: str | Path) -> Path:
    path = Path(base_dir).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()


def _symbols(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    for raw in values:
        if not isinstance(raw, str) or not SYMBOL_PATTERN.fullmatch(raw):
            raise ValueError(f"股票代码必须使用标准格式，例如 300476.SZ 或 00700.HK: {raw}")
        if raw not in result:
            result.append(raw)
    return result


def score_filename(stock_name: str, symbol: str) -> str:
    """Keep the company visible while excluding filesystem-reserved characters."""
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", stock_name).strip(" .")
    return f"{name or symbol}_{symbol}_score.json"


def _score_file_symbol(filename: str) -> str | None:
    match = re.search(r"(?:^|_)(\d{6}\.(?:SH|SZ|BJ)|\d{5}\.HK)_score\.json$", filename)
    return match.group(1) if match else None


def _output_dir(base_dir: str | Path, run_date: str, pool_source: str) -> Path:
    root = _root(base_dir) / "weekly_scoring" / run_date
    return root / UNIVERSE_BOOK if pool_source == UNIVERSE_BOOK else root


def _book_dir(input_root: Path, run_date: str, pool_source: str) -> Path:
    return input_root / "skill_runs" / run_date / pool_source


def _global_context_path(input_root: Path, run_date: str, pool_source: str) -> Path:
    book = _book_dir(input_root, run_date, pool_source)
    candidate = book / "01_global_context.md"
    if candidate.is_file():
        return candidate
    # 宏观上下文与账本无关，universe 池可直接复用当日 fixed_tracked 的宏观总结。
    return input_root / "skill_runs" / run_date / FIXED_TRACKED_BOOK / "01_global_context.md"


def _universe_symbols(input_root: Path) -> list[str]:
    universe_path = input_root / "universe" / UNIVERSE_FILE_NAME
    if not universe_path.is_file():
        raise FileNotFoundError(f"缺少全宇宙文件: {universe_path}")
    document = json.loads(universe_path.read_text(encoding="utf-8"))
    stocks = document.get("stocks") if isinstance(document, dict) else None
    if not isinstance(stocks, list):
        raise ValueError(f"master_universe.stocks 必须是列表: {universe_path}")
    symbols: list[str] = []
    for entry in stocks:
        symbol = entry.get("symbol") if isinstance(entry, dict) else entry
        if isinstance(symbol, str) and symbol:
            symbols.append(symbol)
    return symbols


def build_subagent_prompt(
    symbol: str,
    stock_name: str,
    *,
    run_date: str,
    research_path: Path,
    global_context_path: Path,
    output_path: Path,
) -> str:
    """Reference canonical rules rather than maintaining a second scoring policy."""
    return "\n".join([
        f"你担任 {symbol}（{stock_name}）的每周评分分析师，分析日为 {run_date}。",
        "完整读取以下文件；较长研究包须分段顺序读到末尾，禁止只靠关键词搜索或局部摘要：",
        f"1. 评分规则：{SCORING_POLICY}",
        f"2. 完整研究包（§1—§4）：{research_path}",
        f"3. 宏观上下文：{global_context_path}",
        f"4. 输出 JSON schema：{WEEKLY_SCORE_SCHEMA}",
        "按评分规则执行联网核验。资料发布时间不得晚于分析日，不得使用未来信息。",
        "五个维度分别输出 0—100 的整数与具体理由；规则以本地评分文件为准。",
        "composite_reason 位于 JSON 顶层，不输出 composite_score，综合分由合并脚本计算。",
        "理由中不包含 URL、Markdown 链接、脚注、括号来源引用或独立来源清单。",
        f"只允许写入：{output_path}",
        "不生成交易指令或仓位建议。完成后只返回输出文件路径。",
    ])


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def generate_scoring_tasks(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    input_base_dir: str | Path | None = None,
    symbols: Iterable[str] | None = None,
    pool_source: str = FIXED_TRACKED_BOOK,
) -> dict[str, Any]:
    """Freeze the day's pool and task paths; never prepare missing pipeline inputs."""
    run_date = _date(run_date)
    if pool_source not in POOL_SOURCES:
        raise ValueError(f"不支持的 pool_source: {pool_source}")
    input_root = _root(input_base_dir if input_base_dir is not None else base_dir)
    run_dir = input_root / "skill_runs" / run_date
    book_dir = _book_dir(input_root, run_date, pool_source)
    global_context = _global_context_path(input_root, run_date, pool_source)
    snapshot_path = book_dir / "02_basic_snapshot_payload.json"
    for path in (global_context, snapshot_path, SCORING_POLICY, WEEKLY_SCORE_SCHEMA):
        if not path.is_file() or not path.read_text(encoding="utf-8").strip():
            raise FileNotFoundError(f"缺少或为空的前置文件: {path}；请先准备该日期 run_daily_pipeline 产物")
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("stocks"), dict):
        raise ValueError(f"snapshot.stocks 必须是对象: {snapshot_path}")
    if str(snapshot.get("timestamp", ""))[:10] != run_date:
        raise ValueError("快照日期与分析日不一致，不得使用前一交易日的数据")
    stock_records = snapshot["stocks"]
    if pool_source == FIXED_TRACKED_BOOK:
        pool = list(stock_records)
        manifest_path = run_dir / "run_manifest.json"
        if manifest_path.exists():
            run_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if run_manifest.get("run_date") != run_date:
                raise ValueError("run_manifest 日期与分析日不一致")
            books = [book for book in run_manifest.get("books", []) if book.get("book_type") == "fixed_tracked"]
            if len(books) != 1 or not isinstance(books[0].get("symbols"), list):
                raise ValueError("run_manifest 必须包含一个有 symbols 列表的 fixed_tracked 账本")
            pool = books[0]["symbols"]
    else:
        pool = _universe_symbols(input_root)
    pool = _symbols(pool)
    selected = _symbols(symbols) if symbols is not None else pool
    outside = sorted(set(selected) - set(pool))
    if outside:
        raise ValueError(f"指定股票不属于该日 {pool_source} 池: {outside}")
    if not selected:
        raise ValueError(f"该日 {pool_source} 股票池为空")
    output_dir = _output_dir(base_dir, run_date, pool_source)
    tasks: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []
    for symbol in selected:
        record = stock_records.get(symbol)
        if not isinstance(record, dict):
            reason = "该股票缺少当日快照记录"
        else:
            matches = sorted((book_dir / "04_stock_research").glob(f"*_{symbol}_{run_date}_research.md"))
            if len(matches) != 1:
                reason = "缺少当日研究包" if not matches else "同一股票存在多个当日研究包"
            elif not matches[0].read_text(encoding="utf-8").strip():
                reason = "当日研究包为空"
            else:
                research = matches[0].resolve()
                stock_name = record.get("stock_name") or research.name.removesuffix(f"_{symbol}_{run_date}_research.md")
                output = output_dir / score_filename(stock_name, symbol)
                tasks.append({
                    "symbol": symbol,
                    "stock_name": stock_name,
                    "research_path": str(research),
                    "global_context_path": str(global_context),
                    "output_path": str(output),
                    "prompt": build_subagent_prompt(symbol, stock_name, run_date=run_date, research_path=research, global_context_path=global_context, output_path=output),
                })
                continue
        LOGGER.warning("跳过周度评分: symbol=%s reason=%s", symbol, reason)
        skipped.append({"symbol": symbol, "reason": reason})
    result = {
        "run_date": run_date,
        "book_type": pool_source,
        "pool_source": pool_source,
        "input_base_dir": str(input_root),
        "output_dir": str(output_dir),
        "expected_symbols": selected,
        "tasks": tasks,
        "skipped": skipped,
    }
    _save_json(output_dir / "scoring_manifest.json", result)
    LOGGER.info("周度评分派单已生成: date=%s pool=%s ready=%d skipped=%d", run_date, pool_source, len(tasks), len(skipped))
    return result


def collect_weekly_scores(
    run_date: str, *, base_dir: str | Path = "data", symbols: Iterable[str] | None = None,
    pool_source: str = FIXED_TRACKED_BOOK,
) -> dict[str, Any]:
    """Load only frozen tasks for this date and report errors without blocking valid stocks."""
    run_date = _date(run_date)
    if pool_source not in POOL_SOURCES:
        raise ValueError(f"不支持的 pool_source: {pool_source}")
    output_dir = _output_dir(base_dir, run_date, pool_source)
    manifest_path = output_dir / "scoring_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"缺少评分派单清单: {manifest_path}；请先运行生成阶段")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_source = manifest.get("pool_source", manifest.get("book_type"))
    if manifest.get("run_date") != run_date or manifest_source != pool_source:
        raise ValueError("评分派单清单的日期或账本不匹配")
    expected = _symbols(manifest["expected_symbols"])
    selected = _symbols(symbols) if symbols is not None else expected
    if set(selected) - set(expected):
        raise ValueError("指定股票不在评分派单清单中")
    task_symbols = {task["symbol"] for task in manifest["tasks"]}
    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    missing: list[str] = []
    skipped = [item for item in manifest["skipped"] if item["symbol"] in selected]
    score_files = sorted(output_dir.glob("*_score.json"))
    by_symbol: dict[str, list[Path]] = {}
    for path in score_files:
        file_symbol = _score_file_symbol(path.name)
        if file_symbol is not None:
            by_symbol.setdefault(file_symbol, []).append(path)
    for symbol in selected:
        if symbol not in task_symbols:
            continue
        matches = by_symbol.get(symbol, [])
        if not matches:
            missing.append(symbol)
            LOGGER.warning("缺少周度评分结果: symbol=%s directory=%s", symbol, output_dir)
            continue
        if len(matches) > 1:
            errors = [f"同一股票存在多个评分文件: {[path.name for path in matches]}"]
            invalid.append({"symbol": symbol, "errors": errors})
            LOGGER.warning("周度评分校验失败: symbol=%s errors=%s", symbol, errors)
            continue
        path = matches[0]
        entry, errors = load_score_file(path, expected_symbol=symbol)
        if errors:
            invalid.append({"symbol": symbol, "errors": errors})
            LOGGER.warning("周度评分校验失败: symbol=%s errors=%s", symbol, errors)
        else:
            valid.append(entry)
    unexpected = [path.name for path in score_files if _score_file_symbol(path.name) not in set(selected) & task_symbols]
    return {
        "run_date": run_date,
        "book_type": pool_source,
        "pool_source": pool_source,
        "expected_symbols": selected,
        "items": valid,
        "invalid": invalid,
        "missing_symbols": missing,
        "skipped": skipped,
        "ignored_files": unexpected,
    }


def _md(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def _render_ranking(result: dict[str, Any]) -> str:
    pool_label = result.get("pool_source", FIXED_TRACKED_BOOK)
    lines = [
        f"# {result['run_date']} {pool_label} 周度评分排名",
        "",
        f"应评 {len(result['expected_symbols'])} 只，成功 {len(result['items'])} 只；缺失 {len(result['missing_symbols'])} 只，格式错误 {len(result['invalid'])} 只，缺输入跳过 {len(result['skipped'])} 只。",
        "",
        "五维各占 20%，综合分由代码计算；排名用于全池观察与研究关注，不生成交易动作。",
        "",
        "| 排名 | 代码 | 名称 | 综合分 | 宏观 | 估值 | 量价 | 新闻与预期 | 基本面 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in result["items"]:
        scores = " | ".join(str(item["scores"][f"{dimension}_score"]) for dimension in REFERENCE_WEIGHTS)
        lines.append(f"| {item['rank']} | {item['symbol']} | {_md(item['stock_name'])} | {item['composite_score']:.1f} | {scores} |")
    for item in result["items"]:
        lines.extend(["", f"## {item['rank']}. {_md(item['stock_name'])}（{item['symbol']}）", "", item["composite_reason"].strip()])
        for dimension, label in DIMENSION_LABELS.items():
            lines.extend(["", f"### {label}：{item['scores'][f'{dimension}_score']}", "", item["scores"][f"{dimension}_reason"].strip()])
    if result["missing_symbols"] or result["invalid"] or result["skipped"]:
        lines.extend(["", "## 未完成股票", ""])
        lines.extend(f"- {symbol}：缺少评分文件。" for symbol in result["missing_symbols"])
        lines.extend(f"- {item['symbol']}：评分文件格式校验失败。" for item in result["invalid"])
        lines.extend(f"- {item['symbol']}：{item['reason']}。" for item in result["skipped"])
    return "\n".join(lines) + "\n"


def merge_weekly_scores(
    run_date: str, *, base_dir: str | Path = "data", symbols: Iterable[str] | None = None,
    pool_source: str = FIXED_TRACKED_BOOK,
) -> dict[str, Any]:
    result = collect_weekly_scores(run_date, base_dir=base_dir, symbols=symbols, pool_source=pool_source)
    for item in result["items"]:
        # Five integer scores divided by five is exact to one decimal place.
        item["composite_score"] = round(sum(item["scores"][f"{key}_score"] * weight for key, weight in REFERENCE_WEIGHTS.items()), 1)
    result["items"].sort(key=lambda item: (-item["composite_score"], item["symbol"]))
    for rank, item in enumerate(result["items"], 1):
        item["rank"] = rank
    result["weights"] = REFERENCE_WEIGHTS.copy()
    result["status"] = "empty" if not result["items"] else (
        "partial" if result["invalid"] or result["missing_symbols"] or result["skipped"] else "complete"
    )
    output_dir = _output_dir(base_dir, _date(run_date), pool_source)
    _save_json(output_dir / "weekly_scoring.json", result)
    (output_dir / "weekly_ranking.md").write_text(_render_ranking(result), encoding="utf-8")
    LOGGER.info("周度评分排名已合并: date=%s pool=%s status=%s valid=%d", run_date, pool_source, result["status"], len(result["items"]))
    return result
