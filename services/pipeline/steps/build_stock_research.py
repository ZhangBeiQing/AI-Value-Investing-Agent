"""Step 3: build stock research packages."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from datetime import datetime
from pathlib import Path
import threading
from typing import Any, Iterable, List, Mapping, Optional

from core.logging import init_component_logger
from services.research.financial_report import get_financial_report_summary
from services.research.news_summary import search_stock_news
from services.research.stock_analysis import analyze_stock_dynamics_and_valuation
from services.trading.trade_summary import get_historical_context
from utlity import ensure_stock_subdir, get_stock_data_dir, parse_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ARTIFACT_CACHE_ROOT = PROJECT_ROOT / "data" / "research_artifact_cache"
RESEARCH_ARTIFACT_SCHEMA_VERSION = 1
LOGGER = init_component_logger(
    "BuildStockResearch",
    group="services/pipeline",
    filename_prefix="build_stock_research",
)
_ARTIFACT_LOCKS: dict[str, threading.Lock] = {}
_ARTIFACT_LOCKS_GUARD = threading.Lock()


def _json_block(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _format_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return _json_block(value)
    return str(value)


def _format_news_item(item: dict) -> List[str]:
    title = item.get("title", "未命名")
    dt = item.get("datetime", "")
    category = item.get("category", "")
    impact = item.get("impact_level", "")
    sentiment = item.get("sentiment", "")

    header_parts = []
    if category:
        header_parts.append(f"category: {category}")
    if impact:
        header_parts.append(f"impact: {impact}")
    if sentiment:
        header_parts.append(f"sentiment: {sentiment}")
    header_suffix = (" [" + " / ".join(header_parts) + "]") if header_parts else ""

    lines = [f"- **{title}** ({dt}){header_suffix}"]
    used = {"title", "datetime", "category", "impact_level", "sentiment"}
    preferred_order = [
        "summary",
        "validity_period",
        "audit_analysis",
        "financial_implication",
        "price_driver",
        "risk_warning",
        "source",
        "url",
        "link",
    ]
    for key in preferred_order:
        if key in item and item.get(key) not in (None, ""):
            lines.append(f"  - {key}: {_format_scalar(item.get(key))}")
            used.add(key)

    for key in sorted(k for k in item.keys() if k not in used):
        value = item.get(key)
        if value not in (None, ""):
            lines.append(f"  - {key}: {_format_scalar(value)}")

    return lines


def _format_history_entry(entry: Mapping[str, Any]) -> List[str]:
    start_date = entry.get("start_date") or "未知"
    end_date = entry.get("end_date") or "未知"
    duration_days = entry.get("duration_days")
    action_type = entry.get("action_type") or "未知"
    header = f"- 时间区间: {start_date} -> {end_date}"
    details = [f"  - action_type: {action_type}"]
    if duration_days not in (None, ""):
        details.append(f"  - duration_days: {duration_days}")

    preferred_fields = [
        "scan",
        "analysis_type",
        "history_anchor",
        "allow_reanchor_today",
        "forecast_reliability",
        "valuation_mode",
        "catalyst_and_momentum",
        "trading_mode",
        "key_facts",
        "inferences",
        "valuation_conclusion",
        "risk_reward_setup",
        "motion",
        "court",
        "recommended_action",
        "action_num",
        "price_target",
        "stop_loss",
        "key_risks",
        "next_day_watchlist",
        "confidence_score",
    ]
    for field in preferred_fields:
        value = entry.get(field)
        if value not in (None, "", [], {}):
            details.append(f"  - {field}: {_format_scalar(value)}")

    return [header, *details]


def _artifact_lock(cache_path: Path) -> threading.Lock:
    key = str(cache_path.resolve())
    with _ARTIFACT_LOCKS_GUARD:
        lock = _ARTIFACT_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _ARTIFACT_LOCKS[key] = lock
        return lock


def _artifact_cache_path(symbol: str, run_date: str) -> Path:
    symbol_info = parse_symbol(symbol)
    stock_root = get_stock_data_dir(symbol_info)
    cache_dir = RESEARCH_ARTIFACT_CACHE_ROOT / run_date
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{stock_root.name}_{run_date}_artifact.json"


def _path_mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return 0


def _directory_fingerprint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "entry_count": 0,
            "latest_mtime_ns": 0,
        }

    latest_mtime_ns = _path_mtime_ns(path)
    entry_count = 0
    for child in path.iterdir():
        if child.is_file():
            entry_count += 1
            latest_mtime_ns = max(latest_mtime_ns, _path_mtime_ns(child))
    return {
        "path": str(path),
        "exists": True,
        "entry_count": entry_count,
        "latest_mtime_ns": latest_mtime_ns,
    }


def _artifact_input_fingerprint(symbol: str) -> Dict[str, Any]:
    symbol_info = parse_symbol(symbol)
    stock_root = get_stock_data_dir(symbol_info)
    return {
        "stock_root": str(stock_root),
        "financial_reports": _directory_fingerprint(stock_root / "financial_reports"),
        "forecast": _directory_fingerprint(stock_root / "forecast"),
        "news_json_mtime_ns": _path_mtime_ns(stock_root / "news" / "news.json"),
        "news_audited_mtime_ns": _path_mtime_ns(stock_root / "news" / "news_audited.json"),
    }


def _load_base_artifact(cache_path: Path, symbol: str) -> Optional[Dict[str, Any]]:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("研究缓存读取失败，忽略并重建: %s (%s)", cache_path, exc)
        return None
    if payload.get("schema_version") != RESEARCH_ARTIFACT_SCHEMA_VERSION:
        return None
    if not isinstance(payload.get("price_payload"), dict):
        return None
    if not isinstance(payload.get("news_payload"), dict):
        return None
    if not isinstance(payload.get("financial_payload"), dict):
        return None
    if payload.get("input_fingerprint") != _artifact_input_fingerprint(symbol):
        LOGGER.info("研究缓存输入已变更，忽略并重建: %s", cache_path)
        return None
    return payload


def _build_base_artifact(symbol: str, run_date: str) -> Dict[str, Any]:
    with ThreadPoolExecutor(max_workers=3) as executor:
        price_future = executor.submit(analyze_stock_dynamics_and_valuation, symbol, run_date)
        news_future = executor.submit(search_stock_news, symbol, run_date)
        financial_future = executor.submit(get_financial_report_summary, symbol, run_date)

        price_payload = price_future.result()
        news_raw = news_future.result()
        financial_payload = financial_future.result()

    try:
        news_payload = json.loads(news_raw)
    except Exception:
        news_payload = {"raw_text": news_raw}

    return {
        "schema_version": RESEARCH_ARTIFACT_SCHEMA_VERSION,
        "symbol": symbol,
        "run_date": run_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "input_fingerprint": _artifact_input_fingerprint(symbol),
        "price_payload": price_payload,
        "news_payload": news_payload,
        "financial_payload": financial_payload,
    }


def _load_or_build_base_artifact(symbol: str, run_date: str) -> Dict[str, Any]:
    cache_path = _artifact_cache_path(symbol, run_date)
    lock = _artifact_lock(cache_path)
    with lock:
        cached = _load_base_artifact(cache_path, symbol)
        if cached is not None:
            LOGGER.info("复用研究缓存: %s", cache_path)
            return cached

        built = _build_base_artifact(symbol, run_date)
        cache_path.write_text(
            json.dumps(built, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        LOGGER.info("写入研究缓存: %s", cache_path)
        return built


def build_research_markdown(
    symbol: str,
    run_date: str,
    *,
    snapshot_payload: Mapping[str, Any] | None = None,
    signature: str = "",
    book_type: str = "fixed_tracked",
) -> str:
    symbol_info = parse_symbol(symbol)
    stock_name = symbol_info.stock_name or symbol_info.symbol
    _ = snapshot_payload
    base_artifact = _load_or_build_base_artifact(symbol_info.symbol, run_date)
    price_payload = base_artifact.get("price_payload") or {}
    news_payload = base_artifact.get("news_payload") or {}
    financial_payload = base_artifact.get("financial_payload") or {}
    historical_entries = get_historical_context(signature, symbol_info.symbol, 1) if signature else []

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# {stock_name}（{symbol_info.symbol}）研究包")
    lines.append("")
    lines.append(f"- 请求日期: {run_date}")
    lines.append(f"- 生成时间: {generated_at}")
    lines.append(f"- book_type: {book_type}")
    lines.append(f"- history_signature: {signature or '未指定'}")
    lines.append("")
    lines.append("## 1. 股票指标与估值")
    lines.append("")
    lines.append("### 1.1 Price Report JSON")
    lines.append("```json")
    lines.append(_json_block(price_payload.get("price_report")))
    lines.append("```")
    lines.append("")
    lines.append("### 1.2 Valuation Report (Markdown)")
    lines.append(price_payload.get("valuation_report") or "> 无估值数据或标的不支持。")
    if price_payload.get("valuation_unavailable_reason"):
        lines.extend(["", f"> 说明：{price_payload['valuation_unavailable_reason']}"])
    lines.append("")
    lines.append("## 2. 新闻与公告")
    lines.append("")
    news_items = news_payload.get("news_items") or []
    if news_items:
        for item in news_items:
            if isinstance(item, dict):
                lines.extend(_format_news_item(item))
            else:
                lines.append(f"- {item}")
    else:
        lines.append("> 未找到新闻或工具返回空结果。")
    diagnostics = news_payload.get("diagnostics") or []
    if diagnostics:
        lines.extend(["", "诊断信息："])
        lines.extend(f"- {entry}" for entry in diagnostics)
    lines.append("")
    lines.append("## 3. 财报摘要、机构一致预期与 Forecast")
    lines.append("")
    if financial_payload.get("error"):
        lines.append(f"> 获取失败：{financial_payload['error']}")
    else:
        lines.append((financial_payload.get("content") or "").strip() or "> 未获取到财报内容。")
        metadata = financial_payload.get("metadata")
        if metadata:
            lines.extend(["", "**财报元数据**", "```json", _json_block(metadata), "```"])
    lines.append("")
    lines.append("## 4. 最近一次交易日历史交易总结")
    lines.append("")
    if historical_entries:
        lines.append(
            "以下内容来自当前 signature 对应 `decision_summary.json` 中该股票最近一次交易日的合并总结，可供 subagent 直接继承历史锚点与上一轮庭审结论。"
        )
        lines.append("")
        for entry in historical_entries:
            lines.extend(_format_history_entry(entry))
    else:
        lines.append(f"> 未找到该股票在当前账本（{book_type}）下最近一次交易日的历史交易总结。")
    lines.append("")
    return "\n".join(lines)


def research_output_path(symbol: str, run_date: str, output_dir: str | Path) -> Path:
    symbol_info = parse_symbol(symbol)
    stock_root = get_stock_data_dir(symbol_info)
    ensure_stock_subdir(symbol_info, "financial_reports")
    ensure_stock_subdir(symbol_info, "forecast")
    filename = f"{stock_root.name}_{run_date}_research.md"
    target_dir = Path(output_dir) / "04_stock_research"
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / filename


def write_stock_research_bundle(
    run_date: str,
    output_dir: str | Path,
    symbols: Iterable[str] | None = None,
    *,
    snapshot_payload: Mapping[str, Any] | None = None,
    signature: str = "",
    book_type: str = "fixed_tracked",
    max_workers: int = 1,
) -> None:
    target_symbols = list(symbols) if symbols is not None else []

    def _build_and_write(symbol: str) -> None:
        content = build_research_markdown(
            symbol,
            run_date,
            snapshot_payload=snapshot_payload,
            signature=signature,
            book_type=book_type,
        )
        research_output_path(symbol, run_date, output_dir).write_text(content, encoding="utf-8")

    worker_count = max(1, min(int(max_workers or 1), len(target_symbols) or 1))
    if worker_count <= 1 or len(target_symbols) <= 1:
        for symbol in target_symbols:
            _build_and_write(symbol)
        return

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(_build_and_write, symbol): symbol for symbol in target_symbols}
        for future in as_completed(futures):
            future.result()
