"""Select a source-bound financial brief without silently truncating research."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from pathlib import Path

from commons.stock_utils import is_etf_symbol, parse_symbol
from core.logging import get_logger


LOGGER = get_logger("FinancialDailyBrief")
BRIEF_SECTIONS = (
    "业务与利润来源", "季度趋势与归因", "盈利质量与资产负债",
    "预期差与指引", "估值分母与计算", "未来盈利驱动与情景",
    "风险与反面证据", "验证条件与原文定位",
)
MAX_BRIEF_CHARACTERS = 6500


def brief_path(report_path: Path) -> Path:
    return report_path.with_suffix(".daily.json")


def load_daily_brief(
    report_path: Path, *, symbol: str, as_of_date: str,
) -> tuple[str | None, str]:
    """Return validated Markdown or an explicit reason to retain the full report.

    Hash/shape checks establish identity and freshness, not factual accuracy.
    A human/Author must still audit content against the report before publication.
    """
    target = brief_path(report_path)
    if not target.exists():
        return None, "missing"
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("摘要必须是 JSON 对象")
        expected = hashlib.sha256(report_path.read_bytes()).hexdigest()
        if payload.get("source_sha256") != expected:
            raise ValueError("原报告已变更或摘要绑定错误")
        if payload.get("schema_version") != 1 or payload.get("symbol") != symbol:
            raise ValueError("摘要版本或股票身份不匹配")
        if payload.get("report_filename") != report_path.name:
            raise ValueError("摘要报告身份不匹配")
        knowledge_date = date.fromisoformat(payload["knowledge_cutoff"])
        if knowledge_date > date.fromisoformat(as_of_date):
            raise ValueError("摘要知识截止日晚于分析日")
        content = payload["content"]
        if not isinstance(content, str):
            raise ValueError("摘要正文必须是字符串")
        size = len("".join(content.split()))
        if not 800 <= size <= MAX_BRIEF_CHARACTERS:
            raise ValueError("摘要必须在 800–6500 个非空白字符以内")
        headings = re.findall(r"^## (.+)$", content, re.MULTILINE)
        if headings != list(BRIEF_SECTIONS):
            raise ValueError("摘要缺少规定章节或顺序错误")
        for section in re.split(r"^## .+$", content, flags=re.MULTILINE)[1:]:
            if len("".join(section.split())) < 20:
                raise ValueError("摘要章节为空或疑似占位")
        if re.search(r"https?://|www\.|\[[^\]]+\]\([^)]*\)|\[\^", content):
            raise ValueError("摘要包含禁止的引用")
        if re.search(r"TODO|PLACEHOLDER|待补充|待完善|recommended_action|action_num|price_target|stop_loss", content, re.I):
            raise ValueError("摘要包含占位符或交易字段")
        return content.strip(), "valid"
    except (OSError, ValueError, KeyError, TypeError) as exc:
        LOGGER.warning("财报日常摘要不可用，保留全文: %s (%s)", target, exc)
        return None, f"invalid: {exc}"


def daily_financial_content(
    report_path: Path, full_content: str, *, symbol: str, as_of_date: str,
) -> tuple[str, dict]:
    content, status = load_daily_brief(report_path, symbol=symbol, as_of_date=as_of_date)
    mode = "daily_brief" if content is not None else "full_report_fallback"
    notice = (
        "本节是财报研究摘要，交易角色须完整读取完整版§15，其余章节按需回查。摘要中的历史价格、预期和估值不代表分析日最新值；"
        "当日口径以研究包行情、估值和新闻为准。买卖依赖的决定性财务数字须回查原文对应章节。"
        if content is not None else
        f"日常摘要不可用（{status}），本次保留完整财报；未自动截断。"
    )
    rendered = f"> {notice}\n> 完整财报本地路径：{report_path.resolve()}\n\n{content if content is not None else full_content}"
    return rendered, {"mode": mode, "brief_status": status, "full_report_path": str(report_path.resolve())}


def daily_brief_task(report_path: Path, *, symbol: str, as_of_date: str) -> dict | None:
    """Queue summary-only work for an existing completed report."""
    if is_etf_symbol(parse_symbol(symbol)):
        return None
    if not report_path.is_file():
        return None
    content, status = load_daily_brief(report_path, symbol=symbol, as_of_date=as_of_date)
    if content is not None:
        return None
    return {
        "symbol": symbol, "full_report_path": str(report_path),
        "daily_brief_path": str(brief_path(report_path)), "brief_status": status,
    }
