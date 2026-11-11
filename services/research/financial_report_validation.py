"""Distinguish financial research from disclosure text and enforce knowledge dates."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from pathlib import Path


def is_raw_financial_report(content: str) -> bool:
    """Recognize an official report, without rejecting research quoting a disclosure."""
    opening = content[:4000]
    return (
        bool(re.search(r"(?:本公司|公司董事会).{0,80}(?:董事|保证)", opening, re.S))
        and "主要财务数据" in content
        and bool(re.search(r"(?:季度|年度|半年度)报告", opening))
        and not re.search(r"核心结论|估值分析|基本面深度研究|深度分析", opening)
    )


def summary_file_issue(path: Path) -> str | None:
    if not path.is_file():
        return "报告文件不存在"
    content = path.read_text(encoding="utf-8", errors="replace")
    if len("".join(content.split())) < 80:
        return "报告为空或疑似占位"
    if is_raw_financial_report(content):
        return "财报原文不是基本面研究总结"
    return None


def report_knowledge_cutoff(path: Path) -> date | None:
    """Read explicit research metadata; publication filenames are not knowledge dates.

    An unknown cutoff requires historical review. Dates inside forecast scenarios
    are deliberately not interpreted as publication metadata.
    """
    content = path.read_text(encoding="utf-8", errors="replace")
    prefix = content[:1800]
    date_pattern = r"(20\d{2})\s*(?:年|[-/])\s*(\d{1,2})\s*(?:月|[-/])\s*(\d{1,2})"
    # A financial-period end in a report heading is not a research knowledge date.
    # Keep any separate explicit cutoff on the same line available for parsing.
    prefix_lines = []
    for line in prefix.splitlines():
        if re.match(r"^\s*#{1,6}\s", line) and re.search(r"中期|半年度|季度|年度|财报", line):
            line = re.sub(
                r"截至\s*" + date_pattern + r"\s*日?(?=\s*[）)])",
                "报告期末",
                line,
            )
        prefix_lines.append(line)
    prefix = "\n".join(prefix_lines)
    cutoff_label = (
        r"(?:分析日期|信息截止(?:日|时间)?|可知信息(?:限于|截止)|"
        r"知识截止(?:日)?|数据截止(?:日)?|严格截止|截至)"
    )
    patterns = (
        # Do not attach a label to a date in a subsequent excluded-source clause.
        cutoff_label + r"[^\d\n。，；;]{0,20}" + date_pattern,
        r"(?:严格)?以\s*" + date_pattern + r"\s*日?\s*为\s*(?:信息|知识|数据)截止(?:日|时间)?",
    )
    dates = []
    for pattern in patterns:
        for match in re.finditer(pattern, prefix):
            try:
                dates.append(date(*(int(part) for part in match.groups())))
            except ValueError:
                continue
    # A source-bound brief may provide metadata for an older report lacking a header.
    brief = path.with_suffix(".daily.json")
    if brief.exists():
        try:
            payload = json.loads(brief.read_text(encoding="utf-8"))
            if payload.get("source_sha256") == hashlib.sha256(path.read_bytes()).hexdigest():
                dates.append(date.fromisoformat(payload["knowledge_cutoff"]))
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            pass
    return max(dates) if dates else None


def historical_summary_issue(path: Path, as_of_date: str) -> str | None:
    issue = summary_file_issue(path)
    if issue:
        return issue
    cutoff = report_knowledge_cutoff(path)
    if cutoff is None:
        return "无法确认研究知识截止日，不能仅凭财报公告日文件名复用"
    if cutoff > date.fromisoformat(as_of_date):
        return f"研究知识截止日 {cutoff} 晚于分析日 {as_of_date}"
    return None
