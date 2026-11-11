"""Recover HK statement publication dates from the existing disclosure index."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from commons import SymbolInfo, get_stock_data_dir
from core.logging import get_logger


LOGGER = get_logger("HKDisclosureDates")


def load_hk_financial_notice_dates(
    symbol: SymbolInfo, *, base_dir: str | Path | None = None,
) -> dict[str, datetime]:
    """Map report periods to proven release dates; unknown dates stay unknown."""
    path = get_stock_data_dir(symbol, base_dir=base_dir) / "disclosures" / "index.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        LOGGER.warning("读取港股财报披露日期失败: symbol=%s error=%s", symbol.symbol, exc)
        return {}
    entries = payload.values() if isinstance(payload, dict) else payload
    result: dict[str, datetime] = {}
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("is_financial_report"):
            continue
        title = re.sub(r"\s+", "", str(entry.get("title") or ""))
        if any(word in title for word in ("预告", "預告", "指引", "经营数据", "經營數據", "摘要", "持续督导", "保荐", "董事会", "董事會", "更正", "修订")):
            continue
        if not any(word in title for word in ("业绩", "業績", "年度报告", "年度報告", "中期报告", "中期報告", "季度报告", "季度報告")):
            continue
        match = re.search(r"(20\d{2})年(\d{1,2})月(\d{1,2})日", title)
        period = None
        if match:
            try:
                period = datetime(*(int(part) for part in match.groups()))
            except ValueError:
                continue
        else:
            year = re.search(r"(20\d{2})年?", title)
            if year:
                month = next((month for words, month in (
                    (("第一季度", "一季度"), 3), (("中期", "半年度", "半年"), 6),
                    (("第三季度", "三季度"), 9), (("年度报告", "年度報告", "全年", "年度业绩", "年度業績"), 12),
                ) if any(word in title for word in words)), None)
                if month:
                    period = datetime(int(year.group(1)), month, 31 if month in (3, 12) else 30)
        if period is None:
            continue
        try:
            released = datetime.fromisoformat(str(entry.get("date") or "")[:10])
        except ValueError:
            continue
        if released.date() < period.date():
            continue
        key = period.strftime("%Y%m%d")
        if key not in result or released < result[key]:
            result[key] = released
    return result
