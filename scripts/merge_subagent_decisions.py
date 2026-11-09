#!/usr/bin/env python3
"""Merge validated single-adjudicator verdicts after user confirmation."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.trading.analysis_index import update_analysis_index
from services.trading.debate_pipeline import atomic_write_json, debate_symbol_dir, validate_debate_artifacts


def find_debate_verdict_files(debate_dir: Path) -> list[Path]:
    return sorted(debate_dir.glob('*/final/stock_verdict.json'))


def main() -> int:
    parser = argparse.ArgumentParser(description='人工确认后合并单裁决结果为05')
    parser.add_argument('--date', required=True)
    parser.add_argument('--book-type', default='fixed_tracked', choices=('fixed_tracked', 'us_tracked'))
    parser.add_argument('--base-dir', default=str(PROJECT_ROOT / 'data/skill_runs'))
    args = parser.parse_args()
    book_dir = Path(args.base_dir) / args.date / args.book_type
    if not book_dir.is_dir():
        parser.error(f'账本目录不存在: {book_dir}')
    debate_dir = book_dir / 'debate'
    if not debate_dir.is_dir():
        print('无单股裁决，无需合并')
        return 0
    entries = []
    seen = set()
    errors = []
    for directory in sorted(debate_dir.iterdir()):
        if not directory.is_dir():
            continue
        verdict_path = directory / 'final/stock_verdict.json'
        if not verdict_path.is_file():
            errors.append(f'{directory}: 缺少最终 verdict')
            continue
        try:
            entry = json.loads(verdict_path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc:
            errors.append(f'{verdict_path}: {exc}')
            continue
        if not isinstance(entry, dict) or not isinstance(entry.get('symbol'), str) or not entry['symbol'].strip():
            errors.append(f'{verdict_path}: 缺少合法 symbol')
            continue
        symbol = entry['symbol']
        if symbol in seen:
            errors.append(f'{symbol}: 存在重复最终裁决')
        seen.add(symbol)
        if directory.resolve() != debate_symbol_dir(book_dir, symbol).resolve():
            errors.append(f'{directory}: 股票与目录不匹配')
        errors.extend(validate_debate_artifacts(book_dir, symbol, require_verdict=True))
        entries.append(entry)
    if errors:
        print('单裁决链路校验失败，未写入05:\n' + '\n'.join(f'- {item}' for item in errors))
        return 1
    if not entries:
        print('无单股裁决，无需合并')
        return 0
    decision_path = book_dir / '05_decision.json'
    baseline = json.loads(decision_path.read_text(encoding='utf-8')) if decision_path.exists() else {
        'stock_decisions': [], 'system_risk_notes': [], 'system_focus_items': [],
    }
    decisions = {entry['symbol']: entry for entry in baseline.get('stock_decisions', [])}
    decisions.update({entry['symbol']: entry for entry in entries})
    baseline.update(summary_date=args.date, stock_decisions=list(decisions.values()))
    atomic_write_json(decision_path, baseline)
    update_analysis_index(baseline, args.book_type, args.date, Path(args.base_dir))
    print(f'已合并 {len(entries)} 份单股裁决: {decision_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
