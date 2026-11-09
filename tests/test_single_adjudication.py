from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from services.trading.debate_pipeline import (
    atomic_write_json, prepare_debate_directories, write_review_context,
    validate_debate_artifacts,
)

SYMBOL = '300613.SZ'
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _review(tmp_path, *, shares=100, action='HOLD', quantity=0):
    book = tmp_path / '2026-11-09' / 'fixed_tracked'
    book.mkdir(parents=True)
    for name in ('01_global_context.md', '02_basic_snapshot_payload.json', '03_stock_analysis_input.md'):
        (book / name).write_text('当日输入', encoding='utf-8')
    research = book / '04_stock_research' / f'富瀚微_{SYMBOL}_2026-11-09_research.md'
    research.parent.mkdir()
    research.write_text('当日研究', encoding='utf-8')
    root = prepare_debate_directories(book, SYMBOL)
    for side in ('bull', 'bear'):
        atomic_write_json(root / 'advocates' / side / 'opening.json', {'arguments': [f'{side} 独立核验事实']})
    write_review_context(book, SYMBOL, position_shares=shares)
    verdict = json.loads((PROJECT_ROOT / 'configs/prompt_flow/fixed_tracked/stock_decision.example.json').read_text())
    verdict.update(symbol=SYMBOL, stock_name='富瀚微', action_type=action, action_num=quantity,
                   current_position_pct='5%', sizing_reason='当日真实持仓和可用现金的数量判断。')
    atomic_write_json(root / 'final' / 'stock_verdict.json', verdict)
    return book, root


def test_current_single_workflow_validates_without_optional_rebuttal(tmp_path):
    book, root = _review(tmp_path)
    assert validate_debate_artifacts(book, SYMBOL, require_verdict=True) == []
    assert {path.name for path in root.iterdir() if path.is_dir()} == {'advocates', 'adjudicator', 'final'}


@pytest.mark.parametrize('action,shares,quantity', [('BUY', 0, 100), ('SELL', 100, 100), ('HOLD', 100, 0), ('FLAT', 0, 0)])
def test_single_all_legal_action_semantics(tmp_path, action, shares, quantity):
    book, root = _review(tmp_path, shares=shares, action=action, quantity=quantity)
    assert validate_debate_artifacts(book, SYMBOL, require_verdict=True) == []


@pytest.mark.parametrize('action,shares,quantity,expected', [
    ('SELL', 100, 200, '超过当前持仓'), ('HOLD', 0, 0, '持仓不兼容'),
    ('FLAT', 100, 0, '持仓不兼容'), ('BUY', 0, 1, '100的整数倍'),
])
def test_single_rejects_illegal_position_and_lot(tmp_path, action, shares, quantity, expected):
    book, root = _review(tmp_path, shares=shares, action=action, quantity=quantity)
    assert any(expected in error for error in validate_debate_artifacts(book, SYMBOL, require_verdict=True))


def test_single_unresolved_question_blocks_validation(tmp_path):
    book, root = _review(tmp_path)
    atomic_write_json(root / 'adjudicator/review_request.json', {'questions': [{'id': 'q1', 'question': '分母是否正确？'}]})
    assert validate_debate_artifacts(book, SYMBOL, require_verdict=True)
    atomic_write_json(root / 'adjudicator/review_resolution.json', {'resolutions': [{'id': 'q2', 'conclusion': '已验证'}]})
    assert any('尚未逐项解决' in error for error in validate_debate_artifacts(book, SYMBOL, require_verdict=True))
    atomic_write_json(root / 'adjudicator/review_resolution.json', {'resolutions': [{'id': 'q1', 'conclusion': '已回查原文并重算TTM'}]})
    assert validate_debate_artifacts(book, SYMBOL, require_verdict=True) == []


def test_merge_cli_checks_current_chain_and_does_not_skip_incomplete_stock(tmp_path):
    book, root = _review(tmp_path)
    command = [sys.executable, str(PROJECT_ROOT / 'scripts/merge_subagent_decisions.py'),
               '--date', '2026-11-09', '--book-type', 'fixed_tracked', '--base-dir', str(tmp_path)]
    success = subprocess.run(command, capture_output=True, text=True)
    assert success.returncode == 0, success.stdout + success.stderr
    assert json.loads((book / '05_decision.json').read_text())['stock_decisions'][0]['symbol'] == SYMBOL
    before = (book / '05_decision.json').read_bytes()
    prepare_debate_directories(book, '000001.SZ')
    blocked = subprocess.run(command, capture_output=True, text=True)
    assert blocked.returncode == 1
    assert (book / '05_decision.json').read_bytes() == before


def test_single_invalid_verdict_does_not_create_05(tmp_path):
    book, root = _review(tmp_path, action='SELL', quantity=200)
    command = [sys.executable, str(PROJECT_ROOT / 'scripts/merge_subagent_decisions.py'),
               '--date', '2026-11-09', '--base-dir', str(tmp_path)]
    failed = subprocess.run(command, capture_output=True, text=True)
    assert failed.returncode == 1
    assert not (book / '05_decision.json').exists()
