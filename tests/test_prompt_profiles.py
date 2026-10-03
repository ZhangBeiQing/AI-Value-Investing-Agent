from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.prompting.agent_prompt import build_markdown_prompt
from services.pipeline.steps import build_agent_input


MAIN_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "main_policy.md"
)
INVESTMENT_POLICY = (
    PROJECT_ROOT
    / "configs"
    / "prompt_flow"
    / "fixed_tracked"
    / "investment_policy.md"
)
STOCK_POLICY = (
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


def _context() -> dict[str, str]:
    return {
        "date": "2026-07-31",
        "portfolio_value_amount": "500,000.00 元",
        "positions": '{"CASH": 500000}',
        "position_costs": "{}",
        "position_profit": "{}",
        "position_return_pct": "{}",
    }


def test_main_and_stock_prompt_profiles_have_disjoint_responsibilities() -> None:
    investment_prompt = build_markdown_prompt(
        INVESTMENT_POLICY,
        stock_pool_block="",
        context=_context(),
    )
    main_prompt = build_markdown_prompt(
        MAIN_POLICY,
        stock_pool_block="1. 300613.SZ 富瀚微",
        context=_context(),
    )
    stock_prompt = build_markdown_prompt(
        STOCK_POLICY,
        stock_pool_block="",
        context=_context(),
    )
    web_research_prompt = build_markdown_prompt(
        WEB_RESEARCH_POLICY,
        stock_pool_block="",
        context=_context(),
    )

    assert "建立 P0 队列" in main_prompt
    assert "Bull opening" not in main_prompt
    assert "stock_decision.schema.json" not in main_prompt

    assert "共同研究方法" in stock_prompt
    assert "建立 P0 队列" not in stock_prompt
    assert "暂停等待用户" not in stock_prompt
    assert "创建 subagent" not in stock_prompt
    assert "百家号" in web_research_prompt
    assert "不能进入任何事实" in web_research_prompt

    for placeholder in [
        "{date}",
        "{portfolio_value_amount}",
        "{positions}",
        "{position_costs}",
        "{position_profit}",
        "{position_return_pct}",
        "{stock_pool_block}",
    ]:
        assert placeholder not in main_prompt
        assert placeholder not in stock_prompt

    assert "买在基本面的右侧" in investment_prompt
    assert "分批建仓" in investment_prompt
    assert "建立 P0 队列" not in investment_prompt
    assert "Bull opening" not in investment_prompt


def test_write_agent_input_bundle_adds_fixed_stock_profile(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_prompt(
        run_date: str,
        signature: str,
        prompt_config: str | Path,
        **_: object,
    ) -> str:
        return f"rendered:{Path(prompt_config).name}:{run_date}:{signature}"

    monkeypatch.setattr(
        build_agent_input,
        "get_skill_system_prompt",
        fake_prompt,
    )
    monkeypatch.setattr(
        build_agent_input,
        "get_skill_prompt_context",
        lambda *_args, **_kwargs: _context(),
    )

    build_agent_input.write_agent_input_bundle(
        "2026-07-31",
        tmp_path,
        symbols=["300613.SZ"],
        book_type="fixed_tracked",
        signature="book-fixed_tracked",
        prompt_config=MAIN_POLICY,
        snapshot_payload={"stocks": {}},
    )

    main_input = (tmp_path / "03_agent_input.md").read_text(encoding="utf-8")
    stock_input = (
        tmp_path / "03_stock_analysis_input.md"
    ).read_text(encoding="utf-8")
    assert "rendered:investment_policy.md" in main_input
    assert "rendered:main_policy.md" in main_input
    assert "rendered:investment_policy.md" in stock_input
    assert "rendered:web_research_policy.md" in stock_input
    assert "rendered:stock_analysis_policy.md" in stock_input
    assert main_input.count("rendered:investment_policy.md") == 1
    assert stock_input.count("rendered:investment_policy.md") == 1
    assert stock_input.count("rendered:web_research_policy.md") == 1
    assert "```text" not in main_input
