"""System prompt service wrapper."""

from __future__ import annotations

from pathlib import Path

from typing import Dict, Optional, Sequence

from prompts.agent_prompt import (
    build_agent_prompt_context,
    get_agent_system_prompt,
)


def get_skill_prompt_context(
    run_date: str,
    signature: str,
    *,
    stock_codes: Optional[Sequence[str]] = None,
) -> Dict[str, str]:
    return build_agent_prompt_context(
        run_date,
        signature,
        stock_codes=list(stock_codes) if stock_codes else None,
    )


def get_skill_system_prompt(
    run_date: str,
    signature: str,
    prompt_config: str | Path,
    *,
    stock_codes: Optional[Sequence[str]] = None,
    stock_pool_block_override: Optional[str] = None,
    prompt_context: Optional[Dict[str, str]] = None,
) -> str:
    return get_agent_system_prompt(
        run_date,
        signature,
        stock_codes=list(stock_codes) if stock_codes else None,
        stock_pool_block_override=stock_pool_block_override,
        prompt_config=Path(prompt_config).resolve(),
        prompt_context=prompt_context,
    )


__all__ = ["get_skill_prompt_context", "get_skill_system_prompt"]
