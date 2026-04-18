"""System prompt service wrapper."""

from __future__ import annotations

import os
from pathlib import Path

from typing import Optional, Sequence

from prompts.agent_prompt import get_agent_system_prompt


def get_skill_system_prompt(
    run_date: str,
    signature: str,
    prompt_config: str | Path,
    *,
    stock_codes: Optional[Sequence[str]] = None,
    stock_pool_block_override: Optional[str] = None,
) -> str:
    os.environ["PROMPT_FLOW_CONFIG"] = str(Path(prompt_config).resolve())
    return get_agent_system_prompt(
        run_date,
        signature,
        stock_codes=list(stock_codes) if stock_codes else None,
        stock_pool_block_override=stock_pool_block_override,
    )


__all__ = ["get_skill_system_prompt"]
