"""System prompt service wrapper."""

from __future__ import annotations

import os
from pathlib import Path

from prompts.agent_prompt import get_agent_system_prompt


def get_skill_system_prompt(run_date: str, signature: str, prompt_config: str | Path) -> str:
    os.environ["PROMPT_FLOW_CONFIG"] = str(Path(prompt_config).resolve())
    return get_agent_system_prompt(run_date, signature)


__all__ = ["get_skill_system_prompt"]

