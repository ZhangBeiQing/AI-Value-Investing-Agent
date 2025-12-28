"""
Custom LangChain chat model wrapper for DeepSeek reasoning models.

DeepSeek 的 reasoning 系列（如 deepseek-reasoner）在多轮对话时要求：
1. assistant 历史消息必须包含 `reasoning_content` 字段；
2. 每次响应也会返回 `reasoning_content`，调用方需要保存下来供下一轮使用。

LangChain 默认的 ChatOpenAI 不会回传/回放该字段，本包装器在两处补齐：
- 在构造请求 payload 时，如果历史 AIMessage 中存在 `additional_kwargs["reasoning_content"]`，
  则为对应的 assistant 消息写入 `reasoning_content`；
- 在解析响应时，把返回的 reasoning_content 写入 AIMessage 的 additional_kwargs，
  以便后续轮次继续使用。
"""

from __future__ import annotations

import os
from typing import Any, List

from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatResult
from langchain_openai import ChatOpenAI


def _normalize_api_base(base_url: str | None) -> str:
    """Normalize DeepSeek base URL to include /v1 suffix."""
    if not base_url:
        return "https://api.deepseek.com/v1"
    cleaned = base_url.strip().rstrip("/")
    if not cleaned.endswith("/v1"):
        cleaned = f"{cleaned}/v1"
    return cleaned


class DeepseekReasonerWrapper(ChatOpenAI):
    """ChatOpenAI 子类，增加 reasoning_content 读写支持。"""

    def __init__(
        self,
        *,
        model: str = "deepseek-reasoner",
        api_key: str | None = None,
        base_url: str | None = None,
        **kwargs: Any,
    ) -> None:
        resolved_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not resolved_key:
            raise ValueError("缺少 DeepSeek API Key，请设置 DEEPSEEK_API_KEY 或在初始化时传入 api_key。")
        resolved_base = _normalize_api_base(base_url or os.getenv("DEEPSEEK_API_BASE"))
        super().__init__(
            model=model,
            api_key=resolved_key,
            base_url=resolved_base,
            **kwargs,
        )

    @staticmethod
    def _extract_reasoning_from_message(message: BaseMessage) -> str | None:
        if isinstance(message, AIMessage):
            reasoning = message.additional_kwargs.get("reasoning_content")
            if isinstance(reasoning, str) and reasoning.strip():
                return reasoning
        return None

    def _get_request_payload(  # type: ignore[override]
        self,
        input_: Any,
        *,
        stop: List[str] | None = None,
        **kwargs: Any,
    ) -> dict:
        messages = self._convert_input(input_).to_messages()
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        if "messages" in payload:
            for idx, payload_msg in enumerate(payload["messages"]):
                if idx >= len(messages):
                    break
                reasoning = self._extract_reasoning_from_message(messages[idx])
                if reasoning:
                    payload_msg["reasoning_content"] = reasoning
        return payload

    def _create_chat_result(  # type: ignore[override]
        self,
        response: dict | Any,
        generation_info: dict | None = None,
    ) -> ChatResult:
        response_dict = response if isinstance(response, dict) else response.model_dump()
        choices = response_dict.get("choices", [])
        result = super()._create_chat_result(response, generation_info)
        for generation, choice in zip(result.generations, choices):
            if not isinstance(generation.message, AIMessage):
                continue
            reasoning = (
                choice.get("message", {}).get("reasoning_content")
                if isinstance(choice, dict)
                else None
            )
            if reasoning:
                generation.message.additional_kwargs["reasoning_content"] = reasoning
        return result
