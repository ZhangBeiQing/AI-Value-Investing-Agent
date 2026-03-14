"""Conversation and reasoning extraction helpers."""

from __future__ import annotations


def _get_field(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _get_nested(obj, path, default=None):
    current = obj
    for key in path:
        if current is None:
            return default
        current = _get_field(current, key, None)
    return default if current is None else current


def _reasoning_details_to_text(reasoning_details) -> str:
    if reasoning_details is None:
        return ""
    if isinstance(reasoning_details, str):
        return reasoning_details.strip()
    if isinstance(reasoning_details, list):
        parts = []
        for item in reasoning_details:
            if isinstance(item, str):
                text = item.strip()
                if text:
                    parts.append(text)
                continue
            summary = _get_field(item, "summary")
            if isinstance(summary, list):
                summary_text = "\n".join(str(x).strip() for x in summary if str(x).strip())
            else:
                summary_text = str(summary).strip() if summary is not None else ""
            text = _get_field(item, "text")
            text_text = str(text).strip() if text is not None else ""
            merged = "\n".join(part for part in [summary_text, text_text] if part)
            if merged:
                parts.append(merged)
        return "\n\n".join(parts).strip()
    summary = _get_field(reasoning_details, "summary")
    text = _get_field(reasoning_details, "text")
    values = []
    if isinstance(summary, list):
        values.extend(str(x).strip() for x in summary if str(x).strip())
    elif summary is not None:
        summary_text = str(summary).strip()
        if summary_text:
            values.append(summary_text)
    if text is not None:
        text_value = str(text).strip()
        if text_value:
            values.append(text_value)
    return "\n\n".join(values).strip()


def _extract_message_reasoning_details(msg):
    additional_kwargs = _get_field(msg, "additional_kwargs", {}) or {}
    reasoning = None
    if isinstance(additional_kwargs, dict):
        reasoning = additional_kwargs.get("reasoning_details")
    else:
        reasoning = getattr(additional_kwargs, "reasoning_details", None)
    if reasoning is None:
        reasoning = _get_field(msg, "reasoning_details")
    return _reasoning_details_to_text(reasoning)


def extract_reasoning_details(conversation: dict) -> str:
    messages = _get_nested(conversation, ["messages"], []) or []
    reasoning_blocks = []
    for msg in messages:
        reasoning_text = _extract_message_reasoning_details(msg)
        if reasoning_text:
            reasoning_blocks.append(reasoning_text)
    return "\n\n".join(reasoning_blocks).strip()


def extract_conversation(conversation: dict, output_type: str):
    messages = _get_nested(conversation, ["messages"], []) or []
    if output_type == "final":
        text_blocks = []
        for msg in messages:
            role = _get_field(msg, "role")
            if role not in ("assistant", None):
                continue
            additional_kwargs = _get_field(msg, "additional_kwargs", {}) or {}
            tool_calls = additional_kwargs.get("tool_calls") if isinstance(additional_kwargs, dict) else getattr(additional_kwargs, "tool_calls", None)
            has_tool_call_id = _get_field(msg, "tool_call_id") is not None
            tool_name = _get_field(msg, "name")
            is_tool_invoke = isinstance(tool_calls, list)
            is_tool_message = has_tool_call_id or isinstance(tool_name, str)
            content = _get_field(msg, "content")
            if not is_tool_invoke and not is_tool_message and isinstance(content, str) and content.strip():
                text_blocks.append(content.strip())
        return "\n\n".join(text_blocks).strip()
    return messages


def extract_tool_messages(conversation: dict):
    messages = _get_nested(conversation, ["messages"], []) or []
    tool_messages = []
    for msg in messages:
        tool_call_id = _get_field(msg, "tool_call_id")
        name = _get_field(msg, "name")
        finish_reason = _get_field(msg, "finish_reason")
        if tool_call_id or (isinstance(name, str) and not finish_reason):
            tool_messages.append(msg)
    return tool_messages


def extract_first_tool_message_content(conversation: dict):
    msgs = extract_tool_messages(conversation)
    if not msgs:
        return None
    return _get_field(msgs[0], "content")


__all__ = [
    "extract_reasoning_details",
    "extract_conversation",
    "extract_tool_messages",
    "extract_first_tool_message_content",
]

