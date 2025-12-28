
import os
import json
from pathlib import Path
from typing import Any, Iterable
from dotenv import load_dotenv
load_dotenv()


def _get_field(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _get_nested(obj, path, default=None):
    current = obj
    for key in path:
        current = _get_field(current, key, None)
        if current is None:
            return default
    return current


def _reasoning_details_to_text(reasoning_details) -> str:
    """Flatten MiniMax/OpenAI reasoning_details blocks into a readable string."""

    fragments = []

    def _consume(node, label=None):
        if node is None:
            return
        if isinstance(node, str):
            text = node.strip()
            if text:
                if label:
                    fragments.append(f"[{label}] {text}")
                else:
                    fragments.append(text)
            return
        if isinstance(node, dict):
            node_type = node.get("type") or label
            text_val = node.get("text") or node.get("content")
            if isinstance(text_val, str):
                _consume(text_val, node_type)
            elif isinstance(text_val, list):
                _consume(text_val, node_type)
            # Some providers nest actual text under "content" -> list[dict]
            content_list = node.get("content")
            if isinstance(content_list, list):
                for item in content_list:
                    _consume(item, node_type)
            return
        if isinstance(node, Iterable):
            for item in node:
                _consume(item, label)

    _consume(reasoning_details)
    return "\n".join(fragment for fragment in fragments if fragment).strip()


def _extract_message_reasoning_details(msg):
    additional_kwargs = _get_field(msg, "additional_kwargs", None)
    details = None
    if isinstance(additional_kwargs, dict):
        details = additional_kwargs.get("reasoning_details")
    else:
        details = getattr(additional_kwargs, "reasoning_details", None)
    if details:
        return details
    return _get_nested(msg, ["response_metadata", "reasoning_details"])

def _load_runtime_env() -> dict:
    path = os.environ.get("RUNTIME_ENV_PATH")
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def get_config_value(key: str, default=None):
    _RUNTIME_ENV = _load_runtime_env()
    
    if key in _RUNTIME_ENV:
        return _RUNTIME_ENV[key]
    return os.getenv(key, default)

def write_config_value(key: str, value: any):
    _RUNTIME_ENV = _load_runtime_env()
    _RUNTIME_ENV[key] = value
    path = os.environ.get("RUNTIME_ENV_PATH")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_RUNTIME_ENV, f, ensure_ascii=False, indent=4)


def extract_reasoning_details(conversation: dict) -> str:
    """Return flattened reasoning_details text from the latest AI message if available."""

    messages = _get_field(conversation, "messages", []) or []
    for msg in reversed(messages):
        text = _reasoning_details_to_text(_extract_message_reasoning_details(msg))
        if text:
            return text
    return ""

def extract_conversation(conversation: dict, output_type: str):
    """Extract information from a conversation payload."""

    messages = _get_field(conversation, "messages", []) or []

    if output_type == "all":
        return messages

    if output_type == "final":
        # Prefer the last assistant message that explicitly finished.
        for msg in reversed(messages):
            finish_reason = _get_nested(msg, ["response_metadata", "finish_reason"])
            content = _get_field(msg, "content")
            reasoning_fallback = _get_nested(msg, ["additional_kwargs", "reasoning_content"]) or ""
            details_text = _reasoning_details_to_text(_extract_message_reasoning_details(msg))
            if not reasoning_fallback:
                reasoning_fallback = details_text
            if finish_reason == "stop":
                if isinstance(content, str) and content.strip():
                    return content
                if isinstance(reasoning_fallback, str) and reasoning_fallback.strip():
                    return reasoning_fallback

        # Fallback: last AI-like message with content/reasoning that isn't a tool call.
        for msg in reversed(messages):
            content = _get_field(msg, "content")
            additional_kwargs = _get_field(msg, "additional_kwargs", {}) or {}
            tool_calls = None
            if isinstance(additional_kwargs, dict):
                tool_calls = additional_kwargs.get("tool_calls")
            else:
                tool_calls = getattr(additional_kwargs, "tool_calls", None)

            reasoning_fallback = ""
            if isinstance(additional_kwargs, dict):
                reasoning_fallback = additional_kwargs.get("reasoning_content", "")
            else:
                reasoning_fallback = getattr(additional_kwargs, "reasoning_content", "")
            details_text = _reasoning_details_to_text(_extract_message_reasoning_details(msg))
            if not reasoning_fallback:
                reasoning_fallback = details_text
            elif details_text:
                reasoning_fallback = f"{reasoning_fallback}\n{details_text}".strip()

            is_tool_invoke = isinstance(tool_calls, list)
            has_tool_call_id = _get_field(msg, "tool_call_id") is not None
            tool_name = _get_field(msg, "name")
            is_tool_message = has_tool_call_id or isinstance(tool_name, str)

            if not is_tool_invoke and not is_tool_message and isinstance(content, str) and content.strip():
                return content
            if (
                not is_tool_invoke
                and not is_tool_message
                and isinstance(reasoning_fallback, str)
                and reasoning_fallback.strip()
            ):
                return reasoning_fallback

        return None

    raise ValueError("output_type must be 'final' or 'all'")


def extract_tool_messages(conversation: dict):
    """Return all ToolMessage-like entries from the conversation."""

    messages = _get_field(conversation, "messages", []) or []
    tool_messages = []
    for msg in messages:
        tool_call_id = _get_field(msg, "tool_call_id")
        name = _get_field(msg, "name")
        finish_reason = _get_nested(msg, ["response_metadata", "finish_reason"])
        if tool_call_id or (isinstance(name, str) and not finish_reason):
            tool_messages.append(msg)
    return tool_messages


def extract_first_tool_message_content(conversation: dict):
    """Return the content of the first ToolMessage if available, else None."""
    msgs = extract_tool_messages(conversation)
    if not msgs:
        return None

    first = msgs[0]
    if isinstance(first, dict):
        return first.get("content")
    return getattr(first, "content", None)

