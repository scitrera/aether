"""Helpers for the client-side tool-execution loop used by AetherRemoteAgent."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

try:  # pragma: no cover - import-time only
    from autogen.agentchat.remote.protocol import get_tool_names as _ag2_get_tool_names
except ImportError:  # pragma: no cover
    _ag2_get_tool_names = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


def get_client_tools(sender: Any) -> list[dict[str, Any]]:
    """Return ag2 tool dicts the LLM should see (function-schema form).

    Discovery order (first non-empty wins):
      1. ``sender.tools`` — modern ag2 Tool objects with ``tool_schema``/``function_schema``.
      2. ``sender.client_tools`` — pre-built tool dicts.
      3. ``sender.llm_config["tools"]`` — modern openai-tools form.
      4. ``sender.llm_config["functions"]`` — legacy openai-functions form, auto-wrapped
         into ``{"type": "function", "function": {...}}``.
    """
    tools_attr = getattr(sender, "tools", None)
    out: list[dict[str, Any]] = []
    if tools_attr:
        for t in tools_attr:
            schema = getattr(t, "tool_schema", None)
            if isinstance(schema, dict):
                out.append(schema)
                continue
            fn = getattr(t, "function_schema", None)
            if isinstance(fn, dict):
                out.append({"type": "function", "function": fn})
        if out:
            return out
    raw = getattr(sender, "client_tools", None)
    if isinstance(raw, list):
        filtered = [t for t in raw if isinstance(t, dict)]
        if filtered:
            return filtered
    llm_cfg = getattr(sender, "llm_config", None)
    if isinstance(llm_cfg, dict):
        raw_tools = llm_cfg.get("tools")
        if isinstance(raw_tools, list):
            filtered = [t for t in raw_tools if isinstance(t, dict)]
            if filtered:
                return filtered
        raw_functions = llm_cfg.get("functions")
        if isinstance(raw_functions, list):
            return [
                {"type": "function", "function": dict(f)}
                for f in raw_functions
                if isinstance(f, dict) and f.get("name")
            ]
    return []


def get_client_tool_names(tools: list[dict[str, Any]]) -> set[str]:
    """Set of function names exposed in the tool-dict list."""
    if _ag2_get_tool_names is not None:
        return _ag2_get_tool_names(tools)
    return {t.get("function", {}).get("name", "") for t in tools} - {""}


def detect_client_tool_calls(message: dict[str, Any], known_names: set[str]) -> list[dict[str, Any]]:
    """Return the tool_calls entries from message whose function.name is in known_names."""
    if not known_names:
        return []
    calls = message.get("tool_calls") or []
    out: list[dict[str, Any]] = []
    for tc in calls:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") or {}
        if fn.get("name") in known_names:
            out.append(tc)
    return out


async def execute_client_tools(
    sender: Any,
    message: dict[str, Any],
    calls: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Run the named tool_calls via sender's tool executor.

    Returns a role='tool' message keyed to the tool_call ids:
    - on success: the executor's actual reply (one tool_response per call).
    - on failure (exception, missing executor, or executor reported success=False): a
      synthesized role='tool' message whose ``tool_responses`` carry an error string
      for each requested call. This lets the remote LLM react to tool failures
      instead of silently dropping the calls.

    Returns None only when there are no calls to execute.
    """
    if not calls:
        return None
    filtered = dict(message)
    filtered["tool_calls"] = calls
    a_fn = getattr(sender, "a_generate_tool_calls_reply", None)
    s_fn = getattr(sender, "generate_tool_calls_reply", None)
    try:
        if a_fn is not None and inspect.iscoroutinefunction(a_fn):
            success, result = await a_fn([filtered])
        elif s_fn is not None:
            res = s_fn([filtered])
            if inspect.isawaitable(res):
                res = await res
            success, result = res
        else:
            return _synthesize_tool_error(
                calls,
                f"sender {getattr(sender, 'name', sender)!r} has no generate_tool_calls_reply",
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("client tool execution failed")
        return _synthesize_tool_error(calls, f"tool execution raised: {exc!r}")
    if not success or not isinstance(result, dict):
        return _synthesize_tool_error(
            calls, "tool executor returned no usable result (success=False or non-dict)"
        )
    return result


def _synthesize_tool_error(calls: list[dict[str, Any]], reason: str) -> dict[str, Any]:
    """Build a role='tool' message with an error tool_response per requested call_id."""
    tool_responses = [
        {
            "tool_call_id": tc.get("id", ""),
            "role": "tool",
            "content": f"error: {reason}",
        }
        for tc in calls
    ]
    return {
        "role": "tool",
        "tool_responses": tool_responses,
        "content": "\n\n".join(t["content"] for t in tool_responses),
    }


__all__ = [
    "get_client_tools",
    "get_client_tool_names",
    "detect_client_tool_calls",
    "execute_client_tools",
]
