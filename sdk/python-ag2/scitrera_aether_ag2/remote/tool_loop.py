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
    """Return ag2 tool dicts the LLM should see (function-schema form)."""
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
        return [t for t in raw if isinstance(t, dict)]
    llm_cfg = getattr(sender, "llm_config", None)
    if isinstance(llm_cfg, dict):
        raw_tools = llm_cfg.get("tools")
        if isinstance(raw_tools, list):
            return [t for t in raw_tools if isinstance(t, dict)]
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
    """Run the named tool_calls via sender's tool executor; return a role='tool' message or None."""
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
            logger.warning("sender %s has no generate_tool_calls_reply", getattr(sender, "name", sender))
            return None
    except Exception:  # noqa: BLE001
        logger.exception("client tool execution failed")
        return None
    if not success or not isinstance(result, dict):
        return None
    return result


__all__ = [
    "get_client_tools",
    "get_client_tool_names",
    "detect_client_tool_calls",
    "execute_client_tools",
]
