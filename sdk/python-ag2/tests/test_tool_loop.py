"""Verify client-side tool calls execute locally and continuation is sent."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from autogen.agentchat import ConversableAgent
from scitrera_aether_client import AsyncAgentClient

from scitrera_aether_ag2 import (
    AetherAgentHost,
    AetherIdentity,
    AetherRemoteAgent,
    AetherTransport,
)


class ToolCallingAgent(ConversableAgent):
    """First reply: tool_calls for add. Second reply: final assistant text using the result."""

    def __init__(self, name: str = "tool-bob") -> None:
        super().__init__(
            name=name,
            llm_config=False,
            human_input_mode="NEVER",
            code_execution_config=False,
        )
        self._turns = 0

    async def a_generate_oai_reply(
        self,
        messages: list[dict[str, Any]] | None = None,
        sender: Any | None = None,
        tools: list[dict[str, Any]] | None = None,
        config: Any | None = None,
    ) -> tuple[bool, str | dict[str, Any]]:
        self._turns += 1
        msgs = messages or []
        has_tool_response = any(m.get("role") == "tool" for m in msgs)
        if not has_tool_response:
            return True, {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "add", "arguments": json.dumps({"a": 2, "b": 3})},
                    }
                ],
            }
        last_tool = next((m for m in reversed(msgs) if m.get("role") == "tool"), {})
        return True, f"result: {last_tool.get('content', '')}"


@pytest.mark.asyncio
async def test_tool_loop_executes_client_tool(dev_gateway_endpoint: str) -> None:
    host_identity = AetherIdentity("default", "tooler", "bob")
    caller_identity = AetherIdentity("default", "caller", "alice")

    host = AetherAgentHost(ToolCallingAgent(), host_identity, dev_gateway_endpoint)
    host_task = asyncio.create_task(host.serve())
    await asyncio.sleep(1.5)

    caller_client = AsyncAgentClient(
        workspace=caller_identity.workspace,
        implementation=caller_identity.implementation,
        specifier=caller_identity.specifier,
    )
    caller_transport = AetherTransport(caller_client, caller_identity)
    await caller_client.connect(dev_gateway_endpoint)
    await caller_transport.wait_connected()

    sender = ConversableAgent(
        name="alice",
        llm_config=False,
        human_input_mode="NEVER",
        code_execution_config=False,
    )

    @sender.register_for_execution(name="add")
    def add(a: int, b: int) -> int:
        return a + b

    sender.client_tools = [  # type: ignore[attr-defined]
        {
            "type": "function",
            "function": {
                "name": "add",
                "description": "Add two integers",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "a": {"type": "integer"},
                        "b": {"type": "integer"},
                    },
                    "required": ["a", "b"],
                },
            },
        }
    ]

    try:
        proxy = AetherRemoteAgent(
            name="tooler-bob",
            remote_identity=host_identity,
            transport=caller_transport,
            max_continuations=3,
        )
        await asyncio.wait_for(
            proxy.a_receive({"content": "add 2+3", "role": "user"}, sender),
            timeout=15.0,
        )
        history = proxy.chat_messages[sender]
        roles = [m.get("role") for m in history]
        assistant_idxs = [i for i, r in enumerate(roles) if r == "assistant"]
        tool_idxs = [i for i, r in enumerate(roles) if r == "tool"]
        assert len(assistant_idxs) >= 2, f"expected >=2 assistant messages, got {roles}"
        assert tool_idxs, f"expected at least one role=tool message, got {roles}"
        assert min(tool_idxs) > assistant_idxs[0], (
            f"tool message must follow first assistant: roles={roles}"
        )
        assert max(assistant_idxs) > min(tool_idxs), (
            f"final assistant must follow tool message: roles={roles}"
        )
        final = history[-1]
        assert final.get("role") == "assistant"
        assert "5" in (final.get("content") or ""), f"final reply missing tool result: {final}"
    finally:
        await caller_client.close()
        await host.stop()
        try:
            await asyncio.wait_for(host_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            host_task.cancel()
