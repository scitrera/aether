"""Smoke test: one request through AetherRemoteAgent → host → response."""

from __future__ import annotations

import asyncio
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


class EchoAgent(ConversableAgent):
    """Minimal ConversableAgent with a deterministic, key-free reply."""

    def __init__(self, name: str = "echo-bob") -> None:
        super().__init__(
            name=name,
            llm_config=False,
            human_input_mode="NEVER",
            code_execution_config=False,
        )

    async def a_generate_oai_reply(
        self,
        messages: list[dict[str, Any]] | None = None,
        sender: Any | None = None,
        tools: list[dict[str, Any]] | None = None,
        config: Any | None = None,
    ) -> tuple[bool, str]:
        last = (messages or [{}])[-1]
        return True, f"echo: {last.get('content', '')}"


class RecordingSender:
    """Minimal Agent stub that records what the proxy sends back."""

    def __init__(self, name: str = "alice") -> None:
        self.name = name
        self.description = name
        self.chat_messages: dict[Any, list[dict[str, Any]]] = {}
        self.sent: list[dict[str, Any]] = []

    async def a_send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        self.sent.append(message if isinstance(message, dict) else {"content": message})

    def send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        self.sent.append(message if isinstance(message, dict) else {"content": message})


@pytest.mark.asyncio
async def test_proxy_roundtrip(dev_gateway_endpoint: str) -> None:
    host_identity = AetherIdentity("default", "echo", "bob")
    caller_identity = AetherIdentity("default", "caller", "alice")

    host = AetherAgentHost(EchoAgent(), host_identity, dev_gateway_endpoint)
    host_task = asyncio.create_task(host.serve())
    await asyncio.sleep(1.5)  # let host connect

    caller_client = AsyncAgentClient(
        workspace=caller_identity.workspace,
        implementation=caller_identity.implementation,
        specifier=caller_identity.specifier,
    )
    caller_transport = AetherTransport(caller_client, caller_identity)
    await caller_client.connect(dev_gateway_endpoint)
    await caller_transport.wait_connected()

    try:
        proxy = AetherRemoteAgent(
            name="echo-bob",
            remote_identity=host_identity,
            transport=caller_transport,
        )
        sender = RecordingSender()
        await asyncio.wait_for(
            proxy.a_receive({"content": "hi", "role": "user"}, sender),
            timeout=10.0,
        )
        history = proxy.chat_messages[sender]
        replies = [m for m in history if m.get("role") == "assistant"]
        assert replies, f"no assistant reply in history: {history}"
        assert "echo: hi" in (replies[-1].get("content") or ""), replies
    finally:
        await caller_client.close()
        await host.stop()
        try:
            await asyncio.wait_for(host_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            host_task.cancel()
