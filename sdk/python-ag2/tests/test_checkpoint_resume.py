"""Verify message history survives a host restart via Aether checkpoint."""

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
    def __init__(self, name: str = "ckpt-bob") -> None:
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
    def __init__(self, name: str = "alice") -> None:
        self.name = name
        self.description = name
        self.chat_messages: dict[Any, list[dict[str, Any]]] = {}
        self.sent: list[dict[str, Any]] = []

    async def a_send(self, message: Any, recipient: Any, request_reply: bool | None = None, silent: bool | None = False) -> None:
        self.sent.append(message if isinstance(message, dict) else {"content": message})

    def send(self, message: Any, recipient: Any, request_reply: bool | None = None, silent: bool | None = False) -> None:
        self.sent.append(message if isinstance(message, dict) else {"content": message})


async def _drive_one_turn(host_identity: AetherIdentity, endpoint: str, content: str) -> None:
    caller_identity = AetherIdentity("default", "caller", "alice")
    caller_client = AsyncAgentClient(
        workspace=caller_identity.workspace,
        implementation=caller_identity.implementation,
        specifier=caller_identity.specifier,
    )
    caller_transport = AetherTransport(caller_client, caller_identity)
    await caller_client.connect(endpoint)
    await caller_transport.wait_connected()
    try:
        proxy = AetherRemoteAgent(
            name="ckpt-bob",
            remote_identity=host_identity,
            transport=caller_transport,
        )
        sender = RecordingSender()
        await asyncio.wait_for(
            proxy.a_receive({"content": content, "role": "user"}, sender),
            timeout=10.0,
        )
    finally:
        await caller_client.close()


@pytest.mark.asyncio
async def test_checkpoint_resume_across_host_restart(dev_gateway_endpoint: str) -> None:
    # The host now mirrors RequestMessage.messages + final assistant into
    # agent._oai_messages after each successful turn, so save_history captures
    # real conversation state without any manual seeding. We drive one turn on
    # host1, then verify host2 picks up that turn from the checkpoint on startup.
    host_identity = AetherIdentity("default", "ckpt", "bob")

    agent1 = EchoAgent()
    assert not agent1._oai_messages, "fresh agent should have empty _oai_messages"
    host1 = AetherAgentHost(agent1, host_identity, dev_gateway_endpoint, enable_checkpoints=True)
    host1_task = asyncio.create_task(host1.serve())
    await asyncio.sleep(1.5)

    try:
        await _drive_one_turn(host_identity, dev_gateway_endpoint, "hello-from-alice")
        await asyncio.sleep(0.5)  # let post-stream save_history land
        assert agent1._oai_messages, (
            "host1 mirror should have populated _oai_messages after the turn"
        )
    finally:
        await host1.stop()
        try:
            await asyncio.wait_for(host1_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            host1_task.cancel()

    await asyncio.sleep(0.3)

    agent2 = EchoAgent()
    assert not agent2._oai_messages, "fresh agent should have empty _oai_messages"
    host2 = AetherAgentHost(agent2, host_identity, dev_gateway_endpoint, enable_checkpoints=True)
    host2_task = asyncio.create_task(host2.serve())
    await asyncio.sleep(1.5)

    try:
        assert agent2._oai_messages, "host2 agent _oai_messages was not restored from checkpoint"
        flat = [m for msgs in agent2._oai_messages.values() for m in msgs]
        assert any("hello-from-alice" in (m.get("content") or "") for m in flat), (
            f"restored history missing the driven user content: {agent2._oai_messages}"
        )
        assert any(
            m.get("role") == "assistant" and "hello-from-alice" in (m.get("content") or "")
            for m in flat
        ), f"restored history missing the echoed assistant reply: {agent2._oai_messages}"
    finally:
        await host2.stop()
        try:
            await asyncio.wait_for(host2_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            host2_task.cancel()
