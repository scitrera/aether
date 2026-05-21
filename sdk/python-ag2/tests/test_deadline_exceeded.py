"""Verify RequestEnvelope.deadline_ms is enforced by AetherAgentHost."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from autogen.agentchat import ConversableAgent
from scitrera_aether_client import AsyncAgentClient

from scitrera_aether_ag2 import (
    AetherAgentHost,
    AetherIdentity,
    AetherTransport,
)
from scitrera_aether_ag2.wire import RequestEnvelope
from autogen.agentchat.remote.protocol import RequestMessage


class SlowAgent(ConversableAgent):
    """Agent whose a_generate_oai_reply sleeps long enough to blow past short deadlines."""

    def __init__(self, name: str = "slow-bob", *, sleep_s: float = 2.0) -> None:
        super().__init__(
            name=name,
            llm_config=False,
            human_input_mode="NEVER",
            code_execution_config=False,
        )
        self._sleep_s = sleep_s

    async def a_generate_oai_reply(
        self,
        messages: list[dict[str, Any]] | None = None,
        sender: Any | None = None,
        tools: list[dict[str, Any]] | None = None,
        config: Any | None = None,
    ) -> tuple[bool, str]:
        await asyncio.sleep(self._sleep_s)
        return True, "should never arrive"


@pytest.mark.asyncio
async def test_deadline_exceeded_emits_terminal_error(dev_gateway_endpoint: str) -> None:
    host_identity = AetherIdentity("default", "slow", "bob")
    caller_identity = AetherIdentity("default", "caller", "alice")

    host = AetherAgentHost(SlowAgent(sleep_s=3.0), host_identity, dev_gateway_endpoint)
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

    try:
        envelope = RequestEnvelope(
            correlation_id=str(uuid.uuid4()),
            reply_to=caller_identity.to_topic(),
            request=RequestMessage(
                messages=[{"role": "user", "content": "hi", "name": "alice"}],
                context=None,
                client_tools=[],
            ),
            deadline_ms=300,
        )
        terminal = None
        async for resp in caller_transport.submit_request(host_identity, envelope):
            if resp.done:
                terminal = resp
                break
        assert terminal is not None, "no terminal response received"
        assert terminal.error is not None, f"expected error envelope, got response={terminal.response}"
        assert terminal.error.get("code") == "deadline_exceeded", (
            f"unexpected error code: {terminal.error}"
        )
    finally:
        await caller_client.close()
        await host.stop()
        try:
            await asyncio.wait_for(host_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            host_task.cancel()


@pytest.mark.asyncio
async def test_no_deadline_completes_normally(dev_gateway_endpoint: str) -> None:
    """Sanity check: when deadline_ms is unset, a slow-but-finite agent completes."""
    host_identity = AetherIdentity("default", "slow", "carol")
    caller_identity = AetherIdentity("default", "caller", "bob")

    host = AetherAgentHost(SlowAgent(sleep_s=0.2), host_identity, dev_gateway_endpoint)
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

    try:
        envelope = RequestEnvelope(
            correlation_id=str(uuid.uuid4()),
            reply_to=caller_identity.to_topic(),
            request=RequestMessage(
                messages=[{"role": "user", "content": "hi", "name": "bob"}],
                context=None,
                client_tools=[],
            ),
            deadline_ms=None,
        )
        terminal = None
        async for resp in caller_transport.submit_request(host_identity, envelope):
            if resp.done:
                terminal = resp
                break
        assert terminal is not None
        assert terminal.error is None, f"unexpected error: {terminal.error}"
    finally:
        await caller_client.close()
        await host.stop()
        try:
            await asyncio.wait_for(host_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            host_task.cancel()
