"""Verify ResponseEnvelopes arrive with monotonic sequence and a terminal done=True."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from autogen.agentchat import ConversableAgent
from autogen.agentchat.remote.protocol import RequestMessage
from autogen.events.client_events import StreamEvent
from autogen.io.base import IOStream
from scitrera_aether_client import AsyncAgentClient

from scitrera_aether_ag2 import (
    AetherAgentHost,
    AetherIdentity,
    AetherTransport,
)
from scitrera_aether_ag2.wire import RequestEnvelope, ResponseEnvelope


class StreamingEchoAgent(ConversableAgent):
    """Emits a few StreamEvents via IOStream then returns a final reply."""

    def __init__(self, name: str = "stream-bob") -> None:
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
        content = last.get("content", "")
        stream = IOStream.get_default()
        for chunk in ("he", "llo ", "world"):
            stream.send(StreamEvent(content=chunk))
            await asyncio.sleep(0.01)
        return True, f"echo: {content}"


@pytest.mark.asyncio
async def test_host_streaming_envelope_sequence(dev_gateway_endpoint: str) -> None:
    host_identity = AetherIdentity("default", "stream", "bob")
    caller_identity = AetherIdentity("default", "caller", "carol")

    host = AetherAgentHost(StreamingEchoAgent(), host_identity, dev_gateway_endpoint)
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
            request=RequestMessage(messages=[{"role": "user", "content": "hi"}]),
        )
        collected: list[ResponseEnvelope] = []
        async def _collect() -> None:
            async for env in caller_transport.submit_request(host_identity, envelope):
                collected.append(env)
        await asyncio.wait_for(_collect(), timeout=15.0)

        assert collected, "no envelopes received"
        seqs = [e.sequence for e in collected]
        assert seqs == list(range(len(collected))), f"non-monotonic sequence: {seqs}"
        terminal = [e for e in collected if e.done]
        assert len(terminal) == 1, f"expected exactly one terminal envelope, got {len(terminal)}"
        assert collected[-1].done, "terminal envelope must be last"
        streaming = [e for e in collected if e.response and e.response.streaming_text]
        assert streaming, f"expected at least one streaming_text envelope: {collected}"
    finally:
        await caller_client.close()
        await host.stop()
        try:
            await asyncio.wait_for(host_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            host_task.cancel()
