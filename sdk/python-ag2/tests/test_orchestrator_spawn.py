"""Verify AetherAg2Orchestrator drives the real send-to-offline → spawn → reply path.

This test exercises the full dispatch flow:
  1. Orchestrator connects (registering its ``ag2-subprocess`` profile with the gateway).
  2. Caller registers the agent implementation in the registry, mapping it to the
     orchestrator's profile and the host_entrypoint factory.
  3. Caller sends a TOOL_CALL to ``ag::default::<impl>::<spec>`` (offline target).
  4. The gateway's ``triggerOrchestration`` creates an ``agent_startup`` task that the
     dispatcher routes to our orchestrator.
  5. Orchestrator's ``handle_assignment`` runs (real, not hand-built), which spawns the
     host subprocess via ``spawn_module``.
  6. Subprocess connects to aetherlite, the queued TOOL_CALL is replayed from the stream,
     and the agent replies back to the caller.

No ``handle_assignment(SimpleNamespace(...))`` shortcut, no hand-built proto.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import pytest
from scitrera_aether_client import AsyncAgentClient

from scitrera_aether_ag2 import (
    AetherIdentity,
    AetherRemoteAgent,
    AetherTransport,
)
from scitrera_aether_ag2.orchestrator import AetherAg2Orchestrator


@pytest.mark.asyncio
async def test_orchestrator_real_dispatch_spawn_and_reply(dev_gateway_endpoint: str) -> None:
    orch = AetherAg2Orchestrator(
        implementation=f"orch-e2e-{uuid.uuid4().hex[:8]}",
        gateway=dev_gateway_endpoint,
    )
    orch.connect()

    impl = f"orchhost-{uuid.uuid4().hex[:8]}"
    spec = "alpha"
    target_identity = AetherIdentity("default", impl, spec)
    caller_identity = AetherIdentity("default", "caller", f"orchcaller-{uuid.uuid4().hex[:8]}")

    caller_client = AsyncAgentClient(
        workspace=caller_identity.workspace,
        implementation=caller_identity.implementation,
        specifier=caller_identity.specifier,
    )
    caller_transport = AetherTransport(caller_client, caller_identity)
    await caller_client.connect(dev_gateway_endpoint)
    await caller_transport.wait_connected()

    try:
        register_resp = await caller_client.register_agent(
            implementation=impl,
            profile=AetherAg2Orchestrator.SUPPORTED_PROFILE,
            description="orchestrator e2e — echo agent",
            launch_params={
                "factory": "tests._e2e_factories:make_echo_agent",
            },
            timeout=5.0,
        )
        assert register_resp is not None and register_resp.success, (
            f"register_agent failed: {register_resp}"
        )

        proxy = AetherRemoteAgent(
            name=f"{impl}-{spec}",
            remote_identity=target_identity,
            transport=caller_transport,
        )

        class _Sender:
            name = "orchcaller"
            description = "orchcaller"
            chat_messages: dict[Any, list[dict[str, Any]]] = {}

            async def a_send(self, message: Any, recipient: Any, request_reply: bool | None = None, silent: bool | None = False) -> None:
                pass

            def send(self, message: Any, recipient: Any, request_reply: bool | None = None, silent: bool | None = False) -> None:
                pass

        sender = _Sender()
        delivered = False
        last_err: Exception | None = None
        deadline = time.monotonic() + 25.0
        while time.monotonic() < deadline:
            try:
                await asyncio.wait_for(
                    proxy.a_receive({"content": "ping", "role": "user"}, sender),
                    timeout=8.0,
                )
                delivered = True
                break
            except (asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
                last_err = exc
                await asyncio.sleep(0.5)
        assert delivered, f"orchestrated message never delivered: {last_err}"

        history = proxy.chat_messages[sender]
        replies = [m for m in history if m.get("role") == "assistant"]
        assert replies, f"no assistant reply in history: {history}"
        assert "echo: ping" in (replies[-1].get("content") or ""), (
            f"unexpected reply: {replies}"
        )

        assert orch.active_subprocess_count >= 1, (
            "orchestrator should have spawned at least one subprocess for the assignment"
        )
    finally:
        await caller_client.close()
        orch.shutdown()
