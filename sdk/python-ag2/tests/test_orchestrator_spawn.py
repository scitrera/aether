"""Verify AetherAg2Orchestrator spawns a host subprocess and delivers a message.

Current scope: the orchestrator spawn API itself + post-spawn message delivery to
the connected host. Hand-builds the ``TaskAssignment`` and calls
``handle_assignment`` directly rather than driving the full
send-to-offline → gateway trigger → dispatcher → orchestrator path; that round-trip
hits an aetherlite lite-mode plumbing gap (orchestrated_task_queue rows are never
visible to the polling dispatcher after a successful triggerOrchestration), tracked
separately in ``.slop/ag2-adapter-tech-debt.md`` under the O1 open item.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from scitrera_aether_client import AsyncAgentClient

from scitrera_aether_ag2 import (
    AetherIdentity,
    AetherRemoteAgent,
    AetherTransport,
)
from scitrera_aether_ag2.orchestrator import AetherAg2Orchestrator


def _build_assignment(
    task_id: str,
    workspace: str,
    target_implementation: str,
    specifier: str,
    factory: str,
) -> SimpleNamespace:
    return SimpleNamespace(
        task_id=task_id,
        task_type="ag2-subprocess",
        profile="ag2-subprocess",
        target_implementation=target_implementation,
        workspace=workspace,
        specifier=specifier,
        launch_params={"factory": factory},
        metadata={"hello": "world"},
    )


@pytest.mark.asyncio
async def test_orchestrator_spawn_delivers_message(dev_gateway_endpoint: str) -> None:
    orch = AetherAg2Orchestrator(
        implementation="orch-e2e",
        gateway=dev_gateway_endpoint,
    )
    assignment = _build_assignment(
        task_id="t-e2e-1",
        workspace="default",
        target_implementation="orch-host",
        specifier="alpha",
        factory="tests._e2e_factories:make_echo_agent",
    )
    orch.handle_assignment(assignment)

    target_identity = AetherIdentity("default", "orch-host", "alpha")
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
        deadline = time.monotonic() + 20.0
        last_err: Exception | None = None
        proxy = AetherRemoteAgent(
            name="orch-host-alpha",
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
        while time.monotonic() < deadline:
            try:
                await asyncio.wait_for(
                    proxy.a_receive({"content": "ping", "role": "user"}, sender),
                    timeout=4.0,
                )
                delivered = True
                break
            except (asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
                last_err = exc
                await asyncio.sleep(0.5)
        assert delivered, f"message never delivered to spawned host: {last_err}"

        history = proxy.chat_messages[sender]
        replies = [m for m in history if m.get("role") == "assistant"]
        assert replies, f"no assistant reply: {history}"
        assert "echo: ping" in (replies[-1].get("content") or ""), replies
    finally:
        await caller_client.close()
        orch.shutdown()
