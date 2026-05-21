"""Run a two-agent ag2 conversation where one agent lives in this process,
the other is reached over Aether. Spawns its own aetherlite gateway.

Depends on ``scitrera_aether_ag2.examples._aetherlite`` for the spawn/terminate
boilerplate shared with the other example scripts in this package.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

from autogen.agentchat import ConversableAgent
from scitrera_aether_client import AsyncAgentClient

from scitrera_aether_ag2 import (
    AetherAgentHost,
    AetherIdentity,
    AetherRemoteAgent,
    AetherTransport,
)
from scitrera_aether_ag2.examples._aetherlite import (
    READY_TIMEOUT_S,
    ensure_binary_executable,
    spawn_aetherlite,
    terminate_aetherlite,
    wait_for_tcp,
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Echo agent (no LLM key required)
# ---------------------------------------------------------------------------

class EchoAgent(ConversableAgent):
    """Deterministic agent that echoes its last received message. No LLM needed."""

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


# ---------------------------------------------------------------------------
# Minimal sender that records replies
# ---------------------------------------------------------------------------

class RecordingSender:
    """Minimal Agent stub that records replies sent back by the proxy."""

    def __init__(self, name: str = "alice") -> None:
        self.name = name
        self.description = name
        self.chat_messages: dict[Any, list[dict[str, Any]]] = {}
        self.received: list[dict[str, Any]] = []

    async def a_send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        msg = message if isinstance(message, dict) else {"content": message}
        self.received.append(msg)

    def send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        msg = message if isinstance(message, dict) else {"content": message}
        self.received.append(msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    ensure_binary_executable()

    data_dir = Path(tempfile.mkdtemp(prefix="aether-two-agent-"))
    proc, grpc_port = spawn_aetherlite(data_dir)
    endpoint = f"127.0.0.1:{grpc_port}"

    try:
        wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)

        host_identity = AetherIdentity("default", "echo", "bob")
        caller_identity = AetherIdentity("default", "caller", "alice")

        # 1. Start host in background task
        host = AetherAgentHost(
            EchoAgent(),
            host_identity,
            endpoint,
            enable_checkpoints=False,
            enable_telemetry=False,
        )
        host_task = asyncio.create_task(host.serve())
        await asyncio.sleep(1.5)  # let host connect

        # 2. Caller client + transport
        caller_client = AsyncAgentClient(
            workspace=caller_identity.workspace,
            implementation=caller_identity.implementation,
            specifier=caller_identity.specifier,
        )
        transport = AetherTransport(caller_client, caller_identity)
        await caller_client.connect(endpoint)
        await transport.wait_connected()

        try:
            # 3. Remote agent proxy
            proxy = AetherRemoteAgent(
                name="echo-bob",
                remote_identity=host_identity,
                transport=transport,
            )

            # 4. Drive one turn
            sender = RecordingSender()
            await asyncio.wait_for(
                proxy.a_receive({"content": "hi", "role": "user"}, sender),
                timeout=10.0,
            )

            # 5. Print result
            history = proxy.chat_messages[sender]
            replies = [m for m in history if m.get("role") == "assistant"]
            if replies:
                print(f"Reply from remote agent: {replies[-1].get('content', '')}")
            else:
                print(f"Full history: {history}")

        finally:
            await caller_client.close()
            await host.stop()
            try:
                await asyncio.wait_for(host_task, timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                host_task.cancel()

    finally:
        terminate_aetherlite(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
