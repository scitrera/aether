"""GroupChat-style conversation with two local agents and one AetherRemoteAgent.

Spawns its own aetherlite gateway. Uses a manual round-robin loop instead of
ag2's GroupChatManager so no LLM-based speaker selection is required.

Depends on ``scitrera_aether_ag2.examples._aetherlite`` for the spawn/terminate
boilerplate shared with the other example scripts in this package.

# illustrative GroupChat-equivalent: manual loop used because ag2 GroupChatManager
# requires an LLM to select the next speaker; a deterministic round-robin avoids
# that dependency for a key-free demo.
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
# Agents
# ---------------------------------------------------------------------------

class EchoAgent(ConversableAgent):
    """Deterministic agent that echoes its last received message. No LLM needed."""

    def __init__(self, name: str) -> None:
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
        return True, f"{self.name} echoes: {last.get('content', '')}"


class RecordingSender:
    """Minimal Agent stub used as the initiating 'user' in the round-robin."""

    def __init__(self, name: str) -> None:
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

    data_dir = Path(tempfile.mkdtemp(prefix="aether-groupchat-"))
    proc, grpc_port = spawn_aetherlite(data_dir)
    endpoint = f"127.0.0.1:{grpc_port}"

    try:
        wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)

        # Identities
        remote_identity = AetherIdentity("default", "echo", "carol")
        caller_identity = AetherIdentity("default", "caller", "initiator")

        # 1. Start remote hosted agent (carol)
        host = AetherAgentHost(
            EchoAgent("carol"),
            remote_identity,
            endpoint,
            enable_checkpoints=False,
            enable_telemetry=False,
        )
        host_task = asyncio.create_task(host.serve())
        await asyncio.sleep(1.5)

        # 2. Caller transport
        caller_client = AsyncAgentClient(
            workspace=caller_identity.workspace,
            implementation=caller_identity.implementation,
            specifier=caller_identity.specifier,
        )
        transport = AetherTransport(caller_client, caller_identity)
        await caller_client.connect(endpoint)
        await transport.wait_connected()

        try:
            # 3. Agents: two local, one remote
            alice = EchoAgent("alice")
            bob = EchoAgent("bob")
            carol_proxy = AetherRemoteAgent(
                name="carol",
                remote_identity=remote_identity,
                transport=transport,
            )

            # 4. Manual round-robin over [alice, bob, carol_proxy] for 2 rounds
            # illustrative GroupChat-equivalent: no LLM speaker selection needed
            agents = [alice, bob, carol_proxy]
            initiator = RecordingSender("initiator")
            current_message: dict[str, Any] = {"content": "start: hello group", "role": "user"}
            transcript: list[tuple[str, str]] = [
                ("initiator", current_message["content"])
            ]

            for round_num in range(2):
                for agent in agents:
                    sender = initiator if round_num == 0 else initiator

                    if isinstance(agent, AetherRemoteAgent):
                        await asyncio.wait_for(
                            agent.a_receive(current_message, initiator),
                            timeout=10.0,
                        )
                        history = agent.chat_messages.get(initiator, [])
                        replies = [m for m in history if m.get("role") == "assistant"]
                        reply_text = replies[-1].get("content", "") if replies else "(no reply)"
                    else:
                        # Local ConversableAgent: call generate_reply directly
                        msgs = [current_message]
                        _, reply_text = await agent.a_generate_oai_reply(msgs)

                    transcript.append((agent.name, reply_text))
                    current_message = {"content": reply_text, "role": "user"}

            # 5. Print transcript
            print("--- GroupChat transcript ---")
            for speaker, text in transcript:
                print(f"  [{speaker}]: {text}")
            print("--- end transcript ---")

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
