"""GroupChatManager (LLM-selected speaker) with a remote AetherRemoteAgent participant.

Variant of ``groupchat_with_remote.py`` that demonstrates ag2's real
``GroupChatManager`` with LLM-driven ``select_speaker`` — gated on
``OPENAI_API_KEY`` since speaker selection requires an LLM call. The key-free
companion (``groupchat_with_remote.py``) uses a manual round-robin for the
same scenario.

Spawns its own aetherlite gateway via the shared
``scitrera_aether_ag2.examples._aetherlite`` helper.

Usage::

    export OPENAI_API_KEY=sk-...
    python -m scitrera_aether_ag2.examples.groupchat_with_remote_llm

Set ``AETHER_AG2_LLM_MODEL`` to override the default model
(``gpt-4o-mini``). The model only needs to be capable enough to pick the next
speaker — answer quality from the agents is not the point.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from autogen.agentchat import ConversableAgent, GroupChat, GroupChatManager
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

DEFAULT_MODEL = os.environ.get("AETHER_AG2_LLM_MODEL", "gpt-4o-mini")


def _require_openai_key() -> str:
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise SystemExit(
            "OPENAI_API_KEY is required for this example because "
            "GroupChatManager.select_speaker performs an LLM call. "
            "Use groupchat_with_remote.py for a key-free demo."
        )
    return key


def _llm_config(api_key: str) -> dict[str, Any]:
    return {
        "config_list": [{"model": DEFAULT_MODEL, "api_key": api_key}],
        "temperature": 0,
        "cache_seed": None,
    }


class EchoAgent(ConversableAgent):
    """Deterministic remote-host echo agent. No LLM key needed on the host side."""

    def __init__(self, name: str = "echo-carol") -> None:
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


async def main() -> None:
    api_key = _require_openai_key()
    ensure_binary_executable()

    data_dir = Path(tempfile.mkdtemp(prefix="aether-groupchat-llm-"))
    proc, grpc_port = spawn_aetherlite(data_dir)
    endpoint = f"127.0.0.1:{grpc_port}"

    try:
        wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)

        remote_identity = AetherIdentity("default", "echo", "carol")
        caller_identity = AetherIdentity("default", "caller", "initiator")

        host = AetherAgentHost(
            EchoAgent("carol"),
            remote_identity,
            endpoint,
            enable_checkpoints=False,
            enable_telemetry=False,
        )
        host_task = asyncio.create_task(host.serve())
        await asyncio.sleep(1.5)

        caller_client = AsyncAgentClient(
            workspace=caller_identity.workspace,
            implementation=caller_identity.implementation,
            specifier=caller_identity.specifier,
        )
        transport = AetherTransport(caller_client, caller_identity)
        await caller_client.connect(endpoint)
        await transport.wait_connected()

        try:
            cfg = _llm_config(api_key)

            alice = ConversableAgent(
                name="alice",
                system_message="You are alice, a brief participant in the group chat.",
                llm_config=cfg,
                human_input_mode="NEVER",
            )
            bob = ConversableAgent(
                name="bob",
                system_message="You are bob, a brief participant in the group chat.",
                llm_config=cfg,
                human_input_mode="NEVER",
            )
            carol_proxy = AetherRemoteAgent(
                name="carol",
                description="remote echo agent",
                remote_identity=remote_identity,
                transport=transport,
            )

            groupchat = GroupChat(
                agents=[alice, bob, carol_proxy],
                messages=[],
                max_round=4,
                speaker_selection_method="auto",  # LLM picks the next speaker
            )
            manager = GroupChatManager(groupchat=groupchat, llm_config=cfg)

            await asyncio.to_thread(
                alice.initiate_chat,
                manager,
                message="Briefly greet the group and ask carol to echo 'hello group'.",
            )

            assistant_msgs = [
                m for m in groupchat.messages if m.get("role") == "assistant"
            ]
            print(f"Group chat ran for {len(groupchat.messages)} messages, "
                  f"{len(assistant_msgs)} assistant turns")
            for m in groupchat.messages:
                speaker = m.get("name") or m.get("role") or "?"
                content = (m.get("content") or "")[:120]
                print(f"  {speaker}: {content}")
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
