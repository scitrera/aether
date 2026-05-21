"""Factories for orchestrator-spawned host subprocesses in E2E tests."""

from __future__ import annotations

from typing import Any

from autogen.agentchat import ConversableAgent


class _SpawnedEchoAgent(ConversableAgent):
    def __init__(self, name: str = "spawned-echo") -> None:
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


def make_echo_agent(config: dict) -> ConversableAgent:
    """Factory used by AETHER_AG2_FACTORY in the orchestrator E2E test."""
    return _SpawnedEchoAgent()
