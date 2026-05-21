"""Unit tests for AgentCheckpointer — mock-only, no aetherlite required."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from scitrera_aether_ag2.features.checkpoint import AgentCheckpointer, restore_into_agent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CheckpointResponse:
    """Minimal stand-in for aether_pb2.CheckpointResponse."""

    def __init__(self, data: bytes = b"", success: bool = True) -> None:
        self.data = data
        self.success = success


class _StubClient:
    """AsyncAgentClient stub that records save/load calls."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self.saves: list[tuple[bytes, str]] = []
        self.loads: list[str] = []

    async def checkpoint_save(self, data: bytes, key: str = "", **_: Any) -> _CheckpointResponse:
        self._store[key] = data
        self.saves.append((data, key))
        return _CheckpointResponse(data=data)

    async def checkpoint_load(self, key: str = "", **_: Any) -> _CheckpointResponse:
        self.loads.append(key)
        stored = self._store.get(key, b"")
        return _CheckpointResponse(data=stored)


def _make_agent(name: str = "bob") -> MagicMock:
    """Return a minimal ConversableAgent mock with a real defaultdict for _oai_messages."""
    agent = MagicMock()
    agent.name = name
    agent._oai_messages = defaultdict(list)
    return agent


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_load_history_roundtrip() -> None:
    """save_history + load_history restores _oai_messages keyed by peer name string."""
    client = _StubClient()
    ckpt = AgentCheckpointer(client, "bob")

    peer = MagicMock()
    peer.name = "alice"

    agent = _make_agent("bob")
    agent._oai_messages[peer] = [{"role": "user", "content": "hello"}]

    await ckpt.save_history(agent)

    # Verify the key used
    assert len(client.saves) == 1
    _, key = client.saves[0]
    assert key == "ag2:hist:bob"

    # Restore into a fresh agent
    agent2 = _make_agent("bob")
    await ckpt.load_history(agent2)

    assert "alice" in agent2._oai_messages
    assert agent2._oai_messages["alice"] == [{"role": "user", "content": "hello"}]


@pytest.mark.asyncio
async def test_save_load_context_roundtrip() -> None:
    """save_context + load_context restores an arbitrary dict."""
    client = _StubClient()
    ckpt = AgentCheckpointer(client, "bob")

    ctx = {"task_id": "t-123", "turn": 5, "flags": ["x", "y"]}
    await ckpt.save_context(ctx)

    assert len(client.saves) == 1
    _, key = client.saves[0]
    assert key == "ag2:ctx:bob"

    loaded = await ckpt.load_context()
    assert loaded == ctx


@pytest.mark.asyncio
async def test_load_history_missing_key_is_noop() -> None:
    """load_history on a missing key must not raise and must not modify the agent."""
    client = _StubClient()
    ckpt = AgentCheckpointer(client, "ghost")

    agent = _make_agent("ghost")
    agent._oai_messages["existing"] = [{"role": "assistant", "content": "hi"}]

    # No checkpoint stored — stub returns empty bytes
    await ckpt.load_history(agent)

    # Original entry untouched
    assert agent._oai_messages["existing"] == [{"role": "assistant", "content": "hi"}]


@pytest.mark.asyncio
async def test_load_context_missing_returns_none() -> None:
    """load_context with no stored value returns None."""
    client = _StubClient()
    ckpt = AgentCheckpointer(client, "ghost")

    result = await ckpt.load_context()
    assert result is None


def test_restore_into_agent_resolves_string_keys() -> None:
    """restore_into_agent re-keys string names to Agent objects via peer_lookup."""
    agent = _make_agent("bob")
    alice_obj = MagicMock()
    alice_obj.name = "alice"

    agent._oai_messages["alice"] = [{"role": "user", "content": "hey"}]

    restore_into_agent(agent, peer_lookup=lambda name: alice_obj if name == "alice" else (_ for _ in ()).throw(KeyError(name)))

    assert alice_obj in agent._oai_messages
    assert agent._oai_messages[alice_obj] == [{"role": "user", "content": "hey"}]
    assert "alice" not in agent._oai_messages


def test_restore_into_agent_unknown_key_kept_as_string() -> None:
    """restore_into_agent keeps string key when peer_lookup raises."""
    agent = _make_agent("bob")
    agent._oai_messages["unknown-peer"] = [{"role": "user", "content": "x"}]

    def bad_lookup(name: str) -> Any:
        raise KeyError(name)

    restore_into_agent(agent, peer_lookup=bad_lookup)

    assert "unknown-peer" in agent._oai_messages
