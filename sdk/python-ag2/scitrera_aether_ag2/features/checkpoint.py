"""Persist and restore ConversableAgent message history + context dict via Aether checkpoints."""

# host integration:
#   - in AetherAgentHost.serve(), build AgentCheckpointer(self._client, self.identity.specifier or self.agent.name)
#   - call `await ckpt.load_history(self.agent)` once before the first request
#   - call `await ckpt.save_history(self.agent)` after each successful _handle_request stream

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Callable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from autogen.agentchat import ConversableAgent
    from scitrera_aether_client import AsyncAgentClient

_HIST_KEY_PREFIX = "ag2:hist:"
_CTX_KEY_PREFIX = "ag2:ctx:"


class AgentCheckpointer:
    """Save/restore a ConversableAgent's message history and context via Aether checkpoints."""

    def __init__(self, client: "AsyncAgentClient", agent_name: str, *, timeout: float = 5.0) -> None:
        self._client = client
        self._hist_key = f"{_HIST_KEY_PREFIX}{agent_name}"
        self._ctx_key = f"{_CTX_KEY_PREFIX}{agent_name}"
        self._timeout = timeout

    async def save_history(self, agent: "ConversableAgent") -> None:
        """Persist agent._oai_messages; peer Agent keys are flattened to their .name string."""
        raw: dict[Any, list[dict[str, Any]]] = agent._oai_messages  # type: ignore[attr-defined]
        serializable = {
            (k.name if hasattr(k, "name") else str(k)): v
            for k, v in raw.items()
        }
        payload = json.dumps(serializable).encode()
        resp = await self._client.checkpoint_save(payload, key=self._hist_key, timeout=self._timeout)
        _warn_if_unsuccessful(resp, "save_history", self._hist_key)

    async def load_history(self, agent: "ConversableAgent") -> None:
        """Restore agent._oai_messages from checkpoint; keys are strings (peer names), not Agent refs.

        Downstream code must resolve string keys to Agent objects via restore_into_agent().
        No-op if no checkpoint exists yet.
        """
        resp = await self._client.checkpoint_load(key=self._hist_key, timeout=self._timeout)
        if resp is None or not resp.data:
            return
        data: dict[str, list[dict[str, Any]]] = json.loads(resp.data.decode())
        agent._oai_messages.clear()  # type: ignore[attr-defined]
        agent._oai_messages.update(data)  # type: ignore[attr-defined]

    async def save_context(self, context: dict[str, Any]) -> None:
        """Persist an arbitrary context dict."""
        payload = json.dumps(context).encode()
        resp = await self._client.checkpoint_save(payload, key=self._ctx_key, timeout=self._timeout)
        _warn_if_unsuccessful(resp, "save_context", self._ctx_key)

    async def load_context(self) -> dict[str, Any] | None:
        """Load context dict from checkpoint; returns None if not found."""
        resp = await self._client.checkpoint_load(key=self._ctx_key, timeout=self._timeout)
        if resp is None or not resp.data:
            return None
        return json.loads(resp.data.decode())


def _warn_if_unsuccessful(resp: Any, op: str, key: str) -> None:
    if resp is None:
        logger.warning("checkpoint %s timed out for key=%r", op, key)
        return
    success = getattr(resp, "success", True)
    if success is False:
        logger.warning("checkpoint %s reported failure for key=%r", op, key)


def restore_into_agent(
    agent: "ConversableAgent",
    peer_lookup: Callable[[str], Any],
) -> None:
    """Re-key agent._oai_messages from string peer names back to Agent objects.

    Call after load_history() when actual peer Agent instances are available.
    Entries whose name is not found via peer_lookup are kept as-is (string key).
    """
    old: dict[Any, list[dict[str, Any]]] = dict(agent._oai_messages)  # type: ignore[attr-defined]
    agent._oai_messages.clear()  # type: ignore[attr-defined]
    for key, messages in old.items():
        if isinstance(key, str):
            try:
                resolved = peer_lookup(key)
            except Exception:
                resolved = key
        else:
            resolved = key
        agent._oai_messages[resolved] = messages  # type: ignore[attr-defined]
