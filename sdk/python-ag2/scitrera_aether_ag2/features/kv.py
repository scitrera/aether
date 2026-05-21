"""AetherKVNamespace — dict-like wrapper around Aether KV scopes for ag2 tool code."""

# host integration:
#   - AetherAgentHost can expose `host.kv = AetherKVNamespace(self._client, scope="user-workspace", workspace=self.identity.workspace)` after connect
#   - ConversableAgent tool code can then `await host.kv.get(...)` / `put(...)`

from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator

logger = logging.getLogger(__name__)

# Valid KV scope strings accepted by the Aether async client.
_VALID_SCOPES = frozenset({
    "global",
    "workspace",
    "user",
    "user-workspace",
})


class AetherKVNamespace:
    """Scoped, JSON-serializing wrapper around the Aether KV API.

    All values are JSON-encoded on write and decoded on read so callers
    work with native Python types rather than raw bytes.
    """

    def __init__(
        self,
        client: Any,
        scope: str = "user-workspace",
        workspace: str | None = None,
        prefix: str = "",
    ) -> None:
        if scope not in _VALID_SCOPES:
            raise ValueError(
                f"invalid scope {scope!r}; must be one of {sorted(_VALID_SCOPES)}"
            )
        self._client = client
        self._scope = scope
        self._workspace = workspace or ""
        self._prefix = prefix

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}" if self._prefix else key

    def _kv_kwargs(self) -> dict[str, str]:
        kwargs: dict[str, str] = {"scope": self._scope}
        if self._workspace:
            kwargs["workspace"] = self._workspace
        return kwargs

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    async def get(self, key: str, default: Any = None) -> Any:
        """Return the value stored at *key*, or *default* if absent."""
        resp = await self._client.kv_get(self._full_key(key), **self._kv_kwargs())
        if resp is None or not resp.success or not resp.value:
            return default
        try:
            return json.loads(resp.value)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("AetherKVNamespace.get: failed to decode key %r: %s", key, exc)
            return default

    async def put(self, key: str, value: Any) -> None:
        """Serialize *value* as JSON and store it at *key*."""
        encoded = json.dumps(value).encode()
        await self._client.kv_put(self._full_key(key), encoded, **self._kv_kwargs())

    async def delete(self, key: str) -> None:
        """Delete *key* from the namespace."""
        await self._client.kv_delete(self._full_key(key), **self._kv_kwargs())

    async def list(self, key_prefix: str = "") -> list[str]:
        """Return keys under this namespace, optionally filtered by *key_prefix*.

        The returned keys have the namespace prefix stripped so they
        match what was passed to put/get/delete.
        """
        full_prefix = self._full_key(key_prefix)
        resp = await self._client.kv_list(full_prefix, **self._kv_kwargs())
        if resp is None or not resp.success:
            return []
        raw_keys: list[str] = list(resp.keys)
        if self._prefix:
            # Strip the namespace prefix so callers see bare keys.
            return [k[len(self._prefix):] for k in raw_keys if k.startswith(self._prefix)]
        return raw_keys

    async def increment(self, key: str) -> int:
        """Atomically increment the counter at *key* and return the new value."""
        resp = await self._client.kv_increment(self._full_key(key), **self._kv_kwargs())
        if resp is None:
            raise RuntimeError(f"kv_increment timed out for key {key!r}")
        return int(resp.counter_value)

    async def decrement(self, key: str) -> int:
        """Atomically decrement the counter at *key* and return the new value."""
        resp = await self._client.kv_decrement(self._full_key(key), **self._kv_kwargs())
        if resp is None:
            raise RuntimeError(f"kv_decrement timed out for key {key!r}")
        return int(resp.counter_value)

    def __aiter__(self) -> AsyncIterator[str]:
        return _KVAsyncIterator(self)


class _KVAsyncIterator:
    """AsyncIterator that yields keys from AetherKVNamespace.list("")."""

    __slots__ = ("_ns", "_keys", "_index")

    def __init__(self, ns: AetherKVNamespace) -> None:
        self._ns = ns
        self._keys: list[str] | None = None
        self._index = 0

    def __aiter__(self) -> "_KVAsyncIterator":
        return self

    async def __anext__(self) -> str:
        if self._keys is None:
            self._keys = await self._ns.list("")
        if self._index >= len(self._keys):
            raise StopAsyncIteration
        key = self._keys[self._index]
        self._index += 1
        return key
