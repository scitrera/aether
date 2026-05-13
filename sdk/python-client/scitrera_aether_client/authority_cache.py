"""Authority grant cache for the Aether async client.

This module exposes :class:`AuthorityGrantCache`, a per-actor cache that
keeps recently-issued authority grants warm and proactively invalidates
them when the gateway pushes ``AuthorityGrantRevocation`` events on the
downstream stream.

Caches existing grants keyed by ``(source_session_id, audience_type,
audience_id)``. A cached entry is served while:

* it is not past its server-side ``expires_at`` minus ``soft_renew_skew_s``
  (so callers never receive a grant that's about to expire mid-use), AND
* the gateway has not pushed a ``AuthorityGrantRevocation`` for it (or for
  one of its parents in the same delegation chain).

When the soft-renew window is hit the cache transparently re-exchanges
via :meth:`AsyncServiceClient.exchange_authority_grant`. When the server
includes a ``cache_hint_ttl_seconds`` in its response, that is used as an
upper bound on the soft-renew deadline.

Designed to replace ad-hoc auth context snapshot patterns in client
applications.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from .proto import aether_pb2

logger = logging.getLogger(__name__)


# Cache key is (source_session_id, audience_type, audience_id). Empty
# strings are valid components (e.g. an exchange with no audience).
_CacheKey = Tuple[str, str, str]


@dataclass
class _CacheEntry:
    """One cached grant plus the metadata needed to decide when to refresh."""

    grant: aether_pb2.ACLAuthorityGrantInfo
    expires_at_ms: int  # absolute unix-ms; 0 if grant has no expiry
    cache_hint_ttl_s: int  # 0 if server didn't supply a hint
    cached_at: float  # local monotonic-ish wall-clock of when we stored it


class AuthorityGrantCache:
    """Caches runtime authority grants and listens for revocation pushes.

    Args:
        client: The :class:`AsyncServiceClient` (or any client exposing
            :meth:`exchange_authority_grant`, :meth:`derive_authority_grant`,
            :meth:`derive_authority_grant_for_target`, and
            :meth:`revoke_authority_grant`) the cache will issue ops against.
        soft_renew_skew_s: Re-exchange a grant this many seconds *before*
            its server-side ``expires_at``. Defaults to 30 s, matching the
            connection-lock heartbeat cadence.
        clock: Wall-clock provider returning seconds; pluggable for tests.
        op_timeout: Default timeout (seconds) passed to grant ops.
    """

    def __init__(
        self,
        client: Any,
        *,
        soft_renew_skew_s: float = 30.0,
        clock: Callable[[], float] = time.time,
        op_timeout: float = 10.0,
    ) -> None:
        if soft_renew_skew_s < 0:
            raise ValueError("soft_renew_skew_s must be non-negative")
        self._client = client
        self._soft_renew_skew_s = float(soft_renew_skew_s)
        self._clock = clock
        self._op_timeout = float(op_timeout)

        self._cache: Dict[_CacheKey, _CacheEntry] = {}
        # Single-flight per (key) to collapse concurrent get_or_exchange.
        self._inflight: Dict[_CacheKey, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

        # Track grant_id -> set of cache keys for fast revocation lookup.
        self._grant_id_index: Dict[str, List[_CacheKey]] = {}
        # Track root_grant_id -> set of cache keys (cascade invalidation).
        self._root_grant_id_index: Dict[str, List[_CacheKey]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_or_exchange(
        self,
        source_session_id: str,
        audience_type: str = "",
        audience_id: str = "",
        **exchange_kwargs: Any,
    ) -> Optional[aether_pb2.ACLAuthorityGrantInfo]:
        """Return a cached grant for the key, or exchange one if missing/stale.

        ``exchange_kwargs`` are forwarded to
        :meth:`AsyncServiceClient.exchange_authority_grant`. The cache only
        checks freshness for ``(source_session_id, audience_type,
        audience_id)`` — callers MUST keep the rest of the request shape
        stable for a given key.

        Returns ``None`` when the gateway response is missing or its
        ``success`` flag is false; callers should fall back to direct
        invocation or surface the error.
        """
        key: _CacheKey = (source_session_id, audience_type, audience_id)

        cached = self._get_unexpired(key)
        if cached is not None:
            return cached

        lock = await self._inflight_lock(key)
        async with lock:
            cached = self._get_unexpired(key)
            if cached is not None:
                return cached

            timeout = float(exchange_kwargs.pop("timeout", self._op_timeout))
            response = await self._client.exchange_authority_grant(
                source_session_id=source_session_id,
                audience_type=audience_type,
                audience_id=audience_id,
                timeout=timeout,
                **exchange_kwargs,
            )
            grant = self._extract_grant(response)
            if grant is None:
                return None
            self._store(key, grant, response)
            return grant

    async def derive_for_task(
        self,
        parent_grant_id: str,
        task_id: str,
        target_type: str = "task",
        audience_type: str = "",
        audience_id: str = "",
        **kwargs: Any,
    ) -> Optional[aether_pb2.ACLAuthorityGrantInfo]:
        """Derive (idempotently) a grant scoped for a target task.

        Uses the ``DERIVE_FOR_TARGET`` op so the gateway returns an
        existing visible grant when one is already in place — making this
        safe to call repeatedly without leaking grants.

        ``kwargs`` are forwarded to
        :meth:`AsyncServiceClient.derive_authority_grant_for_target`.
        """
        key: _CacheKey = (f"derive::{parent_grant_id}::{task_id}", audience_type, audience_id)

        cached = self._get_unexpired(key)
        if cached is not None:
            return cached

        lock = await self._inflight_lock(key)
        async with lock:
            cached = self._get_unexpired(key)
            if cached is not None:
                return cached

            timeout = float(kwargs.pop("timeout", self._op_timeout))
            response = await self._client.derive_authority_grant_for_target(
                parent_grant_id=parent_grant_id,
                target_type=target_type,
                target_id=task_id,
                audience_type=audience_type,
                audience_id=audience_id,
                timeout=timeout,
                **kwargs,
            )
            grant = self._extract_grant(response)
            if grant is None:
                return None
            self._store(key, grant, response)
            return grant

    def invalidate(self, grant_id: str) -> int:
        """Drop every cache entry whose grant ID or root grant ID matches.

        Returns the number of entries dropped. Safe to call from sync
        contexts (e.g. inside the downstream-message dispatch).
        """
        keys_to_drop: List[_CacheKey] = []
        keys_to_drop.extend(self._grant_id_index.get(grant_id, ()))
        keys_to_drop.extend(self._root_grant_id_index.get(grant_id, ()))
        # Deduplicate — a grant may live under both indexes if it's a root.
        dropped = 0
        for key in dict.fromkeys(keys_to_drop):
            entry = self._cache.pop(key, None)
            if entry is not None:
                self._unindex(key, entry.grant)
                dropped += 1
        # Also clean the indexes if they still point at empty lists.
        self._grant_id_index.pop(grant_id, None)
        self._root_grant_id_index.pop(grant_id, None)
        return dropped

    async def revoke_all(self) -> None:
        """Best-effort revoke every cached grant on the gateway, then clear.

        Failures are logged but do not propagate — callers typically use
        this during shutdown where re-throwing would mask the underlying
        teardown.
        """
        # Snapshot under the lock; revoke without it so individual op
        # failures do not block other entries.
        async with self._lock:
            entries = list(self._cache.values())
            self._cache.clear()
            self._grant_id_index.clear()
            self._root_grant_id_index.clear()

        for entry in entries:
            grant_id = entry.grant.grant_id
            if not grant_id:
                continue
            try:
                await self._client.revoke_authority_grant(
                    grant_id, timeout=self._op_timeout,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                logger.warning(
                    "AuthorityGrantCache.revoke_all: revoke %s failed: %s",
                    grant_id, exc,
                )

    def stats(self) -> Dict[str, int]:
        """Return cache stats for observability tests."""
        return {
            "size": len(self._cache),
            "grant_ids_indexed": len(self._grant_id_index),
            "root_grant_ids_indexed": len(self._root_grant_id_index),
        }

    # ------------------------------------------------------------------
    # High-level convenience helpers (Phase 4)
    # ------------------------------------------------------------------

    def is_grant_valid(self, grant_id: str) -> bool:
        """Return ``True`` iff ``grant_id`` is in cache and not revoked/stale.

        This is a cache-only predicate — it never round-trips to the
        gateway. Useful for fast guards in hot paths where you already
        seeded the cache via :meth:`get_or_exchange` /
        :meth:`derive_for_task`.
        """
        if not grant_id:
            return False
        keys = self._grant_id_index.get(grant_id)
        if not keys:
            return False
        # ``_get_unexpired`` drops the entry as a side-effect if stale, so
        # call it via any associated key — they all share the same grant.
        # Snapshot first because the index may mutate during iteration.
        for key in list(keys):
            if self._get_unexpired(key) is not None:
                return True
        return False

    def list_active_grants(self) -> List[aether_pb2.ACLAuthorityGrantInfo]:
        """Return a snapshot of every cached grant that is currently fresh.

        Stale or revoked entries are dropped as a side-effect of this
        call (same eviction path as :meth:`get_or_exchange`). The
        returned list is de-duplicated by ``grant_id`` so the same grant
        cached under multiple keys appears once.
        """
        seen: Dict[str, aether_pb2.ACLAuthorityGrantInfo] = {}
        for key in list(self._cache.keys()):
            grant = self._get_unexpired(key)
            if grant is None or not grant.grant_id:
                continue
            seen.setdefault(grant.grant_id, grant)
        return list(seen.values())

    def revoke_local(self, grant_id: str) -> int:
        """Drop ``grant_id`` from the cache without calling the gateway.

        Alias of :meth:`invalidate` with a name that telegraphs the
        local-only semantics. The matching server-side revoke lives on
        the admin client / :meth:`AsyncServiceClient.revoke_authority_grant`.
        Returns the number of entries dropped.
        """
        return self.invalidate(grant_id)

    async def refresh(
        self,
        grant_id: str,
        **exchange_kwargs: Any,
    ) -> Optional[aether_pb2.ACLAuthorityGrantInfo]:
        """Force-drop a cached grant and re-exchange it.

        Returns the fresh grant on success or ``None`` when the cache
        doesn't know the grant or the re-exchange fails. ``exchange_kwargs``
        are forwarded to :meth:`get_or_exchange` — pass ``timeout`` here to
        override the cache default.
        """
        if not grant_id:
            return None
        keys = list(self._grant_id_index.get(grant_id, ()))
        if not keys:
            return None
        # Snapshot the first key's tuple — we re-use it as the
        # re-exchange identity (source_session_id, audience_type,
        # audience_id). Derived entries use a synthetic source_session_id
        # of "derive::..." which is harmless when passed back through
        # get_or_exchange because invalidate() drops the matching entry
        # first, forcing the slow path.
        source_session_id, audience_type, audience_id = keys[0]
        self.invalidate(grant_id)
        if source_session_id.startswith("derive::"):
            # Derived entries cannot be refreshed via exchange — caller
            # must re-derive explicitly with the same parent/target.
            return None
        return await self.get_or_exchange(
            source_session_id,
            audience_type,
            audience_id,
            **exchange_kwargs,
        )

    # ------------------------------------------------------------------
    # Push-event handling
    # ------------------------------------------------------------------

    def handle_revocation_event(self, evt: aether_pb2.AuthorityGrantRevocation) -> int:
        """Invalidate cache entries for a server-pushed revocation event.

        Returns the number of entries dropped. Wired by
        :meth:`AsyncServiceClient.make_authority_cache` into the downstream
        dispatcher; tests may also call this directly.
        """
        dropped = 0
        if evt.grant_id:
            dropped += self.invalidate(evt.grant_id)
        if evt.root_grant_id and evt.root_grant_id != evt.grant_id:
            dropped += self.invalidate(evt.root_grant_id)
        return dropped

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_unexpired(self, key: _CacheKey) -> Optional[aether_pb2.ACLAuthorityGrantInfo]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        if entry.grant.revoked:
            self._cache.pop(key, None)
            self._unindex(key, entry.grant)
            return None
        # Expiry check — both the server-side ``expires_at`` (seconds) and
        # any cache-hint TTL contribute. Skew lets callers never get a
        # token that's about to expire mid-use.
        now_s = self._clock()
        deadline_s = self._effective_deadline_s(entry)
        if deadline_s > 0 and now_s >= deadline_s:
            self._cache.pop(key, None)
            self._unindex(key, entry.grant)
            return None
        return entry.grant

    def _effective_deadline_s(self, entry: _CacheEntry) -> float:
        """Earliest of (expires_at - skew) and (cached_at + cache_hint_ttl)."""
        candidates: List[float] = []
        if entry.expires_at_ms > 0:
            candidates.append(entry.expires_at_ms / 1000.0 - self._soft_renew_skew_s)
        if entry.cache_hint_ttl_s > 0:
            candidates.append(entry.cached_at + float(entry.cache_hint_ttl_s))
        if not candidates:
            return 0.0  # no deadline -> serve forever (until revocation)
        return min(candidates)

    def _store(
        self,
        key: _CacheKey,
        grant: aether_pb2.ACLAuthorityGrantInfo,
        response: Optional[aether_pb2.AuthorityGrantResponse],
    ) -> None:
        # Drop any prior entry under this key from the indexes.
        prior = self._cache.get(key)
        if prior is not None:
            self._unindex(key, prior.grant)

        # ``expires_at`` on ACLAuthorityGrantInfo is unix-MILLISECONDS
        # (see proto comment). Treat 0 as "no expiry".
        entry = _CacheEntry(
            grant=grant,
            expires_at_ms=int(getattr(grant, "expires_at", 0) or 0),
            cache_hint_ttl_s=int(getattr(response, "cache_hint_ttl_seconds", 0) or 0)
                if response is not None else 0,
            cached_at=self._clock(),
        )
        self._cache[key] = entry
        self._index(key, grant)

    def _index(self, key: _CacheKey, grant: aether_pb2.ACLAuthorityGrantInfo) -> None:
        if grant.grant_id:
            self._grant_id_index.setdefault(grant.grant_id, []).append(key)
        if grant.root_grant_id and grant.root_grant_id != grant.grant_id:
            self._root_grant_id_index.setdefault(grant.root_grant_id, []).append(key)

    def _unindex(self, key: _CacheKey, grant: aether_pb2.ACLAuthorityGrantInfo) -> None:
        for index, idx_key in (
            (self._grant_id_index, grant.grant_id),
            (self._root_grant_id_index, grant.root_grant_id),
        ):
            if not idx_key:
                continue
            keys = index.get(idx_key)
            if not keys:
                continue
            try:
                keys.remove(key)
            except ValueError:
                pass
            if not keys:
                index.pop(idx_key, None)

    async def _inflight_lock(self, key: _CacheKey) -> asyncio.Lock:
        async with self._lock:
            lock = self._inflight.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._inflight[key] = lock
            return lock

    @staticmethod
    def _extract_grant(
        response: Optional[aether_pb2.AuthorityGrantResponse],
    ) -> Optional[aether_pb2.ACLAuthorityGrantInfo]:
        if response is None or not getattr(response, "success", False):
            return None
        grant = getattr(response, "grant", None)
        if grant is None or not grant.grant_id:
            return None
        return grant
