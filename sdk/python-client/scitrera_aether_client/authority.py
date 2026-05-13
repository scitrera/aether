"""Authority resolution adapter for the Aether async client.

This module exposes :class:`AsyncAuthorityResolver`, a thin caching
adapter on top of :meth:`BaseAsyncAetherClient.resolve_authority`.
The proxy-http terminator (Phase 3.5c) uses it to validate incoming
``X-Aether-Grant-ID`` headers and project the validated grant into
the minted authorization headers it forwards to the backend.

Design notes:

* **Pull-based.** Cache entries expire by ``min(grant.expires_at - now,
  max_ttl_s)``. There is no push invalidation; an explicit
  :meth:`AsyncAuthorityResolver.invalidate` hook is reserved for when
  push invalidation lands.
* **Single-flight.** Concurrent in-flight resolves for the same key
  collapse via a per-key :class:`asyncio.Lock`.
* **LRU eviction.** A simple :class:`collections.OrderedDict` plus
  ``move_to_end`` keeps eviction O(1) without pulling in third-party
  caches.
* **None on ``ok=false``.** The terminator interprets ``None`` as
  "no grant — fall back to direct mode or fail per its policy".
"""
from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Protocol, Tuple


@dataclass(frozen=True)
class ResolvedAuthorityInfo:
    """Validated, projected grant info — what the terminator needs to mint headers.

    Mirrors the fields of :class:`aether_pb2.AuthorityGrantInfo` so the
    terminator never has to reach back into the proto. Frozen so it's
    safe to share across coroutines.
    """

    grant_id: str
    subject_type: str
    subject_id: str
    root_subject_type: str
    root_subject_id: str
    audience_type: str
    audience_id: str
    max_access_level: int
    workspace_scope: Tuple[str, ...]
    expires_at: int  # unix seconds; 0 if unset
    revoked: bool


class AuthorityResolverProtocol(Protocol):
    """Structural type the proxy terminator depends on.

    Lets tests substitute fakes without subclassing
    :class:`AsyncAuthorityResolver`.
    """

    async def resolve(
        self,
        grant_id: str,
        subject_type: str,
        subject_id: str,
        *,
        actor_type: Optional[str] = None,
        actor_id: Optional[str] = None,
    ) -> Optional[ResolvedAuthorityInfo]: ...


# Cache key is (actor_id_canonical, grant_id, subject_id).
# actor_id_canonical may be None when the client identity is not yet
# resolved at the time of the call.
_CacheKey = Tuple[Optional[str], str, str]


@dataclass
class _CacheEntry:
    info: ResolvedAuthorityInfo
    cached_at: float
    expires_at_local: float  # absolute deadline in resolver clock seconds


class AsyncAuthorityResolver:
    """Caches resolved grants in-memory keyed by ``(actor_id, grant_id, subject_id)``.

    TTL is ``min(grant.expires_at - now, max_ttl_s)``. Pull-based; no push
    invalidation today (see module docstring).
    """

    def __init__(
        self,
        client: Any,  # BaseAsyncAetherClient (kept loose to ease testing)
        *,
        max_ttl_s: int = 60,
        max_entries: int = 10_000,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_ttl_s <= 0:
            raise ValueError("max_ttl_s must be positive")
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._client = client
        self._max_ttl_s = float(max_ttl_s)
        self._max_entries = int(max_entries)
        self._clock = clock

        self._cache: "OrderedDict[_CacheKey, _CacheEntry]" = OrderedDict()
        # Per-key lock for single-flight collapsing of concurrent resolves.
        self._inflight_locks: Dict[_CacheKey, asyncio.Lock] = {}
        # Guards _cache and _inflight_locks; held only briefly.
        self._cache_lock = asyncio.Lock()

        self._hits = 0
        self._misses = 0
        self._evictions = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def resolve(
        self,
        grant_id: str,
        subject_type: str,
        subject_id: str,
        *,
        actor_type: Optional[str] = None,
        actor_id: Optional[str] = None,
    ) -> Optional[ResolvedAuthorityInfo]:
        """Resolve a grant, returning cached info when available.

        Returns ``None`` when the gateway responds with ``ok=false``
        (no grant, expired, revoked, not authorized, etc.). Negative
        results are NOT cached — the resolver re-asks each time so a
        freshly-issued grant becomes visible without a cache flush.
        """
        canonical_actor = self._canonical_actor_id(actor_id)
        key: _CacheKey = (canonical_actor, grant_id, subject_id)

        # Fast path — cache hit while not expired.
        cached = await self._get_unexpired(key)
        if cached is not None:
            self._hits += 1
            return cached

        # Slow path — single-flight per key.
        lock = await self._get_inflight_lock(key)
        async with lock:
            # Re-check after acquiring the lock; another coroutine may have
            # populated the cache while we were queued behind it.
            cached = await self._get_unexpired(key)
            if cached is not None:
                self._hits += 1
                return cached

            self._misses += 1
            response = await self._client.resolve_authority(
                grant_id,
                subject_type,
                subject_id,
                actor_type=actor_type,
                actor_id=actor_id,
            )
            if response is None or not getattr(response, "ok", False):
                # Timeout, error, or explicit deny — do not cache.
                return None

            authority = response.authority
            grant = authority.grant
            info = ResolvedAuthorityInfo(
                grant_id=grant.grant_id,
                subject_type=grant.subject_type,
                subject_id=grant.subject_id,
                root_subject_type=grant.root_subject_type,
                root_subject_id=grant.root_subject_id,
                audience_type=grant.audience_type,
                audience_id=grant.audience_id,
                max_access_level=int(grant.max_access_level),
                workspace_scope=tuple(grant.workspace_scope),
                expires_at=int(grant.expires_at),
                revoked=bool(grant.revoked),
            )

            await self._store(key, info)
            return info

    def invalidate(
        self,
        grant_id: str,
        *,
        actor_id: Optional[str] = None,
        subject_id: Optional[str] = None,
    ) -> None:
        """Drop matching cache entries.

        Passing only ``grant_id`` invalidates every entry for that grant,
        regardless of actor/subject. Provide ``actor_id`` and/or
        ``subject_id`` to narrow the scope.
        """
        canonical_actor = (
            self._canonical_actor_id(actor_id) if actor_id is not None else None
        )
        match_actor = actor_id is not None
        match_subject = subject_id is not None

        to_drop = []
        for key in self._cache:
            cached_actor, cached_grant, cached_subject = key
            if cached_grant != grant_id:
                continue
            if match_actor and cached_actor != canonical_actor:
                continue
            if match_subject and cached_subject != subject_id:
                continue
            to_drop.append(key)

        for key in to_drop:
            self._cache.pop(key, None)

    def stats(self) -> Dict[str, int]:
        """Return cache stats: ``hits``, ``misses``, ``size``, ``evictions``."""
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": len(self._cache),
            "evictions": self._evictions,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _canonical_actor_id(self, actor_id: Optional[str]) -> Optional[str]:
        """Resolve the cache-key actor_id, defaulting to the client's identity.

        Returns ``None`` when neither an explicit actor_id nor a connected
        identity is available; callers tolerate that and the cache key is
        valid either way.
        """
        if actor_id is not None:
            return actor_id
        identity = getattr(self._client, "identity", None)
        if identity is None:
            return None
        return getattr(identity, "actor_id", None)

    async def _get_inflight_lock(self, key: _CacheKey) -> asyncio.Lock:
        async with self._cache_lock:
            lock = self._inflight_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._inflight_locks[key] = lock
            return lock

    async def _get_unexpired(self, key: _CacheKey) -> Optional[ResolvedAuthorityInfo]:
        async with self._cache_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if self._clock() >= entry.expires_at_local:
                # Expired — drop and miss.
                self._cache.pop(key, None)
                return None
            # LRU touch.
            self._cache.move_to_end(key)
            return entry.info

    async def _store(self, key: _CacheKey, info: ResolvedAuthorityInfo) -> None:
        now = self._clock()
        ttl = self._max_ttl_s
        if info.expires_at > 0:
            grant_remaining = float(info.expires_at) - now
            # If the grant is already expired by the gateway's clock,
            # avoid caching a negative-TTL entry; serve once and re-ask.
            if grant_remaining <= 0:
                return
            ttl = min(ttl, grant_remaining)
        deadline = now + ttl

        async with self._cache_lock:
            self._cache[key] = _CacheEntry(
                info=info,
                cached_at=now,
                expires_at_local=deadline,
            )
            self._cache.move_to_end(key)
            # Simple LRU eviction.
            while len(self._cache) > self._max_entries:
                self._cache.popitem(last=False)
                self._evictions += 1
