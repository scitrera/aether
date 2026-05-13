"""Tests for Phase 3.5b: ``resolve_authority``/``connection_status`` client methods
and the :class:`AsyncAuthorityResolver` adapter.

Mirrors the ``_AsyncClientStub`` pattern from ``test_proxy_terminator.py`` —
the client surface is faked just enough for the resolver and the new
``BaseAsyncAetherClient`` methods.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import List, Optional

import pytest

from scitrera_aether_client.authority import (
    AsyncAuthorityResolver,
    ResolvedAuthorityInfo,
)
from scitrera_aether_client.client_async import BaseAsyncAetherClient
from scitrera_aether_client.proto import aether_pb2


# ---------------------------------------------------------------------------
# Client correlation tests (resolve_authority + connection_status)
# ---------------------------------------------------------------------------


class _FakeIdentity:
    def __init__(self, actor_id: str) -> None:
        self.actor_id = actor_id
        self.principal_type = "agent"


def _make_async_client() -> BaseAsyncAetherClient:
    """Bare ``BaseAsyncAetherClient`` with no real connection — just enough
    plumbing to test request_id correlation through ``_send_sync_op``."""
    client = BaseAsyncAetherClient.__new__(BaseAsyncAetherClient)
    BaseAsyncAetherClient.__init__(client, auto_reconnect=False)
    return client


async def _drain_one_upstream(client: BaseAsyncAetherClient) -> aether_pb2.UpstreamMessage:
    """Wait for the next upstream message the client wants to send."""
    return await asyncio.wait_for(client._request_queue.get(), timeout=1.0)


async def _inject_response(
    client: BaseAsyncAetherClient, response: aether_pb2.DownstreamMessage
) -> None:
    """Manually flow a downstream response through the same dispatch the
    listen loop would use, completing any pending future."""
    payload_type = response.WhichOneof("payload")
    if payload_type == "resolve_authority_response":
        resp = response.resolve_authority_response
    elif payload_type == "connection_status_response":
        resp = response.connection_status_response
    else:
        raise AssertionError(f"unsupported payload_type for test: {payload_type}")
    pending = client._pending_requests.pop(resp.request_id, None)
    assert pending is not None, f"no pending future for request_id={resp.request_id!r}"
    pending.set_result(resp)


@pytest.mark.asyncio
async def test_client_resolve_authority_returns_response_proto():
    client = _make_async_client()

    # Build the canned response we'll inject once the client emits the request.
    expected = aether_pb2.ResolveAuthorityResponse(
        ok=True,
        authority=aether_pb2.ResolvedAuthority(
            grant=aether_pb2.AuthorityGrantInfo(
                grant_id="grant-abc",
                subject_type="user",
                subject_id="user-42",
                max_access_level=3,
                workspace_scope=["ws-1"],
            ),
        ),
    )

    async def driver() -> aether_pb2.ResolveAuthorityResponse:
        return await client.resolve_authority(
            grant_id="grant-abc",
            subject_type="user",
            subject_id="user-42",
        )

    task = asyncio.create_task(driver())
    upstream = await _drain_one_upstream(client)
    assert upstream.WhichOneof("payload") == "resolve_authority_request"
    req = upstream.resolve_authority_request
    assert req.grant_id == "grant-abc"
    assert req.subject.principal_type == "user"
    assert req.subject.principal_id == "user-42"
    assert req.request_id  # uuid4 default

    # Echo the request_id back in the canned response and dispatch.
    expected.request_id = req.request_id
    response = aether_pb2.DownstreamMessage()
    response.resolve_authority_response.CopyFrom(expected)
    await _inject_response(client, response)

    got = await asyncio.wait_for(task, timeout=1.0)
    assert got is not None
    assert got.ok is True
    assert got.request_id == req.request_id
    assert got.authority.grant.grant_id == "grant-abc"


@pytest.mark.asyncio
async def test_client_connection_status_returns_response_proto():
    client = _make_async_client()

    async def driver() -> aether_pb2.ConnectionStatusResponse:
        return await client.connection_status(
            principal_type="agent",
            principal_id="ag::ws::impl::spec",
        )

    task = asyncio.create_task(driver())
    upstream = await _drain_one_upstream(client)
    assert upstream.WhichOneof("payload") == "connection_status_request"
    req = upstream.connection_status_request
    assert req.principal.principal_type == "agent"
    assert req.principal.principal_id == "ag::ws::impl::spec"
    assert req.request_id

    canned = aether_pb2.ConnectionStatusResponse(
        request_id=req.request_id,
        ok=True,
        connected=True,
        last_seen_at=1_700_000_000,
    )
    response = aether_pb2.DownstreamMessage()
    response.connection_status_response.CopyFrom(canned)
    await _inject_response(client, response)

    got = await asyncio.wait_for(task, timeout=1.0)
    assert got is not None
    assert got.ok is True
    assert got.connected is True
    assert got.last_seen_at == 1_700_000_000


# ---------------------------------------------------------------------------
# Resolver tests — use a fake client mock surface
# ---------------------------------------------------------------------------


class _FakeClient:
    """Minimal stand-in for ``BaseAsyncAetherClient.resolve_authority``.

    Tracks call count and lets tests stage responses or block via an event.
    """

    def __init__(
        self,
        *,
        response: Optional[aether_pb2.ResolveAuthorityResponse] = None,
        identity: Optional[_FakeIdentity] = None,
        gate: Optional[asyncio.Event] = None,
    ) -> None:
        self.calls: List[dict] = []
        self._response = response
        self.identity = identity
        self._gate = gate

    async def resolve_authority(
        self,
        grant_id: str,
        subject_type: str,
        subject_id: str,
        *,
        actor_type: Optional[str] = None,
        actor_id: Optional[str] = None,
    ):
        self.calls.append(
            {
                "grant_id": grant_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "actor_type": actor_type,
                "actor_id": actor_id,
            }
        )
        if self._gate is not None:
            await self._gate.wait()
        return self._response


def _make_response(
    *,
    grant_id: str = "grant-1",
    subject_type: str = "user",
    subject_id: str = "user-1",
    expires_at: int = 0,
    max_access_level: int = 2,
    workspace_scope: Optional[List[str]] = None,
    ok: bool = True,
    error: str = "",
) -> aether_pb2.ResolveAuthorityResponse:
    if ok:
        return aether_pb2.ResolveAuthorityResponse(
            ok=True,
            authority=aether_pb2.ResolvedAuthority(
                grant=aether_pb2.AuthorityGrantInfo(
                    grant_id=grant_id,
                    subject_type=subject_type,
                    subject_id=subject_id,
                    max_access_level=max_access_level,
                    workspace_scope=workspace_scope or [],
                    expires_at=expires_at,
                ),
            ),
        )
    return aether_pb2.ResolveAuthorityResponse(ok=False, error=error)


@pytest.mark.asyncio
async def test_resolver_caches_repeated_lookups():
    client = _FakeClient(response=_make_response())
    resolver = AsyncAuthorityResolver(client, max_ttl_s=60)

    first = await resolver.resolve("grant-1", "user", "user-1")
    second = await resolver.resolve("grant-1", "user", "user-1")

    assert first is not None
    assert second is not None
    assert first == second
    assert isinstance(first, ResolvedAuthorityInfo)
    assert len(client.calls) == 1
    stats = resolver.stats()
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["size"] == 1


@pytest.mark.asyncio
async def test_resolver_respects_grant_expires_at():
    # grant.expires_at is 100; we'll advance the resolver clock past it.
    fake_now = {"t": 50.0}

    def clock() -> float:
        return fake_now["t"]

    client = _FakeClient(response=_make_response(expires_at=100))
    resolver = AsyncAuthorityResolver(client, max_ttl_s=600, clock=clock)

    info = await resolver.resolve("grant-1", "user", "user-1")
    assert info is not None
    assert len(client.calls) == 1

    # Cache hit while still within the grant window.
    fake_now["t"] = 99.0
    info2 = await resolver.resolve("grant-1", "user", "user-1")
    assert info2 is not None
    assert len(client.calls) == 1

    # Past expiry — must refetch.
    fake_now["t"] = 101.0
    info3 = await resolver.resolve("grant-1", "user", "user-1")
    assert info3 is not None
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_resolver_respects_max_ttl():
    fake_now = {"t": 1000.0}

    def clock() -> float:
        return fake_now["t"]

    # grant.expires_at far in the future; max_ttl_s=1 forces refetch quickly.
    client = _FakeClient(response=_make_response(expires_at=10_000_000))
    resolver = AsyncAuthorityResolver(client, max_ttl_s=1, clock=clock)

    await resolver.resolve("grant-1", "user", "user-1")
    assert len(client.calls) == 1

    # Within TTL — cache hit.
    fake_now["t"] = 1000.5
    await resolver.resolve("grant-1", "user", "user-1")
    assert len(client.calls) == 1

    # Past TTL — refetch.
    fake_now["t"] = 1001.5
    await resolver.resolve("grant-1", "user", "user-1")
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_resolver_returns_none_on_ok_false():
    client = _FakeClient(response=_make_response(ok=False, error="not authorized"))
    resolver = AsyncAuthorityResolver(client)

    out = await resolver.resolve("grant-1", "user", "user-1")
    assert out is None
    # Negative responses are not cached — a follow-up call hits the gateway again.
    out2 = await resolver.resolve("grant-1", "user", "user-1")
    assert out2 is None
    assert len(client.calls) == 2
    stats = resolver.stats()
    assert stats["size"] == 0


@pytest.mark.asyncio
async def test_resolver_collapses_concurrent_inflight():
    gate = asyncio.Event()
    client = _FakeClient(response=_make_response(), gate=gate)
    resolver = AsyncAuthorityResolver(client)

    # Two concurrent resolves for the same key: only one should hit the
    # underlying client.
    task1 = asyncio.create_task(resolver.resolve("grant-1", "user", "user-1"))
    task2 = asyncio.create_task(resolver.resolve("grant-1", "user", "user-1"))

    # Yield until both tasks are queued behind the gate / single-flight lock.
    for _ in range(5):
        await asyncio.sleep(0)

    gate.set()
    out1, out2 = await asyncio.gather(task1, task2)
    assert out1 is not None
    assert out2 is not None
    assert out1 == out2
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_resolver_invalidate_clears_entry():
    client = _FakeClient(response=_make_response())
    resolver = AsyncAuthorityResolver(client)

    await resolver.resolve("grant-1", "user", "user-1")
    assert len(client.calls) == 1
    assert resolver.stats()["size"] == 1

    resolver.invalidate("grant-1")
    assert resolver.stats()["size"] == 0

    await resolver.resolve("grant-1", "user", "user-1")
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_resolver_lru_evicts_oldest_at_max_entries():
    # Each call returns a different grant so all three populate distinct keys.
    class _MultiClient:
        def __init__(self) -> None:
            self.calls: List[str] = []

        async def resolve_authority(
            self,
            grant_id: str,
            subject_type: str,
            subject_id: str,
            *,
            actor_type: Optional[str] = None,
            actor_id: Optional[str] = None,
        ):
            self.calls.append(grant_id)
            return _make_response(grant_id=grant_id, subject_id=subject_id)

        @property
        def identity(self):
            return None

    client = _MultiClient()
    resolver = AsyncAuthorityResolver(client, max_entries=2)

    await resolver.resolve("g1", "user", "u1")
    await resolver.resolve("g2", "user", "u2")
    assert resolver.stats()["size"] == 2

    await resolver.resolve("g3", "user", "u3")
    stats = resolver.stats()
    assert stats["size"] == 2
    assert stats["evictions"] == 1

    # g1 should have been evicted (least-recently inserted/used).
    # Re-resolving it must hit the underlying client again.
    await resolver.resolve("g1", "user", "u1")
    # Calls so far: g1, g2, g3, g1 -> 4 calls
    assert client.calls == ["g1", "g2", "g3", "g1"]


@pytest.mark.asyncio
async def test_resolver_uses_client_identity_when_actor_omitted():
    identity = _FakeIdentity(actor_id="ag::ws::impl::spec")
    client = _FakeClient(response=_make_response(), identity=identity)
    resolver = AsyncAuthorityResolver(client)

    info = await resolver.resolve("grant-1", "user", "user-1")
    assert info is not None
    # Client received the call with no explicit actor override (mirrors the
    # gateway-uses-connection-identity contract). Cache key MUST embed the
    # identity-derived actor_id, so a follow-up call collapses on the cache.
    await resolver.resolve("grant-1", "user", "user-1")
    assert len(client.calls) == 1

    # Confirm the cache key actually used the identity-derived actor_id by
    # peeking at the internal cache (acceptable for white-box test).
    keys = list(resolver._cache.keys())  # noqa: SLF001 — intentional white-box
    assert keys == [("ag::ws::impl::spec", "grant-1", "user-1")]


@pytest.mark.asyncio
async def test_resolver_handles_client_identity_unavailable():
    # client.identity is None — no AttributeError, cache key uses None actor.
    client = _FakeClient(response=_make_response(), identity=None)
    resolver = AsyncAuthorityResolver(client)

    info = await resolver.resolve("grant-1", "user", "user-1")
    assert info is not None
    keys = list(resolver._cache.keys())  # noqa: SLF001 — intentional white-box
    assert keys == [(None, "grant-1", "user-1")]
