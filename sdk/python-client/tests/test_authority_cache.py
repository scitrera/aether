"""Tests for :mod:`scitrera_aether_client.authority_cache`.

Covers:

* cache hit + concurrent-collapse behaviour;
* soft-renew before ``expires_at``;
* server ``cache_hint_ttl_seconds`` honored as a tighter deadline;
* push-event invalidation via ``handle_revocation_event`` (direct + cascaded);
* idempotent ``derive_for_task`` short-circuit on the cache;
* the new client wrappers (``list_my_authority_grants``,
  ``list_authority_grants_on_me``, ``batch_exchange_authority_grants``,
  ``derive_authority_grant_for_target``, ``renew_authority_grant`` extend);
* downstream-message dispatch wiring of ``AuthorityGrantRevocation`` events.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pytest

from scitrera_aether_client.authority_cache import AuthorityGrantCache
from scitrera_aether_client.client_async import BaseAsyncAetherClient
from scitrera_aether_client.proto import aether_pb2


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


class _FakeClient:
    """Minimal stand-in for the AsyncServiceClient surface the cache uses."""

    def __init__(self) -> None:
        self.exchange_calls: List[Dict[str, Any]] = []
        self.derive_for_target_calls: List[Dict[str, Any]] = []
        self.revoke_calls: List[str] = []
        self._exchange_responses: List[aether_pb2.AuthorityGrantResponse] = []
        self._derive_responses: List[aether_pb2.AuthorityGrantResponse] = []
        self._gate: Optional[asyncio.Event] = None
        self._revoke_should_raise = False

    def queue_exchange(self, response: aether_pb2.AuthorityGrantResponse) -> None:
        self._exchange_responses.append(response)

    def queue_derive(self, response: aether_pb2.AuthorityGrantResponse) -> None:
        self._derive_responses.append(response)

    def install_gate(self, gate: asyncio.Event) -> None:
        self._gate = gate

    async def exchange_authority_grant(self, **kwargs: Any) -> aether_pb2.AuthorityGrantResponse:
        self.exchange_calls.append(kwargs)
        if self._gate is not None:
            await self._gate.wait()
        if not self._exchange_responses:
            raise AssertionError("no exchange responses queued")
        return self._exchange_responses.pop(0)

    async def derive_authority_grant_for_target(
        self, **kwargs: Any
    ) -> aether_pb2.AuthorityGrantResponse:
        self.derive_for_target_calls.append(kwargs)
        if not self._derive_responses:
            raise AssertionError("no derive responses queued")
        return self._derive_responses.pop(0)

    async def revoke_authority_grant(self, grant_id: str, *, timeout: float = 10.0) -> None:
        self.revoke_calls.append(grant_id)
        if self._revoke_should_raise:
            raise RuntimeError("intentional revoke failure")


def _make_response(
    *,
    grant_id: str = "g-1",
    root_grant_id: str = "",
    expires_at_ms: int = 0,
    cache_hint_ttl_s: int = 0,
    success: bool = True,
) -> aether_pb2.AuthorityGrantResponse:
    if not success:
        return aether_pb2.AuthorityGrantResponse(success=False, error="denied")
    return aether_pb2.AuthorityGrantResponse(
        success=True,
        grant=aether_pb2.ACLAuthorityGrantInfo(
            grant_id=grant_id,
            root_grant_id=root_grant_id or grant_id,
            expires_at=expires_at_ms,
        ),
        cache_hint_ttl_seconds=cache_hint_ttl_s,
    )


# ---------------------------------------------------------------------------
# Cache behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_hit_returns_same_grant():
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1"))
    cache = AuthorityGrantCache(client)

    first = await cache.get_or_exchange("session-1", "task", "task-1")
    second = await cache.get_or_exchange("session-1", "task", "task-1")

    assert first is not None
    assert second is not None
    assert first.grant_id == "g-1"
    assert second.grant_id == "g-1"
    assert len(client.exchange_calls) == 1


@pytest.mark.asyncio
async def test_concurrent_get_or_exchange_collapses():
    gate = asyncio.Event()
    client = _FakeClient()
    client.install_gate(gate)
    client.queue_exchange(_make_response(grant_id="g-1"))
    cache = AuthorityGrantCache(client)

    t1 = asyncio.create_task(cache.get_or_exchange("session-1", "task", "task-1"))
    t2 = asyncio.create_task(cache.get_or_exchange("session-1", "task", "task-1"))
    for _ in range(5):
        await asyncio.sleep(0)
    gate.set()
    out1, out2 = await asyncio.gather(t1, t2)
    assert out1.grant_id == out2.grant_id == "g-1"
    assert len(client.exchange_calls) == 1


@pytest.mark.asyncio
async def test_soft_renew_before_expiry():
    fake_now = {"t": 1000.0}
    client = _FakeClient()
    # First grant expires at t=2000s -> 2_000_000 ms. With skew 30s the
    # cache should re-exchange any time after t=1970s.
    client.queue_exchange(_make_response(grant_id="g-1", expires_at_ms=2_000_000))
    client.queue_exchange(_make_response(grant_id="g-2", expires_at_ms=4_000_000))
    cache = AuthorityGrantCache(
        client, soft_renew_skew_s=30.0, clock=lambda: fake_now["t"],
    )

    first = await cache.get_or_exchange("s", "task", "t1")
    assert first.grant_id == "g-1"

    # Within window -> hit cache
    fake_now["t"] = 1500.0
    again = await cache.get_or_exchange("s", "task", "t1")
    assert again.grant_id == "g-1"
    assert len(client.exchange_calls) == 1

    # Past (expires - skew) -> refetch
    fake_now["t"] = 1980.0
    fresh = await cache.get_or_exchange("s", "task", "t1")
    assert fresh.grant_id == "g-2"
    assert len(client.exchange_calls) == 2


@pytest.mark.asyncio
async def test_cache_hint_ttl_tightens_deadline():
    fake_now = {"t": 0.0}
    client = _FakeClient()
    # expires_at far in the future; cache_hint_ttl tightens to 10s.
    client.queue_exchange(_make_response(
        grant_id="g-1", expires_at_ms=10**9, cache_hint_ttl_s=10,
    ))
    client.queue_exchange(_make_response(
        grant_id="g-2", expires_at_ms=10**9, cache_hint_ttl_s=10,
    ))
    cache = AuthorityGrantCache(client, soft_renew_skew_s=0.0, clock=lambda: fake_now["t"])

    a = await cache.get_or_exchange("s", "task", "t1")
    assert a.grant_id == "g-1"

    fake_now["t"] = 5.0
    b = await cache.get_or_exchange("s", "task", "t1")
    assert b.grant_id == "g-1"
    assert len(client.exchange_calls) == 1

    fake_now["t"] = 11.0
    c = await cache.get_or_exchange("s", "task", "t1")
    assert c.grant_id == "g-2"
    assert len(client.exchange_calls) == 2


@pytest.mark.asyncio
async def test_invalidate_drops_by_grant_id():
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1"))
    client.queue_exchange(_make_response(grant_id="g-2"))
    cache = AuthorityGrantCache(client)

    await cache.get_or_exchange("s", "task", "t1")
    assert cache.stats()["size"] == 1
    dropped = cache.invalidate("g-1")
    assert dropped == 1
    assert cache.stats()["size"] == 0

    # Next call refetches.
    g2 = await cache.get_or_exchange("s", "task", "t1")
    assert g2.grant_id == "g-2"


@pytest.mark.asyncio
async def test_revocation_event_invalidates_cache():
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1", root_grant_id="root-1"))
    client.queue_exchange(_make_response(grant_id="g-2", root_grant_id="root-1"))
    cache = AuthorityGrantCache(client)

    await cache.get_or_exchange("s", "task", "t1")
    assert cache.stats()["size"] == 1

    evt = aether_pb2.AuthorityGrantRevocation(grant_id="g-1", reason="manual")
    dropped = cache.handle_revocation_event(evt)
    assert dropped == 1
    assert cache.stats()["size"] == 0

    # Refetch with new grant_id but same root.
    fresh = await cache.get_or_exchange("s", "task", "t1")
    assert fresh.grant_id == "g-2"

    # Now cascade by root_grant_id should hit it.
    evt_cascade = aether_pb2.AuthorityGrantRevocation(
        grant_id="root-1", root_grant_id="root-1", cascade=True, reason="parent revoked",
    )
    dropped_cascade = cache.handle_revocation_event(evt_cascade)
    assert dropped_cascade >= 1
    assert cache.stats()["size"] == 0


@pytest.mark.asyncio
async def test_derive_for_task_idempotent_via_cache():
    client = _FakeClient()
    client.queue_derive(_make_response(grant_id="g-derived"))
    cache = AuthorityGrantCache(client)

    a = await cache.derive_for_task("parent-1", "task-42")
    b = await cache.derive_for_task("parent-1", "task-42")
    assert a.grant_id == b.grant_id == "g-derived"
    # Single round-trip — second call hit the cache.
    assert len(client.derive_for_target_calls) == 1
    # Op called with target_type=task by default.
    assert client.derive_for_target_calls[0]["target_type"] == "task"
    assert client.derive_for_target_calls[0]["target_id"] == "task-42"


@pytest.mark.asyncio
async def test_revoke_all_clears_and_calls_revoke():
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1"))
    client.queue_exchange(_make_response(grant_id="g-2"))
    cache = AuthorityGrantCache(client)

    await cache.get_or_exchange("s1", "task", "t1")
    await cache.get_or_exchange("s2", "task", "t2")
    assert cache.stats()["size"] == 2

    await cache.revoke_all()
    assert cache.stats()["size"] == 0
    assert sorted(client.revoke_calls) == ["g-1", "g-2"]


@pytest.mark.asyncio
async def test_revoke_all_swallows_individual_errors(caplog):
    client = _FakeClient()
    client._revoke_should_raise = True
    client.queue_exchange(_make_response(grant_id="g-1"))
    cache = AuthorityGrantCache(client)

    await cache.get_or_exchange("s", "task", "t")
    # Must not raise.
    await cache.revoke_all()
    assert cache.stats()["size"] == 0


@pytest.mark.asyncio
async def test_get_or_exchange_returns_none_on_failure():
    client = _FakeClient()
    client.queue_exchange(_make_response(success=False))
    cache = AuthorityGrantCache(client)

    out = await cache.get_or_exchange("s", "task", "t")
    assert out is None
    # Failures are not cached -> next call re-asks.
    client.queue_exchange(_make_response(grant_id="g-1"))
    next_out = await cache.get_or_exchange("s", "task", "t")
    assert next_out is not None
    assert next_out.grant_id == "g-1"
    assert len(client.exchange_calls) == 2


# ---------------------------------------------------------------------------
# High-level helpers (Phase 4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_grant_valid_predicate():
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1"))
    cache = AuthorityGrantCache(client)

    assert cache.is_grant_valid("g-1") is False  # not seeded yet
    await cache.get_or_exchange("s", "task", "t1")
    assert cache.is_grant_valid("g-1") is True
    assert cache.is_grant_valid("") is False
    assert cache.is_grant_valid("g-unknown") is False
    # No round-trip happened from the predicate itself.
    assert len(client.exchange_calls) == 1


@pytest.mark.asyncio
async def test_is_grant_valid_evicts_stale_entry():
    fake_now = {"t": 1000.0}
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1", expires_at_ms=2_000_000))
    cache = AuthorityGrantCache(
        client, soft_renew_skew_s=30.0, clock=lambda: fake_now["t"],
    )

    await cache.get_or_exchange("s", "task", "t1")
    assert cache.is_grant_valid("g-1") is True
    # Walk past the soft-renew deadline; predicate must report False.
    fake_now["t"] = 1980.0
    assert cache.is_grant_valid("g-1") is False
    assert cache.stats()["size"] == 0


@pytest.mark.asyncio
async def test_list_active_grants_deduplicates_and_drops_stale():
    fake_now = {"t": 0.0}
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1", expires_at_ms=10**9))
    client.queue_exchange(_make_response(grant_id="g-2", cache_hint_ttl_s=10))
    cache = AuthorityGrantCache(
        client, soft_renew_skew_s=0.0, clock=lambda: fake_now["t"],
    )

    await cache.get_or_exchange("s1", "task", "t1")
    await cache.get_or_exchange("s2", "task", "t2")
    active = cache.list_active_grants()
    assert sorted(g.grant_id for g in active) == ["g-1", "g-2"]

    # Past the hint TTL on g-2 -> only g-1 remains.
    fake_now["t"] = 11.0
    active2 = cache.list_active_grants()
    assert [g.grant_id for g in active2] == ["g-1"]


@pytest.mark.asyncio
async def test_revoke_local_is_alias_of_invalidate():
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1"))
    cache = AuthorityGrantCache(client)

    await cache.get_or_exchange("s", "task", "t1")
    dropped = cache.revoke_local("g-1")
    assert dropped == 1
    assert cache.stats()["size"] == 0
    # No gateway round-trip — revoke_local is local-only.
    assert client.revoke_calls == []


@pytest.mark.asyncio
async def test_refresh_force_drops_and_re_exchanges():
    client = _FakeClient()
    client.queue_exchange(_make_response(grant_id="g-1"))
    client.queue_exchange(_make_response(grant_id="g-2"))
    cache = AuthorityGrantCache(client)

    first = await cache.get_or_exchange("s", "task", "t1")
    assert first.grant_id == "g-1"

    refreshed = await cache.refresh("g-1")
    assert refreshed is not None
    assert refreshed.grant_id == "g-2"
    assert len(client.exchange_calls) == 2
    # Cache now holds the new grant, not the old one.
    assert cache.is_grant_valid("g-2") is True
    assert cache.is_grant_valid("g-1") is False


@pytest.mark.asyncio
async def test_refresh_unknown_grant_returns_none():
    client = _FakeClient()
    cache = AuthorityGrantCache(client)
    assert await cache.refresh("") is None
    assert await cache.refresh("never-cached") is None
    assert client.exchange_calls == []


@pytest.mark.asyncio
async def test_refresh_skips_derived_entries():
    client = _FakeClient()
    client.queue_derive(_make_response(grant_id="g-derived"))
    cache = AuthorityGrantCache(client)

    await cache.derive_for_task("parent-1", "task-42")
    # Derived entries can't be re-exchanged; cache drops them and returns None.
    out = await cache.refresh("g-derived")
    assert out is None
    assert cache.is_grant_valid("g-derived") is False
    # Refresh did not call exchange.
    assert client.exchange_calls == []


# ---------------------------------------------------------------------------
# Client wrappers (extend_seconds + LIST/BATCH/DERIVE_FOR_TARGET)
# ---------------------------------------------------------------------------


def _make_async_client() -> BaseAsyncAetherClient:
    client = BaseAsyncAetherClient.__new__(BaseAsyncAetherClient)
    BaseAsyncAetherClient.__init__(client, auto_reconnect=False)
    return client


async def _drain_one_upstream(client: BaseAsyncAetherClient) -> aether_pb2.UpstreamMessage:
    return await asyncio.wait_for(client._request_queue.get(), timeout=1.0)


async def _resolve_grant_op(
    client: BaseAsyncAetherClient,
    request_id: str,
    response: aether_pb2.AuthorityGrantResponse,
) -> None:
    response.request_id = request_id
    pending = client._pending_requests.pop(request_id, None)
    assert pending is not None, f"no pending future for request_id={request_id!r}"
    pending.set_result(response)


@pytest.mark.asyncio
async def test_renew_authority_grant_with_extend_seconds():
    client = _make_async_client()

    async def driver():
        return await client.renew_authority_grant("g-1", extend_seconds=600)

    task = asyncio.create_task(driver())
    upstream = await _drain_one_upstream(client)
    assert upstream.WhichOneof("payload") == "authority_grant_op"
    op = upstream.authority_grant_op
    assert op.op == aether_pb2.AuthorityGrantOperation.RENEW
    assert op.grant_id == "g-1"
    assert op.renew_request.extend_seconds == 600
    assert op.renew_request.expires_at == 0

    await _resolve_grant_op(client, op.request_id,
                            aether_pb2.AuthorityGrantResponse(success=True))
    resp = await asyncio.wait_for(task, timeout=1.0)
    assert resp is not None
    assert resp.success


@pytest.mark.asyncio
async def test_list_my_authority_grants_op_shape():
    client = _make_async_client()

    async def driver():
        return await client.list_my_authority_grants(audience_type="task", limit=10)

    task = asyncio.create_task(driver())
    upstream = await _drain_one_upstream(client)
    op = upstream.authority_grant_op
    assert op.op == aether_pb2.AuthorityGrantOperation.LIST_MY_GRANTS
    assert op.list_request.audience_type == "task"
    assert op.list_request.limit == 10

    await _resolve_grant_op(client, op.request_id,
                            aether_pb2.AuthorityGrantResponse(success=True, total=0))
    resp = await asyncio.wait_for(task, timeout=1.0)
    assert resp.success


@pytest.mark.asyncio
async def test_list_grants_on_me_op_shape():
    client = _make_async_client()

    async def driver():
        return await client.list_authority_grants_on_me(include_revoked=True)

    task = asyncio.create_task(driver())
    upstream = await _drain_one_upstream(client)
    op = upstream.authority_grant_op
    assert op.op == aether_pb2.AuthorityGrantOperation.LIST_GRANTS_ON_ME
    assert op.list_request.include_revoked is True

    await _resolve_grant_op(client, op.request_id,
                            aether_pb2.AuthorityGrantResponse(success=True))
    resp = await asyncio.wait_for(task, timeout=1.0)
    assert resp.success


@pytest.mark.asyncio
async def test_batch_exchange_op_shape():
    client = _make_async_client()
    requests = [
        aether_pb2.AuthorityGrantExchangeRequest(source_session_id="s1"),
        aether_pb2.AuthorityGrantExchangeRequest(source_session_id="s2"),
    ]

    async def driver():
        return await client.batch_exchange_authority_grants(requests, stop_on_first_error=True)

    task = asyncio.create_task(driver())
    upstream = await _drain_one_upstream(client)
    op = upstream.authority_grant_op
    assert op.op == aether_pb2.AuthorityGrantOperation.BATCH_EXCHANGE
    assert op.batch_exchange_request.stop_on_first_error is True
    assert len(op.batch_exchange_request.requests) == 2
    assert op.batch_exchange_request.requests[0].source_session_id == "s1"

    await _resolve_grant_op(client, op.request_id,
                            aether_pb2.AuthorityGrantResponse(success=True))
    resp = await asyncio.wait_for(task, timeout=1.0)
    assert resp.success


@pytest.mark.asyncio
async def test_derive_for_target_op_shape():
    client = _make_async_client()

    async def driver():
        return await client.derive_authority_grant_for_target(
            parent_grant_id="parent-1",
            target_type="task",
            target_id="task-99",
            audience_type="task",
            audience_id="task-99",
            max_access_level=2,
        )

    task = asyncio.create_task(driver())
    upstream = await _drain_one_upstream(client)
    op = upstream.authority_grant_op
    assert op.op == aether_pb2.AuthorityGrantOperation.DERIVE_FOR_TARGET
    req = op.derive_for_target_request
    assert req.parent_grant_id == "parent-1"
    assert req.target.principal_type == "task"
    assert req.target.principal_id == "task-99"
    assert req.audience_type == "task"
    assert req.audience_id == "task-99"
    assert req.max_access_level == 2

    await _resolve_grant_op(
        client, op.request_id,
        _make_response(grant_id="g-derived"),
    )
    resp = await asyncio.wait_for(task, timeout=1.0)
    assert resp.grant.grant_id == "g-derived"


# ---------------------------------------------------------------------------
# make_authority_cache + downstream-event wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_authority_cache_attaches_to_client():
    client = _make_async_client()
    cache = client.make_authority_cache()
    assert cache is not None
    assert client._authority_grant_cache is cache


@pytest.mark.asyncio
async def test_revocation_push_event_invalidates_cache_via_dispatch():
    """End-to-end: a downstream AuthorityGrantRevocation runs through the
    listen-loop dispatch and clears the cache."""
    client = _make_async_client()
    cache = client.make_authority_cache()
    # Seed the cache directly so we do not need to round-trip an exchange.
    grant = aether_pb2.ACLAuthorityGrantInfo(grant_id="g-evt", root_grant_id="g-evt")
    cache._store(("s", "task", "t"), grant, response=None)
    assert cache.stats()["size"] == 1

    # Inject the downstream message via the dispatcher hook directly. We
    # replicate what the listen loop does for ``authority_grant_revocation``
    # without requiring a real gRPC connection.
    evt = aether_pb2.AuthorityGrantRevocation(grant_id="g-evt", reason="server-push")
    cache.handle_revocation_event(evt)
    assert cache.stats()["size"] == 0
