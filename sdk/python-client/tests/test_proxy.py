"""Tests for proxy_http and the httpx transports."""
from __future__ import annotations

import asyncio
import os
import queue
import threading
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from scitrera_aether_client import proxy as proxy_mod
from scitrera_aether_client.exceptions import TimeoutError as AetherTimeoutError
from scitrera_aether_client.proto import aether_pb2
from scitrera_aether_client.proxy import (
    PROXY_BODY_CHUNK_SIZE,
    ProxyError,
    proxy_http,
    proxy_http_async,
)


CHUNK = PROXY_BODY_CHUNK_SIZE


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class _SyncClientStub:
    """Minimal sync client stub matching the BaseAetherClient surface used by proxy_http."""

    def __init__(self) -> None:
        self.request_queue: queue.Queue = queue.Queue()
        self._proxy_dispatcher = None  # set on first proxy call

    def drain_upstream(self) -> List[aether_pb2.UpstreamMessage]:
        out: List[aether_pb2.UpstreamMessage] = []
        while True:
            try:
                out.append(self.request_queue.get_nowait())
            except queue.Empty:
                return out


class _AsyncClientStub:
    def __init__(self) -> None:
        self._request_queue: asyncio.Queue = asyncio.Queue()
        self._proxy_dispatcher = None

    async def drain_upstream(self) -> List[aether_pb2.UpstreamMessage]:
        out: List[aether_pb2.UpstreamMessage] = []
        while True:
            try:
                out.append(self._request_queue.get_nowait())
            except asyncio.QueueEmpty:
                return out


def _build_response(
    request_id: str,
    status: int = 200,
    headers: Optional[dict] = None,
    body: bytes = b"",
    body_chunked: bool = False,
) -> aether_pb2.ProxyHttpResponse:
    resp = aether_pb2.ProxyHttpResponse(
        request_id=request_id,
        status_code=status,
        body=b"" if body_chunked else body,
        body_chunked=body_chunked,
    )
    if headers:
        for k, v in headers.items():
            resp.headers[k] = v
    return resp


def _stream_response_into(client, request_id: str, body: bytes, *, chunked: bool, status: int = 200, delay: float = 0.0):
    """Spawn a thread that pushes a fake response (and chunks) into client._proxy_dispatcher."""

    def _push():
        if delay:
            import time as _t
            _t.sleep(delay)
        dispatcher = client._proxy_dispatcher
        if chunked:
            header = _build_response(
                request_id, status=status, headers={"content-type": "application/octet-stream"},
                body=b"", body_chunked=True,
            )
            dispatcher.handle_response(header)
            seq = 0
            if not body:
                dispatcher.handle_body_chunk(
                    aether_pb2.ProxyHttpBodyChunk(
                        request_id=request_id, is_request=False, seq=0, data=b"", fin=True,
                    )
                )
            else:
                for i in range(0, len(body), CHUNK):
                    piece = body[i:i + CHUNK]
                    is_last = (i + CHUNK) >= len(body)
                    dispatcher.handle_body_chunk(
                        aether_pb2.ProxyHttpBodyChunk(
                            request_id=request_id,
                            is_request=False,
                            seq=seq,
                            data=piece,
                            fin=is_last,
                        )
                    )
                    seq += 1
        else:
            dispatcher.handle_response(_build_response(request_id, status=status, body=body))

    t = threading.Thread(target=_push, daemon=True)
    t.start()
    return t


async def _stream_response_async(client, request_id: str, body: bytes, *, chunked: bool, status: int = 200, delay: float = 0.0):
    if delay:
        await asyncio.sleep(delay)
    dispatcher = client._proxy_dispatcher
    if chunked:
        header = _build_response(
            request_id, status=status, headers={"content-type": "application/octet-stream"},
            body=b"", body_chunked=True,
        )
        dispatcher.handle_response(header)
        seq = 0
        if not body:
            dispatcher.handle_body_chunk(
                aether_pb2.ProxyHttpBodyChunk(
                    request_id=request_id, is_request=False, seq=0, data=b"", fin=True,
                )
            )
        else:
            for i in range(0, len(body), CHUNK):
                piece = body[i:i + CHUNK]
                is_last = (i + CHUNK) >= len(body)
                dispatcher.handle_body_chunk(
                    aether_pb2.ProxyHttpBodyChunk(
                        request_id=request_id,
                        is_request=False,
                        seq=seq,
                        data=piece,
                        fin=is_last,
                    )
                )
                seq += 1
    else:
        dispatcher.handle_response(_build_response(request_id, status=status, body=body))


# ---------------------------------------------------------------------------
# Sync chunk reassembly tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "size",
    [0, 1, CHUNK - 1, CHUNK, 1024 * 1024, 5 * 1024 * 1024],
    ids=["empty", "1B", "256KB-1", "256KB", "1MB", "5MB"],
)
def test_proxy_http_response_chunk_reassembly(size: int):
    client = _SyncClientStub()
    body = os.urandom(size)
    request_id = "test-req-1"

    # Pre-register dispatcher BEFORE the call so we can grab it; proxy_http will
    # reuse the same instance via _get_dispatcher.
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    chunked = size > CHUNK
    _stream_response_into(client, request_id, body, chunked=chunked, delay=0.05)

    resp = proxy_http(
        client,
        target_topic="sv::memorylayer::default",
        method="POST",
        path="/v1/echo",
        headers={"x-trace": "abc"},
        body=b"",  # request body irrelevant to response shape
        timeout=5.0,
        request_id=request_id,
    )
    assert resp.status_code == 200
    assert resp.body == body
    assert not resp.body_chunked


def test_proxy_http_request_chunks_emitted_for_large_body():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-big"
    body = os.urandom(CHUNK * 3 + 1234)

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)

    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="PUT",
        path="/upload",
        body=body,
        timeout=5.0,
        request_id=request_id,
    )

    msgs = client.drain_upstream()
    # First message: ProxyHttpRequest with body_chunked=True and empty body
    assert msgs[0].WhichOneof("payload") == "proxy_http_request"
    assert msgs[0].proxy_http_request.body_chunked is True
    assert msgs[0].proxy_http_request.body == b""

    chunks = [m for m in msgs[1:] if m.WhichOneof("payload") == "proxy_http_body_chunk"]
    assert len(chunks) == 4  # ceil((CHUNK*3 + 1234) / CHUNK) == 4
    reassembled = b"".join(c.proxy_http_body_chunk.data for c in chunks)
    assert reassembled == body
    assert chunks[-1].proxy_http_body_chunk.fin is True
    assert all(c.proxy_http_body_chunk.is_request for c in chunks)


def test_proxy_http_small_body_inline():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-small"
    body = b"hello"

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)
    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="POST",
        path="/x",
        body=body,
        timeout=5.0,
        request_id=request_id,
    )
    msgs = client.drain_upstream()
    assert len(msgs) == 1
    req = msgs[0].proxy_http_request
    assert req.body == body
    assert req.body_chunked is False


# ---------------------------------------------------------------------------
# Error and timeout paths
# ---------------------------------------------------------------------------

def test_proxy_http_error_response_raises():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-err"

    def _push():
        import time as _t
        _t.sleep(0.05)
        err = aether_pb2.ProxyHttpResponse(
            request_id=request_id,
            error=aether_pb2.ProxyError(
                kind=aether_pb2.ProxyError.DIAL_FAILED,
                message="connection refused",
            ),
        )
        client._proxy_dispatcher.handle_response(err)

    threading.Thread(target=_push, daemon=True).start()

    with pytest.raises(ProxyError) as exc_info:
        proxy_http(
            client,
            target_topic="sv::dead::default",
            method="GET",
            path="/",
            timeout=5.0,
            request_id=request_id,
        )
    assert exc_info.value.kind == aether_pb2.ProxyError.DIAL_FAILED
    assert exc_info.value.kind_name == "DIAL_FAILED"
    assert "connection refused" in str(exc_info.value)


def test_proxy_http_timeout():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    with pytest.raises(AetherTimeoutError):
        proxy_http(
            client,
            target_topic="sv::slow::default",
            method="GET",
            path="/",
            timeout=0.2,
            request_id="req-timeout",
        )


# ---------------------------------------------------------------------------
# OBO grant injection
# ---------------------------------------------------------------------------

def test_proxy_http_obo_authorization_injected():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-obo"

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)
    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/me",
        timeout=5.0,
        request_id=request_id,
        authority_mode="obo",
        subject_type="user",
        subject_id="user-42",
        grant_id="grant-xyz",
    )

    msgs = client.drain_upstream()
    req = msgs[0].proxy_http_request
    assert req.authorization.authority_mode == "obo"
    assert req.authorization.subject.principal_type == "user"
    assert req.authorization.subject.principal_id == "user-42"
    assert req.authorization.grant_id == "grant-xyz"


def test_proxy_http_explicit_authorization_passthrough():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-auth"

    auth = aether_pb2.AuthorizationContext(
        authority_mode="obo",
        subject=aether_pb2.PrincipalRef(principal_type="user", principal_id="u1"),
        grant_id="g1",
    )

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)
    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/",
        timeout=5.0,
        request_id=request_id,
        authorization=auth,
    )
    msgs = client.drain_upstream()
    req = msgs[0].proxy_http_request
    assert req.authorization.grant_id == "g1"
    assert req.authorization.subject.principal_id == "u1"


def test_proxy_http_no_authorization_when_unspecified():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-noauth"

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)
    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/",
        timeout=5.0,
        request_id=request_id,
    )
    msgs = client.drain_upstream()
    req = msgs[0].proxy_http_request
    # AuthorizationContext is a message field; default == empty / unset.
    assert not req.HasField("authorization")


def test_proxy_http_app_workspace_and_redirects():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-ws"

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)
    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/",
        timeout=5.0,
        follow_redirects=True,
        app_workspace="default",
        request_id=request_id,
    )
    msgs = client.drain_upstream()
    req = msgs[0].proxy_http_request
    assert req.app_workspace == "default"
    assert req.follow_redirects is True
    assert req.timeout_ms == 5000


def test_proxy_http_backend_kwarg_emitted_on_envelope():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-backend"

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)
    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/",
        timeout=5.0,
        request_id=request_id,
        backend="admin",
    )
    msgs = client.drain_upstream()
    assert msgs[0].proxy_http_request.backend_name == "admin"


def test_proxy_http_backend_kwarg_omitted_leaves_field_empty():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "req-no-backend"

    _stream_response_into(client, request_id, b"ok", chunked=False, delay=0.05)
    proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/",
        timeout=5.0,
        request_id=request_id,
    )
    msgs = client.drain_upstream()
    assert msgs[0].proxy_http_request.backend_name == ""


# ---------------------------------------------------------------------------
# Async path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size", [0, 1, CHUNK - 1, CHUNK, 1024 * 1024, 5 * 1024 * 1024],
    ids=["empty", "1B", "256KB-1", "256KB", "1MB", "5MB"],
)
async def test_proxy_http_async_chunk_reassembly(size: int):
    client = _AsyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    body = os.urandom(size)
    request_id = "areq-1"

    chunked = size > CHUNK

    async def _producer():
        await _stream_response_async(client, request_id, body, chunked=chunked, delay=0.05)

    producer_task = asyncio.create_task(_producer())
    resp = await proxy_http_async(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/x",
        timeout=5.0,
        request_id=request_id,
    )
    await producer_task
    assert resp.status_code == 200
    assert resp.body == body


@pytest.mark.asyncio
async def test_proxy_http_async_error():
    client = _AsyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    request_id = "areq-err"

    async def _producer():
        await asyncio.sleep(0.05)
        err = aether_pb2.ProxyHttpResponse(
            request_id=request_id,
            error=aether_pb2.ProxyError(
                kind=aether_pb2.ProxyError.TIMEOUT, message="upstream timeout"
            ),
        )
        client._proxy_dispatcher.handle_response(err)

    producer_task = asyncio.create_task(_producer())
    with pytest.raises(ProxyError) as info:
        await proxy_http_async(
            client,
            target_topic="sv::svc::default",
            method="GET",
            path="/",
            timeout=5.0,
            request_id=request_id,
        )
    await producer_task
    assert info.value.kind == aether_pb2.ProxyError.TIMEOUT


@pytest.mark.asyncio
async def test_proxy_http_async_timeout():
    client = _AsyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    with pytest.raises(AetherTimeoutError):
        await proxy_http_async(
            client,
            target_topic="sv::slow::default",
            method="GET",
            path="/",
            timeout=0.2,
            request_id="areq-tmo",
        )


# ---------------------------------------------------------------------------
# httpx transport smoke
# ---------------------------------------------------------------------------

def test_httpx_transport_sync_basic():
    import httpx

    from scitrera_aether_client.httpx_transport import AetherHTTPXTransport

    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    # We don't know the request_id ahead of time; we'll watch the queue and
    # respond using whatever the proxy stamps onto the request.
    pending_done = threading.Event()

    def _producer():
        # Wait until the proxy_http_request lands on the queue
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)  # put it back so drain_upstream still sees it
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(
            _build_response(
                rid, status=201, headers={"content-type": "text/plain"}, body=b"created"
            )
        )
        pending_done.set()

    threading.Thread(target=_producer, daemon=True).start()

    transport = AetherHTTPXTransport(client, "sv::svc::default", timeout=5.0)
    with httpx.Client(transport=transport, base_url="http://aether.local") as http:
        r = http.post("/v1/things", content=b"payload", headers={"x-test": "1"})
    assert pending_done.wait(timeout=5.0)
    assert r.status_code == 201
    assert r.content == b"created"


@pytest.mark.asyncio
async def test_httpx_transport_async_basic():
    import httpx

    from scitrera_aether_client.httpx_transport import AetherAsyncHTTPXTransport

    client = _AsyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    async def _producer():
        # Pull the upstream proxy_http_request, stash its request_id, respond.
        msg = await asyncio.wait_for(client._request_queue.get(), timeout=5.0)
        await client._request_queue.put(msg)  # keep history
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(
            _build_response(rid, status=200, body=b"hi")
        )

    transport = AetherAsyncHTTPXTransport(client, "sv::svc::default", timeout=5.0)
    async with httpx.AsyncClient(transport=transport, base_url="http://aether.local") as http:
        producer_task = asyncio.create_task(_producer())
        r = await http.get("/v1/hello")
        await producer_task
    assert r.status_code == 200
    assert r.content == b"hi"
