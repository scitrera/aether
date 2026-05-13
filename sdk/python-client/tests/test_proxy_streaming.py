"""Tests for the stream_response_indefinitely opt-in (SSE / long-poll)."""
from __future__ import annotations

import asyncio
import queue
import threading
import time
from typing import List

import pytest

from scitrera_aether_client import proxy as proxy_mod
from scitrera_aether_client.proto import aether_pb2
from scitrera_aether_client.proxy import (
    ProxyError,
    StreamingProxyResponse,
    proxy_http,
    proxy_http_async,
)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _SyncClientStub:
    def __init__(self) -> None:
        self.request_queue: queue.Queue = queue.Queue()
        self._proxy_dispatcher = proxy_mod._ProxyDispatcher()


class _AsyncClientStub:
    def __init__(self) -> None:
        self._request_queue: asyncio.Queue = asyncio.Queue()
        self._proxy_dispatcher = proxy_mod._ProxyDispatcher()


# ---------------------------------------------------------------------------
# Sync streaming
# ---------------------------------------------------------------------------

def test_proxy_http_stream_emits_indefinite_flag_on_envelope():
    client = _SyncClientStub()

    def _producer():
        time.sleep(0.05)
        msg = client.request_queue.get(timeout=2.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        # Header + chunks + fin.
        client._proxy_dispatcher.handle_response(
            aether_pb2.ProxyHttpResponse(
                request_id=rid, status_code=200, body_chunked=True,
            )
        )
        for i in range(3):
            client._proxy_dispatcher.handle_body_chunk(
                aether_pb2.ProxyHttpBodyChunk(
                    request_id=rid, is_request=False, seq=i,
                    data=f"event-{i}\n".encode(),
                    fin=(i == 2),
                )
            )
            time.sleep(0.005)

    threading.Thread(target=_producer, daemon=True).start()

    resp = proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/events",
        timeout=2.0,
        request_id="req-stream-1",
        stream_response=True,
        stream_idle_timeout_ms=1000,
    )
    assert isinstance(resp, StreamingProxyResponse)
    assert resp.status_code == 200

    chunks: List[bytes] = list(resp.iter_bytes())
    assembled = b"".join(chunks)
    assert assembled == b"event-0\nevent-1\nevent-2\n"

    # Verify the stream_response opt-in fields landed on the wire envelope.
    envelopes = []
    while True:
        try:
            envelopes.append(client.request_queue.get_nowait())
        except queue.Empty:
            break
    request_envelope = next(m for m in envelopes if m.WhichOneof("payload") == "proxy_http_request")
    pr = request_envelope.proxy_http_request
    assert pr.stream_response_indefinitely is True
    assert pr.stream_idle_timeout_ms == 1000


def test_proxy_http_stream_mid_stream_error_raises():
    client = _SyncClientStub()

    def _producer():
        time.sleep(0.05)
        msg = client.request_queue.get(timeout=2.0)
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(
            aether_pb2.ProxyHttpResponse(
                request_id=rid, status_code=200, body_chunked=True,
            )
        )
        client._proxy_dispatcher.handle_body_chunk(
            aether_pb2.ProxyHttpBodyChunk(
                request_id=rid, is_request=False, seq=0,
                data=b"partial", fin=False,
            )
        )
        # Mid-stream PAYLOAD_TOO_LARGE.
        client._proxy_dispatcher.handle_response(
            aether_pb2.ProxyHttpResponse(
                request_id=rid,
                error=aether_pb2.ProxyError(
                    kind=aether_pb2.ProxyError.PAYLOAD_TOO_LARGE,
                    message="too big",
                ),
            )
        )

    threading.Thread(target=_producer, daemon=True).start()

    resp = proxy_http(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/big",
        timeout=2.0,
        request_id="req-stream-err",
        stream_response=True,
    )
    chunks: List[bytes] = []
    with pytest.raises(ProxyError) as info:
        for c in resp.iter_bytes():
            chunks.append(c)
    assert info.value.kind == aether_pb2.ProxyError.PAYLOAD_TOO_LARGE
    # The first chunk must survive even though the stream ended in error.
    assert b"".join(chunks) == b"partial"


# ---------------------------------------------------------------------------
# Async streaming
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_proxy_http_async_stream_drains_chunks():
    client = _AsyncClientStub()

    async def _producer():
        msg = await asyncio.wait_for(client._request_queue.get(), timeout=2.0)
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(
            aether_pb2.ProxyHttpResponse(
                request_id=rid, status_code=200, body_chunked=True,
            )
        )
        for i in range(3):
            client._proxy_dispatcher.handle_body_chunk(
                aether_pb2.ProxyHttpBodyChunk(
                    request_id=rid, is_request=False, seq=i,
                    data=f"a{i}".encode(),
                    fin=(i == 2),
                )
            )
            await asyncio.sleep(0.005)

    producer_task = asyncio.create_task(_producer())
    resp = await proxy_http_async(
        client,
        target_topic="sv::svc::default",
        method="GET",
        path="/events",
        timeout=2.0,
        request_id="areq-stream-1",
        stream_response=True,
    )
    assert isinstance(resp, StreamingProxyResponse)

    collected: List[bytes] = []
    async for chunk in await resp.aiter_bytes():
        collected.append(chunk)
    await producer_task
    assert b"".join(collected) == b"a0a1a2"
