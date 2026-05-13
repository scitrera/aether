"""Tests for AetherRequestsAdapter."""
from __future__ import annotations

import os
import queue
import threading
from typing import List, Optional
from unittest.mock import MagicMock

import pytest
import requests

from scitrera_aether_client import proxy as proxy_mod
from scitrera_aether_client.proto import aether_pb2
from scitrera_aether_client.proxy import PROXY_BODY_CHUNK_SIZE, ProxyError
from scitrera_aether_client.requests_adapter import AetherRequestsAdapter

CHUNK = PROXY_BODY_CHUNK_SIZE


# ---------------------------------------------------------------------------
# Fake sync client (mirrors test_proxy.py pattern)
# ---------------------------------------------------------------------------

class _SyncClientStub:
    def __init__(self) -> None:
        self.request_queue: queue.Queue = queue.Queue()
        self._proxy_dispatcher = None

    def drain_upstream(self) -> List[aether_pb2.UpstreamMessage]:
        out: List[aether_pb2.UpstreamMessage] = []
        while True:
            try:
                out.append(self.request_queue.get_nowait())
            except queue.Empty:
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


def _stream_response_into(
    client,
    request_id: str,
    body: bytes,
    *,
    chunked: bool,
    status: int = 200,
    delay: float = 0.0,
    response_headers: Optional[dict] = None,
):
    def _push():
        import time as _t
        if delay:
            _t.sleep(delay)
        dispatcher = client._proxy_dispatcher
        if chunked:
            header = _build_response(
                request_id,
                status=status,
                headers=response_headers or {"content-type": "application/octet-stream"},
                body=b"",
                body_chunked=True,
            )
            dispatcher.handle_response(header)
            seq = 0
            if not body:
                dispatcher.handle_body_chunk(
                    aether_pb2.ProxyHttpBodyChunk(
                        request_id=request_id, is_request=False, seq=0, data=b"", fin=True
                    )
                )
            else:
                for i in range(0, len(body), CHUNK):
                    piece = body[i : i + CHUNK]
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
            dispatcher.handle_response(
                _build_response(
                    request_id,
                    status=status,
                    headers=response_headers,
                    body=body,
                )
            )

    t = threading.Thread(target=_push, daemon=True)
    t.start()
    return t


def _make_session(client, **adapter_kwargs) -> requests.Session:
    session = requests.Session()
    adapter = AetherRequestsAdapter(client, **adapter_kwargs)
    session.mount("aether+sv://", adapter)
    return session


# ---------------------------------------------------------------------------
# Scheme / URL parsing tests (no real client needed)
# ---------------------------------------------------------------------------

def test_url_parse_specific_instance():
    from scitrera_aether_client.requests_adapter import _parse_target_topic

    topic, path = _parse_target_topic("aether+sv://my-impl--my-spec/some/path")
    assert topic == "my-impl::my-spec"
    assert path == "/some/path"


def test_url_parse_wildcard():
    from scitrera_aether_client.requests_adapter import _parse_target_topic

    topic, path = _parse_target_topic("aether+sv://my-impl/")
    assert topic == "my-impl"
    assert path == "/"


def test_url_parse_with_query():
    from scitrera_aether_client.requests_adapter import _parse_target_topic

    topic, path = _parse_target_topic("aether+sv://impl--spec/resource?foo=bar&baz=1")
    assert topic == "impl::spec"
    assert path == "/resource?foo=bar&baz=1"


def test_url_parse_wildcard_no_trailing_slash():
    from scitrera_aether_client.requests_adapter import _parse_target_topic

    topic, path = _parse_target_topic("aether+sv://impl/resource")
    assert topic == "impl"
    assert path == "/resource"


def test_url_parse_only_first_double_dash_is_delimiter():
    """Extra '--' inside specifier stays in specifier."""
    from scitrera_aether_client.requests_adapter import _parse_target_topic

    topic, path = _parse_target_topic("aether+sv://impl--spec--extra/path")
    assert topic == "impl::spec--extra"
    assert path == "/path"


# ---------------------------------------------------------------------------
# Happy path — specific instance URL
# ---------------------------------------------------------------------------

def test_adapter_specific_instance_happy_path():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    def _producer():
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(
            _build_response(rid, status=200, headers={"x-result": "ok"}, body=b"hello")
        )

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=5.0)
    resp = session.get("aether+sv://svc--default/v1/ping")

    assert resp.status_code == 200
    assert resp.content == b"hello"
    assert resp.headers["x-result"] == "ok"

    msgs = client.drain_upstream()
    req = msgs[0].proxy_http_request
    assert req.target_topic == "svc::default"
    assert req.method == "GET"
    assert req.path == "/v1/ping"


# ---------------------------------------------------------------------------
# Happy path — wildcard URL
# ---------------------------------------------------------------------------

def test_adapter_wildcard_happy_path():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    def _producer():
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(
            _build_response(rid, status=201, body=b"created")
        )

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=5.0)
    resp = session.post("aether+sv://my-svc/items", data=b"payload")

    assert resp.status_code == 201
    msgs = client.drain_upstream()
    req = msgs[0].proxy_http_request
    assert req.target_topic == "my-svc"
    assert req.method == "POST"
    assert req.body == b"payload"


# ---------------------------------------------------------------------------
# Header and method propagation
# ---------------------------------------------------------------------------

def test_adapter_headers_propagated():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    def _producer():
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(_build_response(rid, status=200, body=b""))

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=5.0)
    resp = session.get(
        "aether+sv://svc--inst/path",
        headers={"x-custom-header": "custom-value", "accept": "application/json"},
    )

    assert resp.status_code == 200
    msgs = client.drain_upstream()
    req = msgs[0].proxy_http_request
    assert req.headers.get("x-custom-header") == "custom-value"
    assert req.headers.get("accept") == "application/json"


def test_adapter_put_method():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    def _producer():
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        client._proxy_dispatcher.handle_response(_build_response(rid, status=204, body=b""))

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=5.0)
    resp = session.put("aether+sv://svc--inst/resource/1", data=b"update")

    assert resp.status_code == 204
    msgs = client.drain_upstream()
    assert msgs[0].proxy_http_request.method == "PUT"


# ---------------------------------------------------------------------------
# Large body chunking
# ---------------------------------------------------------------------------

def test_adapter_large_body_chunked():
    """Body > 256 KB must be emitted as body chunks."""
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    large_body = os.urandom(CHUNK * 3 + 999)

    # Wait until all upstream messages have been enqueued, then respond.
    def _producer():
        # Pull messages until we see the fin chunk, then respond.
        collected: List = []
        # We expect 1 header + 4 body chunks = 5 messages total.
        for _ in range(5):
            msg = client.request_queue.get(timeout=5.0)
            collected.append(msg)
        # Re-enqueue so drain_upstream sees them.
        for m in collected:
            client.request_queue.put(m)
        # Respond using the request_id from the header message.
        req_msgs = [m for m in collected if m.WhichOneof("payload") == "proxy_http_request"]
        rid = req_msgs[0].proxy_http_request.request_id
        _stream_response_into(client, rid, b"done", chunked=False, delay=0.0)

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=10.0)
    resp = session.post("aether+sv://svc--inst/upload", data=large_body)

    assert resp.status_code == 200

    msgs = client.drain_upstream()
    req_msgs = [m for m in msgs if m.WhichOneof("payload") == "proxy_http_request"]
    assert len(req_msgs) == 1
    assert req_msgs[0].proxy_http_request.body_chunked is True
    assert req_msgs[0].proxy_http_request.body == b""

    chunks = [m for m in msgs if m.WhichOneof("payload") == "proxy_http_body_chunk"]
    assert len(chunks) == 4  # ceil((CHUNK*3 + 999) / CHUNK) == 4
    reassembled = b"".join(c.proxy_http_body_chunk.data for c in chunks)
    assert reassembled == large_body
    assert chunks[-1].proxy_http_body_chunk.fin is True


# ---------------------------------------------------------------------------
# Large response chunking
# ---------------------------------------------------------------------------

def test_adapter_large_response_body_reassembled():
    """Chunked response body from Aether is reassembled into resp.content."""
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()
    large_body = os.urandom(CHUNK * 2 + 500)

    def _producer():
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        _stream_response_into(client, rid, large_body, chunked=True, delay=0.0)

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=10.0)
    resp = session.get("aether+sv://svc--inst/download")

    assert resp.status_code == 200
    assert resp.content == large_body


# ---------------------------------------------------------------------------
# Error path — ProxyError → requests.exceptions.ConnectionError
# ---------------------------------------------------------------------------

def test_adapter_proxy_error_raises_connection_error():
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    def _producer():
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        import time as _t
        _t.sleep(0.05)
        err = aether_pb2.ProxyHttpResponse(
            request_id=rid,
            error=aether_pb2.ProxyError(
                kind=aether_pb2.ProxyError.DIAL_FAILED,
                message="connection refused",
            ),
        )
        client._proxy_dispatcher.handle_response(err)

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=5.0)
    with pytest.raises(requests.exceptions.ConnectionError):
        session.get("aether+sv://dead--svc/path")


def test_adapter_proxy_error_is_subclass_of_requests_exception():
    """Ensure the raised exception is catchable via requests.exceptions.RequestException."""
    client = _SyncClientStub()
    client._proxy_dispatcher = proxy_mod._ProxyDispatcher()

    def _producer():
        msg = client.request_queue.get(timeout=5.0)
        client.request_queue.put(msg)
        rid = msg.proxy_http_request.request_id
        err = aether_pb2.ProxyHttpResponse(
            request_id=rid,
            error=aether_pb2.ProxyError(
                kind=aether_pb2.ProxyError.TIMEOUT,
                message="upstream timed out",
            ),
        )
        client._proxy_dispatcher.handle_response(err)

    threading.Thread(target=_producer, daemon=True).start()

    session = _make_session(client, timeout=5.0)
    with pytest.raises(requests.exceptions.RequestException):
        session.get("aether+sv://svc--inst/path")
