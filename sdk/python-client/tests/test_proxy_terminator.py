"""Tests for ProxyHttpTerminator (server-side proxy_http terminator).

Mirrors the test surface in ``test_proxy.py`` (initiator side) but inverts
the flow: the test injects ``ProxyHttpRequest`` (and chunks) into the
client's terminator dispatcher and asserts the handler is invoked with
the expected ``MintedRequest`` and that the response is framed correctly
back into the client's outbound queue.
"""
from __future__ import annotations

import asyncio
import os
from typing import List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from scitrera_aether_client.proto import aether_pb2
from scitrera_aether_client.proxy_terminator import (
    PROXY_BODY_CHUNK_SIZE,
    MintedRequest,
    ProxyHttpTerminator,
    _coerce_response,
    _get_terminator_dispatcher,
    _mint_auth_headers,
    _path_allowed,
    _split_path_query,
    _strip_inbound_identity_headers,
)


CHUNK = PROXY_BODY_CHUNK_SIZE


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _AsyncClientStub:
    """Minimal async client surface used by the terminator dispatcher."""

    def __init__(self) -> None:
        self._request_queue: asyncio.Queue = asyncio.Queue()
        # Tests that exercise actor-header minting set this to a real
        # ``aether_pb2.InitConnection`` to mimic a connected client.
        self._init_msg = None
        # The terminator attaches itself to this client via
        # `_get_terminator_dispatcher`. We don't pre-create it.

    @property
    def identity(self):
        """Delegate to the real ``BaseAsyncAetherClient.identity`` so the stub
        exercises the same identity-resolution logic the terminator relies on."""
        from scitrera_aether_client.client_async import BaseAsyncAetherClient
        return BaseAsyncAetherClient.identity.fget(self)

    async def drain_upstream(self) -> List[aether_pb2.UpstreamMessage]:
        out: List[aether_pb2.UpstreamMessage] = []
        while True:
            try:
                out.append(self._request_queue.get_nowait())
            except asyncio.QueueEmpty:
                return out


def _build_request(
    request_id: str,
    method: str = "POST",
    path: str = "/v1/echo",
    headers: Optional[dict] = None,
    body: bytes = b"",
    body_chunked: bool = False,
    authorization: Optional[aether_pb2.AuthorizationContext] = None,
    app_workspace: str = "",
    stream_response_indefinitely: bool = False,
) -> aether_pb2.ProxyHttpRequest:
    req = aether_pb2.ProxyHttpRequest(
        request_id=request_id,
        target_topic="sv::svc::default",
        method=method,
        path=path,
        body=b"" if body_chunked else body,
        body_chunked=body_chunked,
        app_workspace=app_workspace,
        stream_response_indefinitely=stream_response_indefinitely,
    )
    if headers:
        for k, v in headers.items():
            req.headers[k] = v
    if authorization is not None:
        req.authorization.CopyFrom(authorization)
    return req


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_path_allowed_glob_and_prefix():
    assert _path_allowed(["/*"], "/anything") is True
    assert _path_allowed(["*"], "/anything") is True
    assert _path_allowed(["/v1/*"], "/v1/memories") is True
    assert _path_allowed(["/v1/*"], "/v1") is True
    assert _path_allowed(["/v1/*"], "/v2/memories") is False
    assert _path_allowed(["/healthz"], "/healthz") is True
    assert _path_allowed(["/healthz"], "/healthz/x") is False


def test_strip_inbound_identity_headers_case_insensitive():
    headers = {
        "Content-Type": "application/json",
        "X-Auth-Subject-ID": "attacker",
        "x-auth-actor-id": "lowercase-attacker",
        "X-Aether-Grant-ID": "spoof-grant",
        "X-Trace-Id": "keep-me",
    }
    cleaned = _strip_inbound_identity_headers(headers)
    assert "Content-Type" in cleaned
    assert "X-Trace-Id" in cleaned
    # Both X-Auth-* and X-Aether-* must be stripped regardless of case.
    assert all(not k.lower().startswith(("x-auth-", "x-aether-")) for k in cleaned)


def test_mint_auth_headers_direct_no_authz():
    minted = _mint_auth_headers(None)
    assert minted["X-Auth-Authority-Mode"] == "direct"
    assert "X-Auth-Grant-ID" not in minted
    assert "X-Auth-Subject-ID" not in minted


def test_mint_auth_headers_direct_explicit():
    authz = aether_pb2.AuthorizationContext(authority_mode="direct")
    minted = _mint_auth_headers(authz)
    assert minted["X-Auth-Authority-Mode"] == "direct"
    assert "X-Auth-Grant-ID" not in minted


def test_mint_auth_headers_obo():
    authz = aether_pb2.AuthorizationContext(
        authority_mode="on_behalf_of",
        subject=aether_pb2.PrincipalRef(principal_type="user", principal_id="user-42"),
        grant_id="grant-xyz",
    )
    minted = _mint_auth_headers(authz)
    assert minted["X-Auth-Authority-Mode"] == "on_behalf_of"
    assert minted["X-Auth-Grant-ID"] == "grant-xyz"
    assert minted["X-Auth-Subject-Type"] == "user"
    assert minted["X-Auth-Subject-ID"] == "user-42"
    # Backward-compat user/principal headers must mirror the OBO subject.
    assert minted["X-Auth-User-ID"] == "user-42"
    assert minted["X-Auth-Principal-Type"] == "user"


def test_split_path_query():
    assert _split_path_query("/v1/x") == ("/v1/x", "")
    assert _split_path_query("/v1/x?a=1&b=2") == ("/v1/x", "a=1&b=2")
    assert _split_path_query("") == ("/", "")


def test_coerce_response_from_envelope():
    resp = aether_pb2.ProxyHttpResponse(status_code=200, body=b"ok")
    out = _coerce_response(resp, "req-1")
    assert out.request_id == "req-1"  # stamped when missing
    assert out.body == b"ok"


def test_coerce_response_from_triple():
    out = _coerce_response((201, {"content-type": "text/plain"}, b"hi"), "req-2")
    assert out.status_code == 201
    assert out.headers["content-type"] == "text/plain"
    assert out.body == b"hi"


def test_coerce_response_rejects_non_bytes_body():
    with pytest.raises(TypeError):
        _coerce_response((200, {}, "not bytes"), "req-3")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# End-to-end dispatch via the dispatcher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inline_request_dispatched_and_response_framed():
    client = _AsyncClientStub()
    received: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        received.append(req)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id,
            status_code=200,
            body=b"hello-world",
        )

    term = ProxyHttpTerminator(
        client=client,
        handler=handler,
        allow_paths=["/v1/*"],
        header_mode="strict",
    )
    await term.start()

    req = _build_request("req-1", method="POST", path="/v1/echo", body=b"payload")
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    assert len(received) == 1
    assert received[0].method == "POST"
    assert received[0].path == "/v1/echo"
    assert received[0].body == b"payload"

    msgs = await client.drain_upstream()
    assert len(msgs) == 1
    assert msgs[0].WhichOneof("payload") == "proxy_http_response"
    resp = msgs[0].proxy_http_response
    assert resp.request_id == "req-1"
    assert resp.status_code == 200
    assert resp.body == b"hello-world"
    assert not resp.body_chunked


@pytest.mark.asyncio
async def test_chunked_request_reassembled_in_seq_order():
    client = _AsyncClientStub()

    received_body: List[bytes] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        received_body.append(req.body)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b"ok"
        )

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()

    body = os.urandom(CHUNK * 2 + 17)
    req = _build_request("req-chunked", body=b"", body_chunked=True)
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)

    seq = 0
    for offset in range(0, len(body), CHUNK):
        piece = body[offset : offset + CHUNK]
        is_last = (offset + CHUNK) >= len(body)
        await dispatcher.handle_body_chunk(
            aether_pb2.ProxyHttpBodyChunk(
                request_id="req-chunked",
                is_request=True,
                seq=seq,
                data=piece,
                fin=is_last,
            )
        )
        seq += 1

    await dispatcher.wait_idle()
    assert received_body == [body]


@pytest.mark.asyncio
async def test_response_body_chunked_when_oversize():
    client = _AsyncClientStub()
    big_body = os.urandom(CHUNK * 3 + 1234)

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=big_body
        )

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-big"))
    await dispatcher.wait_idle()

    msgs = await client.drain_upstream()
    # Header frame + 4 body chunks (256KB * 3 + 1234 → 4 frames).
    assert msgs[0].WhichOneof("payload") == "proxy_http_response"
    assert msgs[0].proxy_http_response.body_chunked is True
    assert msgs[0].proxy_http_response.body == b""

    chunks = [m for m in msgs[1:] if m.WhichOneof("payload") == "proxy_http_body_chunk"]
    assert len(chunks) == 4
    reassembled = b"".join(c.proxy_http_body_chunk.data for c in chunks)
    assert reassembled == big_body
    assert chunks[-1].proxy_http_body_chunk.fin is True
    assert all(not c.proxy_http_body_chunk.is_request for c in chunks)


# ---------------------------------------------------------------------------
# Path filtering / ACL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_path_denied_returns_acl_error_and_handler_not_invoked():
    client = _AsyncClientStub()
    invoked = MagicMock()

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        invoked()
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(
        client=client, handler=handler, allow_paths=["/v1/*", "/healthz"]
    )
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(
        _build_request("req-deny", method="GET", path="/admin/secret")
    )
    await dispatcher.wait_idle()

    invoked.assert_not_called()
    msgs = await client.drain_upstream()
    assert len(msgs) == 1
    resp = msgs[0].proxy_http_response
    assert resp.HasField("error")
    assert resp.error.kind == aether_pb2.ProxyError.ACL_DENIED


@pytest.mark.asyncio
async def test_path_allowed_invokes_handler():
    client = _AsyncClientStub()
    seen: List[str] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        seen.append(req.path)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(
        client=client, handler=handler, allow_paths=["/v1/*", "/healthz"]
    )
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("r1", path="/v1/memories"))
    await dispatcher.handle_request(_build_request("r2", path="/healthz"))
    await dispatcher.wait_idle()

    assert seen == ["/v1/memories", "/healthz"]


# ---------------------------------------------------------------------------
# Header minting (strict mode security)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_strict_mode_strips_malicious_inbound_auth_headers_and_mints_from_envelope():
    client = _AsyncClientStub()
    captured: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        captured.append(req)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    # `obo_policy="allow_partial"` keeps the original Phase-2a behaviour
    # under test: this case verifies anti-spoofing of inbound headers, not
    # resolver gating. With the post-3.5c default ("require_resolver"), an
    # OBO request without a configured resolver is rejected before the
    # handler runs.
    term = ProxyHttpTerminator(
        client=client,
        handler=handler,
        allow_paths=["/*"],
        header_mode="strict",
        obo_policy="allow_partial",
    )
    await term.start()

    # Caller injects malicious X-Auth-* / X-Aether-* headers AND a real
    # OBO grant in the envelope. Strict mode must drop the inbound
    # spoofs and overwrite with values minted from the envelope.
    authz = aether_pb2.AuthorizationContext(
        authority_mode="on_behalf_of",
        subject=aether_pb2.PrincipalRef(principal_type="user", principal_id="real-user"),
        grant_id="real-grant",
    )
    req = _build_request(
        "req-spoof",
        headers={
            "X-Auth-Subject-ID": "attacker",
            "x-auth-actor-id": "lowercase-attacker",
            "X-Aether-Grant-ID": "spoofed-grant",
            "Content-Type": "application/json",
        },
        authorization=authz,
    )

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    assert len(captured) == 1
    minted_headers = captured[0].headers
    # Spoofs are gone.
    for k in list(minted_headers.keys()):
        if k.lower() in ("x-auth-subject-id",):
            assert minted_headers[k] == "real-user"  # overwritten with envelope value
    # Real value is minted from the envelope.
    assert minted_headers["X-Auth-Authority-Mode"] == "on_behalf_of"
    assert minted_headers["X-Auth-Grant-ID"] == "real-grant"
    assert minted_headers["X-Auth-Subject-ID"] == "real-user"
    assert minted_headers["X-Auth-Subject-Type"] == "user"
    # Non-auth header was preserved.
    assert minted_headers["Content-Type"] == "application/json"
    # No leftover X-Aether-* spoof.
    assert all(not k.lower().startswith("x-aether-") for k in minted_headers)


@pytest.mark.asyncio
async def test_strict_mode_direct_envelope_mints_direct_mode():
    client = _AsyncClientStub()
    captured: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        captured.append(req)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(
        client=client, handler=handler, allow_paths=["/*"], header_mode="strict"
    )
    await term.start()

    # No authorization on the envelope = direct mode.
    req = _build_request("req-direct")
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    headers = captured[0].headers
    assert headers["X-Auth-Authority-Mode"] == "direct"
    assert "X-Auth-Grant-ID" not in headers
    assert "X-Auth-Subject-ID" not in headers


@pytest.mark.asyncio
async def test_passthrough_mode_does_not_strip_or_mint():
    client = _AsyncClientStub()
    captured: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        captured.append(req)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(
        client=client, handler=handler, allow_paths=["/*"], header_mode="passthrough"
    )
    await term.start()

    req = _build_request(
        "req-pass",
        headers={
            "X-Auth-Subject-ID": "preserved",
            "X-Aether-Grant-ID": "also-preserved",
        },
    )
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    headers = captured[0].headers
    assert headers["X-Auth-Subject-ID"] == "preserved"
    assert headers["X-Aether-Grant-ID"] == "also-preserved"


# ---------------------------------------------------------------------------
# Concurrency / multiple in-flight requests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_requests_with_distinct_request_ids():
    client = _AsyncClientStub()
    seen: List[str] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        # Sleep a hair to encourage interleaving.
        await asyncio.sleep(0)
        seen.append(req.request_id)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=req.request_id.encode()
        )

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await asyncio.gather(
        dispatcher.handle_request(_build_request("a")),
        dispatcher.handle_request(_build_request("b")),
        dispatcher.handle_request(_build_request("c")),
    )

    assert sorted(seen) == ["a", "b", "c"]
    msgs = await client.drain_upstream()
    bodies = sorted(m.proxy_http_response.body.decode() for m in msgs)
    assert bodies == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_concurrent_chunked_requests_isolated_by_request_id():
    client = _AsyncClientStub()
    received: List[Tuple[str, bytes]] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        received.append((req.request_id, req.body))
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    body_a = os.urandom(CHUNK + 5)
    body_b = os.urandom(CHUNK + 9)

    # Open both chunked requests, then interleave their chunks.
    await dispatcher.handle_request(_build_request("A", body_chunked=True))
    await dispatcher.handle_request(_build_request("B", body_chunked=True))

    # Push first half of each.
    await dispatcher.handle_body_chunk(
        aether_pb2.ProxyHttpBodyChunk(
            request_id="A", is_request=True, seq=0, data=body_a[:CHUNK], fin=False
        )
    )
    await dispatcher.handle_body_chunk(
        aether_pb2.ProxyHttpBodyChunk(
            request_id="B", is_request=True, seq=0, data=body_b[:CHUNK], fin=False
        )
    )
    # Push tails (with fin) — order shouldn't matter.
    await dispatcher.handle_body_chunk(
        aether_pb2.ProxyHttpBodyChunk(
            request_id="B", is_request=True, seq=1, data=body_b[CHUNK:], fin=True
        )
    )
    await dispatcher.handle_body_chunk(
        aether_pb2.ProxyHttpBodyChunk(
            request_id="A", is_request=True, seq=1, data=body_a[CHUNK:], fin=True
        )
    )
    await dispatcher.wait_idle()

    received_map = dict(received)
    assert received_map["A"] == body_a
    assert received_map["B"] == body_b


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_exception_returns_500_response():
    client = _AsyncClientStub()

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        raise RuntimeError("kaboom")

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-fail"))
    await dispatcher.wait_idle()

    msgs = await client.drain_upstream()
    assert len(msgs) == 1
    resp = msgs[0].proxy_http_response
    assert resp.status_code == 500
    assert b"internal server error" in resp.body


@pytest.mark.asyncio
async def test_streaming_response_request_returns_not_implemented_error():
    client = _AsyncClientStub()

    invoked = MagicMock()

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        invoked()
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(
        _build_request("req-stream", stream_response_indefinitely=True)
    )
    await dispatcher.wait_idle()

    invoked.assert_not_called()
    msgs = await client.drain_upstream()
    resp = msgs[0].proxy_http_response
    assert resp.HasField("error")


# ---------------------------------------------------------------------------
# Hook coexistence with proxy.py initiator
# ---------------------------------------------------------------------------


def test_hook_install_idempotent_and_coexists_with_initiator():
    """The terminator hook should install once and not break the initiator hook."""
    from scitrera_aether_client import client_async as _client_async_mod

    # Importing the module installs the hook (via _ensure_terminator_hooks_installed
    # called from __init__). Importing again must not re-patch.
    from scitrera_aether_client import proxy_terminator  # noqa: F401

    base_async = _client_async_mod.BaseAsyncAetherClient
    assert getattr(base_async, "_proxy_terminator_hook_installed", False) is True
    # The initiator hook from proxy.py must also still be installed.
    assert getattr(base_async, "_proxy_hook_installed", False) is True


@pytest.mark.asyncio
async def test_terminator_stop_unregisters_and_drops_subsequent_requests():
    client = _AsyncClientStub()
    invoked = MagicMock()

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        invoked()
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()
    await term.stop()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-after-stop"))
    await dispatcher.wait_idle()

    invoked.assert_not_called()
    msgs = await client.drain_upstream()
    # No terminator registered → ACL_DENIED falls through.
    assert msgs[0].proxy_http_response.HasField("error")


# ---------------------------------------------------------------------------
# MintedRequest fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_minted_request_query_string_split():
    client = _AsyncClientStub()
    captured: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        captured.append(req)
        return aether_pb2.ProxyHttpResponse(
            request_id=req.request_id, status_code=200, body=b""
        )

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/v1/*"])
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(
        _build_request("req-q", path="/v1/search?q=hello&limit=10")
    )
    await dispatcher.wait_idle()

    assert captured[0].path == "/v1/search"
    assert captured[0].query == "q=hello&limit=10"


@pytest.mark.asyncio
async def test_handler_returning_triple_is_coerced():
    client = _AsyncClientStub()

    async def handler(req: MintedRequest):
        return (201, {"content-type": "text/plain"}, b"created")

    term = ProxyHttpTerminator(client=client, handler=handler, allow_paths=["/*"])
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-triple"))
    await dispatcher.wait_idle()

    msgs = await client.drain_upstream()
    resp = msgs[0].proxy_http_response
    assert resp.status_code == 201
    assert resp.headers["content-type"] == "text/plain"
    assert resp.body == b"created"


# ---------------------------------------------------------------------------
# Actor header minting from client identity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_strict_mode_mints_actor_headers_from_service_client_identity():
    """Strict mode overlays X-Auth-Actor-* from the client's service identity."""
    from scitrera_aether_client._common import create_service_init

    client = _AsyncClientStub()
    # Simulate a connected service client by injecting a fake _init_msg.
    client._init_msg = create_service_init("my-svc", "pod-1")

    received: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        received.append(req)
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    term = ProxyHttpTerminator(client=client, handler=handler, header_mode="strict")
    await term.start()

    req = _build_request("req-svc-actor", method="GET", path="/v1/probe")
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    assert len(received) == 1
    hdrs = received[0].headers
    assert hdrs.get("X-Auth-Actor-Type") == "service"
    assert hdrs.get("X-Auth-Actor-ID") == "sv::my-svc::pod-1"


@pytest.mark.asyncio
async def test_strict_mode_mints_actor_headers_from_agent_client_identity():
    """Strict mode overlays X-Auth-Actor-* from the client's agent identity."""
    from scitrera_aether_client._common import create_agent_init

    client = _AsyncClientStub()
    # Simulate a connected agent client by injecting a fake _init_msg.
    client._init_msg = create_agent_init("ws-42", "my-agent", "inst-0")

    received: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        received.append(req)
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    term = ProxyHttpTerminator(client=client, handler=handler, header_mode="strict")
    await term.start()

    req = _build_request("req-agent-actor", method="GET", path="/v1/probe")
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    assert len(received) == 1
    hdrs = received[0].headers
    assert hdrs.get("X-Auth-Actor-Type") == "agent"
    assert hdrs.get("X-Auth-Actor-ID") == "ag::ws-42::my-agent::inst-0"


@pytest.mark.asyncio
async def test_strict_mode_no_actor_headers_when_identity_absent():
    """Strict mode does not emit actor headers when the client has no _init_msg."""
    client = _AsyncClientStub()
    # No _init_msg set — identity property returns None (or AttributeError
    # is caught via getattr default).

    received: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        received.append(req)
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    term = ProxyHttpTerminator(client=client, handler=handler, header_mode="strict")
    await term.start()

    req = _build_request("req-no-identity", method="GET", path="/v1/probe")
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    assert len(received) == 1
    hdrs = received[0].headers
    assert "X-Auth-Actor-Type" not in hdrs
    assert "X-Auth-Actor-ID" not in hdrs


@pytest.mark.asyncio
async def test_passthrough_mode_does_not_mint_actor_headers():
    """Passthrough mode preserves headers as-is; actor headers are NOT injected."""
    from scitrera_aether_client._common import create_service_init

    client = _AsyncClientStub()
    client._init_msg = create_service_init("my-svc", "pod-1")

    received: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        received.append(req)
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    term = ProxyHttpTerminator(client=client, handler=handler, header_mode="passthrough")
    await term.start()

    req = _build_request(
        "req-passthrough-actor",
        method="GET",
        path="/v1/probe",
        headers={"X-Custom": "keep-me"},
    )
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(req)
    await dispatcher.wait_idle()

    assert len(received) == 1
    hdrs = received[0].headers
    # Passthrough: custom header preserved, actor headers NOT injected by terminator.
    assert hdrs.get("X-Custom") == "keep-me"
    assert "X-Auth-Actor-Type" not in hdrs
    assert "X-Auth-Actor-ID" not in hdrs


# ---------------------------------------------------------------------------
# Phase 3.5c: AsyncAuthorityResolver integration
# ---------------------------------------------------------------------------


class _StubResolver:
    """In-memory ``AuthorityResolverProtocol`` impl for terminator tests."""

    def __init__(self, info=None) -> None:
        self._info = info
        self.calls: List[dict] = []

    async def resolve(
        self,
        grant_id: str,
        subject_type: str,
        subject_id: str,
        *,
        actor_type=None,
        actor_id=None,
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
        return self._info


def _obo_authz(grant_id: str = "g-1", subject_id: str = "user-1") -> aether_pb2.AuthorizationContext:
    return aether_pb2.AuthorizationContext(
        authority_mode="on_behalf_of",
        subject=aether_pb2.PrincipalRef(principal_type="user", principal_id=subject_id),
        grant_id=grant_id,
    )


@pytest.mark.asyncio
async def test_resolver_none_obo_request_rejected():
    """OBO request without a configured resolver must be rejected (default policy)."""
    client = _AsyncClientStub()
    invoked = MagicMock()

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        invoked()
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    # Default obo_policy="require_resolver"; no resolver wired.
    term = ProxyHttpTerminator(client=client, handler=handler, header_mode="strict")
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-obo-no-resolver", authorization=_obo_authz()))
    await dispatcher.wait_idle()

    invoked.assert_not_called()
    msgs = await client.drain_upstream()
    assert len(msgs) == 1
    resp = msgs[0].proxy_http_response
    assert resp.HasField("error")
    assert resp.error.kind == aether_pb2.ProxyError.ACL_DENIED


@pytest.mark.asyncio
async def test_resolver_returns_none_obo_request_rejected():
    """Resolver wired but ``.resolve()`` returning None must reject the OBO request."""
    client = _AsyncClientStub()
    invoked = MagicMock()

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        invoked()
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    resolver = _StubResolver(info=None)
    term = ProxyHttpTerminator(
        client=client, handler=handler, header_mode="strict", resolver=resolver
    )
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-obo-deny", authorization=_obo_authz()))
    await dispatcher.wait_idle()

    invoked.assert_not_called()
    msgs = await client.drain_upstream()
    resp = msgs[0].proxy_http_response
    assert resp.HasField("error")
    assert resp.error.kind == aether_pb2.ProxyError.ACL_DENIED
    # Resolver was actually called.
    assert len(resolver.calls) == 1
    assert resolver.calls[0]["grant_id"] == "g-1"


@pytest.mark.asyncio
async def test_resolver_mints_extended_obo_headers():
    """Happy OBO path: resolver result populates max-access-level, workspace scope, etc."""
    from scitrera_aether_client.authority import ResolvedAuthorityInfo

    client = _AsyncClientStub()
    captured: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        captured.append(req)
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    info = ResolvedAuthorityInfo(
        grant_id="g-1",
        subject_type="user",
        subject_id="user-1",
        root_subject_type="user",
        root_subject_id="root-user-1",
        audience_type="service",
        audience_id="sv::memorylayer::main",
        max_access_level=20,
        workspace_scope=("ws-a", "ws-b"),
        expires_at=0,
        revoked=False,
    )
    resolver = _StubResolver(info=info)
    term = ProxyHttpTerminator(
        client=client, handler=handler, header_mode="strict", resolver=resolver
    )
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(
        _build_request("req-obo-ok", authorization=_obo_authz(grant_id="g-1", subject_id="user-1"))
    )
    await dispatcher.wait_idle()

    assert len(captured) == 1
    hdrs = captured[0].headers
    # Wire-derived headers (from _mint_auth_headers) still present.
    assert hdrs["X-Auth-Authority-Mode"] == "on_behalf_of"
    assert hdrs["X-Auth-Grant-ID"] == "g-1"
    assert hdrs["X-Auth-Subject-Type"] == "user"
    assert hdrs["X-Auth-Subject-ID"] == "user-1"
    # Resolver-derived overlay.
    assert hdrs["X-Auth-Max-Access-Level"] == "20"
    assert hdrs["X-Auth-Workspace-Scope"] == "ws-a,ws-b"
    assert hdrs["X-Auth-Audience-Type"] == "service"
    assert hdrs["X-Auth-Audience-ID"] == "sv::memorylayer::main"
    assert hdrs["X-Auth-Root-Subject-Type"] == "user"
    assert hdrs["X-Auth-Root-Subject-ID"] == "root-user-1"


@pytest.mark.asyncio
async def test_resolver_not_called_for_direct_mode():
    """Direct-mode requests must not invoke the resolver."""
    client = _AsyncClientStub()

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    resolver = _StubResolver(info=None)
    term = ProxyHttpTerminator(
        client=client, handler=handler, header_mode="strict", resolver=resolver
    )
    await term.start()

    # No authorization → direct mode.
    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-direct"))
    await dispatcher.wait_idle()

    assert resolver.calls == []
    msgs = await client.drain_upstream()
    # No error — direct mode passes through the resolver gate untouched.
    assert not msgs[0].proxy_http_response.HasField("error")


@pytest.mark.asyncio
async def test_resolver_workspace_scope_empty_omits_header():
    """An empty ``workspace_scope`` tuple must not emit ``X-Auth-Workspace-Scope``."""
    from scitrera_aether_client.authority import ResolvedAuthorityInfo

    client = _AsyncClientStub()
    captured: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        captured.append(req)
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    info = ResolvedAuthorityInfo(
        grant_id="g-1",
        subject_type="user",
        subject_id="user-1",
        root_subject_type="",
        root_subject_id="",
        audience_type="",
        audience_id="",
        max_access_level=10,
        workspace_scope=(),
        expires_at=0,
        revoked=False,
    )
    resolver = _StubResolver(info=info)
    term = ProxyHttpTerminator(
        client=client, handler=handler, header_mode="strict", resolver=resolver
    )
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-empty-scope", authorization=_obo_authz()))
    await dispatcher.wait_idle()

    assert len(captured) == 1
    hdrs = captured[0].headers
    # Empty scope → header absent (not empty string).
    assert "X-Auth-Workspace-Scope" not in hdrs
    # Empty audience/root subject also omitted.
    assert "X-Auth-Audience-Type" not in hdrs
    assert "X-Auth-Audience-ID" not in hdrs
    assert "X-Auth-Root-Subject-Type" not in hdrs
    assert "X-Auth-Root-Subject-ID" not in hdrs
    # Max access level present (non-zero).
    assert hdrs["X-Auth-Max-Access-Level"] == "10"


@pytest.mark.asyncio
async def test_obo_policy_allow_partial_lets_request_through_without_resolver():
    """``obo_policy="allow_partial"`` falls back to Phase 2a behaviour for OBO."""
    client = _AsyncClientStub()
    captured: List[MintedRequest] = []

    async def handler(req: MintedRequest) -> aether_pb2.ProxyHttpResponse:
        captured.append(req)
        return aether_pb2.ProxyHttpResponse(request_id=req.request_id, status_code=200)

    term = ProxyHttpTerminator(
        client=client,
        handler=handler,
        header_mode="strict",
        obo_policy="allow_partial",
    )
    await term.start()

    dispatcher = _get_terminator_dispatcher(client)
    await dispatcher.handle_request(_build_request("req-obo-partial", authorization=_obo_authz()))
    await dispatcher.wait_idle()

    # Handler invoked — request was not rejected.
    assert len(captured) == 1
    hdrs = captured[0].headers
    # Wire-derived OBO headers present; extended fields absent (no resolver).
    assert hdrs["X-Auth-Authority-Mode"] == "on_behalf_of"
    assert hdrs["X-Auth-Grant-ID"] == "g-1"
    assert "X-Auth-Max-Access-Level" not in hdrs
    assert "X-Auth-Workspace-Scope" not in hdrs
