"""
HTTP-over-Aether proxy support.

Adds ``proxy_http`` (sync) and ``proxy_http_async`` helpers that send a
``ProxyHttpRequest`` envelope through an existing client connection and wait
for the correlated ``ProxyHttpResponse``. Bodies larger than
:data:`PROXY_BODY_CHUNK_SIZE` are streamed via ``ProxyHttpBodyChunk`` frames
in both directions.

Hook mechanism (why monkey-patching):

    The ``BaseAetherClient._listen_loop`` dispatcher in ``client.py`` is a
    closed if/elif chain with no public registration API for new oneof
    payload types. We need to handle two new ``DownstreamMessage`` variants
    (``proxy_http_response``, ``proxy_http_body_chunk``) without editing
    ``client.py`` (it carries unrelated user WIP). The smallest seam is
    ``_do_connect`` — wrapping the gRPC ``responses`` iterator with a
    filter that consumes proxy frames and forwards everything else
    untouched. The patch is installed exactly once at import time and is
    idempotent. If/when ``client.py`` grows a real dispatcher hook this
    module should switch to using it.
"""
from __future__ import annotations

import asyncio
import queue
import threading
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .exceptions import AetherError, TimeoutError as AetherTimeoutError
from .proto import aether_pb2

PROXY_BODY_CHUNK_SIZE = 256 * 1024


class ProxyError(AetherError):
    """Raised when the sidecar/gateway returns a ProxyError envelope."""

    def __init__(
        self,
        message: str,
        kind: int = aether_pb2.ProxyError.UNKNOWN,
        request_id: Optional[str] = None,
    ) -> None:
        kind_name = aether_pb2.ProxyError.Kind.Name(kind) if kind is not None else "UNKNOWN"
        super().__init__(message, code=kind_name, details=request_id)
        self.kind = kind
        self.kind_name = kind_name
        self.request_id = request_id


class _ProxyPending:
    """Accumulator for one outstanding proxy request."""

    def __init__(
        self,
        request_id: str,
        fut: Optional[asyncio.Future] = None,
        streaming: bool = False,
    ) -> None:
        self.request_id = request_id
        self.header: Optional[aether_pb2.ProxyHttpResponse] = None
        self.header_received = False
        self.chunks: Dict[int, bytes] = {}
        self.complete_event = threading.Event()
        self.fut = fut  # asyncio.Future for async path
        self.error: Optional[BaseException] = None
        self.fin_seen = False
        self.final: Optional[aether_pb2.ProxyHttpResponse] = None
        # Streaming-mode plumbing.
        self.streaming = streaming
        self.stream: Optional["_ProxyStream"] = (
            _ProxyStream() if streaming else None
        )
        # Header signalling for streaming callers (which return as soon as
        # the header lands, without waiting for fin).
        self.header_event = threading.Event()

    def try_finalize(self) -> bool:
        if not self.header_received:
            return False
        if self.header is not None and not self.header.body_chunked:
            return True
        return self.fin_seen

    def mark_fin(self) -> None:
        self.fin_seen = True


class _ProxyStream:
    """Thread-safe FIFO of body chunks for streaming proxy responses.

    Iterating the stream yields ``bytes`` chunks until the producer signals
    end-of-stream (via :meth:`close`). If the producer encounters a terminal
    proxy error (e.g. mid-stream TIMEOUT / PAYLOAD_TOO_LARGE), :meth:`close`
    is called with that ``ProxyError`` and the next iteration raises it.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._chunks: List[bytes] = []
        self._closed = False
        self._error: Optional[BaseException] = None

    def push(self, data: bytes) -> None:
        if not data:
            return
        with self._cond:
            if self._closed:
                return
            self._chunks.append(data)
            self._cond.notify_all()

    def close(self, error: Optional[BaseException] = None) -> None:
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._error = error
            self._cond.notify_all()

    def __iter__(self) -> "_ProxyStream":
        return self

    def __next__(self) -> bytes:
        with self._cond:
            while not self._chunks and not self._closed:
                self._cond.wait()
            if self._chunks:
                return self._chunks.pop(0)
            if self._error is not None:
                raise self._error
            raise StopIteration

    # Async support: produce an async iterator wrapping the same buffer.
    def aiter(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> "_ProxyStreamAsync":
        return _ProxyStreamAsync(self, loop or asyncio.get_event_loop())


class _ProxyStreamAsync:
    """Async iterator wrapper around :class:`_ProxyStream` that bridges the
    threading.Condition into an asyncio loop via ``run_in_executor``."""

    def __init__(self, inner: "_ProxyStream", loop: asyncio.AbstractEventLoop) -> None:
        self._inner = inner
        self._loop = loop

    def __aiter__(self) -> "_ProxyStreamAsync":
        return self

    async def __anext__(self) -> bytes:
        # asyncio cannot propagate StopIteration through a Future, so wrap
        # the iteration in a sentinel-returning helper that signals end of
        # stream via None rather than StopIteration.
        def _step() -> Optional[bytes]:
            try:
                return self._inner.__next__()
            except StopIteration:
                return None

        result = await self._loop.run_in_executor(None, _step)
        if result is None:
            raise StopAsyncIteration
        return result


class _ProxyDispatcher:
    """Per-client registry routing proxy_http_response / body_chunk frames."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: Dict[str, _ProxyPending] = {}

    def register(self, p: _ProxyPending) -> None:
        with self._lock:
            self._pending[p.request_id] = p

    def pop(self, request_id: str) -> Optional[_ProxyPending]:
        with self._lock:
            return self._pending.pop(request_id, None)

    def get(self, request_id: str) -> Optional[_ProxyPending]:
        with self._lock:
            return self._pending.get(request_id)

    def fail_all(self, exc: BaseException) -> None:
        with self._lock:
            pendings = list(self._pending.values())
            self._pending.clear()
        for p in pendings:
            p.error = exc
            if p.fut is not None and not p.fut.done():
                try:
                    p.fut.get_loop().call_soon_threadsafe(p.fut.set_exception, exc)
                except RuntimeError:
                    pass
            p.complete_event.set()

    def handle_response(self, resp: aether_pb2.ProxyHttpResponse) -> bool:
        p = self.get(resp.request_id)
        if p is None:
            return False
        # Streaming pending: a header may arrive once at start, then
        # subsequent header frames are mid-stream terminal errors.
        if p.streaming and p.header_received:
            if resp.HasField("error"):
                err = ProxyError(
                    resp.error.message or "proxy error",
                    kind=resp.error.kind,
                    request_id=p.request_id,
                )
                if p.stream is not None:
                    p.stream.close(err)
                p.complete_event.set()
                self.pop(p.request_id)
            return True
        p.header = resp
        p.header_received = True
        # For streaming pending, signal the header event so the caller can
        # build its iterator before any chunks arrive.
        if p.streaming:
            p.header_event.set()
            if p.fut is not None and not p.fut.done():
                try:
                    p.fut.get_loop().call_soon_threadsafe(p.fut.set_result, resp)
                except RuntimeError:
                    pass
            # Streaming responses do not finalise on header — the caller
            # drains the body stream until it is closed.
            return True
        if p.try_finalize():
            self._complete(p)
        return True

    def handle_body_chunk(self, chunk: aether_pb2.ProxyHttpBodyChunk) -> bool:
        if chunk.is_request:
            return False  # echoed request chunks are not for clients
        p = self.get(chunk.request_id)
        if p is None:
            return False
        if p.streaming and p.stream is not None:
            if chunk.data:
                p.stream.push(chunk.data)
            if chunk.fin:
                p.stream.close()
                p.complete_event.set()
                self.pop(p.request_id)
            return True
        if chunk.data:
            p.chunks[chunk.seq] = chunk.data
        if chunk.fin:
            p.mark_fin()
        if p.try_finalize():
            self._complete(p)
        return True

    def _complete(self, p: _ProxyPending) -> None:
        # Reassemble final response (header.body or chunked body)
        try:
            final = self._build_final(p)
        except Exception as e:  # pragma: no cover - defensive
            p.error = e
            if p.fut is not None and not p.fut.done():
                try:
                    p.fut.get_loop().call_soon_threadsafe(p.fut.set_exception, e)
                except RuntimeError:
                    pass
            p.complete_event.set()
            self.pop(p.request_id)
            return
        if p.fut is not None and not p.fut.done():
            try:
                p.fut.get_loop().call_soon_threadsafe(p.fut.set_result, final)
            except RuntimeError:
                pass
        p.final = final
        p.complete_event.set()
        self.pop(p.request_id)

    @staticmethod
    def _build_final(p: _ProxyPending) -> aether_pb2.ProxyHttpResponse:
        header = p.header
        assert header is not None
        if not header.body_chunked:
            return header
        # Reassemble body from chunks in seq order.
        ordered = sorted(p.chunks.items())
        body = b"".join(data for _, data in ordered)
        rebuilt = aether_pb2.ProxyHttpResponse()
        rebuilt.CopyFrom(header)
        rebuilt.body = body
        rebuilt.body_chunked = False
        return rebuilt


def _get_dispatcher(client: Any) -> _ProxyDispatcher:
    d = getattr(client, "_proxy_dispatcher", None)
    if d is None:
        d = _ProxyDispatcher()
        client._proxy_dispatcher = d
    return d


def _build_authorization(
    *,
    authority_mode: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    grant_id: Optional[str] = None,
) -> Optional[aether_pb2.AuthorizationContext]:
    if not (authority_mode or subject_type or subject_id or grant_id):
        return None
    ac = aether_pb2.AuthorizationContext()
    if authority_mode:
        ac.authority_mode = authority_mode
    if subject_type or subject_id:
        ac.subject.principal_type = subject_type or ""
        ac.subject.principal_id = subject_id or ""
    if grant_id:
        ac.grant_id = grant_id
    return ac


def _build_request(
    *,
    request_id: str,
    target_topic: str,
    method: str,
    path: str,
    headers: Optional[Mapping[str, str]],
    body: bytes,
    body_chunked: bool,
    authorization: Optional[aether_pb2.AuthorizationContext],
    app_workspace: Optional[str],
    timeout_ms: int,
    follow_redirects: bool,
    backend: Optional[str] = None,
    stream_response: bool = False,
    stream_idle_timeout_ms: int = 0,
    max_response_body_bytes: int = 0,
) -> aether_pb2.ProxyHttpRequest:
    req = aether_pb2.ProxyHttpRequest(
        request_id=request_id,
        target_topic=target_topic,
        method=method.upper(),
        path=path,
        body=b"" if body_chunked else body,
        body_chunked=body_chunked,
        app_workspace=app_workspace or "",
        timeout_ms=timeout_ms,
        follow_redirects=follow_redirects,
        backend_name=backend or "",
        stream_response_indefinitely=stream_response,
        stream_idle_timeout_ms=stream_idle_timeout_ms,
        max_response_body_bytes=max_response_body_bytes,
    )
    if headers:
        for k, v in headers.items():
            req.headers[k] = v
    if authorization is not None:
        req.authorization.CopyFrom(authorization)
    return req


def _iter_chunks(request_id: str, body: bytes) -> Tuple[aether_pb2.ProxyHttpBodyChunk, ...]:
    if not body:
        # body_chunked=True with empty body still requires a fin frame.
        return (
            aether_pb2.ProxyHttpBodyChunk(
                request_id=request_id, is_request=True, seq=0, data=b"", fin=True
            ),
        )
    chunks = []
    seq = 0
    for i in range(0, len(body), PROXY_BODY_CHUNK_SIZE):
        piece = body[i:i + PROXY_BODY_CHUNK_SIZE]
        is_last = (i + PROXY_BODY_CHUNK_SIZE) >= len(body)
        chunks.append(
            aether_pb2.ProxyHttpBodyChunk(
                request_id=request_id,
                is_request=True,
                seq=seq,
                data=piece,
                fin=is_last,
            )
        )
        seq += 1
    return tuple(chunks)


def _ensure_hooks_installed() -> None:
    """Install class-level wrappers on BaseAetherClient/BaseAsyncAetherClient."""
    from . import client as _client_mod
    from . import client_async as _client_async_mod

    base_sync = _client_mod.BaseAetherClient
    base_async = _client_async_mod.BaseAsyncAetherClient

    if not getattr(base_sync, "_proxy_hook_installed", False):
        _install_sync_hook(base_sync)
        base_sync._proxy_hook_installed = True

    if not getattr(base_async, "_proxy_hook_installed", False):
        _install_async_hook(base_async)
        base_async._proxy_hook_installed = True


def _install_sync_hook(base_sync: type) -> None:
    original_do_connect = base_sync._do_connect

    def _patched_do_connect(self, init_msg, target):  # type: ignore[no-untyped-def]
        # Take over the stub so we can intercept the responses iterator.
        from .proto import aether_pb2_grpc as _grpc_mod
        import grpc

        if self.tls_enabled:
            credentials = self._build_tls_credentials()
            self.channel = grpc.secure_channel(target, credentials)
        else:
            self.channel = grpc.insecure_channel(target)
        self.stub = _grpc_mod.AetherGatewayStub(self.channel)

        if self._session_id:
            init_with_resume = aether_pb2.InitConnection()
            init_with_resume.CopyFrom(init_msg)
            init_with_resume.resume_session_id = self._session_id
            self.request_queue.put(aether_pb2.UpstreamMessage(init=init_with_resume))
        else:
            self.request_queue.put(aether_pb2.UpstreamMessage(init=init_msg))

        raw_responses = self.stub.Connect(self._request_generator())
        responses = _wrap_sync_responses(self, raw_responses)

        self._stream_thread = threading.Thread(
            target=self._listen_loop, args=(responses,), daemon=True
        )
        self._stream_thread.start()

    # Only install the patch if the new code path will be used by callers
    # of proxy_http. We patch _do_connect unconditionally so that even
    # connections made before proxy_http() is first called work.
    base_sync._do_connect = _patched_do_connect  # type: ignore[assignment]
    base_sync._original_do_connect = original_do_connect  # type: ignore[attr-defined]


def _install_async_hook(base_async: type) -> None:
    original_do_connect = base_async._do_connect

    async def _patched_do_connect(self, init_msg, target):  # type: ignore[no-untyped-def]
        from .proto import aether_pb2_grpc as _grpc_mod
        import grpc.aio as _grpc_aio  # type: ignore

        if self.tls_enabled:
            credentials = self._build_tls_credentials()
            self.channel = _grpc_aio.secure_channel(target, credentials)
        else:
            self.channel = _grpc_aio.insecure_channel(target)
        self.stub = _grpc_mod.AetherGatewayStub(self.channel)

        raw_stream = self.stub.Connect(self._request_generator())
        self._stream = _AsyncResponseFilter(self, raw_stream)

        if self._session_id:
            init_with_resume = aether_pb2.InitConnection()
            init_with_resume.CopyFrom(init_msg)
            init_with_resume.resume_session_id = self._session_id
            await self._request_queue.put(
                aether_pb2.UpstreamMessage(init=init_with_resume)
            )
        else:
            await self._request_queue.put(aether_pb2.UpstreamMessage(init=init_msg))

        self._listen_task = asyncio.create_task(self._listen_loop())

    base_async._do_connect = _patched_do_connect  # type: ignore[assignment]
    base_async._original_do_connect = original_do_connect  # type: ignore[attr-defined]


def _wrap_sync_responses(client: Any, raw_iter):
    dispatcher = _get_dispatcher(client)

    def _gen():
        try:
            for response in raw_iter:
                payload_type = response.WhichOneof("payload")
                if payload_type == "proxy_http_response":
                    if dispatcher.handle_response(response.proxy_http_response):
                        continue
                elif payload_type == "proxy_http_body_chunk":
                    if dispatcher.handle_body_chunk(response.proxy_http_body_chunk):
                        continue
                yield response
        except Exception as e:  # noqa: BLE001
            dispatcher.fail_all(e)
            raise

    return _gen()


class _AsyncResponseFilter:
    """Async iterator wrapping the gRPC stream to peel off proxy frames.

    The wrapped object is typically a ``grpc.aio.StreamStreamCall`` which
    implements the async-iterator *protocol* via ``__aiter__`` only — its
    underlying iterator is what carries ``__anext__``. We resolve that once
    at first use and cache it; iterating ``self._inner`` directly would
    raise ``AttributeError: 'StreamStreamCall' object has no attribute
    '__anext__'``.
    """

    def __init__(self, client: Any, inner) -> None:
        self._client = client
        self._inner = inner
        self._inner_iter = None  # type: Any
        self._dispatcher = _get_dispatcher(client)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._inner_iter is None:
            # gRPC StreamStreamCall exposes async iteration via __aiter__()
            # only — calling __anext__ directly on the call object raises
            # AttributeError. Resolve once and cache.
            self._inner_iter = self._inner.__aiter__()
        while True:
            msg = await self._inner_iter.__anext__()
            payload_type = msg.WhichOneof("payload")
            if payload_type == "proxy_http_response":
                if self._dispatcher.handle_response(msg.proxy_http_response):
                    continue
            elif payload_type == "proxy_http_body_chunk":
                if self._dispatcher.handle_body_chunk(msg.proxy_http_body_chunk):
                    continue
            return msg

    def cancel(self):  # pass-through used by client.disconnect()
        return self._inner.cancel()


class StreamingProxyResponse:
    """Streaming wrapper returned when ``stream_response=True``.

    Iterating the response yields ``bytes`` chunks as they arrive from the
    backend. The response stops cleanly on EOF; mid-stream errors raise
    :class:`ProxyError`.
    """

    def __init__(
        self,
        header: aether_pb2.ProxyHttpResponse,
        stream: "_ProxyStream",
        request_id: str,
    ) -> None:
        self._header = header
        self._stream = stream
        self.request_id = request_id

    @property
    def status_code(self) -> int:
        return self._header.status_code

    @property
    def headers(self) -> Dict[str, str]:
        return dict(self._header.headers)

    def iter_bytes(self) -> "_ProxyStream":
        return self._stream

    def __iter__(self) -> "_ProxyStream":
        return iter(self._stream)

    async def aiter_bytes(self) -> "_ProxyStreamAsync":
        return self._stream.aiter()

    def close(self) -> None:
        self._stream.close()


def proxy_http(
    client: Any,
    target_topic: str,
    method: str,
    path: str,
    headers: Optional[Mapping[str, str]] = None,
    body: bytes = b"",
    *,
    timeout: float = 30.0,
    follow_redirects: bool = False,
    app_workspace: Optional[str] = None,
    request_id: Optional[str] = None,
    authorization: Optional[aether_pb2.AuthorizationContext] = None,
    authority_mode: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    grant_id: Optional[str] = None,
    backend: Optional[str] = None,
    stream_response: bool = False,
    stream_idle_timeout_ms: int = 0,
    max_response_body_bytes: int = 0,
) -> Any:
    """Send an HTTP request through Aether and return the response.

    Bodies > 256 KB are split across ``ProxyHttpBodyChunk`` frames.
    OBO grants can be supplied either via a pre-built ``authorization``
    context or via ``authority_mode``/``subject_*``/``grant_id`` shorthand.

    ``backend`` optionally pins the request to a named terminator backend.
    The backend's allow-list still applies — explicit naming selects which
    backend's ACL is consulted, not whether the request is allowed.

    When ``stream_response=True``, ``timeout`` becomes the time-to-first-byte
    deadline only; subsequent body bytes are governed by
    ``stream_idle_timeout_ms`` (default 30000 when 0). The return value is
    a :class:`StreamingProxyResponse` whose iterator yields chunks as they
    arrive.
    """
    _ensure_hooks_installed()

    if request_id is None:
        request_id = str(uuid.uuid4())

    body_chunked = len(body) > PROXY_BODY_CHUNK_SIZE
    auth = authorization or _build_authorization(
        authority_mode=authority_mode,
        subject_type=subject_type,
        subject_id=subject_id,
        grant_id=grant_id,
    )

    req = _build_request(
        request_id=request_id,
        target_topic=target_topic,
        method=method,
        path=path,
        headers=headers,
        body=body,
        body_chunked=body_chunked,
        authorization=auth,
        app_workspace=app_workspace,
        timeout_ms=int(timeout * 1000) if timeout else 0,
        follow_redirects=follow_redirects,
        backend=backend,
        stream_response=stream_response,
        stream_idle_timeout_ms=stream_idle_timeout_ms,
        max_response_body_bytes=max_response_body_bytes,
    )

    dispatcher = _get_dispatcher(client)
    pending = _ProxyPending(request_id, streaming=stream_response)
    dispatcher.register(pending)

    try:
        client.request_queue.put(aether_pb2.UpstreamMessage(proxy_http_request=req))
        if body_chunked:
            for chunk in _iter_chunks(request_id, body):
                client.request_queue.put(
                    aether_pb2.UpstreamMessage(proxy_http_body_chunk=chunk)
                )

        if stream_response:
            # Wait only for the header (TTFB), not for fin.
            if not pending.header_event.wait(timeout=timeout if timeout else None):
                dispatcher.pop(request_id)
                raise AetherTimeoutError(
                    "proxy_http stream TTFB timed out",
                    operation="proxy_http",
                    timeout_seconds=timeout,
                )
            header = pending.header
            if header is None:  # pragma: no cover - defensive
                raise AetherError("proxy_http: missing streaming header")
            if header.HasField("error"):
                err = ProxyError(
                    header.error.message or "proxy error",
                    kind=header.error.kind,
                    request_id=request_id,
                )
                if pending.stream is not None:
                    pending.stream.close()
                dispatcher.pop(request_id)
                raise err
            assert pending.stream is not None
            return StreamingProxyResponse(header, pending.stream, request_id)

        deadline = time.monotonic() + timeout if timeout else None
        while not pending.complete_event.is_set():
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            if remaining == 0.0:
                dispatcher.pop(request_id)
                raise AetherTimeoutError(
                    "proxy_http timed out",
                    operation="proxy_http",
                    timeout_seconds=timeout,
                )
            pending.complete_event.wait(timeout=remaining if remaining else 0.1)

        if pending.error:
            raise pending.error
        final = pending.final
        if final is None:  # pragma: no cover - defensive
            raise AetherError("proxy_http: missing response payload")
        if final.HasField("error"):
            raise ProxyError(
                final.error.message or "proxy error",
                kind=final.error.kind,
                request_id=request_id,
            )
        return final
    finally:
        if not stream_response:
            dispatcher.pop(request_id)


async def proxy_http_async(
    client: Any,
    target_topic: str,
    method: str,
    path: str,
    headers: Optional[Mapping[str, str]] = None,
    body: bytes = b"",
    *,
    timeout: float = 30.0,
    follow_redirects: bool = False,
    app_workspace: Optional[str] = None,
    request_id: Optional[str] = None,
    authorization: Optional[aether_pb2.AuthorizationContext] = None,
    authority_mode: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    grant_id: Optional[str] = None,
    backend: Optional[str] = None,
    stream_response: bool = False,
    stream_idle_timeout_ms: int = 0,
    max_response_body_bytes: int = 0,
) -> Any:
    _ensure_hooks_installed()

    if request_id is None:
        request_id = str(uuid.uuid4())

    body_chunked = len(body) > PROXY_BODY_CHUNK_SIZE
    auth = authorization or _build_authorization(
        authority_mode=authority_mode,
        subject_type=subject_type,
        subject_id=subject_id,
        grant_id=grant_id,
    )
    req = _build_request(
        request_id=request_id,
        target_topic=target_topic,
        method=method,
        path=path,
        headers=headers,
        body=body,
        body_chunked=body_chunked,
        authorization=auth,
        app_workspace=app_workspace,
        timeout_ms=int(timeout * 1000) if timeout else 0,
        follow_redirects=follow_redirects,
        backend=backend,
        stream_response=stream_response,
        stream_idle_timeout_ms=stream_idle_timeout_ms,
        max_response_body_bytes=max_response_body_bytes,
    )

    dispatcher = _get_dispatcher(client)
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    pending = _ProxyPending(request_id, fut=fut, streaming=stream_response)
    dispatcher.register(pending)

    try:
        await client._request_queue.put(
            aether_pb2.UpstreamMessage(proxy_http_request=req)
        )
        if body_chunked:
            for chunk in _iter_chunks(request_id, body):
                await client._request_queue.put(
                    aether_pb2.UpstreamMessage(proxy_http_body_chunk=chunk)
                )

        try:
            final = await asyncio.wait_for(fut, timeout=timeout if timeout else None)
        except asyncio.TimeoutError:
            raise AetherTimeoutError(
                "proxy_http_async timed out",
                operation="proxy_http_async",
                timeout_seconds=timeout,
            )

        if stream_response:
            # ``final`` carries the header; build the streaming wrapper. The
            # dispatcher entry stays registered until fin / error closes the
            # stream.
            if final.HasField("error"):
                if pending.stream is not None:
                    pending.stream.close()
                dispatcher.pop(request_id)
                raise ProxyError(
                    final.error.message or "proxy error",
                    kind=final.error.kind,
                    request_id=request_id,
                )
            assert pending.stream is not None
            return StreamingProxyResponse(final, pending.stream, request_id)

        if final.HasField("error"):
            raise ProxyError(
                final.error.message or "proxy error",
                kind=final.error.kind,
                request_id=request_id,
            )
        return final
    finally:
        if not stream_response:
            dispatcher.pop(request_id)


__all__ = [
    "PROXY_BODY_CHUNK_SIZE",
    "ProxyError",
    "StreamingProxyResponse",
    "proxy_http",
    "proxy_http_async",
]


# Install the proxy-frame dispatcher hook on the base client classes at
# MODULE IMPORT time, not on first ``proxy_http_async`` call. The hook
# patches ``BaseAsyncAetherClient._do_connect`` to wrap the gRPC responses
# iterator with a filter that peels off ``ProxyHttpResponse`` /
# ``ProxyHttpBodyChunk`` envelopes; the patch only takes effect for
# connections opened AFTER it lands. Lazy installation creates a footgun:
# any client that connects before something later in the process happens to
# call ``proxy_http_async`` will silently drop proxy responses for the
# lifetime of that connection.
#
# We install eagerly here so consumers of the SDK do not have to remember
# to import ``scitrera_aether_client.proxy`` (or call ``_ensure_hooks_installed``)
# before opening connections. ``__init__.py`` imports this module
# unconditionally so the hooks land as soon as ``scitrera_aether_client``
# is loaded.
_ensure_hooks_installed()
