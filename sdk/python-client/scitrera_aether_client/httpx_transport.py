"""httpx transports that route requests over an Aether proxy connection.

Drop-in usage::

    transport = AetherHTTPXTransport(client, "sv::memorylayer::default")
    with httpx.Client(transport=transport) as http:
        http.get("http://example/path")

The host portion of the URL is ignored — the target topic determines
where the request lands. Path, query string, headers, and body are
forwarded verbatim.
"""
from __future__ import annotations

from typing import Any, Optional

try:
    import httpx
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "httpx is required for AetherHTTPXTransport. Install with `pip install httpx`."
    ) from e

from .proto import aether_pb2
from .proxy import (
    ProxyError,
    StreamingProxyResponse,
    proxy_http,
    proxy_http_async,
)


def _path_with_query(url: "httpx.URL") -> str:
    raw = url.raw_path
    if isinstance(raw, bytes):
        return raw.decode("ascii")
    return str(raw) if raw else "/"


def _proxy_response_to_httpx(
    request: "httpx.Request", resp: aether_pb2.ProxyHttpResponse
) -> "httpx.Response":
    headers = list(resp.headers.items())
    return httpx.Response(
        status_code=resp.status_code,
        headers=headers,
        content=resp.body,
        request=request,
    )


def _streaming_proxy_response_to_httpx(
    request: "httpx.Request", resp: "StreamingProxyResponse"
) -> "httpx.Response":
    """Wrap a :class:`StreamingProxyResponse` as an ``httpx.Response`` whose
    body iterates incrementally."""
    headers = list(resp.headers.items())
    return httpx.Response(
        status_code=resp.status_code,
        headers=headers,
        stream=_HttpxStreamAdapter(resp),
        request=request,
    )


class _HttpxStreamAdapter(httpx.SyncByteStream):
    """Bridge :class:`StreamingProxyResponse` into ``httpx.SyncByteStream``."""

    def __init__(self, resp: "StreamingProxyResponse") -> None:
        self._resp = resp

    def __iter__(self):
        for chunk in self._resp.iter_bytes():
            if chunk:
                yield chunk

    def close(self) -> None:
        self._resp.close()


class _HttpxAsyncStreamAdapter(httpx.AsyncByteStream):
    """Async variant of :class:`_HttpxStreamAdapter`."""

    def __init__(self, resp: "StreamingProxyResponse") -> None:
        self._resp = resp

    async def __aiter__(self):
        async for chunk in await self._resp.aiter_bytes():
            if chunk:
                yield chunk

    async def aclose(self) -> None:
        self._resp.close()


class AetherHTTPXTransport(httpx.BaseTransport):
    """Synchronous httpx transport routing through an Aether client.

    ``backend`` optionally pins every request through this transport to a
    named terminator backend. The backend's allow-list still applies.

    ``stream_response`` opts every request through this transport into the
    unbounded streaming path. When set, ``timeout`` becomes the
    time-to-first-byte deadline only; chunks flow incrementally as the
    backend produces them and the response is closed by EOF / idle / max
    cap.
    """

    def __init__(
        self,
        client: Any,
        target_topic: str,
        *,
        timeout: float = 30.0,
        follow_redirects: bool = False,
        app_workspace: Optional[str] = None,
        authorization: Optional[aether_pb2.AuthorizationContext] = None,
        authority_mode: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        grant_id: Optional[str] = None,
        backend: Optional[str] = None,
        stream_response: bool = False,
        stream_idle_timeout_ms: int = 0,
        max_response_body_bytes: int = 0,
    ) -> None:
        self._client = client
        self._target_topic = target_topic
        self._timeout = timeout
        self._follow_redirects = follow_redirects
        self._app_workspace = app_workspace
        self._authorization = authorization
        self._authority_mode = authority_mode
        self._subject_type = subject_type
        self._subject_id = subject_id
        self._grant_id = grant_id
        self._backend = backend
        self._stream_response = stream_response
        self._stream_idle_timeout_ms = stream_idle_timeout_ms
        self._max_response_body_bytes = max_response_body_bytes

    def handle_request(self, request: "httpx.Request") -> "httpx.Response":
        body = request.read()
        headers = {k: v for k, v in request.headers.items()}
        try:
            resp = proxy_http(
                self._client,
                self._target_topic,
                request.method,
                _path_with_query(request.url),
                headers=headers,
                body=body,
                timeout=self._timeout,
                follow_redirects=self._follow_redirects,
                app_workspace=self._app_workspace,
                authorization=self._authorization,
                authority_mode=self._authority_mode,
                subject_type=self._subject_type,
                subject_id=self._subject_id,
                grant_id=self._grant_id,
                backend=self._backend,
                stream_response=self._stream_response,
                stream_idle_timeout_ms=self._stream_idle_timeout_ms,
                max_response_body_bytes=self._max_response_body_bytes,
            )
        except ProxyError as e:
            raise httpx.HTTPError(str(e)) from e
        if isinstance(resp, StreamingProxyResponse):
            return _streaming_proxy_response_to_httpx(request, resp)
        return _proxy_response_to_httpx(request, resp)

    def close(self) -> None:  # nothing to clean up; client lifecycle is external
        return None


class AetherAsyncHTTPXTransport(httpx.AsyncBaseTransport):
    """Asynchronous httpx transport routing through an Aether async client.

    ``backend`` optionally pins every request through this transport to a
    named terminator backend. The backend's allow-list still applies.

    ``stream_response`` opts into the unbounded streaming response path.
    """

    def __init__(
        self,
        client: Any,
        target_topic: str,
        *,
        timeout: float = 30.0,
        follow_redirects: bool = False,
        app_workspace: Optional[str] = None,
        authorization: Optional[aether_pb2.AuthorizationContext] = None,
        authority_mode: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        grant_id: Optional[str] = None,
        backend: Optional[str] = None,
        stream_response: bool = False,
        stream_idle_timeout_ms: int = 0,
        max_response_body_bytes: int = 0,
    ) -> None:
        self._client = client
        self._target_topic = target_topic
        self._timeout = timeout
        self._follow_redirects = follow_redirects
        self._app_workspace = app_workspace
        self._authorization = authorization
        self._authority_mode = authority_mode
        self._subject_type = subject_type
        self._subject_id = subject_id
        self._grant_id = grant_id
        self._backend = backend
        self._stream_response = stream_response
        self._stream_idle_timeout_ms = stream_idle_timeout_ms
        self._max_response_body_bytes = max_response_body_bytes

    async def handle_async_request(self, request: "httpx.Request") -> "httpx.Response":
        body = await request.aread()
        headers = {k: v for k, v in request.headers.items()}
        try:
            resp = await proxy_http_async(
                self._client,
                self._target_topic,
                request.method,
                _path_with_query(request.url),
                headers=headers,
                body=body,
                timeout=self._timeout,
                follow_redirects=self._follow_redirects,
                app_workspace=self._app_workspace,
                authorization=self._authorization,
                authority_mode=self._authority_mode,
                subject_type=self._subject_type,
                subject_id=self._subject_id,
                grant_id=self._grant_id,
                backend=self._backend,
                stream_response=self._stream_response,
                stream_idle_timeout_ms=self._stream_idle_timeout_ms,
                max_response_body_bytes=self._max_response_body_bytes,
            )
        except ProxyError as e:
            raise httpx.HTTPError(str(e)) from e
        if isinstance(resp, StreamingProxyResponse):
            return httpx.Response(
                status_code=resp.status_code,
                headers=list(resp.headers.items()),
                stream=_HttpxAsyncStreamAdapter(resp),
                request=request,
            )
        return _proxy_response_to_httpx(request, resp)

    async def aclose(self) -> None:
        return None


__all__ = ["AetherHTTPXTransport", "AetherAsyncHTTPXTransport"]
