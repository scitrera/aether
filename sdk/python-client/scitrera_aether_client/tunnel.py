"""
Tunnel-over-Aether support.

Adds ``tunnel_dial`` (sync) and ``tunnel_dial_async`` helpers that drive a
``TunnelOpen`` / ``TunnelData`` / ``TunnelClose`` session through an existing
client connection. Both directions are framed at :data:`TUNNEL_CHUNK_SIZE`
bytes per ``TunnelData`` and respect ``TunnelAck``-based credit flow control.

Hook mechanism (why monkey-patching):

    Mirrors ``proxy.py``: the ``BaseAetherClient._listen_loop`` dispatcher in
    ``client.py`` is a closed if/elif chain. We need to handle three new
    ``DownstreamMessage`` variants (``tunnel_data``, ``tunnel_ack``,
    ``tunnel_close``) without editing ``client.py`` (it carries unrelated
    user WIP). Same seam as proxy: wrap the gRPC ``responses`` iterator
    with a filter that consumes tunnel frames and forwards everything
    else untouched. Patch is installed exactly once at import time and is
    idempotent.

Bidirectional credit flow (T14):
    The wire ``UpstreamMessage`` oneof carries ``tunnel_ack``, so credits
    flow in both directions: the caller emits a ``TunnelAck`` upstream
    after consuming inbound bytes, and the sidecar emits a ``TunnelAck``
    upstream after consuming bytes it forwarded to its backend. The
    gateway's ``routeTunnelAck`` forwards each ack to the opposite peer.
"""
from __future__ import annotations

import asyncio
import queue
import socket
import threading
import time
import uuid
from typing import Any, Dict, Mapping, Optional

from .exceptions import AetherError, TimeoutError as AetherTimeoutError
from .proto import aether_pb2

TUNNEL_CHUNK_SIZE = 256 * 1024
DEFAULT_INITIAL_CREDITS = 1024 * 1024
INBOUND_ACK_THRESHOLD = 256 * 1024


_PROTOCOL_MAP = {
    "tcp": aether_pb2.TunnelOpen.TCP,
    "udp": aether_pb2.TunnelOpen.UDP,
    "websocket": aether_pb2.TunnelOpen.WEBSOCKET,
    "ws": aether_pb2.TunnelOpen.WEBSOCKET,
}


class TunnelError(AetherError):
    """Base class for tunnel transport failures."""


class TunnelClosedError(TunnelError):
    """Raised when an operation is attempted on a tunnel that has been closed."""


class TunnelPeerResetError(TunnelError):
    """Raised when the peer sent ``TunnelClose{PEER_RESET}``."""


class TunnelIdleTimeoutError(TunnelError):
    """Raised when the peer sent ``TunnelClose{IDLE_TIMEOUT}``."""


class TunnelQuotaExceededError(TunnelError):
    """Raised when the peer sent ``TunnelClose{QUOTA}``."""


_REASON_TO_EXC = {
    aether_pb2.TunnelClose.PEER_RESET: TunnelPeerResetError,
    aether_pb2.TunnelClose.IDLE_TIMEOUT: TunnelIdleTimeoutError,
    aether_pb2.TunnelClose.QUOTA: TunnelQuotaExceededError,
    aether_pb2.TunnelClose.ERROR: TunnelError,
}


def _exc_for_close(reason: int, detail: str) -> TunnelError:
    if reason == aether_pb2.TunnelClose.NORMAL:
        return TunnelClosedError(detail or "tunnel closed", code="NORMAL")
    cls = _REASON_TO_EXC.get(reason, TunnelError)
    name = aether_pb2.TunnelClose.Reason.Name(reason)
    return cls(detail or name.lower().replace("_", " "), code=name)


def _resolve_protocol(p: str) -> int:
    if isinstance(p, int):
        return p
    key = p.lower()
    if key not in _PROTOCOL_MAP:
        raise ValueError(f"unsupported tunnel protocol: {p!r}")
    return _PROTOCOL_MAP[key]


# ---------------------------------------------------------------------------
# Sync state
# ---------------------------------------------------------------------------

class _SyncTunnelState:
    """Per-tunnel state shared between AetherTunnel and the dispatcher."""

    def __init__(self, tunnel_id: str, initial_credits: int) -> None:
        self.tunnel_id = tunnel_id
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        # inbound (sidecar -> caller): deque of bytes pending read
        self.inbound: bytearray = bytearray()
        self.inbound_eof = False
        self.inbound_consumed_since_ack = 0
        # outbound (caller -> sidecar): credit-driven
        self.outbound_credits = initial_credits
        self.outbound_seq = 0
        self.outbound_fin_sent = False
        # closure
        self.closed = False
        self.error: Optional[BaseException] = None

    def signal(self) -> None:
        self.cond.notify_all()


class _SyncTunnelDispatcher:
    """Per-client registry routing tunnel_data / tunnel_ack / tunnel_close frames."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tunnels: Dict[str, _SyncTunnelState] = {}

    def register(self, state: _SyncTunnelState) -> None:
        with self._lock:
            self._tunnels[state.tunnel_id] = state

    def unregister(self, tunnel_id: str) -> None:
        with self._lock:
            self._tunnels.pop(tunnel_id, None)

    def get(self, tunnel_id: str) -> Optional[_SyncTunnelState]:
        with self._lock:
            return self._tunnels.get(tunnel_id)

    def fail_all(self, exc: BaseException) -> None:
        with self._lock:
            states = list(self._tunnels.values())
            self._tunnels.clear()
        for s in states:
            with s.lock:
                if not s.closed:
                    s.closed = True
                    s.error = exc
                    s.signal()

    def handle_data(self, frame: aether_pb2.TunnelData) -> bool:
        s = self.get(frame.tunnel_id)
        if s is None:
            return False
        with s.lock:
            if frame.data:
                s.inbound.extend(frame.data)
            if frame.fin:
                s.inbound_eof = True
            s.signal()
        return True

    def handle_ack(self, frame: aether_pb2.TunnelAck) -> bool:
        s = self.get(frame.tunnel_id)
        if s is None:
            return False
        with s.lock:
            s.outbound_credits += int(frame.credits)
            s.signal()
        return True

    def handle_close(self, frame: aether_pb2.TunnelClose) -> bool:
        s = self.get(frame.tunnel_id)
        if s is None:
            return False
        exc = _exc_for_close(frame.reason, frame.detail)
        # NORMAL close just signals EOF (and unblocks pending writes); other
        # reasons surface as an error on the next read/write.
        with s.lock:
            s.inbound_eof = True
            if frame.reason != aether_pb2.TunnelClose.NORMAL:
                s.error = exc
            s.closed = True
            s.signal()
        return True


def _get_sync_dispatcher(client: Any) -> _SyncTunnelDispatcher:
    d = getattr(client, "_tunnel_dispatcher", None)
    if d is None:
        d = _SyncTunnelDispatcher()
        client._tunnel_dispatcher = d
    return d


# ---------------------------------------------------------------------------
# Async state
# ---------------------------------------------------------------------------

class _AsyncTunnelState:
    def __init__(self, tunnel_id: str, initial_credits: int, loop: asyncio.AbstractEventLoop) -> None:
        self.tunnel_id = tunnel_id
        self.loop = loop
        self.lock = asyncio.Lock()
        self.event = asyncio.Event()
        self.inbound: bytearray = bytearray()
        self.inbound_eof = False
        self.inbound_consumed_since_ack = 0
        self.outbound_credits = initial_credits
        self.outbound_seq = 0
        self.outbound_fin_sent = False
        self.closed = False
        self.error: Optional[BaseException] = None

    def signal_threadsafe(self) -> None:
        try:
            self.loop.call_soon_threadsafe(self.event.set)
        except RuntimeError:
            pass


class _AsyncTunnelDispatcher:
    def __init__(self) -> None:
        self._tunnels: Dict[str, _AsyncTunnelState] = {}

    def register(self, state: _AsyncTunnelState) -> None:
        self._tunnels[state.tunnel_id] = state

    def unregister(self, tunnel_id: str) -> None:
        self._tunnels.pop(tunnel_id, None)

    def get(self, tunnel_id: str) -> Optional[_AsyncTunnelState]:
        return self._tunnels.get(tunnel_id)

    def fail_all(self, exc: BaseException) -> None:
        states = list(self._tunnels.values())
        self._tunnels.clear()
        for s in states:
            if not s.closed:
                s.closed = True
                s.error = exc
                s.signal_threadsafe()

    def handle_data(self, frame: aether_pb2.TunnelData) -> bool:
        s = self.get(frame.tunnel_id)
        if s is None:
            return False
        if frame.data:
            s.inbound.extend(frame.data)
        if frame.fin:
            s.inbound_eof = True
        s.signal_threadsafe()
        return True

    def handle_ack(self, frame: aether_pb2.TunnelAck) -> bool:
        s = self.get(frame.tunnel_id)
        if s is None:
            return False
        s.outbound_credits += int(frame.credits)
        s.signal_threadsafe()
        return True

    def handle_close(self, frame: aether_pb2.TunnelClose) -> bool:
        s = self.get(frame.tunnel_id)
        if s is None:
            return False
        exc = _exc_for_close(frame.reason, frame.detail)
        s.inbound_eof = True
        if frame.reason != aether_pb2.TunnelClose.NORMAL:
            s.error = exc
        s.closed = True
        s.signal_threadsafe()
        return True


def _get_async_dispatcher(client: Any) -> _AsyncTunnelDispatcher:
    d = getattr(client, "_tunnel_dispatcher_async", None)
    if d is None:
        d = _AsyncTunnelDispatcher()
        client._tunnel_dispatcher_async = d
    return d


# ---------------------------------------------------------------------------
# Hook installation (mirrors proxy.py)
# ---------------------------------------------------------------------------

def _ensure_hooks_installed() -> None:
    from . import client as _client_mod
    from . import client_async as _client_async_mod

    base_sync = _client_mod.BaseAetherClient
    base_async = _client_async_mod.BaseAsyncAetherClient

    if not getattr(base_sync, "_tunnel_hook_installed", False):
        _install_sync_hook(base_sync)
        base_sync._tunnel_hook_installed = True

    if not getattr(base_async, "_tunnel_hook_installed", False):
        _install_async_hook(base_async)
        base_async._tunnel_hook_installed = True


def _install_sync_hook(base_sync: type) -> None:
    original_do_connect = base_sync._do_connect

    def _patched_do_connect(self, init_msg, target):  # type: ignore[no-untyped-def]
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

    base_sync._do_connect = _patched_do_connect  # type: ignore[assignment]
    base_sync._original_do_connect_tunnel = original_do_connect  # type: ignore[attr-defined]


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
    base_async._original_do_connect_tunnel = original_do_connect  # type: ignore[attr-defined]


def _wrap_sync_responses(client: Any, raw_iter):
    dispatcher = _get_sync_dispatcher(client)

    def _gen():
        try:
            for response in raw_iter:
                payload_type = response.WhichOneof("payload")
                if payload_type == "tunnel_data":
                    if dispatcher.handle_data(response.tunnel_data):
                        continue
                elif payload_type == "tunnel_ack":
                    if dispatcher.handle_ack(response.tunnel_ack):
                        continue
                elif payload_type == "tunnel_close":
                    if dispatcher.handle_close(response.tunnel_close):
                        continue
                yield response
        except Exception as e:  # noqa: BLE001
            dispatcher.fail_all(e)
            raise

    return _gen()


class _AsyncResponseFilter:
    def __init__(self, client: Any, inner) -> None:
        self._client = client
        self._inner = inner
        self._dispatcher = _get_async_dispatcher(client)

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            msg = await self._inner.__anext__()
            payload_type = msg.WhichOneof("payload")
            if payload_type == "tunnel_data":
                if self._dispatcher.handle_data(msg.tunnel_data):
                    continue
            elif payload_type == "tunnel_ack":
                if self._dispatcher.handle_ack(msg.tunnel_ack):
                    continue
            elif payload_type == "tunnel_close":
                if self._dispatcher.handle_close(msg.tunnel_close):
                    continue
            return msg

    def cancel(self):
        return self._inner.cancel()


# ---------------------------------------------------------------------------
# Upstream tunnel-ack send (proto-gap workaround)
# ---------------------------------------------------------------------------

def _send_tunnel_ack_upstream_sync(client: Any, tunnel_id: str, credits: int) -> None:
    """Emit an upstream TunnelAck granting credits to the sidecar."""
    if credits <= 0:
        return
    try:
        ack = aether_pb2.TunnelAck(tunnel_id=tunnel_id, credits=credits)
        client.request_queue.put(aether_pb2.UpstreamMessage(tunnel_ack=ack))
    except (ValueError, TypeError):
        return


async def _send_tunnel_ack_upstream_async(client: Any, tunnel_id: str, credits: int) -> None:
    """Emit an upstream TunnelAck (async) granting credits to the sidecar."""
    if credits <= 0:
        return
    try:
        ack = aether_pb2.TunnelAck(tunnel_id=tunnel_id, credits=credits)
        await client._request_queue.put(aether_pb2.UpstreamMessage(tunnel_ack=ack))
    except (ValueError, TypeError):
        return


# ---------------------------------------------------------------------------
# Sync AetherTunnel
# ---------------------------------------------------------------------------

class AetherTunnel:
    """Synchronous tunnel handle. ``read``/``write``/``close``/``shutdown``."""

    def __init__(
        self,
        client: Any,
        state: _SyncTunnelState,
        dispatcher: _SyncTunnelDispatcher,
    ) -> None:
        self._client = client
        self._state = state
        self._dispatcher = dispatcher
        self.tunnel_id = state.tunnel_id

    # -- properties ---------------------------------------------------------

    @property
    def closed(self) -> bool:
        with self._state.lock:
            return self._state.closed and not self._state.inbound

    # -- IO -----------------------------------------------------------------

    def read(self, n: int = -1) -> bytes:
        s = self._state
        out_credits = 0
        out: bytes = b""
        with s.lock:
            while True:
                if s.error and not s.inbound:
                    raise s.error
                if s.inbound:
                    if n is None or n < 0:
                        out = bytes(s.inbound)
                        s.inbound.clear()
                    else:
                        take = min(n, len(s.inbound))
                        out = bytes(s.inbound[:take])
                        del s.inbound[:take]
                    s.inbound_consumed_since_ack += len(out)
                    if s.inbound_consumed_since_ack >= INBOUND_ACK_THRESHOLD:
                        out_credits = s.inbound_consumed_since_ack
                        s.inbound_consumed_since_ack = 0
                    break
                if s.inbound_eof:
                    if s.error:
                        raise s.error
                    return b""
                if s.closed:
                    if s.error:
                        raise s.error
                    return b""
                s.cond.wait()
        if out_credits:
            _send_tunnel_ack_upstream_sync(self._client, self.tunnel_id, out_credits)
        return out

    def write(self, b: bytes) -> int:
        if not isinstance(b, (bytes, bytearray, memoryview)):
            raise TypeError("write requires bytes-like object")
        data = bytes(b)
        if not data:
            return 0
        s = self._state
        total = 0
        offset = 0
        while offset < len(data):
            with s.lock:
                while True:
                    if s.error:
                        raise s.error
                    if s.closed:
                        raise TunnelClosedError("tunnel closed", code="CLOSED")
                    if s.outbound_fin_sent:
                        raise TunnelClosedError("write after shutdown(SHUT_WR)", code="HALF_CLOSED")
                    if s.outbound_credits > 0:
                        break
                    s.cond.wait()
                avail = min(s.outbound_credits, TUNNEL_CHUNK_SIZE, len(data) - offset)
                piece = data[offset:offset + avail]
                seq = s.outbound_seq
                s.outbound_seq += 1
                s.outbound_credits -= avail
            frame = aether_pb2.TunnelData(
                tunnel_id=self.tunnel_id, seq=seq, data=piece, fin=False
            )
            self._client.request_queue.put(aether_pb2.UpstreamMessage(tunnel_data=frame))
            offset += avail
            total += avail
        return total

    def shutdown(self, how: int) -> None:
        """Half-close. ``how`` mirrors socket.SHUT_RD/SHUT_WR/SHUT_RDWR."""
        s = self._state
        if how in (socket.SHUT_WR, socket.SHUT_RDWR):
            with s.lock:
                if s.outbound_fin_sent or s.closed:
                    pass
                else:
                    s.outbound_fin_sent = True
                    seq = s.outbound_seq
                    s.outbound_seq += 1
            if not s.closed:
                fin = aether_pb2.TunnelData(
                    tunnel_id=self.tunnel_id, seq=seq, data=b"", fin=True
                )
                try:
                    self._client.request_queue.put(aether_pb2.UpstreamMessage(tunnel_data=fin))
                except Exception:  # noqa: BLE001
                    pass
        if how in (socket.SHUT_RD, socket.SHUT_RDWR):
            with s.lock:
                s.inbound.clear()
                s.inbound_eof = True
                s.signal()

    def close(self) -> None:
        s = self._state
        with s.lock:
            already = s.closed
            s.closed = True
            s.signal()
        if not already:
            try:
                self._client.request_queue.put(
                    aether_pb2.UpstreamMessage(
                        tunnel_close=aether_pb2.TunnelClose(
                            tunnel_id=self.tunnel_id,
                            reason=aether_pb2.TunnelClose.NORMAL,
                        )
                    )
                )
            except Exception:  # noqa: BLE001
                pass
        self._dispatcher.unregister(self.tunnel_id)

    # -- context manager ----------------------------------------------------

    def __enter__(self) -> "AetherTunnel":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Async AetherTunnel
# ---------------------------------------------------------------------------

class AsyncAetherTunnel:
    """Async tunnel handle. ``read``/``write``/``close``/``shutdown``."""

    def __init__(
        self,
        client: Any,
        state: _AsyncTunnelState,
        dispatcher: _AsyncTunnelDispatcher,
    ) -> None:
        self._client = client
        self._state = state
        self._dispatcher = dispatcher
        self.tunnel_id = state.tunnel_id

    @property
    def closed(self) -> bool:
        return self._state.closed and not self._state.inbound

    async def read(self, n: int = -1) -> bytes:
        s = self._state
        while True:
            if s.error and not s.inbound:
                raise s.error
            if s.inbound:
                if n is None or n < 0:
                    out = bytes(s.inbound)
                    s.inbound.clear()
                else:
                    take = min(n, len(s.inbound))
                    out = bytes(s.inbound[:take])
                    del s.inbound[:take]
                s.inbound_consumed_since_ack += len(out)
                if s.inbound_consumed_since_ack >= INBOUND_ACK_THRESHOLD:
                    credits = s.inbound_consumed_since_ack
                    s.inbound_consumed_since_ack = 0
                    await _send_tunnel_ack_upstream_async(self._client, self.tunnel_id, credits)
                return out
            if s.inbound_eof:
                if s.error:
                    raise s.error
                return b""
            if s.closed:
                if s.error:
                    raise s.error
                return b""
            s.event.clear()
            await s.event.wait()

    async def write(self, b: bytes) -> int:
        if not isinstance(b, (bytes, bytearray, memoryview)):
            raise TypeError("write requires bytes-like object")
        data = bytes(b)
        if not data:
            return 0
        s = self._state
        total = 0
        offset = 0
        while offset < len(data):
            while True:
                if s.error:
                    raise s.error
                if s.closed:
                    raise TunnelClosedError("tunnel closed", code="CLOSED")
                if s.outbound_fin_sent:
                    raise TunnelClosedError("write after shutdown(SHUT_WR)", code="HALF_CLOSED")
                if s.outbound_credits > 0:
                    break
                s.event.clear()
                await s.event.wait()
            avail = min(s.outbound_credits, TUNNEL_CHUNK_SIZE, len(data) - offset)
            piece = data[offset:offset + avail]
            seq = s.outbound_seq
            s.outbound_seq += 1
            s.outbound_credits -= avail
            frame = aether_pb2.TunnelData(
                tunnel_id=self.tunnel_id, seq=seq, data=piece, fin=False
            )
            await self._client._request_queue.put(
                aether_pb2.UpstreamMessage(tunnel_data=frame)
            )
            offset += avail
            total += avail
        return total

    async def shutdown(self, how: int) -> None:
        s = self._state
        if how in (socket.SHUT_WR, socket.SHUT_RDWR):
            send_fin = False
            seq = 0
            if not s.outbound_fin_sent and not s.closed:
                s.outbound_fin_sent = True
                seq = s.outbound_seq
                s.outbound_seq += 1
                send_fin = True
            if send_fin:
                fin = aether_pb2.TunnelData(
                    tunnel_id=self.tunnel_id, seq=seq, data=b"", fin=True
                )
                try:
                    await self._client._request_queue.put(
                        aether_pb2.UpstreamMessage(tunnel_data=fin)
                    )
                except Exception:  # noqa: BLE001
                    pass
        if how in (socket.SHUT_RD, socket.SHUT_RDWR):
            s.inbound.clear()
            s.inbound_eof = True
            s.event.set()

    async def close(self) -> None:
        s = self._state
        already = s.closed
        s.closed = True
        s.event.set()
        if not already:
            try:
                await self._client._request_queue.put(
                    aether_pb2.UpstreamMessage(
                        tunnel_close=aether_pb2.TunnelClose(
                            tunnel_id=self.tunnel_id,
                            reason=aether_pb2.TunnelClose.NORMAL,
                        )
                    )
                )
            except Exception:  # noqa: BLE001
                pass
        self._dispatcher.unregister(self.tunnel_id)

    async def __aenter__(self) -> "AsyncAetherTunnel":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _build_tunnel_open(
    *,
    tunnel_id: str,
    target_topic: str,
    protocol: int,
    remote_hint: Optional[str],
    metadata: Optional[Mapping[str, str]],
    authorization: Optional[aether_pb2.AuthorizationContext],
    idle_timeout_ms: int,
    max_bytes: int,
    backend: Optional[str] = None,
) -> aether_pb2.TunnelOpen:
    tunnel_open = aether_pb2.TunnelOpen(
        tunnel_id=tunnel_id,
        target_topic=target_topic,
        protocol=protocol,
        remote_hint=remote_hint or "",
        idle_timeout_ms=idle_timeout_ms,
        max_bytes=max_bytes,
        backend_name=backend or "",
    )
    if metadata:
        for k, v in metadata.items():
            tunnel_open.metadata[k] = v
    if authorization is not None:
        tunnel_open.authorization.CopyFrom(authorization)
    return tunnel_open


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def tunnel_dial(
    client: Any,
    target_topic: str,
    protocol: str = "tcp",
    remote_hint: Optional[str] = None,
    *,
    idle_timeout_ms: int = 300_000,
    max_bytes: int = 0,
    metadata: Optional[Mapping[str, str]] = None,
    initial_credits: int = DEFAULT_INITIAL_CREDITS,
    authorization: Optional[aether_pb2.AuthorizationContext] = None,
    authority_mode: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    grant_id: Optional[str] = None,
    tunnel_id: Optional[str] = None,
    backend: Optional[str] = None,
) -> AetherTunnel:
    """Open a tunnel through Aether and return an ``AetherTunnel`` handle.

    ``backend`` optionally pins the tunnel to a named terminator backend.
    The backend's allow-list still applies — explicit naming selects which
    backend's ACL is consulted, not whether the tunnel is allowed.
    """
    _ensure_hooks_installed()

    if tunnel_id is None:
        tunnel_id = str(uuid.uuid4())
    proto = _resolve_protocol(protocol)
    auth = authorization or _build_authorization(
        authority_mode=authority_mode,
        subject_type=subject_type,
        subject_id=subject_id,
        grant_id=grant_id,
    )

    dispatcher = _get_sync_dispatcher(client)
    state = _SyncTunnelState(tunnel_id, initial_credits)
    dispatcher.register(state)

    open_msg = _build_tunnel_open(
        tunnel_id=tunnel_id,
        target_topic=target_topic,
        protocol=proto,
        remote_hint=remote_hint,
        metadata=metadata,
        authorization=auth,
        idle_timeout_ms=idle_timeout_ms,
        max_bytes=max_bytes,
        backend=backend,
    )
    client.request_queue.put(aether_pb2.UpstreamMessage(tunnel_open=open_msg))
    return AetherTunnel(client, state, dispatcher)


async def tunnel_dial_async(
    client: Any,
    target_topic: str,
    protocol: str = "tcp",
    remote_hint: Optional[str] = None,
    *,
    idle_timeout_ms: int = 300_000,
    max_bytes: int = 0,
    metadata: Optional[Mapping[str, str]] = None,
    initial_credits: int = DEFAULT_INITIAL_CREDITS,
    authorization: Optional[aether_pb2.AuthorizationContext] = None,
    authority_mode: Optional[str] = None,
    subject_type: Optional[str] = None,
    subject_id: Optional[str] = None,
    grant_id: Optional[str] = None,
    tunnel_id: Optional[str] = None,
    backend: Optional[str] = None,
) -> AsyncAetherTunnel:
    """Open a tunnel through Aether and return an ``AsyncAetherTunnel`` handle.

    ``backend`` optionally pins the tunnel to a named terminator backend.
    """
    _ensure_hooks_installed()

    if tunnel_id is None:
        tunnel_id = str(uuid.uuid4())
    proto = _resolve_protocol(protocol)
    auth = authorization or _build_authorization(
        authority_mode=authority_mode,
        subject_type=subject_type,
        subject_id=subject_id,
        grant_id=grant_id,
    )

    loop = asyncio.get_event_loop()
    dispatcher = _get_async_dispatcher(client)
    state = _AsyncTunnelState(tunnel_id, initial_credits, loop)
    dispatcher.register(state)

    open_msg = _build_tunnel_open(
        tunnel_id=tunnel_id,
        target_topic=target_topic,
        protocol=proto,
        remote_hint=remote_hint,
        metadata=metadata,
        authorization=auth,
        idle_timeout_ms=idle_timeout_ms,
        max_bytes=max_bytes,
        backend=backend,
    )
    await client._request_queue.put(aether_pb2.UpstreamMessage(tunnel_open=open_msg))
    return AsyncAetherTunnel(client, state, dispatcher)


# Convenience: bind methods on BaseAetherClient / BaseAsyncAetherClient at import time.
def _bind_client_methods() -> None:
    try:
        from . import client as _client_mod
        from . import client_async as _client_async_mod
    except Exception:  # noqa: BLE001
        return

    def _tunnel_dial_method(self, target_topic, protocol="tcp", remote_hint=None, **kw):
        return tunnel_dial(self, target_topic, protocol, remote_hint, **kw)

    async def _tunnel_dial_async_method(self, target_topic, protocol="tcp", remote_hint=None, **kw):
        return await tunnel_dial_async(self, target_topic, protocol, remote_hint, **kw)

    setattr(_client_mod.BaseAetherClient, "tunnel_dial", _tunnel_dial_method)
    setattr(_client_async_mod.BaseAsyncAetherClient, "tunnel_dial_async", _tunnel_dial_async_method)


_bind_client_methods()


__all__ = [
    "TUNNEL_CHUNK_SIZE",
    "DEFAULT_INITIAL_CREDITS",
    "AetherTunnel",
    "AsyncAetherTunnel",
    "TunnelError",
    "TunnelClosedError",
    "TunnelPeerResetError",
    "TunnelIdleTimeoutError",
    "TunnelQuotaExceededError",
    "tunnel_dial",
    "tunnel_dial_async",
]
