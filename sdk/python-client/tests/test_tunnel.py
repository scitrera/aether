"""Tests for tunnel_dial / AetherTunnel."""
from __future__ import annotations

import asyncio
import os
import queue
import socket
import threading
import time
from typing import List, Optional

import pytest

from scitrera_aether_client import tunnel as tunnel_mod
from scitrera_aether_client.proto import aether_pb2
from scitrera_aether_client.tunnel import (
    TUNNEL_CHUNK_SIZE,
    AetherTunnel,
    AsyncAetherTunnel,
    TunnelClosedError,
    TunnelIdleTimeoutError,
    TunnelPeerResetError,
    TunnelQuotaExceededError,
    tunnel_dial,
    tunnel_dial_async,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class _SyncClientStub:
    def __init__(self) -> None:
        self.request_queue: queue.Queue = queue.Queue()
        self._tunnel_dispatcher = None

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
        self._tunnel_dispatcher_async = None

    async def drain_upstream(self) -> List[aether_pb2.UpstreamMessage]:
        out: List[aether_pb2.UpstreamMessage] = []
        while True:
            try:
                out.append(self._request_queue.get_nowait())
            except asyncio.QueueEmpty:
                return out


def _push_data(client, tunnel_id: str, data: bytes, *, fin: bool = False) -> None:
    frame = aether_pb2.TunnelData(tunnel_id=tunnel_id, data=data, fin=fin)
    client._tunnel_dispatcher.handle_data(frame)


def _push_ack(client, tunnel_id: str, credits: int) -> None:
    frame = aether_pb2.TunnelAck(tunnel_id=tunnel_id, credits=credits)
    client._tunnel_dispatcher.handle_ack(frame)


def _push_close(client, tunnel_id: str, reason: int, detail: str = "") -> None:
    frame = aether_pb2.TunnelClose(tunnel_id=tunnel_id, reason=reason, detail=detail)
    client._tunnel_dispatcher.handle_close(frame)


def _drain_upstream_data(msgs):
    """Filter UpstreamMessage list to tunnel_data frames."""
    return [m.tunnel_data for m in msgs if m.WhichOneof("payload") == "tunnel_data"]


# ---------------------------------------------------------------------------
# Sync lifecycle
# ---------------------------------------------------------------------------

def test_open_write_read_close_lifecycle():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="tid-1")
    assert isinstance(tun, AetherTunnel)
    assert tun.tunnel_id == "tid-1"

    # First upstream is the TunnelOpen
    msgs = client.drain_upstream()
    assert msgs[0].WhichOneof("payload") == "tunnel_open"
    open_msg = msgs[0].tunnel_open
    assert open_msg.tunnel_id == "tid-1"
    assert open_msg.target_topic == "sv::svc::default"
    assert open_msg.protocol == aether_pb2.TunnelOpen.TCP

    # Write some data — caller -> sidecar
    n = tun.write(b"hello world")
    assert n == 11
    msgs = client.drain_upstream()
    data_frames = _drain_upstream_data(msgs)
    assert len(data_frames) == 1
    assert data_frames[0].data == b"hello world"
    assert data_frames[0].fin is False

    # Inbound data arrives from sidecar
    _push_data(client, "tid-1", b"server-says-hi")
    assert tun.read(64) == b"server-says-hi"

    # Close and verify TunnelClose emitted
    tun.close()
    msgs = client.drain_upstream()
    closes = [m for m in msgs if m.WhichOneof("payload") == "tunnel_close"]
    assert len(closes) == 1
    assert closes[0].tunnel_close.reason == aether_pb2.TunnelClose.NORMAL

    # Idempotent close
    tun.close()
    msgs = client.drain_upstream()
    assert not [m for m in msgs if m.WhichOneof("payload") == "tunnel_close"]


def test_protocol_resolution():
    client = _SyncClientStub()
    tunnel_dial(client, "sv::svc::default", "udp", tunnel_id="t-udp")
    msgs = client.drain_upstream()
    assert msgs[0].tunnel_open.protocol == aether_pb2.TunnelOpen.UDP

    client = _SyncClientStub()
    tunnel_dial(client, "sv::svc::default", "ws", tunnel_id="t-ws")
    msgs = client.drain_upstream()
    assert msgs[0].tunnel_open.protocol == aether_pb2.TunnelOpen.WEBSOCKET


def test_unknown_protocol_raises():
    client = _SyncClientStub()
    with pytest.raises(ValueError):
        tunnel_dial(client, "sv::svc::default", "smtp", tunnel_id="bad")


def test_metadata_and_options_passed():
    client = _SyncClientStub()
    tunnel_dial(
        client, "sv::svc::default", "tcp",
        remote_hint="vnc:5901",
        idle_timeout_ms=60_000,
        max_bytes=1024,
        metadata={"sub": "wamp"},
        tunnel_id="t-meta",
    )
    msgs = client.drain_upstream()
    open_msg = msgs[0].tunnel_open
    assert open_msg.remote_hint == "vnc:5901"
    assert open_msg.idle_timeout_ms == 60_000
    assert open_msg.max_bytes == 1024
    assert dict(open_msg.metadata) == {"sub": "wamp"}


def test_session_token_not_set_in_v1():
    client = _SyncClientStub()
    tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-st")
    msgs = client.drain_upstream()
    assert msgs[0].tunnel_open.session_token == ""


def test_obo_authorization_injected():
    client = _SyncClientStub()
    tunnel_dial(
        client, "sv::svc::default", "tcp",
        authority_mode="obo",
        subject_type="user",
        subject_id="u-99",
        grant_id="g-77",
        tunnel_id="t-obo",
    )
    msgs = client.drain_upstream()
    open_msg = msgs[0].tunnel_open
    assert open_msg.authorization.authority_mode == "obo"
    assert open_msg.authorization.subject.principal_id == "u-99"
    assert open_msg.authorization.grant_id == "g-77"


# ---------------------------------------------------------------------------
# 10 MB echo round-trip
# ---------------------------------------------------------------------------

def test_echo_round_trip_10mb():
    client = _SyncClientStub()
    tun = tunnel_dial(
        client, "sv::svc::default", "tcp",
        initial_credits=10 * 1024 * 1024 + TUNNEL_CHUNK_SIZE,
        tunnel_id="t-echo",
    )
    payload = os.urandom(10 * 1024 * 1024)

    # Echo loop: read upstream tunnel_data frames and feed each one back as inbound.
    stop = threading.Event()
    received_chunks: List[bytes] = []

    def _echoer():
        while not stop.is_set():
            try:
                msg = client.request_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            kind = msg.WhichOneof("payload")
            if kind == "tunnel_open":
                continue
            if kind == "tunnel_close":
                stop.set()
                return
            if kind == "tunnel_data":
                if msg.tunnel_data.fin:
                    return
                received_chunks.append(msg.tunnel_data.data)
                _push_data(client, "t-echo", msg.tunnel_data.data, fin=False)

    t = threading.Thread(target=_echoer, daemon=True)
    t.start()

    # Writer thread: emit the payload in chunks (write() will split as needed).
    def _writer():
        tun.write(payload)
    w = threading.Thread(target=_writer, daemon=True)
    w.start()

    # Reader: pull until we have the full payload back.
    out = bytearray()
    deadline = time.monotonic() + 30.0
    while len(out) < len(payload) and time.monotonic() < deadline:
        chunk = tun.read(TUNNEL_CHUNK_SIZE)
        if not chunk:
            break
        out.extend(chunk)

    stop.set()
    w.join(timeout=5.0)
    t.join(timeout=5.0)
    assert bytes(out) == payload
    # Frames the caller emitted match expected count
    assert b"".join(received_chunks) == payload


# ---------------------------------------------------------------------------
# Half-close (FIN)
# ---------------------------------------------------------------------------

def test_shutdown_shut_wr_sends_fin():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-fin")
    client.drain_upstream()  # discard open

    tun.write(b"abc")
    tun.shutdown(socket.SHUT_WR)

    msgs = client.drain_upstream()
    data_frames = _drain_upstream_data(msgs)
    assert any(f.fin for f in data_frames)
    assert data_frames[-1].fin is True
    assert data_frames[-1].data == b""

    # Subsequent write must fail
    with pytest.raises(TunnelClosedError):
        tun.write(b"more")


def test_inbound_fin_makes_read_return_empty():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-rfin")

    _push_data(client, "t-rfin", b"part1")
    _push_data(client, "t-rfin", b"", fin=True)

    assert tun.read(8) == b"part1"
    assert tun.read(8) == b""
    assert tun.read() == b""


# ---------------------------------------------------------------------------
# Close-reason error mapping
# ---------------------------------------------------------------------------

def test_idle_timeout_close_raises():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-idle")
    _push_close(client, "t-idle", aether_pb2.TunnelClose.IDLE_TIMEOUT, "idle 300s")

    with pytest.raises(TunnelIdleTimeoutError):
        tun.read(8)


def test_quota_close_raises():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", max_bytes=1024, tunnel_id="t-q")
    _push_close(client, "t-q", aether_pb2.TunnelClose.QUOTA, "1024 exceeded")

    with pytest.raises(TunnelQuotaExceededError):
        tun.read(8)
    with pytest.raises(TunnelQuotaExceededError):
        tun.write(b"more")


def test_peer_reset_close_raises():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-pr")
    _push_close(client, "t-pr", aether_pb2.TunnelClose.PEER_RESET, "backend gone")

    with pytest.raises(TunnelPeerResetError):
        tun.read(8)


def test_normal_close_drains_pending_then_eof():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-nc")
    _push_data(client, "t-nc", b"tail-bytes")
    _push_close(client, "t-nc", aether_pb2.TunnelClose.NORMAL, "")
    assert tun.read(64) == b"tail-bytes"
    assert tun.read(64) == b""


# ---------------------------------------------------------------------------
# Outbound flow control
# ---------------------------------------------------------------------------

def test_writer_blocks_until_ack_arrives():
    client = _SyncClientStub()
    tun = tunnel_dial(
        client, "sv::svc::default", "tcp",
        initial_credits=8,  # very small
        tunnel_id="t-fc",
    )
    client.drain_upstream()  # drop open

    payload = b"X" * 64  # 8x more than initial credits
    done = threading.Event()
    err: List[BaseException] = []

    def _do_write():
        try:
            tun.write(payload)
        except BaseException as e:  # noqa: BLE001
            err.append(e)
        finally:
            done.set()

    threading.Thread(target=_do_write, daemon=True).start()

    # Wait long enough for the first 8 bytes to flush, then verify we're stalled.
    time.sleep(0.1)
    assert not done.is_set()
    msgs = client.drain_upstream()
    sent = sum(len(m.tunnel_data.data) for m in msgs if m.WhichOneof("payload") == "tunnel_data")
    assert sent == 8
    assert not done.is_set()

    # Replenish credits in chunks of 8 to drain the rest.
    for _ in range(7):
        _push_ack(client, "t-fc", 8)
    done.wait(timeout=5.0)
    assert done.is_set()
    assert not err

    msgs = client.drain_upstream()
    sent2 = sum(len(m.tunnel_data.data) for m in msgs if m.WhichOneof("payload") == "tunnel_data")
    assert sent + sent2 == 64


def test_write_chunk_size_caps_at_256k():
    client = _SyncClientStub()
    tun = tunnel_dial(
        client, "sv::svc::default", "tcp",
        initial_credits=10 * TUNNEL_CHUNK_SIZE,
        tunnel_id="t-chk",
    )
    client.drain_upstream()
    body = os.urandom(TUNNEL_CHUNK_SIZE * 3 + 17)
    tun.write(body)
    msgs = client.drain_upstream()
    data_frames = _drain_upstream_data(msgs)
    assert all(len(f.data) <= TUNNEL_CHUNK_SIZE for f in data_frames)
    assert b"".join(f.data for f in data_frames) == body


# ---------------------------------------------------------------------------
# Two concurrent tunnels independent
# ---------------------------------------------------------------------------

def test_two_tunnels_are_independent():
    client = _SyncClientStub()
    a = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="A")
    b = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="B")
    assert a.tunnel_id != b.tunnel_id

    _push_data(client, "A", b"only-A")
    _push_data(client, "B", b"only-B")

    assert a.read(64) == b"only-A"
    assert b.read(64) == b"only-B"

    # Closing one should not affect the other
    a.close()
    _push_data(client, "B", b"still-flowing")
    assert b.read(64) == b"still-flowing"
    b.close()


# ---------------------------------------------------------------------------
# read(n) semantics: short reads + blocking
# ---------------------------------------------------------------------------

def test_read_returns_partial_when_buffer_smaller():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-pr")
    _push_data(client, "t-pr", b"abc")
    assert tun.read(2) == b"ab"
    assert tun.read(2) == b"c"


def test_read_blocks_until_data_arrives():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", tunnel_id="t-blk")

    result: List[bytes] = []
    def _reader():
        result.append(tun.read(16))
    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    time.sleep(0.05)
    assert not result  # still blocked
    _push_data(client, "t-blk", b"payload")
    t.join(timeout=2.0)
    assert result == [b"payload"]


# ---------------------------------------------------------------------------
# Async path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_open_write_read_close():
    client = _AsyncClientStub()
    tun = await tunnel_dial_async(client, "sv::svc::default", "tcp", tunnel_id="at-1")
    assert isinstance(tun, AsyncAetherTunnel)

    msgs = await client.drain_upstream()
    assert msgs[0].WhichOneof("payload") == "tunnel_open"

    n = await tun.write(b"hello")
    assert n == 5
    msgs = await client.drain_upstream()
    data_frames = [m.tunnel_data for m in msgs if m.WhichOneof("payload") == "tunnel_data"]
    assert data_frames[0].data == b"hello"

    # Inbound
    frame = aether_pb2.TunnelData(tunnel_id="at-1", data=b"server-bytes")
    client._tunnel_dispatcher_async.handle_data(frame)
    # Allow signal to propagate (call_soon_threadsafe)
    await asyncio.sleep(0)
    got = await tun.read(64)
    assert got == b"server-bytes"

    await tun.close()
    msgs = await client.drain_upstream()
    closes = [m for m in msgs if m.WhichOneof("payload") == "tunnel_close"]
    assert len(closes) == 1


@pytest.mark.asyncio
async def test_async_idle_timeout():
    client = _AsyncClientStub()
    tun = await tunnel_dial_async(client, "sv::svc::default", "tcp", tunnel_id="at-i")
    frame = aether_pb2.TunnelClose(
        tunnel_id="at-i", reason=aether_pb2.TunnelClose.IDLE_TIMEOUT, detail=""
    )
    client._tunnel_dispatcher_async.handle_close(frame)
    await asyncio.sleep(0)
    with pytest.raises(TunnelIdleTimeoutError):
        await tun.read(8)


@pytest.mark.asyncio
async def test_async_outbound_flow_control():
    client = _AsyncClientStub()
    tun = await tunnel_dial_async(
        client, "sv::svc::default", "tcp",
        initial_credits=8,
        tunnel_id="at-fc",
    )
    await client.drain_upstream()

    payload = b"Y" * 24
    write_task = asyncio.create_task(tun.write(payload))

    # Give the writer a chance to send up to 8 bytes
    await asyncio.sleep(0.05)
    assert not write_task.done()
    msgs = await client.drain_upstream()
    sent = sum(len(m.tunnel_data.data) for m in msgs if m.WhichOneof("payload") == "tunnel_data")
    assert sent == 8

    # Replenish in two rounds to fully drain
    client._tunnel_dispatcher_async.handle_ack(
        aether_pb2.TunnelAck(tunnel_id="at-fc", credits=8)
    )
    await asyncio.sleep(0)
    client._tunnel_dispatcher_async.handle_ack(
        aether_pb2.TunnelAck(tunnel_id="at-fc", credits=8)
    )
    await asyncio.wait_for(write_task, timeout=2.0)

    msgs = await client.drain_upstream()
    sent_total = sent + sum(
        len(m.tunnel_data.data) for m in msgs if m.WhichOneof("payload") == "tunnel_data"
    )
    assert sent_total == 24


@pytest.mark.asyncio
async def test_async_shutdown_shut_wr_sends_fin():
    client = _AsyncClientStub()
    tun = await tunnel_dial_async(client, "sv::svc::default", "tcp", tunnel_id="at-fin")
    await client.drain_upstream()

    await tun.write(b"abc")
    await tun.shutdown(socket.SHUT_WR)
    msgs = await client.drain_upstream()
    data_frames = [m.tunnel_data for m in msgs if m.WhichOneof("payload") == "tunnel_data"]
    assert any(f.fin for f in data_frames)


@pytest.mark.asyncio
async def test_async_two_tunnels_independent():
    client = _AsyncClientStub()
    a = await tunnel_dial_async(client, "sv::svc::default", "tcp", tunnel_id="aA")
    b = await tunnel_dial_async(client, "sv::svc::default", "tcp", tunnel_id="aB")
    client._tunnel_dispatcher_async.handle_data(
        aether_pb2.TunnelData(tunnel_id="aA", data=b"A-only")
    )
    client._tunnel_dispatcher_async.handle_data(
        aether_pb2.TunnelData(tunnel_id="aB", data=b"B-only")
    )
    await asyncio.sleep(0)
    assert await a.read(64) == b"A-only"
    assert await b.read(64) == b"B-only"
    await a.close()
    await b.close()


def test_tunnel_dial_backend_kwarg_emitted_on_envelope():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", "10.0.0.1:5000",
                      tunnel_id="tid-backend", backend="tcp-b")
    msgs = client.drain_upstream()
    open_msgs = [m.tunnel_open for m in msgs if m.WhichOneof("payload") == "tunnel_open"]
    assert len(open_msgs) == 1
    assert open_msgs[0].backend_name == "tcp-b"
    assert open_msgs[0].remote_hint == "10.0.0.1:5000"
    tun.close()


def test_tunnel_dial_backend_kwarg_omitted_leaves_field_empty():
    client = _SyncClientStub()
    tun = tunnel_dial(client, "sv::svc::default", "tcp", "10.0.0.1:5000",
                      tunnel_id="tid-no-backend")
    msgs = client.drain_upstream()
    open_msgs = [m.tunnel_open for m in msgs if m.WhichOneof("payload") == "tunnel_open"]
    assert len(open_msgs) == 1
    assert open_msgs[0].backend_name == ""
    tun.close()
