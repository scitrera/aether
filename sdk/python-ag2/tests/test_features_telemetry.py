"""Unit tests for AetherTelemetry — mock-only, no live gateway required."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scitrera_aether_ag2.features.telemetry import AetherTelemetry


# ---------------------------------------------------------------------------
# Helpers / stubs
# ---------------------------------------------------------------------------

def _make_client(
    send_event_mock: AsyncMock | None = None,
    send_metric_mock: AsyncMock | None = None,
) -> MagicMock:
    """Return a minimal AsyncAgentClient stub with recorded send_* methods."""
    client = MagicMock()
    client.send_event = send_event_mock or AsyncMock()
    client.send_metric = send_metric_mock or AsyncMock()
    return client


def _decode_event(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode())


def _make_request(messages: list[dict[str, Any]] | None = None) -> MagicMock:
    """Minimal RequestMessage stub."""
    req = MagicMock()
    req.messages = messages or [{"role": "user", "content": "hello"}]
    return req


def _make_response(
    message: dict[str, Any] | None = None,
    streaming_text: str | None = None,
) -> MagicMock:
    """Minimal ServiceResponse stub."""
    resp = MagicMock()
    resp.message = message
    resp.streaming_text = streaming_text
    resp.context = None
    resp.input_required = None
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_on_request_received_emits_event() -> None:
    """on_request_received calls send_event once with the expected payload structure."""
    send_event = AsyncMock()
    client = _make_client(send_event_mock=send_event)
    tel = AetherTelemetry(client, agent_name="test-agent")

    request = _make_request(messages=[{"role": "user", "content": "hi"}, {"role": "system", "content": "sys"}])
    await tel.on_request_received("corr-001", request)

    send_event.assert_awaited_once()
    payload = _decode_event(send_event.call_args[0][0])

    assert payload["name"] == "ag2.request.received"
    assert payload["correlation_id"] == "corr-001"
    assert payload["message_count"] == 2
    assert payload["trace_id"] == "corr-001"
    assert payload["agent"] == "test-agent"
    assert "timestamp_ms" in payload


@pytest.mark.asyncio
async def test_on_response_chunk_records_metric() -> None:
    """on_response_chunk calls send_metric once (chunk counter increment)."""
    send_metric = AsyncMock()
    client = _make_client(send_metric_mock=send_metric)
    tel = AetherTelemetry(client, agent_name="test-agent")

    response = _make_response(streaming_text="Hello")
    await tel.on_response_chunk("corr-002", response)

    send_metric.assert_awaited_once()
    # The metric proto is opaque to us here; just verify it was sent
    metric_arg = send_metric.call_args[0][0]
    assert metric_arg is not None  # real aether_pb2.Metric instance


@pytest.mark.asyncio
async def test_on_request_completed_emits_terminal_event() -> None:
    """on_request_completed sends an event with correlation_id and total_chunks."""
    send_event = AsyncMock()
    client = _make_client(send_event_mock=send_event)
    tel = AetherTelemetry(client, agent_name="test-agent")

    await tel.on_request_completed("corr-003", total_chunks=5)

    send_event.assert_awaited_once()
    payload = _decode_event(send_event.call_args[0][0])

    assert payload["name"] == "ag2.request.completed"
    assert payload["correlation_id"] == "corr-003"
    assert payload["total_chunks"] == 5
    assert payload["trace_id"] == "corr-003"


@pytest.mark.asyncio
async def test_on_request_failed_emits_failed_event() -> None:
    """on_request_failed emits a failed event with error_code and retryable fields."""
    send_event = AsyncMock()
    client = _make_client(send_event_mock=send_event)
    tel = AetherTelemetry(client, agent_name="test-agent")

    error = {"error_code": "timeout", "retryable": True}
    await tel.on_request_failed("corr-004", error)

    send_event.assert_awaited_once()
    payload = _decode_event(send_event.call_args[0][0])

    assert payload["name"] == "ag2.request.failed"
    assert payload["correlation_id"] == "corr-004"
    assert payload["error_code"] == "timeout"
    assert payload["retryable"] is True
    assert payload["trace_id"] == "corr-004"


@pytest.mark.asyncio
async def test_emit_event_standalone_with_explicit_trace_id() -> None:
    """emit_event works standalone with an explicit trace_id."""
    send_event = AsyncMock()
    client = _make_client(send_event_mock=send_event)
    tel = AetherTelemetry(client, agent_name="my-agent")

    await tel.emit_event(
        "custom.event",
        payload={"key": "value"},
        trace_id="trace-xyz",
    )

    send_event.assert_awaited_once()
    payload = _decode_event(send_event.call_args[0][0])

    assert payload["name"] == "custom.event"
    assert payload["key"] == "value"
    assert payload["trace_id"] == "trace-xyz"
    assert payload["agent"] == "my-agent"


@pytest.mark.asyncio
async def test_emit_metric_standalone_with_explicit_trace_id() -> None:
    """emit_metric works standalone with tags and explicit trace_id."""
    send_metric = AsyncMock()
    client = _make_client(send_metric_mock=send_metric)
    tel = AetherTelemetry(client, agent_name="my-agent")

    await tel.emit_metric(
        "custom.metric",
        qty=3.5,
        kind="gauge",
        tags={"env": "test"},
        trace_id="trace-abc",
    )

    send_metric.assert_awaited_once()
    metric_arg = send_metric.call_args[0][0]
    assert metric_arg is not None

    # Verify the proto was constructed correctly
    assert metric_arg.trace_id == "trace-abc"
    assert len(metric_arg.entries) == 1
    entry = metric_arg.entries[0]
    assert entry.name == "custom.metric"
    assert entry.kind == "gauge"
    assert abs(entry.qty - 3.5) < 1e-9
    assert metric_arg.metadata["agent"] == "my-agent"
    assert metric_arg.metadata["env"] == "test"


@pytest.mark.asyncio
async def test_emit_event_send_failure_is_swallowed() -> None:
    """A failure in send_event does not propagate — it is logged and swallowed."""
    send_event = AsyncMock(side_effect=RuntimeError("network error"))
    client = _make_client(send_event_mock=send_event)
    tel = AetherTelemetry(client, agent_name="test-agent")

    # Should not raise
    await tel.emit_event("test.event", payload={})
    send_event.assert_awaited_once()


@pytest.mark.asyncio
async def test_emit_metric_no_op_when_metrics_unavailable() -> None:
    """When _METRICS_AVAILABLE is False, emit_metric is a no-op and send_metric is never called."""
    send_metric = AsyncMock()
    client = _make_client(send_metric_mock=send_metric)
    tel = AetherTelemetry(client, agent_name="test-agent")

    with patch("scitrera_aether_ag2.features.telemetry._METRICS_AVAILABLE", False):
        await tel.emit_metric("some.metric", qty=1.0)

    send_metric.assert_not_awaited()


@pytest.mark.asyncio
async def test_dropped_event_increments_counter() -> None:
    """Each failed send_event bumps dropped_events; dropped_count reflects the sum."""
    send_event = AsyncMock(side_effect=RuntimeError("network down"))
    client = _make_client(send_event_mock=send_event)
    tel = AetherTelemetry(client, agent_name="test-agent")

    assert tel.dropped_count == 0
    await tel.emit_event("evt", payload={})
    await tel.emit_event("evt", payload={})
    assert tel.dropped_events == 2
    assert tel.dropped_metrics == 0
    assert tel.dropped_count == 2


@pytest.mark.asyncio
async def test_dropped_metric_increments_counter() -> None:
    """Each failed send_metric bumps dropped_metrics."""
    send_metric = AsyncMock(side_effect=RuntimeError("metrics broken"))
    client = _make_client(send_metric_mock=send_metric)
    tel = AetherTelemetry(client, agent_name="test-agent")

    assert tel.dropped_count == 0
    await tel.emit_metric("m", qty=1.0)
    assert tel.dropped_metrics == 1
    assert tel.dropped_count == 1


@pytest.mark.asyncio
async def test_successful_sends_do_not_increment_dropped_counters() -> None:
    send_event = AsyncMock()
    send_metric = AsyncMock()
    client = _make_client(send_event_mock=send_event, send_metric_mock=send_metric)
    tel = AetherTelemetry(client, agent_name="test-agent")

    await tel.emit_event("evt", payload={})
    await tel.emit_metric("m", qty=2.0)
    assert tel.dropped_count == 0
