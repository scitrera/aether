"""Bridge ag2 events / token usage / tool calls to Aether EVENT and METRIC topics.

host integration:
  - in AetherAgentHost.__init__, build AetherTelemetry(self._client, self.identity.specifier or self.agent.name)
  - call lifecycle methods from _handle_request — on_request_received before processing,
    on_response_chunk inside the AgentService loop, on_request_completed after success,
    on_request_failed on exception

ImportError tolerance:
  If scitrera_aether_client.new_metric is unavailable at runtime (unusual install), all
  metric calls degrade to no-ops.  A WARNING is emitted once at module import time.
  Events are unaffected (send_event takes raw bytes with no extra dependency).
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional import guard for metric builder
# ---------------------------------------------------------------------------
try:
    from scitrera_aether_client.metrics import new_metric as _new_metric
    _METRICS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _METRICS_AVAILABLE = False
    logger.warning(
        "scitrera_aether_client.metrics not importable; AetherTelemetry metric calls "
        "will be no-ops.  Install scitrera-aether-client to enable metric emission."
    )

if TYPE_CHECKING:
    from scitrera_aether_client import AsyncAgentClient
    from autogen.agentchat.remote.protocol import RequestMessage, ServiceResponse


class AetherTelemetry:
    """Lifecycle emitter that maps ag2 request/response events to Aether EVENT and METRIC messages.

    This class is deliberately host-side only.  It holds a reference to an
    already-connected ``AsyncAgentClient`` and emits events/metrics through it.
    All operations are best-effort: individual send failures are caught and
    logged at WARNING level so a telemetry hiccup never kills request handling.

    Args:
        client: A connected ``AsyncAgentClient`` instance (owns the Aether connection).
        agent_name: Human-readable name used as a tag on every emitted metric/event.
    """

    def __init__(self, client: "AsyncAgentClient", agent_name: str) -> None:
        self._client = client
        self._agent_name = agent_name

    # ------------------------------------------------------------------
    # Lifecycle hooks (called by host at well-defined points)
    # ------------------------------------------------------------------

    async def on_request_received(
        self,
        correlation_id: str,
        request: "RequestMessage",
    ) -> None:
        """Emit EVENT ag2.request.received with message count."""
        await self.emit_event(
            name="ag2.request.received",
            payload={
                "correlation_id": correlation_id,
                "message_count": len(request.messages),
            },
            trace_id=correlation_id,
        )

    async def on_response_chunk(
        self,
        correlation_id: str,
        response: "ServiceResponse",
    ) -> None:
        """Emit METRIC ag2.response.chunks += 1.

        Token counts are NOT available on ServiceResponse (it exposes only
        ``message``, ``context``, ``input_required``, ``streaming_text``).
        See tech-debt entry for the resolution path.
        """
        await self.emit_metric(
            name="ag2.response.chunks",
            qty=1.0,
            kind="counter",
            tags={"agent": self._agent_name},
            trace_id=correlation_id,
        )

    async def on_request_completed(
        self,
        correlation_id: str,
        total_chunks: int,
    ) -> None:
        """Emit EVENT ag2.request.completed with total chunk count."""
        await self.emit_event(
            name="ag2.request.completed",
            payload={
                "correlation_id": correlation_id,
                "total_chunks": total_chunks,
            },
            trace_id=correlation_id,
        )

    async def on_request_failed(
        self,
        correlation_id: str,
        error: dict[str, Any],
    ) -> None:
        """Emit EVENT ag2.request.failed with error details.

        Args:
            correlation_id: Request trace identifier.
            error: Dict with at minimum ``error_code`` (str) and ``retryable`` (bool).
        """
        await self.emit_event(
            name="ag2.request.failed",
            payload={
                "correlation_id": correlation_id,
                "error_code": error.get("error_code", "unknown"),
                "retryable": error.get("retryable", False),
            },
            trace_id=correlation_id,
        )

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    async def emit_event(
        self,
        name: str,
        payload: dict[str, Any],
        *,
        trace_id: str | None = None,
    ) -> None:
        """Send an EVENT message to the Aether workflow-engine fan-in.

        Args:
            name: Event name (e.g. ``"ag2.request.received"``).
            payload: Arbitrary JSON-serialisable dict.  ``name``, ``agent``,
                ``timestamp_ms``, and (optionally) ``trace_id`` are merged in
                automatically.
            trace_id: Optional correlation/trace identifier.
        """
        body: dict[str, Any] = {
            "name": name,
            "agent": self._agent_name,
            "timestamp_ms": int(time.time() * 1000),
        }
        if trace_id is not None:
            body["trace_id"] = trace_id
        body.update(payload)
        raw = json.dumps(body).encode()
        try:
            await self._client.send_event(raw)
        except Exception:
            logger.warning("AetherTelemetry: failed to emit event %r", name, exc_info=True)

    async def emit_metric(
        self,
        name: str,
        qty: float,
        *,
        kind: str = "counter",
        tags: dict[str, str] | None = None,
        trace_id: str | None = None,
    ) -> None:
        """Send a METRIC message to the Aether metrics-bridge fan-in.

        Args:
            name: Metric name (e.g. ``"ag2.response.chunks"``).
            qty: Additive delta.  Negative values require ``capability/metric_credit``.
            kind: Metric kind string (``"counter"``, ``"gauge"``, etc.).
            tags: Optional string key/value metadata attached as Metric.metadata.
            trace_id: Optional trace identifier forwarded as ``Metric.trace_id``.
        """
        if not _METRICS_AVAILABLE:
            return  # no-op degradation — ImportError path
        try:
            builder = _new_metric().add(name, kind=kind, qty=qty)
            if trace_id is not None:
                builder = builder.trace(trace_id)
            builder = builder.tag("agent", self._agent_name)
            if tags:
                for k, v in tags.items():
                    builder = builder.tag(k, v)
            metric = builder.build()
            await self._client.send_metric(metric)
        except Exception:
            logger.warning("AetherTelemetry: failed to emit metric %r", name, exc_info=True)
