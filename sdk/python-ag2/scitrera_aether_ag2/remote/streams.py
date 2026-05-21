"""AetherTransport — wraps AsyncAgentClient with correlation routing."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from scitrera_aether_client import AsyncAgentClient
from scitrera_aether_client.proto import aether_pb2

from ..identity import AetherIdentity
from ..wire import RequestEnvelope, ResponseEnvelope

logger = logging.getLogger(__name__)

RequestHandler = Callable[[RequestEnvelope], Awaitable[None]]

RESPONSE_QUEUE_MAXSIZE = 64


class AetherTransport:
    """Per-process async transport bound to a single AsyncAgentClient.

    v1 constraint: one identity per transport (either a caller OR a host, not both).
    """

    def __init__(self, client: AsyncAgentClient, local_identity: AetherIdentity) -> None:
        self.client = client
        self.local_identity = local_identity
        self._loop: asyncio.AbstractEventLoop | None = None
        self._response_queues: dict[str, asyncio.Queue[ResponseEnvelope]] = {}
        self._host_handlers: dict[str, RequestHandler] = {}
        self._connected = asyncio.Event()
        client.on_message = self._on_message  # type: ignore[assignment]
        client.on_connect = self._on_connect  # type: ignore[assignment]

    async def _on_connect(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._connected.set()

    async def _on_message(self, msg: Any) -> None:
        try:
            if msg.message_type == aether_pb2.TOOL_CALL:
                envelope = RequestEnvelope.decode(msg.payload)
                handler = self._host_handlers.get(self.local_identity.to_topic())
                if handler is None:
                    logger.warning("no host handler registered for %s", self.local_identity.to_topic())
                    return
                await handler(envelope)
            elif msg.message_type == aether_pb2.CHAT:
                try:
                    envelope = ResponseEnvelope.decode(msg.payload)
                except Exception as decode_exc:
                    await self._surface_decode_error(msg.payload, decode_exc)
                    return
                queue = self._response_queues.get(envelope.correlation_id)
                if queue is None:
                    logger.warning("no response queue for correlation_id=%s", envelope.correlation_id)
                    return
                await queue.put(envelope)
            else:
                logger.debug("ignoring message_type=%s", msg.message_type)
        except Exception:  # noqa: BLE001
            logger.exception("transport on_message dispatch failed")

    async def _surface_decode_error(self, payload: bytes, exc: Exception) -> None:
        """Best-effort: extract correlation_id from a malformed response and notify its requester."""
        cid: str | None = None
        try:
            raw = json.loads(payload.decode("utf-8", errors="replace"))
            if isinstance(raw, dict):
                cid = raw.get("correlation_id")
        except Exception:  # noqa: BLE001
            pass
        if cid is None:
            logger.exception("response decode failed and no correlation_id recoverable: %s", exc)
            return
        queue = self._response_queues.get(cid)
        if queue is None:
            logger.warning("decode error for correlation_id=%s but no waiter: %s", cid, exc)
            return
        synthetic = ResponseEnvelope(
            correlation_id=cid,
            sequence=-1,
            done=True,
            error={"code": "decode_error", "message": str(exc), "retryable": False},
        )
        await queue.put(synthetic)

    def register_host(self, topic: str, handler: RequestHandler) -> None:
        if topic in self._host_handlers:
            raise RuntimeError(f"host already registered for {topic}")
        self._host_handlers[topic] = handler

    async def wait_connected(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self._connected.wait(), timeout)

    async def submit_request(
        self, target: AetherIdentity, envelope: RequestEnvelope
    ) -> AsyncIterator[ResponseEnvelope]:
        """Send envelope to target topic, async-iterate response envelopes until done."""
        queue: asyncio.Queue[ResponseEnvelope] = asyncio.Queue(maxsize=RESPONSE_QUEUE_MAXSIZE)
        self._response_queues[envelope.correlation_id] = queue
        try:
            await self.client.send_message_to_agent(
                workspace=target.workspace,
                implementation=target.implementation,
                specifier=target.specifier,
                payload=envelope.encode(),
                message_type=aether_pb2.TOOL_CALL,
            )
            while True:
                item = await queue.get()
                yield item
                if item.done:
                    return
        finally:
            self._response_queues.pop(envelope.correlation_id, None)

    async def send_response(self, reply_to_topic: str, envelope: ResponseEnvelope) -> None:
        target = AetherIdentity.from_topic(reply_to_topic)
        await self.client.send_message_to_agent(
            workspace=target.workspace,
            implementation=target.implementation,
            specifier=target.specifier,
            payload=envelope.encode(),
            message_type=aether_pb2.CHAT,
        )
