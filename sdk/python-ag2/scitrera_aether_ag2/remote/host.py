"""AetherAgentHost — wraps a ConversableAgent and serves it over Aether."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from typing import Any

from autogen.agentchat import ConversableAgent
from autogen.agentchat.remote.agent_service import AgentService
from scitrera_aether_client import AsyncAgentClient

from ..features.checkpoint import AgentCheckpointer
from ..features.telemetry import AetherTelemetry
from ..identity import AetherIdentity
from ..wire import RequestEnvelope, ResponseEnvelope
from .streams import AetherTransport

logger = logging.getLogger(__name__)

DEDUPE_WINDOW = 128


class AetherAgentHost:
    """Hosts a ConversableAgent on Aether, dispatches incoming requests to AgentService."""

    def __init__(
        self,
        agent: ConversableAgent,
        identity: AetherIdentity,
        endpoint: str,
        *,
        checkpointer: AgentCheckpointer | None = None,
        telemetry: AetherTelemetry | None = None,
        enable_checkpoints: bool = True,
        enable_telemetry: bool = True,
    ) -> None:
        self.agent = agent
        self.identity = identity
        self.endpoint = endpoint
        self._client: AsyncAgentClient | None = None
        self._transport: AetherTransport | None = None
        self._stopped = asyncio.Event()
        self._seen_correlations: OrderedDict[str, None] = OrderedDict()
        self._explicit_checkpointer = checkpointer
        self._explicit_telemetry = telemetry
        self._enable_checkpoints = enable_checkpoints
        self._enable_telemetry = enable_telemetry
        self._checkpointer: AgentCheckpointer | None = None
        self._telemetry: AetherTelemetry | None = None

    async def serve(self) -> None:
        self._client = AsyncAgentClient(
            workspace=self.identity.workspace,
            implementation=self.identity.implementation,
            specifier=self.identity.specifier,
        )
        self._transport = AetherTransport(self._client, self.identity)
        self._transport.register_host(self.identity.to_topic(), self._handle_request)
        await self._client.connect(self.endpoint)
        await self._transport.wait_connected()
        logger.info("AetherAgentHost connected as %s", self.identity.to_topic())

        ckpt_name = self.identity.specifier or self.agent.name
        if self._explicit_checkpointer is not None:
            self._checkpointer = self._explicit_checkpointer
        elif self._enable_checkpoints:
            self._checkpointer = AgentCheckpointer(self._client, ckpt_name)

        if self._explicit_telemetry is not None:
            self._telemetry = self._explicit_telemetry
        elif self._enable_telemetry:
            self._telemetry = AetherTelemetry(self._client, ckpt_name)

        if self._checkpointer is not None:
            try:
                await self._checkpointer.load_history(self.agent)
            except Exception:  # noqa: BLE001
                logger.exception("checkpoint load_history failed; continuing without restored history")

        try:
            done, _ = await asyncio.wait(
                [
                    asyncio.create_task(self._client.wait_until_disconnected()),
                    asyncio.create_task(self._stopped.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in done:
                t.result()
        finally:
            await self._client.close()

    async def stop(self) -> None:
        self._stopped.set()
        if self._client is not None:
            await self._client.close()

    async def _handle_request(self, envelope: RequestEnvelope) -> None:
        assert self._transport is not None
        if envelope.correlation_id in self._seen_correlations:
            logger.warning("dropping duplicate correlation_id=%s", envelope.correlation_id)
            return
        self._seen_correlations[envelope.correlation_id] = None
        if len(self._seen_correlations) > DEDUPE_WINDOW:
            self._seen_correlations.popitem(last=False)
        if self._telemetry is not None:
            try:
                await self._telemetry.on_request_received(
                    envelope.correlation_id,
                    envelope.request,
                    conversation_id=envelope.conversation_id,
                )
            except Exception:  # noqa: BLE001
                logger.exception("telemetry.on_request_received failed; continuing")

        service = AgentService(self.agent)
        sequence = 0
        final_assistant: dict[str, Any] | None = None
        deadline_s = envelope.deadline_ms / 1000.0 if envelope.deadline_ms else None

        async def _stream() -> None:
            nonlocal sequence, final_assistant
            assert self._transport is not None
            async for service_response in service(envelope.request):
                if self._telemetry is not None:
                    try:
                        await self._telemetry.on_response_chunk(
                            envelope.correlation_id,
                            service_response,
                            conversation_id=envelope.conversation_id,
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception("telemetry.on_response_chunk failed; continuing")
                if service_response.message is not None:
                    final_assistant = service_response.message
                await self._transport.send_response(
                    envelope.reply_to,
                    ResponseEnvelope(
                        correlation_id=envelope.correlation_id,
                        sequence=sequence,
                        done=False,
                        response=service_response,
                    ),
                )
                sequence += 1

        try:
            if deadline_s is not None:
                async with asyncio.timeout(deadline_s):
                    await _stream()
            else:
                await _stream()
            self._mirror_into_oai_messages(envelope, final_assistant)
            if self._checkpointer is not None:
                try:
                    await self._checkpointer.save_history(self.agent)
                except Exception:  # noqa: BLE001
                    logger.exception("checkpoint save_history failed; continuing")
            if self._telemetry is not None:
                try:
                    await self._telemetry.on_request_completed(
                        envelope.correlation_id,
                        sequence,
                        conversation_id=envelope.conversation_id,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("telemetry.on_request_completed failed; continuing")
            await self._transport.send_response(
                envelope.reply_to,
                ResponseEnvelope(
                    correlation_id=envelope.correlation_id,
                    sequence=sequence,
                    done=True,
                    response=None,
                ),
            )
        except asyncio.TimeoutError:
            logger.warning(
                "deadline_exceeded for correlation_id=%s deadline_ms=%s",
                envelope.correlation_id,
                envelope.deadline_ms,
            )
            if self._telemetry is not None:
                try:
                    await self._telemetry.on_request_failed(
                        envelope.correlation_id,
                        {"error_code": "deadline_exceeded", "retryable": False},
                        conversation_id=envelope.conversation_id,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("telemetry.on_request_failed failed; continuing")
            await self._transport.send_response(
                envelope.reply_to,
                ResponseEnvelope(
                    correlation_id=envelope.correlation_id,
                    sequence=sequence,
                    done=True,
                    error={
                        "code": "deadline_exceeded",
                        "message": f"deadline_ms={envelope.deadline_ms} exceeded",
                        "retryable": False,
                    },
                ),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("agent service failed for correlation_id=%s", envelope.correlation_id)
            if self._telemetry is not None:
                try:
                    await self._telemetry.on_request_failed(
                        envelope.correlation_id,
                        {"error_code": "internal", "retryable": False},
                        conversation_id=envelope.conversation_id,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("telemetry.on_request_failed failed; continuing")
            await self._transport.send_response(
                envelope.reply_to,
                ResponseEnvelope(
                    correlation_id=envelope.correlation_id,
                    sequence=sequence,
                    done=True,
                    error={"code": "internal", "message": str(exc), "retryable": False},
                ),
            )

    def _mirror_into_oai_messages(
        self,
        envelope: RequestEnvelope,
        final_assistant: dict[str, Any] | None,
    ) -> None:
        """Mirror the request/response transcript into agent._oai_messages so save_history persists it.

        ag2's AgentService threads conversation state through RequestMessage.messages and never
        populates _oai_messages itself. Without this mirror, a checkpoint save right after a turn
        would persist an empty history.
        """
        peer_name = self._derive_peer_name(envelope)
        if peer_name is None:
            return
        mirrored: list[dict[str, Any]] = [dict(m) for m in envelope.request.messages]
        if final_assistant is not None:
            mirrored.append(dict(final_assistant))
        try:
            self.agent._oai_messages[peer_name] = mirrored  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            logger.exception("failed to mirror messages into _oai_messages for peer=%r", peer_name)

    @staticmethod
    def _derive_peer_name(envelope: RequestEnvelope) -> str | None:
        for m in reversed(envelope.request.messages):
            if m.get("role") == "user":
                name = m.get("name")
                if name:
                    return str(name)
                break
        try:
            return AetherIdentity.from_topic(envelope.reply_to).specifier
        except ValueError:
            return None
