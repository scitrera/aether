"""AetherAgentHost — wraps a ConversableAgent and serves it over Aether."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict

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
                await self._telemetry.on_request_received(envelope.correlation_id, envelope.request)
            except Exception:  # noqa: BLE001
                logger.exception("telemetry.on_request_received failed; continuing")
        service = AgentService(self.agent)
        sequence = 0
        try:
            async for service_response in service(envelope.request):
                if self._telemetry is not None:
                    try:
                        await self._telemetry.on_response_chunk(envelope.correlation_id, service_response)
                    except Exception:  # noqa: BLE001
                        logger.exception("telemetry.on_response_chunk failed; continuing")
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
            if self._checkpointer is not None:
                try:
                    await self._checkpointer.save_history(self.agent)
                except Exception:  # noqa: BLE001
                    logger.exception("checkpoint save_history failed; continuing")
            if self._telemetry is not None:
                try:
                    await self._telemetry.on_request_completed(envelope.correlation_id, sequence)
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
        except Exception as exc:  # noqa: BLE001
            logger.exception("agent service failed for correlation_id=%s", envelope.correlation_id)
            if self._telemetry is not None:
                try:
                    await self._telemetry.on_request_failed(
                        envelope.correlation_id,
                        {"error_code": "internal", "retryable": False},
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
