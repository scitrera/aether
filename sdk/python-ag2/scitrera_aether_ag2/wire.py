"""Wire envelopes for ag2 ↔ Aether messages (correlation, streaming, errors)."""

from __future__ import annotations

from typing import Any

from autogen.agentchat.remote.protocol import RequestMessage, ServiceResponse
from pydantic import BaseModel


SCHEMA_VERSION = 1


class RequestEnvelope(BaseModel):
    """ag2 RequestMessage wrapped with correlation + reply-to routing.

    ``conversation_id`` groups all continuation requests that make up a single
    logical caller turn (tool-call continuations, HITL injections, etc.).
    Older peers that don't set it still decode cleanly — the field is optional
    and defaults to ``None`` to keep the wire format backward compatible.
    """

    schema_version: int = SCHEMA_VERSION
    correlation_id: str
    reply_to: str
    request: RequestMessage
    deadline_ms: int | None = None
    conversation_id: str | None = None

    def encode(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def decode(cls, payload: bytes) -> "RequestEnvelope":
        env = cls.model_validate_json(payload.decode("utf-8"))
        if env.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported RequestEnvelope schema_version={env.schema_version}; expected {SCHEMA_VERSION}"
            )
        return env


class ResponseEnvelope(BaseModel):
    """One frame of a streamed ag2 ServiceResponse, keyed by correlation_id."""

    schema_version: int = SCHEMA_VERSION
    correlation_id: str
    sequence: int
    done: bool
    response: ServiceResponse | None = None
    error: dict[str, Any] | None = None

    def encode(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def decode(cls, payload: bytes) -> "ResponseEnvelope":
        env = cls.model_validate_json(payload.decode("utf-8"))
        if env.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported ResponseEnvelope schema_version={env.schema_version}; expected {SCHEMA_VERSION}"
            )
        return env
