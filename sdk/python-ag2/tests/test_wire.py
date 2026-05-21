"""Wire envelope serialization tests — focus on backward compatibility."""

from __future__ import annotations

import json

import pytest
from autogen.agentchat.remote.protocol import RequestMessage

from scitrera_aether_ag2.wire import (
    SCHEMA_VERSION,
    RequestEnvelope,
    ResponseEnvelope,
)


def _make_request(**kw) -> RequestMessage:
    return RequestMessage(
        messages=kw.get("messages", [{"role": "user", "content": "hi", "name": "alice"}]),
        context=None,
        client_tools=[],
    )


class TestRequestEnvelopeConversationId:
    def test_conversation_id_defaults_to_none(self) -> None:
        env = RequestEnvelope(
            correlation_id="c1",
            reply_to="ag::w::i::s",
            request=_make_request(),
        )
        assert env.conversation_id is None

    def test_conversation_id_roundtrip(self) -> None:
        original = RequestEnvelope(
            correlation_id="c1",
            reply_to="ag::w::i::s",
            request=_make_request(),
            conversation_id="conv-42",
        )
        decoded = RequestEnvelope.decode(original.encode())
        assert decoded.conversation_id == "conv-42"

    def test_legacy_payload_without_conversation_id_decodes(self) -> None:
        """A payload from an older sender that omits conversation_id must still decode."""
        legacy_payload = {
            "schema_version": SCHEMA_VERSION,
            "correlation_id": "c1",
            "reply_to": "ag::w::i::s",
            "request": {
                "messages": [{"role": "user", "content": "hi", "name": "alice"}],
                "context": None,
                "client_tools": [],
            },
            # no conversation_id, no deadline_ms — like a v1.0 sender
        }
        decoded = RequestEnvelope.decode(json.dumps(legacy_payload).encode("utf-8"))
        assert decoded.conversation_id is None
        assert decoded.correlation_id == "c1"

    def test_schema_version_mismatch_raises(self) -> None:
        bad_payload = {
            "schema_version": SCHEMA_VERSION + 99,
            "correlation_id": "c1",
            "reply_to": "ag::w::i::s",
            "request": {
                "messages": [{"role": "user", "content": "hi", "name": "alice"}],
                "context": None,
                "client_tools": [],
            },
        }
        with pytest.raises(ValueError, match="schema_version"):
            RequestEnvelope.decode(json.dumps(bad_payload).encode("utf-8"))


class TestResponseEnvelopeUnaffected:
    """conversation_id is request-side only — ResponseEnvelope is unchanged."""

    def test_roundtrip(self) -> None:
        env = ResponseEnvelope(correlation_id="c1", sequence=0, done=True, response=None)
        decoded = ResponseEnvelope.decode(env.encode())
        assert decoded.correlation_id == "c1"
        assert decoded.done is True
