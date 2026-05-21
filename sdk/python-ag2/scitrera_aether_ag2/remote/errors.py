"""Typed errors raised by AetherRemoteAgent."""

from __future__ import annotations


class RemoteAgentError(RuntimeError):
    """Base error for remote ag2 agent invocations."""


class HITLRequired(RemoteAgentError):
    """Raised when the remote agent requested human input and no auto-handler is configured."""

    def __init__(self, prompt: str, *, correlation_id: str):
        super().__init__(prompt)
        self.prompt = prompt
        self.correlation_id = correlation_id
