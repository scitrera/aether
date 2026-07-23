"""ag2 ↔ Aether adapter — public API re-exports."""

__version__ = "0.0.2"

from .identity import AetherIdentity
from .remote import (
    AetherAgentHost,
    AetherRemoteAgent,
    AetherTransport,
    HITLRequired,
    RemoteAgentError,
)
from .wire import RequestEnvelope, ResponseEnvelope

__all__ = [
    "AetherIdentity",
    "AetherAgentHost",
    "AetherRemoteAgent",
    "AetherTransport",
    "HITLRequired",
    "RemoteAgentError",
    "RequestEnvelope",
    "ResponseEnvelope",
]
