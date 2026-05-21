"""Mainstream ag2 integration: host (server-side) + proxy (client-side) over Aether."""

from .errors import HITLRequired, RemoteAgentError
from .host import AetherAgentHost
from .proxy import AetherRemoteAgent
from .streams import AetherTransport

__all__ = [
    "AetherAgentHost",
    "AetherRemoteAgent",
    "AetherTransport",
    "HITLRequired",
    "RemoteAgentError",
]
