"""Map ag2 agent names to Aether workspaces/identities; ACL helper.

ACL note: tenant isolation comes free because the Aether gateway already blocks
cross-workspace sends server-side (server/pkg/models/identity.go). No ACL code
is needed in this module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from ..identity import AetherIdentity


@dataclass(frozen=True)
class WorkspaceResolverConfig:
    """Configuration for mapping ag2 agent names to Aether identities."""

    default_workspace: str = "default"
    implementation_suffix: str | None = None  # appended to agent.name to form implementation
    specifier: str = "default"


def resolve_identity(
    agent_name: str,
    config: WorkspaceResolverConfig | None = None,
) -> AetherIdentity:
    """Map an ag2 agent name to an AetherIdentity.

    Default: workspace=``default``, implementation=agent_name, specifier=``default``.
    """
    cfg = config or WorkspaceResolverConfig()
    impl = agent_name
    if cfg.implementation_suffix:
        impl = f"{agent_name}{cfg.implementation_suffix}"
    return AetherIdentity(
        workspace=cfg.default_workspace,
        implementation=impl,
        specifier=cfg.specifier,
    )


def resolve_identity_from_env(
    agent_name: str,
    env: dict[str, str] | None = None,
) -> AetherIdentity:
    """Resolve an AetherIdentity reading env vars, falling back to os.environ.

    Environment variables (all optional):
      AETHER_AG2_WORKSPACE — workspace (default: "default")
      AETHER_AG2_IMPL      — implementation override (default: agent_name)
      AETHER_AG2_SPEC      — specifier (default: "default")

    The env variant is used by orchestrator-spawned subprocesses to discover
    their identity at startup without hard-coded configuration.
    """
    source = env if env is not None else os.environ
    workspace = source.get("AETHER_AG2_WORKSPACE", "default")
    implementation = source.get("AETHER_AG2_IMPL", agent_name)
    specifier = source.get("AETHER_AG2_SPEC", "default")
    return AetherIdentity(
        workspace=workspace,
        implementation=implementation,
        specifier=specifier,
    )
