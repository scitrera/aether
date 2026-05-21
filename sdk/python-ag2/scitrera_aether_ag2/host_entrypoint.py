"""Entry point for orchestrator-spawned ag2 hosts.

Run via::

    python -m scitrera_aether_ag2.host_entrypoint

Environment variables (all provided by AetherAg2Orchestrator):

- ``AETHER_AG2_FACTORY`` (**required**) — ``pkg.module:callable_name`` that
  returns a ``ConversableAgent`` when called with a single ``dict`` argument.
- ``AETHER_AG2_CONFIG`` — JSON-encoded ``dict`` passed to the factory
  (default: ``{}``).
- ``AETHER_AG2_WORKSPACE`` — Aether workspace (default: ``"default"``).
- ``AETHER_AG2_IMPL`` — Agent implementation identifier (default: ``"agent"``).
- ``AETHER_AG2_SPEC`` — Agent specifier (default: ``"default"``).
- ``AETHER_GATEWAY_ENDPOINT`` — Gateway address (default: ``"localhost:50051"``).
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
from typing import Any

from .identity import AetherIdentity
from .remote.host import AetherAgentHost


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_factory(factory_str: str) -> Any:
    """Import and return the callable described by ``module:callable`` string.

    Args:
        factory_str: A string of the form ``"pkg.module:callable_name"``.

    Returns:
        The resolved callable.

    Raises:
        SystemExit: If the string is malformed or the import fails.
    """
    if ":" not in factory_str:
        raise SystemExit(
            f"AETHER_AG2_FACTORY must be in 'module:callable' format, got: {factory_str!r}"
        )
    module_path, callable_name = factory_str.rsplit(":", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise SystemExit(f"Cannot import factory module {module_path!r}: {exc}") from exc
    if not hasattr(module, callable_name):
        raise SystemExit(
            f"Module {module_path!r} has no attribute {callable_name!r}"
        )
    return getattr(module, callable_name)


def build_host_from_env(env: dict) -> AetherAgentHost:
    """Construct an :class:`AetherAgentHost` from an environment dict.

    This is the testable helper extracted from :func:`main`.  It reads all
    required context from *env* (mirrors ``os.environ``), resolves the factory,
    calls it to obtain an agent, builds the identity, and returns a ready-to-
    serve host.  It does **not** call ``serve()``.

    Args:
        env: Mapping of environment variable names to string values.

    Returns:
        A configured :class:`AetherAgentHost`.

    Raises:
        SystemExit: If any required variable is missing or the factory fails.
    """
    factory_str = env.get("AETHER_AG2_FACTORY")
    if not factory_str:
        raise SystemExit(
            "Required environment variable AETHER_AG2_FACTORY is not set. "
            "It must be a 'module:callable' string that returns a ConversableAgent."
        )

    config_raw = env.get("AETHER_AG2_CONFIG", "{}")
    try:
        config: dict = json.loads(config_raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(
            f"AETHER_AG2_CONFIG is not valid JSON: {exc}"
        ) from exc

    workspace = env.get("AETHER_AG2_WORKSPACE", "default")
    implementation = env.get("AETHER_AG2_IMPL", "agent")
    specifier = env.get("AETHER_AG2_SPEC", "default")
    endpoint = env.get("AETHER_GATEWAY_ENDPOINT", "localhost:50051")

    factory = _resolve_factory(factory_str)
    try:
        agent = factory(config)
    except Exception as exc:
        raise SystemExit(
            f"Factory {factory_str!r} raised an error: {exc}"
        ) from exc

    identity = AetherIdentity(
        workspace=workspace,
        implementation=implementation,
        specifier=specifier,
    )
    return AetherAgentHost(agent=agent, identity=identity, endpoint=endpoint)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Boot an :class:`AetherAgentHost` from environment variables and serve."""
    host = build_host_from_env(dict(os.environ))
    asyncio.run(host.serve())


if __name__ == "__main__":
    main()
