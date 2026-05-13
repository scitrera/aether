"""
Echo test module for Aether client.

This module provides example echo agents for testing and demonstration purposes.
Both synchronous and asynchronous variants are available.
"""

from .echo_agent import (
    create_echo_agent,
    setup_handlers,
    run_echo_agent,
)

from .echo_agent_async import (
    create_async_echo_agent,
    setup_async_handlers,
    run_async_echo_agent,
)

__all__ = [
    # Sync echo agent
    "create_echo_agent",
    "setup_handlers",
    "run_echo_agent",
    # Async echo agent
    "create_async_echo_agent",
    "setup_async_handlers",
    "run_async_echo_agent",
]
