#!/usr/bin/env python3
"""
Async Echo Agent - connects to aether gateway and echoes received messages to stdout.

This async variant demonstrates usage of the AsyncAgentClient for applications
that need to integrate with asyncio event loops.

Environment Variables:
    AETHER_GATEWAY: Gateway address (default: localhost:50051)
    AETHER_WORKSPACE: Workspace to connect to (required)
    AETHER_IMPLEMENTATION: Agent implementation name (default: echo-agent)
    AETHER_SPECIFIER: Unique specifier for this agent instance (required)
    AETHER_AUTH_TOKEN: Authentication token from task metadata (optional)

Usage:
    # Direct invocation for testing:
    AETHER_WORKSPACE=default AETHER_SPECIFIER=echo-01 python echo_agent_async.py

    # Launched by orchestrator with full configuration:
    AETHER_GATEWAY=localhost:50051 \
    AETHER_WORKSPACE=default \
    AETHER_IMPLEMENTATION=echo-agent \
    AETHER_SPECIFIER=echo-instance-123 \
    AETHER_AUTH_TOKEN=secret-token \
    python echo_agent_async.py
"""

import argparse
import asyncio
import os
import signal
import sys
from datetime import datetime

from scitrera_aether_client import AsyncAgentClient
from scitrera_aether_client.exceptions import (
    AetherError,
    ConnectionError,
    ReconnectionError,
    AuthenticationError,
    DuplicateIdentityError,
    InvalidArgumentError,
)


def create_async_echo_agent(
        gateway: str,
        workspace: str,
        implementation: str,
        specifier: str,
        auth_token: str | None = None,
) -> AsyncAgentClient:
    """Create and configure an async echo agent client.

    Args:
        gateway: Gateway address (host:port)
        workspace: Workspace to connect to
        implementation: Agent implementation name
        specifier: Unique specifier for this agent instance
        auth_token: Optional authentication token

    Returns:
        Configured AsyncAgentClient instance (not yet connected)
    """
    credentials = {}
    if auth_token:
        credentials['token'] = auth_token

    client = AsyncAgentClient(
        workspace=workspace,
        implementation=implementation,
        specifier=specifier,
        credentials=credentials,
        auto_reconnect=True,
        max_retries=10,
        initial_backoff=1.0,
        max_backoff=30.0,
    )

    return client


def setup_async_handlers(client: AsyncAgentClient) -> None:
    """Set up async message handlers for the echo agent.

    All received messages are echoed to stdout with timestamps and metadata.
    Handlers can be either sync or async functions with AsyncAgentClient.
    """

    async def on_message(msg):
        """Echo incoming messages to stdout (async handler)."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        source = msg.source_topic
        try:
            payload = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            payload = f"<binary data: {len(msg.payload)} bytes>"

        print(f"[{timestamp}] MESSAGE from {source}:")
        print(f"  Payload: {payload}")
        print(f"  Msg: {msg}")
        print(flush=True)

    async def on_task_assignment(assignment):
        """Log task assignments received by this agent (async handler)."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] TASK ASSIGNMENT:")
        print(f"  Task ID: {assignment.task_id}")
        print(f"  Task Type: {assignment.task_type}")
        print(f"  Assigned To: {assignment.assigned_to}")
        print(f"  Metadata: {dict(assignment.metadata)}")
        print(flush=True)

    async def on_config(config):
        """Log configuration snapshots (async handler)."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] CONFIG SNAPSHOT:")
        print(f"  Config: {config}")
        print(flush=True)

    async def on_connect():
        """Log successful connection (async handler)."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] CONNECTED to gateway")
        print(flush=True)

    async def on_disconnect(reason):
        """Log disconnection (async handler)."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] DISCONNECTED: {reason}")
        print(flush=True)

    async def on_error(error):
        """Log errors (async handler)."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] ERROR: code={error.code}, message={error.message}",
              file=sys.stderr)
        print(flush=True)

    client.on_message = on_message
    client.on_task_assignment = on_task_assignment
    client.on_config = on_config
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_error = on_error


async def run_async_echo_agent(
        gateway: str = "localhost:50051",
        workspace: str | None = None,
        implementation: str = "echo-agent",
        specifier: str | None = None,
        auth_token: str | None = None,
) -> None:
    """Run the async echo agent until interrupted.

    Args:
        gateway: Gateway address (host:port)
        workspace: Workspace to connect to
        implementation: Agent implementation name
        specifier: Unique specifier for this agent instance
        auth_token: Optional authentication token

    Raises:
        InvalidArgumentError: If workspace or specifier is not provided.
        AuthenticationError: If authentication fails.
        DuplicateIdentityError: If another agent with same identity is connected.
        ConnectionError: If connection to gateway fails.
        ReconnectionError: If reconnection attempts are exhausted.
    """
    if not workspace:
        raise InvalidArgumentError(
            message="workspace is required",
            argument="workspace"
        )
    if not specifier:
        raise InvalidArgumentError(
            message="specifier is required",
            argument="specifier"
        )

    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        print(f"\n[{datetime.now().isoformat(timespec='milliseconds')}] "
              f"Received signal {signum}, shutting down...")
        shutdown_event.set()

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
          f"Async Echo Agent starting...")
    print(f"  Gateway: {gateway}")
    print(f"  Workspace: {workspace}")
    print(f"  Implementation: {implementation}")
    print(f"  Specifier: {specifier}")
    print(f"  Auth Token: {'<provided>' if auth_token else '<none>'}")
    print(flush=True)

    # Use async context manager for automatic cleanup
    async with create_async_echo_agent(
            gateway=gateway,
            workspace=workspace,
            implementation=implementation,
            specifier=specifier,
            auth_token=auth_token,
    ) as client:
        setup_async_handlers(client)

        try:
            await client.connect(gateway)

            # Wait for shutdown signal or disconnection
            # Create tasks for both waiting conditions
            shutdown_task = asyncio.create_task(shutdown_event.wait())
            disconnect_task = asyncio.create_task(client.wait_until_disconnected())

            # Wait for either shutdown signal or disconnection
            done, pending = await asyncio.wait(
                [shutdown_task, disconnect_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    # Expected: task was cancelled, ignore the exception.
                    pass

        except AuthenticationError as e:
            print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
                  f"AUTHENTICATION ERROR: {e}", file=sys.stderr)
            raise
        except DuplicateIdentityError as e:
            print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
                  f"DUPLICATE IDENTITY: Another agent with this identity is already connected: {e}",
                  file=sys.stderr)
            raise
        except (ConnectionError, ReconnectionError) as e:
            print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
                  f"CONNECTION ERROR: {e}", file=sys.stderr)
            raise
        except AetherError as e:
            print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
                  f"AETHER ERROR: {e}", file=sys.stderr)
            raise
        except Exception as e:
            print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
                  f"UNEXPECTED ERROR: {e}", file=sys.stderr)
            raise
        finally:
            print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
                  f"Async Echo Agent stopped.")


def main():
    """Main entry point - reads configuration from environment and args."""
    parser = argparse.ArgumentParser(
        description="Async Echo Agent - echoes received messages to stdout"
    )
    parser.add_argument(
        "--gateway",
        default=os.environ.get("AETHER_GATEWAY", "localhost:50051"),
        help="Gateway address (default: localhost:50051)"
    )
    parser.add_argument(
        "--workspace",
        default=os.environ.get("AETHER_WORKSPACE"),
        help="Workspace to connect to (required)"
    )
    parser.add_argument(
        "--implementation",
        default=os.environ.get("AETHER_IMPLEMENTATION", "echo-agent"),
        help="Agent implementation name (default: echo-agent)"
    )
    parser.add_argument(
        "--specifier",
        default=os.environ.get("AETHER_SPECIFIER"),
        help="Unique specifier for this agent instance (required)"
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("AETHER_AUTH_TOKEN"),
        help="Authentication token"
    )

    args = parser.parse_args()

    if not args.workspace:
        parser.error("--workspace or AETHER_WORKSPACE is required")
    if not args.specifier:
        parser.error("--specifier or AETHER_SPECIFIER is required")

    asyncio.run(run_async_echo_agent(
        gateway=args.gateway,
        workspace=args.workspace,
        implementation=args.implementation,
        specifier=args.specifier,
        auth_token=args.token,
    ))


if __name__ == "__main__":
    main()
