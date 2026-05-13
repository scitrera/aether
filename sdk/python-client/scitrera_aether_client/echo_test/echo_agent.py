#!/usr/bin/env python3
"""
Echo Agent - connects to aether gateway and echoes received messages to stdout.

This agent is designed to be launched by an orchestrator (echo_orchestrator.py).
It uses token-based authentication where the token is passed via environment
variables from the orchestrator's task assignment metadata.

Environment Variables:
    AETHER_GATEWAY: Gateway address (default: localhost:50051)
    AETHER_WORKSPACE: Workspace to connect to (required)
    AETHER_IMPLEMENTATION: Agent implementation name (default: echo-agent)
    AETHER_SPECIFIER: Unique specifier for this agent instance (required)
    AETHER_AUTH_TOKEN: Authentication token from task metadata (optional)

Usage:
    # Direct invocation for testing:
    AETHER_WORKSPACE=default AETHER_SPECIFIER=echo-01 python echo_agent.py

    # Launched by orchestrator with full configuration:
    AETHER_GATEWAY=localhost:50051 \
    AETHER_WORKSPACE=default \
    AETHER_IMPLEMENTATION=echo-agent \
    AETHER_SPECIFIER=echo-instance-123 \
    AETHER_AUTH_TOKEN=secret-token \
    python echo_agent.py
"""

import argparse
import os
import signal
import sys
import threading
from datetime import datetime

from scitrera_aether_client import AgentClient
from scitrera_aether_client.exceptions import (
    AetherError,
    ConnectionError,
    ReconnectionError,
    AuthenticationError,
    DuplicateIdentityError,
    InvalidArgumentError,
)


def create_echo_agent(
        gateway: str,
        workspace: str,
        implementation: str,
        specifier: str,
        auth_token: str | None = None,
) -> AgentClient:
    """Create and configure an echo agent client.

    Args:
        gateway: Gateway address (host:port)
        workspace: Workspace to connect to
        implementation: Agent implementation name
        specifier: Unique specifier for this agent instance
        auth_token: Optional authentication token

    Returns:
        Configured AgentClient instance (not yet connected)
    """
    credentials = {}
    if auth_token:
        credentials['token'] = auth_token

    client = AgentClient(
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


def setup_handlers(client: AgentClient) -> None:
    """Set up message handlers for the echo agent.

    All received messages are echoed to stdout with timestamps and metadata.
    """

    def on_message(msg):
        """Echo incoming messages to stdout."""
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

    def on_task_assignment(assignment):
        """Log task assignments received by this agent."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] TASK ASSIGNMENT:")
        print(f"  Task ID: {assignment.task_id}")
        print(f"  Task Type: {assignment.task_type}")
        print(f"  Assigned To: {assignment.assigned_to}")
        print(f"  Metadata: {dict(assignment.metadata)}")
        print(flush=True)

    def on_config(config):
        """Log configuration snapshots."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] CONFIG SNAPSHOT:")
        print(f'{"  Config: {config}"}')
        # print(f"  Workspace KV entries: {len(config.workspace_kv)}")
        # print(f"  Global KV entries: {len(config.global_kv)}")
        print(flush=True)

    def on_connect():
        """Log successful connection."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] CONNECTED to gateway")
        print(flush=True)

    def on_disconnect(reason):
        """Log disconnection."""
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        print(f"[{timestamp}] DISCONNECTED: {reason}")
        print(flush=True)

    def on_error(error):
        """Log errors."""
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


def run_echo_agent(
        gateway: str = "localhost:50051",
        workspace: str | None = None,
        implementation: str = "echo-agent",
        specifier: str | None = None,
        auth_token: str | None = None,
) -> None:
    """Run the echo agent until interrupted.

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

    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        print(f"\n[{datetime.now().isoformat(timespec='milliseconds')}] "
              f"Received signal {signum}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"[{datetime.now().isoformat(timespec='milliseconds')}] "
          f"Echo Agent starting...")
    print(f"  Gateway: {gateway}")
    print(f"  Workspace: {workspace}")
    print(f"  Implementation: {implementation}")
    print(f"  Specifier: {specifier}")
    print(f"  Auth Token: {'<provided>' if auth_token else '<none>'}")
    print(flush=True)

    # Use context manager for automatic cleanup
    with create_echo_agent(
            gateway=gateway,
            workspace=workspace,
            implementation=implementation,
            specifier=specifier,
            auth_token=auth_token,
    ) as client:
        setup_handlers(client)

        try:
            client.connect(gateway)

            # Wait for shutdown signal
            while not shutdown_event.is_set():
                shutdown_event.wait(timeout=1.0)

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
                  f"Echo Agent stopped.")


def main():
    """Main entry point - reads configuration from environment and args."""
    parser = argparse.ArgumentParser(
        description="Echo Agent - echoes received messages to stdout"
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

    run_echo_agent(
        gateway=args.gateway,
        workspace=args.workspace,
        implementation=args.implementation,
        specifier=args.specifier,
        auth_token=args.token,
    )


if __name__ == "__main__":
    main()
