#!/usr/bin/env python3
"""
Echo Orchestrator - receives task assignments and launches echo agents.

This orchestrator handles startup requests from the gateway and spawns
echo agent processes with appropriate configuration and authentication tokens.

The orchestrator supports the following profiles:
- "echo": Standard echo agent using subprocess
- "echo-alternative": Reserved for alternative implementations (threading, etc.)

Example:
    from scitrera_aether_client.echo_test.echo_orchestrator import EchoOrchestrator

    orchestrator = EchoOrchestrator(gateway="localhost:50051")
    orchestrator.run()

Environment Variables:
    AETHER_GATEWAY: Gateway address (default: localhost:50051)
"""

import json
import os
from pathlib import Path
from typing import List

from scitrera_aether_client.orchestrator import MultiprocessOrchestrator
from scitrera_aether_client.proto import aether_pb2


class EchoOrchestrator(MultiprocessOrchestrator):
    """
    Orchestrator that launches echo agents in response to task assignments.

    Extends MultiprocessOrchestrator to leverage subprocess management,
    output capture, process health monitoring, and graceful shutdown.

    This orchestrator handles the "echo" and "echo-alternative" profiles,
    spawning echo agent subprocesses that echo back received messages.

    Example:
        orchestrator = EchoOrchestrator(gateway="localhost:50051")
        orchestrator.run()
    """

    def get_implementation(self) -> str:
        """Return the orchestrator implementation identifier."""
        return "scitrera_aether_client.echo_test.echo_orchestrator.EchoOrchestrator"

    def get_supported_profiles(self) -> List[str]:
        """Return the list of profiles this orchestrator supports."""
        return ["echo", "echo-alternative"]

    def handle_assignment(self, assignment: aether_pb2.TaskAssignment) -> None:
        """
        Handle task assignment by launching an echo agent.

        The specifier for the agent is determined by (in order of preference):
        1. assignment.specifier - The specifier field from TaskAssignment
        2. launch_params['specifier'] - Specifier in launch params (legacy)
        3. assignment.task_id - Fallback to task ID if no specifier provided

        The auth_token can be provided in launch_params by the gateway, or
        this orchestrator will generate one if not present.

        Args:
            assignment: The task assignment from the gateway.
        """
        # Log additional assignment details
        self._log(f"  Assigned To: {assignment.assigned_to}")
        self._log(f"  Launch Params: {dict(assignment.launch_params)}")
        self._log(f"  Metadata: {dict(assignment.metadata)}")

        # Extract configuration from assignment
        launch_params = dict(assignment.launch_params)
        workspace = assignment.workspace or "default"
        implementation = assignment.target_implementation or "echo-agent"
        # Use specifier from assignment (preferred), then launch_params, then task_id as fallback
        specifier = (
            assignment.specifier
            or launch_params.get("specifier")
            or assignment.task_id
        )
        profile = assignment.profile

        # Extract auth token from launch_params (orchestrator generates one if not present)
        auth_token = launch_params.get("auth_token")

        if profile == "echo":
            self._launch_echo_agent(
                task_id=assignment.task_id,
                workspace=workspace,
                implementation=implementation,
                specifier=specifier,
                auth_token=auth_token,
            )
        elif profile == "echo-alternative":
            self._log("Profile 'echo-alternative' not yet implemented")
        else:
            self._log(f"Unknown profile: {profile}")

    def _launch_echo_agent(
        self,
        task_id: str,
        workspace: str,
        implementation: str,
        specifier: str,
        auth_token: str | None = None,
    ) -> None:
        """
        Launch an echo agent subprocess.

        Uses the parent class spawn_subprocess() for subprocess management,
        output capture, and process tracking.

        Args:
            task_id: Unique identifier for this task.
            workspace: Workspace for the agent.
            implementation: Implementation identifier.
            specifier: Unique specifier for the agent.
            auth_token: Optional authentication token (generated if None).
        """
        # Find the echo_agent.py script
        script_path = Path(__file__).parent / "echo_agent.py"

        self._log(
            f"Launching echo agent subprocess for task {task_id} "
            f"with implementation '{implementation}'"
        )

        # Use parent class spawn_subprocess for all the heavy lifting
        info = self.spawn_subprocess(
            task_id=task_id,
            script_path=script_path,
            workspace=workspace,
            implementation=implementation,
            specifier=specifier,
            auth_token=auth_token,
        )

        if info is None:
            self._log(f"ERROR: Failed to launch echo agent for task {task_id}")

    def on_message(self, msg: aether_pb2.IncomingMessage) -> None:
        """
        Handle incoming messages.

        Logs message details and attempts to parse JSON payloads.

        Args:
            msg: The incoming message from the gateway.
        """
        self._log(f"Message from {msg.source_topic}:")
        try:
            event = json.loads(msg.payload.decode())
            self._log(f"  Event type: {event.get('event_type')}")
            self._log(f"  Data: {event}")
        except Exception:
            self._log(f"  Raw: {msg.payload.decode()}")


def main():
    """Run the echo orchestrator."""
    gateway = os.environ.get("AETHER_GATEWAY", "localhost:50051")
    orchestrator = EchoOrchestrator(gateway=gateway)
    orchestrator.run()


if __name__ == "__main__":
    main()
