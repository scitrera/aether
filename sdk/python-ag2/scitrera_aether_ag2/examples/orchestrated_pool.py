"""Demonstrate AetherAg2Orchestrator spawning a host subprocess on demand.

Spawns its own aetherlite gateway.

Depends on ``scitrera_aether_ag2.examples._aetherlite`` for the spawn/terminate
boilerplate shared with the other example scripts in this package.

NOTE: Full end-to-end orchestration (message -> offline agent -> orchestrator
TaskAssignment -> subprocess spawn -> message delivered) requires the gateway's
dispatcher to be wired up, which in aetherlite's lite-mode requires PostgreSQL
for the task store.  Because the aetherlite binary used in examples/tests ships
without PostgreSQL, we demonstrate the shape of the developer experience by
directly calling orchestrator.handle_assignment() with a hand-built assignment
object.  See tech-debt entry "orchestrated_pool: hand-built assignment" for the
full end-to-end resolution path.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
import types
from pathlib import Path
from typing import Any

from autogen.agentchat import ConversableAgent

from scitrera_aether_ag2.examples._aetherlite import (
    READY_TIMEOUT_S,
    ensure_binary_executable,
    spawn_aetherlite,
    terminate_aetherlite,
    wait_for_tcp,
)
from scitrera_aether_ag2.orchestrator import AetherAg2Orchestrator

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level factory (referenced by launch_params["factory"])
# ---------------------------------------------------------------------------

def make_echo_agent(config: dict) -> ConversableAgent:
    """Factory invoked by AetherAg2Orchestrator when it spawns a host subprocess.

    Receives the assignment metadata dict and returns a ConversableAgent.
    The factory string passed in launch_params is:
        "scitrera_aether_ag2.examples.orchestrated_pool:make_echo_agent"
    """
    name = config.get("agent_name", "echo-agent")

    class _EchoAgent(ConversableAgent):
        def __init__(self) -> None:
            super().__init__(
                name=name,
                llm_config=False,
                human_input_mode="NEVER",
                code_execution_config=False,
            )

        async def a_generate_oai_reply(
            self,
            messages: list[dict[str, Any]] | None = None,
            sender: Any | None = None,
            tools: list[dict[str, Any]] | None = None,
            config: Any | None = None,
        ) -> tuple[bool, str]:
            last = (messages or [{}])[-1]
            return True, f"echo: {last.get('content', '')}"

    return _EchoAgent()


# ---------------------------------------------------------------------------
# Fake assignment (simulates what the gateway would send via TaskAssignment)
# ---------------------------------------------------------------------------

def _make_fake_assignment(endpoint: str) -> Any:
    """Build a SimpleNamespace that mirrors the TaskAssignment proto shape.

    Used because full gateway orchestration requires PostgreSQL task-store
    support that is not available in the lite binary.  The shape here matches
    what AetherAg2Orchestrator.handle_assignment() reads.
    """
    return types.SimpleNamespace(
        task_id="fake-task-001",
        workspace="default",
        target_implementation="example-agent",
        specifier="bob",
        launch_params={
            "factory": "scitrera_aether_ag2.examples.orchestrated_pool:make_echo_agent",
        },
        metadata={
            "agent_name": "bob",
            "gateway": endpoint,
        },
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    ensure_binary_executable()

    data_dir = Path(tempfile.mkdtemp(prefix="aether-orch-pool-"))
    proc, grpc_port = spawn_aetherlite(data_dir)
    endpoint = f"127.0.0.1:{grpc_port}"

    try:
        wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)

        # Build an orchestrator connected to the live aetherlite instance.
        # In production, orch.run() would block and receive TaskAssignment
        # messages from the gateway dispatcher.  Here we call handle_assignment
        # directly with a hand-built assignment to show the spawn API.
        orch = AetherAg2Orchestrator(
            implementation="example-orch",
            gateway=endpoint,
        )

        assignment = _make_fake_assignment(endpoint)

        print(f"Dispatching fake assignment: task_id={assignment.task_id}")
        print(f"  factory: {assignment.launch_params['factory']}")
        print(f"  target:  ag::default::{assignment.target_implementation}::{assignment.specifier}")

        # handle_assignment calls spawn_module which launches:
        #   python -m scitrera_aether_ag2.host_entrypoint
        # with env vars populated from the assignment.
        orch.handle_assignment(assignment)

        # Give the subprocess a moment to start and connect
        await asyncio.sleep(2.0)

        count = orch.active_subprocess_count
        subprocs = orch.get_all_subprocesses()  # dict[task_id, SubprocessInfo]
        alive = [si for si in subprocs.values() if si.is_running()]
        print(f"Active subprocess count reported by orchestrator: {count}")
        print(f"Subprocesses still running: {len(alive)}")
        print("Orchestrator spawn API exercised successfully.")

        # Clean up spawned subprocesses
        for si in list(subprocs.values()):
            try:
                orch.terminate_process(si)
            except Exception:
                pass

    finally:
        terminate_aetherlite(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
