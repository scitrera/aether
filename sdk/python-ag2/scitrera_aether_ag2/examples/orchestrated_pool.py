"""Demonstrate AetherAg2Orchestrator spawning a host subprocess on demand.

Spawns its own aetherlite gateway.

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
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import types
from pathlib import Path
from typing import Any

from autogen.agentchat import ConversableAgent

from scitrera_aether_ag2.orchestrator import AetherAg2Orchestrator

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[4]
AETHERLITE_BIN = Path(os.environ.get("AETHERLITE_BIN", str(REPO_ROOT / "server" / "aetherlite")))
READY_TIMEOUT_S = 20.0


# ---------------------------------------------------------------------------
# Aetherlite helpers (self-contained; not imported from conftest)
# ---------------------------------------------------------------------------

def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_tcp(host: str, port: int, timeout: float, proc: subprocess.Popen) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"aetherlite exited early with rc={proc.returncode}")
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError as exc:
            last_err = exc
            time.sleep(0.1)
    raise TimeoutError(f"aetherlite at {host}:{port} not ready after {timeout}s: {last_err}")


def _spawn_aetherlite(data_dir: Path) -> tuple[subprocess.Popen, int]:
    grpc_port = _pick_free_port()
    admin_port = _pick_free_port()
    workflow_admin_port = _pick_free_port()
    env = {**os.environ, "AETHER_ALLOW_DEV_MODE": "true"}
    cmd = [
        str(AETHERLITE_BIN),
        "--dev",
        "--insecure-admin",
        "--port", str(grpc_port),
        "--admin-port", str(admin_port),
        "--workflow-admin-port", str(workflow_admin_port),
        "--data-dir", str(data_dir),
    ]
    log_file = (data_dir / "aetherlite.log").open("w")
    proc = subprocess.Popen(
        cmd, stdout=log_file, stderr=subprocess.STDOUT,
        env=env, start_new_session=True,
    )
    proc._log_file = log_file  # type: ignore[attr-defined]
    return proc, grpc_port


def _terminate_aetherlite(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5.0)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5.0)
    log_file = getattr(proc, "_log_file", None)
    if log_file is not None:
        log_file.close()


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
    if not AETHERLITE_BIN.is_file() or not os.access(AETHERLITE_BIN, os.X_OK):
        raise SystemExit(
            f"aetherlite binary not found at {AETHERLITE_BIN}; "
            "build it with: cd <repo>/server && go build -o aetherlite ./cmd/aetherlite"
        )

    data_dir = Path(tempfile.mkdtemp(prefix="aether-orch-pool-"))
    proc, grpc_port = _spawn_aetherlite(data_dir)
    endpoint = f"127.0.0.1:{grpc_port}"

    try:
        _wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)

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
        _terminate_aetherlite(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
