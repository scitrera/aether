"""Run a two-agent ag2 conversation where one agent lives in this process,
the other is reached over Aether. Spawns its own aetherlite gateway."""

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
from pathlib import Path
from typing import Any

from autogen.agentchat import ConversableAgent
from scitrera_aether_client import AsyncAgentClient

from scitrera_aether_ag2 import (
    AetherAgentHost,
    AetherIdentity,
    AetherRemoteAgent,
    AetherTransport,
)

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
# Echo agent (no LLM key required)
# ---------------------------------------------------------------------------

class EchoAgent(ConversableAgent):
    """Deterministic agent that echoes its last received message. No LLM needed."""

    def __init__(self, name: str = "echo-bob") -> None:
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


# ---------------------------------------------------------------------------
# Minimal sender that records replies
# ---------------------------------------------------------------------------

class RecordingSender:
    """Minimal Agent stub that records replies sent back by the proxy."""

    def __init__(self, name: str = "alice") -> None:
        self.name = name
        self.description = name
        self.chat_messages: dict[Any, list[dict[str, Any]]] = {}
        self.received: list[dict[str, Any]] = []

    async def a_send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        msg = message if isinstance(message, dict) else {"content": message}
        self.received.append(msg)

    def send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        msg = message if isinstance(message, dict) else {"content": message}
        self.received.append(msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    if not AETHERLITE_BIN.is_file() or not os.access(AETHERLITE_BIN, os.X_OK):
        raise SystemExit(
            f"aetherlite binary not found at {AETHERLITE_BIN}; "
            "build it with: cd <repo>/server && go build -o aetherlite ./cmd/aetherlite"
        )

    data_dir = Path(tempfile.mkdtemp(prefix="aether-two-agent-"))
    proc, grpc_port = _spawn_aetherlite(data_dir)
    endpoint = f"127.0.0.1:{grpc_port}"

    try:
        _wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)

        host_identity = AetherIdentity("default", "echo", "bob")
        caller_identity = AetherIdentity("default", "caller", "alice")

        # 1. Start host in background task
        host = AetherAgentHost(
            EchoAgent(),
            host_identity,
            endpoint,
            enable_checkpoints=False,
            enable_telemetry=False,
        )
        host_task = asyncio.create_task(host.serve())
        await asyncio.sleep(1.5)  # let host connect

        # 2. Caller client + transport
        caller_client = AsyncAgentClient(
            workspace=caller_identity.workspace,
            implementation=caller_identity.implementation,
            specifier=caller_identity.specifier,
        )
        transport = AetherTransport(caller_client, caller_identity)
        await caller_client.connect(endpoint)
        await transport.wait_connected()

        try:
            # 3. Remote agent proxy
            proxy = AetherRemoteAgent(
                name="echo-bob",
                remote_identity=host_identity,
                transport=transport,
            )

            # 4. Drive one turn
            sender = RecordingSender()
            await asyncio.wait_for(
                proxy.a_receive({"content": "hi", "role": "user"}, sender),
                timeout=10.0,
            )

            # 5. Print result
            history = proxy.chat_messages[sender]
            replies = [m for m in history if m.get("role") == "assistant"]
            if replies:
                print(f"Reply from remote agent: {replies[-1].get('content', '')}")
            else:
                print(f"Full history: {history}")

        finally:
            await caller_client.close()
            await host.stop()
            try:
                await asyncio.wait_for(host_task, timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                host_task.cancel()

    finally:
        _terminate_aetherlite(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
