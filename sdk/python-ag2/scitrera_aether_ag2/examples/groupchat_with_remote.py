"""GroupChat-style conversation with two local agents and one AetherRemoteAgent.

Spawns its own aetherlite gateway. Uses a manual round-robin loop instead of
ag2's GroupChatManager so no LLM-based speaker selection is required.

# illustrative GroupChat-equivalent: manual loop used because ag2 GroupChatManager
# requires an LLM to select the next speaker; a deterministic round-robin avoids
# that dependency for a key-free demo.
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
# Agents
# ---------------------------------------------------------------------------

class EchoAgent(ConversableAgent):
    """Deterministic agent that echoes its last received message. No LLM needed."""

    def __init__(self, name: str) -> None:
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
        return True, f"{self.name} echoes: {last.get('content', '')}"


class RecordingSender:
    """Minimal Agent stub used as the initiating 'user' in the round-robin."""

    def __init__(self, name: str) -> None:
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

    data_dir = Path(tempfile.mkdtemp(prefix="aether-groupchat-"))
    proc, grpc_port = _spawn_aetherlite(data_dir)
    endpoint = f"127.0.0.1:{grpc_port}"

    try:
        _wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)

        # Identities
        remote_identity = AetherIdentity("default", "echo", "carol")
        caller_identity = AetherIdentity("default", "caller", "initiator")

        # 1. Start remote hosted agent (carol)
        host = AetherAgentHost(
            EchoAgent("carol"),
            remote_identity,
            endpoint,
            enable_checkpoints=False,
            enable_telemetry=False,
        )
        host_task = asyncio.create_task(host.serve())
        await asyncio.sleep(1.5)

        # 2. Caller transport
        caller_client = AsyncAgentClient(
            workspace=caller_identity.workspace,
            implementation=caller_identity.implementation,
            specifier=caller_identity.specifier,
        )
        transport = AetherTransport(caller_client, caller_identity)
        await caller_client.connect(endpoint)
        await transport.wait_connected()

        try:
            # 3. Agents: two local, one remote
            alice = EchoAgent("alice")
            bob = EchoAgent("bob")
            carol_proxy = AetherRemoteAgent(
                name="carol",
                remote_identity=remote_identity,
                transport=transport,
            )

            # 4. Manual round-robin over [alice, bob, carol_proxy] for 2 rounds
            # illustrative GroupChat-equivalent: no LLM speaker selection needed
            agents = [alice, bob, carol_proxy]
            initiator = RecordingSender("initiator")
            current_message: dict[str, Any] = {"content": "start: hello group", "role": "user"}
            transcript: list[tuple[str, str]] = [
                ("initiator", current_message["content"])
            ]

            for round_num in range(2):
                for agent in agents:
                    sender = initiator if round_num == 0 else initiator

                    if isinstance(agent, AetherRemoteAgent):
                        await asyncio.wait_for(
                            agent.a_receive(current_message, initiator),
                            timeout=10.0,
                        )
                        history = agent.chat_messages.get(initiator, [])
                        replies = [m for m in history if m.get("role") == "assistant"]
                        reply_text = replies[-1].get("content", "") if replies else "(no reply)"
                    else:
                        # Local ConversableAgent: call generate_reply directly
                        msgs = [current_message]
                        _, reply_text = await agent.a_generate_oai_reply(msgs)

                    transcript.append((agent.name, reply_text))
                    current_message = {"content": reply_text, "role": "user"}

            # 5. Print transcript
            print("--- GroupChat transcript ---")
            for speaker, text in transcript:
                print(f"  [{speaker}]: {text}")
            print("--- end transcript ---")

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
