"""Shared aetherlite-spawn helper used by the example scripts.

Private to ``scitrera_aether_ag2.examples`` — not a public API. The examples
all need to spin up their own aetherlite gateway in a tempdir, wait for the
gRPC port to accept connections, then tear it down on exit; rather than
duplicate the boilerplate across each script, they import the helpers from
here.

Not used by tests — ``tests/conftest.py`` has its own session-scoped variant
with retry/backoff suited to parallel test runs.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
AETHERLITE_BIN = Path(os.environ.get("AETHERLITE_BIN", str(REPO_ROOT / "server" / "aetherlite")))
READY_TIMEOUT_S = 20.0


def pick_free_port() -> int:
    """Bind/release an ephemeral TCP port and return its number.

    Caller still races with anyone else on the host; for examples this is
    fine — for tests, prefer ``conftest._pick_free_port`` which retries.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_tcp(host: str, port: int, timeout: float, proc: subprocess.Popen) -> None:
    """Block until host:port accepts a TCP connection or proc dies / timeout fires."""
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


def spawn_aetherlite(data_dir: Path) -> tuple[subprocess.Popen, int]:
    """Spawn an aetherlite subprocess in dev mode and return (process, gRPC port).

    The process is started in its own session so the caller can SIGTERM the
    whole group on shutdown.
    """
    grpc_port = pick_free_port()
    admin_port = pick_free_port()
    workflow_admin_port = pick_free_port()
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


def terminate_aetherlite(proc: subprocess.Popen) -> None:
    """SIGTERM the aetherlite process group; escalate to SIGKILL on timeout."""
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


def ensure_binary_executable() -> None:
    """Fail fast with a friendly message if AETHERLITE_BIN can't be exec'd."""
    if not AETHERLITE_BIN.is_file() or not os.access(AETHERLITE_BIN, os.X_OK):
        raise SystemExit(
            f"aetherlite binary not found at {AETHERLITE_BIN}; "
            "build it with: cd <repo>/server && go build -o aetherlite ./cmd/aetherlite"
        )


__all__ = [
    "AETHERLITE_BIN",
    "READY_TIMEOUT_S",
    "REPO_ROOT",
    "ensure_binary_executable",
    "pick_free_port",
    "spawn_aetherlite",
    "terminate_aetherlite",
    "wait_for_tcp",
]
