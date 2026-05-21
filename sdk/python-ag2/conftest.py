"""Top-level pytest fixtures: spin up an isolated aetherlite gateway per test session."""

from __future__ import annotations

import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BIN = REPO_ROOT / "server" / "aetherlite"
READY_TIMEOUT_S = 20.0
SPAWN_RETRIES = 3
EARLY_DEATH_S = 2.0
LOG_TAIL_LINES = 50


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


def _spawn_once(bin_path: Path, data_dir: Path) -> tuple[subprocess.Popen, int, Path]:
    grpc_port = _pick_free_port()
    admin_port = _pick_free_port()
    workflow_admin_port = _pick_free_port()
    env = {**os.environ, "AETHER_ALLOW_DEV_MODE": "true"}
    cmd = [
        str(bin_path),
        "--dev",
        "--insecure-admin",
        "--port", str(grpc_port),
        "--admin-port", str(admin_port),
        "--workflow-admin-port", str(workflow_admin_port),
        "--data-dir", str(data_dir),
    ]
    log_path = data_dir / "aetherlite.log"
    log_file = log_path.open("w")
    proc = subprocess.Popen(
        cmd, stdout=log_file, stderr=subprocess.STDOUT,
        env=env, start_new_session=True,
    )
    proc._omc_log_file = log_file  # type: ignore[attr-defined]
    logger.info("spawned aetherlite pid=%s gRPC=%s admin=%s data_dir=%s",
                proc.pid, grpc_port, admin_port, data_dir)
    return proc, grpc_port, log_path


def _terminate(proc: subprocess.Popen) -> None:
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
    log_file = getattr(proc, "_omc_log_file", None)
    if log_file is not None:
        log_file.close()


def _tail_log(log_path: Path, lines: int) -> str:
    try:
        with log_path.open("r") as f:
            return "".join(f.readlines()[-lines:])
    except OSError as exc:
        return f"(log unreadable: {exc})"


@pytest.fixture(scope="session")
def aetherlite_endpoint(request: pytest.FixtureRequest) -> Iterator[str]:
    """Spawn a session-scoped aetherlite subprocess on an ephemeral port with a tempdir data-dir.

    Skips when AETHER_RUN_E2E != "1" so plain `pytest` works without infra.
    Honors AETHERLITE_BIN to override the binary path.
    """
    if os.environ.get("AETHER_RUN_E2E") != "1":
        pytest.skip("Set AETHER_RUN_E2E=1 to enable E2E tests against a spawned aetherlite")

    bin_path = Path(os.environ.get("AETHERLITE_BIN", DEFAULT_BIN))
    if not bin_path.is_file() or not os.access(bin_path, os.X_OK):
        pytest.skip(
            f"aetherlite binary not found or not executable at {bin_path}; "
            f"build it with `cd {REPO_ROOT}/server && go build -o aetherlite ./cmd/aetherlite`"
        )

    data_dir = Path(tempfile.mkdtemp(prefix="aether-ag2-test-"))
    proc: subprocess.Popen | None = None
    grpc_port = 0
    log_path = data_dir / "aetherlite.log"
    last_err: Exception | None = None
    for attempt in range(1, SPAWN_RETRIES + 1):
        proc, grpc_port, log_path = _spawn_once(bin_path, data_dir)
        try:
            _wait_for_tcp("127.0.0.1", grpc_port, READY_TIMEOUT_S, proc)
            last_err = None
            break
        except (TimeoutError, RuntimeError) as exc:
            last_err = exc
            elapsed = 0.0
            if proc.poll() is not None and elapsed < EARLY_DEATH_S:
                logger.warning("aetherlite spawn attempt %d failed: %s; retrying", attempt, exc)
                _terminate(proc)
                proc = None
                continue
            _terminate(proc)
            raise
    if proc is None or last_err is not None:
        raise RuntimeError(f"aetherlite failed to spawn after {SPAWN_RETRIES} attempts: {last_err}")

    try:
        yield f"127.0.0.1:{grpc_port}"
    finally:
        _terminate(proc)
        if request.session.testsfailed:
            sys.stderr.write(
                f"\n----- aetherlite log tail ({log_path}) -----\n"
                f"{_tail_log(log_path, LOG_TAIL_LINES)}"
                f"----- end aetherlite log tail -----\n"
            )
        if os.environ.get("AETHER_KEEP_DATA") != "1":
            shutil.rmtree(data_dir, ignore_errors=True)
        else:
            logger.info("AETHER_KEEP_DATA=1: kept aetherlite data_dir at %s (log: %s)",
                        data_dir, log_path)


@pytest.fixture
def dev_gateway_endpoint(aetherlite_endpoint: str) -> str:
    """Alias kept for existing tests; resolves via aetherlite_endpoint."""
    return aetherlite_endpoint
