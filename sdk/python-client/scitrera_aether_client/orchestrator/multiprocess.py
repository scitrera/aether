"""
Multiprocess Orchestrator - spawns agents as subprocesses.

This module provides a concrete orchestrator implementation that launches
agents as local subprocesses. It's suitable for single-machine deployments
or development/testing environments.

The MultiprocessOrchestrator handles:
- Spawning agent scripts or Python modules as subprocesses
- Reading and logging subprocess output
- Monitoring process health and automatic cleanup
- Graceful termination with configurable timeouts

Example:
    from scitrera_aether_client.orchestrator.multiprocess import MultiprocessOrchestrator

    class MyOrchestrator(MultiprocessOrchestrator):
        def get_implementation(self):
            return "my-orchestrator"

        def get_supported_profiles(self):
            return ["my-profile"]

        def handle_assignment(self, assignment):
            self.spawn_subprocess(
                task_id=assignment.task_id,
                script_path="/path/to/agent.py",
                env={
                    "WORKSPACE": assignment.workspace,
                    "SPECIFIER": assignment.specifier,
                },
            )
"""

import logging
import os
import secrets
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from . import BaseOrchestrator, LaunchedProcess


@dataclass
class SubprocessInfo(LaunchedProcess):
    """
    Extended process info for subprocess-based agents.

    Extends LaunchedProcess with subprocess-specific fields.

    Attributes:
        process: The subprocess.Popen object.
        output_thread: Thread reading the process stdout/stderr.
        _stop_output: Flag to signal the output thread to stop.
    """
    process: Optional[subprocess.Popen] = None
    output_thread: Optional[threading.Thread] = None
    _stop_output: bool = field(default=False, repr=False)

    @property
    def pid(self) -> Optional[int]:
        """Return the process ID, or None if not started."""
        return self.process.pid if self.process else None

    @property
    def returncode(self) -> Optional[int]:
        """Return the exit code, or None if still running."""
        if self.process:
            return self.process.poll()
        return None

    def is_running(self) -> bool:
        """Return True if the process is still running."""
        return self.process is not None and self.process.poll() is None


class MultiprocessOrchestrator(BaseOrchestrator):
    """
    Orchestrator that spawns agents as local subprocesses.

    This orchestrator provides a complete implementation for spawning
    and managing agent processes on the local machine. It handles:

    - Subprocess creation with environment configuration
    - Stdout/stderr capture and logging
    - Process health monitoring
    - Graceful and forced termination

    Subclasses must implement:
    - get_implementation(): Returns the orchestrator identifier
    - get_supported_profiles(): Returns supported profile list
    - handle_assignment(): Processes task assignments

    Subclasses can use:
    - spawn_subprocess(): Launch an agent as a subprocess
    - spawn_module(): Launch a Python module as a subprocess

    Example:
        class EchoOrchestrator(MultiprocessOrchestrator):
            def get_implementation(self):
                return "echo-orchestrator"

            def get_supported_profiles(self):
                return ["echo"]

            def handle_assignment(self, assignment):
                self.spawn_subprocess(
                    task_id=assignment.task_id,
                    script_path=Path(__file__).parent / "echo_agent.py",
                    workspace=assignment.workspace,
                    implementation=assignment.target_implementation,
                    specifier=assignment.specifier,
                )
    """

    def __init__(
        self,
        gateway: str = "localhost:50051",
        *,
        specifier: Optional[str] = None,
        terminate_timeout: float = 5.0,
        kill_timeout: float = 2.0,
        output_log_prefix: Optional[str] = None,
        tls_enabled: bool = False,
        tls_root_cert: Optional[bytes] = None,
        tls_root_cert_path: Optional[str] = None,
        tls_client_cert: Optional[bytes] = None,
        tls_client_cert_path: Optional[str] = None,
        tls_client_key: Optional[bytes] = None,
        tls_client_key_path: Optional[str] = None,
    ):
        """
        Initialize the multiprocess orchestrator.

        Args:
            gateway: Gateway address in host:port format.
            specifier: Optional unique specifier for this orchestrator instance.
            terminate_timeout: Seconds to wait after SIGTERM before SIGKILL.
            kill_timeout: Seconds to wait after SIGKILL before giving up.
            output_log_prefix: Default prefix for agent output logs
                (defaults to "Agent").
            tls_enabled: Whether to use TLS for the connection.
            tls_root_cert: PEM-encoded root CA certificate bytes.
            tls_root_cert_path: Path to root CA certificate file.
            tls_client_cert: PEM-encoded client certificate bytes (for mTLS).
            tls_client_cert_path: Path to client certificate file.
            tls_client_key: PEM-encoded client private key bytes (for mTLS).
            tls_client_key_path: Path to client private key file.
        """
        super().__init__(
            gateway=gateway,
            specifier=specifier,
            tls_enabled=tls_enabled,
            tls_root_cert=tls_root_cert,
            tls_root_cert_path=tls_root_cert_path,
            tls_client_cert=tls_client_cert,
            tls_client_cert_path=tls_client_cert_path,
            tls_client_key=tls_client_key,
            tls_client_key_path=tls_client_key_path,
        )

        self._terminate_timeout = terminate_timeout
        self._kill_timeout = kill_timeout
        self._output_log_prefix = output_log_prefix or "Agent"

    # =========================================================================
    # Process Spawning
    # =========================================================================

    def spawn_subprocess(
        self,
        task_id: str,
        script_path: Union[str, Path],
        *,
        workspace: str = "default",
        implementation: str = "agent",
        specifier: Optional[str] = None,
        auth_token: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        args: Optional[List[str]] = None,
        cwd: Optional[Union[str, Path]] = None,
        python_executable: Optional[str] = None,
        capture_output: bool = True,
        output_callback: Optional[Callable[[str, str], None]] = None,
    ) -> Optional[SubprocessInfo]:
        """
        Spawn an agent script as a subprocess.

        Launches a Python script in a new subprocess with the specified
        environment configuration. Automatically sets standard Aether
        environment variables (AETHER_GATEWAY, AETHER_WORKSPACE, etc.).

        Args:
            task_id: Unique identifier for this task/agent.
            script_path: Path to the Python script to run.
            workspace: Workspace for the agent (default: "default").
            implementation: Implementation identifier for the agent.
            specifier: Unique specifier for the agent (auto-generated if None).
            auth_token: Authentication token (auto-generated if None).
            env: Additional environment variables to set.
            args: Additional command-line arguments to pass to the script.
            cwd: Working directory for the subprocess.
            python_executable: Python interpreter to use (default: sys.executable).
            capture_output: Whether to capture and log stdout/stderr.
            output_callback: Optional callback for output lines.
                Called with (specifier, line) for each output line.

        Returns:
            SubprocessInfo if successful, None if failed.

        Example:
            info = self.spawn_subprocess(
                task_id="task-123",
                script_path="/agents/echo_agent.py",
                workspace="production",
                implementation="echo",
                specifier="echo-1",
                env={"CUSTOM_VAR": "value"},
            )
        """
        script_path = Path(script_path)
        if not script_path.exists():
            self._log("Script not found: %s" % script_path, level=logging.ERROR)
            return None

        # Generate defaults
        specifier = specifier or task_id
        auth_token = auth_token or secrets.token_urlsafe(32)
        python_exe = python_executable or sys.executable

        # Build environment
        process_env = os.environ.copy()
        process_env.update({
            "AETHER_GATEWAY": self.gateway,
            "AETHER_WORKSPACE": workspace,
            "AETHER_IMPLEMENTATION": implementation,
            "AETHER_SPECIFIER": specifier,
            "AETHER_AUTH_TOKEN": auth_token,
        })
        if env:
            process_env.update(env)

        # Build command
        cmd = [python_exe, str(script_path)]
        if args:
            cmd.extend(args)

        self._log(f"Spawning subprocess for task {task_id}")
        self._log(f"  Script: {script_path}")
        self._log(f"  Workspace: {workspace}")
        self._log(f"  Implementation: {implementation}")
        self._log(f"  Specifier: {specifier}")

        try:
            # Create subprocess
            popen_kwargs: Dict[str, Any] = {
                "env": process_env,
            }
            if cwd:
                popen_kwargs["cwd"] = str(cwd)

            if capture_output:
                popen_kwargs.update({
                    "stdout": subprocess.PIPE,
                    "stderr": subprocess.STDOUT,
                    "text": True,
                })

            process = subprocess.Popen(cmd, **popen_kwargs)

            # Create process info
            info = SubprocessInfo(
                task_id=task_id,
                workspace=workspace,
                implementation=implementation,
                specifier=specifier,
                auth_token=auth_token,
                process=process,
            )

            # Start output reader thread if capturing
            if capture_output and process.stdout:
                output_thread = threading.Thread(
                    target=self._read_process_output,
                    args=(info, output_callback),
                    daemon=True,
                    name=f"output-reader-{specifier}",
                )
                info.output_thread = output_thread
                output_thread.start()

            # Track the process
            self.track_process(info)

            self._log(f"Subprocess started with PID {process.pid}")
            return info

        except Exception as e:
            self._log("Error spawning subprocess: %s" % e, level=logging.ERROR)
            return None

    def spawn_module(
        self,
        task_id: str,
        module_name: str,
        *,
        workspace: str = "default",
        implementation: str = "agent",
        specifier: Optional[str] = None,
        auth_token: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        args: Optional[List[str]] = None,
        cwd: Optional[Union[str, Path]] = None,
        python_executable: Optional[str] = None,
        capture_output: bool = True,
        output_callback: Optional[Callable[[str, str], None]] = None,
    ) -> Optional[SubprocessInfo]:
        """
        Spawn a Python module as a subprocess using `python -m`.

        Similar to spawn_subprocess but runs a module instead of a script.

        Args:
            task_id: Unique identifier for this task/agent.
            module_name: Python module name to run (e.g., "my_package.agent").
            workspace: Workspace for the agent (default: "default").
            implementation: Implementation identifier for the agent.
            specifier: Unique specifier for the agent (auto-generated if None).
            auth_token: Authentication token (auto-generated if None).
            env: Additional environment variables to set.
            args: Additional command-line arguments to pass to the module.
            cwd: Working directory for the subprocess.
            python_executable: Python interpreter to use (default: sys.executable).
            capture_output: Whether to capture and log stdout/stderr.
            output_callback: Optional callback for output lines.

        Returns:
            SubprocessInfo if successful, None if failed.

        Example:
            info = self.spawn_module(
                task_id="task-123",
                module_name="myagents.echo_agent",
                workspace="production",
            )
        """
        # Generate defaults
        specifier = specifier or task_id
        auth_token = auth_token or secrets.token_urlsafe(32)
        python_exe = python_executable or sys.executable

        # Build environment
        process_env = os.environ.copy()
        process_env.update({
            "AETHER_GATEWAY": self.gateway,
            "AETHER_WORKSPACE": workspace,
            "AETHER_IMPLEMENTATION": implementation,
            "AETHER_SPECIFIER": specifier,
            "AETHER_AUTH_TOKEN": auth_token,
        })
        if env:
            process_env.update(env)

        # Build command: python -m module_name [args]
        cmd = [python_exe, "-m", module_name]
        if args:
            cmd.extend(args)

        self._log(f"Spawning module subprocess for task {task_id}")
        self._log(f"  Module: {module_name}")
        self._log(f"  Workspace: {workspace}")
        self._log(f"  Implementation: {implementation}")
        self._log(f"  Specifier: {specifier}")

        try:
            # Create subprocess
            popen_kwargs: Dict[str, Any] = {
                "env": process_env,
            }
            if cwd:
                popen_kwargs["cwd"] = str(cwd)

            if capture_output:
                popen_kwargs.update({
                    "stdout": subprocess.PIPE,
                    "stderr": subprocess.STDOUT,
                    "text": True,
                })

            process = subprocess.Popen(cmd, **popen_kwargs)

            # Create process info
            info = SubprocessInfo(
                task_id=task_id,
                workspace=workspace,
                implementation=implementation,
                specifier=specifier,
                auth_token=auth_token,
                process=process,
            )

            # Start output reader thread if capturing
            if capture_output and process.stdout:
                output_thread = threading.Thread(
                    target=self._read_process_output,
                    args=(info, output_callback),
                    daemon=True,
                    name=f"output-reader-{specifier}",
                )
                info.output_thread = output_thread
                output_thread.start()

            # Track the process
            self.track_process(info)

            self._log(f"Module subprocess started with PID {process.pid}")
            return info

        except Exception as e:
            self._log("Error spawning module subprocess: %s" % e, level=logging.ERROR)
            return None

    # =========================================================================
    # Output Reading
    # =========================================================================

    def _read_process_output(
        self,
        info: SubprocessInfo,
        callback: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        """
        Read stdout from a subprocess in a dedicated thread.

        This method runs in a separate thread and performs blocking
        readline() calls. It terminates when the subprocess closes
        its stdout (process exits) or when _stop_output is set.

        Args:
            info: The subprocess info containing the process handle.
            callback: Optional callback called with (specifier, line)
                for each line of output.
        """
        specifier = info.specifier
        prefix = f"{self._output_log_prefix} {specifier}"

        try:
            while not info._stop_output and info.process and info.process.stdout:
                line = info.process.stdout.readline()
                if not line:
                    # EOF - process has closed stdout
                    break

                line = line.rstrip()

                # Call custom callback if provided
                if callback:
                    try:
                        callback(specifier, line)
                    except Exception as e:
                        self._log(f"Output callback error: {e}", prefix=prefix)

                # Log the line
                self._log(line, prefix=prefix)

        except Exception as e:
            self._log(f"Output reader error: {e}", prefix=prefix)

    # =========================================================================
    # Process Health Monitoring
    # =========================================================================

    def _check_processes(self) -> None:
        """
        Check status of tracked processes and clean up terminated ones.

        This is called periodically from the main run loop. It checks
        each tracked process and removes any that have exited.
        """
        with self._processes_lock:
            for task_id, process in list(self._processes.items()):
                if not isinstance(process, SubprocessInfo):
                    continue

                if process.process is None:
                    continue

                returncode = process.process.poll()
                if returncode is not None:
                    self._log(
                        f"Process {process.specifier} (task {task_id}) "
                        f"exited with code {returncode}"
                    )

                    # Signal output thread to stop
                    process._stop_output = True

                    # Call cleanup hook
                    try:
                        self.cleanup_process(process)
                    except Exception as e:
                        self._log(f"Error in cleanup_process: {e}")

                    # Remove from tracking
                    del self._processes[task_id]

    # =========================================================================
    # Process Termination
    # =========================================================================

    def terminate_process(self, process: LaunchedProcess) -> None:
        """
        Terminate a subprocess gracefully, with fallback to SIGKILL.

        First sends SIGTERM and waits for terminate_timeout seconds.
        If the process is still running, sends SIGKILL and waits
        for kill_timeout seconds.

        Args:
            process: The process to terminate.
        """
        if not isinstance(process, SubprocessInfo):
            return

        if process.process is None:
            return

        # Signal output thread to stop
        process._stop_output = True

        pid = process.pid
        specifier = process.specifier

        # Check if already terminated
        if process.process.poll() is not None:
            self._log(f"Process {specifier} (PID {pid}) already terminated")
            self._wait_for_output_thread(process)
            return

        # Try graceful termination (SIGTERM)
        self._log(f"Terminating process {specifier} (PID {pid})...")
        try:
            process.process.terminate()
            try:
                process.process.wait(timeout=self._terminate_timeout)
                self._log(f"Process {specifier} terminated gracefully")
                self._wait_for_output_thread(process)
                return
            except subprocess.TimeoutExpired:
                self._log(
                    f"Process {specifier} did not terminate gracefully, "
                    f"sending SIGKILL..."
                )
        except Exception as e:
            self._log(f"Error sending SIGTERM to {specifier}: {e}")

        # Force kill (SIGKILL)
        try:
            process.process.kill()
            try:
                process.process.wait(timeout=self._kill_timeout)
                self._log(f"Process {specifier} killed")
            except subprocess.TimeoutExpired:
                self._log(f"WARNING: Process {specifier} did not respond to SIGKILL")
        except Exception as e:
            self._log(f"Error sending SIGKILL to {specifier}: {e}")

        self._wait_for_output_thread(process)

    def _wait_for_output_thread(
        self,
        process: SubprocessInfo,
        timeout: float = 1.0,
    ) -> None:
        """
        Wait for the output reader thread to finish.

        Args:
            process: The subprocess info.
            timeout: Maximum time to wait in seconds.
        """
        if process.output_thread and process.output_thread.is_alive():
            process.output_thread.join(timeout=timeout)

    # =========================================================================
    # Convenience Methods
    # =========================================================================

    def get_subprocess(self, task_id: str) -> Optional[SubprocessInfo]:
        """
        Get subprocess info by task ID.

        Args:
            task_id: The task ID to look up.

        Returns:
            SubprocessInfo if found and is a subprocess, None otherwise.
        """
        process = self.get_process(task_id)
        if isinstance(process, SubprocessInfo):
            return process
        return None

    def get_all_subprocesses(self) -> Dict[str, SubprocessInfo]:
        """
        Get all tracked subprocesses.

        Returns:
            Dictionary mapping task IDs to SubprocessInfo.
        """
        result: Dict[str, SubprocessInfo] = {}
        with self._processes_lock:
            for task_id, process in self._processes.items():
                if isinstance(process, SubprocessInfo):
                    result[task_id] = process
        return result

    @property
    def active_subprocess_count(self) -> int:
        """Return the number of currently running subprocesses."""
        count = 0
        with self._processes_lock:
            for process in self._processes.values():
                if isinstance(process, SubprocessInfo) and process.is_running():
                    count += 1
        return count


# Exports
__all__ = [
    "MultiprocessOrchestrator",
    "SubprocessInfo",
]
