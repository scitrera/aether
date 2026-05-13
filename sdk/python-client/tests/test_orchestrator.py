"""
Unit tests for the multiprocessing orchestrator.

This module tests:
- SubprocessInfo dataclass
- MultiprocessOrchestrator initialization
- Process spawning (spawn_subprocess, spawn_module)
- Output reading and logging
- Process health monitoring
- Process termination
- Convenience methods
"""
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from scitrera_aether_client.orchestrator import (
    LaunchedProcess,
    MultiprocessOrchestrator,
    SubprocessInfo,
)
from scitrera_aether_client.proto import aether_pb2


# =============================================================================
# Test Constants
# =============================================================================

TEST_GATEWAY = "localhost:50051"
TEST_WORKSPACE = "test-workspace"
TEST_IMPLEMENTATION = "test-impl"
TEST_SPECIFIER = "test-spec"
TEST_TASK_ID = "task-123"
TEST_PROFILES = ["profile-1", "profile-2"]


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def temp_script() -> Path:
    """Create a temporary Python script that prints and exits."""
    script_content = """
import os
import sys
import time

print("Hello from subprocess!")
print("Args:", sys.argv[1:])
print("AETHER_GATEWAY:", os.environ.get("AETHER_GATEWAY", "NOT_SET"))
print("AETHER_WORKSPACE:", os.environ.get("AETHER_WORKSPACE", "NOT_SET"))
print("Done!")
sys.exit(0)
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script_content)
        temp_path = Path(f.name)
    yield temp_path
    # Cleanup
    temp_path.unlink(missing_ok=True)


@pytest.fixture
def temp_long_running_script() -> Path:
    """Create a temporary Python script that runs until terminated."""
    script_content = """
import sys
import time
import signal

def handler(signum, frame):
    print("Received signal:", signum)
    sys.exit(0)

signal.signal(signal.SIGTERM, handler)

print("Starting long-running process...")
while True:
    time.sleep(0.1)
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script_content)
        temp_path = Path(f.name)
    yield temp_path
    # Cleanup
    temp_path.unlink(missing_ok=True)


@pytest.fixture
def temp_error_script() -> Path:
    """Create a temporary Python script that exits with error."""
    script_content = """
import sys
print("About to fail!")
sys.exit(1)
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script_content)
        temp_path = Path(f.name)
    yield temp_path
    # Cleanup
    temp_path.unlink(missing_ok=True)


class ConcreteOrchestrator(MultiprocessOrchestrator):  # type: ignore[misc]
    """Concrete implementation of MultiprocessOrchestrator for testing."""

    def __init__(self, **kwargs):
        # Don't call super().__init__ to avoid creating the OrchestratorClient
        # which would require all the client infrastructure.
        # This is intentional for testing purposes.
        self.gateway = kwargs.get("gateway", TEST_GATEWAY)
        self._specifier = kwargs.get("specifier")
        self._terminate_timeout = kwargs.get("terminate_timeout", 5.0)
        self._kill_timeout = kwargs.get("kill_timeout", 2.0)
        self._output_log_prefix = kwargs.get("output_log_prefix", "Agent")

        # Initialize process tracking
        self._processes: Dict[str, LaunchedProcess] = {}
        self._processes_lock = threading.Lock()
        self._print_lock = threading.Lock()
        self._shutdown_requested = False

        # Mock the client
        self.client = MagicMock()
        self.client.is_running = True

        # Track method calls for testing
        self._logged_messages: List[str] = []
        self._cleanup_calls: List[LaunchedProcess] = []

    def get_implementation(self) -> str:
        return "test-orchestrator"

    def get_supported_profiles(self) -> List[str]:
        return TEST_PROFILES.copy()

    def handle_assignment(self, assignment: aether_pb2.TaskAssignment) -> None:
        pass  # No-op for testing

    def _log(self, message: str, prefix: Optional[str] = None, level: int = 0) -> None:
        """Override to capture log messages for testing."""
        self._logged_messages.append(f"[{prefix or 'Orchestrator'}] {message}")

    def cleanup_process(self, process: LaunchedProcess) -> None:
        """Override to track cleanup calls."""
        self._cleanup_calls.append(process)


@pytest.fixture
def orchestrator() -> ConcreteOrchestrator:
    """Create a ConcreteOrchestrator instance for testing."""
    return ConcreteOrchestrator()


@pytest.fixture
def orchestrator_with_timeouts() -> ConcreteOrchestrator:
    """Create an orchestrator with fast timeouts for testing."""
    return ConcreteOrchestrator(
        terminate_timeout=0.5,
        kill_timeout=0.5,
    )


# =============================================================================
# SubprocessInfo Tests
# =============================================================================

class TestSubprocessInfo:
    """Tests for the SubprocessInfo dataclass."""

    def test_subprocessinfo_creation(self):
        """Test creating a SubprocessInfo with defaults."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        assert info.task_id == TEST_TASK_ID
        assert info.workspace == TEST_WORKSPACE
        assert info.implementation == TEST_IMPLEMENTATION
        assert info.specifier == TEST_SPECIFIER
        assert info.auth_token == "token-123"
        assert info.process is None
        assert info.output_thread is None
        assert info._stop_output is False

    def test_subprocessinfo_pid_none_without_process(self):
        """Test that pid is None when no process is set."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        assert info.pid is None

    def test_subprocessinfo_pid_with_process(self):
        """Test that pid returns process pid when set."""
        mock_process = MagicMock()
        mock_process.pid = 12345

        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
            process=mock_process,
        )

        assert info.pid == 12345

    def test_subprocessinfo_returncode_none_without_process(self):
        """Test that returncode is None when no process is set."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        assert info.returncode is None

    def test_subprocessinfo_returncode_running(self):
        """Test returncode is None when process is still running."""
        mock_process = MagicMock()
        mock_process.poll.return_value = None  # Still running

        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
            process=mock_process,
        )

        assert info.returncode is None
        mock_process.poll.assert_called_once()

    def test_subprocessinfo_returncode_exited(self):
        """Test returncode returns exit code when process has exited."""
        mock_process = MagicMock()
        mock_process.poll.return_value = 0

        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
            process=mock_process,
        )

        assert info.returncode == 0

    def test_subprocessinfo_is_running_false_without_process(self):
        """Test is_running returns False when no process is set."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        assert info.is_running() is False

    def test_subprocessinfo_is_running_true_when_running(self):
        """Test is_running returns True when process is running."""
        mock_process = MagicMock()
        mock_process.poll.return_value = None  # Still running

        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
            process=mock_process,
        )

        assert info.is_running() is True

    def test_subprocessinfo_is_running_false_when_exited(self):
        """Test is_running returns False when process has exited."""
        mock_process = MagicMock()
        mock_process.poll.return_value = 0  # Exited

        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
            process=mock_process,
        )

        assert info.is_running() is False


# =============================================================================
# MultiprocessOrchestrator Initialization Tests
# =============================================================================

class TestMultiprocessOrchestratorInit:
    """Tests for MultiprocessOrchestrator initialization."""

    def test_default_initialization(self):
        """Test default initialization values via concrete implementation."""
        orch = ConcreteOrchestrator()

        assert orch.gateway == TEST_GATEWAY
        assert orch._terminate_timeout == 5.0
        assert orch._kill_timeout == 2.0
        assert orch._output_log_prefix == "Agent"

    def test_custom_initialization(self):
        """Test custom initialization values."""
        orch = ConcreteOrchestrator(
            gateway="custom:50052",
            specifier="custom-spec",
            terminate_timeout=10.0,
            kill_timeout=3.0,
            output_log_prefix="CustomAgent",
        )

        assert orch.gateway == "custom:50052"
        assert orch._specifier == "custom-spec"
        assert orch._terminate_timeout == 10.0
        assert orch._kill_timeout == 3.0
        assert orch._output_log_prefix == "CustomAgent"

    def test_get_implementation(self):
        """Test get_implementation returns correct value."""
        orch = ConcreteOrchestrator()
        assert orch.get_implementation() == "test-orchestrator"

    def test_get_supported_profiles(self):
        """Test get_supported_profiles returns correct value."""
        orch = ConcreteOrchestrator()
        assert orch.get_supported_profiles() == TEST_PROFILES


# =============================================================================
# spawn_subprocess Tests
# =============================================================================

class TestSpawnSubprocess:
    """Tests for the spawn_subprocess method."""

    def test_spawn_subprocess_script_not_found(self, orchestrator: ConcreteOrchestrator):
        """Test spawn_subprocess returns None when script doesn't exist."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path="/nonexistent/path/to/script.py",
        )

        assert result is None
        assert any("not found" in msg for msg in orchestrator._logged_messages)

    def test_spawn_subprocess_success(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess successfully creates a subprocess."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
        )

        try:
            assert result is not None
            assert isinstance(result, SubprocessInfo)
            assert result.task_id == TEST_TASK_ID
            assert result.workspace == TEST_WORKSPACE
            assert result.implementation == TEST_IMPLEMENTATION
            assert result.specifier == TEST_SPECIFIER
            assert result.process is not None
            assert result.pid is not None
            assert result.auth_token is not None  # Auto-generated

            # Wait for process to complete
            result.process.wait(timeout=5)
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_auto_generates_specifier(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess auto-generates specifier from task_id."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
        )

        try:
            assert result is not None
            assert result.specifier == TEST_TASK_ID  # Uses task_id when not specified
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_custom_specifier(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess uses custom specifier when provided."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
            specifier="custom-specifier",
        )

        try:
            assert result is not None
            assert result.specifier == "custom-specifier"
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_sets_env_vars(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess sets Aether environment variables."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
        )

        try:
            assert result is not None
            # Process should have completed successfully with proper env vars
            result.process.wait(timeout=5)
            assert result.returncode == 0
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_with_custom_env(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess with additional environment variables."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
            env={"CUSTOM_VAR": "custom_value"},
        )

        try:
            assert result is not None
            result.process.wait(timeout=5)
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_with_args(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess with command line arguments."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
            args=["--arg1", "value1", "--arg2", "value2"],
        )

        try:
            assert result is not None
            result.process.wait(timeout=5)
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_tracks_process(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess tracks the process."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
        )

        try:
            assert result is not None
            # Process should be tracked
            tracked = orchestrator.get_process(TEST_TASK_ID)
            assert tracked is result
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_no_capture_output(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess without output capture."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
            capture_output=False,
        )

        try:
            assert result is not None
            assert result.output_thread is None  # No output thread when not capturing
            result.process.wait(timeout=5)
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_subprocess_with_output_callback(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test spawn_subprocess with custom output callback."""
        captured_output = []

        def callback(specifier: str, line: str):
            captured_output.append((specifier, line))

        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
            specifier="my-spec",
            output_callback=callback,
        )

        try:
            assert result is not None
            # Wait for process to complete
            result.process.wait(timeout=5)
            # Give output thread time to process
            time.sleep(0.2)

            # Should have captured some output
            assert len(captured_output) > 0
            assert all(s == "my-spec" for s, _ in captured_output)
        finally:
            if result and result.process:
                result.process.kill()


# =============================================================================
# spawn_module Tests
# =============================================================================

class TestSpawnModule:
    """Tests for the spawn_module method."""

    def test_spawn_module_success(self, orchestrator: ConcreteOrchestrator):
        """Test spawn_module successfully creates a subprocess."""
        # Use a built-in module that will exit quickly
        result = orchestrator.spawn_module(
            task_id=TEST_TASK_ID,
            module_name="site",  # Built-in module that prints info and exits
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
        )

        try:
            assert result is not None
            assert isinstance(result, SubprocessInfo)
            assert result.task_id == TEST_TASK_ID
            assert result.process is not None

            # Wait for process to complete
            result.process.wait(timeout=5)
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_module_auto_generates_specifier(
        self,
        orchestrator: ConcreteOrchestrator,
    ):
        """Test spawn_module auto-generates specifier from task_id."""
        result = orchestrator.spawn_module(
            task_id=TEST_TASK_ID,
            module_name="site",
        )

        try:
            assert result is not None
            assert result.specifier == TEST_TASK_ID
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_module_with_custom_specifier(
        self,
        orchestrator: ConcreteOrchestrator,
    ):
        """Test spawn_module uses custom specifier when provided."""
        result = orchestrator.spawn_module(
            task_id=TEST_TASK_ID,
            module_name="site",
            specifier="module-specifier",
        )

        try:
            assert result is not None
            assert result.specifier == "module-specifier"
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_module_with_args(
        self,
        orchestrator: ConcreteOrchestrator,
    ):
        """Test spawn_module with command line arguments."""
        result = orchestrator.spawn_module(
            task_id=TEST_TASK_ID,
            module_name="site",
            args=["--help"],
        )

        try:
            assert result is not None
            result.process.wait(timeout=5)
        finally:
            if result and result.process:
                result.process.kill()

    def test_spawn_module_nonexistent_module(
        self,
        orchestrator: ConcreteOrchestrator,
    ):
        """Test spawn_module with nonexistent module returns valid process (fails at runtime)."""
        result = orchestrator.spawn_module(
            task_id=TEST_TASK_ID,
            module_name="nonexistent_module_xyz",
        )

        try:
            # The process is created but will fail when Python can't find the module
            assert result is not None
            result.process.wait(timeout=5)
            # Module not found typically returns exit code 1
            assert result.returncode != 0
        finally:
            if result and result.process:
                result.process.kill()


# =============================================================================
# Process Management Tests
# =============================================================================

class TestProcessManagement:
    """Tests for process tracking and management."""

    def test_track_process(self, orchestrator: ConcreteOrchestrator):
        """Test tracking a process."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        orchestrator.track_process(info)

        assert orchestrator.get_process(TEST_TASK_ID) is info

    def test_untrack_process(self, orchestrator: ConcreteOrchestrator):
        """Test untracking a process."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        orchestrator.track_process(info)
        untracked = orchestrator.untrack_process(TEST_TASK_ID)

        assert untracked is info
        assert orchestrator.get_process(TEST_TASK_ID) is None

    def test_untrack_nonexistent_process(self, orchestrator: ConcreteOrchestrator):
        """Test untracking a nonexistent process returns None."""
        result = orchestrator.untrack_process("nonexistent-task")
        assert result is None

    def test_get_process(self, orchestrator: ConcreteOrchestrator):
        """Test getting a tracked process."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        orchestrator.track_process(info)

        assert orchestrator.get_process(TEST_TASK_ID) is info
        assert orchestrator.get_process("nonexistent") is None

    def test_get_subprocess(self, orchestrator: ConcreteOrchestrator):
        """Test get_subprocess returns SubprocessInfo only."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        orchestrator.track_process(info)

        result = orchestrator.get_subprocess(TEST_TASK_ID)
        assert result is info
        assert isinstance(result, SubprocessInfo)

    def test_get_subprocess_returns_none_for_base_process(
        self,
        orchestrator: ConcreteOrchestrator,
    ):
        """Test get_subprocess returns None for non-SubprocessInfo process."""
        # Track a base LaunchedProcess instead
        base_info = LaunchedProcess(
            task_id="base-task",
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        orchestrator.track_process(base_info)

        result = orchestrator.get_subprocess("base-task")
        assert result is None

    def test_get_all_processes(self, orchestrator: ConcreteOrchestrator):
        """Test getting all tracked processes."""
        info1 = SubprocessInfo(
            task_id="task-1",
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier="spec-1",
            auth_token="token-1",
        )
        info2 = SubprocessInfo(
            task_id="task-2",
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier="spec-2",
            auth_token="token-2",
        )

        orchestrator.track_process(info1)
        orchestrator.track_process(info2)

        all_processes = orchestrator.get_all_processes()
        assert len(all_processes) == 2
        assert "task-1" in all_processes
        assert "task-2" in all_processes

    def test_get_all_subprocesses(self, orchestrator: ConcreteOrchestrator):
        """Test getting all tracked subprocesses."""
        subprocess_info = SubprocessInfo(
            task_id="subprocess-task",
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier="spec-1",
            auth_token="token-1",
        )
        base_info = LaunchedProcess(
            task_id="base-task",
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier="spec-2",
            auth_token="token-2",
        )

        orchestrator.track_process(subprocess_info)
        orchestrator.track_process(base_info)

        subprocesses = orchestrator.get_all_subprocesses()
        assert len(subprocesses) == 1
        assert "subprocess-task" in subprocesses
        assert "base-task" not in subprocesses

    def test_process_count(self, orchestrator: ConcreteOrchestrator):
        """Test process count property."""
        assert orchestrator.process_count == 0

        info1 = SubprocessInfo(
            task_id="task-1",
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier="spec-1",
            auth_token="token-1",
        )
        orchestrator.track_process(info1)
        assert orchestrator.process_count == 1

        info2 = SubprocessInfo(
            task_id="task-2",
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier="spec-2",
            auth_token="token-2",
        )
        orchestrator.track_process(info2)
        assert orchestrator.process_count == 2

    def test_active_subprocess_count(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_long_running_script: Path,
    ):
        """Test active_subprocess_count property."""
        assert orchestrator.active_subprocess_count == 0

        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_long_running_script,
        )

        try:
            assert result is not None
            # Give process time to start
            time.sleep(0.1)
            assert orchestrator.active_subprocess_count == 1

            # Terminate the process
            result.process.terminate()
            result.process.wait(timeout=2)
            assert orchestrator.active_subprocess_count == 0
        finally:
            if result and result.process:
                result.process.kill()


# =============================================================================
# Process Termination Tests
# =============================================================================

class TestProcessTermination:
    """Tests for process termination methods."""

    def test_terminate_process_already_terminated(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test terminating an already terminated process."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
        )

        try:
            assert result is not None
            # Wait for process to complete naturally
            result.process.wait(timeout=5)

            # Should handle gracefully without error
            orchestrator.terminate_process(result)
        finally:
            if result and result.process:
                result.process.kill()

    def test_terminate_process_graceful(
        self,
        orchestrator_with_timeouts: ConcreteOrchestrator,
        temp_long_running_script: Path,
    ):
        """Test graceful process termination."""
        result = orchestrator_with_timeouts.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_long_running_script,
        )

        try:
            assert result is not None
            # Give process time to start
            time.sleep(0.1)
            assert result.is_running()

            # Terminate should work gracefully
            orchestrator_with_timeouts.terminate_process(result)

            # Process should be terminated
            assert result.returncode is not None
        finally:
            if result and result.process:
                result.process.kill()

    def test_terminate_process_no_process(
        self,
        orchestrator: ConcreteOrchestrator,
    ):
        """Test terminate_process with SubprocessInfo without a process."""
        info = SubprocessInfo(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
            process=None,
        )

        # Should handle gracefully without error
        orchestrator.terminate_process(info)

    def test_terminate_process_non_subprocess_info(
        self,
        orchestrator: ConcreteOrchestrator,
    ):
        """Test terminate_process with non-SubprocessInfo."""
        base_info = LaunchedProcess(
            task_id=TEST_TASK_ID,
            workspace=TEST_WORKSPACE,
            implementation=TEST_IMPLEMENTATION,
            specifier=TEST_SPECIFIER,
            auth_token="token-123",
        )

        # Should handle gracefully without error (no-op for non-SubprocessInfo)
        orchestrator.terminate_process(base_info)


# =============================================================================
# Check Processes Tests
# =============================================================================

class TestCheckProcesses:
    """Tests for process health checking."""

    def test_check_processes_removes_exited(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test that _check_processes removes exited processes."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
        )

        try:
            assert result is not None
            # Wait for process to complete
            result.process.wait(timeout=5)

            # Check processes should clean up
            orchestrator._check_processes()

            # Process should be removed from tracking
            assert orchestrator.get_process(TEST_TASK_ID) is None
        finally:
            if result and result.process:
                result.process.kill()

    def test_check_processes_calls_cleanup(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_script: Path,
    ):
        """Test that _check_processes calls cleanup_process."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_script,
        )

        try:
            assert result is not None
            # Wait for process to complete
            result.process.wait(timeout=5)

            # Check processes
            orchestrator._check_processes()

            # cleanup_process should have been called
            assert len(orchestrator._cleanup_calls) == 1
            assert orchestrator._cleanup_calls[0] is result
        finally:
            if result and result.process:
                result.process.kill()

    def test_check_processes_ignores_running(
        self,
        orchestrator: ConcreteOrchestrator,
        temp_long_running_script: Path,
    ):
        """Test that _check_processes ignores running processes."""
        result = orchestrator.spawn_subprocess(
            task_id=TEST_TASK_ID,
            script_path=temp_long_running_script,
        )

        try:
            assert result is not None
            # Give process time to start
            time.sleep(0.1)

            # Check processes
            orchestrator._check_processes()

            # Process should still be tracked
            assert orchestrator.get_process(TEST_TASK_ID) is result
        finally:
            if result and result.process:
                result.process.kill()


# =============================================================================
# Lifecycle Tests
# =============================================================================

class TestOrchestratorLifecycle:
    """Tests for orchestrator lifecycle methods."""

    def test_is_shutdown_requested(self, orchestrator: ConcreteOrchestrator):
        """Test is_shutdown_requested property."""
        assert orchestrator.is_shutdown_requested is False

        orchestrator.request_shutdown()

        assert orchestrator.is_shutdown_requested is True

    def test_request_shutdown(self, orchestrator: ConcreteOrchestrator):
        """Test request_shutdown method."""
        orchestrator.request_shutdown()
        assert orchestrator._shutdown_requested is True


# =============================================================================
# Logging Tests
# =============================================================================

class TestOrchestratorLogging:
    """Tests for orchestrator logging methods."""

    def test_log_method(self, orchestrator: ConcreteOrchestrator):
        """Test the public log method."""
        orchestrator.log("Test message")

        assert any("Test message" in msg for msg in orchestrator._logged_messages)

    def test_log_with_prefix(self, orchestrator: ConcreteOrchestrator):
        """Test internal _log with custom prefix."""
        orchestrator._log("Test message", prefix="CustomPrefix")

        assert any("CustomPrefix" in msg and "Test message" in msg
                   for msg in orchestrator._logged_messages)
