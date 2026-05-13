"""
Orchestrator package for managing agent and task lifecycle.

This package provides base classes and implementations for orchestrators
that handle task assignments from the Aether gateway and spawn/manage
agent processes.

Classes:
    BaseOrchestrator: Abstract base class defining the orchestrator interface.
    LaunchedProcess: Dataclass tracking a launched agent/task process.
    MultiprocessOrchestrator: Orchestrator that spawns agents as subprocesses.
    SubprocessInfo: Extended process info for subprocess-based agents.

Example:
    from scitrera_aether_client.orchestrator import BaseOrchestrator

    class MyOrchestrator(BaseOrchestrator):
        def handle_assignment(self, assignment):
            # Launch your agent based on the assignment
            pass

        def get_supported_profiles(self):
            return ["my-profile"]

        def get_implementation(self):
            return "my-orchestrator"

Example using MultiprocessOrchestrator:
    from scitrera_aether_client.orchestrator import MultiprocessOrchestrator

    class MyOrchestrator(MultiprocessOrchestrator):
        def get_implementation(self):
            return "my-orchestrator"

        def get_supported_profiles(self):
            return ["my-profile"]

        def handle_assignment(self, assignment):
            self.spawn_subprocess(
                task_id=assignment.task_id,
                script_path="/path/to/agent.py",
                workspace=assignment.workspace,
                implementation=assignment.target_implementation,
                specifier=assignment.specifier,
            )
"""

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ..proto import aether_pb2

logger = logging.getLogger("aether.orchestrator")


@dataclass
class LaunchedProcess:
    """
    Tracks a launched agent or task process.

    This dataclass stores information about a process that was spawned
    by an orchestrator in response to a task assignment.

    Attributes:
        task_id: The unique identifier of the task assignment.
        workspace: The workspace the agent is operating in.
        implementation: The implementation type of the agent.
        specifier: The unique specifier for this agent instance.
        auth_token: Authentication token for the agent.
        started_at: Timestamp when the process was launched.
        process: The underlying process object (type depends on implementation).
        metadata: Additional metadata about the launched process.
    """
    task_id: str
    workspace: str
    implementation: str
    specifier: str
    auth_token: str
    started_at: datetime = field(default_factory=datetime.now)
    process: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseOrchestrator(ABC):
    """
    Abstract base class for Aether orchestrators.

    An orchestrator is responsible for:
    - Connecting to the Aether gateway as an orchestrator principal
    - Receiving task assignments for profiles it supports
    - Spawning and managing agent/task processes
    - Handling lifecycle events (connection, disconnection, errors)
    - Graceful shutdown of managed processes

    Subclasses must implement:
    - get_implementation(): Returns the orchestrator implementation identifier
    - get_supported_profiles(): Returns list of profiles this orchestrator handles
    - handle_assignment(): Processes a task assignment and spawns an agent

    Subclasses may override:
    - on_connect(): Called when connected to the gateway
    - on_disconnect(): Called when disconnected from the gateway
    - on_error(): Called when an error occurs
    - on_message(): Called when a message is received
    - cleanup_process(): Called to clean up a terminated process

    Example:
        class MyOrchestrator(BaseOrchestrator):
            def get_implementation(self):
                return "my-orchestrator"

            def get_supported_profiles(self):
                return ["my-profile"]

            def handle_assignment(self, assignment):
                # Spawn agent process based on assignment
                process_info = spawn_my_agent(assignment)
                self.track_process(process_info)
    """

    def __init__(
        self,
        gateway: str = "localhost:50051",
        *,
        specifier: Optional[str] = None,
        tls_enabled: bool = False,
        tls_root_cert: Optional[bytes] = None,
        tls_root_cert_path: Optional[str] = None,
        tls_client_cert: Optional[bytes] = None,
        tls_client_cert_path: Optional[str] = None,
        tls_client_key: Optional[bytes] = None,
        tls_client_key_path: Optional[str] = None,
    ):
        """
        Initialize the base orchestrator.

        Args:
            gateway: Gateway address in host:port format.
            specifier: Optional unique specifier for this orchestrator instance.
            tls_enabled: Whether to use TLS for the connection.
            tls_root_cert: PEM-encoded root CA certificate bytes.
            tls_root_cert_path: Path to root CA certificate file.
            tls_client_cert: PEM-encoded client certificate bytes (for mTLS).
            tls_client_cert_path: Path to client certificate file.
            tls_client_key: PEM-encoded client private key bytes (for mTLS).
            tls_client_key_path: Path to client private key file.
        """
        # Import here to avoid circular imports
        from ..client import OrchestratorClient

        self.gateway = gateway
        self._specifier = specifier
        self._tls_enabled = tls_enabled
        self._tls_root_cert = tls_root_cert
        self._tls_root_cert_path = tls_root_cert_path
        self._tls_client_cert = tls_client_cert
        self._tls_client_cert_path = tls_client_cert_path
        self._tls_client_key = tls_client_key
        self._tls_client_key_path = tls_client_key_path

        # Create the underlying client
        self.client: OrchestratorClient = OrchestratorClient(
            implementation=self.get_implementation(),
            supported_profiles=self.get_supported_profiles(),
            specifier=specifier,
            tls_enabled=tls_enabled,
            tls_root_cert=tls_root_cert,
            tls_root_cert_path=tls_root_cert_path,
            tls_client_cert=tls_client_cert,
            tls_client_cert_path=tls_client_cert_path,
            tls_client_key=tls_client_key,
            tls_client_key_path=tls_client_key_path,
        )

        # Process tracking
        self._processes: Dict[str, LaunchedProcess] = {}
        self._processes_lock = threading.Lock()

        # Shutdown flag
        self._shutdown_requested = False

        # Setup callback handlers
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Configure client callback handlers."""
        self.client.on_task_assignment = self._on_task_assignment
        self.client.on_message = self._on_message_wrapper
        self.client.on_connect = self._on_connect_wrapper
        self.client.on_disconnect = self._on_disconnect_wrapper
        self.client.on_error = self._on_error_wrapper

    def _on_connect_wrapper(self) -> None:
        """Wrapper for connect callback."""
        self.on_connect()

    def _on_disconnect_wrapper(self, reason: str) -> None:
        """Wrapper for disconnect callback."""
        self.on_disconnect(reason)

    def _on_error_wrapper(self, error: aether_pb2.ErrorResponse) -> None:
        """Wrapper for error callback."""
        self.on_error(error)

    def _on_message_wrapper(self, msg: aether_pb2.IncomingMessage) -> None:
        """Wrapper for message callback."""
        self.on_message(msg)

    def _on_task_assignment(self, assignment: aether_pb2.TaskAssignment) -> None:
        """
        Internal handler for task assignments.

        Logs the assignment and delegates to the abstract handle_assignment method.
        """
        self._log("Task assignment received:")
        self._log(f"  Task ID: {assignment.task_id}")
        self._log(f"  Task Type: {assignment.task_type}")
        self._log(f"  Profile: {assignment.profile}")
        self._log(f"  Target Implementation: {assignment.target_implementation}")
        self._log(f"  Workspace: {assignment.workspace}")
        self._log(f"  Specifier: {assignment.specifier}")

        try:
            self.handle_assignment(assignment)
        except Exception as e:
            self._log("Error handling assignment: %s" % e, level=logging.ERROR)

    # =========================================================================
    # Abstract Methods (must be implemented by subclasses)
    # =========================================================================

    @abstractmethod
    def get_implementation(self) -> str:
        """
        Return the orchestrator implementation identifier.

        This identifier is used to register the orchestrator with the gateway
        and should be unique per orchestrator type.

        Returns:
            A string identifying this orchestrator implementation.

        Example:
            return "my-company.my-orchestrator"
        """
        ...

    @abstractmethod
    def get_supported_profiles(self) -> List[str]:
        """
        Return the list of profiles this orchestrator supports.

        Profiles define the types of agents/tasks this orchestrator can spawn.
        The gateway will route task assignments to orchestrators based on
        matching profiles.

        Returns:
            A list of profile strings this orchestrator can handle.

        Example:
            return ["echo", "worker", "gpu-agent"]
        """
        ...

    @abstractmethod
    def handle_assignment(self, assignment: aether_pb2.TaskAssignment) -> None:
        """
        Handle a task assignment by spawning an appropriate agent.

        This method is called when the gateway sends a task assignment.
        Implementations should:
        1. Extract relevant configuration from the assignment
        2. Spawn an agent process (subprocess, container, etc.)
        3. Track the spawned process using track_process()

        Args:
            assignment: The task assignment from the gateway containing:
                - task_id: Unique identifier for this task
                - task_type: Type of task to perform
                - profile: The profile that matched this orchestrator
                - target_implementation: Implementation type for the agent
                - workspace: Workspace the agent should operate in
                - specifier: Unique specifier for the agent
                - launch_params: Additional parameters for launching
                - metadata: Task metadata

        Raises:
            Exception: If the assignment cannot be handled.

        Example:
            def handle_assignment(self, assignment):
                process = subprocess.Popen(...)
                self.track_process(LaunchedProcess(
                    task_id=assignment.task_id,
                    workspace=assignment.workspace,
                    ...
                    process=process,
                ))
        """
        ...

    # =========================================================================
    # Lifecycle Hooks (can be overridden by subclasses)
    # =========================================================================

    def on_connect(self) -> None:
        """
        Called when the orchestrator connects to the gateway.

        Override this method to perform actions when the connection
        is established (e.g., logging, initialization).
        """
        self._log("Connected to gateway")

    def on_disconnect(self, reason: str) -> None:
        """
        Called when the orchestrator disconnects from the gateway.

        Override this method to handle disconnection events.

        Args:
            reason: Description of why the disconnection occurred.
        """
        self._log(f"Disconnected: {reason}")

    def on_error(self, error: aether_pb2.ErrorResponse) -> None:
        """
        Called when an error is received from the gateway.

        Override this method to handle error responses.

        Args:
            error: The error response from the gateway.
        """
        self._log("code=%s, message=%s" % (error.code, error.message), level=logging.ERROR)

    def on_message(self, msg: aether_pb2.IncomingMessage) -> None:
        """
        Called when a message is received from the gateway.

        Override this method to handle incoming messages.

        Args:
            msg: The incoming message from the gateway.
        """
        self._log(f"Message from {msg.source_topic}: {len(msg.payload)} bytes")

    def cleanup_process(self, process: LaunchedProcess) -> None:
        """
        Clean up a terminated process.

        Called when a process is detected as terminated. Override this
        method to perform custom cleanup (e.g., logging, resource release).

        The default implementation does nothing.

        Args:
            process: The launched process that has terminated.
        """
        pass

    # =========================================================================
    # Process Tracking
    # =========================================================================

    def track_process(self, process: LaunchedProcess) -> None:
        """
        Register a launched process for tracking.

        Call this method after spawning a new agent process to track it
        for lifecycle management and cleanup.

        Args:
            process: The launched process information to track.
        """
        with self._processes_lock:
            self._processes[process.task_id] = process
        self._log(f"Tracking process for task {process.task_id}")

    def untrack_process(self, task_id: str) -> Optional[LaunchedProcess]:
        """
        Remove a process from tracking.

        Args:
            task_id: The task ID of the process to untrack.

        Returns:
            The untracked process info, or None if not found.
        """
        with self._processes_lock:
            return self._processes.pop(task_id, None)

    def get_process(self, task_id: str) -> Optional[LaunchedProcess]:
        """
        Get a tracked process by task ID.

        Args:
            task_id: The task ID to look up.

        Returns:
            The process info, or None if not found.
        """
        with self._processes_lock:
            return self._processes.get(task_id)

    def get_all_processes(self) -> Dict[str, LaunchedProcess]:
        """
        Get a copy of all tracked processes.

        Returns:
            A dictionary mapping task IDs to process info.
        """
        with self._processes_lock:
            return dict(self._processes)

    @property
    def process_count(self) -> int:
        """Return the number of tracked processes."""
        with self._processes_lock:
            return len(self._processes)

    # =========================================================================
    # Logging
    # =========================================================================

    def _log(self, message: str, prefix: Optional[str] = None, level: int = logging.INFO) -> None:
        """
        Log a message using the ``aether.orchestrator`` logger.

        Args:
            message: The message to log.
            prefix: Optional prefix (defaults to "Orchestrator").
            level: Logging level (default ``logging.INFO``).
        """
        prefix = prefix or "Orchestrator"
        logger.log(level, "[%s] %s", prefix, message)

    def log(self, message: str) -> None:
        """
        Public logging method for subclasses.

        Args:
            message: The message to log.
        """
        self._log(message)

    # =========================================================================
    # Lifecycle Management
    # =========================================================================

    @property
    def is_running(self) -> bool:
        """Return True if the client connection is active."""
        return self.client.is_running

    @property
    def is_shutdown_requested(self) -> bool:
        """Return True if shutdown has been requested."""
        return self._shutdown_requested

    def connect(self, gateway: Optional[str] = None) -> None:
        """
        Connect to the Aether gateway.

        Args:
            gateway: Optional gateway address override.
        """
        target = gateway or self.gateway
        self._log(f"Connecting to gateway at {target}...")
        self.client.connect(target)

    def close(self) -> None:
        """Close the gateway connection."""
        self._shutdown_requested = True
        self.client.close()
        self._log("Connection closed")

    def request_shutdown(self) -> None:
        """Request a graceful shutdown of the orchestrator."""
        self._shutdown_requested = True

    def run(self) -> None:
        """
        Run the orchestrator main loop.

        This is a blocking method that:
        1. Connects to the gateway
        2. Enters a main loop processing events
        3. Handles graceful shutdown on KeyboardInterrupt

        Subclasses should override _run_loop() if they need custom
        main loop behavior.

        Raises:
            Exception: If connection fails.
        """
        self._log(f"Starting orchestrator: {self.get_implementation()}")
        self._log(f"Supported profiles: {self.get_supported_profiles()}")

        self.connect()

        try:
            self._run_loop()
        except KeyboardInterrupt:
            self._log("Shutdown requested (KeyboardInterrupt)")
        finally:
            self._shutdown()

    def _run_loop(self) -> None:
        """
        Main event loop for the orchestrator.

        Override this method to implement custom main loop behavior.
        The default implementation sleeps until shutdown is requested.
        """
        import time

        while self.is_running and not self._shutdown_requested:
            self._check_processes()
            time.sleep(0.1)

    async def run_async(self) -> None:
        """Async variant of :meth:`run`.

        Connects to the gateway and enters an async main loop, allowing
        subclasses to run coroutines (e.g. web servers, periodic tasks)
        concurrently via ``asyncio.create_task``.

        Subclasses should override :meth:`_async_run_loop` for custom
        async behavior.
        """
        import asyncio

        self._log(f"Starting orchestrator: {self.get_implementation()}")
        self._log(f"Supported profiles: {self.get_supported_profiles()}")

        self.connect()

        try:
            await self._async_run_loop()
        except (KeyboardInterrupt, asyncio.CancelledError):
            self._log("Shutdown requested")
        finally:
            self._shutdown()

    async def _async_run_loop(self) -> None:
        """Async main event loop for the orchestrator.

        Override this method to implement custom async main loop behavior.
        The default implementation mirrors ``_run_loop`` but uses
        ``asyncio.sleep`` so other coroutines can run concurrently.
        """
        import asyncio

        while self.is_running and not self._shutdown_requested:
            self._check_processes()
            await asyncio.sleep(0.1)

    def _check_processes(self) -> None:
        """
        Check status of tracked processes.

        Override this method to implement process health checking
        and cleanup. The default implementation does nothing.
        """
        pass

    def _shutdown(self) -> None:
        """
        Perform graceful shutdown.

        This method:
        1. Signals all tracked processes to terminate
        2. Waits for processes to exit (with timeout)
        3. Closes the gateway connection

        Override terminate_process() to customize process termination.
        """
        self._log("Shutting down...")
        self._shutdown_requested = True

        # Terminate all tracked processes
        processes = self.get_all_processes()
        for task_id, process in processes.items():
            self._log(f"Terminating process for task {task_id}...")
            try:
                self.terminate_process(process)
            except Exception as e:
                self._log(f"Error terminating process {task_id}: {e}")

        # Close the client connection
        self.close()
        self._log("Orchestrator stopped")

    def terminate_process(self, process: LaunchedProcess) -> None:
        """
        Terminate a tracked process.

        Override this method to implement custom process termination
        (e.g., sending SIGTERM, calling container stop, etc.).

        The default implementation does nothing.

        Args:
            process: The process to terminate.
        """
        pass


# Import from submodules for convenience
from .multiprocess import MultiprocessOrchestrator, SubprocessInfo

# Exports
__all__ = [
    "BaseOrchestrator",
    "LaunchedProcess",
    "MultiprocessOrchestrator",
    "SubprocessInfo",
]
