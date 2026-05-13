"""
Unit tests for the synchronous Aether client.

This module tests:
- Client initialization for all client types
- BaseAetherClient functionality
- Message sending methods
- KV operations
- Checkpoint operations
- Task creation
- Error handling
- TLS configuration
- Context manager usage
"""
import queue
import threading
import time
from typing import Dict, List
from unittest.mock import MagicMock, patch

import grpc
import pytest

from scitrera_aether_client.client import (
    AgentClient,
    BaseAetherClient,
    MetricsBridgeClient,
    OrchestratorClient,
    TaskClient,
    UserClient,
    WorkflowEngineClient,
)
from scitrera_aether_client._common import (
    CHAT,
    CONTROL,
    EVENT,
    METRIC,
    OPAQUE,
    SELF_ASSIGN,
    TARGETED,
    create_topic_agent,
    create_topic_task,
    create_topic_user,
    create_topic_global_agents,
    create_topic_global_users,
)
from scitrera_aether_client.exceptions import (
    AuthenticationError,
    ConnectionError,
    InvalidArgumentError,
)
from scitrera_aether_client.proto import aether_pb2


# =============================================================================
# BaseAetherClient Tests
# =============================================================================

class TestBaseAetherClientInit:
    """Tests for BaseAetherClient initialization."""

    def test_default_initialization(self):
        """Test default initialization values."""
        client = BaseAetherClient()

        assert client.max_retries == 5
        assert client.initial_backoff == 1.0
        assert client.max_backoff == 30.0
        assert client.backoff_multiplier == 2.0
        assert client.auto_reconnect is True
        assert client.tls_enabled is False
        assert client.target is None
        assert client.channel is None
        assert client.stub is None
        assert client._session_id is None

    def test_custom_initialization(self):
        """Test custom initialization values."""
        client = BaseAetherClient(
            max_retries=10,
            initial_backoff=0.5,
            max_backoff=60.0,
            backoff_multiplier=1.5,
            auto_reconnect=False,
            tls_enabled=True,
        )

        assert client.max_retries == 10
        assert client.initial_backoff == 0.5
        assert client.max_backoff == 60.0
        assert client.backoff_multiplier == 1.5
        assert client.auto_reconnect is False
        assert client.tls_enabled is True

    def test_tls_configuration(
        self,
        mock_tls_root_cert: bytes,
        mock_tls_client_cert: bytes,
        mock_tls_client_key: bytes,
    ):
        """Test TLS configuration with certificates."""
        client = BaseAetherClient(
            tls_enabled=True,
            tls_root_cert=mock_tls_root_cert,
            tls_client_cert=mock_tls_client_cert,
            tls_client_key=mock_tls_client_key,
        )

        assert client.tls_enabled is True
        assert client._tls_root_cert == mock_tls_root_cert
        assert client._tls_client_cert == mock_tls_client_cert
        assert client._tls_client_key == mock_tls_client_key

    def test_callback_defaults_to_none(self):
        """Test that callbacks default to None."""
        client = BaseAetherClient()

        assert client.on_message is None
        assert client.on_config is None
        assert client.on_signal is None
        assert client.on_error is None
        assert client.on_kv_response is None
        assert client.on_task_assignment is None
        assert client.on_checkpoint_response is None
        assert client.on_connect is None
        assert client.on_disconnect is None


class TestBaseAetherClientIsRunning:
    """Tests for the is_running property."""

    def test_is_running_initially_true(self):
        """Test that is_running is True initially (stop event not set)."""
        client = BaseAetherClient()
        assert client.is_running is True

    def test_is_running_false_after_stop(self):
        """Test that is_running is False after stop event is set."""
        client = BaseAetherClient()
        client._stop_event.set()
        assert client.is_running is False

    def test_is_running_true_during_reconnect(self):
        """Test that is_running is True during reconnection."""
        client = BaseAetherClient()
        client._stop_event.set()
        client._reconnecting = True
        assert client.is_running is True


class TestBaseAetherClientBackoff:
    """Tests for backoff calculation."""

    def test_calculate_backoff_first_attempt(self):
        """First reconnect attempt is zero-delay (see async client test)."""
        client = BaseAetherClient(initial_backoff=1.0, max_backoff=30.0, backoff_multiplier=2.0)
        assert client._calculate_backoff(0) == 0.0

    def test_calculate_backoff_multiple_attempts(self):
        """Test backoff increases with attempts."""
        client = BaseAetherClient(initial_backoff=1.0, max_backoff=30.0, backoff_multiplier=2.0)

        # Second attempt (attempt=1) should be around 2.0 +/- 25%
        backoff = client._calculate_backoff(1)
        assert 1.5 <= backoff <= 2.5

        # Third attempt (attempt=2) should be around 4.0 +/- 25%
        backoff = client._calculate_backoff(2)
        assert 3.0 <= backoff <= 5.0

    def test_calculate_backoff_respects_max(self):
        """Test backoff doesn't exceed max_backoff."""
        client = BaseAetherClient(initial_backoff=1.0, max_backoff=10.0, backoff_multiplier=2.0)

        # Attempt 10 would be 1024 without cap, but should be capped at 10 +/- 25%
        backoff = client._calculate_backoff(10)
        assert 7.5 <= backoff <= 12.5


class MockGrpcError(grpc.RpcError):
    """Mock gRPC error class for testing."""

    def __init__(self, code: grpc.StatusCode, details: str = ""):
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class TestBaseAetherClientRecoverableError:
    """Tests for recoverable error checking."""

    def test_grpc_unavailable_is_recoverable(self):
        """Test that UNAVAILABLE is recoverable."""
        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Service unavailable")

        client = BaseAetherClient()
        assert client._is_recoverable_error(error) is True

    def test_grpc_permission_denied_not_recoverable(self):
        """Test that PERMISSION_DENIED is not recoverable."""
        error = MockGrpcError(grpc.StatusCode.PERMISSION_DENIED, "Permission denied")

        client = BaseAetherClient()
        assert client._is_recoverable_error(error) is False

    def test_grpc_unauthenticated_not_recoverable(self):
        """Test that UNAUTHENTICATED is not recoverable."""
        error = MockGrpcError(grpc.StatusCode.UNAUTHENTICATED, "Unauthenticated")

        client = BaseAetherClient()
        assert client._is_recoverable_error(error) is False

    def test_grpc_already_exists_not_recoverable(self):
        """Test that ALREADY_EXISTS is not recoverable."""
        error = MockGrpcError(grpc.StatusCode.ALREADY_EXISTS, "Already exists")

        client = BaseAetherClient()
        assert client._is_recoverable_error(error) is False

    def test_aether_authentication_error_not_recoverable(self):
        """Test that AuthenticationError is not recoverable."""
        error = AuthenticationError(message="test")
        client = BaseAetherClient()
        assert client._is_recoverable_error(error) is False


class TestBaseAetherClientReconnect:
    """Regression tests for reconnect flow.

    These tests guard against a prior bug where ``_listen_loop`` pre-set
    ``self._reconnecting = True`` before calling ``_attempt_reconnect``,
    which then short-circuited at its ``if self._reconnecting: return``
    guard — so the reconnect loop never ran while the client looked alive
    (``is_running`` stayed True because ``_reconnecting`` was stuck True).
    """

    def _run_listen_loop_with_error(
        self,
        client: BaseAetherClient,
        error: grpc.RpcError,
    ) -> MagicMock:
        """Drive ``_listen_loop`` once with a stream that raises ``error``.

        Returns the ``_attempt_reconnect`` mock so callers can assert against
        call count / state snapshots captured at call time.
        """

        def _raising_responses():
            raise error
            yield  # pragma: no cover — marker for generator semantics

        snapshots: List[Dict[str, bool]] = []

        def _fake_attempt_reconnect():
            # Capture state at the moment reconnect is invoked so we can
            # prove the guard flag wasn't pre-set by the listen loop.
            snapshots.append({"_reconnecting": client._reconnecting})

        mock = MagicMock(side_effect=_fake_attempt_reconnect)
        mock.snapshots = snapshots
        client._attempt_reconnect = mock
        client._listen_loop(_raising_responses())
        return mock

    def test_listen_loop_triggers_reconnect_on_recoverable_error(self):
        """Recoverable grpc errors must invoke ``_attempt_reconnect``."""
        client = BaseAetherClient(auto_reconnect=True)
        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Socket closed")

        mock = self._run_listen_loop_with_error(client, error)

        assert mock.call_count == 1, (
            "auto_reconnect=True + recoverable error must call _attempt_reconnect"
        )

    def test_listen_loop_does_not_preset_reconnecting_flag(self):
        """``_reconnecting`` must be False when ``_attempt_reconnect`` is invoked.

        Pre-setting ``self._reconnecting = True`` in ``_listen_loop`` would
        cause ``_attempt_reconnect`` to short-circuit at its re-entry guard,
        leaving the client in a silent "reconnecting forever" state with no
        actual reconnect attempted (the observable symptom of the original
        bug: orchestrator process alive, but absent from the gateway's
        profile index).
        """
        client = BaseAetherClient(auto_reconnect=True)
        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Socket closed")

        mock = self._run_listen_loop_with_error(client, error)

        assert mock.snapshots == [{"_reconnecting": False}], (
            "_listen_loop must not pre-set _reconnecting; that bit is owned "
            "by _attempt_reconnect itself (past its re-entry guard)."
        )

    def test_listen_loop_skips_reconnect_when_auto_reconnect_disabled(self):
        """When auto_reconnect=False, reconnect must not fire."""
        client = BaseAetherClient(auto_reconnect=False)
        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Socket closed")

        mock = self._run_listen_loop_with_error(client, error)

        assert mock.call_count == 0

    def test_listen_loop_skips_reconnect_on_non_recoverable_error(self):
        """Non-recoverable errors (e.g., PERMISSION_DENIED) must not reconnect."""
        client = BaseAetherClient(auto_reconnect=True)
        error = MockGrpcError(grpc.StatusCode.PERMISSION_DENIED, "denied")

        mock = self._run_listen_loop_with_error(client, error)

        assert mock.call_count == 0

    def test_listen_loop_fires_on_disconnect_when_not_reconnecting(self):
        """If no reconnect is queued, on_disconnect must fire once."""
        client = BaseAetherClient(auto_reconnect=False)
        disconnects: List[str] = []
        client.on_disconnect = lambda reason: disconnects.append(reason)

        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Socket closed")
        self._run_listen_loop_with_error(client, error)

        assert disconnects == ["connection lost"]

    def test_listen_loop_suppresses_on_disconnect_when_reconnecting(self):
        """When a reconnect is queued, on_disconnect must NOT fire.

        The new connection's on_connect will fire instead; firing both
        would confuse state machines observing connection lifecycle.
        """
        client = BaseAetherClient(auto_reconnect=True)
        disconnects: List[str] = []
        client.on_disconnect = lambda reason: disconnects.append(reason)

        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Socket closed")
        self._run_listen_loop_with_error(client, error)

        assert disconnects == []


class TestBaseAetherClientClose:
    """Tests for close method."""

    def test_close_sets_stop_event(self):
        """Test that close sets the stop event."""
        client = BaseAetherClient()
        client._stop_event.clear()

        client.close()

        assert client._stop_event.is_set()

    def test_close_puts_sentinel_in_queue(self):
        """Test that close puts None sentinel in request queue."""
        client = BaseAetherClient()

        client.close()

        assert client.request_queue.get_nowait() is None

    def test_close_closes_channel(self):
        """Test that close closes the gRPC channel."""
        client = BaseAetherClient()
        mock_channel = MagicMock()
        client.channel = mock_channel

        client.close()

        mock_channel.close.assert_called_once()


class TestBaseAetherClientContextManager:
    """Tests for context manager usage."""

    def test_context_manager_enter_returns_client(self):
        """Test that __enter__ returns the client."""
        client = BaseAetherClient()

        with client as c:
            assert c is client

    def test_context_manager_exit_calls_close(self):
        """Test that __exit__ calls close."""
        client = BaseAetherClient()

        with patch.object(client, 'close') as mock_close:
            with client:
                pass
            mock_close.assert_called_once()


class TestBaseAetherClientKVOperations:
    """Tests for KV operations."""

    def test_kv_get(self):
        """Test KV get operation."""
        client = BaseAetherClient()

        client.kv_get("test_key", scope="global")

        msg = client.request_queue.get_nowait()
        assert msg.HasField("kv_op")
        assert msg.kv_op.op == aether_pb2.KVOperation.GET
        assert msg.kv_op.key == "test_key"
        assert msg.kv_op.scope == aether_pb2.KVOperation.GLOBAL

    def test_kv_put(self, test_kv_value: bytes):
        """Test KV put operation."""
        client = BaseAetherClient()

        client.kv_put("test_key", test_kv_value, scope="workspace", workspace="test-ws", ttl=3600)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("kv_op")
        assert msg.kv_op.op == aether_pb2.KVOperation.PUT
        assert msg.kv_op.key == "test_key"
        assert msg.kv_op.value == test_kv_value
        assert msg.kv_op.scope == aether_pb2.KVOperation.WORKSPACE
        assert msg.kv_op.workspace == "test-ws"
        assert msg.kv_op.ttl == 3600

    def test_kv_list(self):
        """Test KV list operation."""
        client = BaseAetherClient()

        client.kv_list("prefix_", scope="user", user_id="user-123")

        msg = client.request_queue.get_nowait()
        assert msg.HasField("kv_op")
        assert msg.kv_op.op == aether_pb2.KVOperation.LIST
        assert msg.kv_op.key == "prefix_"
        assert msg.kv_op.scope == aether_pb2.KVOperation.USER
        assert msg.kv_op.user_id == "user-123"

    def test_kv_delete(self):
        """Test KV delete operation."""
        client = BaseAetherClient()

        client.kv_delete("test_key", scope="user-workspace", user_id="user-123", workspace="test-ws")

        msg = client.request_queue.get_nowait()
        assert msg.HasField("kv_op")
        assert msg.kv_op.op == aether_pb2.KVOperation.DELETE
        assert msg.kv_op.key == "test_key"
        assert msg.kv_op.scope == aether_pb2.KVOperation.USER_WORKSPACE


class TestBaseAetherClientCheckpointOperations:
    """Tests for checkpoint operations."""

    def test_checkpoint_save(self, test_checkpoint_data: bytes):
        """Test checkpoint save operation."""
        client = BaseAetherClient()

        client.checkpoint_save(test_checkpoint_data, key="my_checkpoint", ttl=7200)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.SAVE
        assert msg.checkpoint_op.key == "my_checkpoint"
        assert msg.checkpoint_op.data == test_checkpoint_data
        assert msg.checkpoint_op.ttl == 7200

    def test_checkpoint_load(self):
        """Test checkpoint load operation."""
        client = BaseAetherClient()

        client.checkpoint_load(key="my_checkpoint")

        msg = client.request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.LOAD
        assert msg.checkpoint_op.key == "my_checkpoint"

    def test_checkpoint_delete(self):
        """Test checkpoint delete operation."""
        client = BaseAetherClient()

        client.checkpoint_delete(key="my_checkpoint")

        msg = client.request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.DELETE
        assert msg.checkpoint_op.key == "my_checkpoint"

    def test_checkpoint_list(self):
        """Test checkpoint list operation."""
        client = BaseAetherClient()

        client.checkpoint_list()

        msg = client.request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.LIST


class TestBaseAetherClientCreateTask:
    """Tests for task creation."""

    def test_create_task_self_assign(self):
        """Test task creation with self-assign mode."""
        client = BaseAetherClient()

        client.create_task(
            task_type="echo",
            workspace="test-workspace",
            metadata={"key": "value"},
        )

        msg = client.request_queue.get_nowait()
        assert msg.HasField("create_task")
        assert msg.create_task.task_type == "echo"
        assert msg.create_task.workspace == "test-workspace"
        assert msg.create_task.assignment_mode == SELF_ASSIGN
        assert msg.create_task.metadata["key"] == "value"

    def test_create_task_targeted(self):
        """Test task creation with targeted mode."""
        client = BaseAetherClient()

        client.create_task(
            task_type="process",
            workspace="test-workspace",
            target_agent_id="agent-123",
        )

        msg = client.request_queue.get_nowait()
        assert msg.HasField("create_task")
        assert msg.create_task.assignment_mode == TARGETED
        assert msg.create_task.target_agent_id == "agent-123"

    def test_create_task_with_launch_params(self):
        """Test task creation with launch parameter overrides."""
        client = BaseAetherClient()

        client.create_task(
            task_type="custom",
            workspace="test-workspace",
            launch_param_overrides={"cpu": "4", "memory": "8GB"},
        )

        msg = client.request_queue.get_nowait()
        assert msg.create_task.launch_param_overrides["cpu"] == "4"
        assert msg.create_task.launch_param_overrides["memory"] == "8GB"


class TestBaseAetherClientTaskQuery:
    """Tests for task query operations."""

    def test_query_tasks_list(self):
        """Test querying tasks with filters."""
        client = BaseAetherClient()

        # Call in a non-blocking way — just check the request was enqueued
        # We can't await response in sync without a server, so use a short timeout thread
        import threading

        def do_query():
            try:
                client.query_tasks(workspace="test-ws", status="running",
                                   task_type="echo", limit=10, offset=5, timeout=0.1)
            except Exception:
                pass

        t = threading.Thread(target=do_query)
        t.start()
        import time
        time.sleep(0.05)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("task_query")
        assert msg.task_query.op == aether_pb2.TaskQuery.LIST
        assert msg.task_query.filter.workspace == "test-ws"
        assert msg.task_query.filter.status == aether_pb2.TASK_STATUS_RUNNING
        assert msg.task_query.filter.task_type == "echo"
        assert msg.task_query.filter.limit == 10
        assert msg.task_query.filter.offset == 5

        t.join(timeout=1.0)

    def test_get_task_by_id(self):
        """Test getting a specific task by ID."""
        client = BaseAetherClient()

        import threading

        def do_get():
            try:
                client.get_task("task-123", timeout=0.1)
            except Exception:
                pass

        t = threading.Thread(target=do_get)
        t.start()
        import time
        time.sleep(0.05)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("task_query")
        assert msg.task_query.op == aether_pb2.TaskQuery.GET
        assert msg.task_query.task_id == "task-123"

        t.join(timeout=1.0)

    def test_cancel_task(self):
        """Test cancelling a task."""
        client = BaseAetherClient()

        import threading

        def do_cancel():
            try:
                client.cancel_task("task-456", reason="no longer needed", timeout=0.1)
            except Exception:
                pass

        t = threading.Thread(target=do_cancel)
        t.start()
        import time
        time.sleep(0.05)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.CANCEL
        assert msg.task_op.task_id == "task-456"
        assert msg.task_op.reason == "no longer needed"

        t.join(timeout=1.0)

    def test_retry_task(self):
        """Test retrying a task."""
        client = BaseAetherClient()

        import threading

        def do_retry():
            try:
                client.retry_task("task-789", timeout=0.1)
            except Exception:
                pass

        t = threading.Thread(target=do_retry)
        t.start()
        import time
        time.sleep(0.05)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.RETRY
        assert msg.task_op.task_id == "task-789"

        t.join(timeout=1.0)


class TestBaseAetherClientTLS:
    """Tests for TLS credential building."""

    def test_build_tls_credentials_basic(self, mock_tls_root_cert: bytes):
        """Test building basic TLS credentials."""
        client = BaseAetherClient(
            tls_enabled=True,
            tls_root_cert=mock_tls_root_cert,
        )

        with patch('grpc.ssl_channel_credentials') as mock_ssl:
            mock_ssl.return_value = MagicMock()
            _creds = client._build_tls_credentials()

            mock_ssl.assert_called_once_with(
                root_certificates=mock_tls_root_cert,
                private_key=None,
                certificate_chain=None,
            )

    def test_build_tls_credentials_mtls(
        self,
        mock_tls_root_cert: bytes,
        mock_tls_client_cert: bytes,
        mock_tls_client_key: bytes,
    ):
        """Test building mTLS credentials."""
        client = BaseAetherClient(
            tls_enabled=True,
            tls_root_cert=mock_tls_root_cert,
            tls_client_cert=mock_tls_client_cert,
            tls_client_key=mock_tls_client_key,
        )

        with patch('grpc.ssl_channel_credentials') as mock_ssl:
            mock_ssl.return_value = MagicMock()
            _creds = client._build_tls_credentials()

            mock_ssl.assert_called_once_with(
                root_certificates=mock_tls_root_cert,
                private_key=mock_tls_client_key,
                certificate_chain=mock_tls_client_cert,
            )

    def test_build_tls_credentials_cert_without_key_raises(self, mock_tls_client_cert: bytes):
        """Test that providing cert without key raises error."""
        client = BaseAetherClient(
            tls_enabled=True,
            tls_client_cert=mock_tls_client_cert,
        )

        with pytest.raises(InvalidArgumentError):
            client._build_tls_credentials()

    def test_build_tls_credentials_key_without_cert_raises(self, mock_tls_client_key: bytes):
        """Test that providing key without cert raises error."""
        client = BaseAetherClient(
            tls_enabled=True,
            tls_client_key=mock_tls_client_key,
        )

        with pytest.raises(InvalidArgumentError):
            client._build_tls_credentials()


# =============================================================================
# AgentClient Tests
# =============================================================================

class TestAgentClientInit:
    """Tests for AgentClient initialization."""

    def test_agent_client_init(
        self,
        test_workspace: str,
        test_implementation: str,
        test_specifier: str,
    ):
        """Test AgentClient initialization."""
        client = AgentClient(
            workspace=test_workspace,
            implementation=test_implementation,
            specifier=test_specifier,
        )

        assert client.workspace == test_workspace
        assert client.implementation == test_implementation
        assert client.specifier == test_specifier
        assert client.init is not None
        assert client.init.HasField("agent")

    def test_agent_client_with_credentials(
        self,
        test_workspace: str,
        test_implementation: str,
        test_specifier: str,
        test_credentials: Dict[str, str],
    ):
        """Test AgentClient initialization with credentials."""
        client = AgentClient(
            workspace=test_workspace,
            implementation=test_implementation,
            specifier=test_specifier,
            credentials=test_credentials,
        )

        assert client.init.credentials["token"] == test_credentials["token"]


class TestAgentClientMessaging:
    """Tests for AgentClient messaging methods."""

    def test_send_message_to_agent(self, agent_client: AgentClient, test_payload: bytes):
        """Test sending message to another agent."""
        agent_client.send_message_to_agent(
            workspace="other-ws",
            implementation="other-impl",
            specifier="other-spec",
            payload=test_payload,
        )

        msg = agent_client.request_queue.get_nowait()
        assert msg.HasField("send")
        assert msg.send.target_topic == "ag::other-ws::other-impl::other-spec"
        assert msg.send.payload == test_payload
        assert msg.send.message_type == OPAQUE

    def test_send_message_to_task(self, agent_client: AgentClient, test_payload: bytes):
        """Test sending message to a task."""
        agent_client.send_message_to_task(
            workspace="task-ws",
            implementation="task-impl",
            payload=test_payload,
            unique_specifier="task-spec",
        )

        msg = agent_client.request_queue.get_nowait()
        assert msg.send.target_topic == "tu::task-ws::task-impl::task-spec"

    def test_send_message_to_user_session(self, agent_client: AgentClient, test_payload: bytes):
        """Test sending message to a user session."""
        agent_client.send_message_to_user_session(
            user_id="user-123",
            window_id="window-456",
            payload=test_payload,
        )

        msg = agent_client.request_queue.get_nowait()
        assert msg.send.target_topic == "us::user-123::window-456"

    def test_send_broadcast_to_agents(self, agent_client: AgentClient, test_payload: bytes):
        """Test broadcasting to all agents in workspace."""
        agent_client.send_broadcast_to_agents(
            workspace="broadcast-ws",
            payload=test_payload,
        )

        msg = agent_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ga::broadcast-ws"

    def test_send_event(self, agent_client: AgentClient, test_payload: bytes):
        """Test sending event to workflow engine."""
        agent_client.send_event(test_payload)

        msg = agent_client.request_queue.get_nowait()
        assert msg.send.target_topic == "event::*"
        assert msg.send.message_type == EVENT

    def test_send_metric(self, agent_client: AgentClient, test_metric):
        """Test sending metric to metrics bridge."""
        agent_client.send_metric(test_metric)

        msg = agent_client.request_queue.get_nowait()
        assert msg.send.target_topic == "metric::*"
        assert msg.send.message_type == METRIC


class TestAgentClientWorkspace:
    """Tests for AgentClient workspace switching."""

    def test_switch_workspace(self, agent_client: AgentClient):
        """Test workspace switching."""
        agent_client.switch_workspace("new-workspace")

        assert agent_client.workspace == "new-workspace"

        msg = agent_client.request_queue.get_nowait()
        assert msg.HasField("switch_workspace")
        assert msg.switch_workspace.new_workspace_id == "new-workspace"


# =============================================================================
# TaskClient Tests
# =============================================================================

class TestTaskClientInit:
    """Tests for TaskClient initialization."""

    def test_task_client_with_unique_specifier(
        self,
        test_workspace: str,
        test_implementation: str,
    ):
        """Test TaskClient with unique specifier (named task)."""
        client = TaskClient(
            workspace=test_workspace,
            implementation=test_implementation,
            unique_specifier="named-task",
        )

        assert client.unique_specifier == "named-task"
        assert client.init.task.unique_specifier == "named-task"

    def test_task_client_without_unique_specifier(
        self,
        test_workspace: str,
        test_implementation: str,
    ):
        """Test TaskClient without unique specifier (non-unique task)."""
        client = TaskClient(
            workspace=test_workspace,
            implementation=test_implementation,
        )

        assert client.unique_specifier == ""
        assert client.init.task.unique_specifier == ""


class TestTaskClientMessaging:
    """Tests for TaskClient messaging methods."""

    def test_send_message_to_agent(self, task_client: TaskClient, test_payload: bytes):
        """Test sending message to an agent."""
        task_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = task_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"

    def test_send_event(self, task_client: TaskClient, test_payload: bytes):
        """Test sending event to workflow engine."""
        task_client.send_event(test_payload)

        msg = task_client.request_queue.get_nowait()
        assert msg.send.target_topic == "event::*"
        assert msg.send.message_type == EVENT


# =============================================================================
# UserClient Tests
# =============================================================================

class TestUserClientInit:
    """Tests for UserClient initialization."""

    def test_user_client_init(
        self,
        test_user_id: str,
        test_window_id: str,
    ):
        """Test UserClient initialization."""
        client = UserClient(
            user_id=test_user_id,
            window_id=test_window_id,
        )

        assert client.user_id == test_user_id
        assert client.window_id == test_window_id
        assert client.init.HasField("user")
        assert client.init.user.user_id == test_user_id
        assert client.init.user.window_id == test_window_id


class TestUserClientMessaging:
    """Tests for UserClient messaging methods."""

    def test_send_message_to_agent(self, user_client: UserClient, test_payload: bytes):
        """Test sending message to an agent."""
        user_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = user_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"

    def test_send_message_to_task(self, user_client: UserClient, test_payload: bytes):
        """Test sending message to a task."""
        user_client.send_message_to_task(
            workspace="task-ws",
            implementation="task-impl",
            payload=test_payload,
        )

        msg = user_client.request_queue.get_nowait()
        # Non-unique task topic
        assert msg.send.target_topic == "ta::task-ws::task-impl::"

    def test_send_message_to_agent_stamps_app_workspace(self, user_client: UserClient, test_payload: bytes):
        """Test that UserClient.send_message_to_agent stamps app_workspace on SendMessage."""
        user_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
            app_workspace="default",
        )

        msg = user_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"
        assert msg.send.app_workspace == "default"

    def test_send_message_to_agent_empty_app_workspace_by_default(self, user_client: UserClient, test_payload: bytes):
        """Test that app_workspace defaults to empty string when not supplied."""
        user_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = user_client.request_queue.get_nowait()
        assert msg.send.app_workspace == ""


# =============================================================================
# OrchestratorClient Tests
# =============================================================================

class TestOrchestratorClientInit:
    """Tests for OrchestratorClient initialization."""

    def test_orchestrator_client_init(
        self,
        test_implementation: str,
        test_profiles: List[str],
    ):
        """Test OrchestratorClient initialization."""
        client = OrchestratorClient(
            implementation=test_implementation,
            supported_profiles=test_profiles,
            specifier="orch-1",
        )

        assert client.implementation == test_implementation
        assert client.supported_profiles == test_profiles
        assert client.specifier == "orch-1"
        assert client.init.HasField("orchestrator")

    def test_orchestrator_client_generates_specifier(
        self,
        test_implementation: str,
        test_profiles: List[str],
    ):
        """Test that OrchestratorClient generates a specifier if not provided."""
        client = OrchestratorClient(
            implementation=test_implementation,
            supported_profiles=test_profiles,
        )

        assert client.specifier is not None
        assert len(client.specifier) == 8  # UUID[:8]

    def test_orchestrator_client_requires_implementation(self, test_profiles: List[str]):
        """Test that OrchestratorClient requires implementation."""
        with pytest.raises(InvalidArgumentError):
            OrchestratorClient(
                implementation="",
                supported_profiles=test_profiles,
            )

    def test_orchestrator_client_requires_profiles(self, test_implementation: str):
        """Test that OrchestratorClient requires at least one profile."""
        with pytest.raises(InvalidArgumentError):
            OrchestratorClient(
                implementation=test_implementation,
                supported_profiles=[],
            )


class TestOrchestratorClientMessaging:
    """Tests for OrchestratorClient messaging methods."""

    def test_send_status_to_agent(self, orchestrator_client: OrchestratorClient, test_payload: bytes):
        """Test sending status to an agent."""
        orchestrator_client.send_status_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = orchestrator_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"
        assert msg.send.message_type == CONTROL

    def test_send_status_to_task(self, orchestrator_client: OrchestratorClient, test_payload: bytes):
        """Test sending status to a task."""
        orchestrator_client.send_status_to_task(
            workspace="task-ws",
            implementation="task-impl",
            payload=test_payload,
            unique_specifier="task-spec",
        )

        msg = orchestrator_client.request_queue.get_nowait()
        assert msg.send.target_topic == "tu::task-ws::task-impl::task-spec"
        assert msg.send.message_type == CONTROL


# =============================================================================
# WorkflowEngineClient Tests
# =============================================================================

class TestWorkflowEngineClientInit:
    """Tests for WorkflowEngineClient initialization."""

    def test_workflow_engine_client_init(self):
        """Test WorkflowEngineClient initialization."""
        client = WorkflowEngineClient()

        assert client.init.HasField("workflow_engine")


class TestWorkflowEngineClientMessaging:
    """Tests for WorkflowEngineClient messaging methods."""

    def test_send_command_to_agent(
        self,
        workflow_engine_client: WorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test sending command to an agent."""
        workflow_engine_client.send_command_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = workflow_engine_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"
        assert msg.send.message_type == CONTROL

    def test_send_broadcast_to_agents(
        self,
        workflow_engine_client: WorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test broadcasting to all agents."""
        workflow_engine_client.send_broadcast_to_agents(
            workspace="broadcast-ws",
            payload=test_payload,
        )

        msg = workflow_engine_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ga::broadcast-ws"
        assert msg.send.message_type == CONTROL

    def test_send_broadcast_to_users(
        self,
        workflow_engine_client: WorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test broadcasting to all users."""
        workflow_engine_client.send_broadcast_to_users(
            workspace="broadcast-ws",
            payload=test_payload,
        )

        msg = workflow_engine_client.request_queue.get_nowait()
        assert msg.send.target_topic == "gu::broadcast-ws"
        assert msg.send.message_type == OPAQUE

    def test_send_message_to_user(
        self,
        workflow_engine_client: WorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test sending message to a specific user."""
        workflow_engine_client.send_message_to_user(
            user_id="user-123",
            window_id="window-456",
            payload=test_payload,
        )

        msg = workflow_engine_client.request_queue.get_nowait()
        assert msg.send.target_topic == "us::user-123::window-456"

    def test_send_metric(
        self,
        workflow_engine_client: WorkflowEngineClient,
        test_metric,
    ):
        """Test sending metric to metrics bridge."""
        workflow_engine_client.send_metric(test_metric)

        msg = workflow_engine_client.request_queue.get_nowait()
        assert msg.send.target_topic == "metric::*"
        assert msg.send.message_type == METRIC


# =============================================================================
# MetricsBridgeClient Tests
# =============================================================================

class TestMetricsBridgeClientInit:
    """Tests for MetricsBridgeClient initialization."""

    def test_metrics_bridge_client_init(self):
        """Test MetricsBridgeClient initialization."""
        client = MetricsBridgeClient()

        assert client.init.HasField("metrics_bridge")


class TestMetricsBridgeClientMessaging:
    """Tests for MetricsBridgeClient messaging methods."""

    def test_send_acknowledgment(
        self,
        metrics_bridge_client: MetricsBridgeClient,
        test_payload: bytes,
    ):
        """Test sending acknowledgment."""
        metrics_bridge_client.send_acknowledgment(
            target_topic="ag::ws::impl::spec",
            payload=test_payload,
        )

        msg = metrics_bridge_client.request_queue.get_nowait()
        assert msg.send.target_topic == "ag::ws::impl::spec"
        assert msg.send.message_type == CONTROL


# =============================================================================
# Exception Handling Tests
# =============================================================================

class TestErrorHandling:
    """Tests for error handling methods."""

    def test_on_error_with_grpc_error(self):
        """Test _on_error with gRPC error."""
        client = BaseAetherClient()
        mock_callback = MagicMock()
        client.on_error = mock_callback

        grpc_error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Service unavailable")

        result = client._on_error(grpc_error)

        assert isinstance(result, ConnectionError)
        mock_callback.assert_called_once()

    def test_on_error_with_non_recoverable_error_stops_client(self):
        """Test that non-recoverable errors stop the client."""
        client = BaseAetherClient()
        client._stop_event.clear()

        grpc_error = MockGrpcError(grpc.StatusCode.PERMISSION_DENIED, "Permission denied")

        client._on_error(grpc_error)

        assert client._stop_event.is_set()

    def test_on_error_with_aether_error(self):
        """Test _on_error with AetherError."""
        client = BaseAetherClient()
        mock_callback = MagicMock()
        client.on_error = mock_callback

        aether_error = AuthenticationError(message="Invalid token")

        result = client._on_error(aether_error)

        assert result is aether_error
        mock_callback.assert_called_once()

    def test_on_error_without_callback(self, caplog):
        """Test _on_error logs when no callback set."""
        import logging
        client = BaseAetherClient()

        error = aether_pb2.ErrorResponse(code="TEST_ERROR", message="Test message")

        with caplog.at_level(logging.ERROR, logger="aether.client"):
            client._on_error(error)

        assert "TEST_ERROR" in caplog.text
        assert "Test message" in caplog.text


# =============================================================================
# Sync Operations Tests
# =============================================================================

class TestSyncCheckpointOperations:
    """Tests for synchronous checkpoint operations."""

    def test_checkpoint_save_sync_with_response(self, test_checkpoint_data: bytes):
        """Test synchronous checkpoint save with immediate response."""
        client = BaseAetherClient()

        # Simulate server delivering a correlated response via _pending_requests
        def add_response():
            # Wait for the sync method to register its request_id
            for _ in range(100):
                time.sleep(0.01)
                with client._pending_requests_lock:
                    if client._pending_requests:
                        req_id = next(iter(client._pending_requests))
                        q = client._pending_requests.pop(req_id)
                        response = aether_pb2.CheckpointResponse(success=True)
                        q.put(response)
                        return

        thread = threading.Thread(target=add_response)
        thread.start()

        result = client.checkpoint_save_sync(test_checkpoint_data, timeout=1.0)
        thread.join()

        assert result is not None
        assert result.success is True

    def test_checkpoint_save_sync_timeout(self, test_checkpoint_data: bytes):
        """Test synchronous checkpoint save with timeout."""
        client = BaseAetherClient()

        result = client.checkpoint_save_sync(test_checkpoint_data, timeout=0.1)

        assert result is None

    def test_checkpoint_load_sync_with_response(self):
        """Test synchronous checkpoint load with response."""
        client = BaseAetherClient()

        def add_response():
            for _ in range(100):
                time.sleep(0.01)
                with client._pending_requests_lock:
                    if client._pending_requests:
                        req_id = next(iter(client._pending_requests))
                        q = client._pending_requests.pop(req_id)
                        response = aether_pb2.CheckpointResponse(success=True, data=b"checkpoint data")
                        q.put(response)
                        return

        thread = threading.Thread(target=add_response)
        thread.start()

        result = client.checkpoint_load_sync(timeout=1.0)
        thread.join()

        assert result is not None
        assert result.data == b"checkpoint data"

    def test_checkpoint_delete_sync_with_response(self):
        """Test synchronous checkpoint delete with response."""
        client = BaseAetherClient()

        def add_response():
            for _ in range(100):
                time.sleep(0.01)
                with client._pending_requests_lock:
                    if client._pending_requests:
                        req_id = next(iter(client._pending_requests))
                        q = client._pending_requests.pop(req_id)
                        response = aether_pb2.CheckpointResponse(success=True)
                        q.put(response)
                        return

        thread = threading.Thread(target=add_response)
        thread.start()

        result = client.checkpoint_delete_sync(timeout=1.0)
        thread.join()

        assert result is not None
        assert result.success is True

    def test_checkpoint_list_sync_with_response(self):
        """Test synchronous checkpoint list with response."""
        client = BaseAetherClient()

        def add_response():
            for _ in range(100):
                time.sleep(0.01)
                with client._pending_requests_lock:
                    if client._pending_requests:
                        req_id = next(iter(client._pending_requests))
                        q = client._pending_requests.pop(req_id)
                        response = aether_pb2.CheckpointResponse(success=True)
                        response.keys.extend(["key1", "key2", "key3"])
                        q.put(response)
                        return

        thread = threading.Thread(target=add_response)
        thread.start()

        result = client.checkpoint_list_sync(timeout=1.0)
        thread.join()

        assert result is not None
        assert list(result.keys) == ["key1", "key2", "key3"]


# =============================================================================
# Topic Creation Tests
# =============================================================================

class TestTopicCreation:
    """Tests for topic creation helpers."""

    def test_create_topic_agent(self):
        """Test agent topic creation."""
        topic = create_topic_agent("workspace", "impl", "spec")
        assert topic == "ag::workspace::impl::spec"

    def test_create_topic_task_unique(self):
        """Test unique task topic creation."""
        topic = create_topic_task("workspace", "impl", "unique-id")
        assert topic == "tu::workspace::impl::unique-id"

    def test_create_topic_task_non_unique(self):
        """Test non-unique task topic creation."""
        topic = create_topic_task("workspace", "impl", "")
        assert topic == "ta::workspace::impl::"

    def test_create_topic_user(self):
        """Test user topic creation."""
        topic = create_topic_user("user-123", "window-456")
        assert topic == "us::user-123::window-456"

    def test_create_topic_global_agents(self):
        """Test global agents broadcast topic creation."""
        topic = create_topic_global_agents("workspace")
        assert topic == "ga::workspace"

    def test_create_topic_global_users(self):
        """Test global users broadcast topic creation."""
        topic = create_topic_global_users("workspace")
        assert topic == "gu::workspace"


# =============================================================================
# Message Type Tests
# =============================================================================

class TestMessageTypes:
    """Tests for message type handling."""

    def test_send_message_with_custom_type(self, agent_client: AgentClient, test_payload: bytes):
        """Test sending message with custom message type."""
        agent_client.send_message_to_agent(
            workspace="ws",
            implementation="impl",
            specifier="spec",
            payload=test_payload,
            message_type=CONTROL,
        )

        msg = agent_client.request_queue.get_nowait()
        assert msg.send.message_type == CONTROL

    def test_message_type_constants(self):
        """Test that message type constants are correct."""
        assert CHAT == aether_pb2.CHAT
        assert CONTROL == aether_pb2.CONTROL
        assert EVENT == aether_pb2.EVENT
        assert METRIC == aether_pb2.METRIC


# =============================================================================
# Request Generator Tests
# =============================================================================

class TestRequestGenerator:
    """Tests for the request generator."""

    def test_request_generator_yields_queued_messages(self):
        """Test that request generator yields queued messages."""
        client = BaseAetherClient()

        # Queue a message
        msg = aether_pb2.UpstreamMessage()
        client.request_queue.put(msg)
        client.request_queue.put(None)  # Sentinel to stop

        generator = client._request_generator()
        yielded_msg = next(generator)

        assert yielded_msg is msg

    def test_request_generator_stops_on_sentinel(self):
        """Test that request generator stops on None sentinel."""
        client = BaseAetherClient()

        client.request_queue.put(None)

        generator = client._request_generator()
        messages = list(generator)

        assert len(messages) == 0

    def test_request_generator_stops_on_stop_event(self):
        """Test that request generator respects stop event."""
        client = BaseAetherClient()
        client._stop_event.set()

        generator = client._request_generator()
        messages = list(generator)

        assert len(messages) == 0


# =============================================================================
# Callback Assignment Tests
# =============================================================================

class TestCallbackAssignment:
    """Tests for callback assignment."""

    def test_can_assign_message_callback(
        self,
        agent_client: AgentClient,
        mock_message_callback: MagicMock,
    ):
        """Test assigning message callback."""
        agent_client.on_message = mock_message_callback
        assert agent_client.on_message is mock_message_callback

    def test_can_assign_config_callback(
        self,
        agent_client: AgentClient,
        mock_config_callback: MagicMock,
    ):
        """Test assigning config callback."""
        agent_client.on_config = mock_config_callback
        assert agent_client.on_config is mock_config_callback

    def test_can_assign_connect_callback(
        self,
        agent_client: AgentClient,
        mock_connect_callback: MagicMock,
    ):
        """Test assigning connect callback."""
        agent_client.on_connect = mock_connect_callback
        assert agent_client.on_connect is mock_connect_callback

    def test_can_assign_disconnect_callback(
        self,
        agent_client: AgentClient,
        mock_disconnect_callback: MagicMock,
    ):
        """Test assigning disconnect callback."""
        agent_client.on_disconnect = mock_disconnect_callback
        assert agent_client.on_disconnect is mock_disconnect_callback


# =============================================================================
# CreateTask Sync Tests (create_task_sync — blocking with server response)
# =============================================================================

class TestSyncCreateTask:
    """Tests for create_task_sync — blocking variant that waits for CreateTaskResponse.

    The bare create_task() is fire-and-forget and already covered in
    TestBaseAetherClientCreateTask. These tests exercise the correlated-response
    path introduced alongside the CreateTaskResponse proto addition.
    """

    def test_create_task_sync_with_response(self):
        """create_task_sync receives the correlated CreateTaskResponse and returns it."""
        client = BaseAetherClient()

        def add_response():
            for _ in range(100):
                time.sleep(0.01)
                with client._pending_requests_lock:
                    if client._pending_requests:
                        req_id = next(iter(client._pending_requests))
                        q = client._pending_requests.pop(req_id)
                        response = aether_pb2.CreateTaskResponse(
                            success=True,
                            task_id="task-abc",
                            status="pending",
                            request_id=req_id,
                        )
                        q.put(response)
                        return

        thread = threading.Thread(target=add_response)
        thread.start()

        result = client.create_task_sync(
            task_type="sandbox_lease",
            workspace="_apps",
            timeout=1.0,
        )
        thread.join()

        assert result is not None
        assert result.success is True
        assert result.task_id == "task-abc"
        assert result.status == "pending"

    def test_create_task_sync_timeout(self):
        """create_task_sync returns None when no response arrives within timeout."""
        client = BaseAetherClient()

        result = client.create_task_sync(
            task_type="sandbox_lease",
            workspace="_apps",
            timeout=0.1,
        )

        assert result is None

    def test_create_task_sync_stamps_request_id_on_request(self):
        """Request enqueued for create_task_sync must carry a non-empty request_id.

        The server's ``handleCreateTask`` uses a non-empty request_id as the
        signal to send a CreateTaskResponse. Without it, the client would
        block forever — hence the implementation must stamp one.
        """
        client = BaseAetherClient()

        # Start the call in a thread; it will block on the response queue.
        # We don't care about the result — just that the request landed.
        def call():
            client.create_task_sync(
                task_type="sandbox_lease",
                workspace="_apps",
                timeout=0.05,  # short — we're only inspecting the outbound request
            )

        thread = threading.Thread(target=call)
        thread.start()

        # Give it a moment to enqueue
        time.sleep(0.02)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("create_task")
        assert msg.create_task.task_type == "sandbox_lease"
        assert msg.create_task.workspace == "_apps"
        assert msg.create_task.request_id, (
            "create_task_sync must stamp a non-empty request_id"
        )

        thread.join()

    def test_create_task_sync_forwards_authorization_context(self):
        """``authorization`` kwarg must land on the outbound CreateTaskRequest.

        This is the OBO hook the gateway relies on to evaluate ACL against
        the user's delegated grant instead of the agent's narrower identity.
        Without it, per-user attribution and workspace ACL checks fail.
        """
        client = BaseAetherClient()

        auth = aether_pb2.AuthorizationContext(
            authority_mode="on_behalf_of",
            subject=aether_pb2.PrincipalRef(
                principal_type="user", principal_id="alice@example.com",
            ),
            grant_id="grant-abc",
        )

        def call():
            client.create_task_sync(
                task_type="sandbox_lease",
                workspace="default",
                authorization=auth,
                timeout=0.05,
            )

        thread = threading.Thread(target=call)
        thread.start()
        time.sleep(0.02)

        msg = client.request_queue.get_nowait()
        assert msg.HasField("create_task")
        req_auth = msg.create_task.authorization
        assert req_auth.authority_mode == "on_behalf_of"
        assert req_auth.grant_id == "grant-abc"
        assert req_auth.subject.principal_type == "user"
        assert req_auth.subject.principal_id == "alice@example.com"

        thread.join()


# =============================================================================
# Task Lifecycle Tests (complete_task / fail_task)
# =============================================================================

class TestSyncTaskLifecycle:
    """Tests for complete_task and fail_task on sync client."""

    def test_complete_task_method_exists(self, agent_client: AgentClient):
        """complete_task method exists on BaseAetherClient."""
        assert hasattr(agent_client, 'complete_task')
        assert callable(agent_client.complete_task)

    def test_fail_task_method_exists(self, agent_client: AgentClient):
        """fail_task method exists on BaseAetherClient."""
        assert hasattr(agent_client, 'fail_task')
        assert callable(agent_client.fail_task)

    def test_complete_task_queues_upstream_message(self, agent_client: AgentClient):
        """complete_task puts a COMPLETE TaskOperation on the request queue."""
        from unittest.mock import patch
        import queue as _queue

        q = _queue.Queue()
        agent_client.request_queue = q

        # Patch the internal response queue to return immediately
        response = aether_pb2.TaskOperationResponse(success=True)
        agent_client._task_op_response_queue.put(response)

        result = agent_client.complete_task("task-123", timeout=1.0)

        assert not q.empty()
        msg = q.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.COMPLETE
        assert msg.task_op.task_id == "task-123"

    def test_fail_task_queues_upstream_message_with_reason(self, agent_client: AgentClient):
        """fail_task puts a FAIL TaskOperation with reason on the request queue."""
        import queue as _queue

        q = _queue.Queue()
        agent_client.request_queue = q

        response = aether_pb2.TaskOperationResponse(success=True)
        agent_client._task_op_response_queue.put(response)

        agent_client.fail_task("task-456", reason="something broke", timeout=1.0)

        assert not q.empty()
        msg = q.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.FAIL
        assert msg.task_op.task_id == "task-456"
        assert msg.task_op.reason == "something broke"

    def test_complete_task_returns_none_on_timeout(self, agent_client: AgentClient):
        """complete_task returns None when no response arrives within timeout."""
        import queue as _queue

        agent_client.request_queue = _queue.Queue()
        result = agent_client.complete_task("task-789", timeout=0.05)
        assert result is None

    def test_fail_task_returns_none_on_timeout(self, agent_client: AgentClient):
        """fail_task returns None when no response arrives within timeout."""
        import queue as _queue

        agent_client.request_queue = _queue.Queue()
        result = agent_client.fail_task("task-789", reason="err", timeout=0.05)
        assert result is None


# =============================================================================
# Workspace Operation Tests
# =============================================================================

class TestSyncWorkspaceOps:
    """Tests for workspace operations on sync client."""

    def test_workspace_op_method_exists(self, agent_client: AgentClient):
        """workspace_op method exists on BaseAetherClient."""
        assert hasattr(agent_client, 'workspace_op')
        assert callable(agent_client.workspace_op)

    def test_workspace_response_queue_initialized(self, agent_client: AgentClient):
        """_workspace_response_queue is initialized on BaseAetherClient."""
        assert hasattr(agent_client, '_workspace_response_queue')

    def test_workspace_response_callback_initializable(self, agent_client: AgentClient):
        """on_workspace_response callback attribute exists and is assignable."""
        assert hasattr(agent_client, 'on_workspace_response')
        agent_client.on_workspace_response = lambda resp: None
        assert agent_client.on_workspace_response is not None

    def test_workspace_op_queues_upstream_message(self, agent_client: AgentClient):
        """workspace_op puts a WorkspaceOperation on the request queue."""
        import queue as _queue

        q = _queue.Queue()
        agent_client.request_queue = q

        from scitrera_aether_client.proto import aether_pb2 as pb
        op = pb.WorkspaceOperation()
        agent_client._workspace_response_queue.put(object())  # mock response

        agent_client.workspace_op(op, timeout=1.0)

        assert not q.empty()
        msg = q.get_nowait()
        assert msg.HasField("workspace_op")

    def test_workspace_op_returns_none_on_timeout(self, agent_client: AgentClient):
        """workspace_op returns None when no response arrives within timeout."""
        import queue as _queue

        agent_client.request_queue = _queue.Queue()
        op = aether_pb2.WorkspaceOperation()
        result = agent_client.workspace_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Agent Operation Tests
# =============================================================================

class TestSyncAgentOps:
    """Tests for agent operations on sync client."""

    def test_agent_op_method_exists(self, agent_client: AgentClient):
        """agent_op method exists on BaseAetherClient."""
        assert hasattr(agent_client, 'agent_op')
        assert callable(agent_client.agent_op)

    def test_agent_response_queue_initialized(self, agent_client: AgentClient):
        """_agent_response_queue is initialized on BaseAetherClient."""
        assert hasattr(agent_client, '_agent_response_queue')

    def test_agent_op_queues_upstream_message(self, agent_client: AgentClient):
        """agent_op puts an AgentOperation on the request queue."""
        import queue as _queue

        q = _queue.Queue()
        agent_client.request_queue = q

        op = aether_pb2.AgentOperation()
        agent_client._agent_response_queue.put(object())  # mock response

        agent_client.agent_op(op, timeout=1.0)

        assert not q.empty()
        msg = q.get_nowait()
        assert msg.HasField("agent_op")

    def test_agent_op_returns_none_on_timeout(self, agent_client: AgentClient):
        """agent_op returns None when no response arrives within timeout."""
        import queue as _queue

        agent_client.request_queue = _queue.Queue()
        op = aether_pb2.AgentOperation()
        result = agent_client.agent_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# ACL Operation Tests
# =============================================================================

class TestSyncACLOps:
    """Tests for ACL operations on sync client."""

    def test_acl_op_method_exists(self, agent_client: AgentClient):
        """acl_op method exists on BaseAetherClient."""
        assert hasattr(agent_client, 'acl_op')
        assert callable(agent_client.acl_op)

    def test_acl_response_queue_initialized(self, agent_client: AgentClient):
        """_acl_response_queue is initialized on BaseAetherClient."""
        assert hasattr(agent_client, '_acl_response_queue')

    def test_acl_op_queues_upstream_message(self, agent_client: AgentClient):
        """acl_op puts an ACLOperation on the request queue."""
        import queue as _queue

        q = _queue.Queue()
        agent_client.request_queue = q

        op = aether_pb2.ACLOperation()
        agent_client._acl_response_queue.put(object())  # mock response

        agent_client.acl_op(op, timeout=1.0)

        assert not q.empty()
        msg = q.get_nowait()
        assert msg.HasField("acl_op")

    def test_acl_op_returns_none_on_timeout(self, agent_client: AgentClient):
        """acl_op returns None when no response arrives within timeout."""
        import queue as _queue

        agent_client.request_queue = _queue.Queue()
        op = aether_pb2.ACLOperation()
        result = agent_client.acl_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Workflow Operation Tests
# =============================================================================

class TestSyncWorkflowOps:
    """Tests for workflow operations on sync client."""

    def test_workflow_op_method_exists(self, agent_client: AgentClient):
        """workflow_op method exists on BaseAetherClient."""
        assert hasattr(agent_client, 'workflow_op')
        assert callable(agent_client.workflow_op)

    def test_workflow_response_queue_initialized(self, agent_client: AgentClient):
        """_workflow_response_queue is initialized on BaseAetherClient."""
        assert hasattr(agent_client, '_workflow_response_queue')

    def test_workflow_op_queues_upstream_message(self, agent_client: AgentClient):
        """workflow_op puts a WorkflowOperation on the request queue."""
        import queue as _queue

        q = _queue.Queue()
        agent_client.request_queue = q

        op = aether_pb2.WorkflowOperation()
        agent_client._workflow_response_queue.put(object())  # mock response

        agent_client.workflow_op(op, timeout=1.0)

        assert not q.empty()
        msg = q.get_nowait()
        assert msg.HasField("workflow_op")

    def test_workflow_op_returns_none_on_timeout(self, agent_client: AgentClient):
        """workflow_op returns None when no response arrives within timeout."""
        import queue as _queue

        agent_client.request_queue = _queue.Queue()
        op = aether_pb2.WorkflowOperation()
        result = agent_client.workflow_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Token Operation Tests
# =============================================================================

class TestSyncTokenCRUD:
    """Tests for token CRUD operations on sync client."""

    def test_list_tokens_method_exists(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'list_tokens')
        assert callable(agent_client.list_tokens)

    def test_get_token_method_exists(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'get_token')
        assert callable(agent_client.get_token)

    def test_create_token_method_exists(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'create_token')
        assert callable(agent_client.create_token)

    def test_delete_token_method_exists(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'delete_token')
        assert callable(agent_client.delete_token)

    def test_revoke_token_method_exists(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'revoke_token')
        assert callable(agent_client.revoke_token)

    def test_token_op_low_level_exists(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'token_op')
        assert callable(agent_client.token_op)

    def test_pending_requests_initialized(self, agent_client: AgentClient):
        assert hasattr(agent_client, '_pending_requests')
        assert isinstance(agent_client._pending_requests, dict)

    def test_token_response_callback_initializable(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'on_token_response')

    def test_create_token_queues_message(self, agent_client: AgentClient):
        """Verify create_token puts the correct proto on the request queue."""
        import threading

        agent_client._stop_event = threading.Event()
        agent_client.request_queue = queue.Queue()

        def call():
            agent_client.create_token("test-key", "agent", timeout=0.1)

        t = threading.Thread(target=call)
        t.start()

        import time
        time.sleep(0.05)
        msg = agent_client.request_queue.get(timeout=0.5)
        assert msg.HasField('token_op')
        assert msg.token_op.op == aether_pb2.TokenOperation.CREATE
        assert msg.token_op.create_request.name == "test-key"
        assert msg.token_op.create_request.principal_type == "agent"
        assert msg.token_op.request_id != ""
        t.join(timeout=1)

    def test_token_op_returns_none_on_timeout(self, agent_client: AgentClient):
        """token_op returns None when no response arrives within timeout."""
        agent_client.request_queue = queue.Queue()
        op = aether_pb2.TokenOperation()
        result = agent_client.token_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Authority Grant Operation Tests
# =============================================================================

class TestSyncAuthorityGrantOps:
    """Tests for runtime and admin authority-grant operations on sync client."""

    def test_authority_grant_op_method_exists(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'authority_grant_op')
        assert callable(agent_client.authority_grant_op)

    def test_authority_grant_response_queue_initialized(self, agent_client: AgentClient):
        assert hasattr(agent_client, '_authority_grant_response_queue')

    def test_authority_grant_response_callback_initializable(self, agent_client: AgentClient):
        assert hasattr(agent_client, 'on_authority_grant_response')

    def test_exchange_authority_grant_queues_message(self, agent_client: AgentClient):
        """Verify exchange_authority_grant puts the correct proto on the request queue."""
        agent_client._stop_event = threading.Event()
        agent_client.request_queue = queue.Queue()

        def call():
            agent_client.exchange_authority_grant(
                source_session_id="sess-123",
                workspace_scope=["ws-a"],
                resource_scope={"kv_key": ["cfg/*"]},
                operation_scope=["kv_get"],
                max_access_level=aether_pb2.ACCESS_LEVEL_READ,
                audience_type="session",
                audience_id="sess-actor",
                valid_while_audience_active=True,
                expires_at=1234,
                renewable_until=5678,
                may_delegate=True,
                remaining_hops=2,
                reason="test",
                metadata={"source": "pytest"},
                timeout=0.1,
            )

        t = threading.Thread(target=call)
        t.start()

        time.sleep(0.05)
        msg = agent_client.request_queue.get(timeout=0.5)
        assert msg.HasField('authority_grant_op')
        assert msg.authority_grant_op.op == aether_pb2.AuthorityGrantOperation.EXCHANGE
        assert msg.authority_grant_op.request_id != ""
        exchange = msg.authority_grant_op.exchange_request
        assert exchange.source_session_id == "sess-123"
        assert exchange.workspace_scope == ["ws-a"]
        assert exchange.resource_scope[0].resource_type == "kv_key"
        assert list(exchange.resource_scope[0].patterns) == ["cfg/*"]
        assert exchange.operation_scope == ["kv_get"]
        assert exchange.audience_type == "session"
        assert exchange.audience_id == "sess-actor"
        assert exchange.valid_while_audience_active is True
        assert exchange.expires_at == 1234
        assert exchange.renewable_until == 5678
        assert exchange.may_delegate is True
        assert exchange.remaining_hops == 2
        assert exchange.metadata["source"] == "pytest"
        t.join(timeout=1)

    def test_authority_grant_op_returns_none_on_timeout(self, agent_client: AgentClient):
        """authority_grant_op returns None when no response arrives within timeout."""
        agent_client.request_queue = queue.Queue()
        op = aether_pb2.AuthorityGrantOperation()
        result = agent_client.authority_grant_op(op, timeout=0.05)
        assert result is None

# =============================================================================
# ServiceClient Tests
# =============================================================================

class TestServiceClient:
    """Tests for ServiceClient initialization and attributes."""

    def test_init_stores_implementation_and_specifier(self):
        """ServiceClient stores implementation and specifier attributes."""
        from scitrera_aether_client.client import ServiceClient
        client = ServiceClient(implementation="my-svc", specifier="pod-1")
        assert client.implementation == "my-svc"
        assert client.specifier == "pod-1"

    def test_init_creates_init_proto(self):
        """ServiceClient creates a valid InitConnection proto with service identity."""
        from scitrera_aether_client.client import ServiceClient
        client = ServiceClient(implementation="my-svc", specifier="pod-1")
        assert client.init is not None
        assert client.init.HasField("service")
        assert client.init.service.implementation == "my-svc"
        assert client.init.service.specifier == "pod-1"

    def test_init_empty_implementation_raises(self):
        """ServiceClient raises InvalidArgumentError for empty implementation."""
        from scitrera_aether_client.client import ServiceClient
        from scitrera_aether_client.exceptions import InvalidArgumentError
        with pytest.raises(InvalidArgumentError):
            ServiceClient(implementation="", specifier="pod-1")

    def test_init_empty_specifier_raises(self):
        """ServiceClient raises InvalidArgumentError for empty specifier."""
        from scitrera_aether_client.client import ServiceClient
        from scitrera_aether_client.exceptions import InvalidArgumentError
        with pytest.raises(InvalidArgumentError):
            ServiceClient(implementation="my-svc", specifier="")

    def test_init_with_credentials(self):
        """ServiceClient passes credentials into the init proto."""
        from scitrera_aether_client.client import ServiceClient
        creds = {"api_key": "secret-123"}
        client = ServiceClient(implementation="my-svc", specifier="pod-1", credentials=creds)
        assert client.init.credentials["api_key"] == "secret-123"

    def test_inherits_base_client(self):
        """ServiceClient is a subclass of BaseAetherClient."""
        from scitrera_aether_client.client import ServiceClient, BaseAetherClient
        client = ServiceClient(implementation="my-svc", specifier="pod-1")
        assert isinstance(client, BaseAetherClient)

    def test_send_message_to_agent_queues_message(self):
        """send_message_to_agent puts correct UpstreamMessage on request_queue."""
        from scitrera_aether_client.client import ServiceClient
        client = ServiceClient(implementation="my-svc", specifier="pod-1")
        client.send_message_to_agent("ws1", "my-agent", "spec-a", b"payload")
        msg = client.request_queue.get_nowait()
        assert msg.HasField("send")
        assert msg.send.target_topic == "ag::ws1::my-agent::spec-a"
        assert msg.send.payload == b"payload"

    def test_send_message_to_task_queues_message(self):
        """send_message_to_task puts correct UpstreamMessage on request_queue."""
        from scitrera_aether_client.client import ServiceClient
        client = ServiceClient(implementation="my-svc", specifier="pod-1")
        client.send_message_to_task("ws1", "my-task", b"payload", unique_specifier="u1")
        msg = client.request_queue.get_nowait()
        assert msg.HasField("send")
        assert msg.send.target_topic == "tu::ws1::my-task::u1"

    def test_send_message_to_user_session_queues_message(self):
        """send_message_to_user_session puts correct UpstreamMessage on request_queue."""
        from scitrera_aether_client.client import ServiceClient
        client = ServiceClient(implementation="my-svc", specifier="pod-1")
        client.send_message_to_user_session("user-1", "win-2", b"payload")
        msg = client.request_queue.get_nowait()
        assert msg.HasField("send")
        assert msg.send.target_topic == "us::user-1::win-2"

    def test_send_message_to_user_workspace_queues_message(self):
        """send_message_to_user_workspace puts correct UpstreamMessage on request_queue."""
        from scitrera_aether_client.client import ServiceClient
        client = ServiceClient(implementation="my-svc", specifier="pod-1")
        client.send_message_to_user_workspace("user-1", "workspace-a", b"payload")
        msg = client.request_queue.get_nowait()
        assert msg.HasField("send")
        assert msg.send.target_topic == "uw::user-1::workspace-a"

    def test_exported_from_package(self):
        """ServiceClient is importable from the top-level package."""
        import scitrera_aether_client
        assert hasattr(scitrera_aether_client, "ServiceClient")
        assert scitrera_aether_client.ServiceClient is not None


# =============================================================================
# Agent Admin API Tests
# =============================================================================

class TestAgentAdminAPI:
    """Tests for agent registry methods on BaseAetherClient."""

    def test_register_agent_queues_upstream_message(self, agent_client: AgentClient):
        """register_agent calls agent_op with REGISTER and correct fields."""
        mock_result = object()
        agent_client._send_lockable_op = MagicMock(return_value=mock_result)

        result = agent_client.register_agent(
            implementation="myorg/my-agent",
            profile="k8s",
            description="A test agent",
            launch_params={"image": "myorg/agent:latest"},
            timeout=5.0,
        )

        assert result is mock_result
        call_args = agent_client._send_lockable_op.call_args
        msg = call_args[0][2]  # third positional: the UpstreamMessage
        assert msg.HasField("agent_op")
        assert msg.agent_op.op == aether_pb2.AgentOperation.REGISTER
        assert msg.agent_op.agent.implementation == "myorg/my-agent"
        assert msg.agent_op.agent.orchestrator_profile == "k8s"
        assert msg.agent_op.agent.description == "A test agent"
        assert msg.agent_op.agent.launch_params["image"] == "myorg/agent:latest"
        assert msg.agent_op.agent.launch_params["profile"] == "k8s"

    def test_register_agent_sets_default_profile_in_launch_params(self, agent_client: AgentClient):
        """register_agent injects 'profile' into launch_params when not provided."""
        agent_client._send_lockable_op = MagicMock(return_value=None)
        agent_client.register_agent(implementation="myorg/my-agent", profile="local")
        msg = agent_client._send_lockable_op.call_args[0][2]
        assert msg.agent_op.agent.launch_params["profile"] == "local"

    def test_get_agent_queues_correct_message(self, agent_client: AgentClient):
        """get_agent calls agent_op with GET operation and implementation."""
        agent_client._send_lockable_op = MagicMock(return_value=None)
        agent_client.get_agent(implementation="myorg/my-agent", timeout=3.0)
        msg = agent_client._send_lockable_op.call_args[0][2]
        assert msg.agent_op.op == aether_pb2.AgentOperation.GET
        assert msg.agent_op.implementation == "myorg/my-agent"

    def test_list_agents_queues_correct_message(self, agent_client: AgentClient):
        """list_agents calls agent_op with LIST operation and filter fields."""
        agent_client._send_lockable_op = MagicMock(return_value=None)
        agent_client.list_agents(profile="k8s", limit=10, offset=5, timeout=3.0)
        msg = agent_client._send_lockable_op.call_args[0][2]
        assert msg.agent_op.op == aether_pb2.AgentOperation.LIST
        assert msg.agent_op.filter.orchestrator_profile == "k8s"
        assert msg.agent_op.filter.limit == 10
        assert msg.agent_op.filter.offset == 5

    def test_delete_agent_queues_correct_message(self, agent_client: AgentClient):
        """delete_agent calls agent_op with DELETE operation and implementation."""
        agent_client._send_lockable_op = MagicMock(return_value=None)
        agent_client.delete_agent(implementation="myorg/my-agent", timeout=3.0)
        msg = agent_client._send_lockable_op.call_args[0][2]
        assert msg.agent_op.op == aether_pb2.AgentOperation.DELETE
        assert msg.agent_op.implementation == "myorg/my-agent"

    def test_launch_agent_queues_correct_message(self, agent_client: AgentClient):
        """launch_agent calls agent_op with LAUNCH and correct launch params."""
        agent_client._send_lockable_op = MagicMock(return_value=None)
        agent_client.launch_agent(
            implementation="myorg/my-agent",
            workspace="ws1",
            specifier="inst-1",
            param_overrides={"replicas": "2"},
            timeout=5.0,
        )
        msg = agent_client._send_lockable_op.call_args[0][2]
        assert msg.agent_op.op == aether_pb2.AgentOperation.LAUNCH
        assert msg.agent_op.implementation == "myorg/my-agent"
        assert msg.agent_op.launch_params.workspace == "ws1"
        assert msg.agent_op.launch_params.specifier == "inst-1"
        assert msg.agent_op.launch_params.param_overrides["replicas"] == "2"

    def test_register_agent_returns_none_on_timeout(self, agent_client: AgentClient):
        """register_agent returns None when agent_op times out."""
        result = agent_client.register_agent(
            implementation="myorg/my-agent", timeout=0.01
        )
        assert result is None

    def test_get_agent_returns_none_on_timeout(self, agent_client: AgentClient):
        """get_agent returns None when agent_op times out."""
        result = agent_client.get_agent(implementation="myorg/my-agent", timeout=0.01)
        assert result is None


# =============================================================================
# Scheduling API Tests
# =============================================================================

class TestSchedulingAPI:
    """Tests for scheduling convenience methods on BaseAetherClient."""

    def test_create_schedule_sync_queues_workflow_op(self, agent_client: AgentClient):
        """create_schedule_sync puts a CREATE_SCHEDULE WorkflowOperation on the queue."""
        import json as _json
        agent_client._send_sync_op = MagicMock(return_value=None)

        agent_client.create_schedule_sync(
            schedule_id="sched-1",
            name="My Schedule",
            schedule_type="cron",
            schedule_expr="0 * * * *",
            workspace="ws1",
            miss_policy="fire_once",
            max_concurrent=1,
            timeout=5.0,
        )

        assert agent_client._send_sync_op.called
        msg, req_id, timeout = agent_client._send_sync_op.call_args[0]
        assert msg.HasField("workflow_op")
        assert msg.workflow_op.op == aether_pb2.WorkflowOperation.CREATE_SCHEDULE
        data = _json.loads(msg.workflow_op.data.decode())
        assert data["id"] == "sched-1"
        assert data["name"] == "My Schedule"
        assert data["schedule_type"] == "cron"
        assert data["schedule_expr"] == "0 * * * *"
        assert data["workspace"] == "ws1"
        assert data["miss_policy"] == "fire_once"
        assert data["max_concurrent"] == 1
        assert timeout == 5.0

    def test_upsert_schedule_sync_uses_upsert_op(self, agent_client: AgentClient):
        """upsert_schedule_sync puts an UPSERT_SCHEDULE WorkflowOperation."""
        agent_client._send_sync_op = MagicMock(return_value=None)

        agent_client.upsert_schedule_sync(
            schedule_id="sched-2",
            name="Updated",
            schedule_type="interval",
            schedule_expr="3600s",
        )

        msg, _, _ = agent_client._send_sync_op.call_args[0]
        assert msg.workflow_op.op == aether_pb2.WorkflowOperation.UPSERT_SCHEDULE

    def test_delete_schedule_sync_uses_delete_op(self, agent_client: AgentClient):
        """delete_schedule_sync puts a DELETE_SCHEDULE WorkflowOperation."""
        agent_client._send_sync_op = MagicMock(return_value=None)
        agent_client.delete_schedule_sync("sched-3", timeout=3.0)
        msg, _, timeout = agent_client._send_sync_op.call_args[0]
        assert msg.workflow_op.op == aether_pb2.WorkflowOperation.DELETE_SCHEDULE
        assert msg.workflow_op.id == "sched-3"
        assert timeout == 3.0

    def test_list_schedules_sync_uses_list_op(self, agent_client: AgentClient):
        """list_schedules_sync puts a LIST_SCHEDULES WorkflowOperation."""
        agent_client._send_sync_op = MagicMock(return_value=None)
        agent_client.list_schedules_sync(workspace="ws1", timeout=4.0)
        msg, _, timeout = agent_client._send_sync_op.call_args[0]
        assert msg.workflow_op.op == aether_pb2.WorkflowOperation.LIST_SCHEDULES
        assert msg.workflow_op.workspace == "ws1"
        assert timeout == 4.0

    def test_create_schedule_sync_includes_action_when_provided(self, agent_client: AgentClient):
        """create_schedule_sync includes action dict in data when provided."""
        import json as _json
        agent_client._send_sync_op = MagicMock(return_value=None)
        action = {"type": "launch_agent", "implementation": "myorg/bot"}
        agent_client.create_schedule_sync(
            schedule_id="s1", name="n", schedule_type="cron",
            schedule_expr="* * * * *", action=action,
        )
        msg, _, _ = agent_client._send_sync_op.call_args[0]
        data = _json.loads(msg.workflow_op.data.decode())
        assert data["action"]["type"] == "launch_agent"

    def test_create_schedule_sync_includes_workflow_id_when_provided(self, agent_client: AgentClient):
        """create_schedule_sync includes workflow_id in data when provided."""
        import json as _json
        agent_client._send_sync_op = MagicMock(return_value=None)
        agent_client.create_schedule_sync(
            schedule_id="s1", name="n", schedule_type="cron",
            schedule_expr="* * * * *", workflow_id="wf-42",
        )
        msg, _, _ = agent_client._send_sync_op.call_args[0]
        data = _json.loads(msg.workflow_op.data.decode())
        assert data["workflow_id"] == "wf-42"

    def test_create_schedule_sync_returns_none_on_timeout(self, agent_client: AgentClient):
        """create_schedule_sync returns None when the RPC times out."""
        result = agent_client.create_schedule_sync(
            schedule_id="s1", name="n", schedule_type="cron",
            schedule_expr="* * * * *", timeout=0.01,
        )
        assert result is None


# =============================================================================
# Audit Query Tests
# =============================================================================

class TestAuditQuery:
    """Tests for audit_query_sync on BaseAetherClient."""

    def test_audit_query_sync_sends_audit_query_message(self, agent_client: AgentClient):
        """audit_query_sync calls _send_sync_op with an AuditQuery message."""
        agent_client._send_sync_op = MagicMock(return_value=None)

        agent_client.audit_query_sync(
            event_type="message",
            actor_type="agent",
            actor_id="myorg/my-agent::ws::spec",
            workspace="ws1",
            operation="send",
            limit=50,
            timeout=7.0,
        )

        assert agent_client._send_sync_op.called
        msg, req_id, timeout = agent_client._send_sync_op.call_args[0]
        assert msg.HasField("audit_query")
        q = msg.audit_query
        assert q.event_type == "message"
        assert q.actor_type == "agent"
        assert q.actor_id == "myorg/my-agent::ws::spec"
        assert q.workspace == "ws1"
        assert q.operation == "send"
        assert q.limit == 50
        assert req_id != ""
        assert q.request_id == req_id
        assert timeout == 7.0

    def test_audit_query_sync_propagates_all_filter_fields(self, agent_client: AgentClient):
        """audit_query_sync sets all optional filter fields on the AuditQuery proto."""
        agent_client._send_sync_op = MagicMock(return_value=None)

        agent_client.audit_query_sync(
            resource_type="kv_key",
            resource_id="cfg/foo",
            start_time=1000,
            end_time=2000,
            only_failures=True,
            offset=10,
            subject_type="user",
            subject_id="alice",
            authority_mode="on_behalf_of",
            authority_grant_id="grant-abc",
            exclude_actor_types=["system"],
            exclude_workspaces=["internal"],
            exclude_service_direct=True,
        )

        msg, _, _ = agent_client._send_sync_op.call_args[0]
        q = msg.audit_query
        assert q.resource_type == "kv_key"
        assert q.resource_id == "cfg/foo"
        assert q.start_time == 1000
        assert q.end_time == 2000
        assert q.only_failures is True
        assert q.offset == 10
        assert q.subject_type == "user"
        assert q.subject_id == "alice"
        assert q.authority_mode == "on_behalf_of"
        assert q.authority_grant_id == "grant-abc"
        assert list(q.exclude_actor_types) == ["system"]
        assert list(q.exclude_workspaces) == ["internal"]
        assert q.exclude_service_direct is True

    def test_audit_query_sync_returns_none_on_timeout(self, agent_client: AgentClient):
        """audit_query_sync returns None when no response arrives."""
        result = agent_client.audit_query_sync(timeout=0.01)
        assert result is None

    def test_audit_query_sync_uses_unique_request_ids(self, agent_client: AgentClient):
        """Two successive audit_query_sync calls use different request IDs."""
        ids = []

        def capture(msg, req_id, timeout):
            ids.append(req_id)
            return None

        agent_client._send_sync_op = capture
        agent_client.audit_query_sync(timeout=0.01)
        agent_client.audit_query_sync(timeout=0.01)
        assert len(ids) == 2
        assert ids[0] != ids[1]


# =============================================================================
# wait_until_disconnected Tests
# =============================================================================

class TestWaitUntilDisconnected:
    """Tests for wait_until_disconnected on BaseAetherClient."""

    def test_returns_immediately_when_stop_event_set(self, agent_client: AgentClient):
        """wait_until_disconnected returns promptly if _stop_event is already set."""
        agent_client._stop_event.set()
        agent_client.wait_until_disconnected()  # should not block

    def test_blocks_until_stop_event_set(self, agent_client: AgentClient):
        """wait_until_disconnected blocks until _stop_event is set from another thread."""
        results = []

        def setter():
            time.sleep(0.05)
            agent_client._stop_event.set()

        t = threading.Thread(target=setter)
        t.start()
        agent_client.wait_until_disconnected()
        results.append("done")
        t.join(timeout=1)
        assert results == ["done"]

    def test_method_exists_on_base_client(self):
        """wait_until_disconnected exists on BaseAetherClient."""
        assert hasattr(BaseAetherClient, "wait_until_disconnected")
        assert callable(BaseAetherClient.wait_until_disconnected)


# =============================================================================
# submit_audit_event Tests
# =============================================================================

class TestSubmitAuditEvent:
    """Tests for foreign audit-event submission on BaseAetherClient."""

    def test_submit_audit_event_happy_path(self, agent_client: AgentClient):
        """submit_audit_event resolves to AuditSubmitResponse(success=True) on ack."""
        from scitrera_aether_client import AuditSubmitResponse

        agent_client.request_queue = queue.Queue()

        results: List = []

        def call():
            results.append(
                agent_client.submit_audit_event(
                    event_type="custom",
                    operation="ingest",
                    metadata={"trace_id": "t-1"},
                    timeout=1.0,
                )
            )

        t = threading.Thread(target=call)
        t.start()

        # Drain the upstream request and deliver a canned ack via the pending
        # request registry (mirrors the dispatch loop's resolution path).
        msg = agent_client.request_queue.get(timeout=0.5)
        assert msg.HasField("submit_audit_event")
        req_id = msg.submit_audit_event.client_request_id
        assert req_id != ""
        with agent_client._pending_requests_lock:
            pending = agent_client._pending_requests.pop(req_id)
        pending.put(aether_pb2.SubmitAuditEventResponse(
            client_request_id=req_id,
            success=True,
        ))

        t.join(timeout=1)
        assert len(results) == 1
        resp = results[0]
        assert isinstance(resp, AuditSubmitResponse)
        assert resp.success is True
        assert resp.client_request_id == req_id
        assert resp.error_code == ""

    def test_submit_audit_event_server_error(self, agent_client: AgentClient):
        """submit_audit_event returns AuditSubmitResponse with the gateway error."""
        from scitrera_aether_client import AuditSubmitResponse

        agent_client.request_queue = queue.Queue()
        results: List = []

        def call():
            results.append(
                agent_client.submit_audit_event(
                    event_type="connection",  # forbidden category
                    operation="open",
                    timeout=1.0,
                )
            )

        t = threading.Thread(target=call)
        t.start()

        msg = agent_client.request_queue.get(timeout=0.5)
        req_id = msg.submit_audit_event.client_request_id
        with agent_client._pending_requests_lock:
            pending = agent_client._pending_requests.pop(req_id)
        pending.put(aether_pb2.SubmitAuditEventResponse(
            client_request_id=req_id,
            success=False,
            error_code="ERR_AUDIT_TYPE_FORBIDDEN",
            error_message="event_type 'connection' is reserved",
        ))

        t.join(timeout=1)
        assert len(results) == 1
        resp = results[0]
        assert isinstance(resp, AuditSubmitResponse)
        assert resp.success is False
        assert resp.error_code == "ERR_AUDIT_TYPE_FORBIDDEN"
        assert "reserved" in resp.error_message

    def test_submit_audit_event_returns_none_on_timeout(self, agent_client: AgentClient):
        """submit_audit_event returns None when no response arrives in time."""
        agent_client.request_queue = queue.Queue()
        result = agent_client.submit_audit_event(
            event_type="custom",
            timeout=0.05,
        )
        assert result is None
