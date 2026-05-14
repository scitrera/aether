"""
Unit tests for the asynchronous Aether client.

This module tests:
- Async client initialization for all client types
- BaseAsyncAetherClient functionality
- Async message sending methods
- Async KV operations
- Async checkpoint operations
- Task creation
- Error handling
- TLS configuration
- Async context manager usage
- Callback support for both sync and async functions
"""
import asyncio
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from scitrera_aether_client.client_async import (
    AsyncAgentClient,
    AsyncMetricsBridgeClient,
    AsyncOrchestratorClient,
    AsyncTaskClient,
    AsyncUserClient,
    AsyncWorkflowEngineClient,
    BaseAsyncAetherClient,
    _maybe_await,
)
from scitrera_aether_client._common import (
    CHAT,
    CONTROL,
    EVENT,
    METRIC,
    OPAQUE,
    SELF_ASSIGN,
    TARGETED,
)
from scitrera_aether_client.exceptions import (
    AuthenticationError,
    ConnectionError,
    InvalidArgumentError,
)
from scitrera_aether_client.proto import aether_pb2


# =============================================================================
# Helper Tests
# =============================================================================

class TestMaybeAwait:
    """Tests for the _maybe_await helper function."""

    @pytest.mark.asyncio
    async def test_maybe_await_with_sync_function(self):
        """Test _maybe_await with a synchronous function."""
        def sync_func(x):
            return x * 2

        result = await _maybe_await(sync_func, 5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_maybe_await_with_async_function(self):
        """Test _maybe_await with an asynchronous function."""
        async def async_func(x):
            return x * 2

        result = await _maybe_await(async_func, 5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_maybe_await_with_no_args(self):
        """Test _maybe_await with no arguments."""
        def sync_func():
            return "hello"

        result = await _maybe_await(sync_func)
        assert result == "hello"


# =============================================================================
# BaseAsyncAetherClient Tests
# =============================================================================

class TestBaseAsyncAetherClientInit:
    """Tests for BaseAsyncAetherClient initialization."""

    def test_default_initialization(self):
        """Test default initialization values."""
        client = BaseAsyncAetherClient()

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
        client = BaseAsyncAetherClient(
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
        client = BaseAsyncAetherClient(
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
        client = BaseAsyncAetherClient()

        assert client.on_message is None
        assert client.on_config is None
        assert client.on_signal is None
        assert client.on_error is None
        assert client.on_kv_response is None
        assert client.on_task_assignment is None
        assert client.on_checkpoint_response is None
        assert client.on_connect is None
        assert client.on_disconnect is None


class TestBaseAsyncAetherClientIsRunning:
    """Tests for the is_running property."""

    def test_is_running_initially_true(self):
        """Test that is_running is True initially (stop event not set)."""
        client = BaseAsyncAetherClient()
        assert client.is_running is True

    def test_is_running_false_after_stop(self):
        """Test that is_running is False after stop event is set."""
        client = BaseAsyncAetherClient()
        client._stop_event.set()
        assert client.is_running is False

    def test_is_running_true_during_reconnect(self):
        """Test that is_running is True during reconnection."""
        client = BaseAsyncAetherClient()
        client._stop_event.set()
        client._reconnecting = True
        assert client.is_running is True


class TestBaseAsyncAetherClientBackoff:
    """Tests for backoff calculation."""

    def test_calculate_backoff_first_attempt(self):
        """First reconnect attempt is zero-delay so a clean stream rotation
        (e.g. periodic gateway freshness cuts) doesn't waste a full second
        of dead time before re-establishing."""
        client = BaseAsyncAetherClient(initial_backoff=1.0, max_backoff=30.0, backoff_multiplier=2.0)
        assert client._calculate_backoff(0) == 0.0

    def test_calculate_backoff_multiple_attempts(self):
        """Test backoff increases with attempts."""
        client = BaseAsyncAetherClient(initial_backoff=1.0, max_backoff=30.0, backoff_multiplier=2.0)

        # Second attempt (attempt=1) should be around 2.0 +/- 25%
        backoff = client._calculate_backoff(1)
        assert 1.5 <= backoff <= 2.5

        # Third attempt (attempt=2) should be around 4.0 +/- 25%
        backoff = client._calculate_backoff(2)
        assert 3.0 <= backoff <= 5.0

    def test_calculate_backoff_respects_max(self):
        """Test backoff doesn't exceed max_backoff."""
        client = BaseAsyncAetherClient(initial_backoff=1.0, max_backoff=10.0, backoff_multiplier=2.0)

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


class TestBaseAsyncAetherClientRecoverableError:
    """Tests for recoverable error checking."""

    def test_grpc_unavailable_is_recoverable(self):
        """Test that UNAVAILABLE is recoverable."""
        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Service unavailable")

        client = BaseAsyncAetherClient()
        assert client._is_recoverable_error(error) is True

    def test_grpc_permission_denied_not_recoverable(self):
        """Test that PERMISSION_DENIED is not recoverable."""
        error = MockGrpcError(grpc.StatusCode.PERMISSION_DENIED, "Permission denied")

        client = BaseAsyncAetherClient()
        assert client._is_recoverable_error(error) is False

    def test_grpc_unauthenticated_not_recoverable(self):
        """Test that UNAUTHENTICATED is not recoverable."""
        error = MockGrpcError(grpc.StatusCode.UNAUTHENTICATED, "Unauthenticated")

        client = BaseAsyncAetherClient()
        assert client._is_recoverable_error(error) is False

    def test_grpc_already_exists_not_recoverable(self):
        """Test that ALREADY_EXISTS is not recoverable."""
        error = MockGrpcError(grpc.StatusCode.ALREADY_EXISTS, "Already exists")

        client = BaseAsyncAetherClient()
        assert client._is_recoverable_error(error) is False

    def test_aether_authentication_error_not_recoverable(self):
        """Test that AuthenticationError is not recoverable."""
        error = AuthenticationError(message="test")
        client = BaseAsyncAetherClient()
        assert client._is_recoverable_error(error) is False


class TestBaseAsyncAetherClientClose:
    """Tests for close method."""

    @pytest.mark.asyncio
    async def test_close_sets_stop_event(self):
        """Test that close sets the stop event."""
        client = BaseAsyncAetherClient()
        client._stop_event.clear()

        await client.close()

        assert client._stop_event.is_set()

    @pytest.mark.asyncio
    async def test_close_puts_sentinel_in_queue(self):
        """Test that close puts None sentinel in request queue."""
        client = BaseAsyncAetherClient()

        await client.close()

        assert client._request_queue.get_nowait() is None

    @pytest.mark.asyncio
    async def test_close_closes_channel(self):
        """Test that close closes the gRPC channel."""
        client = BaseAsyncAetherClient()
        mock_channel = MagicMock()
        mock_channel.close = AsyncMock()
        client.channel = mock_channel

        await client.close()

        mock_channel.close.assert_called_once()


class TestBaseAsyncAetherClientContextManager:
    """Tests for async context manager usage."""

    @pytest.mark.asyncio
    async def test_context_manager_enter_returns_client(self):
        """Test that __aenter__ returns the client."""
        client = BaseAsyncAetherClient()

        async with client as c:
            assert c is client

    @pytest.mark.asyncio
    async def test_context_manager_exit_calls_close(self):
        """Test that __aexit__ calls close."""
        client = BaseAsyncAetherClient()

        with patch.object(client, 'close', new_callable=AsyncMock) as mock_close:
            async with client:
                pass
            mock_close.assert_called_once()


class TestBaseAsyncAetherClientKVOperations:
    """Tests for async KV operations."""

    @pytest.mark.asyncio
    async def test_kv_put_nowait(self, test_kv_value: bytes):
        """Test async KV put_nowait operation."""
        client = BaseAsyncAetherClient()

        await client.kv_put_nowait("test_key", test_kv_value, scope="workspace", workspace="test-ws", ttl=3600)

        msg = client._request_queue.get_nowait()
        assert msg.HasField("kv_op")
        assert msg.kv_op.op == aether_pb2.KVOperation.PUT
        assert msg.kv_op.key == "test_key"
        assert msg.kv_op.value == test_kv_value
        assert msg.kv_op.scope == aether_pb2.KVOperation.WORKSPACE
        assert msg.kv_op.workspace == "test-ws"
        assert msg.kv_op.ttl == 3600

    @pytest.mark.asyncio
    async def test_kv_delete_nowait(self):
        """Test async KV delete_nowait operation."""
        client = BaseAsyncAetherClient()

        await client.kv_delete_nowait("test_key", scope="user-workspace", user_id="user-123", workspace="test-ws")

        msg = client._request_queue.get_nowait()
        assert msg.HasField("kv_op")
        assert msg.kv_op.op == aether_pb2.KVOperation.DELETE
        assert msg.kv_op.key == "test_key"
        assert msg.kv_op.scope == aether_pb2.KVOperation.USER_WORKSPACE


class TestBaseAsyncAetherClientProgressOperations:
    """Tests for async progress-reporting (ProgressReport → ProgressKind)."""

    @pytest.mark.asyncio
    async def test_report_progress_chat_kind(self):
        """report_progress(kind=CHAT) lands on the outgoing ProgressReport."""
        client = BaseAsyncAetherClient()

        await client.report_progress(
            task_id="t-1",
            state="running",
            step_name="Processing",
            step_detail="Running agent pipeline...",
            recipient="us::dev@example.com::win-abc",
            request_id="req-1",
            metadata={"thread_id": "chat-t1"},
            kind=aether_pb2.PROGRESS_KIND_CHAT,
        )

        msg = client._request_queue.get_nowait()
        assert msg.HasField("progress")
        report = msg.progress
        assert report.task_id == "t-1"
        assert report.state == "running"
        assert report.kind == aether_pb2.PROGRESS_KIND_CHAT
        assert report.recipient == "us::dev@example.com::win-abc"
        assert report.metadata["thread_id"] == "chat-t1"
        assert report.step.name == "Processing"

    @pytest.mark.asyncio
    async def test_report_progress_default_kind_unspecified(self):
        """Omitting kind results in PROGRESS_KIND_UNSPECIFIED (legacy senders)."""
        client = BaseAsyncAetherClient()

        await client.report_progress(task_id="t-2", state="running")

        msg = client._request_queue.get_nowait()
        assert msg.HasField("progress")
        assert msg.progress.kind == aether_pb2.PROGRESS_KIND_UNSPECIFIED


class TestBaseAsyncAetherClientCheckpointOperations:
    """Tests for async checkpoint operations."""

    @pytest.mark.asyncio
    async def test_checkpoint_save_nowait(self, test_checkpoint_data: bytes):
        """Test async checkpoint save_nowait operation."""
        client = BaseAsyncAetherClient()

        await client.checkpoint_save_nowait(test_checkpoint_data, key="my_checkpoint", ttl=7200)

        msg = client._request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.SAVE
        assert msg.checkpoint_op.key == "my_checkpoint"
        assert msg.checkpoint_op.data == test_checkpoint_data
        assert msg.checkpoint_op.ttl == 7200

    @pytest.mark.asyncio
    async def test_checkpoint_load_nowait(self):
        """Test async checkpoint load_nowait operation."""
        client = BaseAsyncAetherClient()

        await client.checkpoint_load_nowait(key="my_checkpoint")

        msg = client._request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.LOAD
        assert msg.checkpoint_op.key == "my_checkpoint"

    @pytest.mark.asyncio
    async def test_checkpoint_delete_nowait(self):
        """Test async checkpoint delete_nowait operation."""
        client = BaseAsyncAetherClient()

        await client.checkpoint_delete_nowait(key="my_checkpoint")

        msg = client._request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.DELETE
        assert msg.checkpoint_op.key == "my_checkpoint"

    @pytest.mark.asyncio
    async def test_checkpoint_list_nowait(self):
        """Test async checkpoint list_nowait operation."""
        client = BaseAsyncAetherClient()

        await client.checkpoint_list_nowait()

        msg = client._request_queue.get_nowait()
        assert msg.HasField("checkpoint_op")
        assert msg.checkpoint_op.op == aether_pb2.CheckpointOperation.LIST


class TestBaseAsyncAetherClientCreateTask:
    """Tests for async task creation."""

    @pytest.mark.asyncio
    async def test_create_task_self_assign(self):
        """Test async task creation with self-assign mode."""
        client = BaseAsyncAetherClient()

        await client.create_task(
            task_type="echo",
            workspace="test-workspace",
            metadata={"key": "value"},
        )

        msg = client._request_queue.get_nowait()
        assert msg.HasField("create_task")
        assert msg.create_task.task_type == "echo"
        assert msg.create_task.workspace == "test-workspace"
        assert msg.create_task.assignment_mode == SELF_ASSIGN
        assert msg.create_task.metadata["key"] == "value"

    @pytest.mark.asyncio
    async def test_create_task_targeted(self):
        """Test async task creation with targeted mode."""
        client = BaseAsyncAetherClient()

        await client.create_task(
            task_type="process",
            workspace="test-workspace",
            target_agent_id="agent-123",
        )

        msg = client._request_queue.get_nowait()
        assert msg.HasField("create_task")
        assert msg.create_task.assignment_mode == TARGETED
        assert msg.create_task.target_agent_id == "agent-123"

    @pytest.mark.asyncio
    async def test_create_task_with_launch_params(self):
        """Test async task creation with launch parameter overrides."""
        client = BaseAsyncAetherClient()

        await client.create_task(
            task_type="custom",
            workspace="test-workspace",
            launch_param_overrides={"cpu": "4", "memory": "8GB"},
        )

        msg = client._request_queue.get_nowait()
        assert msg.create_task.launch_param_overrides["cpu"] == "4"
        assert msg.create_task.launch_param_overrides["memory"] == "8GB"


class TestBaseAsyncAetherClientTaskQuery:
    """Tests for async task query operations."""

    @pytest.mark.asyncio
    async def test_query_tasks_list(self):
        """Test querying tasks with filters."""
        client = BaseAsyncAetherClient()

        # Call without awaiting response (will timeout, but we check the request)
        import asyncio
        task = asyncio.create_task(client.query_tasks(
            workspace="test-ws", status="running", task_type="echo", limit=10, offset=5, timeout=0.1
        ))
        # Let the queue get populated
        await asyncio.sleep(0.01)

        msg = client._request_queue.get_nowait()
        assert msg.HasField("task_query")
        assert msg.task_query.op == aether_pb2.TaskQuery.LIST
        assert msg.task_query.filter.workspace == "test-ws"
        assert msg.task_query.filter.status == aether_pb2.TASK_STATUS_RUNNING
        assert msg.task_query.filter.task_type == "echo"
        assert msg.task_query.filter.limit == 10
        assert msg.task_query.filter.offset == 5

        # Let timeout complete
        result = await task
        assert result is None  # Timed out (no server response)

    @pytest.mark.asyncio
    async def test_get_task_by_id(self):
        """Test getting a specific task by ID."""
        client = BaseAsyncAetherClient()

        import asyncio
        task = asyncio.create_task(client.get_task("task-123", timeout=0.1))
        await asyncio.sleep(0.01)

        msg = client._request_queue.get_nowait()
        assert msg.HasField("task_query")
        assert msg.task_query.op == aether_pb2.TaskQuery.GET
        assert msg.task_query.task_id == "task-123"

        result = await task
        assert result is None

    @pytest.mark.asyncio
    async def test_cancel_task(self):
        """Test cancelling a task."""
        client = BaseAsyncAetherClient()

        import asyncio
        task = asyncio.create_task(client.cancel_task("task-456", reason="no longer needed", timeout=0.1))
        await asyncio.sleep(0.01)

        msg = client._request_queue.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.CANCEL
        assert msg.task_op.task_id == "task-456"
        assert msg.task_op.reason == "no longer needed"

        result = await task
        assert result is None

    @pytest.mark.asyncio
    async def test_retry_task(self):
        """Test retrying a task."""
        client = BaseAsyncAetherClient()

        import asyncio
        task = asyncio.create_task(client.retry_task("task-789", timeout=0.1))
        await asyncio.sleep(0.01)

        msg = client._request_queue.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.RETRY
        assert msg.task_op.task_id == "task-789"

        result = await task
        assert result is None

    @pytest.mark.asyncio
    async def test_query_tasks_with_response(self):
        """Test that query_tasks returns response when available."""
        client = BaseAsyncAetherClient()

        # Pre-populate the response queue
        response = aether_pb2.TaskQueryResponse(success=True)
        await client._task_query_response_queue.put(response)

        # Should clear the pre-existing response, then send request and timeout
        import asyncio
        result = await client.query_tasks(workspace="test-ws", timeout=0.1)
        # The pre-existing response was cleared, new request timed out
        assert result is None

    @pytest.mark.asyncio
    async def test_cancel_task_with_response(self):
        """Test that cancel_task returns response when available."""
        client = BaseAsyncAetherClient()

        import asyncio

        async def simulate_response():
            # Wait for the sync method to register its request_id
            for _ in range(100):
                await asyncio.sleep(0.01)
                if client._pending_requests:
                    req_id = next(iter(client._pending_requests))
                    fut = client._pending_requests.pop(req_id)
                    response = aether_pb2.TaskOperationResponse(success=True, message="cancelled")
                    if not fut.done():
                        fut.set_result(response)
                    return

        asyncio.create_task(simulate_response())
        result = await client.cancel_task("task-abc", timeout=1.0)

        assert result is not None
        assert result.success is True
        assert result.message == "cancelled"


class TestBaseAsyncAetherClientTLS:
    """Tests for TLS credential building."""

    def test_build_tls_credentials_basic(self, mock_tls_root_cert: bytes):
        """Test building basic TLS credentials."""
        client = BaseAsyncAetherClient(
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
        client = BaseAsyncAetherClient(
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
        client = BaseAsyncAetherClient(
            tls_enabled=True,
            tls_client_cert=mock_tls_client_cert,
        )

        with pytest.raises(InvalidArgumentError):
            client._build_tls_credentials()

    def test_build_tls_credentials_key_without_cert_raises(self, mock_tls_client_key: bytes):
        """Test that providing key without cert raises error."""
        client = BaseAsyncAetherClient(
            tls_enabled=True,
            tls_client_key=mock_tls_client_key,
        )

        with pytest.raises(InvalidArgumentError):
            client._build_tls_credentials()


class TestBaseAsyncAetherClientErrorHandling:
    """Tests for async error handling methods."""

    @pytest.mark.asyncio
    async def test_on_error_with_grpc_error(self):
        """Test _on_error with gRPC error."""
        client = BaseAsyncAetherClient()
        mock_callback = MagicMock()
        client.on_error = mock_callback

        grpc_error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Service unavailable")

        result = await client._on_error(grpc_error)

        assert isinstance(result, ConnectionError)
        mock_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_error_with_async_callback(self):
        """Test _on_error with async callback."""
        client = BaseAsyncAetherClient()
        mock_callback = AsyncMock()
        client.on_error = mock_callback

        grpc_error = MockGrpcError(grpc.StatusCode.UNAVAILABLE, "Service unavailable")

        result = await client._on_error(grpc_error)

        assert isinstance(result, ConnectionError)
        mock_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_error_with_non_recoverable_error_stops_client(self):
        """Test that non-recoverable errors stop the client."""
        client = BaseAsyncAetherClient()
        client._stop_event.clear()

        grpc_error = MockGrpcError(grpc.StatusCode.PERMISSION_DENIED, "Permission denied")

        await client._on_error(grpc_error)

        assert client._stop_event.is_set()

    @pytest.mark.asyncio
    async def test_on_error_with_aether_error(self):
        """Test _on_error with AetherError."""
        client = BaseAsyncAetherClient()
        mock_callback = MagicMock()
        client.on_error = mock_callback

        aether_error = AuthenticationError(message="Invalid token")

        result = await client._on_error(aether_error)

        assert result is aether_error
        mock_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_error_without_callback(self, caplog):
        """Test _on_error logs when no callback set."""
        import logging
        client = BaseAsyncAetherClient()

        error = aether_pb2.ErrorResponse(code="TEST_ERROR", message="Test message")

        with caplog.at_level(logging.ERROR, logger="aether.client.async"):
            await client._on_error(error)

        assert "TEST_ERROR" in caplog.text
        assert "Test message" in caplog.text


# =============================================================================
# AsyncAgentClient Tests
# =============================================================================

class TestAsyncAgentClientInit:
    """Tests for AsyncAgentClient initialization."""

    def test_async_agent_client_init(
        self,
        test_workspace: str,
        test_implementation: str,
        test_specifier: str,
    ):
        """Test AsyncAgentClient initialization."""
        client = AsyncAgentClient(
            workspace=test_workspace,
            implementation=test_implementation,
            specifier=test_specifier,
        )

        assert client.workspace == test_workspace
        assert client.implementation == test_implementation
        assert client.specifier == test_specifier
        assert client.init is not None
        assert client.init.HasField("agent")

    def test_async_agent_client_with_credentials(
        self,
        test_workspace: str,
        test_implementation: str,
        test_specifier: str,
        test_credentials: Dict[str, str],
    ):
        """Test AsyncAgentClient initialization with credentials."""
        client = AsyncAgentClient(
            workspace=test_workspace,
            implementation=test_implementation,
            specifier=test_specifier,
            credentials=test_credentials,
        )

        assert client.init.credentials["token"] == test_credentials["token"]


class TestAsyncAgentClientMessaging:
    """Tests for AsyncAgentClient messaging methods."""

    @pytest.mark.asyncio
    async def test_send_message_to_agent(self, async_agent_client: AsyncAgentClient, test_payload: bytes):
        """Test async sending message to another agent."""
        await async_agent_client.send_message_to_agent(
            workspace="other-ws",
            implementation="other-impl",
            specifier="other-spec",
            payload=test_payload,
        )

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("send")
        assert msg.send.target_topic == "ag::other-ws::other-impl::other-spec"
        assert msg.send.payload == test_payload
        assert msg.send.message_type == OPAQUE

    @pytest.mark.asyncio
    async def test_send_message_to_task(self, async_agent_client: AsyncAgentClient, test_payload: bytes):
        """Test async sending message to a task."""
        await async_agent_client.send_message_to_task(
            workspace="task-ws",
            implementation="task-impl",
            payload=test_payload,
            unique_specifier="task-spec",
        )

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.send.target_topic == "tu::task-ws::task-impl::task-spec"

    @pytest.mark.asyncio
    async def test_send_message_to_user_session(self, async_agent_client: AsyncAgentClient, test_payload: bytes):
        """Test async sending message to a user session."""
        await async_agent_client.send_message_to_user_session(
            user_id="user-123",
            window_id="window-456",
            payload=test_payload,
        )

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.send.target_topic == "us::user-123::window-456"

    @pytest.mark.asyncio
    async def test_send_broadcast_to_agents(self, async_agent_client: AsyncAgentClient, test_payload: bytes):
        """Test async broadcasting to all agents in workspace."""
        await async_agent_client.send_broadcast_to_agents(
            workspace="broadcast-ws",
            payload=test_payload,
        )

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ga::broadcast-ws"

    @pytest.mark.asyncio
    async def test_send_event(self, async_agent_client: AsyncAgentClient, test_payload: bytes):
        """Test async sending event to workflow engine."""
        await async_agent_client.send_event(test_payload)

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.send.target_topic == "event::*"
        assert msg.send.message_type == EVENT

    @pytest.mark.asyncio
    async def test_send_metric(self, async_agent_client: AsyncAgentClient, test_metric):
        """Test async sending metric to metrics bridge."""
        await async_agent_client.send_metric(test_metric)

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.send.target_topic == "metric::*"
        assert msg.send.message_type == METRIC


class TestAsyncAgentClientWorkspace:
    """Tests for AsyncAgentClient workspace switching."""

    @pytest.mark.asyncio
    async def test_switch_workspace(self, async_agent_client: AsyncAgentClient):
        """Test async workspace switching."""
        await async_agent_client.switch_workspace("new-workspace")

        assert async_agent_client.workspace == "new-workspace"

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("switch_workspace")
        assert msg.switch_workspace.new_workspace_id == "new-workspace"


# =============================================================================
# AsyncTaskClient Tests
# =============================================================================

class TestAsyncTaskClientInit:
    """Tests for AsyncTaskClient initialization."""

    def test_async_task_client_with_unique_specifier(
        self,
        test_workspace: str,
        test_implementation: str,
    ):
        """Test AsyncTaskClient with unique specifier (named task)."""
        client = AsyncTaskClient(
            workspace=test_workspace,
            implementation=test_implementation,
            unique_specifier="named-task",
        )

        assert client.unique_specifier == "named-task"
        assert client.init.task.unique_specifier == "named-task"

    def test_async_task_client_without_unique_specifier(
        self,
        test_workspace: str,
        test_implementation: str,
    ):
        """Test AsyncTaskClient without unique specifier (non-unique task)."""
        client = AsyncTaskClient(
            workspace=test_workspace,
            implementation=test_implementation,
        )

        assert client.unique_specifier == ""
        assert client.init.task.unique_specifier == ""


class TestAsyncTaskClientMessaging:
    """Tests for AsyncTaskClient messaging methods."""

    @pytest.mark.asyncio
    async def test_send_message_to_agent(self, async_task_client: AsyncTaskClient, test_payload: bytes):
        """Test async sending message to an agent."""
        await async_task_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = async_task_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"

    @pytest.mark.asyncio
    async def test_send_event(self, async_task_client: AsyncTaskClient, test_payload: bytes):
        """Test async sending event to workflow engine."""
        await async_task_client.send_event(test_payload)

        msg = async_task_client._request_queue.get_nowait()
        assert msg.send.target_topic == "event::*"
        assert msg.send.message_type == EVENT


# =============================================================================
# AsyncUserClient Tests
# =============================================================================

class TestAsyncUserClientInit:
    """Tests for AsyncUserClient initialization."""

    def test_async_user_client_init(
        self,
        test_user_id: str,
        test_window_id: str,
    ):
        """Test AsyncUserClient initialization."""
        client = AsyncUserClient(
            user_id=test_user_id,
            window_id=test_window_id,
        )

        assert client.user_id == test_user_id
        assert client.window_id == test_window_id
        assert client.init.HasField("user")
        assert client.init.user.user_id == test_user_id
        assert client.init.user.window_id == test_window_id


class TestAsyncUserClientMessaging:
    """Tests for AsyncUserClient messaging methods."""

    @pytest.mark.asyncio
    async def test_send_message_to_agent(self, async_user_client: AsyncUserClient, test_payload: bytes):
        """Test async sending message to an agent."""
        await async_user_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = async_user_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"

    @pytest.mark.asyncio
    async def test_send_message_to_task(self, async_user_client: AsyncUserClient, test_payload: bytes):
        """Test async sending message to a task."""
        await async_user_client.send_message_to_task(
            workspace="task-ws",
            implementation="task-impl",
            payload=test_payload,
        )

        msg = async_user_client._request_queue.get_nowait()
        # Non-unique task topic
        assert msg.send.target_topic == "ta::task-ws::task-impl::"

    @pytest.mark.asyncio
    async def test_send_message_to_agent_stamps_app_workspace(
        self, async_user_client: AsyncUserClient, test_payload: bytes
    ):
        """Test that AsyncUserClient.send_message_to_agent stamps app_workspace on SendMessage."""
        await async_user_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
            app_workspace="default",
        )

        msg = async_user_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"
        assert msg.send.app_workspace == "default"

    @pytest.mark.asyncio
    async def test_send_message_to_agent_empty_app_workspace_by_default(
        self, async_user_client: AsyncUserClient, test_payload: bytes
    ):
        """Test that app_workspace defaults to empty string when not supplied."""
        await async_user_client.send_message_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = async_user_client._request_queue.get_nowait()
        assert msg.send.app_workspace == ""


# =============================================================================
# AsyncOrchestratorClient Tests
# =============================================================================

class TestAsyncOrchestratorClientInit:
    """Tests for AsyncOrchestratorClient initialization."""

    def test_async_orchestrator_client_init(
        self,
        test_implementation: str,
        test_profiles: List[str],
    ):
        """Test AsyncOrchestratorClient initialization."""
        client = AsyncOrchestratorClient(
            implementation=test_implementation,
            supported_profiles=test_profiles,
            specifier="orch-1",
        )

        assert client.implementation == test_implementation
        assert client.supported_profiles == test_profiles
        assert client.specifier == "orch-1"
        assert client.init.HasField("orchestrator")

    def test_async_orchestrator_client_generates_specifier(
        self,
        test_implementation: str,
        test_profiles: List[str],
    ):
        """Test that AsyncOrchestratorClient generates a specifier if not provided."""
        client = AsyncOrchestratorClient(
            implementation=test_implementation,
            supported_profiles=test_profiles,
        )

        assert client.specifier is not None
        assert len(client.specifier) == 8  # UUID[:8]

    def test_async_orchestrator_client_requires_implementation(self, test_profiles: List[str]):
        """Test that AsyncOrchestratorClient requires implementation."""
        with pytest.raises(InvalidArgumentError):
            AsyncOrchestratorClient(
                implementation="",
                supported_profiles=test_profiles,
            )

    def test_async_orchestrator_client_requires_profiles(self, test_implementation: str):
        """Test that AsyncOrchestratorClient requires at least one profile."""
        with pytest.raises(InvalidArgumentError):
            AsyncOrchestratorClient(
                implementation=test_implementation,
                supported_profiles=[],
            )


class TestAsyncOrchestratorClientMessaging:
    """Tests for AsyncOrchestratorClient messaging methods."""

    @pytest.mark.asyncio
    async def test_send_status_to_agent(self, async_orchestrator_client: AsyncOrchestratorClient, test_payload: bytes):
        """Test async sending status to an agent."""
        await async_orchestrator_client.send_status_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = async_orchestrator_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"
        assert msg.send.message_type == CONTROL

    @pytest.mark.asyncio
    async def test_send_status_to_task(self, async_orchestrator_client: AsyncOrchestratorClient, test_payload: bytes):
        """Test async sending status to a task."""
        await async_orchestrator_client.send_status_to_task(
            workspace="task-ws",
            implementation="task-impl",
            payload=test_payload,
            unique_specifier="task-spec",
        )

        msg = async_orchestrator_client._request_queue.get_nowait()
        assert msg.send.target_topic == "tu::task-ws::task-impl::task-spec"
        assert msg.send.message_type == CONTROL


# =============================================================================
# AsyncWorkflowEngineClient Tests
# =============================================================================

class TestAsyncWorkflowEngineClientInit:
    """Tests for AsyncWorkflowEngineClient initialization."""

    def test_async_workflow_engine_client_init(self):
        """Test AsyncWorkflowEngineClient initialization."""
        client = AsyncWorkflowEngineClient()

        assert client.init.HasField("workflow_engine")


class TestAsyncWorkflowEngineClientMessaging:
    """Tests for AsyncWorkflowEngineClient messaging methods."""

    @pytest.mark.asyncio
    async def test_send_command_to_agent(
        self,
        async_workflow_engine_client: AsyncWorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test async sending command to an agent."""
        await async_workflow_engine_client.send_command_to_agent(
            workspace="agent-ws",
            implementation="agent-impl",
            specifier="agent-spec",
            payload=test_payload,
        )

        msg = async_workflow_engine_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ag::agent-ws::agent-impl::agent-spec"
        assert msg.send.message_type == CONTROL

    @pytest.mark.asyncio
    async def test_send_broadcast_to_agents(
        self,
        async_workflow_engine_client: AsyncWorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test async broadcasting to all agents."""
        await async_workflow_engine_client.send_broadcast_to_agents(
            workspace="broadcast-ws",
            payload=test_payload,
        )

        msg = async_workflow_engine_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ga::broadcast-ws"
        assert msg.send.message_type == CONTROL

    @pytest.mark.asyncio
    async def test_send_broadcast_to_users(
        self,
        async_workflow_engine_client: AsyncWorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test async broadcasting to all users."""
        await async_workflow_engine_client.send_broadcast_to_users(
            workspace="broadcast-ws",
            payload=test_payload,
        )

        msg = async_workflow_engine_client._request_queue.get_nowait()
        assert msg.send.target_topic == "gu::broadcast-ws"
        assert msg.send.message_type == OPAQUE

    @pytest.mark.asyncio
    async def test_send_message_to_user(
        self,
        async_workflow_engine_client: AsyncWorkflowEngineClient,
        test_payload: bytes,
    ):
        """Test async sending message to a specific user."""
        await async_workflow_engine_client.send_message_to_user(
            user_id="user-123",
            window_id="window-456",
            payload=test_payload,
        )

        msg = async_workflow_engine_client._request_queue.get_nowait()
        assert msg.send.target_topic == "us::user-123::window-456"

    @pytest.mark.asyncio
    async def test_send_metric(
        self,
        async_workflow_engine_client: AsyncWorkflowEngineClient,
        test_metric,
    ):
        """Test async sending metric to metrics bridge."""
        await async_workflow_engine_client.send_metric(test_metric)

        msg = async_workflow_engine_client._request_queue.get_nowait()
        assert msg.send.target_topic == "metric::*"
        assert msg.send.message_type == METRIC


# =============================================================================
# AsyncMetricsBridgeClient Tests
# =============================================================================

class TestAsyncMetricsBridgeClientInit:
    """Tests for AsyncMetricsBridgeClient initialization."""

    def test_async_metrics_bridge_client_init(self):
        """Test AsyncMetricsBridgeClient initialization."""
        client = AsyncMetricsBridgeClient()

        assert client.init.HasField("metrics_bridge")


class TestAsyncMetricsBridgeClientMessaging:
    """Tests for AsyncMetricsBridgeClient messaging methods."""

    @pytest.mark.asyncio
    async def test_send_acknowledgment(
        self,
        async_metrics_bridge_client: AsyncMetricsBridgeClient,
        test_payload: bytes,
    ):
        """Test async sending acknowledgment."""
        await async_metrics_bridge_client.send_acknowledgment(
            target_topic="ag::ws::impl::spec",
            payload=test_payload,
        )

        msg = async_metrics_bridge_client._request_queue.get_nowait()
        assert msg.send.target_topic == "ag::ws::impl::spec"
        assert msg.send.message_type == CONTROL


# =============================================================================
# Async Callback Assignment Tests
# =============================================================================

class TestAsyncCallbackAssignment:
    """Tests for async callback assignment."""

    def test_can_assign_sync_message_callback(
        self,
        async_agent_client: AsyncAgentClient,
        mock_message_callback: MagicMock,
    ):
        """Test assigning sync message callback."""
        async_agent_client.on_message = mock_message_callback
        assert async_agent_client.on_message is mock_message_callback

    def test_can_assign_async_message_callback(
        self,
        async_agent_client: AsyncAgentClient,
        mock_async_message_callback: AsyncMock,
    ):
        """Test assigning async message callback."""
        async_agent_client.on_message = mock_async_message_callback
        assert async_agent_client.on_message is mock_async_message_callback

    def test_can_assign_sync_connect_callback(
        self,
        async_agent_client: AsyncAgentClient,
        mock_connect_callback: MagicMock,
    ):
        """Test assigning sync connect callback."""
        async_agent_client.on_connect = mock_connect_callback
        assert async_agent_client.on_connect is mock_connect_callback

    def test_can_assign_async_connect_callback(
        self,
        async_agent_client: AsyncAgentClient,
        mock_async_connect_callback: AsyncMock,
    ):
        """Test assigning async connect callback."""
        async_agent_client.on_connect = mock_async_connect_callback
        assert async_agent_client.on_connect is mock_async_connect_callback

    def test_can_assign_sync_disconnect_callback(
        self,
        async_agent_client: AsyncAgentClient,
        mock_disconnect_callback: MagicMock,
    ):
        """Test assigning sync disconnect callback."""
        async_agent_client.on_disconnect = mock_disconnect_callback
        assert async_agent_client.on_disconnect is mock_disconnect_callback

    def test_can_assign_async_disconnect_callback(
        self,
        async_agent_client: AsyncAgentClient,
        mock_async_disconnect_callback: AsyncMock,
    ):
        """Test assigning async disconnect callback."""
        async_agent_client.on_disconnect = mock_async_disconnect_callback
        assert async_agent_client.on_disconnect is mock_async_disconnect_callback


# =============================================================================
# Message Type Tests for Async Client
# =============================================================================

class TestAsyncMessageTypes:
    """Tests for message type handling in async client."""

    @pytest.mark.asyncio
    async def test_send_message_with_custom_type(self, async_agent_client: AsyncAgentClient, test_payload: bytes):
        """Test async sending message with custom message type."""
        await async_agent_client.send_message_to_agent(
            workspace="ws",
            implementation="impl",
            specifier="spec",
            payload=test_payload,
            message_type=CONTROL,
        )

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.send.message_type == CONTROL


# =============================================================================
# Request Generator Tests for Async Client
# =============================================================================

class TestAsyncRequestGenerator:
    """Tests for the async request generator."""

    @pytest.mark.asyncio
    async def test_request_generator_yields_queued_messages(self):
        """Test that async request generator yields queued messages."""
        client = BaseAsyncAetherClient()

        # Queue a message
        msg = aether_pb2.UpstreamMessage()
        await client._request_queue.put(msg)
        await client._request_queue.put(None)  # Sentinel to stop

        generator = client._request_generator()
        yielded_msg = await generator.__anext__()

        assert yielded_msg is msg

    @pytest.mark.asyncio
    async def test_request_generator_stops_on_sentinel(self):
        """Test that async request generator stops on None sentinel."""
        client = BaseAsyncAetherClient()

        await client._request_queue.put(None)

        generator = client._request_generator()
        messages = []
        async for msg in generator:
            messages.append(msg)

        assert len(messages) == 0

    @pytest.mark.asyncio
    async def test_request_generator_stops_on_stop_event(self):
        """Test that async request generator respects stop event."""
        client = BaseAsyncAetherClient()
        client._stop_event.set()

        generator = client._request_generator()
        messages = []

        # Use a timeout to prevent hanging
        try:
            async with asyncio.timeout(0.5):
                async for msg in generator:
                    messages.append(msg)
        except asyncio.TimeoutError:
            # Intentionally ignore TimeoutError: the timeout is only to prevent the test
            # from hanging if the generator does not terminate promptly.
            pass

        assert len(messages) == 0


# =============================================================================
# Sync Response Tests with Timeout
# =============================================================================

class TestAsyncSyncOperationsWithTimeout:
    """Tests for synchronous-style async operations with timeout."""

    @pytest.mark.asyncio
    async def test_kv_put_timeout(self, test_kv_value: bytes):
        """Test async kv_put returns None on timeout."""
        client = BaseAsyncAetherClient()

        result = await client.kv_put("test_key", test_kv_value, timeout=0.1)

        assert result is None

    @pytest.mark.asyncio
    async def test_kv_delete_timeout(self):
        """Test async kv_delete returns None on timeout."""
        client = BaseAsyncAetherClient()

        result = await client.kv_delete("test_key", timeout=0.1)

        assert result is None

    @pytest.mark.asyncio
    async def test_checkpoint_save_timeout(self, test_checkpoint_data: bytes):
        """Test async checkpoint_save returns None on timeout."""
        client = BaseAsyncAetherClient()

        result = await client.checkpoint_save(test_checkpoint_data, timeout=0.1)

        assert result is None

    @pytest.mark.asyncio
    async def test_checkpoint_load_timeout(self):
        """Test async checkpoint_load returns None on timeout."""
        client = BaseAsyncAetherClient()

        result = await client.checkpoint_load(timeout=0.1)

        assert result is None

    @pytest.mark.asyncio
    async def test_checkpoint_delete_timeout(self):
        """Test async checkpoint_delete returns None on timeout."""
        client = BaseAsyncAetherClient()

        result = await client.checkpoint_delete(timeout=0.1)

        assert result is None

    @pytest.mark.asyncio
    async def test_checkpoint_list_timeout(self):
        """Test async checkpoint_list returns None on timeout."""
        client = BaseAsyncAetherClient()

        result = await client.checkpoint_list(timeout=0.1)

        assert result is None


# =============================================================================
# Response Handling Tests
# =============================================================================

class TestAsyncResponseHandling:
    """Tests for async response handling with pre-populated queues."""

    @pytest.mark.asyncio
    async def test_checkpoint_save_with_response(self, test_checkpoint_data: bytes):
        """Test async checkpoint_save with immediate response."""
        client = BaseAsyncAetherClient()

        async def add_response():
            for _ in range(100):
                await asyncio.sleep(0.01)
                if client._pending_requests:
                    req_id = next(iter(client._pending_requests))
                    fut = client._pending_requests.pop(req_id)
                    response = aether_pb2.CheckpointResponse(success=True)
                    if not fut.done():
                        fut.set_result(response)
                    return

        task = asyncio.create_task(add_response())

        result = await client.checkpoint_save(test_checkpoint_data, timeout=1.0)
        await task

        assert result is not None
        assert result.success is True

    @pytest.mark.asyncio
    async def test_checkpoint_load_with_response(self):
        """Test async checkpoint_load with response."""
        client = BaseAsyncAetherClient()

        async def add_response():
            for _ in range(100):
                await asyncio.sleep(0.01)
                if client._pending_requests:
                    req_id = next(iter(client._pending_requests))
                    fut = client._pending_requests.pop(req_id)
                    response = aether_pb2.CheckpointResponse(success=True, data=b"checkpoint data")
                    if not fut.done():
                        fut.set_result(response)
                    return

        task = asyncio.create_task(add_response())

        result = await client.checkpoint_load(timeout=1.0)
        await task

        assert result is not None
        assert result.data == b"checkpoint data"

    @pytest.mark.asyncio
    async def test_kv_put_with_response(self, test_kv_value: bytes):
        """Test async kv_put with response."""
        client = BaseAsyncAetherClient()

        async def add_response():
            for _ in range(100):
                await asyncio.sleep(0.01)
                if client._pending_requests:
                    req_id = next(iter(client._pending_requests))
                    fut = client._pending_requests.pop(req_id)
                    response = aether_pb2.KVResponse(success=True)
                    if not fut.done():
                        fut.set_result(response)
                    return

        task = asyncio.create_task(add_response())

        result = await client.kv_put("test_key", test_kv_value, timeout=1.0)
        await task

        assert result is not None
        assert result.success is True

    @pytest.mark.asyncio
    async def test_create_task_sync_with_response(self):
        """create_task_sync receives the correlated CreateTaskResponse and returns it."""
        client = BaseAsyncAetherClient()

        async def add_response():
            for _ in range(100):
                await asyncio.sleep(0.01)
                if client._pending_requests:
                    req_id = next(iter(client._pending_requests))
                    fut = client._pending_requests.pop(req_id)
                    response = aether_pb2.CreateTaskResponse(
                        success=True,
                        task_id="task-xyz",
                        status="pending",
                        request_id=req_id,
                    )
                    if not fut.done():
                        fut.set_result(response)
                    return

        task = asyncio.create_task(add_response())

        result = await client.create_task_sync(
            task_type="sandbox_lease",
            workspace="_apps",
            timeout=1.0,
        )
        await task

        assert result is not None
        assert result.success is True
        assert result.task_id == "task-xyz"

    @pytest.mark.asyncio
    async def test_create_task_sync_timeout(self):
        """create_task_sync returns None when no response arrives within timeout."""
        client = BaseAsyncAetherClient()

        result = await client.create_task_sync(
            task_type="sandbox_lease",
            workspace="_apps",
            timeout=0.1,
        )

        assert result is None


# =============================================================================
# Async Task Lifecycle Tests (complete_task / fail_task)
# =============================================================================

class TestAsyncTaskLifecycle:
    """Tests for complete_task and fail_task on async client."""

    def test_complete_task_method_exists(self, async_agent_client: AsyncAgentClient):
        """complete_task method exists on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, 'complete_task')
        assert callable(async_agent_client.complete_task)

    def test_fail_task_method_exists(self, async_agent_client: AsyncAgentClient):
        """fail_task method exists on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, 'fail_task')
        assert callable(async_agent_client.fail_task)

    @pytest.mark.asyncio
    async def test_complete_task_queues_upstream_message(self, async_agent_client: AsyncAgentClient):
        """complete_task puts a COMPLETE TaskOperation on the request queue."""
        response = aether_pb2.TaskOperationResponse(success=True)
        await async_agent_client._task_op_response_queue.put(response)

        result = await async_agent_client.complete_task("task-123", timeout=1.0)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.COMPLETE
        assert msg.task_op.task_id == "task-123"

    @pytest.mark.asyncio
    async def test_fail_task_queues_upstream_message_with_reason(self, async_agent_client: AsyncAgentClient):
        """fail_task puts a FAIL TaskOperation with reason on the request queue."""
        response = aether_pb2.TaskOperationResponse(success=True)
        await async_agent_client._task_op_response_queue.put(response)

        await async_agent_client.fail_task("task-456", reason="something broke", timeout=1.0)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("task_op")
        assert msg.task_op.op == aether_pb2.TaskOperation.FAIL
        assert msg.task_op.task_id == "task-456"
        assert msg.task_op.reason == "something broke"

    @pytest.mark.asyncio
    async def test_complete_task_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """complete_task returns None when no response arrives within timeout."""
        result = await async_agent_client.complete_task("task-789", timeout=0.05)
        assert result is None

    @pytest.mark.asyncio
    async def test_fail_task_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """fail_task returns None when no response arrives within timeout."""
        result = await async_agent_client.fail_task("task-789", reason="err", timeout=0.05)
        assert result is None


# =============================================================================
# Async Workspace Operation Tests
# =============================================================================

class TestAsyncWorkspaceOps:
    """Tests for workspace operations on async client."""

    def test_workspace_op_method_exists(self, async_agent_client: AsyncAgentClient):
        """workspace_op method exists on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, 'workspace_op')
        assert callable(async_agent_client.workspace_op)

    def test_workspace_response_queue_initialized(self, async_agent_client: AsyncAgentClient):
        """_workspace_response_queue is initialized on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, '_workspace_response_queue')

    def test_workspace_response_callback_initializable(self, async_agent_client: AsyncAgentClient):
        """on_workspace_response callback attribute exists and is assignable."""
        assert hasattr(async_agent_client, 'on_workspace_response')
        async_agent_client.on_workspace_response = lambda resp: None
        assert async_agent_client.on_workspace_response is not None

    @pytest.mark.asyncio
    async def test_workspace_op_queues_upstream_message(self, async_agent_client: AsyncAgentClient):
        """workspace_op puts a WorkspaceOperation on the request queue."""
        op = aether_pb2.WorkspaceOperation()
        await async_agent_client._workspace_response_queue.put(object())  # mock response

        await async_agent_client.workspace_op(op, timeout=1.0)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("workspace_op")

    @pytest.mark.asyncio
    async def test_workspace_op_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """workspace_op returns None when no response arrives within timeout."""
        op = aether_pb2.WorkspaceOperation()
        result = await async_agent_client.workspace_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Async Agent Operation Tests
# =============================================================================

class TestAsyncAgentOps:
    """Tests for agent operations on async client."""

    def test_agent_op_method_exists(self, async_agent_client: AsyncAgentClient):
        """agent_op method exists on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, 'agent_op')
        assert callable(async_agent_client.agent_op)

    def test_agent_response_queue_initialized(self, async_agent_client: AsyncAgentClient):
        """_agent_response_queue is initialized on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, '_agent_response_queue')

    @pytest.mark.asyncio
    async def test_agent_op_queues_upstream_message(self, async_agent_client: AsyncAgentClient):
        """agent_op puts an AgentOperation on the request queue."""
        op = aether_pb2.AgentOperation()
        await async_agent_client._agent_response_queue.put(object())  # mock response

        await async_agent_client.agent_op(op, timeout=1.0)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("agent_op")

    @pytest.mark.asyncio
    async def test_agent_op_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """agent_op returns None when no response arrives within timeout."""
        op = aether_pb2.AgentOperation()
        result = await async_agent_client.agent_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Async ACL Operation Tests
# =============================================================================

class TestAsyncACLOps:
    """Tests for ACL operations on async client."""

    def test_acl_op_method_exists(self, async_agent_client: AsyncAgentClient):
        """acl_op method exists on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, 'acl_op')
        assert callable(async_agent_client.acl_op)

    def test_acl_response_queue_initialized(self, async_agent_client: AsyncAgentClient):
        """_acl_response_queue is initialized on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, '_acl_response_queue')

    @pytest.mark.asyncio
    async def test_acl_op_queues_upstream_message(self, async_agent_client: AsyncAgentClient):
        """acl_op puts an ACLOperation on the request queue."""
        op = aether_pb2.ACLOperation()
        await async_agent_client._acl_response_queue.put(object())  # mock response

        await async_agent_client.acl_op(op, timeout=1.0)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("acl_op")

    @pytest.mark.asyncio
    async def test_acl_op_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """acl_op returns None when no response arrives within timeout."""
        op = aether_pb2.ACLOperation()
        result = await async_agent_client.acl_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Async Workflow Operation Tests
# =============================================================================

class TestAsyncWorkflowOps:
    """Tests for workflow operations on async client."""

    def test_workflow_op_method_exists(self, async_agent_client: AsyncAgentClient):
        """workflow_op method exists on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, 'workflow_op')
        assert callable(async_agent_client.workflow_op)

    def test_workflow_response_queue_initialized(self, async_agent_client: AsyncAgentClient):
        """_workflow_response_queue is initialized on BaseAsyncAetherClient."""
        assert hasattr(async_agent_client, '_workflow_response_queue')

    @pytest.mark.asyncio
    async def test_workflow_op_queues_upstream_message(self, async_agent_client: AsyncAgentClient):
        """workflow_op puts a WorkflowOperation on the request queue."""
        op = aether_pb2.WorkflowOperation()
        await async_agent_client._workflow_response_queue.put(object())  # mock response

        await async_agent_client.workflow_op(op, timeout=1.0)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("workflow_op")

    @pytest.mark.asyncio
    async def test_workflow_op_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """workflow_op returns None when no response arrives within timeout."""
        op = aether_pb2.WorkflowOperation()
        result = await async_agent_client.workflow_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Async Token Operation Tests
# =============================================================================

class TestAsyncTokenCRUD:
    """Tests for token CRUD operations on async client."""

    def test_list_tokens_method_exists(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'list_tokens')
        assert callable(async_agent_client.list_tokens)

    def test_get_token_method_exists(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'get_token')
        assert callable(async_agent_client.get_token)

    def test_create_token_method_exists(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'create_token')
        assert callable(async_agent_client.create_token)

    def test_delete_token_method_exists(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'delete_token')
        assert callable(async_agent_client.delete_token)

    def test_revoke_token_method_exists(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'revoke_token')
        assert callable(async_agent_client.revoke_token)

    def test_token_op_low_level_exists(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'token_op')
        assert callable(async_agent_client.token_op)

    def test_pending_requests_initialized(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, '_pending_requests')
        assert isinstance(async_agent_client._pending_requests, dict)

    def test_token_response_callback_initializable(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'on_token_response')

    @pytest.mark.asyncio
    async def test_create_token_queues_message(self, async_agent_client: AsyncAgentClient):
        """Verify create_token puts the correct proto on the request queue."""
        async_agent_client._request_queue = asyncio.Queue()

        async def call():
            await async_agent_client.create_token("test-key", "agent", timeout=0.1)

        task = asyncio.create_task(call())
        await asyncio.sleep(0.05)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField('token_op')
        assert msg.token_op.op == aether_pb2.TokenOperation.CREATE
        assert msg.token_op.create_request.name == "test-key"
        assert msg.token_op.create_request.principal_type == "agent"
        assert msg.token_op.request_id != ""
        await task

    @pytest.mark.asyncio
    async def test_token_op_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """token_op returns None when no response arrives within timeout."""
        op = aether_pb2.TokenOperation()
        result = await async_agent_client.token_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Async Authority Grant Operation Tests
# =============================================================================

class TestAsyncAuthorityGrantOps:
    """Tests for runtime and admin authority-grant operations on async client."""

    def test_authority_grant_op_method_exists(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'authority_grant_op')
        assert callable(async_agent_client.authority_grant_op)

    def test_authority_grant_response_queue_initialized(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, '_authority_grant_response_queue')

    def test_authority_grant_response_callback_initializable(self, async_agent_client: AsyncAgentClient):
        assert hasattr(async_agent_client, 'on_authority_grant_response')

    @pytest.mark.asyncio
    async def test_exchange_authority_grant_queues_message(self, async_agent_client: AsyncAgentClient):
        """Verify exchange_authority_grant puts the correct proto on the async request queue."""
        async_agent_client._request_queue = asyncio.Queue()

        async def call():
            await async_agent_client.exchange_authority_grant(
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

        task = asyncio.create_task(call())
        await asyncio.sleep(0.05)

        assert not async_agent_client._request_queue.empty()
        msg = async_agent_client._request_queue.get_nowait()
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
        await task

    @pytest.mark.asyncio
    async def test_authority_grant_op_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """authority_grant_op returns None when no response arrives within timeout."""
        op = aether_pb2.AuthorityGrantOperation()
        result = await async_agent_client.authority_grant_op(op, timeout=0.05)
        assert result is None


# =============================================================================
# Async submit_audit_event Tests
# =============================================================================

class TestAsyncSubmitAuditEvent:
    """Tests for async foreign audit-event submission on BaseAsyncAetherClient."""

    @pytest.mark.asyncio
    async def test_submit_audit_event_happy_path(self, async_agent_client: AsyncAgentClient):
        """submit_audit_event resolves to AuditSubmitResponse(success=True) on ack."""
        from scitrera_aether_client import AuditSubmitResponse

        async_agent_client._request_queue = asyncio.Queue()

        async def call():
            return await async_agent_client.submit_audit_event(
                event_type="custom",
                operation="ingest",
                metadata={"trace_id": "t-1"},
                timeout=1.0,
            )

        task = asyncio.create_task(call())
        # Wait for the request to be queued.
        msg = await asyncio.wait_for(async_agent_client._request_queue.get(), timeout=0.5)
        assert msg.HasField("submit_audit_event")
        req_id = msg.submit_audit_event.client_request_id
        assert req_id != ""

        # Resolve the pending future as the dispatch loop would.
        fut = async_agent_client._pending_requests.pop(req_id)
        fut.set_result(aether_pb2.SubmitAuditEventResponse(
            client_request_id=req_id,
            success=True,
        ))

        resp = await asyncio.wait_for(task, timeout=1.0)
        assert isinstance(resp, AuditSubmitResponse)
        assert resp.success is True
        assert resp.client_request_id == req_id
        assert resp.error_code == ""

    @pytest.mark.asyncio
    async def test_submit_audit_event_server_error(self, async_agent_client: AsyncAgentClient):
        """submit_audit_event returns AuditSubmitResponse with the gateway error."""
        from scitrera_aether_client import AuditSubmitResponse

        async_agent_client._request_queue = asyncio.Queue()

        async def call():
            return await async_agent_client.submit_audit_event(
                event_type="connection",
                operation="open",
                timeout=1.0,
            )

        task = asyncio.create_task(call())
        msg = await asyncio.wait_for(async_agent_client._request_queue.get(), timeout=0.5)
        req_id = msg.submit_audit_event.client_request_id
        fut = async_agent_client._pending_requests.pop(req_id)
        fut.set_result(aether_pb2.SubmitAuditEventResponse(
            client_request_id=req_id,
            success=False,
            error_code="ERR_AUDIT_TYPE_FORBIDDEN",
            error_message="event_type 'connection' is reserved",
        ))

        resp = await asyncio.wait_for(task, timeout=1.0)
        assert isinstance(resp, AuditSubmitResponse)
        assert resp.success is False
        assert resp.error_code == "ERR_AUDIT_TYPE_FORBIDDEN"
        assert "reserved" in resp.error_message

    @pytest.mark.asyncio
    async def test_submit_audit_event_returns_none_on_timeout(self, async_agent_client: AsyncAgentClient):
        """submit_audit_event returns None when no response arrives within timeout."""
        async_agent_client._request_queue = asyncio.Queue()
        result = await async_agent_client.submit_audit_event(
            event_type="custom",
            timeout=0.05,
        )
        assert result is None


# =============================================================================
# ErrorResponse request_id correlation tests
# =============================================================================

class TestListenLoopErrorCorrelation:
    """Tests for ErrorResponse.request_id correlation in _listen_loop."""

    @pytest.mark.asyncio
    async def test_correlated_error_rejects_pending_future(self):
        """A correlated ErrorResponse (non-empty request_id) should set_exception
        on the matching pending future with the appropriate AetherError subclass."""
        from scitrera_aether_client.exceptions import PermissionDeniedError

        client = BaseAsyncAetherClient(auto_reconnect=False)

        # Pre-register a pending future for request "abc-123".
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        client._pending_requests["abc-123"] = fut

        # Build a DownstreamMessage carrying an ErrorResponse with request_id.
        err_response = aether_pb2.ErrorResponse(
            code="ERR_PERMISSION_DENIED",
            message="denied",
            request_id="abc-123",
        )
        downstream = aether_pb2.DownstreamMessage(error=err_response)

        # Inject a mock stream that yields one message then stops.
        async def _mock_stream():
            yield downstream

        client._stream = _mock_stream()
        client._stop_event.clear()

        await client._listen_loop()

        # The future must be done and must carry a PermissionDeniedError.
        assert fut.done(), "Future should have been resolved by the listen loop"
        exc = fut.exception()
        assert isinstance(exc, PermissionDeniedError), (
            f"Expected PermissionDeniedError, got {type(exc).__name__}: {exc}"
        )
        assert exc.code == "ERR_PERMISSION_DENIED"
        assert exc.message == "denied"
        # Future must have been removed from pending map.
        assert "abc-123" not in client._pending_requests

    @pytest.mark.asyncio
    async def test_uncorrelated_error_routes_to_on_error(self):
        """An ErrorResponse with empty request_id should call the global _on_error
        handler and must NOT reject any pending future."""
        client = BaseAsyncAetherClient(auto_reconnect=False)

        errors_received = []
        client.on_error = lambda err: errors_received.append(err)

        # Register a pending future that should remain untouched.
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        client._pending_requests["should-not-be-touched"] = fut

        # Build an ErrorResponse with an empty request_id (un-correlated).
        err_response = aether_pb2.ErrorResponse(
            code="CONNECTION_ERROR",
            message="connection-scoped error",
            request_id="",  # empty — must not match anything
        )
        downstream = aether_pb2.DownstreamMessage(error=err_response)

        async def _mock_stream():
            yield downstream

        client._stream = _mock_stream()
        client._stop_event.clear()

        await client._listen_loop()

        # The pending future for an unrelated request_id must remain untouched.
        assert not fut.done(), "Pending future should not have been touched"
        # The global error handler should have been invoked.
        assert len(errors_received) == 1
        assert errors_received[0].code == "CONNECTION_ERROR"
        # Cleanup.
        fut.cancel()

