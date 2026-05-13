"""
Pytest configuration and fixtures for the Aether client test suite.

This module provides:
- Mock gRPC server and client fixtures
- Pre-configured client instances for testing
- Common test data fixtures
- Async fixtures for async client testing
"""
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

# Import client classes
from scitrera_aether_client.client import (
    AgentClient,
    MetricsBridgeClient,
    OrchestratorClient,
    TaskClient,
    UserClient,
    WorkflowEngineClient,
)
from scitrera_aether_client.client_async import (
    AsyncAgentClient,
    AsyncMetricsBridgeClient,
    AsyncOrchestratorClient,
    AsyncTaskClient,
    AsyncUserClient,
    AsyncWorkflowEngineClient,
)
from scitrera_aether_client.exceptions import (
    AetherError,
    AuthenticationError,
    ConnectionError,
    DuplicateIdentityError,
    InvalidArgumentError,
    ReconnectionError,
)
from scitrera_aether_client.proto import aether_pb2


# =============================================================================
# Test Configuration Constants
# =============================================================================

DEFAULT_TARGET = "localhost:50051"
TEST_WORKSPACE = "test-workspace"
TEST_IMPLEMENTATION = "test-impl"
TEST_SPECIFIER = "test-spec"
TEST_USER_ID = "test-user-123"
TEST_WINDOW_ID = "test-window-456"
TEST_PROFILES = ["profile-1", "profile-2"]


# =============================================================================
# Common Test Data Fixtures
# =============================================================================

@pytest.fixture
def test_workspace() -> str:
    """Default test workspace identifier."""
    return TEST_WORKSPACE


@pytest.fixture
def test_implementation() -> str:
    """Default test implementation identifier."""
    return TEST_IMPLEMENTATION


@pytest.fixture
def test_specifier() -> str:
    """Default test specifier."""
    return TEST_SPECIFIER


@pytest.fixture
def test_user_id() -> str:
    """Default test user ID."""
    return TEST_USER_ID


@pytest.fixture
def test_window_id() -> str:
    """Default test window ID."""
    return TEST_WINDOW_ID


@pytest.fixture
def test_profiles() -> List[str]:
    """Default test profiles for orchestrator."""
    return TEST_PROFILES.copy()


@pytest.fixture
def test_credentials() -> Dict[str, str]:
    """Test credentials dictionary."""
    return {"token": "test-token-123", "api_key": "test-api-key"}


@pytest.fixture
def test_payload() -> bytes:
    """Sample test payload as bytes."""
    return b'{"message": "Hello, Aether!"}'


@pytest.fixture
def test_metric() -> aether_pb2.Metric:
    """Sample Metric proto for testing send_metric."""
    m = aether_pb2.Metric()
    m.trace_id = "test-trace-1"
    entry = m.entries.add()
    entry.name = "test.counter"
    entry.kind = "counter"
    entry.qty = 1.0
    return m


@pytest.fixture
def test_kv_value() -> bytes:
    """Sample KV store value."""
    return b'{"key": "value", "count": 42}'


@pytest.fixture
def test_checkpoint_data() -> bytes:
    """Sample checkpoint data."""
    return b'{"state": "checkpoint_state", "version": 1}'


# =============================================================================
# Mock gRPC Fixtures
# =============================================================================

@pytest.fixture
def mock_grpc_channel():
    """Create a mock gRPC channel."""
    channel = MagicMock()
    channel.close = MagicMock()
    return channel


@pytest.fixture
def mock_grpc_stub():
    """Create a mock gRPC stub for AetherGateway."""
    stub = MagicMock()
    # Mock the Connect method to return a mock stream
    stub.Connect = MagicMock(return_value=iter([]))
    return stub


@pytest.fixture
def mock_async_grpc_channel():
    """Create a mock async gRPC channel."""
    channel = MagicMock()
    channel.close = AsyncMock()
    return channel


@pytest.fixture
def mock_async_grpc_stub():
    """Create a mock async gRPC stub for AetherGateway."""
    stub = MagicMock()
    # Mock the Connect method to return a mock async iterator
    async def mock_connect(*args, **kwargs):
        # Explicit empty async generator: yields no items.
        if False:
            yield None
    stub.Connect = MagicMock(return_value=mock_connect())
    return stub


# =============================================================================
# Mock Protobuf Message Fixtures
# =============================================================================

@pytest.fixture
def mock_incoming_message(test_payload: bytes) -> aether_pb2.IncomingMessage:
    """Create a mock incoming message."""
    msg = aether_pb2.IncomingMessage()
    msg.source_topic = "ag::test-workspace::test-impl::test-spec"
    msg.payload = test_payload
    msg.message_type = aether_pb2.CHAT
    return msg


@pytest.fixture
def mock_config_snapshot() -> aether_pb2.ConfigSnapshot:
    """Create a mock config snapshot."""
    config = aether_pb2.ConfigSnapshot()
    config.workspace_id = TEST_WORKSPACE
    config.data["config_key"] = "config_value"
    return config


@pytest.fixture
def mock_signal_force_disconnect() -> aether_pb2.Signal:
    """Create a mock FORCE_DISCONNECT signal."""
    signal = aether_pb2.Signal()
    signal.type = aether_pb2.Signal.FORCE_DISCONNECT
    signal.reason = "Test disconnect"
    return signal


@pytest.fixture
def mock_error_response() -> aether_pb2.ErrorResponse:
    """Create a mock error response."""
    error = aether_pb2.ErrorResponse()
    error.code = "TEST_ERROR"
    error.message = "Test error message"
    return error


@pytest.fixture
def mock_kv_response(test_kv_value: bytes) -> aether_pb2.KVResponse:
    """Create a mock KV response."""
    response = aether_pb2.KVResponse()
    response.success = True
    response.value = test_kv_value
    response.key = "test_key"
    return response


@pytest.fixture
def mock_task_assignment() -> aether_pb2.TaskAssignment:
    """Create a mock task assignment."""
    assignment = aether_pb2.TaskAssignment()
    assignment.task_id = "task-123"
    assignment.task_type = "echo"
    assignment.workspace = TEST_WORKSPACE
    return assignment


@pytest.fixture
def mock_checkpoint_response(test_checkpoint_data: bytes) -> aether_pb2.CheckpointResponse:
    """Create a mock checkpoint response."""
    response = aether_pb2.CheckpointResponse()
    response.success = True
    response.data = test_checkpoint_data
    response.key = "default"
    return response


@pytest.fixture
def mock_connection_ack() -> aether_pb2.ConnectionAck:
    """Create a mock connection acknowledgement."""
    ack = aether_pb2.ConnectionAck()
    ack.session_id = "test-session-id-123"
    ack.resumed = False
    return ack


# =============================================================================
# Sync Client Fixtures (not connected)
# =============================================================================

@pytest.fixture
def agent_client(
    test_workspace: str,
    test_implementation: str,
    test_specifier: str,
) -> AgentClient:
    """Create an AgentClient instance (not connected)."""
    return AgentClient(
        workspace=test_workspace,
        implementation=test_implementation,
        specifier=test_specifier,
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def task_client(
    test_workspace: str,
    test_implementation: str,
) -> TaskClient:
    """Create a TaskClient instance (not connected)."""
    return TaskClient(
        workspace=test_workspace,
        implementation=test_implementation,
        unique_specifier="unique-task-1",
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def user_client(
    test_user_id: str,
    test_window_id: str,
) -> UserClient:
    """Create a UserClient instance (not connected)."""
    return UserClient(
        user_id=test_user_id,
        window_id=test_window_id,
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def orchestrator_client(
    test_implementation: str,
    test_profiles: List[str],
) -> OrchestratorClient:
    """Create an OrchestratorClient instance (not connected)."""
    return OrchestratorClient(
        implementation=test_implementation,
        supported_profiles=test_profiles,
        specifier="orch-spec-1",
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def workflow_engine_client() -> WorkflowEngineClient:
    """Create a WorkflowEngineClient instance (not connected)."""
    return WorkflowEngineClient(
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def metrics_bridge_client() -> MetricsBridgeClient:
    """Create a MetricsBridgeClient instance (not connected)."""
    return MetricsBridgeClient(
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


# =============================================================================
# Async Client Fixtures (not connected)
# =============================================================================

@pytest.fixture
def async_agent_client(
    test_workspace: str,
    test_implementation: str,
    test_specifier: str,
) -> AsyncAgentClient:
    """Create an AsyncAgentClient instance (not connected)."""
    return AsyncAgentClient(
        workspace=test_workspace,
        implementation=test_implementation,
        specifier=test_specifier,
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def async_task_client(
    test_workspace: str,
    test_implementation: str,
) -> AsyncTaskClient:
    """Create an AsyncTaskClient instance (not connected)."""
    return AsyncTaskClient(
        workspace=test_workspace,
        implementation=test_implementation,
        unique_specifier="unique-task-1",
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def async_user_client(
    test_user_id: str,
    test_window_id: str,
) -> AsyncUserClient:
    """Create an AsyncUserClient instance (not connected)."""
    return AsyncUserClient(
        user_id=test_user_id,
        window_id=test_window_id,
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def async_orchestrator_client(
    test_implementation: str,
    test_profiles: List[str],
) -> AsyncOrchestratorClient:
    """Create an AsyncOrchestratorClient instance (not connected)."""
    return AsyncOrchestratorClient(
        implementation=test_implementation,
        supported_profiles=test_profiles,
        specifier="orch-spec-1",
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def async_workflow_engine_client() -> AsyncWorkflowEngineClient:
    """Create an AsyncWorkflowEngineClient instance (not connected)."""
    return AsyncWorkflowEngineClient(
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


@pytest.fixture
def async_metrics_bridge_client() -> AsyncMetricsBridgeClient:
    """Create an AsyncMetricsBridgeClient instance (not connected)."""
    return AsyncMetricsBridgeClient(
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
        auto_reconnect=False,
    )


# =============================================================================
# Exception Fixtures
# =============================================================================

@pytest.fixture
def aether_error() -> AetherError:
    """Create a basic AetherError."""
    return AetherError(
        message="Test error",
        code="TEST_CODE",
        details="Test details",
    )


@pytest.fixture
def connection_error() -> ConnectionError:
    """Create a ConnectionError."""
    return ConnectionError(
        message="Failed to connect",
        code="UNAVAILABLE",
        details="Connection refused",
    )


@pytest.fixture
def authentication_error() -> AuthenticationError:
    """Create an AuthenticationError."""
    return AuthenticationError(
        message="Invalid credentials",
        details="Token expired",
    )


@pytest.fixture
def duplicate_identity_error() -> DuplicateIdentityError:
    """Create a DuplicateIdentityError."""
    return DuplicateIdentityError(
        message="Identity already connected",
        identity="ag::test-workspace::test-impl::test-spec",
    )


@pytest.fixture
def reconnection_error() -> ReconnectionError:
    """Create a ReconnectionError."""
    return ReconnectionError(
        message="Failed to reconnect",
        attempts=5,
    )


@pytest.fixture
def invalid_argument_error() -> InvalidArgumentError:
    """Create an InvalidArgumentError."""
    return InvalidArgumentError(
        message="Invalid parameter",
        argument="workspace",
    )


# =============================================================================
# TLS/mTLS Test Fixtures
# =============================================================================

@pytest.fixture
def mock_tls_root_cert() -> bytes:
    """Mock root CA certificate bytes."""
    return b"-----BEGIN CERTIFICATE-----\nMOCK_ROOT_CERT\n-----END CERTIFICATE-----\n"


@pytest.fixture
def mock_tls_client_cert() -> bytes:
    """Mock client certificate bytes."""
    return b"-----BEGIN CERTIFICATE-----\nMOCK_CLIENT_CERT\n-----END CERTIFICATE-----\n"


@pytest.fixture
def mock_tls_client_key() -> bytes:
    """Mock client private key bytes."""
    return b"-----BEGIN PRIVATE KEY-----\nMOCK_CLIENT_KEY\n-----END PRIVATE KEY-----\n"


# =============================================================================
# Callback Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_message_callback() -> MagicMock:
    """Create a mock message callback."""
    return MagicMock()


@pytest.fixture
def mock_config_callback() -> MagicMock:
    """Create a mock config callback."""
    return MagicMock()


@pytest.fixture
def mock_signal_callback() -> MagicMock:
    """Create a mock signal callback."""
    return MagicMock()


@pytest.fixture
def mock_error_callback() -> MagicMock:
    """Create a mock error callback."""
    return MagicMock()


@pytest.fixture
def mock_connect_callback() -> MagicMock:
    """Create a mock connect callback."""
    return MagicMock()


@pytest.fixture
def mock_disconnect_callback() -> MagicMock:
    """Create a mock disconnect callback."""
    return MagicMock()


@pytest.fixture
def mock_async_message_callback() -> AsyncMock:
    """Create a mock async message callback."""
    return AsyncMock()


@pytest.fixture
def mock_async_connect_callback() -> AsyncMock:
    """Create a mock async connect callback."""
    return AsyncMock()


@pytest.fixture
def mock_async_disconnect_callback() -> AsyncMock:
    """Create a mock async disconnect callback."""
    return AsyncMock()


# =============================================================================
# Topic Fixtures
# =============================================================================

@pytest.fixture
def agent_topic(
    test_workspace: str,
    test_implementation: str,
    test_specifier: str,
) -> str:
    """Generate an agent topic string."""
    return f"ag::{test_workspace}::{test_implementation}::{test_specifier}"


@pytest.fixture
def task_topic(
    test_workspace: str,
    test_implementation: str,
) -> str:
    """Generate a unique task topic string."""
    return f"tu::{test_workspace}::{test_implementation}::unique-task-1"


@pytest.fixture
def user_topic(
    test_user_id: str,
    test_window_id: str,
) -> str:
    """Generate a user topic string."""
    return f"us::{test_user_id}::{test_window_id}"


@pytest.fixture
def broadcast_agent_topic(test_workspace: str) -> str:
    """Generate a global agent broadcast topic."""
    return f"ga::{test_workspace}"


@pytest.fixture
def broadcast_user_topic(test_workspace: str) -> str:
    """Generate a global user broadcast topic."""
    return f"gu::{test_workspace}"


# =============================================================================
# Helper Functions for Tests
# =============================================================================

def create_downstream_response(
    payload_type: str,
    **kwargs,
) -> aether_pb2.DownstreamMessage:
    """
    Helper to create a DownstreamMessage with the specified payload type.

    Args:
        payload_type: One of "msg", "config", "signal", "error", "kv",
                      "task_assignment", "checkpoint", "connection_ack"
        **kwargs: Arguments passed to the appropriate payload constructor

    Returns:
        A DownstreamMessage with the specified payload
    """
    response = aether_pb2.DownstreamMessage()

    if payload_type == "msg":
        msg = aether_pb2.IncomingMessage(**kwargs)
        response.msg.CopyFrom(msg)
    elif payload_type == "config":
        config = aether_pb2.ConfigSnapshot(**kwargs)
        response.config.CopyFrom(config)
    elif payload_type == "signal":
        signal = aether_pb2.Signal(**kwargs)
        response.signal.CopyFrom(signal)
    elif payload_type == "error":
        error = aether_pb2.ErrorResponse(**kwargs)
        response.error.CopyFrom(error)
    elif payload_type == "kv":
        kv = aether_pb2.KVResponse(**kwargs)
        response.kv.CopyFrom(kv)
    elif payload_type == "task_assignment":
        assignment = aether_pb2.TaskAssignment(**kwargs)
        response.task_assignment.CopyFrom(assignment)
    elif payload_type == "checkpoint":
        checkpoint = aether_pb2.CheckpointResponse(**kwargs)
        response.checkpoint.CopyFrom(checkpoint)
    elif payload_type == "connection_ack":
        ack = aether_pb2.ConnectionAck(**kwargs)
        response.connection_ack.CopyFrom(ack)

    return response
