__version__ = "0.1.60"

# Import the proxy module for its side effect: installs the
# ``ProxyHttpResponse`` / ``ProxyHttpBodyChunk`` dispatcher hook on
# ``BaseAetherClient._do_connect`` and ``BaseAsyncAetherClient._do_connect``.
# Hooks must be in place BEFORE any client opens its gRPC connection,
# otherwise the responses iterator is the unwrapped one and inbound proxy
# frames are silently dropped (calls to ``proxy_http`` / ``proxy_http_async``
# would hang until their timeout fires). Importing here means every consumer
# of the SDK gets the hooks for free, with no requirement to import
# ``scitrera_aether_client.proxy`` explicitly.
from . import proxy as _proxy_side_effect  # noqa: F401

from .client import (
    # Client classes (sync)
    AgentClient,
    TaskClient,
    UserClient,
    OrchestratorClient,
    WorkflowEngineClient,
    MetricsBridgeClient,
    ServiceClient,
    # Audit submission response
    AuditSubmitResponse,
)

from .client_async import (
    # Client classes (async)
    AsyncAgentClient,
    AsyncTaskClient,
    AsyncUserClient,
    AsyncServiceClient,
    AsyncOrchestratorClient,
    AsyncWorkflowEngineClient,
    AsyncMetricsBridgeClient,
)

from .orchestrator import (
    # Orchestrator classes
    BaseOrchestrator,
    LaunchedProcess,
    MultiprocessOrchestrator,
    SubprocessInfo,
)

from .admin import AdminClient
from .admin_async import AsyncAdminClient

from .metrics import (
    MetricBuilder,
    new_metric,
)

from ._common import (
    # Message type constants
    MESSAGE_TYPE_UNSPECIFIED,
    OPAQUE,
    CHAT,
    CONTROL,
    TOOL_CALL,
    EVENT,
    METRIC,

    # Task assignment mode constants
    SELF_ASSIGN,
    TARGETED,
    POOL,

    # KV operation type constants
    KV_GET,
    KV_PUT,
    KV_LIST,
    KV_DELETE,

    # KV scope constants
    KV_SCOPE_GLOBAL,
    KV_SCOPE_WORKSPACE,
    KV_SCOPE_USER,
    KV_SCOPE_USER_WORKSPACE,
    KV_SCOPE_GLOBAL_EXCLUSIVE,
    KV_SCOPE_WORKSPACE_EXCLUSIVE,
    KV_SCOPE_USER_SHARED,
    KV_SCOPE_USER_WORKSPACE_SHARED,

    # Auth credentials builder
    Credentials,
)

from .exceptions import (
    # Base exception
    AetherError,
    # Connection exceptions
    ConnectionError,
    ConnectionClosedError,
    ReconnectionError,
    # Auth exceptions
    AuthenticationError,
    PermissionDeniedError,
    # Identity exceptions
    DuplicateIdentityError,
    # Timeout exception
    TimeoutError,
    # Request/Response exceptions
    InvalidArgumentError,
    NotFoundError,
    NotImplementedError,
    # Message/Protocol exceptions
    MessageError,
    KVOperationError,
    CheckpointError,
    # Utility functions
    from_grpc_error,
    is_recoverable_error,
)

from .types import (
    # Configuration TypedDicts
    ConnectionConfig,
    TLSConfig,
    AgentConfig,
    TaskConfig,
    UserConfig,
    OrchestratorConfig,
    # KV Types
    KVScope,
    KVGetParams,
    KVPutParams,
    KVListParams,
    KVDeleteParams,
    # Task Types
    TaskAssignmentMode,
    CreateTaskParams,
    # Checkpoint Types
    CheckpointSaveParams,
    CheckpointLoadParams,
    # Message Types
    MessageType,
    SendMessageParams,
    # Message Shape Protocols
    IncomingMessageLike,
    ConfigLike,
    # Callback Protocols
    MessageCallback,
    ConfigCallback,
    SignalCallback,
    ErrorCallback,
    KVResponseCallback,
    TaskAssignmentCallback,
    CheckpointResponseCallback,
    ProgressCallback,
    ConnectCallback,
    DisconnectCallback,
    # Sync Callback Type Aliases
    SyncMessageCallback,
    SyncConfigCallback,
    SyncSignalCallback,
    SyncErrorCallback,
    SyncKVResponseCallback,
    SyncTaskAssignmentCallback,
    SyncCheckpointResponseCallback,
    SyncProgressCallback,
    SyncConnectCallback,
    SyncDisconnectCallback,
    # Async Callback Type Aliases
    AsyncMessageCallback,
    AsyncConfigCallback,
    AsyncSignalCallback,
    AsyncErrorCallback,
    AsyncKVResponseCallback,
    AsyncTaskAssignmentCallback,
    AsyncCheckpointResponseCallback,
    AsyncProgressCallback,
    AsyncConnectCallback,
    AsyncDisconnectCallback,
    # Union Type Aliases
    AnyMessageCallback,
    AnyConfigCallback,
    AnySignalCallback,
    AnyErrorCallback,
    AnyKVResponseCallback,
    AnyTaskAssignmentCallback,
    AnyCheckpointResponseCallback,
    AnyProgressCallback,
    AnyConnectCallback,
    AnyDisconnectCallback,
)

__all__ = (
    # Client classes (sync)
    'AgentClient',
    'TaskClient',
    'UserClient',
    'OrchestratorClient',
    'WorkflowEngineClient',
    'MetricsBridgeClient',
    'ServiceClient',

    # Client classes (async)
    'AsyncAgentClient',
    'AsyncTaskClient',
    'AsyncUserClient',
    'AsyncServiceClient',
    'AsyncOrchestratorClient',
    'AsyncWorkflowEngineClient',
    'AsyncMetricsBridgeClient',

    # Orchestrator classes
    'BaseOrchestrator',
    'LaunchedProcess',
    'MultiprocessOrchestrator',
    'SubprocessInfo',

    # Admin clients
    'AdminClient',
    'AsyncAdminClient',

    # Audit submission response
    'AuditSubmitResponse',

    # Message type constants
    'MESSAGE_TYPE_UNSPECIFIED',
    'OPAQUE',
    'CHAT',
    'CONTROL',
    'TOOL_CALL',
    'EVENT',
    'METRIC',

    # Task assignment mode constants
    'SELF_ASSIGN',
    'TARGETED',
    'POOL',

    # KV operation type constants
    'KV_GET',
    'KV_PUT',
    'KV_LIST',
    'KV_DELETE',

    # KV scope constants
    'KV_SCOPE_GLOBAL',
    'KV_SCOPE_WORKSPACE',
    'KV_SCOPE_USER',
    'KV_SCOPE_USER_WORKSPACE',
    'KV_SCOPE_GLOBAL_EXCLUSIVE',
    'KV_SCOPE_WORKSPACE_EXCLUSIVE',
    'KV_SCOPE_USER_SHARED',
    'KV_SCOPE_USER_WORKSPACE_SHARED',

    # Exceptions - Base
    'AetherError',

    # Exceptions - Connection
    'ConnectionError',
    'ConnectionClosedError',
    'ReconnectionError',

    # Exceptions - Auth
    'AuthenticationError',
    'PermissionDeniedError',

    # Exceptions - Identity
    'DuplicateIdentityError',

    # Exceptions - Timeout
    'TimeoutError',

    # Exceptions - Request/Response
    'InvalidArgumentError',
    'NotFoundError',
    'NotImplementedError',

    # Exceptions - Message/Protocol
    'MessageError',
    'KVOperationError',
    'CheckpointError',

    # Exception utility functions
    'from_grpc_error',
    'is_recoverable_error',

    # Types - Configuration TypedDicts
    'ConnectionConfig',
    'TLSConfig',
    'AgentConfig',
    'TaskConfig',
    'UserConfig',
    'OrchestratorConfig',

    # Types - KV
    'KVScope',
    'KVGetParams',
    'KVPutParams',
    'KVListParams',
    'KVDeleteParams',

    # Types - Task
    'TaskAssignmentMode',
    'CreateTaskParams',

    # Types - Checkpoint
    'CheckpointSaveParams',
    'CheckpointLoadParams',

    # Types - Message
    'MessageType',
    'SendMessageParams',

    # Types - Message Shape Protocols
    'IncomingMessageLike',
    'ConfigLike',

    # Types - Callback Protocols
    'MessageCallback',
    'ConfigCallback',
    'SignalCallback',
    'ErrorCallback',
    'KVResponseCallback',
    'TaskAssignmentCallback',
    'CheckpointResponseCallback',
    'ProgressCallback',
    'ConnectCallback',
    'DisconnectCallback',

    # Types - Sync Callback Type Aliases
    'SyncMessageCallback',
    'SyncConfigCallback',
    'SyncSignalCallback',
    'SyncErrorCallback',
    'SyncKVResponseCallback',
    'SyncTaskAssignmentCallback',
    'SyncCheckpointResponseCallback',
    'SyncProgressCallback',
    'SyncConnectCallback',
    'SyncDisconnectCallback',

    # Types - Async Callback Type Aliases
    'AsyncMessageCallback',
    'AsyncConfigCallback',
    'AsyncSignalCallback',
    'AsyncErrorCallback',
    'AsyncKVResponseCallback',
    'AsyncTaskAssignmentCallback',
    'AsyncCheckpointResponseCallback',
    'AsyncProgressCallback',
    'AsyncConnectCallback',
    'AsyncDisconnectCallback',

    # Types - Union Type Aliases
    'AnyMessageCallback',
    'AnyConfigCallback',
    'AnySignalCallback',
    'AnyErrorCallback',
    'AnyKVResponseCallback',
    'AnyTaskAssignmentCallback',
    'AnyCheckpointResponseCallback',
    'AnyProgressCallback',
    'AnyConnectCallback',
    'AnyDisconnectCallback',

    # Types - Other
    'Credentials',

    # Metric builder
    'MetricBuilder',
    'new_metric',
)
