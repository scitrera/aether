"""
Type definitions for the Aether client.

This module provides comprehensive type definitions including:
- TypedDict classes for configuration and response structures
- Protocol classes for callbacks (supporting both sync and async)
- Type aliases for common patterns

These types are designed for use with static type checkers like mypy
and provide better IDE support and documentation.
"""
from typing import (
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
    TypedDict,
)

# Import proto types for callback signatures
# These are imported at type-checking time only to avoid circular imports
from .proto import aether_pb2


# =============================================================================
# Configuration TypedDicts
# =============================================================================

class ConnectionConfig(TypedDict, total=False):
    """
    Configuration options for client connections.

    All fields are optional and have sensible defaults.

    Attributes:
        max_retries: Maximum number of connection attempts (0 = infinite for reconnect).
        initial_backoff: Initial backoff delay in seconds.
        max_backoff: Maximum backoff delay in seconds.
        backoff_multiplier: Multiplier for exponential backoff.
        auto_reconnect: Whether to automatically reconnect on connection loss.
    """
    max_retries: int
    initial_backoff: float
    max_backoff: float
    backoff_multiplier: float
    auto_reconnect: bool


class TLSConfig(TypedDict, total=False):
    """
    TLS/mTLS configuration for secure connections.

    Attributes:
        root_certificates: PEM-encoded root CA certificates.
        private_key: PEM-encoded client private key (for mTLS).
        certificate_chain: PEM-encoded client certificate chain (for mTLS).
        server_hostname_override: Override server hostname for certificate validation.
    """
    root_certificates: bytes
    private_key: bytes
    certificate_chain: bytes
    server_hostname_override: str


class AgentConfig(TypedDict):
    """
    Configuration for an Agent client.

    Attributes:
        workspace: The workspace to connect to.
        implementation: Agent implementation type.
        specifier: Unique specifier for this agent instance.
    """
    workspace: str
    implementation: str
    specifier: str


class TaskConfig(TypedDict):
    """
    Configuration for a Task client.

    Attributes:
        workspace: The workspace to connect to.
        implementation: Task implementation type.
        unique_specifier: Optional unique specifier (empty for non-unique tasks).
    """
    workspace: str
    implementation: str
    unique_specifier: str


class UserConfig(TypedDict):
    """
    Configuration for a User client.

    Attributes:
        user_id: The user's unique identifier.
        window_id: The window/session identifier.
    """
    user_id: str
    window_id: str


class OrchestratorConfig(TypedDict, total=False):
    """
    Configuration for an Orchestrator client.

    Attributes:
        implementation: Orchestrator implementation type.
        supported_profiles: List of profiles this orchestrator can handle.
        specifier: Optional unique specifier (generated if not provided).
    """
    implementation: str
    supported_profiles: List[str]
    specifier: str


# =============================================================================
# KV Operation Types
# =============================================================================

KVScope = Literal[
    "global", "workspace", "user", "user-workspace",
    "global-exclusive", "workspace-exclusive", "user-shared", "user-workspace-shared",
]


class KVGetParams(TypedDict, total=False):
    """
    Parameters for KV GET operation.

    Attributes:
        key: The key to retrieve.
        scope: One of "global", "workspace", "user", "user-workspace".
        user_id: Required for "user" and "user-workspace" scopes.
        workspace: Required for "workspace" and "user-workspace" scopes.
        timeout: Timeout in seconds for synchronous get.
    """
    key: str
    scope: KVScope
    user_id: str
    workspace: str
    timeout: float


class KVPutParams(TypedDict, total=False):
    """
    Parameters for KV PUT operation.

    Attributes:
        key: The key to store.
        value: The value to store (bytes).
        scope: One of "global", "workspace", "user", "user-workspace".
        user_id: Required for "user" and "user-workspace" scopes.
        workspace: Required for "workspace" and "user-workspace" scopes.
        ttl: Time-to-live in seconds (0 = no expiration).
        timeout: Timeout in seconds for synchronous put.
    """
    key: str
    value: bytes
    scope: KVScope
    user_id: str
    workspace: str
    ttl: int
    timeout: float


class KVListParams(TypedDict, total=False):
    """
    Parameters for KV LIST operation.

    Attributes:
        key_prefix: Prefix to filter keys (empty for all).
        scope: One of "global", "workspace", "user", "user-workspace".
        user_id: Required for "user" and "user-workspace" scopes.
        workspace: Required for "workspace" and "user-workspace" scopes.
        timeout: Timeout in seconds for synchronous list.
    """
    key_prefix: str
    scope: KVScope
    user_id: str
    workspace: str
    timeout: float


class KVDeleteParams(TypedDict, total=False):
    """
    Parameters for KV DELETE operation.

    Attributes:
        key: The key to delete.
        scope: One of "global", "workspace", "user", "user-workspace".
        user_id: Required for "user" and "user-workspace" scopes.
        workspace: Required for "workspace" and "user-workspace" scopes.
        timeout: Timeout in seconds for synchronous delete.
    """
    key: str
    scope: KVScope
    user_id: str
    workspace: str
    timeout: float


# =============================================================================
# Task Creation Types
# =============================================================================

TaskAssignmentMode = Literal["SELF_ASSIGN", "TARGETED", "POOL"]


class CreateTaskParams(TypedDict, total=False):
    """
    Parameters for task creation.

    Attributes:
        task_type: The type of task to create.
        workspace: The workspace for the task.
        target_agent_id: For TARGETED mode, the agent to assign to.
        launch_param_overrides: Optional parameter overrides for orchestration.
        metadata: Optional task metadata.
        assignment_mode: SELF_ASSIGN (default), TARGETED, or POOL.
    """
    task_type: str
    workspace: str
    target_agent_id: str
    launch_param_overrides: Dict[str, str]
    metadata: Dict[str, str]
    assignment_mode: TaskAssignmentMode


# =============================================================================
# Checkpoint Types
# =============================================================================

class CheckpointSaveParams(TypedDict, total=False):
    """
    Parameters for checkpoint SAVE operation.

    Attributes:
        data: The checkpoint data to save (bytes).
        key: Optional checkpoint key (default: "default").
        ttl: Time-to-live in seconds (-1 = server default, 0 = no expiration).
        timeout: Timeout in seconds for synchronous save.
    """
    data: bytes
    key: str
    ttl: int
    timeout: float


class CheckpointLoadParams(TypedDict, total=False):
    """
    Parameters for checkpoint LOAD operation.

    Attributes:
        key: Optional checkpoint key (default: "default").
        timeout: Timeout in seconds for synchronous load.
    """
    key: str
    timeout: float


# =============================================================================
# Message Types
# =============================================================================

MessageType = Literal["MESSAGE_TYPE_UNSPECIFIED", "CHAT", "CONTROL", "TOOL_CALL", "EVENT", "METRIC"]


class SendMessageParams(TypedDict, total=False):
    """
    Parameters for sending a message.

    Attributes:
        target_topic: The destination topic.
        payload: Message payload (bytes).
        message_type: One of CHAT, CONTROL, TOOL_CALL, EVENT, METRIC.
    """
    target_topic: str
    payload: bytes
    message_type: MessageType


# =============================================================================
# Callback Protocols
# =============================================================================

# Type variable for callback return types
T = TypeVar('T')


@runtime_checkable
class MessageCallback(Protocol):
    """
    Protocol for message callbacks.

    Can be either synchronous or asynchronous.

    Example:
        def on_message(msg: IncomingMessage) -> None:
            print(f"Received: {msg.payload}")

        async def on_message_async(msg: IncomingMessage) -> None:
            await process_message(msg)
    """
    def __call__(self, msg: aether_pb2.IncomingMessage) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class ConfigCallback(Protocol):
    """
    Protocol for configuration snapshot callbacks.

    Called when the client receives a ConfigSnapshot from the server.
    """
    def __call__(self, config: aether_pb2.ConfigSnapshot) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class SignalCallback(Protocol):
    """
    Protocol for signal callbacks.

    Called when the client receives a Signal from the server.
    """
    def __call__(self, signal: aether_pb2.Signal) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class ErrorCallback(Protocol):
    """
    Protocol for error callbacks.

    Called when the client receives an ErrorResponse from the server.
    """
    def __call__(self, error: aether_pb2.ErrorResponse) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class KVResponseCallback(Protocol):
    """
    Protocol for KV response callbacks.

    Called when the client receives a KVResponse from the server.
    """
    def __call__(self, response: aether_pb2.KVResponse) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class TaskAssignmentCallback(Protocol):
    """
    Protocol for task assignment callbacks.

    Called when the client receives a TaskAssignment from the server.
    """
    def __call__(self, assignment: aether_pb2.TaskAssignment) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class CheckpointResponseCallback(Protocol):
    """
    Protocol for checkpoint response callbacks.

    Called when the client receives a CheckpointResponse from the server.
    """
    def __call__(self, response: aether_pb2.CheckpointResponse) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class ProgressCallback(Protocol):
    """
    Protocol for progress update callbacks.

    Called when the client receives a ProgressUpdate from the server.
    Progress updates are sent by agents/tasks to report work status.
    """
    def __call__(self, update: aether_pb2.ProgressUpdate) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class ConnectCallback(Protocol):
    """
    Protocol for connection established callbacks.

    Called when the client successfully connects (or reconnects) to the server.
    """
    def __call__(self) -> Union[None, Awaitable[None]]:
        ...


@runtime_checkable
class DisconnectCallback(Protocol):
    """
    Protocol for disconnection callbacks.

    Called when the client disconnects from the server.
    The reason parameter describes why the disconnection occurred.
    """
    def __call__(self, reason: str) -> Union[None, Awaitable[None]]:
        ...


# =============================================================================
# Message Shape Protocols
# =============================================================================

@runtime_checkable
class IncomingMessageLike(Protocol):
    """Shape of the object delivered to message callbacks.
    Consumers see .source_topic and .payload without knowing it's protobuf."""
    source_topic: str
    payload: bytes


@runtime_checkable
class ConfigLike(Protocol):
    """Shape of the object delivered to on_config callback."""
    kv: Mapping[str, str]
    global_kv: Mapping[str, str]
    task_context: Mapping[str, str]


# =============================================================================
# Type Aliases (for backward compatibility and convenience)
# =============================================================================

# Sync callback type aliases
SyncMessageCallback = Callable[[aether_pb2.IncomingMessage], None]
SyncConfigCallback = Callable[[aether_pb2.ConfigSnapshot], None]
SyncSignalCallback = Callable[[aether_pb2.Signal], None]
SyncErrorCallback = Callable[[aether_pb2.ErrorResponse], None]
SyncKVResponseCallback = Callable[[aether_pb2.KVResponse], None]
SyncTaskAssignmentCallback = Callable[[aether_pb2.TaskAssignment], None]
SyncCheckpointResponseCallback = Callable[[aether_pb2.CheckpointResponse], None]
SyncProgressCallback = Callable[[aether_pb2.ProgressUpdate], None]
SyncConnectCallback = Callable[[], None]
SyncDisconnectCallback = Callable[[str], None]

# Async callback type aliases
AsyncMessageCallback = Callable[[aether_pb2.IncomingMessage], Awaitable[None]]
AsyncConfigCallback = Callable[[aether_pb2.ConfigSnapshot], Awaitable[None]]
AsyncSignalCallback = Callable[[aether_pb2.Signal], Awaitable[None]]
AsyncErrorCallback = Callable[[aether_pb2.ErrorResponse], Awaitable[None]]
AsyncKVResponseCallback = Callable[[aether_pb2.KVResponse], Awaitable[None]]
AsyncTaskAssignmentCallback = Callable[[aether_pb2.TaskAssignment], Awaitable[None]]
AsyncCheckpointResponseCallback = Callable[[aether_pb2.CheckpointResponse], Awaitable[None]]
AsyncProgressCallback = Callable[[aether_pb2.ProgressUpdate], Awaitable[None]]
AsyncConnectCallback = Callable[[], Awaitable[None]]
AsyncDisconnectCallback = Callable[[str], Awaitable[None]]

# Union type aliases (accept both sync and async)
AnyMessageCallback = Union[SyncMessageCallback, AsyncMessageCallback]
AnyConfigCallback = Union[SyncConfigCallback, AsyncConfigCallback]
AnySignalCallback = Union[SyncSignalCallback, AsyncSignalCallback]
AnyErrorCallback = Union[SyncErrorCallback, AsyncErrorCallback]
AnyKVResponseCallback = Union[SyncKVResponseCallback, AsyncKVResponseCallback]
AnyTaskAssignmentCallback = Union[SyncTaskAssignmentCallback, AsyncTaskAssignmentCallback]
AnyCheckpointResponseCallback = Union[SyncCheckpointResponseCallback, AsyncCheckpointResponseCallback]
AnyProgressCallback = Union[SyncProgressCallback, AsyncProgressCallback]
AnyConnectCallback = Union[SyncConnectCallback, AsyncConnectCallback]
AnyDisconnectCallback = Union[SyncDisconnectCallback, AsyncDisconnectCallback]


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Configuration TypedDicts
    "ConnectionConfig",
    "TLSConfig",
    "AgentConfig",
    "TaskConfig",
    "UserConfig",
    "OrchestratorConfig",

    # KV Types
    "KVScope",
    "KVGetParams",
    "KVPutParams",
    "KVListParams",
    "KVDeleteParams",

    # Task Types
    "TaskAssignmentMode",
    "CreateTaskParams",

    # Checkpoint Types
    "CheckpointSaveParams",
    "CheckpointLoadParams",

    # Message Types
    "MessageType",
    "SendMessageParams",

    # Message Shape Protocols
    "IncomingMessageLike",
    "ConfigLike",

    # Callback Protocols
    "MessageCallback",
    "ConfigCallback",
    "SignalCallback",
    "ErrorCallback",
    "KVResponseCallback",
    "TaskAssignmentCallback",
    "CheckpointResponseCallback",
    "ProgressCallback",
    "ConnectCallback",
    "DisconnectCallback",

    # Sync Callback Type Aliases
    "SyncMessageCallback",
    "SyncConfigCallback",
    "SyncSignalCallback",
    "SyncErrorCallback",
    "SyncKVResponseCallback",
    "SyncTaskAssignmentCallback",
    "SyncCheckpointResponseCallback",
    "SyncProgressCallback",
    "SyncConnectCallback",
    "SyncDisconnectCallback",

    # Async Callback Type Aliases
    "AsyncMessageCallback",
    "AsyncConfigCallback",
    "AsyncSignalCallback",
    "AsyncErrorCallback",
    "AsyncKVResponseCallback",
    "AsyncTaskAssignmentCallback",
    "AsyncCheckpointResponseCallback",
    "AsyncProgressCallback",
    "AsyncConnectCallback",
    "AsyncDisconnectCallback",

    # Union Type Aliases
    "AnyMessageCallback",
    "AnyConfigCallback",
    "AnySignalCallback",
    "AnyErrorCallback",
    "AnyKVResponseCallback",
    "AnyTaskAssignmentCallback",
    "AnyCheckpointResponseCallback",
    "AnyProgressCallback",
    "AnyConnectCallback",
    "AnyDisconnectCallback",

]
