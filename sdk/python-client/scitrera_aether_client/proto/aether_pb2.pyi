from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MessageType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MESSAGE_TYPE_UNSPECIFIED: _ClassVar[MessageType]
    CHAT: _ClassVar[MessageType]
    CONTROL: _ClassVar[MessageType]
    TOOL_CALL: _ClassVar[MessageType]
    EVENT: _ClassVar[MessageType]
    METRIC: _ClassVar[MessageType]
    OPAQUE: _ClassVar[MessageType]

class PrincipalType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRINCIPAL_TYPE_UNSPECIFIED: _ClassVar[PrincipalType]
    PRINCIPAL_AGENT: _ClassVar[PrincipalType]
    PRINCIPAL_TASK: _ClassVar[PrincipalType]
    PRINCIPAL_USER: _ClassVar[PrincipalType]
    PRINCIPAL_ORCHESTRATOR: _ClassVar[PrincipalType]
    PRINCIPAL_WORKFLOW_ENGINE: _ClassVar[PrincipalType]
    PRINCIPAL_METRICS_BRIDGE: _ClassVar[PrincipalType]
    PRINCIPAL_BRIDGE: _ClassVar[PrincipalType]
    PRINCIPAL_SERVICE: _ClassVar[PrincipalType]

class TaskStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_STATUS_UNSPECIFIED: _ClassVar[TaskStatus]
    TASK_STATUS_QUEUED: _ClassVar[TaskStatus]
    TASK_STATUS_RUNNING: _ClassVar[TaskStatus]
    TASK_STATUS_COMPLETED: _ClassVar[TaskStatus]
    TASK_STATUS_FAILED: _ClassVar[TaskStatus]
    TASK_STATUS_CANCELLED: _ClassVar[TaskStatus]
    TASK_STATUS_WAITING_INPUT: _ClassVar[TaskStatus]
    TASK_STATUS_WAITING_AUTHORITY: _ClassVar[TaskStatus]
    TASK_STATUS_WAITING_DEPENDENCY: _ClassVar[TaskStatus]
    TASK_STATUS_HIBERNATED: _ClassVar[TaskStatus]
    TASK_STATUS_REJECTED: _ClassVar[TaskStatus]

class HealthStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HEALTH_STATUS_UNSPECIFIED: _ClassVar[HealthStatus]
    HEALTH_STATUS_HEALTHY: _ClassVar[HealthStatus]
    HEALTH_STATUS_DEGRADED: _ClassVar[HealthStatus]
    HEALTH_STATUS_UNHEALTHY: _ClassVar[HealthStatus]

class HealthCheckStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HEALTH_CHECK_STATUS_UNSPECIFIED: _ClassVar[HealthCheckStatus]
    HEALTH_CHECK_STATUS_OK: _ClassVar[HealthCheckStatus]
    HEALTH_CHECK_STATUS_ERROR: _ClassVar[HealthCheckStatus]

class AccessLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACCESS_LEVEL_UNSPECIFIED: _ClassVar[AccessLevel]
    ACCESS_LEVEL_NONE: _ClassVar[AccessLevel]
    ACCESS_LEVEL_READ: _ClassVar[AccessLevel]
    ACCESS_LEVEL_READWRITE: _ClassVar[AccessLevel]
    ACCESS_LEVEL_MANAGE: _ClassVar[AccessLevel]
    ACCESS_LEVEL_ADMIN: _ClassVar[AccessLevel]
    ACCESS_LEVEL_SUPERADMIN: _ClassVar[AccessLevel]

class TaskAssignmentMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SELF_ASSIGN: _ClassVar[TaskAssignmentMode]
    TARGETED: _ClassVar[TaskAssignmentMode]
    POOL: _ClassVar[TaskAssignmentMode]

class TaskClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_CLASS_UNSPECIFIED: _ClassVar[TaskClass]
    TASK_CLASS_INTERACTIVE: _ClassVar[TaskClass]
    TASK_CLASS_BACKGROUND: _ClassVar[TaskClass]
    TASK_CLASS_BATCH: _ClassVar[TaskClass]

class WaitReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WAIT_REASON_UNSPECIFIED: _ClassVar[WaitReason]
    WAIT_REASON_INPUT: _ClassVar[WaitReason]
    WAIT_REASON_AUTHORITY: _ClassVar[WaitReason]
    WAIT_REASON_DEPENDENCY: _ClassVar[WaitReason]
    WAIT_REASON_HIBERNATION: _ClassVar[WaitReason]

class AuthorityRequestStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUTHORITY_REQUEST_STATUS_UNSPECIFIED: _ClassVar[AuthorityRequestStatus]
    AUTHORITY_REQUEST_STATUS_PENDING: _ClassVar[AuthorityRequestStatus]
    AUTHORITY_REQUEST_STATUS_APPROVED: _ClassVar[AuthorityRequestStatus]
    AUTHORITY_REQUEST_STATUS_DENIED: _ClassVar[AuthorityRequestStatus]
    AUTHORITY_REQUEST_STATUS_EXPIRED: _ClassVar[AuthorityRequestStatus]
    AUTHORITY_REQUEST_STATUS_CANCELLED: _ClassVar[AuthorityRequestStatus]

class ProgressKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROGRESS_KIND_UNSPECIFIED: _ClassVar[ProgressKind]
    PROGRESS_KIND_CHAT: _ClassVar[ProgressKind]
    PROGRESS_KIND_APP: _ClassVar[ProgressKind]
    PROGRESS_KIND_TASK: _ClassVar[ProgressKind]
MESSAGE_TYPE_UNSPECIFIED: MessageType
CHAT: MessageType
CONTROL: MessageType
TOOL_CALL: MessageType
EVENT: MessageType
METRIC: MessageType
OPAQUE: MessageType
PRINCIPAL_TYPE_UNSPECIFIED: PrincipalType
PRINCIPAL_AGENT: PrincipalType
PRINCIPAL_TASK: PrincipalType
PRINCIPAL_USER: PrincipalType
PRINCIPAL_ORCHESTRATOR: PrincipalType
PRINCIPAL_WORKFLOW_ENGINE: PrincipalType
PRINCIPAL_METRICS_BRIDGE: PrincipalType
PRINCIPAL_BRIDGE: PrincipalType
PRINCIPAL_SERVICE: PrincipalType
TASK_STATUS_UNSPECIFIED: TaskStatus
TASK_STATUS_QUEUED: TaskStatus
TASK_STATUS_RUNNING: TaskStatus
TASK_STATUS_COMPLETED: TaskStatus
TASK_STATUS_FAILED: TaskStatus
TASK_STATUS_CANCELLED: TaskStatus
TASK_STATUS_WAITING_INPUT: TaskStatus
TASK_STATUS_WAITING_AUTHORITY: TaskStatus
TASK_STATUS_WAITING_DEPENDENCY: TaskStatus
TASK_STATUS_HIBERNATED: TaskStatus
TASK_STATUS_REJECTED: TaskStatus
HEALTH_STATUS_UNSPECIFIED: HealthStatus
HEALTH_STATUS_HEALTHY: HealthStatus
HEALTH_STATUS_DEGRADED: HealthStatus
HEALTH_STATUS_UNHEALTHY: HealthStatus
HEALTH_CHECK_STATUS_UNSPECIFIED: HealthCheckStatus
HEALTH_CHECK_STATUS_OK: HealthCheckStatus
HEALTH_CHECK_STATUS_ERROR: HealthCheckStatus
ACCESS_LEVEL_UNSPECIFIED: AccessLevel
ACCESS_LEVEL_NONE: AccessLevel
ACCESS_LEVEL_READ: AccessLevel
ACCESS_LEVEL_READWRITE: AccessLevel
ACCESS_LEVEL_MANAGE: AccessLevel
ACCESS_LEVEL_ADMIN: AccessLevel
ACCESS_LEVEL_SUPERADMIN: AccessLevel
SELF_ASSIGN: TaskAssignmentMode
TARGETED: TaskAssignmentMode
POOL: TaskAssignmentMode
TASK_CLASS_UNSPECIFIED: TaskClass
TASK_CLASS_INTERACTIVE: TaskClass
TASK_CLASS_BACKGROUND: TaskClass
TASK_CLASS_BATCH: TaskClass
WAIT_REASON_UNSPECIFIED: WaitReason
WAIT_REASON_INPUT: WaitReason
WAIT_REASON_AUTHORITY: WaitReason
WAIT_REASON_DEPENDENCY: WaitReason
WAIT_REASON_HIBERNATION: WaitReason
AUTHORITY_REQUEST_STATUS_UNSPECIFIED: AuthorityRequestStatus
AUTHORITY_REQUEST_STATUS_PENDING: AuthorityRequestStatus
AUTHORITY_REQUEST_STATUS_APPROVED: AuthorityRequestStatus
AUTHORITY_REQUEST_STATUS_DENIED: AuthorityRequestStatus
AUTHORITY_REQUEST_STATUS_EXPIRED: AuthorityRequestStatus
AUTHORITY_REQUEST_STATUS_CANCELLED: AuthorityRequestStatus
PROGRESS_KIND_UNSPECIFIED: ProgressKind
PROGRESS_KIND_CHAT: ProgressKind
PROGRESS_KIND_APP: ProgressKind
PROGRESS_KIND_TASK: ProgressKind

class UpstreamMessage(_message.Message):
    __slots__ = ("init", "send", "switch_workspace", "kv_op", "create_task", "checkpoint_op", "admin_query", "session_op", "task_query", "task_op", "workspace_op", "agent_op", "acl_op", "progress", "workflow_op", "workflow_response", "token_op", "audit_query", "authority_grant_op", "proxy_http_request", "proxy_http_body_chunk", "tunnel_open", "tunnel_data", "tunnel_close", "proxy_http_response", "tunnel_ack", "resolve_authority_request", "connection_status_request", "submit_audit_event", "authority_request_op")
    INIT_FIELD_NUMBER: _ClassVar[int]
    SEND_FIELD_NUMBER: _ClassVar[int]
    SWITCH_WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    KV_OP_FIELD_NUMBER: _ClassVar[int]
    CREATE_TASK_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_OP_FIELD_NUMBER: _ClassVar[int]
    ADMIN_QUERY_FIELD_NUMBER: _ClassVar[int]
    SESSION_OP_FIELD_NUMBER: _ClassVar[int]
    TASK_QUERY_FIELD_NUMBER: _ClassVar[int]
    TASK_OP_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_OP_FIELD_NUMBER: _ClassVar[int]
    AGENT_OP_FIELD_NUMBER: _ClassVar[int]
    ACL_OP_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_OP_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    TOKEN_OP_FIELD_NUMBER: _ClassVar[int]
    AUDIT_QUERY_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_OP_FIELD_NUMBER: _ClassVar[int]
    PROXY_HTTP_REQUEST_FIELD_NUMBER: _ClassVar[int]
    PROXY_HTTP_BODY_CHUNK_FIELD_NUMBER: _ClassVar[int]
    TUNNEL_OPEN_FIELD_NUMBER: _ClassVar[int]
    TUNNEL_DATA_FIELD_NUMBER: _ClassVar[int]
    TUNNEL_CLOSE_FIELD_NUMBER: _ClassVar[int]
    PROXY_HTTP_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    TUNNEL_ACK_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_AUTHORITY_REQUEST_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_STATUS_REQUEST_FIELD_NUMBER: _ClassVar[int]
    SUBMIT_AUDIT_EVENT_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_REQUEST_OP_FIELD_NUMBER: _ClassVar[int]
    init: InitConnection
    send: SendMessage
    switch_workspace: SwitchWorkspace
    kv_op: KVOperation
    create_task: CreateTaskRequest
    checkpoint_op: CheckpointOperation
    admin_query: AdminQuery
    session_op: SessionOperation
    task_query: TaskQuery
    task_op: TaskOperation
    workspace_op: WorkspaceOperation
    agent_op: AgentOperation
    acl_op: ACLOperation
    progress: ProgressReport
    workflow_op: WorkflowOperation
    workflow_response: WorkflowResponse
    token_op: TokenOperation
    audit_query: AuditQuery
    authority_grant_op: AuthorityGrantOperation
    proxy_http_request: ProxyHttpRequest
    proxy_http_body_chunk: ProxyHttpBodyChunk
    tunnel_open: TunnelOpen
    tunnel_data: TunnelData
    tunnel_close: TunnelClose
    proxy_http_response: ProxyHttpResponse
    tunnel_ack: TunnelAck
    resolve_authority_request: ResolveAuthorityRequest
    connection_status_request: ConnectionStatusRequest
    submit_audit_event: SubmitAuditEventRequest
    authority_request_op: AuthorityRequestOperation
    def __init__(self, init: _Optional[_Union[InitConnection, _Mapping]] = ..., send: _Optional[_Union[SendMessage, _Mapping]] = ..., switch_workspace: _Optional[_Union[SwitchWorkspace, _Mapping]] = ..., kv_op: _Optional[_Union[KVOperation, _Mapping]] = ..., create_task: _Optional[_Union[CreateTaskRequest, _Mapping]] = ..., checkpoint_op: _Optional[_Union[CheckpointOperation, _Mapping]] = ..., admin_query: _Optional[_Union[AdminQuery, _Mapping]] = ..., session_op: _Optional[_Union[SessionOperation, _Mapping]] = ..., task_query: _Optional[_Union[TaskQuery, _Mapping]] = ..., task_op: _Optional[_Union[TaskOperation, _Mapping]] = ..., workspace_op: _Optional[_Union[WorkspaceOperation, _Mapping]] = ..., agent_op: _Optional[_Union[AgentOperation, _Mapping]] = ..., acl_op: _Optional[_Union[ACLOperation, _Mapping]] = ..., progress: _Optional[_Union[ProgressReport, _Mapping]] = ..., workflow_op: _Optional[_Union[WorkflowOperation, _Mapping]] = ..., workflow_response: _Optional[_Union[WorkflowResponse, _Mapping]] = ..., token_op: _Optional[_Union[TokenOperation, _Mapping]] = ..., audit_query: _Optional[_Union[AuditQuery, _Mapping]] = ..., authority_grant_op: _Optional[_Union[AuthorityGrantOperation, _Mapping]] = ..., proxy_http_request: _Optional[_Union[ProxyHttpRequest, _Mapping]] = ..., proxy_http_body_chunk: _Optional[_Union[ProxyHttpBodyChunk, _Mapping]] = ..., tunnel_open: _Optional[_Union[TunnelOpen, _Mapping]] = ..., tunnel_data: _Optional[_Union[TunnelData, _Mapping]] = ..., tunnel_close: _Optional[_Union[TunnelClose, _Mapping]] = ..., proxy_http_response: _Optional[_Union[ProxyHttpResponse, _Mapping]] = ..., tunnel_ack: _Optional[_Union[TunnelAck, _Mapping]] = ..., resolve_authority_request: _Optional[_Union[ResolveAuthorityRequest, _Mapping]] = ..., connection_status_request: _Optional[_Union[ConnectionStatusRequest, _Mapping]] = ..., submit_audit_event: _Optional[_Union[SubmitAuditEventRequest, _Mapping]] = ..., authority_request_op: _Optional[_Union[AuthorityRequestOperation, _Mapping]] = ...) -> None: ...

class DownstreamMessage(_message.Message):
    __slots__ = ("msg", "config", "signal", "error", "kv", "task_assignment", "connection_ack", "checkpoint", "admin", "session_response", "task_query", "task_op", "workspace", "agent", "acl", "progress_update", "workflow_response", "workflow_op", "token", "audit_response", "authority_grant", "create_task", "proxy_http_response", "proxy_http_body_chunk", "tunnel_ack", "tunnel_close", "tunnel_data", "proxy_http_request", "resolve_authority_response", "connection_status_response", "authority_grant_revocation", "submit_audit_event_response", "authority_request_response", "authority_request_event", "task_hibernated")
    MSG_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    KV_FIELD_NUMBER: _ClassVar[int]
    TASK_ASSIGNMENT_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_ACK_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    ADMIN_FIELD_NUMBER: _ClassVar[int]
    SESSION_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    TASK_QUERY_FIELD_NUMBER: _ClassVar[int]
    TASK_OP_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    ACL_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_UPDATE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_OP_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    AUDIT_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_FIELD_NUMBER: _ClassVar[int]
    CREATE_TASK_FIELD_NUMBER: _ClassVar[int]
    PROXY_HTTP_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    PROXY_HTTP_BODY_CHUNK_FIELD_NUMBER: _ClassVar[int]
    TUNNEL_ACK_FIELD_NUMBER: _ClassVar[int]
    TUNNEL_CLOSE_FIELD_NUMBER: _ClassVar[int]
    TUNNEL_DATA_FIELD_NUMBER: _ClassVar[int]
    PROXY_HTTP_REQUEST_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_AUTHORITY_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_STATUS_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_REVOCATION_FIELD_NUMBER: _ClassVar[int]
    SUBMIT_AUDIT_EVENT_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_REQUEST_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_REQUEST_EVENT_FIELD_NUMBER: _ClassVar[int]
    TASK_HIBERNATED_FIELD_NUMBER: _ClassVar[int]
    msg: IncomingMessage
    config: ConfigSnapshot
    signal: Signal
    error: ErrorResponse
    kv: KVResponse
    task_assignment: TaskAssignment
    connection_ack: ConnectionAck
    checkpoint: CheckpointResponse
    admin: AdminResponse
    session_response: SessionOperationResponse
    task_query: TaskQueryResponse
    task_op: TaskOperationResponse
    workspace: WorkspaceResponse
    agent: AgentResponse
    acl: ACLResponse
    progress_update: ProgressUpdate
    workflow_response: WorkflowResponse
    workflow_op: WorkflowOperation
    token: TokenResponse
    audit_response: AuditQueryResponse
    authority_grant: AuthorityGrantResponse
    create_task: CreateTaskResponse
    proxy_http_response: ProxyHttpResponse
    proxy_http_body_chunk: ProxyHttpBodyChunk
    tunnel_ack: TunnelAck
    tunnel_close: TunnelClose
    tunnel_data: TunnelData
    proxy_http_request: ProxyHttpRequest
    resolve_authority_response: ResolveAuthorityResponse
    connection_status_response: ConnectionStatusResponse
    authority_grant_revocation: AuthorityGrantRevocation
    submit_audit_event_response: SubmitAuditEventResponse
    authority_request_response: AuthorityRequestOperationResponse
    authority_request_event: AuthorityRequestEvent
    task_hibernated: TaskHibernated
    def __init__(self, msg: _Optional[_Union[IncomingMessage, _Mapping]] = ..., config: _Optional[_Union[ConfigSnapshot, _Mapping]] = ..., signal: _Optional[_Union[Signal, _Mapping]] = ..., error: _Optional[_Union[ErrorResponse, _Mapping]] = ..., kv: _Optional[_Union[KVResponse, _Mapping]] = ..., task_assignment: _Optional[_Union[TaskAssignment, _Mapping]] = ..., connection_ack: _Optional[_Union[ConnectionAck, _Mapping]] = ..., checkpoint: _Optional[_Union[CheckpointResponse, _Mapping]] = ..., admin: _Optional[_Union[AdminResponse, _Mapping]] = ..., session_response: _Optional[_Union[SessionOperationResponse, _Mapping]] = ..., task_query: _Optional[_Union[TaskQueryResponse, _Mapping]] = ..., task_op: _Optional[_Union[TaskOperationResponse, _Mapping]] = ..., workspace: _Optional[_Union[WorkspaceResponse, _Mapping]] = ..., agent: _Optional[_Union[AgentResponse, _Mapping]] = ..., acl: _Optional[_Union[ACLResponse, _Mapping]] = ..., progress_update: _Optional[_Union[ProgressUpdate, _Mapping]] = ..., workflow_response: _Optional[_Union[WorkflowResponse, _Mapping]] = ..., workflow_op: _Optional[_Union[WorkflowOperation, _Mapping]] = ..., token: _Optional[_Union[TokenResponse, _Mapping]] = ..., audit_response: _Optional[_Union[AuditQueryResponse, _Mapping]] = ..., authority_grant: _Optional[_Union[AuthorityGrantResponse, _Mapping]] = ..., create_task: _Optional[_Union[CreateTaskResponse, _Mapping]] = ..., proxy_http_response: _Optional[_Union[ProxyHttpResponse, _Mapping]] = ..., proxy_http_body_chunk: _Optional[_Union[ProxyHttpBodyChunk, _Mapping]] = ..., tunnel_ack: _Optional[_Union[TunnelAck, _Mapping]] = ..., tunnel_close: _Optional[_Union[TunnelClose, _Mapping]] = ..., tunnel_data: _Optional[_Union[TunnelData, _Mapping]] = ..., proxy_http_request: _Optional[_Union[ProxyHttpRequest, _Mapping]] = ..., resolve_authority_response: _Optional[_Union[ResolveAuthorityResponse, _Mapping]] = ..., connection_status_response: _Optional[_Union[ConnectionStatusResponse, _Mapping]] = ..., authority_grant_revocation: _Optional[_Union[AuthorityGrantRevocation, _Mapping]] = ..., submit_audit_event_response: _Optional[_Union[SubmitAuditEventResponse, _Mapping]] = ..., authority_request_response: _Optional[_Union[AuthorityRequestOperationResponse, _Mapping]] = ..., authority_request_event: _Optional[_Union[AuthorityRequestEvent, _Mapping]] = ..., task_hibernated: _Optional[_Union[TaskHibernated, _Mapping]] = ...) -> None: ...

class TaskHibernated(_message.Message):
    __slots__ = ("task_id", "descriptor")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    descriptor: HibernationDescriptor
    def __init__(self, task_id: _Optional[str] = ..., descriptor: _Optional[_Union[HibernationDescriptor, _Mapping]] = ...) -> None: ...

class ConnectionAck(_message.Message):
    __slots__ = ("session_id", "resumed", "assigned_id")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RESUMED_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    resumed: bool
    assigned_id: str
    def __init__(self, session_id: _Optional[str] = ..., resumed: bool = ..., assigned_id: _Optional[str] = ...) -> None: ...

class InitConnection(_message.Message):
    __slots__ = ("agent", "task", "user", "orchestrator", "workflow_engine", "metrics_bridge", "bridge", "service", "credentials", "resume_session_id")
    class CredentialsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    AGENT_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATOR_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ENGINE_FIELD_NUMBER: _ClassVar[int]
    METRICS_BRIDGE_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    RESUME_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    agent: AgentIdentity
    task: TaskIdentity
    user: UserIdentity
    orchestrator: OrchestratorIdentity
    workflow_engine: WorkflowEngineIdentity
    metrics_bridge: MetricsBridgeIdentity
    bridge: BridgeIdentity
    service: ServiceIdentity
    credentials: _containers.ScalarMap[str, str]
    resume_session_id: str
    def __init__(self, agent: _Optional[_Union[AgentIdentity, _Mapping]] = ..., task: _Optional[_Union[TaskIdentity, _Mapping]] = ..., user: _Optional[_Union[UserIdentity, _Mapping]] = ..., orchestrator: _Optional[_Union[OrchestratorIdentity, _Mapping]] = ..., workflow_engine: _Optional[_Union[WorkflowEngineIdentity, _Mapping]] = ..., metrics_bridge: _Optional[_Union[MetricsBridgeIdentity, _Mapping]] = ..., bridge: _Optional[_Union[BridgeIdentity, _Mapping]] = ..., service: _Optional[_Union[ServiceIdentity, _Mapping]] = ..., credentials: _Optional[_Mapping[str, str]] = ..., resume_session_id: _Optional[str] = ...) -> None: ...

class WorkflowEngineIdentity(_message.Message):
    __slots__ = ("instance_id",)
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    def __init__(self, instance_id: _Optional[str] = ...) -> None: ...

class MetricsBridgeIdentity(_message.Message):
    __slots__ = ("instance_id",)
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    def __init__(self, instance_id: _Optional[str] = ...) -> None: ...

class OrchestratorIdentity(_message.Message):
    __slots__ = ("implementation", "specifier", "supported_profiles")
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    SUPPORTED_PROFILES_FIELD_NUMBER: _ClassVar[int]
    implementation: str
    specifier: str
    supported_profiles: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, implementation: _Optional[str] = ..., specifier: _Optional[str] = ..., supported_profiles: _Optional[_Iterable[str]] = ...) -> None: ...

class BridgeIdentity(_message.Message):
    __slots__ = ("implementation", "specifier")
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    implementation: str
    specifier: str
    def __init__(self, implementation: _Optional[str] = ..., specifier: _Optional[str] = ...) -> None: ...

class ServiceIdentity(_message.Message):
    __slots__ = ("implementation", "specifier")
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    implementation: str
    specifier: str
    def __init__(self, implementation: _Optional[str] = ..., specifier: _Optional[str] = ...) -> None: ...

class AgentIdentity(_message.Message):
    __slots__ = ("workspace", "implementation", "specifier")
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    workspace: str
    implementation: str
    specifier: str
    def __init__(self, workspace: _Optional[str] = ..., implementation: _Optional[str] = ..., specifier: _Optional[str] = ...) -> None: ...

class TaskIdentity(_message.Message):
    __slots__ = ("workspace", "implementation", "unique_specifier")
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    workspace: str
    implementation: str
    unique_specifier: str
    def __init__(self, workspace: _Optional[str] = ..., implementation: _Optional[str] = ..., unique_specifier: _Optional[str] = ...) -> None: ...

class UserIdentity(_message.Message):
    __slots__ = ("user_id", "window_id")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    WINDOW_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    window_id: str
    def __init__(self, user_id: _Optional[str] = ..., window_id: _Optional[str] = ...) -> None: ...

class PrincipalRef(_message.Message):
    __slots__ = ("principal_type", "principal_id")
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    principal_type: str
    principal_id: str
    def __init__(self, principal_type: _Optional[str] = ..., principal_id: _Optional[str] = ...) -> None: ...

class AuthorizationContext(_message.Message):
    __slots__ = ("authority_mode", "subject", "grant_id", "resolved")
    AUTHORITY_MODE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_FIELD_NUMBER: _ClassVar[int]
    authority_mode: str
    subject: PrincipalRef
    grant_id: str
    resolved: ResolvedAuthorityInfo
    def __init__(self, authority_mode: _Optional[str] = ..., subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., grant_id: _Optional[str] = ..., resolved: _Optional[_Union[ResolvedAuthorityInfo, _Mapping]] = ...) -> None: ...

class ResolvedAuthorityInfo(_message.Message):
    __slots__ = ("root_subject", "audience_type", "audience_id", "max_access_level", "workspace_scope", "expires_at_ms")
    ROOT_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    root_subject: PrincipalRef
    audience_type: str
    audience_id: str
    max_access_level: int
    workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    expires_at_ms: int
    def __init__(self, root_subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., max_access_level: _Optional[int] = ..., workspace_scope: _Optional[_Iterable[str]] = ..., expires_at_ms: _Optional[int] = ...) -> None: ...

class SendMessage(_message.Message):
    __slots__ = ("target_topic", "payload", "message_type", "authorization", "app_workspace")
    TARGET_TOPIC_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_FIELD_NUMBER: _ClassVar[int]
    APP_WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    target_topic: str
    payload: bytes
    message_type: MessageType
    authorization: AuthorizationContext
    app_workspace: str
    def __init__(self, target_topic: _Optional[str] = ..., payload: _Optional[bytes] = ..., message_type: _Optional[_Union[MessageType, str]] = ..., authorization: _Optional[_Union[AuthorizationContext, _Mapping]] = ..., app_workspace: _Optional[str] = ...) -> None: ...

class Metric(_message.Message):
    __slots__ = ("trace_id", "entries", "metadata", "client_timestamp_ms")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CLIENT_TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    trace_id: str
    entries: _containers.RepeatedCompositeFieldContainer[MetricEntry]
    metadata: _containers.ScalarMap[str, str]
    client_timestamp_ms: int
    def __init__(self, trace_id: _Optional[str] = ..., entries: _Optional[_Iterable[_Union[MetricEntry, _Mapping]]] = ..., metadata: _Optional[_Mapping[str, str]] = ..., client_timestamp_ms: _Optional[int] = ...) -> None: ...

class MetricEntry(_message.Message):
    __slots__ = ("name", "kind", "qty")
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    QTY_FIELD_NUMBER: _ClassVar[int]
    name: str
    kind: str
    qty: float
    def __init__(self, name: _Optional[str] = ..., kind: _Optional[str] = ..., qty: _Optional[float] = ...) -> None: ...

class SwitchWorkspace(_message.Message):
    __slots__ = ("new_workspace_id",)
    NEW_WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    new_workspace_id: str
    def __init__(self, new_workspace_id: _Optional[str] = ...) -> None: ...

class KVOperation(_message.Message):
    __slots__ = ("op", "scope", "key", "value", "user_id", "workspace", "ttl", "request_id", "authorization", "guard_value", "delta_value")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        GET: _ClassVar[KVOperation.OpType]
        PUT: _ClassVar[KVOperation.OpType]
        LIST: _ClassVar[KVOperation.OpType]
        DELETE: _ClassVar[KVOperation.OpType]
        INCREMENT: _ClassVar[KVOperation.OpType]
        DECREMENT: _ClassVar[KVOperation.OpType]
        INCREMENT_IF: _ClassVar[KVOperation.OpType]
        DECREMENT_IF: _ClassVar[KVOperation.OpType]
    GET: KVOperation.OpType
    PUT: KVOperation.OpType
    LIST: KVOperation.OpType
    DELETE: KVOperation.OpType
    INCREMENT: KVOperation.OpType
    DECREMENT: KVOperation.OpType
    INCREMENT_IF: KVOperation.OpType
    DECREMENT_IF: KVOperation.OpType
    class Scope(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SCOPE_UNSPECIFIED: _ClassVar[KVOperation.Scope]
        GLOBAL: _ClassVar[KVOperation.Scope]
        WORKSPACE: _ClassVar[KVOperation.Scope]
        USER: _ClassVar[KVOperation.Scope]
        USER_WORKSPACE: _ClassVar[KVOperation.Scope]
        GLOBAL_EXCLUSIVE: _ClassVar[KVOperation.Scope]
        WORKSPACE_EXCLUSIVE: _ClassVar[KVOperation.Scope]
        USER_SHARED: _ClassVar[KVOperation.Scope]
        USER_WORKSPACE_SHARED: _ClassVar[KVOperation.Scope]
    SCOPE_UNSPECIFIED: KVOperation.Scope
    GLOBAL: KVOperation.Scope
    WORKSPACE: KVOperation.Scope
    USER: KVOperation.Scope
    USER_WORKSPACE: KVOperation.Scope
    GLOBAL_EXCLUSIVE: KVOperation.Scope
    WORKSPACE_EXCLUSIVE: KVOperation.Scope
    USER_SHARED: KVOperation.Scope
    USER_WORKSPACE_SHARED: KVOperation.Scope
    OP_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_FIELD_NUMBER: _ClassVar[int]
    GUARD_VALUE_FIELD_NUMBER: _ClassVar[int]
    DELTA_VALUE_FIELD_NUMBER: _ClassVar[int]
    op: KVOperation.OpType
    scope: KVOperation.Scope
    key: str
    value: bytes
    user_id: str
    workspace: str
    ttl: int
    request_id: str
    authorization: AuthorizationContext
    guard_value: int
    delta_value: int
    def __init__(self, op: _Optional[_Union[KVOperation.OpType, str]] = ..., scope: _Optional[_Union[KVOperation.Scope, str]] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ..., user_id: _Optional[str] = ..., workspace: _Optional[str] = ..., ttl: _Optional[int] = ..., request_id: _Optional[str] = ..., authorization: _Optional[_Union[AuthorizationContext, _Mapping]] = ..., guard_value: _Optional[int] = ..., delta_value: _Optional[int] = ...) -> None: ...

class KVResponse(_message.Message):
    __slots__ = ("success", "value", "keys", "kv_map", "request_id", "counter_value", "applied")
    class KvMapEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    KV_MAP_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    COUNTER_VALUE_FIELD_NUMBER: _ClassVar[int]
    APPLIED_FIELD_NUMBER: _ClassVar[int]
    success: bool
    value: bytes
    keys: _containers.RepeatedScalarFieldContainer[str]
    kv_map: _containers.ScalarMap[str, bytes]
    request_id: str
    counter_value: int
    applied: bool
    def __init__(self, success: bool = ..., value: _Optional[bytes] = ..., keys: _Optional[_Iterable[str]] = ..., kv_map: _Optional[_Mapping[str, bytes]] = ..., request_id: _Optional[str] = ..., counter_value: _Optional[int] = ..., applied: bool = ...) -> None: ...

class IncomingMessage(_message.Message):
    __slots__ = ("source_topic", "payload", "message_type", "workspace")
    SOURCE_TOPIC_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    source_topic: str
    payload: bytes
    message_type: MessageType
    workspace: str
    def __init__(self, source_topic: _Optional[str] = ..., payload: _Optional[bytes] = ..., message_type: _Optional[_Union[MessageType, str]] = ..., workspace: _Optional[str] = ...) -> None: ...

class ConfigSnapshot(_message.Message):
    __slots__ = ("kv", "global_kv", "task_context", "workspace_exclusive_kv", "global_exclusive_kv")
    class KvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class GlobalKvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class TaskContextEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class WorkspaceExclusiveKvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class GlobalExclusiveKvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    KV_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_KV_FIELD_NUMBER: _ClassVar[int]
    TASK_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_EXCLUSIVE_KV_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_EXCLUSIVE_KV_FIELD_NUMBER: _ClassVar[int]
    kv: _containers.ScalarMap[str, bytes]
    global_kv: _containers.ScalarMap[str, bytes]
    task_context: _containers.ScalarMap[str, str]
    workspace_exclusive_kv: _containers.ScalarMap[str, bytes]
    global_exclusive_kv: _containers.ScalarMap[str, bytes]
    def __init__(self, kv: _Optional[_Mapping[str, bytes]] = ..., global_kv: _Optional[_Mapping[str, bytes]] = ..., task_context: _Optional[_Mapping[str, str]] = ..., workspace_exclusive_kv: _Optional[_Mapping[str, bytes]] = ..., global_exclusive_kv: _Optional[_Mapping[str, bytes]] = ...) -> None: ...

class Signal(_message.Message):
    __slots__ = ("type", "reason")
    class SignalType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        FORCE_DISCONNECT: _ClassVar[Signal.SignalType]
        GRACEFUL_DISCONNECT: _ClassVar[Signal.SignalType]
    FORCE_DISCONNECT: Signal.SignalType
    GRACEFUL_DISCONNECT: Signal.SignalType
    TYPE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    type: Signal.SignalType
    reason: str
    def __init__(self, type: _Optional[_Union[Signal.SignalType, str]] = ..., reason: _Optional[str] = ...) -> None: ...

class ErrorResponse(_message.Message):
    __slots__ = ("code", "message", "retryable", "retry_after_ms", "request_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_MS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    retryable: bool
    retry_after_ms: int
    request_id: str
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., retryable: bool = ..., retry_after_ms: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class CreateTaskRequest(_message.Message):
    __slots__ = ("task_type", "workspace", "assignment_mode", "target_agent_id", "launch_param_overrides", "metadata", "payload", "target_implementation", "authorization", "request_id", "target_identity", "task_class", "context_id")
    class LaunchParamOverridesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_MODE_FIELD_NUMBER: _ClassVar[int]
    TARGET_AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_PARAM_OVERRIDES_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TARGET_IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TASK_CLASS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    task_type: str
    workspace: str
    assignment_mode: TaskAssignmentMode
    target_agent_id: str
    launch_param_overrides: _containers.ScalarMap[str, str]
    metadata: _containers.ScalarMap[str, str]
    payload: bytes
    target_implementation: str
    authorization: AuthorizationContext
    request_id: str
    target_identity: str
    task_class: TaskClass
    context_id: str
    def __init__(self, task_type: _Optional[str] = ..., workspace: _Optional[str] = ..., assignment_mode: _Optional[_Union[TaskAssignmentMode, str]] = ..., target_agent_id: _Optional[str] = ..., launch_param_overrides: _Optional[_Mapping[str, str]] = ..., metadata: _Optional[_Mapping[str, str]] = ..., payload: _Optional[bytes] = ..., target_implementation: _Optional[str] = ..., authorization: _Optional[_Union[AuthorizationContext, _Mapping]] = ..., request_id: _Optional[str] = ..., target_identity: _Optional[str] = ..., task_class: _Optional[_Union[TaskClass, str]] = ..., context_id: _Optional[str] = ...) -> None: ...

class CreateTaskResponse(_message.Message):
    __slots__ = ("success", "task_id", "status", "error_code", "error_message", "request_id", "assigned_to", "task_token", "authority_grant_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_TO_FIELD_NUMBER: _ClassVar[int]
    TASK_TOKEN_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    task_id: str
    status: str
    error_code: str
    error_message: str
    request_id: str
    assigned_to: str
    task_token: str
    authority_grant_id: str
    def __init__(self, success: bool = ..., task_id: _Optional[str] = ..., status: _Optional[str] = ..., error_code: _Optional[str] = ..., error_message: _Optional[str] = ..., request_id: _Optional[str] = ..., assigned_to: _Optional[str] = ..., task_token: _Optional[str] = ..., authority_grant_id: _Optional[str] = ...) -> None: ...

class TaskAssignment(_message.Message):
    __slots__ = ("task_id", "task_type", "assigned_to", "metadata", "assigned_at", "profile", "launch_params", "target_implementation", "workspace", "specifier", "payload", "task_class", "checkpoint_key", "resume_session_id")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class LaunchParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_TO_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_AT_FIELD_NUMBER: _ClassVar[int]
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_PARAMS_FIELD_NUMBER: _ClassVar[int]
    TARGET_IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TASK_CLASS_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_KEY_FIELD_NUMBER: _ClassVar[int]
    RESUME_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    task_type: str
    assigned_to: str
    metadata: _containers.ScalarMap[str, str]
    assigned_at: int
    profile: str
    launch_params: _containers.ScalarMap[str, str]
    target_implementation: str
    workspace: str
    specifier: str
    payload: bytes
    task_class: TaskClass
    checkpoint_key: str
    resume_session_id: str
    def __init__(self, task_id: _Optional[str] = ..., task_type: _Optional[str] = ..., assigned_to: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., assigned_at: _Optional[int] = ..., profile: _Optional[str] = ..., launch_params: _Optional[_Mapping[str, str]] = ..., target_implementation: _Optional[str] = ..., workspace: _Optional[str] = ..., specifier: _Optional[str] = ..., payload: _Optional[bytes] = ..., task_class: _Optional[_Union[TaskClass, str]] = ..., checkpoint_key: _Optional[str] = ..., resume_session_id: _Optional[str] = ...) -> None: ...

class CheckpointOperation(_message.Message):
    __slots__ = ("op", "key", "data", "ttl", "request_id")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SAVE: _ClassVar[CheckpointOperation.OpType]
        LOAD: _ClassVar[CheckpointOperation.OpType]
        DELETE: _ClassVar[CheckpointOperation.OpType]
        LIST: _ClassVar[CheckpointOperation.OpType]
    SAVE: CheckpointOperation.OpType
    LOAD: CheckpointOperation.OpType
    DELETE: CheckpointOperation.OpType
    LIST: CheckpointOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    op: CheckpointOperation.OpType
    key: str
    data: bytes
    ttl: int
    request_id: str
    def __init__(self, op: _Optional[_Union[CheckpointOperation.OpType, str]] = ..., key: _Optional[str] = ..., data: _Optional[bytes] = ..., ttl: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class CheckpointResponse(_message.Message):
    __slots__ = ("success", "data", "keys", "error", "saved_at", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    SAVED_AT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    data: bytes
    keys: _containers.RepeatedScalarFieldContainer[str]
    error: str
    saved_at: int
    request_id: str
    def __init__(self, success: bool = ..., data: _Optional[bytes] = ..., keys: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ..., saved_at: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class AdminQuery(_message.Message):
    __slots__ = ("op", "session_id", "filter", "request_id")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        GET_HEALTH: _ClassVar[AdminQuery.OpType]
        GET_INFO: _ClassVar[AdminQuery.OpType]
        GET_STATS: _ClassVar[AdminQuery.OpType]
        LIST_CONNECTIONS: _ClassVar[AdminQuery.OpType]
        GET_CONNECTION: _ClassVar[AdminQuery.OpType]
    GET_HEALTH: AdminQuery.OpType
    GET_INFO: AdminQuery.OpType
    GET_STATS: AdminQuery.OpType
    LIST_CONNECTIONS: AdminQuery.OpType
    GET_CONNECTION: AdminQuery.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    op: AdminQuery.OpType
    session_id: str
    filter: ConnectionFilter
    request_id: str
    def __init__(self, op: _Optional[_Union[AdminQuery.OpType, str]] = ..., session_id: _Optional[str] = ..., filter: _Optional[_Union[ConnectionFilter, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ConnectionFilter(_message.Message):
    __slots__ = ("type", "workspace", "limit", "offset")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    type: PrincipalType
    workspace: str
    limit: int
    offset: int
    def __init__(self, type: _Optional[_Union[PrincipalType, str]] = ..., workspace: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class ConnectionInfo(_message.Message):
    __slots__ = ("session_id", "type", "identity", "workspace", "implementation", "specifier", "connected_at", "duration", "remote_addr", "last_activity")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_AT_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    REMOTE_ADDR_FIELD_NUMBER: _ClassVar[int]
    LAST_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    type: PrincipalType
    identity: str
    workspace: str
    implementation: str
    specifier: str
    connected_at: int
    duration: str
    remote_addr: str
    last_activity: int
    def __init__(self, session_id: _Optional[str] = ..., type: _Optional[_Union[PrincipalType, str]] = ..., identity: _Optional[str] = ..., workspace: _Optional[str] = ..., implementation: _Optional[str] = ..., specifier: _Optional[str] = ..., connected_at: _Optional[int] = ..., duration: _Optional[str] = ..., remote_addr: _Optional[str] = ..., last_activity: _Optional[int] = ...) -> None: ...

class AdminResponse(_message.Message):
    __slots__ = ("success", "error", "health", "info", "stats", "connection", "connections", "total_count", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    HEALTH_FIELD_NUMBER: _ClassVar[int]
    INFO_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_FIELD_NUMBER: _ClassVar[int]
    CONNECTIONS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    health: HealthInfo
    info: GatewayInfo
    stats: GatewayStats
    connection: ConnectionInfo
    connections: _containers.RepeatedCompositeFieldContainer[ConnectionInfo]
    total_count: int
    request_id: str
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., health: _Optional[_Union[HealthInfo, _Mapping]] = ..., info: _Optional[_Union[GatewayInfo, _Mapping]] = ..., stats: _Optional[_Union[GatewayStats, _Mapping]] = ..., connection: _Optional[_Union[ConnectionInfo, _Mapping]] = ..., connections: _Optional[_Iterable[_Union[ConnectionInfo, _Mapping]]] = ..., total_count: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class HealthInfo(_message.Message):
    __slots__ = ("status", "timestamp", "checks", "stats")
    class ChecksEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: HealthCheck
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[HealthCheck, _Mapping]] = ...) -> None: ...
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CHECKS_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    status: HealthStatus
    timestamp: int
    checks: _containers.MessageMap[str, HealthCheck]
    stats: GatewayStats
    def __init__(self, status: _Optional[_Union[HealthStatus, str]] = ..., timestamp: _Optional[int] = ..., checks: _Optional[_Mapping[str, HealthCheck]] = ..., stats: _Optional[_Union[GatewayStats, _Mapping]] = ...) -> None: ...

class HealthCheck(_message.Message):
    __slots__ = ("status", "latency", "error")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LATENCY_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    status: HealthCheckStatus
    latency: str
    error: str
    def __init__(self, status: _Optional[_Union[HealthCheckStatus, str]] = ..., latency: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class GatewayInfo(_message.Message):
    __slots__ = ("gateway_id", "version", "started_at", "uptime", "go_version", "num_goroutines", "memory_alloc_mb", "num_connections")
    GATEWAY_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    UPTIME_FIELD_NUMBER: _ClassVar[int]
    GO_VERSION_FIELD_NUMBER: _ClassVar[int]
    NUM_GOROUTINES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_ALLOC_MB_FIELD_NUMBER: _ClassVar[int]
    NUM_CONNECTIONS_FIELD_NUMBER: _ClassVar[int]
    gateway_id: str
    version: str
    started_at: int
    uptime: str
    go_version: str
    num_goroutines: int
    memory_alloc_mb: float
    num_connections: int
    def __init__(self, gateway_id: _Optional[str] = ..., version: _Optional[str] = ..., started_at: _Optional[int] = ..., uptime: _Optional[str] = ..., go_version: _Optional[str] = ..., num_goroutines: _Optional[int] = ..., memory_alloc_mb: _Optional[float] = ..., num_connections: _Optional[int] = ...) -> None: ...

class GatewayStats(_message.Message):
    __slots__ = ("agent_connections", "task_connections", "user_connections", "orchestrator_connections", "workflow_engine_connected", "metrics_bridge_connected", "total_tasks", "pending_tasks", "running_tasks", "completed_tasks", "failed_tasks", "messages_per_second", "total_messages", "active_timers", "pending_timers")
    AGENT_CONNECTIONS_FIELD_NUMBER: _ClassVar[int]
    TASK_CONNECTIONS_FIELD_NUMBER: _ClassVar[int]
    USER_CONNECTIONS_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATOR_CONNECTIONS_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ENGINE_CONNECTED_FIELD_NUMBER: _ClassVar[int]
    METRICS_BRIDGE_CONNECTED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_TASKS_FIELD_NUMBER: _ClassVar[int]
    PENDING_TASKS_FIELD_NUMBER: _ClassVar[int]
    RUNNING_TASKS_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_TASKS_FIELD_NUMBER: _ClassVar[int]
    FAILED_TASKS_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_PER_SECOND_FIELD_NUMBER: _ClassVar[int]
    TOTAL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TIMERS_FIELD_NUMBER: _ClassVar[int]
    PENDING_TIMERS_FIELD_NUMBER: _ClassVar[int]
    agent_connections: int
    task_connections: int
    user_connections: int
    orchestrator_connections: int
    workflow_engine_connected: bool
    metrics_bridge_connected: bool
    total_tasks: int
    pending_tasks: int
    running_tasks: int
    completed_tasks: int
    failed_tasks: int
    messages_per_second: float
    total_messages: int
    active_timers: int
    pending_timers: int
    def __init__(self, agent_connections: _Optional[int] = ..., task_connections: _Optional[int] = ..., user_connections: _Optional[int] = ..., orchestrator_connections: _Optional[int] = ..., workflow_engine_connected: bool = ..., metrics_bridge_connected: bool = ..., total_tasks: _Optional[int] = ..., pending_tasks: _Optional[int] = ..., running_tasks: _Optional[int] = ..., completed_tasks: _Optional[int] = ..., failed_tasks: _Optional[int] = ..., messages_per_second: _Optional[float] = ..., total_messages: _Optional[int] = ..., active_timers: _Optional[int] = ..., pending_timers: _Optional[int] = ...) -> None: ...

class SessionOperation(_message.Message):
    __slots__ = ("op", "session_id", "reason", "request_id", "filter", "authorization")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DISCONNECT: _ClassVar[SessionOperation.OpType]
        LIST: _ClassVar[SessionOperation.OpType]
        GET: _ClassVar[SessionOperation.OpType]
    DISCONNECT: SessionOperation.OpType
    LIST: SessionOperation.OpType
    GET: SessionOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_FIELD_NUMBER: _ClassVar[int]
    op: SessionOperation.OpType
    session_id: str
    reason: str
    request_id: str
    filter: ConnectionFilter
    authorization: AuthorizationContext
    def __init__(self, op: _Optional[_Union[SessionOperation.OpType, str]] = ..., session_id: _Optional[str] = ..., reason: _Optional[str] = ..., request_id: _Optional[str] = ..., filter: _Optional[_Union[ConnectionFilter, _Mapping]] = ..., authorization: _Optional[_Union[AuthorizationContext, _Mapping]] = ...) -> None: ...

class SessionOperationResponse(_message.Message):
    __slots__ = ("success", "message", "error", "request_id", "connection", "connections", "total_count")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_FIELD_NUMBER: _ClassVar[int]
    CONNECTIONS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    error: str
    request_id: str
    connection: ConnectionInfo
    connections: _containers.RepeatedCompositeFieldContainer[ConnectionInfo]
    total_count: int
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., error: _Optional[str] = ..., request_id: _Optional[str] = ..., connection: _Optional[_Union[ConnectionInfo, _Mapping]] = ..., connections: _Optional[_Iterable[_Union[ConnectionInfo, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class TaskQuery(_message.Message):
    __slots__ = ("op", "task_id", "filter", "request_id")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        LIST: _ClassVar[TaskQuery.OpType]
        GET: _ClassVar[TaskQuery.OpType]
    LIST: TaskQuery.OpType
    GET: TaskQuery.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    op: TaskQuery.OpType
    task_id: str
    filter: TaskFilter
    request_id: str
    def __init__(self, op: _Optional[_Union[TaskQuery.OpType, str]] = ..., task_id: _Optional[str] = ..., filter: _Optional[_Union[TaskFilter, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class TaskFilter(_message.Message):
    __slots__ = ("status", "workspace", "task_type", "limit", "offset", "statuses", "subject_type", "subject_id", "authority_mode", "authority_grant_id", "root_authority_grant_id", "parent_task_id", "task_class", "exclude_task_classes", "context_id", "exclude_statuses")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    STATUSES_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_MODE_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_CLASS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TASK_CLASSES_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_STATUSES_FIELD_NUMBER: _ClassVar[int]
    status: TaskStatus
    workspace: str
    task_type: str
    limit: int
    offset: int
    statuses: _containers.RepeatedScalarFieldContainer[TaskStatus]
    subject_type: str
    subject_id: str
    authority_mode: str
    authority_grant_id: str
    root_authority_grant_id: str
    parent_task_id: str
    task_class: TaskClass
    exclude_task_classes: _containers.RepeatedScalarFieldContainer[TaskClass]
    context_id: str
    exclude_statuses: _containers.RepeatedScalarFieldContainer[TaskStatus]
    def __init__(self, status: _Optional[_Union[TaskStatus, str]] = ..., workspace: _Optional[str] = ..., task_type: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., statuses: _Optional[_Iterable[_Union[TaskStatus, str]]] = ..., subject_type: _Optional[str] = ..., subject_id: _Optional[str] = ..., authority_mode: _Optional[str] = ..., authority_grant_id: _Optional[str] = ..., root_authority_grant_id: _Optional[str] = ..., parent_task_id: _Optional[str] = ..., task_class: _Optional[_Union[TaskClass, str]] = ..., exclude_task_classes: _Optional[_Iterable[_Union[TaskClass, str]]] = ..., context_id: _Optional[str] = ..., exclude_statuses: _Optional[_Iterable[_Union[TaskStatus, str]]] = ...) -> None: ...

class TaskInfo(_message.Message):
    __slots__ = ("task_id", "task_type", "status", "workspace", "target_topic", "assigned_to", "created_at", "started_at", "completed_at", "attempt", "max_attempts", "error", "metadata", "authority_mode", "subject_type", "subject_id", "root_subject_type", "root_subject_id", "authority_grant_id", "root_authority_grant_id", "parent_authority_grant_id", "creator_actor_id", "parent_task_id", "task_class", "disconnected_at", "grace_window_ms", "wait_spec", "depends_on", "context_id", "paused_at")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    TARGET_TOPIC_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_TO_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_MODE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATOR_ACTOR_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_CLASS_FIELD_NUMBER: _ClassVar[int]
    DISCONNECTED_AT_FIELD_NUMBER: _ClassVar[int]
    GRACE_WINDOW_MS_FIELD_NUMBER: _ClassVar[int]
    WAIT_SPEC_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    PAUSED_AT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    task_type: str
    status: TaskStatus
    workspace: str
    target_topic: str
    assigned_to: str
    created_at: int
    started_at: int
    completed_at: int
    attempt: int
    max_attempts: int
    error: str
    metadata: _containers.ScalarMap[str, str]
    authority_mode: str
    subject_type: str
    subject_id: str
    root_subject_type: str
    root_subject_id: str
    authority_grant_id: str
    root_authority_grant_id: str
    parent_authority_grant_id: str
    creator_actor_id: str
    parent_task_id: str
    task_class: TaskClass
    disconnected_at: int
    grace_window_ms: int
    wait_spec: WaitSpec
    depends_on: _containers.RepeatedScalarFieldContainer[str]
    context_id: str
    paused_at: int
    def __init__(self, task_id: _Optional[str] = ..., task_type: _Optional[str] = ..., status: _Optional[_Union[TaskStatus, str]] = ..., workspace: _Optional[str] = ..., target_topic: _Optional[str] = ..., assigned_to: _Optional[str] = ..., created_at: _Optional[int] = ..., started_at: _Optional[int] = ..., completed_at: _Optional[int] = ..., attempt: _Optional[int] = ..., max_attempts: _Optional[int] = ..., error: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., authority_mode: _Optional[str] = ..., subject_type: _Optional[str] = ..., subject_id: _Optional[str] = ..., root_subject_type: _Optional[str] = ..., root_subject_id: _Optional[str] = ..., authority_grant_id: _Optional[str] = ..., root_authority_grant_id: _Optional[str] = ..., parent_authority_grant_id: _Optional[str] = ..., creator_actor_id: _Optional[str] = ..., parent_task_id: _Optional[str] = ..., task_class: _Optional[_Union[TaskClass, str]] = ..., disconnected_at: _Optional[int] = ..., grace_window_ms: _Optional[int] = ..., wait_spec: _Optional[_Union[WaitSpec, _Mapping]] = ..., depends_on: _Optional[_Iterable[str]] = ..., context_id: _Optional[str] = ..., paused_at: _Optional[int] = ...) -> None: ...

class TaskQueryResponse(_message.Message):
    __slots__ = ("success", "error", "task", "tasks", "total_count", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    TASKS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    task: TaskInfo
    tasks: _containers.RepeatedCompositeFieldContainer[TaskInfo]
    total_count: int
    request_id: str
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., task: _Optional[_Union[TaskInfo, _Mapping]] = ..., tasks: _Optional[_Iterable[_Union[TaskInfo, _Mapping]]] = ..., total_count: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class TaskOperation(_message.Message):
    __slots__ = ("op", "task_id", "reason", "request_id", "wait_spec")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RETRY: _ClassVar[TaskOperation.OpType]
        CANCEL: _ClassVar[TaskOperation.OpType]
        COMPLETE: _ClassVar[TaskOperation.OpType]
        FAIL: _ClassVar[TaskOperation.OpType]
        PAUSE: _ClassVar[TaskOperation.OpType]
        WAIT_FOR: _ClassVar[TaskOperation.OpType]
        RESUME: _ClassVar[TaskOperation.OpType]
        REJECT: _ClassVar[TaskOperation.OpType]
    RETRY: TaskOperation.OpType
    CANCEL: TaskOperation.OpType
    COMPLETE: TaskOperation.OpType
    FAIL: TaskOperation.OpType
    PAUSE: TaskOperation.OpType
    WAIT_FOR: TaskOperation.OpType
    RESUME: TaskOperation.OpType
    REJECT: TaskOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    WAIT_SPEC_FIELD_NUMBER: _ClassVar[int]
    op: TaskOperation.OpType
    task_id: str
    reason: str
    request_id: str
    wait_spec: WaitSpec
    def __init__(self, op: _Optional[_Union[TaskOperation.OpType, str]] = ..., task_id: _Optional[str] = ..., reason: _Optional[str] = ..., request_id: _Optional[str] = ..., wait_spec: _Optional[_Union[WaitSpec, _Mapping]] = ...) -> None: ...

class WaitSpec(_message.Message):
    __slots__ = ("reason", "expected_principal", "input_match", "authority_request_id", "depends_on", "wake_on_any", "timeout_ms", "scheduled_wake_unix_ms", "hibernation")
    class InputMatchEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REASON_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    INPUT_MATCH_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    WAKE_ON_ANY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_WAKE_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    HIBERNATION_FIELD_NUMBER: _ClassVar[int]
    reason: WaitReason
    expected_principal: str
    input_match: _containers.ScalarMap[str, str]
    authority_request_id: str
    depends_on: _containers.RepeatedScalarFieldContainer[str]
    wake_on_any: bool
    timeout_ms: int
    scheduled_wake_unix_ms: int
    hibernation: HibernationDescriptor
    def __init__(self, reason: _Optional[_Union[WaitReason, str]] = ..., expected_principal: _Optional[str] = ..., input_match: _Optional[_Mapping[str, str]] = ..., authority_request_id: _Optional[str] = ..., depends_on: _Optional[_Iterable[str]] = ..., wake_on_any: bool = ..., timeout_ms: _Optional[int] = ..., scheduled_wake_unix_ms: _Optional[int] = ..., hibernation: _Optional[_Union[HibernationDescriptor, _Mapping]] = ...) -> None: ...

class HibernationDescriptor(_message.Message):
    __slots__ = ("checkpoint_key", "resume_session_id", "wake_event_types", "escalation_policy")
    CHECKPOINT_KEY_FIELD_NUMBER: _ClassVar[int]
    RESUME_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    WAKE_EVENT_TYPES_FIELD_NUMBER: _ClassVar[int]
    ESCALATION_POLICY_FIELD_NUMBER: _ClassVar[int]
    checkpoint_key: str
    resume_session_id: str
    wake_event_types: _containers.RepeatedScalarFieldContainer[str]
    escalation_policy: str
    def __init__(self, checkpoint_key: _Optional[str] = ..., resume_session_id: _Optional[str] = ..., wake_event_types: _Optional[_Iterable[str]] = ..., escalation_policy: _Optional[str] = ...) -> None: ...

class TaskOperationResponse(_message.Message):
    __slots__ = ("success", "message", "error", "task", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    error: str
    task: TaskInfo
    request_id: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ..., error: _Optional[str] = ..., task: _Optional[_Union[TaskInfo, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class WorkspaceOperation(_message.Message):
    __slots__ = ("op", "workspace_id", "filter", "workspace", "request_id")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        LIST: _ClassVar[WorkspaceOperation.OpType]
        GET: _ClassVar[WorkspaceOperation.OpType]
        CREATE: _ClassVar[WorkspaceOperation.OpType]
        UPDATE: _ClassVar[WorkspaceOperation.OpType]
        DELETE: _ClassVar[WorkspaceOperation.OpType]
        GET_MESSAGE_FLOW: _ClassVar[WorkspaceOperation.OpType]
    LIST: WorkspaceOperation.OpType
    GET: WorkspaceOperation.OpType
    CREATE: WorkspaceOperation.OpType
    UPDATE: WorkspaceOperation.OpType
    DELETE: WorkspaceOperation.OpType
    GET_MESSAGE_FLOW: WorkspaceOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    op: WorkspaceOperation.OpType
    workspace_id: str
    filter: WorkspaceFilter
    workspace: WorkspaceInfo
    request_id: str
    def __init__(self, op: _Optional[_Union[WorkspaceOperation.OpType, str]] = ..., workspace_id: _Optional[str] = ..., filter: _Optional[_Union[WorkspaceFilter, _Mapping]] = ..., workspace: _Optional[_Union[WorkspaceInfo, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class WorkspaceFilter(_message.Message):
    __slots__ = ("tenant_id", "limit", "offset")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    limit: int
    offset: int
    def __init__(self, tenant_id: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class WorkspaceInfo(_message.Message):
    __slots__ = ("workspace_id", "display_name", "description", "tenant_id", "created_at", "updated_at", "metadata", "active_agents", "active_tasks", "active_users", "total_messages")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_AGENTS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TASKS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_USERS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    workspace_id: str
    display_name: str
    description: str
    tenant_id: str
    created_at: int
    updated_at: int
    metadata: _containers.ScalarMap[str, str]
    active_agents: int
    active_tasks: int
    active_users: int
    total_messages: int
    def __init__(self, workspace_id: _Optional[str] = ..., display_name: _Optional[str] = ..., description: _Optional[str] = ..., tenant_id: _Optional[str] = ..., created_at: _Optional[int] = ..., updated_at: _Optional[int] = ..., metadata: _Optional[_Mapping[str, str]] = ..., active_agents: _Optional[int] = ..., active_tasks: _Optional[int] = ..., active_users: _Optional[int] = ..., total_messages: _Optional[int] = ...) -> None: ...

class WorkspaceResponse(_message.Message):
    __slots__ = ("success", "error", "message", "workspace", "workspaces", "total_count", "message_flow", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FLOW_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    message: str
    workspace: WorkspaceInfo
    workspaces: _containers.RepeatedCompositeFieldContainer[WorkspaceInfo]
    total_count: int
    message_flow: MessageFlowInfo
    request_id: str
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., message: _Optional[str] = ..., workspace: _Optional[_Union[WorkspaceInfo, _Mapping]] = ..., workspaces: _Optional[_Iterable[_Union[WorkspaceInfo, _Mapping]]] = ..., total_count: _Optional[int] = ..., message_flow: _Optional[_Union[MessageFlowInfo, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class MessageFlowInfo(_message.Message):
    __slots__ = ("workspace_id", "nodes", "edges", "updated_at")
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    EDGES_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    workspace_id: str
    nodes: _containers.RepeatedCompositeFieldContainer[FlowNode]
    edges: _containers.RepeatedCompositeFieldContainer[FlowEdge]
    updated_at: int
    def __init__(self, workspace_id: _Optional[str] = ..., nodes: _Optional[_Iterable[_Union[FlowNode, _Mapping]]] = ..., edges: _Optional[_Iterable[_Union[FlowEdge, _Mapping]]] = ..., updated_at: _Optional[int] = ...) -> None: ...

class FlowNode(_message.Message):
    __slots__ = ("id", "label", "type", "status", "implementation", "specifier", "topic")
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    id: str
    label: str
    type: PrincipalType
    status: str
    implementation: str
    specifier: str
    topic: str
    def __init__(self, id: _Optional[str] = ..., label: _Optional[str] = ..., type: _Optional[_Union[PrincipalType, str]] = ..., status: _Optional[str] = ..., implementation: _Optional[str] = ..., specifier: _Optional[str] = ..., topic: _Optional[str] = ...) -> None: ...

class FlowEdge(_message.Message):
    __slots__ = ("to", "label", "count")
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    to: str
    label: str
    count: int
    def __init__(self, to: _Optional[str] = ..., label: _Optional[str] = ..., count: _Optional[int] = ..., **kwargs) -> None: ...

class AgentOperation(_message.Message):
    __slots__ = ("op", "implementation", "filter", "agent", "launch_params", "request_id")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        LIST: _ClassVar[AgentOperation.OpType]
        GET: _ClassVar[AgentOperation.OpType]
        REGISTER: _ClassVar[AgentOperation.OpType]
        UPDATE: _ClassVar[AgentOperation.OpType]
        DELETE: _ClassVar[AgentOperation.OpType]
        LAUNCH: _ClassVar[AgentOperation.OpType]
        LIST_ORCHESTRATORS: _ClassVar[AgentOperation.OpType]
    LIST: AgentOperation.OpType
    GET: AgentOperation.OpType
    REGISTER: AgentOperation.OpType
    UPDATE: AgentOperation.OpType
    DELETE: AgentOperation.OpType
    LAUNCH: AgentOperation.OpType
    LIST_ORCHESTRATORS: AgentOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_PARAMS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    op: AgentOperation.OpType
    implementation: str
    filter: AgentFilter
    agent: AgentRegistrationInfo
    launch_params: AgentLaunchParams
    request_id: str
    def __init__(self, op: _Optional[_Union[AgentOperation.OpType, str]] = ..., implementation: _Optional[str] = ..., filter: _Optional[_Union[AgentFilter, _Mapping]] = ..., agent: _Optional[_Union[AgentRegistrationInfo, _Mapping]] = ..., launch_params: _Optional[_Union[AgentLaunchParams, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class AgentFilter(_message.Message):
    __slots__ = ("orchestrator_profile", "limit", "offset")
    ORCHESTRATOR_PROFILE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    orchestrator_profile: str
    limit: int
    offset: int
    def __init__(self, orchestrator_profile: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class AgentRegistrationInfo(_message.Message):
    __slots__ = ("implementation", "orchestrator_profile", "description", "launch_params", "registered_at", "updated_at")
    class LaunchParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATOR_PROFILE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_PARAMS_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    implementation: str
    orchestrator_profile: str
    description: str
    launch_params: _containers.ScalarMap[str, str]
    registered_at: int
    updated_at: int
    def __init__(self, implementation: _Optional[str] = ..., orchestrator_profile: _Optional[str] = ..., description: _Optional[str] = ..., launch_params: _Optional[_Mapping[str, str]] = ..., registered_at: _Optional[int] = ..., updated_at: _Optional[int] = ...) -> None: ...

class AgentLaunchParams(_message.Message):
    __slots__ = ("specifier", "workspace", "param_overrides")
    class ParamOverridesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    PARAM_OVERRIDES_FIELD_NUMBER: _ClassVar[int]
    specifier: str
    workspace: str
    param_overrides: _containers.ScalarMap[str, str]
    def __init__(self, specifier: _Optional[str] = ..., workspace: _Optional[str] = ..., param_overrides: _Optional[_Mapping[str, str]] = ...) -> None: ...

class OrchestratorInfo(_message.Message):
    __slots__ = ("orchestrator_id", "profiles", "connected_at")
    ORCHESTRATOR_ID_FIELD_NUMBER: _ClassVar[int]
    PROFILES_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_AT_FIELD_NUMBER: _ClassVar[int]
    orchestrator_id: str
    profiles: _containers.RepeatedScalarFieldContainer[str]
    connected_at: int
    def __init__(self, orchestrator_id: _Optional[str] = ..., profiles: _Optional[_Iterable[str]] = ..., connected_at: _Optional[int] = ...) -> None: ...

class AgentLaunchResult(_message.Message):
    __slots__ = ("task_id", "message")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    message: str
    def __init__(self, task_id: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class AgentResponse(_message.Message):
    __slots__ = ("success", "error", "message", "agent", "agents", "total_count", "orchestrators", "launch_result", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    AGENTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATORS_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_RESULT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    message: str
    agent: AgentRegistrationInfo
    agents: _containers.RepeatedCompositeFieldContainer[AgentRegistrationInfo]
    total_count: int
    orchestrators: _containers.RepeatedCompositeFieldContainer[OrchestratorInfo]
    launch_result: AgentLaunchResult
    request_id: str
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., message: _Optional[str] = ..., agent: _Optional[_Union[AgentRegistrationInfo, _Mapping]] = ..., agents: _Optional[_Iterable[_Union[AgentRegistrationInfo, _Mapping]]] = ..., total_count: _Optional[int] = ..., orchestrators: _Optional[_Iterable[_Union[OrchestratorInfo, _Mapping]]] = ..., launch_result: _Optional[_Union[AgentLaunchResult, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ACLOperation(_message.Message):
    __slots__ = ("op", "rule_id", "rule_category", "retention_days", "rule_filter", "audit_filter", "grant_request", "fallback_request", "request_id")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        LIST_RULES: _ClassVar[ACLOperation.OpType]
        GET_RULE: _ClassVar[ACLOperation.OpType]
        GRANT: _ClassVar[ACLOperation.OpType]
        REVOKE: _ClassVar[ACLOperation.OpType]
        QUERY_AUDIT: _ClassVar[ACLOperation.OpType]
        GET_FALLBACK_POLICY: _ClassVar[ACLOperation.OpType]
        SET_FALLBACK_POLICY: _ClassVar[ACLOperation.OpType]
        CLEANUP_EXPIRED: _ClassVar[ACLOperation.OpType]
        CLEANUP_AUDIT_LOGS: _ClassVar[ACLOperation.OpType]
    LIST_RULES: ACLOperation.OpType
    GET_RULE: ACLOperation.OpType
    GRANT: ACLOperation.OpType
    REVOKE: ACLOperation.OpType
    QUERY_AUDIT: ACLOperation.OpType
    GET_FALLBACK_POLICY: ACLOperation.OpType
    SET_FALLBACK_POLICY: ACLOperation.OpType
    CLEANUP_EXPIRED: ACLOperation.OpType
    CLEANUP_AUDIT_LOGS: ACLOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    RULE_ID_FIELD_NUMBER: _ClassVar[int]
    RULE_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    RULE_FILTER_FIELD_NUMBER: _ClassVar[int]
    AUDIT_FILTER_FIELD_NUMBER: _ClassVar[int]
    GRANT_REQUEST_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_REQUEST_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    op: ACLOperation.OpType
    rule_id: str
    rule_category: str
    retention_days: int
    rule_filter: ACLRuleFilter
    audit_filter: ACLAuditFilter
    grant_request: ACLGrantRequest
    fallback_request: ACLSetFallbackRequest
    request_id: str
    def __init__(self, op: _Optional[_Union[ACLOperation.OpType, str]] = ..., rule_id: _Optional[str] = ..., rule_category: _Optional[str] = ..., retention_days: _Optional[int] = ..., rule_filter: _Optional[_Union[ACLRuleFilter, _Mapping]] = ..., audit_filter: _Optional[_Union[ACLAuditFilter, _Mapping]] = ..., grant_request: _Optional[_Union[ACLGrantRequest, _Mapping]] = ..., fallback_request: _Optional[_Union[ACLSetFallbackRequest, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ACLRuleFilter(_message.Message):
    __slots__ = ("principal_type", "principal_id", "resource_type", "resource_id", "limit", "offset")
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    principal_type: str
    principal_id: str
    resource_type: str
    resource_id: str
    limit: int
    offset: int
    def __init__(self, principal_type: _Optional[str] = ..., principal_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class ACLAuditFilter(_message.Message):
    __slots__ = ("start_time", "end_time", "principal_type", "principal_id", "resource_type", "resource_id", "decision", "workspace", "limit", "offset")
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    start_time: int
    end_time: int
    principal_type: str
    principal_id: str
    resource_type: str
    resource_id: str
    decision: str
    workspace: str
    limit: int
    offset: int
    def __init__(self, start_time: _Optional[int] = ..., end_time: _Optional[int] = ..., principal_type: _Optional[str] = ..., principal_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., decision: _Optional[str] = ..., workspace: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class ACLGrantRequest(_message.Message):
    __slots__ = ("principal_type", "principal_id", "resource_type", "resource_id", "access_level", "granted_by", "reason", "expires_at")
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    GRANTED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    principal_type: str
    principal_id: str
    resource_type: str
    resource_id: str
    access_level: int
    granted_by: str
    reason: str
    expires_at: int
    def __init__(self, principal_type: _Optional[str] = ..., principal_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., access_level: _Optional[int] = ..., granted_by: _Optional[str] = ..., reason: _Optional[str] = ..., expires_at: _Optional[int] = ...) -> None: ...

class ACLSetFallbackRequest(_message.Message):
    __slots__ = ("rule_category", "fallback_access_level", "updated_by")
    RULE_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    rule_category: str
    fallback_access_level: int
    updated_by: str
    def __init__(self, rule_category: _Optional[str] = ..., fallback_access_level: _Optional[int] = ..., updated_by: _Optional[str] = ...) -> None: ...

class ACLAuthorityGrantFilter(_message.Message):
    __slots__ = ("root_grant_id", "subject_type", "subject_id", "delegate_type", "delegate_id", "audience_type", "audience_id", "include_revoked", "active_only", "limit", "offset")
    ROOT_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DELEGATE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DELEGATE_ID_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_REVOKED_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    root_grant_id: str
    subject_type: str
    subject_id: str
    delegate_type: str
    delegate_id: str
    audience_type: str
    audience_id: str
    include_revoked: bool
    active_only: bool
    limit: int
    offset: int
    def __init__(self, root_grant_id: _Optional[str] = ..., subject_type: _Optional[str] = ..., subject_id: _Optional[str] = ..., delegate_type: _Optional[str] = ..., delegate_id: _Optional[str] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., include_revoked: bool = ..., active_only: bool = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class ACLAuthorityGrantResourceScopeEntry(_message.Message):
    __slots__ = ("resource_type", "patterns")
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PATTERNS_FIELD_NUMBER: _ClassVar[int]
    resource_type: str
    patterns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, resource_type: _Optional[str] = ..., patterns: _Optional[_Iterable[str]] = ...) -> None: ...

class ACLAuthorityGrantRequest(_message.Message):
    __slots__ = ("subject", "delegate", "issued_by", "root_subject", "parent_grant_id", "may_delegate", "remaining_hops", "workspace_scope", "resource_scope", "operation_scope", "max_access_level", "audience_type", "audience_id", "valid_while_audience_active", "expires_at", "renewable_until", "reason", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DELEGATE_FIELD_NUMBER: _ClassVar[int]
    ISSUED_BY_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    PARENT_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    MAY_DELEGATE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_HOPS_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    VALID_WHILE_AUDIENCE_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    RENEWABLE_UNTIL_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    subject: PrincipalRef
    delegate: PrincipalRef
    issued_by: PrincipalRef
    root_subject: PrincipalRef
    parent_grant_id: str
    may_delegate: bool
    remaining_hops: int
    workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    resource_scope: _containers.RepeatedCompositeFieldContainer[ACLAuthorityGrantResourceScopeEntry]
    operation_scope: _containers.RepeatedScalarFieldContainer[str]
    max_access_level: int
    audience_type: str
    audience_id: str
    valid_while_audience_active: bool
    expires_at: int
    renewable_until: int
    reason: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., delegate: _Optional[_Union[PrincipalRef, _Mapping]] = ..., issued_by: _Optional[_Union[PrincipalRef, _Mapping]] = ..., root_subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., parent_grant_id: _Optional[str] = ..., may_delegate: bool = ..., remaining_hops: _Optional[int] = ..., workspace_scope: _Optional[_Iterable[str]] = ..., resource_scope: _Optional[_Iterable[_Union[ACLAuthorityGrantResourceScopeEntry, _Mapping]]] = ..., operation_scope: _Optional[_Iterable[str]] = ..., max_access_level: _Optional[int] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., valid_while_audience_active: bool = ..., expires_at: _Optional[int] = ..., renewable_until: _Optional[int] = ..., reason: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ACLRenewAuthorityGrantRequest(_message.Message):
    __slots__ = ("grant_id", "expires_at", "extend_seconds")
    GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    EXTEND_SECONDS_FIELD_NUMBER: _ClassVar[int]
    grant_id: str
    expires_at: int
    extend_seconds: int
    def __init__(self, grant_id: _Optional[str] = ..., expires_at: _Optional[int] = ..., extend_seconds: _Optional[int] = ...) -> None: ...

class ACLRuleInfo(_message.Message):
    __slots__ = ("rule_id", "principal_type", "principal_id", "resource_type", "resource_id", "access_level", "access_level_name", "granted_by", "granted_at", "expires_at", "reason")
    RULE_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    ACCESS_LEVEL_NAME_FIELD_NUMBER: _ClassVar[int]
    GRANTED_BY_FIELD_NUMBER: _ClassVar[int]
    GRANTED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    rule_id: str
    principal_type: str
    principal_id: str
    resource_type: str
    resource_id: str
    access_level: int
    access_level_name: str
    granted_by: str
    granted_at: int
    expires_at: int
    reason: str
    def __init__(self, rule_id: _Optional[str] = ..., principal_type: _Optional[str] = ..., principal_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., access_level: _Optional[int] = ..., access_level_name: _Optional[str] = ..., granted_by: _Optional[str] = ..., granted_at: _Optional[int] = ..., expires_at: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class ACLFallbackPolicyInfo(_message.Message):
    __slots__ = ("policy_id", "rule_category", "fallback_access_level", "fallback_access_level_name", "updated_by", "updated_at")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    RULE_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_ACCESS_LEVEL_NAME_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    rule_category: str
    fallback_access_level: int
    fallback_access_level_name: str
    updated_by: str
    updated_at: int
    def __init__(self, policy_id: _Optional[str] = ..., rule_category: _Optional[str] = ..., fallback_access_level: _Optional[int] = ..., fallback_access_level_name: _Optional[str] = ..., updated_by: _Optional[str] = ..., updated_at: _Optional[int] = ...) -> None: ...

class ACLAuditEntryInfo(_message.Message):
    __slots__ = ("audit_id", "timestamp", "decision", "access_level", "access_level_name", "principal_type", "principal_id", "resource_type", "resource_id", "operation", "workspace", "rule_id", "fallback_applied", "gateway_id", "session_id", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    AUDIT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    ACCESS_LEVEL_NAME_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    RULE_ID_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_APPLIED_FIELD_NUMBER: _ClassVar[int]
    GATEWAY_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    audit_id: int
    timestamp: int
    decision: str
    access_level: int
    access_level_name: str
    principal_type: str
    principal_id: str
    resource_type: str
    resource_id: str
    operation: str
    workspace: str
    rule_id: str
    fallback_applied: bool
    gateway_id: str
    session_id: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, audit_id: _Optional[int] = ..., timestamp: _Optional[int] = ..., decision: _Optional[str] = ..., access_level: _Optional[int] = ..., access_level_name: _Optional[str] = ..., principal_type: _Optional[str] = ..., principal_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., operation: _Optional[str] = ..., workspace: _Optional[str] = ..., rule_id: _Optional[str] = ..., fallback_applied: bool = ..., gateway_id: _Optional[str] = ..., session_id: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ACLAuthorityGrantInfo(_message.Message):
    __slots__ = ("grant_id", "root_grant_id", "subject", "delegate", "issued_by", "root_subject", "parent_grant_id", "may_delegate", "remaining_hops", "workspace_scope", "resource_scope", "operation_scope", "max_access_level", "access_level_name", "audience_type", "audience_id", "valid_while_audience_active", "expires_at", "renewable_until", "renewed_at", "revoked", "revoked_at", "reason", "metadata", "created_at")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DELEGATE_FIELD_NUMBER: _ClassVar[int]
    ISSUED_BY_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    PARENT_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    MAY_DELEGATE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_HOPS_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    ACCESS_LEVEL_NAME_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    VALID_WHILE_AUDIENCE_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    RENEWABLE_UNTIL_FIELD_NUMBER: _ClassVar[int]
    RENEWED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    grant_id: str
    root_grant_id: str
    subject: PrincipalRef
    delegate: PrincipalRef
    issued_by: PrincipalRef
    root_subject: PrincipalRef
    parent_grant_id: str
    may_delegate: bool
    remaining_hops: int
    workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    resource_scope: _containers.RepeatedCompositeFieldContainer[ACLAuthorityGrantResourceScopeEntry]
    operation_scope: _containers.RepeatedScalarFieldContainer[str]
    max_access_level: int
    access_level_name: str
    audience_type: str
    audience_id: str
    valid_while_audience_active: bool
    expires_at: int
    renewable_until: int
    renewed_at: int
    revoked: bool
    revoked_at: int
    reason: str
    metadata: _containers.ScalarMap[str, str]
    created_at: int
    def __init__(self, grant_id: _Optional[str] = ..., root_grant_id: _Optional[str] = ..., subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., delegate: _Optional[_Union[PrincipalRef, _Mapping]] = ..., issued_by: _Optional[_Union[PrincipalRef, _Mapping]] = ..., root_subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., parent_grant_id: _Optional[str] = ..., may_delegate: bool = ..., remaining_hops: _Optional[int] = ..., workspace_scope: _Optional[_Iterable[str]] = ..., resource_scope: _Optional[_Iterable[_Union[ACLAuthorityGrantResourceScopeEntry, _Mapping]]] = ..., operation_scope: _Optional[_Iterable[str]] = ..., max_access_level: _Optional[int] = ..., access_level_name: _Optional[str] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., valid_while_audience_active: bool = ..., expires_at: _Optional[int] = ..., renewable_until: _Optional[int] = ..., renewed_at: _Optional[int] = ..., revoked: bool = ..., revoked_at: _Optional[int] = ..., reason: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., created_at: _Optional[int] = ...) -> None: ...

class ACLCleanupResult(_message.Message):
    __slots__ = ("deleted_count", "message")
    DELETED_COUNT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    deleted_count: int
    message: str
    def __init__(self, deleted_count: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...

class ACLResponse(_message.Message):
    __slots__ = ("success", "error", "message", "rule", "rules", "total_rules", "fallback_policy", "audit_entries", "total_audit_entries", "cleanup_result", "authority_grant", "authority_grants", "total_authority_grants", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RULE_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_RULES_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_POLICY_FIELD_NUMBER: _ClassVar[int]
    AUDIT_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_AUDIT_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    CLEANUP_RESULT_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_AUTHORITY_GRANTS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    message: str
    rule: ACLRuleInfo
    rules: _containers.RepeatedCompositeFieldContainer[ACLRuleInfo]
    total_rules: int
    fallback_policy: ACLFallbackPolicyInfo
    audit_entries: _containers.RepeatedCompositeFieldContainer[ACLAuditEntryInfo]
    total_audit_entries: int
    cleanup_result: ACLCleanupResult
    authority_grant: ACLAuthorityGrantInfo
    authority_grants: _containers.RepeatedCompositeFieldContainer[ACLAuthorityGrantInfo]
    total_authority_grants: int
    request_id: str
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., message: _Optional[str] = ..., rule: _Optional[_Union[ACLRuleInfo, _Mapping]] = ..., rules: _Optional[_Iterable[_Union[ACLRuleInfo, _Mapping]]] = ..., total_rules: _Optional[int] = ..., fallback_policy: _Optional[_Union[ACLFallbackPolicyInfo, _Mapping]] = ..., audit_entries: _Optional[_Iterable[_Union[ACLAuditEntryInfo, _Mapping]]] = ..., total_audit_entries: _Optional[int] = ..., cleanup_result: _Optional[_Union[ACLCleanupResult, _Mapping]] = ..., authority_grant: _Optional[_Union[ACLAuthorityGrantInfo, _Mapping]] = ..., authority_grants: _Optional[_Iterable[_Union[ACLAuthorityGrantInfo, _Mapping]]] = ..., total_authority_grants: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class AuthorityGrantOperation(_message.Message):
    __slots__ = ("op", "grant_id", "exchange_request", "derive_request", "renew_request", "request_id", "list_request", "batch_exchange_request", "derive_for_target_request")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        EXCHANGE: _ClassVar[AuthorityGrantOperation.OpType]
        DERIVE: _ClassVar[AuthorityGrantOperation.OpType]
        GET: _ClassVar[AuthorityGrantOperation.OpType]
        RENEW: _ClassVar[AuthorityGrantOperation.OpType]
        REVOKE: _ClassVar[AuthorityGrantOperation.OpType]
        LIST_MY_GRANTS: _ClassVar[AuthorityGrantOperation.OpType]
        LIST_GRANTS_ON_ME: _ClassVar[AuthorityGrantOperation.OpType]
        BATCH_EXCHANGE: _ClassVar[AuthorityGrantOperation.OpType]
        DERIVE_FOR_TARGET: _ClassVar[AuthorityGrantOperation.OpType]
    EXCHANGE: AuthorityGrantOperation.OpType
    DERIVE: AuthorityGrantOperation.OpType
    GET: AuthorityGrantOperation.OpType
    RENEW: AuthorityGrantOperation.OpType
    REVOKE: AuthorityGrantOperation.OpType
    LIST_MY_GRANTS: AuthorityGrantOperation.OpType
    LIST_GRANTS_ON_ME: AuthorityGrantOperation.OpType
    BATCH_EXCHANGE: AuthorityGrantOperation.OpType
    DERIVE_FOR_TARGET: AuthorityGrantOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    EXCHANGE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    DERIVE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    RENEW_REQUEST_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    LIST_REQUEST_FIELD_NUMBER: _ClassVar[int]
    BATCH_EXCHANGE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    DERIVE_FOR_TARGET_REQUEST_FIELD_NUMBER: _ClassVar[int]
    op: AuthorityGrantOperation.OpType
    grant_id: str
    exchange_request: AuthorityGrantExchangeRequest
    derive_request: AuthorityGrantDeriveRequest
    renew_request: ACLRenewAuthorityGrantRequest
    request_id: str
    list_request: AuthorityGrantListRequest
    batch_exchange_request: AuthorityGrantBatchExchangeRequest
    derive_for_target_request: AuthorityGrantDeriveForTargetRequest
    def __init__(self, op: _Optional[_Union[AuthorityGrantOperation.OpType, str]] = ..., grant_id: _Optional[str] = ..., exchange_request: _Optional[_Union[AuthorityGrantExchangeRequest, _Mapping]] = ..., derive_request: _Optional[_Union[AuthorityGrantDeriveRequest, _Mapping]] = ..., renew_request: _Optional[_Union[ACLRenewAuthorityGrantRequest, _Mapping]] = ..., request_id: _Optional[str] = ..., list_request: _Optional[_Union[AuthorityGrantListRequest, _Mapping]] = ..., batch_exchange_request: _Optional[_Union[AuthorityGrantBatchExchangeRequest, _Mapping]] = ..., derive_for_target_request: _Optional[_Union[AuthorityGrantDeriveForTargetRequest, _Mapping]] = ...) -> None: ...

class AuthorityGrantExchangeRequest(_message.Message):
    __slots__ = ("source_session_id", "workspace_scope", "resource_scope", "operation_scope", "max_access_level", "audience_type", "audience_id", "valid_while_audience_active", "expires_at", "renewable_until", "may_delegate", "remaining_hops", "reason", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SOURCE_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    VALID_WHILE_AUDIENCE_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    RENEWABLE_UNTIL_FIELD_NUMBER: _ClassVar[int]
    MAY_DELEGATE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_HOPS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    source_session_id: str
    workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    resource_scope: _containers.RepeatedCompositeFieldContainer[ACLAuthorityGrantResourceScopeEntry]
    operation_scope: _containers.RepeatedScalarFieldContainer[str]
    max_access_level: int
    audience_type: str
    audience_id: str
    valid_while_audience_active: bool
    expires_at: int
    renewable_until: int
    may_delegate: bool
    remaining_hops: int
    reason: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, source_session_id: _Optional[str] = ..., workspace_scope: _Optional[_Iterable[str]] = ..., resource_scope: _Optional[_Iterable[_Union[ACLAuthorityGrantResourceScopeEntry, _Mapping]]] = ..., operation_scope: _Optional[_Iterable[str]] = ..., max_access_level: _Optional[int] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., valid_while_audience_active: bool = ..., expires_at: _Optional[int] = ..., renewable_until: _Optional[int] = ..., may_delegate: bool = ..., remaining_hops: _Optional[int] = ..., reason: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class AuthorityGrantDeriveRequest(_message.Message):
    __slots__ = ("parent_grant_id", "delegate", "workspace_scope", "resource_scope", "operation_scope", "max_access_level", "audience_type", "audience_id", "valid_while_audience_active", "expires_at", "renewable_until", "may_delegate", "remaining_hops", "reason", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PARENT_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    DELEGATE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    VALID_WHILE_AUDIENCE_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    RENEWABLE_UNTIL_FIELD_NUMBER: _ClassVar[int]
    MAY_DELEGATE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_HOPS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    parent_grant_id: str
    delegate: PrincipalRef
    workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    resource_scope: _containers.RepeatedCompositeFieldContainer[ACLAuthorityGrantResourceScopeEntry]
    operation_scope: _containers.RepeatedScalarFieldContainer[str]
    max_access_level: int
    audience_type: str
    audience_id: str
    valid_while_audience_active: bool
    expires_at: int
    renewable_until: int
    may_delegate: bool
    remaining_hops: int
    reason: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, parent_grant_id: _Optional[str] = ..., delegate: _Optional[_Union[PrincipalRef, _Mapping]] = ..., workspace_scope: _Optional[_Iterable[str]] = ..., resource_scope: _Optional[_Iterable[_Union[ACLAuthorityGrantResourceScopeEntry, _Mapping]]] = ..., operation_scope: _Optional[_Iterable[str]] = ..., max_access_level: _Optional[int] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., valid_while_audience_active: bool = ..., expires_at: _Optional[int] = ..., renewable_until: _Optional[int] = ..., may_delegate: bool = ..., remaining_hops: _Optional[int] = ..., reason: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class AuthorityGrantResponse(_message.Message):
    __slots__ = ("success", "error", "message", "grant", "request_id", "grants", "total", "cache_hint_ttl_seconds")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    GRANT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    GRANTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    CACHE_HINT_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    message: str
    grant: ACLAuthorityGrantInfo
    request_id: str
    grants: _containers.RepeatedCompositeFieldContainer[ACLAuthorityGrantInfo]
    total: int
    cache_hint_ttl_seconds: int
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., message: _Optional[str] = ..., grant: _Optional[_Union[ACLAuthorityGrantInfo, _Mapping]] = ..., request_id: _Optional[str] = ..., grants: _Optional[_Iterable[_Union[ACLAuthorityGrantInfo, _Mapping]]] = ..., total: _Optional[int] = ..., cache_hint_ttl_seconds: _Optional[int] = ...) -> None: ...

class AuthorityGrantListRequest(_message.Message):
    __slots__ = ("audience_type", "audience_id", "include_revoked", "limit", "offset")
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_REVOKED_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    audience_type: str
    audience_id: str
    include_revoked: bool
    limit: int
    offset: int
    def __init__(self, audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., include_revoked: bool = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class AuthorityGrantBatchExchangeRequest(_message.Message):
    __slots__ = ("requests", "stop_on_first_error")
    REQUESTS_FIELD_NUMBER: _ClassVar[int]
    STOP_ON_FIRST_ERROR_FIELD_NUMBER: _ClassVar[int]
    requests: _containers.RepeatedCompositeFieldContainer[AuthorityGrantExchangeRequest]
    stop_on_first_error: bool
    def __init__(self, requests: _Optional[_Iterable[_Union[AuthorityGrantExchangeRequest, _Mapping]]] = ..., stop_on_first_error: bool = ...) -> None: ...

class AuthorityGrantDeriveForTargetRequest(_message.Message):
    __slots__ = ("parent_grant_id", "target", "audience_type", "audience_id", "operation_scope", "max_access_level", "expires_at", "renewable_until", "may_delegate", "remaining_hops", "reason")
    PARENT_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    RENEWABLE_UNTIL_FIELD_NUMBER: _ClassVar[int]
    MAY_DELEGATE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_HOPS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    parent_grant_id: str
    target: PrincipalRef
    audience_type: str
    audience_id: str
    operation_scope: _containers.RepeatedScalarFieldContainer[str]
    max_access_level: int
    expires_at: int
    renewable_until: int
    may_delegate: bool
    remaining_hops: int
    reason: str
    def __init__(self, parent_grant_id: _Optional[str] = ..., target: _Optional[_Union[PrincipalRef, _Mapping]] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., operation_scope: _Optional[_Iterable[str]] = ..., max_access_level: _Optional[int] = ..., expires_at: _Optional[int] = ..., renewable_until: _Optional[int] = ..., may_delegate: bool = ..., remaining_hops: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class AuthorityIdentity(_message.Message):
    __slots__ = ("subject", "root_subject", "delegate", "issued_by")
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DELEGATE_FIELD_NUMBER: _ClassVar[int]
    ISSUED_BY_FIELD_NUMBER: _ClassVar[int]
    subject: PrincipalRef
    root_subject: PrincipalRef
    delegate: PrincipalRef
    issued_by: PrincipalRef
    def __init__(self, subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., root_subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., delegate: _Optional[_Union[PrincipalRef, _Mapping]] = ..., issued_by: _Optional[_Union[PrincipalRef, _Mapping]] = ...) -> None: ...

class AuthoritySpan(_message.Message):
    __slots__ = ("workspace_scope", "max_access_level", "audience_type", "audience_id", "valid_while_audience_active", "expires_at", "renewable_until", "revoked")
    WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    VALID_WHILE_AUDIENCE_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    RENEWABLE_UNTIL_FIELD_NUMBER: _ClassVar[int]
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    max_access_level: int
    audience_type: str
    audience_id: str
    valid_while_audience_active: bool
    expires_at: int
    renewable_until: int
    revoked: bool
    def __init__(self, workspace_scope: _Optional[_Iterable[str]] = ..., max_access_level: _Optional[int] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., valid_while_audience_active: bool = ..., expires_at: _Optional[int] = ..., renewable_until: _Optional[int] = ..., revoked: bool = ...) -> None: ...

class AuthorityGrantRevocation(_message.Message):
    __slots__ = ("grant_id", "root_grant_id", "reason", "revoked_at", "cascade")
    GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    CASCADE_FIELD_NUMBER: _ClassVar[int]
    grant_id: str
    root_grant_id: str
    reason: str
    revoked_at: int
    cascade: bool
    def __init__(self, grant_id: _Optional[str] = ..., root_grant_id: _Optional[str] = ..., reason: _Optional[str] = ..., revoked_at: _Optional[int] = ..., cascade: bool = ...) -> None: ...

class AuthorityRequestRoutingTarget(_message.Message):
    __slots__ = ("principal", "capability")
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    CAPABILITY_FIELD_NUMBER: _ClassVar[int]
    principal: PrincipalRef
    capability: str
    def __init__(self, principal: _Optional[_Union[PrincipalRef, _Mapping]] = ..., capability: _Optional[str] = ...) -> None: ...

class AuthorityRequestResourceScopeEntry(_message.Message):
    __slots__ = ("resource_type", "patterns")
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PATTERNS_FIELD_NUMBER: _ClassVar[int]
    resource_type: str
    patterns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, resource_type: _Optional[str] = ..., patterns: _Optional[_Iterable[str]] = ...) -> None: ...

class AuthorityRequest(_message.Message):
    __slots__ = ("request_id", "status", "requesting_actor", "target_subject", "desired_workspace_scope", "desired_resource_scope", "desired_operation_scope", "requested_access_level", "requested_duration_seconds", "audience_type", "audience_id", "routing_target", "reason", "task_id", "metadata", "created_at", "expires_at", "resolved_at", "granted_grant_id", "resolved_by", "resolution_reason")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REQUESTING_ACTOR_FIELD_NUMBER: _ClassVar[int]
    TARGET_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DESIRED_WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    DESIRED_RESOURCE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    DESIRED_OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    ROUTING_TARGET_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_AT_FIELD_NUMBER: _ClassVar[int]
    GRANTED_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_BY_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_REASON_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    status: AuthorityRequestStatus
    requesting_actor: PrincipalRef
    target_subject: PrincipalRef
    desired_workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    desired_resource_scope: _containers.RepeatedCompositeFieldContainer[AuthorityRequestResourceScopeEntry]
    desired_operation_scope: _containers.RepeatedScalarFieldContainer[str]
    requested_access_level: AccessLevel
    requested_duration_seconds: int
    audience_type: str
    audience_id: str
    routing_target: AuthorityRequestRoutingTarget
    reason: str
    task_id: str
    metadata: _containers.ScalarMap[str, str]
    created_at: int
    expires_at: int
    resolved_at: int
    granted_grant_id: str
    resolved_by: PrincipalRef
    resolution_reason: str
    def __init__(self, request_id: _Optional[str] = ..., status: _Optional[_Union[AuthorityRequestStatus, str]] = ..., requesting_actor: _Optional[_Union[PrincipalRef, _Mapping]] = ..., target_subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., desired_workspace_scope: _Optional[_Iterable[str]] = ..., desired_resource_scope: _Optional[_Iterable[_Union[AuthorityRequestResourceScopeEntry, _Mapping]]] = ..., desired_operation_scope: _Optional[_Iterable[str]] = ..., requested_access_level: _Optional[_Union[AccessLevel, str]] = ..., requested_duration_seconds: _Optional[int] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., routing_target: _Optional[_Union[AuthorityRequestRoutingTarget, _Mapping]] = ..., reason: _Optional[str] = ..., task_id: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., created_at: _Optional[int] = ..., expires_at: _Optional[int] = ..., resolved_at: _Optional[int] = ..., granted_grant_id: _Optional[str] = ..., resolved_by: _Optional[_Union[PrincipalRef, _Mapping]] = ..., resolution_reason: _Optional[str] = ...) -> None: ...

class CreateAuthorityRequestPayload(_message.Message):
    __slots__ = ("requesting_actor", "target_subject", "desired_workspace_scope", "desired_resource_scope", "desired_operation_scope", "requested_access_level", "requested_duration_seconds", "audience_type", "audience_id", "routing_target", "reason", "task_id", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REQUESTING_ACTOR_FIELD_NUMBER: _ClassVar[int]
    TARGET_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DESIRED_WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    DESIRED_RESOURCE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    DESIRED_OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    ROUTING_TARGET_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    requesting_actor: PrincipalRef
    target_subject: PrincipalRef
    desired_workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    desired_resource_scope: _containers.RepeatedCompositeFieldContainer[AuthorityRequestResourceScopeEntry]
    desired_operation_scope: _containers.RepeatedScalarFieldContainer[str]
    requested_access_level: AccessLevel
    requested_duration_seconds: int
    audience_type: str
    audience_id: str
    routing_target: AuthorityRequestRoutingTarget
    reason: str
    task_id: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, requesting_actor: _Optional[_Union[PrincipalRef, _Mapping]] = ..., target_subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., desired_workspace_scope: _Optional[_Iterable[str]] = ..., desired_resource_scope: _Optional[_Iterable[_Union[AuthorityRequestResourceScopeEntry, _Mapping]]] = ..., desired_operation_scope: _Optional[_Iterable[str]] = ..., requested_access_level: _Optional[_Union[AccessLevel, str]] = ..., requested_duration_seconds: _Optional[int] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., routing_target: _Optional[_Union[AuthorityRequestRoutingTarget, _Mapping]] = ..., reason: _Optional[str] = ..., task_id: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ResolveAuthorityRequestPayload(_message.Message):
    __slots__ = ("decision", "granted_workspace_scope", "granted_resource_scope", "granted_operation_scope", "granted_access_level", "granted_duration_seconds", "reason", "may_delegate", "remaining_hops")
    class Decision(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DECISION_UNSPECIFIED: _ClassVar[ResolveAuthorityRequestPayload.Decision]
        APPROVE: _ClassVar[ResolveAuthorityRequestPayload.Decision]
        DENY: _ClassVar[ResolveAuthorityRequestPayload.Decision]
    DECISION_UNSPECIFIED: ResolveAuthorityRequestPayload.Decision
    APPROVE: ResolveAuthorityRequestPayload.Decision
    DENY: ResolveAuthorityRequestPayload.Decision
    DECISION_FIELD_NUMBER: _ClassVar[int]
    GRANTED_WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    GRANTED_RESOURCE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    GRANTED_OPERATION_SCOPE_FIELD_NUMBER: _ClassVar[int]
    GRANTED_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    GRANTED_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    MAY_DELEGATE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_HOPS_FIELD_NUMBER: _ClassVar[int]
    decision: ResolveAuthorityRequestPayload.Decision
    granted_workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    granted_resource_scope: _containers.RepeatedCompositeFieldContainer[AuthorityRequestResourceScopeEntry]
    granted_operation_scope: _containers.RepeatedScalarFieldContainer[str]
    granted_access_level: AccessLevel
    granted_duration_seconds: int
    reason: str
    may_delegate: bool
    remaining_hops: int
    def __init__(self, decision: _Optional[_Union[ResolveAuthorityRequestPayload.Decision, str]] = ..., granted_workspace_scope: _Optional[_Iterable[str]] = ..., granted_resource_scope: _Optional[_Iterable[_Union[AuthorityRequestResourceScopeEntry, _Mapping]]] = ..., granted_operation_scope: _Optional[_Iterable[str]] = ..., granted_access_level: _Optional[_Union[AccessLevel, str]] = ..., granted_duration_seconds: _Optional[int] = ..., reason: _Optional[str] = ..., may_delegate: bool = ..., remaining_hops: _Optional[int] = ...) -> None: ...

class AuthorityRequestListFilter(_message.Message):
    __slots__ = ("status", "workspace", "limit", "offset", "matching_capabilities")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    MATCHING_CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    status: AuthorityRequestStatus
    workspace: str
    limit: int
    offset: int
    matching_capabilities: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, status: _Optional[_Union[AuthorityRequestStatus, str]] = ..., workspace: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., matching_capabilities: _Optional[_Iterable[str]] = ...) -> None: ...

class AuthorityRequestOperation(_message.Message):
    __slots__ = ("op", "request_id", "create", "resolve", "list_filter", "client_request_id", "reason")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        AUTHORITY_REQUEST_OP_UNSPECIFIED: _ClassVar[AuthorityRequestOperation.OpType]
        CREATE: _ClassVar[AuthorityRequestOperation.OpType]
        GET: _ClassVar[AuthorityRequestOperation.OpType]
        LIST_PENDING: _ClassVar[AuthorityRequestOperation.OpType]
        RESOLVE: _ClassVar[AuthorityRequestOperation.OpType]
        CANCEL: _ClassVar[AuthorityRequestOperation.OpType]
    AUTHORITY_REQUEST_OP_UNSPECIFIED: AuthorityRequestOperation.OpType
    CREATE: AuthorityRequestOperation.OpType
    GET: AuthorityRequestOperation.OpType
    LIST_PENDING: AuthorityRequestOperation.OpType
    RESOLVE: AuthorityRequestOperation.OpType
    CANCEL: AuthorityRequestOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    RESOLVE_FIELD_NUMBER: _ClassVar[int]
    LIST_FILTER_FIELD_NUMBER: _ClassVar[int]
    CLIENT_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    op: AuthorityRequestOperation.OpType
    request_id: str
    create: CreateAuthorityRequestPayload
    resolve: ResolveAuthorityRequestPayload
    list_filter: AuthorityRequestListFilter
    client_request_id: str
    reason: str
    def __init__(self, op: _Optional[_Union[AuthorityRequestOperation.OpType, str]] = ..., request_id: _Optional[str] = ..., create: _Optional[_Union[CreateAuthorityRequestPayload, _Mapping]] = ..., resolve: _Optional[_Union[ResolveAuthorityRequestPayload, _Mapping]] = ..., list_filter: _Optional[_Union[AuthorityRequestListFilter, _Mapping]] = ..., client_request_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class AuthorityRequestOperationResponse(_message.Message):
    __slots__ = ("success", "error", "client_request_id", "request", "requests", "total_count")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CLIENT_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    client_request_id: str
    request: AuthorityRequest
    requests: _containers.RepeatedCompositeFieldContainer[AuthorityRequest]
    total_count: int
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., client_request_id: _Optional[str] = ..., request: _Optional[_Union[AuthorityRequest, _Mapping]] = ..., requests: _Optional[_Iterable[_Union[AuthorityRequest, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class AuthorityRequestEvent(_message.Message):
    __slots__ = ("event_type", "request", "emitted_at")
    class EventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        AUTHORITY_REQUEST_EVENT_UNSPECIFIED: _ClassVar[AuthorityRequestEvent.EventType]
        AUTHORITY_REQUEST_EVENT_CREATED: _ClassVar[AuthorityRequestEvent.EventType]
        AUTHORITY_REQUEST_EVENT_APPROVED: _ClassVar[AuthorityRequestEvent.EventType]
        AUTHORITY_REQUEST_EVENT_DENIED: _ClassVar[AuthorityRequestEvent.EventType]
        AUTHORITY_REQUEST_EVENT_EXPIRED: _ClassVar[AuthorityRequestEvent.EventType]
        AUTHORITY_REQUEST_EVENT_CANCELLED: _ClassVar[AuthorityRequestEvent.EventType]
    AUTHORITY_REQUEST_EVENT_UNSPECIFIED: AuthorityRequestEvent.EventType
    AUTHORITY_REQUEST_EVENT_CREATED: AuthorityRequestEvent.EventType
    AUTHORITY_REQUEST_EVENT_APPROVED: AuthorityRequestEvent.EventType
    AUTHORITY_REQUEST_EVENT_DENIED: AuthorityRequestEvent.EventType
    AUTHORITY_REQUEST_EVENT_EXPIRED: AuthorityRequestEvent.EventType
    AUTHORITY_REQUEST_EVENT_CANCELLED: AuthorityRequestEvent.EventType
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    EMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    event_type: AuthorityRequestEvent.EventType
    request: AuthorityRequest
    emitted_at: int
    def __init__(self, event_type: _Optional[_Union[AuthorityRequestEvent.EventType, str]] = ..., request: _Optional[_Union[AuthorityRequest, _Mapping]] = ..., emitted_at: _Optional[int] = ...) -> None: ...

class TokenOperation(_message.Message):
    __slots__ = ("op", "token_id", "create_request", "filter", "request_id")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        LIST: _ClassVar[TokenOperation.OpType]
        GET: _ClassVar[TokenOperation.OpType]
        CREATE: _ClassVar[TokenOperation.OpType]
        DELETE: _ClassVar[TokenOperation.OpType]
        REVOKE: _ClassVar[TokenOperation.OpType]
    LIST: TokenOperation.OpType
    GET: TokenOperation.OpType
    CREATE: TokenOperation.OpType
    DELETE: TokenOperation.OpType
    REVOKE: TokenOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    CREATE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    op: TokenOperation.OpType
    token_id: str
    create_request: TokenCreateRequest
    filter: TokenFilter
    request_id: str
    def __init__(self, op: _Optional[_Union[TokenOperation.OpType, str]] = ..., token_id: _Optional[str] = ..., create_request: _Optional[_Union[TokenCreateRequest, _Mapping]] = ..., filter: _Optional[_Union[TokenFilter, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class TokenCreateRequest(_message.Message):
    __slots__ = ("name", "principal_type", "workspace_patterns", "scopes", "expires_in_hours", "created_by")
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_PATTERNS_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_HOURS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    name: str
    principal_type: str
    workspace_patterns: _containers.RepeatedScalarFieldContainer[str]
    scopes: _containers.RepeatedScalarFieldContainer[str]
    expires_in_hours: int
    created_by: str
    def __init__(self, name: _Optional[str] = ..., principal_type: _Optional[str] = ..., workspace_patterns: _Optional[_Iterable[str]] = ..., scopes: _Optional[_Iterable[str]] = ..., expires_in_hours: _Optional[int] = ..., created_by: _Optional[str] = ...) -> None: ...

class TokenFilter(_message.Message):
    __slots__ = ("limit", "offset", "include_revoked")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_REVOKED_FIELD_NUMBER: _ClassVar[int]
    limit: int
    offset: int
    include_revoked: bool
    def __init__(self, limit: _Optional[int] = ..., offset: _Optional[int] = ..., include_revoked: bool = ...) -> None: ...

class TokenInfo(_message.Message):
    __slots__ = ("id", "name", "principal_type", "workspace_patterns", "scopes", "created_by", "expires_at", "last_used_at", "revoked", "revoked_at", "created_at", "updated_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_PATTERNS_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_USED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    principal_type: str
    workspace_patterns: _containers.RepeatedScalarFieldContainer[str]
    scopes: _containers.RepeatedScalarFieldContainer[str]
    created_by: str
    expires_at: int
    last_used_at: int
    revoked: bool
    revoked_at: int
    created_at: int
    updated_at: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., principal_type: _Optional[str] = ..., workspace_patterns: _Optional[_Iterable[str]] = ..., scopes: _Optional[_Iterable[str]] = ..., created_by: _Optional[str] = ..., expires_at: _Optional[int] = ..., last_used_at: _Optional[int] = ..., revoked: bool = ..., revoked_at: _Optional[int] = ..., created_at: _Optional[int] = ..., updated_at: _Optional[int] = ...) -> None: ...

class TokenResponse(_message.Message):
    __slots__ = ("success", "error", "message", "token", "tokens", "total_count", "plaintext_token", "created_token", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    PLAINTEXT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    CREATED_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    message: str
    token: TokenInfo
    tokens: _containers.RepeatedCompositeFieldContainer[TokenInfo]
    total_count: int
    plaintext_token: str
    created_token: TokenInfo
    request_id: str
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., message: _Optional[str] = ..., token: _Optional[_Union[TokenInfo, _Mapping]] = ..., tokens: _Optional[_Iterable[_Union[TokenInfo, _Mapping]]] = ..., total_count: _Optional[int] = ..., plaintext_token: _Optional[str] = ..., created_token: _Optional[_Union[TokenInfo, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ProgressReport(_message.Message):
    __slots__ = ("task_id", "state", "completion", "summary", "step", "recipient", "request_id", "metadata", "kind")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    state: str
    completion: float
    summary: str
    step: ProgressStep
    recipient: str
    request_id: str
    metadata: _containers.ScalarMap[str, str]
    kind: ProgressKind
    def __init__(self, task_id: _Optional[str] = ..., state: _Optional[str] = ..., completion: _Optional[float] = ..., summary: _Optional[str] = ..., step: _Optional[_Union[ProgressStep, _Mapping]] = ..., recipient: _Optional[str] = ..., request_id: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., kind: _Optional[_Union[ProgressKind, str]] = ...) -> None: ...

class ProgressStep(_message.Message):
    __slots__ = ("name", "detail", "sequence", "total_steps", "step_type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_STEPS_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    detail: str
    sequence: int
    total_steps: int
    step_type: str
    def __init__(self, name: _Optional[str] = ..., detail: _Optional[str] = ..., sequence: _Optional[int] = ..., total_steps: _Optional[int] = ..., step_type: _Optional[str] = ...) -> None: ...

class ProgressUpdate(_message.Message):
    __slots__ = ("source", "task_id", "state", "completion", "summary", "step", "timestamp_ms", "workspace", "request_id", "metadata", "recipient", "kind")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    source: str
    task_id: str
    state: str
    completion: float
    summary: str
    step: ProgressStep
    timestamp_ms: int
    workspace: str
    request_id: str
    metadata: _containers.ScalarMap[str, str]
    recipient: str
    kind: ProgressKind
    def __init__(self, source: _Optional[str] = ..., task_id: _Optional[str] = ..., state: _Optional[str] = ..., completion: _Optional[float] = ..., summary: _Optional[str] = ..., step: _Optional[_Union[ProgressStep, _Mapping]] = ..., timestamp_ms: _Optional[int] = ..., workspace: _Optional[str] = ..., request_id: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., recipient: _Optional[str] = ..., kind: _Optional[_Union[ProgressKind, str]] = ...) -> None: ...

class WorkflowOperation(_message.Message):
    __slots__ = ("op", "id", "secondary_id", "workspace", "data", "request_id", "status_filter")
    class OpType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        LIST_RULES: _ClassVar[WorkflowOperation.OpType]
        GET_RULE: _ClassVar[WorkflowOperation.OpType]
        CREATE_RULE: _ClassVar[WorkflowOperation.OpType]
        UPDATE_RULE: _ClassVar[WorkflowOperation.OpType]
        DELETE_RULE: _ClassVar[WorkflowOperation.OpType]
        LIST_WORKFLOWS: _ClassVar[WorkflowOperation.OpType]
        GET_WORKFLOW: _ClassVar[WorkflowOperation.OpType]
        CREATE_WORKFLOW: _ClassVar[WorkflowOperation.OpType]
        DELETE_WORKFLOW: _ClassVar[WorkflowOperation.OpType]
        LIST_SCHEDULES: _ClassVar[WorkflowOperation.OpType]
        CREATE_SCHEDULE: _ClassVar[WorkflowOperation.OpType]
        DELETE_SCHEDULE: _ClassVar[WorkflowOperation.OpType]
        LIST_EXECUTIONS: _ClassVar[WorkflowOperation.OpType]
        GET_EXECUTION: _ClassVar[WorkflowOperation.OpType]
        CANCEL_EXECUTION: _ClassVar[WorkflowOperation.OpType]
        LIST_STATE_MACHINES: _ClassVar[WorkflowOperation.OpType]
        GET_STATE_MACHINE: _ClassVar[WorkflowOperation.OpType]
        CREATE_STATE_MACHINE: _ClassVar[WorkflowOperation.OpType]
        DELETE_STATE_MACHINE: _ClassVar[WorkflowOperation.OpType]
        LIST_SM_INSTANCES: _ClassVar[WorkflowOperation.OpType]
        GET_SM_INSTANCE: _ClassVar[WorkflowOperation.OpType]
        CREATE_SM_INSTANCE: _ClassVar[WorkflowOperation.OpType]
        SEND_SM_EVENT: _ClassVar[WorkflowOperation.OpType]
        UPSERT_SCHEDULE: _ClassVar[WorkflowOperation.OpType]
    LIST_RULES: WorkflowOperation.OpType
    GET_RULE: WorkflowOperation.OpType
    CREATE_RULE: WorkflowOperation.OpType
    UPDATE_RULE: WorkflowOperation.OpType
    DELETE_RULE: WorkflowOperation.OpType
    LIST_WORKFLOWS: WorkflowOperation.OpType
    GET_WORKFLOW: WorkflowOperation.OpType
    CREATE_WORKFLOW: WorkflowOperation.OpType
    DELETE_WORKFLOW: WorkflowOperation.OpType
    LIST_SCHEDULES: WorkflowOperation.OpType
    CREATE_SCHEDULE: WorkflowOperation.OpType
    DELETE_SCHEDULE: WorkflowOperation.OpType
    LIST_EXECUTIONS: WorkflowOperation.OpType
    GET_EXECUTION: WorkflowOperation.OpType
    CANCEL_EXECUTION: WorkflowOperation.OpType
    LIST_STATE_MACHINES: WorkflowOperation.OpType
    GET_STATE_MACHINE: WorkflowOperation.OpType
    CREATE_STATE_MACHINE: WorkflowOperation.OpType
    DELETE_STATE_MACHINE: WorkflowOperation.OpType
    LIST_SM_INSTANCES: WorkflowOperation.OpType
    GET_SM_INSTANCE: WorkflowOperation.OpType
    CREATE_SM_INSTANCE: WorkflowOperation.OpType
    SEND_SM_EVENT: WorkflowOperation.OpType
    UPSERT_SCHEDULE: WorkflowOperation.OpType
    OP_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SECONDARY_ID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    op: WorkflowOperation.OpType
    id: str
    secondary_id: str
    workspace: str
    data: bytes
    request_id: str
    status_filter: str
    def __init__(self, op: _Optional[_Union[WorkflowOperation.OpType, str]] = ..., id: _Optional[str] = ..., secondary_id: _Optional[str] = ..., workspace: _Optional[str] = ..., data: _Optional[bytes] = ..., request_id: _Optional[str] = ..., status_filter: _Optional[str] = ...) -> None: ...

class WorkflowResponse(_message.Message):
    __slots__ = ("success", "error", "message", "data", "total_count", "request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: str
    message: str
    data: bytes
    total_count: int
    request_id: str
    def __init__(self, success: bool = ..., error: _Optional[str] = ..., message: _Optional[str] = ..., data: _Optional[bytes] = ..., total_count: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class MessageEnvelope(_message.Message):
    __slots__ = ("source", "payload", "message_type", "timestamp_ms", "metadata", "workspace")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    source: str
    payload: bytes
    message_type: MessageType
    timestamp_ms: int
    metadata: _containers.ScalarMap[str, str]
    workspace: str
    def __init__(self, source: _Optional[str] = ..., payload: _Optional[bytes] = ..., message_type: _Optional[_Union[MessageType, str]] = ..., timestamp_ms: _Optional[int] = ..., metadata: _Optional[_Mapping[str, str]] = ..., workspace: _Optional[str] = ...) -> None: ...

class AuditQuery(_message.Message):
    __slots__ = ("request_id", "start_time", "end_time", "event_type", "actor_type", "actor_id", "resource_type", "resource_id", "operation", "workspace", "only_failures", "limit", "offset", "subject_type", "subject_id", "authority_mode", "authority_grant_id", "authorization", "exclude_actor_types", "exclude_workspaces", "exclude_service_direct")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ACTOR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ACTOR_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    ONLY_FAILURES_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_MODE_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_ACTOR_TYPES_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_WORKSPACES_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_SERVICE_DIRECT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    start_time: int
    end_time: int
    event_type: str
    actor_type: str
    actor_id: str
    resource_type: str
    resource_id: str
    operation: str
    workspace: str
    only_failures: bool
    limit: int
    offset: int
    subject_type: str
    subject_id: str
    authority_mode: str
    authority_grant_id: str
    authorization: AuthorizationContext
    exclude_actor_types: _containers.RepeatedScalarFieldContainer[str]
    exclude_workspaces: _containers.RepeatedScalarFieldContainer[str]
    exclude_service_direct: bool
    def __init__(self, request_id: _Optional[str] = ..., start_time: _Optional[int] = ..., end_time: _Optional[int] = ..., event_type: _Optional[str] = ..., actor_type: _Optional[str] = ..., actor_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., operation: _Optional[str] = ..., workspace: _Optional[str] = ..., only_failures: bool = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., subject_type: _Optional[str] = ..., subject_id: _Optional[str] = ..., authority_mode: _Optional[str] = ..., authority_grant_id: _Optional[str] = ..., authorization: _Optional[_Union[AuthorizationContext, _Mapping]] = ..., exclude_actor_types: _Optional[_Iterable[str]] = ..., exclude_workspaces: _Optional[_Iterable[str]] = ..., exclude_service_direct: bool = ...) -> None: ...

class AuditQueryResponse(_message.Message):
    __slots__ = ("request_id", "success", "error", "entries", "total_count")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    success: bool
    error: str
    entries: _containers.RepeatedCompositeFieldContainer[AuditEntry]
    total_count: int
    def __init__(self, request_id: _Optional[str] = ..., success: bool = ..., error: _Optional[str] = ..., entries: _Optional[_Iterable[_Union[AuditEntry, _Mapping]]] = ..., total_count: _Optional[int] = ...) -> None: ...

class AuditEntry(_message.Message):
    __slots__ = ("audit_id", "timestamp", "event_type", "actor_type", "actor_id", "resource_type", "resource_id", "operation", "workspace", "session_id", "gateway_id", "success", "error_message", "metadata_json", "subject_type", "subject_id", "root_subject_type", "root_subject_id", "authority_mode", "root_authority_grant_id", "authority_grant_id", "parent_authority_grant_id", "source")
    AUDIT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ACTOR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ACTOR_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    GATEWAY_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_MODE_FIELD_NUMBER: _ClassVar[int]
    ROOT_AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_AUTHORITY_GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    audit_id: int
    timestamp: int
    event_type: str
    actor_type: str
    actor_id: str
    resource_type: str
    resource_id: str
    operation: str
    workspace: str
    session_id: str
    gateway_id: str
    success: bool
    error_message: str
    metadata_json: str
    subject_type: str
    subject_id: str
    root_subject_type: str
    root_subject_id: str
    authority_mode: str
    root_authority_grant_id: str
    authority_grant_id: str
    parent_authority_grant_id: str
    source: str
    def __init__(self, audit_id: _Optional[int] = ..., timestamp: _Optional[int] = ..., event_type: _Optional[str] = ..., actor_type: _Optional[str] = ..., actor_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., operation: _Optional[str] = ..., workspace: _Optional[str] = ..., session_id: _Optional[str] = ..., gateway_id: _Optional[str] = ..., success: bool = ..., error_message: _Optional[str] = ..., metadata_json: _Optional[str] = ..., subject_type: _Optional[str] = ..., subject_id: _Optional[str] = ..., root_subject_type: _Optional[str] = ..., root_subject_id: _Optional[str] = ..., authority_mode: _Optional[str] = ..., root_authority_grant_id: _Optional[str] = ..., authority_grant_id: _Optional[str] = ..., parent_authority_grant_id: _Optional[str] = ..., source: _Optional[str] = ...) -> None: ...

class SubmitAuditEventRequest(_message.Message):
    __slots__ = ("event_type", "operation", "resource_type", "resource_id", "workspace", "success", "error_message", "metadata", "client_request_id")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CLIENT_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    operation: str
    resource_type: str
    resource_id: str
    workspace: str
    success: bool
    error_message: str
    metadata: _containers.ScalarMap[str, str]
    client_request_id: str
    def __init__(self, event_type: _Optional[str] = ..., operation: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., workspace: _Optional[str] = ..., success: bool = ..., error_message: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., client_request_id: _Optional[str] = ...) -> None: ...

class SubmitAuditEventResponse(_message.Message):
    __slots__ = ("client_request_id", "success", "error_code", "error_message")
    CLIENT_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    client_request_id: str
    success: bool
    error_code: str
    error_message: str
    def __init__(self, client_request_id: _Optional[str] = ..., success: bool = ..., error_code: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class ProxyHttpRequest(_message.Message):
    __slots__ = ("request_id", "target_topic", "method", "path", "headers", "body", "body_chunked", "authorization", "app_workspace", "timeout_ms", "follow_redirects", "backend_name", "stream_response_indefinitely", "stream_idle_timeout_ms", "max_response_body_bytes", "proxy_chain_depth")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_TOPIC_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    BODY_CHUNKED_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_FIELD_NUMBER: _ClassVar[int]
    APP_WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_REDIRECTS_FIELD_NUMBER: _ClassVar[int]
    BACKEND_NAME_FIELD_NUMBER: _ClassVar[int]
    STREAM_RESPONSE_INDEFINITELY_FIELD_NUMBER: _ClassVar[int]
    STREAM_IDLE_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_RESPONSE_BODY_BYTES_FIELD_NUMBER: _ClassVar[int]
    PROXY_CHAIN_DEPTH_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    target_topic: str
    method: str
    path: str
    headers: _containers.ScalarMap[str, str]
    body: bytes
    body_chunked: bool
    authorization: AuthorizationContext
    app_workspace: str
    timeout_ms: int
    follow_redirects: bool
    backend_name: str
    stream_response_indefinitely: bool
    stream_idle_timeout_ms: int
    max_response_body_bytes: int
    proxy_chain_depth: int
    def __init__(self, request_id: _Optional[str] = ..., target_topic: _Optional[str] = ..., method: _Optional[str] = ..., path: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ..., body: _Optional[bytes] = ..., body_chunked: bool = ..., authorization: _Optional[_Union[AuthorizationContext, _Mapping]] = ..., app_workspace: _Optional[str] = ..., timeout_ms: _Optional[int] = ..., follow_redirects: bool = ..., backend_name: _Optional[str] = ..., stream_response_indefinitely: bool = ..., stream_idle_timeout_ms: _Optional[int] = ..., max_response_body_bytes: _Optional[int] = ..., proxy_chain_depth: _Optional[int] = ...) -> None: ...

class ProxyHttpResponse(_message.Message):
    __slots__ = ("request_id", "status_code", "headers", "body", "body_chunked", "error")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    BODY_CHUNKED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    status_code: int
    headers: _containers.ScalarMap[str, str]
    body: bytes
    body_chunked: bool
    error: ProxyError
    def __init__(self, request_id: _Optional[str] = ..., status_code: _Optional[int] = ..., headers: _Optional[_Mapping[str, str]] = ..., body: _Optional[bytes] = ..., body_chunked: bool = ..., error: _Optional[_Union[ProxyError, _Mapping]] = ...) -> None: ...

class ProxyHttpBodyChunk(_message.Message):
    __slots__ = ("request_id", "is_request", "seq", "data", "fin")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    IS_REQUEST_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    FIN_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    is_request: bool
    seq: int
    data: bytes
    fin: bool
    def __init__(self, request_id: _Optional[str] = ..., is_request: bool = ..., seq: _Optional[int] = ..., data: _Optional[bytes] = ..., fin: bool = ...) -> None: ...

class ProxyError(_message.Message):
    __slots__ = ("kind", "message")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[ProxyError.Kind]
        DIAL_FAILED: _ClassVar[ProxyError.Kind]
        TIMEOUT: _ClassVar[ProxyError.Kind]
        UPSTREAM_RESET: _ClassVar[ProxyError.Kind]
        ACL_DENIED: _ClassVar[ProxyError.Kind]
        SIDECAR_UNAVAILABLE: _ClassVar[ProxyError.Kind]
        PAYLOAD_TOO_LARGE: _ClassVar[ProxyError.Kind]
        DECODE_FAILED: _ClassVar[ProxyError.Kind]
    UNKNOWN: ProxyError.Kind
    DIAL_FAILED: ProxyError.Kind
    TIMEOUT: ProxyError.Kind
    UPSTREAM_RESET: ProxyError.Kind
    ACL_DENIED: ProxyError.Kind
    SIDECAR_UNAVAILABLE: ProxyError.Kind
    PAYLOAD_TOO_LARGE: ProxyError.Kind
    DECODE_FAILED: ProxyError.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    kind: ProxyError.Kind
    message: str
    def __init__(self, kind: _Optional[_Union[ProxyError.Kind, str]] = ..., message: _Optional[str] = ...) -> None: ...

class TunnelOpen(_message.Message):
    __slots__ = ("tunnel_id", "target_topic", "protocol", "remote_hint", "metadata", "authorization", "idle_timeout_ms", "max_bytes", "session_token", "backend_name", "proxy_chain_depth")
    class Protocol(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TCP: _ClassVar[TunnelOpen.Protocol]
        UDP: _ClassVar[TunnelOpen.Protocol]
        WEBSOCKET: _ClassVar[TunnelOpen.Protocol]
    TCP: TunnelOpen.Protocol
    UDP: TunnelOpen.Protocol
    WEBSOCKET: TunnelOpen.Protocol
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TUNNEL_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_TOPIC_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    REMOTE_HINT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZATION_FIELD_NUMBER: _ClassVar[int]
    IDLE_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_BYTES_FIELD_NUMBER: _ClassVar[int]
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    BACKEND_NAME_FIELD_NUMBER: _ClassVar[int]
    PROXY_CHAIN_DEPTH_FIELD_NUMBER: _ClassVar[int]
    tunnel_id: str
    target_topic: str
    protocol: TunnelOpen.Protocol
    remote_hint: str
    metadata: _containers.ScalarMap[str, str]
    authorization: AuthorizationContext
    idle_timeout_ms: int
    max_bytes: int
    session_token: str
    backend_name: str
    proxy_chain_depth: int
    def __init__(self, tunnel_id: _Optional[str] = ..., target_topic: _Optional[str] = ..., protocol: _Optional[_Union[TunnelOpen.Protocol, str]] = ..., remote_hint: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ..., authorization: _Optional[_Union[AuthorizationContext, _Mapping]] = ..., idle_timeout_ms: _Optional[int] = ..., max_bytes: _Optional[int] = ..., session_token: _Optional[str] = ..., backend_name: _Optional[str] = ..., proxy_chain_depth: _Optional[int] = ...) -> None: ...

class TunnelData(_message.Message):
    __slots__ = ("tunnel_id", "seq", "data", "fin")
    TUNNEL_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    FIN_FIELD_NUMBER: _ClassVar[int]
    tunnel_id: str
    seq: int
    data: bytes
    fin: bool
    def __init__(self, tunnel_id: _Optional[str] = ..., seq: _Optional[int] = ..., data: _Optional[bytes] = ..., fin: bool = ...) -> None: ...

class TunnelClose(_message.Message):
    __slots__ = ("tunnel_id", "reason", "detail")
    class Reason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NORMAL: _ClassVar[TunnelClose.Reason]
        PEER_RESET: _ClassVar[TunnelClose.Reason]
        IDLE_TIMEOUT: _ClassVar[TunnelClose.Reason]
        QUOTA: _ClassVar[TunnelClose.Reason]
        ERROR: _ClassVar[TunnelClose.Reason]
    NORMAL: TunnelClose.Reason
    PEER_RESET: TunnelClose.Reason
    IDLE_TIMEOUT: TunnelClose.Reason
    QUOTA: TunnelClose.Reason
    ERROR: TunnelClose.Reason
    TUNNEL_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    tunnel_id: str
    reason: TunnelClose.Reason
    detail: str
    def __init__(self, tunnel_id: _Optional[str] = ..., reason: _Optional[_Union[TunnelClose.Reason, str]] = ..., detail: _Optional[str] = ...) -> None: ...

class TunnelAck(_message.Message):
    __slots__ = ("tunnel_id", "ack_seq", "credits")
    TUNNEL_ID_FIELD_NUMBER: _ClassVar[int]
    ACK_SEQ_FIELD_NUMBER: _ClassVar[int]
    CREDITS_FIELD_NUMBER: _ClassVar[int]
    tunnel_id: str
    ack_seq: int
    credits: int
    def __init__(self, tunnel_id: _Optional[str] = ..., ack_seq: _Optional[int] = ..., credits: _Optional[int] = ...) -> None: ...

class ResolveAuthorityRequest(_message.Message):
    __slots__ = ("request_id", "actor", "grant_id", "subject", "audience_type", "audience_id")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    actor: PrincipalRef
    grant_id: str
    subject: PrincipalRef
    audience_type: str
    audience_id: str
    def __init__(self, request_id: _Optional[str] = ..., actor: _Optional[_Union[PrincipalRef, _Mapping]] = ..., grant_id: _Optional[str] = ..., subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ...) -> None: ...

class ResolveAuthorityResponse(_message.Message):
    __slots__ = ("request_id", "ok", "error", "authority")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    ok: bool
    error: str
    authority: ResolvedAuthority
    def __init__(self, request_id: _Optional[str] = ..., ok: bool = ..., error: _Optional[str] = ..., authority: _Optional[_Union[ResolvedAuthority, _Mapping]] = ...) -> None: ...

class ResolvedAuthority(_message.Message):
    __slots__ = ("actor", "subject", "grant")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    GRANT_FIELD_NUMBER: _ClassVar[int]
    actor: PrincipalRef
    subject: PrincipalRef
    grant: AuthorityGrantInfo
    def __init__(self, actor: _Optional[_Union[PrincipalRef, _Mapping]] = ..., subject: _Optional[_Union[PrincipalRef, _Mapping]] = ..., grant: _Optional[_Union[AuthorityGrantInfo, _Mapping]] = ...) -> None: ...

class AuthorityGrantInfo(_message.Message):
    __slots__ = ("grant_id", "subject_type", "subject_id", "root_subject_type", "root_subject_id", "audience_type", "audience_id", "max_access_level", "workspace_scope", "expires_at", "revoked")
    GRANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROOT_SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_ACCESS_LEVEL_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    grant_id: str
    subject_type: str
    subject_id: str
    root_subject_type: str
    root_subject_id: str
    audience_type: str
    audience_id: str
    max_access_level: int
    workspace_scope: _containers.RepeatedScalarFieldContainer[str]
    expires_at: int
    revoked: bool
    def __init__(self, grant_id: _Optional[str] = ..., subject_type: _Optional[str] = ..., subject_id: _Optional[str] = ..., root_subject_type: _Optional[str] = ..., root_subject_id: _Optional[str] = ..., audience_type: _Optional[str] = ..., audience_id: _Optional[str] = ..., max_access_level: _Optional[int] = ..., workspace_scope: _Optional[_Iterable[str]] = ..., expires_at: _Optional[int] = ..., revoked: bool = ...) -> None: ...

class ConnectionStatusRequest(_message.Message):
    __slots__ = ("request_id", "principal")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    principal: PrincipalRef
    def __init__(self, request_id: _Optional[str] = ..., principal: _Optional[_Union[PrincipalRef, _Mapping]] = ...) -> None: ...

class ConnectionStatusResponse(_message.Message):
    __slots__ = ("request_id", "ok", "error", "connected", "last_seen_at")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_AT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    ok: bool
    error: str
    connected: bool
    last_seen_at: int
    def __init__(self, request_id: _Optional[str] = ..., ok: bool = ..., error: _Optional[str] = ..., connected: bool = ..., last_seen_at: _Optional[int] = ...) -> None: ...
