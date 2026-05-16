import logging
import queue
import random
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional, Dict, List, Union

import grpc

logger = logging.getLogger("aether.client")

from ._common import (
    create_agent_init,
    create_task_init,
    create_user_init,
    create_orchestrator_init,
    create_workflow_engine_init,
    create_metrics_bridge_init,
    create_service_init,
    create_topic_agent,
    create_topic_task,
    create_topic_user,
    create_topic_global_agents,
    create_topic_global_users,
    create_topic_user_workspace,
    SELF_ASSIGN,
    TARGETED,
    POOL,
    _scope_to_proto,
    _env_tls_kwargs_filter,
)
from .exceptions import (
    AetherError,
    ConnectionError,
    ReconnectionError,
    InvalidArgumentError,
    from_grpc_error,
    is_recoverable_error,
)
from .proto import aether_pb2
from .proto import aether_pb2_grpc


def make_wait_spec(reason: int,
                   *,
                   expected_principal: str = "",
                   input_match: Optional[Dict[str, str]] = None,
                   authority_request_id: str = "",
                   depends_on: Optional[List[str]] = None,
                   wake_on_any: bool = False,
                   timeout_ms: int = 0,
                   scheduled_wake_unix_ms: int = 0) -> aether_pb2.WaitSpec:
    """
    Build an ``aether_pb2.WaitSpec`` using field-name kwargs instead of
    positional proto construction.

    The ``reason`` enum value (one of ``aether_pb2.WAIT_REASON_INPUT``,
    ``WAIT_REASON_AUTHORITY``, ``WAIT_REASON_DEPENDENCY``, or
    ``WAIT_REASON_HIBERNATION``) selects which other fields are meaningful —
    see the WaitSpec proto comments for the full field-reason matrix.

    This helper is part of the public API.  Callers can import it directly::

        from scitrera_aether_client.client import make_wait_spec

    Args:
        reason: ``aether_pb2.WaitReason`` enum value (required).
        expected_principal: For INPUT waits — canonical principal string of
            the actor whose message will satisfy the wait condition.
        input_match: For INPUT waits — key/value pairs that must match the
            incoming message metadata.
        authority_request_id: For AUTHORITY waits — the id of the pending
            authority request being tracked.
        depends_on: For DEPENDENCY waits — list of task IDs to wait on.
        wake_on_any: For DEPENDENCY waits — if True, wake when *any* listed
            task terminates; if False (default), wait for *all*.
        timeout_ms: Maximum wait duration in milliseconds (0 = no timeout).
        scheduled_wake_unix_ms: Unix timestamp (ms) at which the task waker
            will force-wake regardless of other conditions (0 = disabled).

    Returns:
        Populated ``aether_pb2.WaitSpec`` instance.
    """
    return aether_pb2.WaitSpec(
        reason=reason,  # type: ignore[arg-type]
        expected_principal=expected_principal,
        input_match=input_match or {},
        authority_request_id=authority_request_id,
        depends_on=depends_on or [],
        wake_on_any=wake_on_any,
        timeout_ms=timeout_ms,
        scheduled_wake_unix_ms=scheduled_wake_unix_ms,
    )


def _principal_ref(principal_type: str, principal_id: str) -> aether_pb2.PrincipalRef:
    return aether_pb2.PrincipalRef(
        principal_type=principal_type,
        principal_id=principal_id,
    )


def _authority_resource_scope_entries(resource_scope: Optional[Dict[str, List[str]]]) -> List[aether_pb2.ACLAuthorityGrantResourceScopeEntry]:
    if not resource_scope:
        return []

    return [
        aether_pb2.ACLAuthorityGrantResourceScopeEntry(
            resource_type=resource_type,
            patterns=patterns,
        )
        for resource_type, patterns in resource_scope.items()
    ]


def make_hibernation_descriptor(*,
                               checkpoint_key: str,
                               resume_session_id: str = "",
                               wake_event_types: Optional[List[str]] = None,
                               escalation_policy: str = "") -> aether_pb2.HibernationDescriptor:
    """
    Build an ``aether_pb2.HibernationDescriptor``.

    Args:
        checkpoint_key: Required. The checkpoint key the worker SAVE'd before
            hibernating; the gateway validates its existence.
        resume_session_id: Optional. Session id to resume on wake; empty = fresh.
        wake_event_types: Optional. Reserved for future use.
        escalation_policy: ``""`` / ``"fail"`` (default) — fail the task on
            timeout. ``"retry"`` — re-queue for fresh worker spawn.
            ``"alert"`` — stay hibernated, log warning.

    Returns:
        Populated ``aether_pb2.HibernationDescriptor`` instance.

    Raises:
        ValueError: if ``checkpoint_key`` is empty.
    """
    if not checkpoint_key:
        raise ValueError("make_hibernation_descriptor: checkpoint_key is required")
    return aether_pb2.HibernationDescriptor(
        checkpoint_key=checkpoint_key,
        resume_session_id=resume_session_id,
        wake_event_types=wake_event_types or [],
        escalation_policy=escalation_policy,
    )


def make_authority_request_routing(*,
                                   principal: Optional[aether_pb2.PrincipalRef] = None,
                                   capability: str = "") -> aether_pb2.AuthorityRequestRoutingTarget:
    """
    Build an ``aether_pb2.AuthorityRequestRoutingTarget``. Exactly one of
    ``principal`` or ``capability`` must be provided; supplying both or
    neither raises :class:`ValueError`.

    A capability routing target accepts any actor whose ACL ``CheckAccess``
    against the supplied gate succeeds. A principal routing target restricts
    resolution to that single approver.

    Args:
        principal: Specific approver / role / group identity.
        capability: Capability-gate string ("capability/approve/<action>").

    Returns:
        Populated ``AuthorityRequestRoutingTarget``.

    Raises:
        ValueError: If both ``principal`` and ``capability`` are set, or
            both are empty.
    """
    capability = (capability or "").strip()
    has_principal = principal is not None and (principal.principal_id or principal.principal_type)
    has_capability = bool(capability)
    if has_principal and has_capability:
        raise ValueError(
            "make_authority_request_routing: exactly one of principal or capability must be set"
        )
    if not has_principal and not has_capability:
        raise ValueError(
            "make_authority_request_routing: principal or capability is required"
        )
    target = aether_pb2.AuthorityRequestRoutingTarget()
    if has_principal:
        target.principal.CopyFrom(principal)
    else:
        target.capability = capability
    return target


def make_authority_request_resource_scope_entry(resource_type: str,
                                                patterns: List[str]) -> aether_pb2.AuthorityRequestResourceScopeEntry:
    """
    Build an ``aether_pb2.AuthorityRequestResourceScopeEntry`` from a
    resource-type string + a list of glob patterns. Convenience wrapper that
    matches the shape used inside :class:`BaseAetherClient.request_authority`.
    """
    return aether_pb2.AuthorityRequestResourceScopeEntry(
        resource_type=resource_type,
        patterns=list(patterns or []),
    )


def make_resource_schema_entry(*,
                               resource_type_prefix: str,
                               permission_verbs: Optional[List[str]] = None,
                               resource_id_schema: str = "") -> aether_pb2.AgentResourceSchemaEntry:
    """
    Build an :class:`aether_pb2.AgentResourceSchemaEntry` for use in
    :meth:`BaseAetherClient.register_agent` / :meth:`BaseAetherClient.update_agent`.

    Phase 5 (Stage B) introduces resource-schema declarations on agent
    registrations. Each entry declares one resource family the agent owns:
    the gateway uses ``resource_type_prefix`` to enforce uniqueness across
    registrations and to attribute ACL audit events to the owning agent.

    Args:
        resource_type_prefix: The resource-type prefix this agent owns
            (e.g. ``"chat/"`` or ``"docmgmt/document"``). Required and
            non-empty; uniqueness is enforced by the gateway across active
            registrations.
        permission_verbs: Optional list of allowed verbs for this resource
            family (e.g. ``["read", "write"]``). Informational — used by
            tooling and the AgentCard generator, not by ACL enforcement.
        resource_id_schema: Optional JSON Schema fragment (as a string)
            describing the resource_id shape under this prefix.

    Returns:
        Populated :class:`aether_pb2.AgentResourceSchemaEntry`.

    Raises:
        ValueError: if ``resource_type_prefix`` is empty.
    """
    if not resource_type_prefix:
        raise ValueError(
            "make_resource_schema_entry: resource_type_prefix is required"
        )
    return aether_pb2.AgentResourceSchemaEntry(
        resource_type_prefix=resource_type_prefix,
        permission_verbs=list(permission_verbs or []),
        resource_id_schema=resource_id_schema,
    )


def make_extension(*,
                   uri: str,
                   version: str = "",
                   required: bool = False,
                   json_schema: str = "") -> aether_pb2.ExtensionDeclaration:
    """
    Build an :class:`aether_pb2.ExtensionDeclaration` for use in connect-time
    extension negotiation (Phase 6).

    Pass the result list via the ``extensions`` keyword to
    :func:`create_agent_init` / :func:`create_user_init` / etc., or to the
    ``extensions`` parameter on :meth:`AgentClient.connect` and the parallel
    methods on the other client classes.

    Args:
        uri: Globally unique extension URI (recommended ``"https://..."``
            style). Required.
        version: Optional version string. Empty = any version.
        required: When ``True``, the gateway MUST reject the connection if
            it does not support this extension. When ``False`` (default),
            unsupported extensions are reported in
            :class:`aether_pb2.ConnectionAck.negotiated_extensions` but do
            not block the connection.
        json_schema: Optional JSON Schema (as a string) describing the
            shape of extension-specific data. Informational only.

    Returns:
        Populated :class:`aether_pb2.ExtensionDeclaration`.

    Raises:
        ValueError: if ``uri`` is empty.
    """
    if not uri:
        raise ValueError("make_extension: uri is required")
    return aether_pb2.ExtensionDeclaration(
        uri=uri,
        version=version,
        required=required,
        json_schema=json_schema,
    )


@dataclass
class AuditSubmitResponse:
    """Response from ``submit_audit_event`` / ``submit_audit_event_async``.

    Mirrors :class:`aether_pb2.SubmitAuditEventResponse`. ``success`` reports
    whether the gateway accepted the event into the asynchronous audit queue
    (acceptance is not a persistence guarantee — drop-on-overflow still
    applies). On rejection, ``error_code`` and ``error_message`` carry the
    gateway's reason (e.g. ``ERR_AUDIT_TYPE_FORBIDDEN``,
    ``ERR_AUDIT_RATE_LIMITED``, ``ERR_PERMISSION_DENIED``).
    """
    success: bool
    error_code: str = ""
    error_message: str = ""
    client_request_id: str = ""


class BaseAetherClient:
    """Base class for all Aether client types."""

    def __init__(self,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 backoff_multiplier: float = 2.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
        """
        Initialize the base client.

        Args:
            max_retries: Maximum number of connection attempts (0 = infinite for reconnect)
            initial_backoff: Initial backoff delay in seconds
            max_backoff: Maximum backoff delay in seconds
            backoff_multiplier: Multiplier for exponential backoff
            auto_reconnect: Whether to automatically reconnect on connection loss
            tls_enabled: Whether to use TLS for the connection
            tls_root_cert: Root CA certificate bytes for server verification (optional, uses system CA if not provided)
            tls_root_cert_path: Path to root CA certificate file (alternative to tls_root_cert)
            tls_client_cert: Client certificate bytes for mTLS (optional)
            tls_client_cert_path: Path to client certificate file (alternative to tls_client_cert)
            tls_client_key: Client private key bytes for mTLS (optional)
            tls_client_key_path: Path to client private key file (alternative to tls_client_key)
        """
        self.target: Optional[str] = None
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[aether_pb2_grpc.AetherGatewayStub] = None
        self.request_queue: queue.Queue[Any] = queue.Queue()

        # TLS configuration
        self.tls_enabled = tls_enabled
        self._tls_root_cert = tls_root_cert
        self._tls_root_cert_path = tls_root_cert_path
        self._tls_client_cert = tls_client_cert
        self._tls_client_cert_path = tls_client_cert_path
        self._tls_client_key = tls_client_key
        self._tls_client_key_path = tls_client_key_path

        # Retry configuration
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        self.auto_reconnect = auto_reconnect

        # Message callbacks
        self.on_message: Optional[Callable[[aether_pb2.IncomingMessage], None]] = None
        self.on_config: Optional[Callable[[aether_pb2.ConfigSnapshot], None]] = None
        self.on_signal: Optional[Callable[[aether_pb2.Signal], None]] = None
        self.on_error: Optional[Callable[[aether_pb2.ErrorResponse], None]] = None
        self.on_kv_response: Optional[Callable[[aether_pb2.KVResponse], None]] = None
        self.on_task_assignment: Optional[Callable[[aether_pb2.TaskAssignment], None]] = None
        self.on_checkpoint_response: Optional[Callable[[aether_pb2.CheckpointResponse], None]] = None
        self.on_progress: Optional[Callable[[aether_pb2.ProgressUpdate], None]] = None
        self.on_connect: Optional[Callable[[], None]] = None
        self.on_disconnect: Optional[Callable[[str], None]] = None
        # Phase 4 (Stage C): task subscription event handler.
        # Set directly or via register_task_event_handler().
        self.on_task_event: Optional[Callable[[aether_pb2.TaskEvent], None]] = None

        # Typed message callbacks (SDK routes by message_type)
        self.on_chat_message: Optional[Callable[[aether_pb2.IncomingMessage], None]] = None
        self.on_control_message: Optional[Callable[[aether_pb2.IncomingMessage], None]] = None
        self.on_tool_call: Optional[Callable[[aether_pb2.IncomingMessage], None]] = None
        self.on_event: Optional[Callable[[aether_pb2.IncomingMessage], None]] = None
        self.on_metric: Optional[Callable[[aether_pb2.IncomingMessage], None]] = None

        self._stop_event = threading.Event()
        self._stream_thread: Optional[threading.Thread] = None
        self._force_disconnect = False  # Set when FORCE_DISCONNECT received
        self._reconnecting = False  # Set during reconnection attempts
        self._reconnect_attempt = 0  # Current reconnection attempt number
        self._connection_confirmed = False  # Set when server responds
        self._session_id: Optional[str] = None  # Session ID from server for reconnection

        # Phase 6: extension negotiation state populated when the server's
        # ConnectionAck arrives. ``_negotiated_extensions`` keeps the URIs
        # the server confirmed support for; ``_server_supported_extensions``
        # lists native extensions the server offered that the client did
        # NOT declare (discovery hint). Empty until connect() succeeds.
        self._negotiated_extensions: List[str] = []
        self._server_supported_extensions: List[str] = []

        # Unified pending request registry: request_id -> Queue
        # Used for all response types that support request_id correlation
        self._pending_requests: Dict[str, queue.Queue] = {}
        self._pending_requests_lock = threading.Lock()

        # Fallback queues for async callback usage (when no request_id match)
        self._kv_response_queue: queue.Queue[aether_pb2.KVResponse] = queue.Queue()
        self._checkpoint_response_queue: queue.Queue[aether_pb2.CheckpointResponse] = queue.Queue()
        self._task_query_response_queue: queue.Queue = queue.Queue()
        self._task_op_response_queue: queue.Queue = queue.Queue()
        self._workspace_response_queue: queue.Queue = queue.Queue()
        self._agent_response_queue: queue.Queue = queue.Queue()
        self._acl_response_queue: queue.Queue = queue.Queue()
        self._workflow_response_queue: queue.Queue = queue.Queue()
        self._authority_grant_response_queue: queue.Queue = queue.Queue()

        # Per-operation-type locks for ops without request_id (workspace, agent, ACL)
        self._workspace_op_lock = threading.Lock()
        self._agent_op_lock = threading.Lock()
        self._acl_op_lock = threading.Lock()

        # Workspace/agent/ACL/workflow response callbacks
        self.on_workspace_response: Optional[Callable] = None
        self.on_agent_response: Optional[Callable] = None
        self.on_acl_response: Optional[Callable] = None
        self.on_workflow_response: Optional[Callable] = None
        self.on_token_response: Optional[Callable] = None
        self.on_authority_grant_response: Optional[Callable] = None

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set() or self._reconnecting

    def negotiated_extensions(self) -> List[str]:
        """
        Return the list of extension URIs the server confirmed support for
        at connection time (Phase 6).

        Populated when the server's :class:`aether_pb2.ConnectionAck`
        message arrives. Empty until :meth:`connect` has succeeded, and
        empty when the client declared no extensions.
        """
        return list(self._negotiated_extensions)

    def server_supported_extensions(self) -> List[str]:
        """
        Return the list of extensions the server natively supports that
        the client did NOT declare. Useful for discovering what optional
        extensions are available. Empty until connect() succeeds.
        """
        return list(self._server_supported_extensions)

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate backoff delay with jitter.

        First attempt is zero-delay (see async client for rationale).
        """
        if attempt == 0:
            return 0.0
        delay = min(self.initial_backoff * (self.backoff_multiplier ** attempt), self.max_backoff)
        # Add jitter (±25%)
        jitter = delay * 0.25 * (random.random() * 2 - 1)
        return delay + jitter

    def _is_recoverable_error(self, error: Union[grpc.RpcError, AetherError]) -> bool:
        """Check if an error is recoverable (should trigger reconnection)."""
        return is_recoverable_error(error)

    def _on_error(self, error: Union[grpc.RpcError, aether_pb2.ErrorResponse, AetherError],
                  from_listen_loop: bool = False) -> Optional[AetherError]:
        """
        Handle errors and optionally convert to AetherError.

        Args:
            error: The error to handle (gRPC error, protobuf ErrorResponse, or AetherError)
            from_listen_loop: Whether this error came from the listen loop

        Returns:
            The converted AetherError if applicable, for potential re-raising
        """
        aether_error: Optional[AetherError] = None

        if isinstance(error, grpc.RpcError):
            # Convert gRPC error to appropriate AetherError
            aether_error = from_grpc_error(error)

            # Handle non-recoverable errors
            if not is_recoverable_error(error):
                logger.error("Non-recoverable error: %s", aether_error)
                self._stop_event.set()
                return aether_error

            # For recoverable errors from listen loop, don't stop - let reconnection happen
            if from_listen_loop and self.auto_reconnect and not self._force_disconnect:
                logger.warning("Connection error (will reconnect): %s", aether_error)
                return aether_error

            # Convert to ErrorResponse for backward compatibility with user handler
            error = aether_pb2.ErrorResponse(code=aether_error.code or "", message=aether_error.message)

        elif isinstance(error, AetherError):
            aether_error = error
            error = aether_pb2.ErrorResponse(code=aether_error.code or "", message=aether_error.message)

        if self.on_error:
            self.on_error(error)
        else:
            logger.error("Error: %s - %s", error.code, error.message)

        return aether_error

    def _request_generator(self):
        while not self._stop_event.is_set():
            try:
                request = self.request_queue.get(timeout=0.1)
                if request is None:  # Sentinel for closing
                    break
                yield request
            except queue.Empty:
                continue

    def _listen_loop(self, responses):
        """Listen for responses from the server."""
        should_reconnect = False
        try:
            for response in responses:
                if self._stop_event.is_set():
                    break

                # Connection confirmed when we receive first response from server
                if not self._connection_confirmed:
                    self._connection_confirmed = True
                    if self._reconnecting:
                        logger.info("Reconnected successfully")
                    self._reconnect_attempt = 0  # Reset attempt counter on successful connection
                    if self.on_connect:
                        self.on_connect()

                payload_type = response.WhichOneof("payload")
                if payload_type == "msg":
                    msg = response.msg

                    # Route to typed callback if registered
                    if msg.message_type == aether_pb2.CHAT and self.on_chat_message:
                        self.on_chat_message(msg)
                    elif msg.message_type == aether_pb2.CONTROL and self.on_control_message:
                        self.on_control_message(msg)
                    elif msg.message_type == aether_pb2.TOOL_CALL and self.on_tool_call:
                        self.on_tool_call(msg)
                    elif msg.message_type == aether_pb2.EVENT and self.on_event:
                        self.on_event(msg)
                    elif msg.message_type == aether_pb2.METRIC and self.on_metric:
                        self.on_metric(msg)

                    # Always call catch-all if registered
                    if self.on_message:
                        self.on_message(msg)
                elif payload_type == "config":
                    if self.on_config:
                        self.on_config(response.config)
                elif payload_type == "signal":
                    # Handle FORCE_DISCONNECT signal by closing the connection
                    if response.signal.type == aether_pb2.Signal.FORCE_DISCONNECT:
                        logger.warning("Received FORCE_DISCONNECT: %s", response.signal.reason)
                        self._force_disconnect = True
                        self._stop_event.set()
                        if self.on_disconnect:
                            self.on_disconnect(response.signal.reason)
                        break
                    elif response.signal.type == aether_pb2.Signal.GRACEFUL_DISCONNECT:
                        logger.info("Received GRACEFUL_DISCONNECT: %s", response.signal.reason)
                        # Do not set _force_disconnect — allow auto-reconnect
                        self._stop_event.set()
                        if self.on_disconnect:
                            self.on_disconnect(response.signal.reason)
                        break
                    if self.on_signal:
                        self.on_signal(response.signal)
                elif payload_type == "error":
                    err = response.error
                    req_id = err.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending is not None:
                        from scitrera_aether_client.exceptions import error_response_to_aether_error
                        pending.put_nowait(error_response_to_aether_error(err))
                    else:
                        self._on_error(err)
                elif payload_type == "kv":
                    resp = response.kv
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                    else:
                        self._kv_response_queue.put(resp)
                        if self.on_kv_response:
                            self.on_kv_response(resp)
                elif payload_type == "task_assignment":
                    if self.on_task_assignment:
                        self.on_task_assignment(response.task_assignment)
                elif payload_type == "checkpoint":
                    resp = response.checkpoint
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                    else:
                        self._checkpoint_response_queue.put(resp)
                        if self.on_checkpoint_response:
                            self.on_checkpoint_response(resp)
                elif payload_type == "task_query":
                    resp = response.task_query
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                    else:
                        self._task_query_response_queue.put(resp)
                elif payload_type == "task_op":
                    resp = response.task_op
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                    else:
                        self._task_op_response_queue.put(resp)
                elif payload_type == "workspace":
                    self._workspace_response_queue.put(response.workspace)
                    if self.on_workspace_response:
                        self.on_workspace_response(response.workspace)
                elif payload_type == "agent":
                    self._agent_response_queue.put(response.agent)
                    if self.on_agent_response:
                        self.on_agent_response(response.agent)
                elif payload_type == "acl":
                    self._acl_response_queue.put(response.acl)
                    if self.on_acl_response:
                        self.on_acl_response(response.acl)
                elif payload_type == "workflow_response":
                    resp = response.workflow_response
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                    else:
                        self._workflow_response_queue.put(resp)
                        if self.on_workflow_response:
                            self.on_workflow_response(resp)
                elif payload_type == "token":
                    resp = response.token
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                    elif self.on_token_response:
                        self.on_token_response(resp)
                elif payload_type == "authority_grant":
                    resp = response.authority_grant
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                    else:
                        self._authority_grant_response_queue.put(resp)
                        if self.on_authority_grant_response:
                            self.on_authority_grant_response(resp)
                elif payload_type == "create_task":
                    resp = response.create_task
                    req_id = resp.request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                elif payload_type == "submit_audit_event_response":
                    resp = response.submit_audit_event_response
                    req_id = resp.client_request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                elif payload_type == "progress_update":
                    if self.on_progress:
                        self.on_progress(response.progress_update)
                elif payload_type == "task_subscription_response":
                    # Phase 4 (Stage C): correlated response to subscribe/unsubscribe ops.
                    resp = response.task_subscription_response
                    req_id = resp.client_request_id
                    with self._pending_requests_lock:
                        pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending:
                        pending.put(resp)
                elif payload_type == "task_event":
                    # Phase 4 (Stage C): push event from a task subscription stream.
                    if self.on_task_event:
                        self.on_task_event(response.task_event)
                elif payload_type == "connection_ack":
                    # Store session ID for reconnection
                    ack = response.connection_ack
                    self._session_id = ack.session_id
                    if ack.resumed:
                        logger.info("Session resumed (session_id=%s...)", self._session_id[:8])
                    # Phase 6: capture extension negotiation outcome so
                    # callers can introspect via negotiated_extensions().
                    self._negotiated_extensions = [
                        nx.uri for nx in ack.negotiated_extensions if nx.supported
                    ]
                    self._server_supported_extensions = list(
                        ack.server_supported_extensions
                    )
        except grpc.RpcError as e:
            if not self._stop_event.is_set():
                self._on_error(e, from_listen_loop=True)
                # Check if we should reconnect. Signal intent via the local
                # ``should_reconnect`` flag only — ``_attempt_reconnect`` sets
                # ``self._reconnecting`` itself after passing its re-entry guard.
                # Pre-setting ``self._reconnecting`` here would cause the guard
                # ``if self._reconnecting: return`` to short-circuit the reconnect
                # loop, leaving the client in a "reconnecting forever" state with
                # no actual reconnect ever attempted.
                if self.auto_reconnect and self._is_recoverable_error(e) and not self._force_disconnect:
                    should_reconnect = True
        finally:
            # Don't fire disconnect handler if we're about to reconnect — the
            # new connection will fire on_connect when it's established.
            if self.on_disconnect and not self._force_disconnect and not should_reconnect:
                self.on_disconnect("connection lost")

        # Trigger reconnection if needed (outside the try/finally to avoid recursion issues)
        if should_reconnect and not self._stop_event.is_set():
            self._attempt_reconnect()

    def _attempt_reconnect(self):
        """
        Attempt to reconnect with exponential backoff.

        This method runs a reconnection loop until either a connection is
        successfully re-established, the maximum number of retries (if set)
        is exceeded, or a forceful disconnect is requested. When the maximum
        number of retries is exceeded, the client stops attempting to reconnect
        and sets the internal stop event without raising an exception.
        """
        # Prevent nested reconnect calls - if already reconnecting, just return
        # The existing reconnect loop will handle retries
        if self._reconnecting:
            return

        self._reconnecting = True
        # Don't reset attempt counter here - it persists across reconnect calls
        # It gets reset only when we successfully receive a response from server

        try:
            while not self._force_disconnect:
                # Check if we've exceeded max retries (0 = infinite)
                if self.max_retries > 0 and self._reconnect_attempt >= self.max_retries:
                    logger.error("Max reconnection attempts (%d) exceeded, giving up", self.max_retries)
                    self._stop_event.set()
                    # Don't raise here - this is called from listen loop context
                    return

                backoff = self._calculate_backoff(self._reconnect_attempt)
                logger.info("Reconnecting in %.1fs (attempt %d)...", backoff, self._reconnect_attempt + 1)

                # Increment attempt counter BEFORE trying - it resets on successful server response
                self._reconnect_attempt += 1

                # Wait for backoff period, but check for force disconnect periodically
                wait_end = time.time() + backoff
                while time.time() < wait_end:
                    if self._force_disconnect:
                        return
                    time.sleep(0.1)

                try:
                    # Stop old generator by setting stop event BEFORE closing channel
                    self._stop_event.set()

                    # Clean up old connection
                    if self.channel:
                        try:
                            self.channel.close()
                        except Exception:
                            pass

                    # Give old generator a moment to exit (it checks stop_event with 0.1s timeout)
                    time.sleep(0.15)

                    # Clear the request queue (now safe - old generator has stopped)
                    while not self.request_queue.empty():
                        try:
                            self.request_queue.get_nowait()
                        except queue.Empty:
                            break

                    # Reset state flags before reconnection attempt
                    self._stop_event.clear()
                    self._force_disconnect = False
                    self._connection_confirmed = False

                    # Attempt reconnection - don't print success here,
                    # _listen_loop will print it when server actually responds
                    self._do_connect(self._init_msg, self.target)

                    # Return and let listen loop run - if server responds, _connection_confirmed
                    # will be set and _reconnect_attempt will be reset to 0
                    # If connection fails, listen loop will call us again
                    return

                except grpc.RpcError as e:
                    if not self._is_recoverable_error(e):
                        logger.error("Non-recoverable error during reconnect: %s", e.code().name)
                        self._stop_event.set()
                        return
                    # Attempt already incremented above, just continue loop
                except Exception as e:
                    logger.error("Reconnection error: %s", e)
                    # Attempt already incremented above, just continue loop
        finally:
            self._reconnecting = False

    def _load_file(self, path: str) -> bytes:
        """Load a file and return its contents as bytes."""
        with open(path, 'rb') as f:
            return f.read()

    def _build_tls_credentials(self) -> grpc.ChannelCredentials:
        """Build TLS credentials from configured certificates."""
        # Load root CA certificate (optional - uses system CA if not provided)
        root_cert = self._tls_root_cert
        if root_cert is None and self._tls_root_cert_path:
            root_cert = self._load_file(self._tls_root_cert_path)

        # Load client certificate and key for mTLS (both required if either is provided)
        client_cert = self._tls_client_cert
        if client_cert is None and self._tls_client_cert_path:
            client_cert = self._load_file(self._tls_client_cert_path)

        client_key = self._tls_client_key
        if client_key is None and self._tls_client_key_path:
            client_key = self._load_file(self._tls_client_key_path)

        # Validate mTLS configuration - both cert and key must be provided together
        if (client_cert is None) != (client_key is None):
            raise InvalidArgumentError(
                message="Both tls_client_cert and tls_client_key must be provided for mTLS",
                argument="tls_client_cert/tls_client_key"
            )

        return grpc.ssl_channel_credentials(
            root_certificates=root_cert,
            private_key=client_key,
            certificate_chain=client_cert
        )

    def _do_connect(self, init_msg: aether_pb2.InitConnection, target: str):
        """Internal method to establish connection (no retry logic)."""
        if self.tls_enabled:
            credentials = self._build_tls_credentials()
            self.channel = grpc.secure_channel(target, credentials)
        else:
            self.channel = grpc.insecure_channel(target)
        self.stub = aether_pb2_grpc.AetherGatewayStub(self.channel)

        # If we have a session ID from a previous connection, include it for session resume
        if self._session_id:
            # Create a copy of init_msg with resume_session_id set
            init_with_resume = aether_pb2.InitConnection()
            init_with_resume.CopyFrom(init_msg)
            init_with_resume.resume_session_id = self._session_id
            self.request_queue.put(aether_pb2.UpstreamMessage(init=init_with_resume))
        else:
            self.request_queue.put(aether_pb2.UpstreamMessage(init=init_msg))

        responses = self.stub.Connect(self._request_generator())

        self._stream_thread = threading.Thread(target=self._listen_loop, args=(responses,), daemon=True)
        self._stream_thread.start()

    def _connect(self, init_msg: aether_pb2.InitConnection, target: str = "localhost:50051"):
        """Initializes the connection with retry logic."""
        self.target = target
        self._init_msg = init_msg  # Store for reconnection
        self._stop_event.clear()
        self._force_disconnect = False
        self._reconnecting = False
        self._reconnect_attempt = 0
        self._connection_confirmed = False

        attempt = 0
        last_error = None

        while attempt < self.max_retries or self.max_retries == 0:
            try:
                self._do_connect(init_msg, target)
                # Note: Connection isn't truly confirmed until server responds
                # _listen_loop will call on_connect when first response arrives
                logger.info("Connected to %s", target)
                return  # Success (stream started)

            except grpc.RpcError as e:
                # Convert to AetherError for consistent error handling
                aether_error = from_grpc_error(e)
                last_error = aether_error

                if not self._is_recoverable_error(e):
                    logger.error("Non-recoverable connection error: %s", aether_error)
                    raise aether_error

                attempt += 1
                if self.max_retries > 0 and attempt >= self.max_retries:
                    break

                backoff = self._calculate_backoff(attempt - 1)
                logger.warning("Connection failed, retrying in %.1fs (attempt %d/%d)...",
                               backoff, attempt, self.max_retries)
                time.sleep(backoff)

            except AetherError:
                # Re-raise AetherErrors as-is
                raise

            except Exception as e:
                last_error = ConnectionError(
                    message=str(e),
                    details=type(e).__name__
                )
                attempt += 1
                if self.max_retries > 0 and attempt >= self.max_retries:
                    break

                backoff = self._calculate_backoff(attempt - 1)
                logger.warning("Connection error: %s, retrying in %.1fs...", e, backoff)
                time.sleep(backoff)

        # All retries exhausted
        logger.error("Failed to connect after %d attempts", attempt)
        if last_error:
            if isinstance(last_error, AetherError):
                raise ReconnectionError(
                    message=f"Failed to connect to {target}: {last_error.message}",
                    attempts=attempt
                )
            raise ReconnectionError(
                message=f"Failed to connect to {target}",
                attempts=attempt
            )
        raise ReconnectionError(
            message=f"Failed to connect to {target}",
            attempts=attempt
        )

    def _send_sync_op(self, message: aether_pb2.UpstreamMessage, request_id: str,
                      timeout: float = 10.0):
        """
        Send an upstream message and wait for the correlated response.

        Uses the unified _pending_requests registry to route the response
        back to this caller by request_id, making concurrent sync ops safe.

        Args:
            message: The UpstreamMessage to send
            request_id: The request_id set on the inner operation proto
            timeout: Timeout in seconds

        Returns:
            The response proto, or None on timeout
        """
        response_queue: queue.Queue = queue.Queue(maxsize=1)
        with self._pending_requests_lock:
            self._pending_requests[request_id] = response_queue
        try:
            self.request_queue.put(message)
            try:
                result = response_queue.get(timeout=timeout)
            except queue.Empty:
                return None
            # If the listen loop posted an AetherError (correlated error
            # response), re-raise it so callers get a proper exception rather
            # than a silent None or a raw error object.
            if isinstance(result, AetherError):
                raise result
            return result
        finally:
            with self._pending_requests_lock:
                self._pending_requests.pop(request_id, None)

    def _send_message(self, target_topic: str, payload: bytes, message_type: int = aether_pb2.OPAQUE,
                      app_workspace: str = ""):
        """Send a message to a target topic.

        ``app_workspace`` is an optional hint carrying the user's active app
        workspace (e.g. ``"default"``). When set, the gateway stamps it into
        the task-authority grant's WorkspaceScope at triggerOrchestration time
        so spawned agents can create resources in the user's workspace.
        Ignored for non-user principals.
        """
        msg = aether_pb2.SendMessage(
            target_topic=target_topic,
            payload=payload,
            message_type=message_type,  # type: ignore[arg-type]
            app_workspace=app_workspace,
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(send=msg))

    def _switch_workspace(self, new_workspace_id: str):
        """Switch to a different workspace."""
        sw = aether_pb2.SwitchWorkspace(new_workspace_id=new_workspace_id)
        self.request_queue.put(aether_pb2.UpstreamMessage(switch_workspace=sw))

    # =========================================================================
    # KV Operations with full scope support
    # =========================================================================

    def kv_get(self, key: str, scope: str = "global",
               user_id: str = "", workspace: str = "") -> None:
        """
        Get a value from the KV store.

        Args:
            key: The key to retrieve
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.GET,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    def kv_put(self, key: str, value: bytes, scope: str = "global",
               user_id: str = "", workspace: str = "", ttl: int = 0) -> None:
        """
        Put a value in the KV store.

        Args:
            key: The key to store
            value: The value to store (bytes)
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            ttl: Time-to-live in seconds (0 = no expiration)
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.PUT,
            scope=_scope_to_proto(scope),
            key=key,
            value=value,
            user_id=user_id,
            workspace=workspace,
            ttl=ttl
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    def kv_list(self, key_prefix: str = "", scope: str = "global",
                user_id: str = "", workspace: str = "") -> None:
        """
        List keys from the KV store.

        Args:
            key_prefix: Prefix to filter keys (empty for all)
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.LIST,
            scope=_scope_to_proto(scope),
            key=key_prefix,
            user_id=user_id,
            workspace=workspace
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    def kv_delete(self, key: str, scope: str = "global",
                  user_id: str = "", workspace: str = "") -> None:
        """
        Delete a key from the KV store.

        Args:
            key: The key to delete
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.DELETE,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    def kv_increment(self, key: str, scope: str = "global",
                     user_id: str = "", workspace: str = "",
                     ttl: int = 0) -> None:
        """
        Atomically increment a counter in the KV store.

        Args:
            key: The key to increment
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            ttl: Time-to-live in seconds applied on first increment (0 = no expiration)
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.INCREMENT,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace,
            ttl=ttl
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    def kv_decrement(self, key: str, scope: str = "global",
                     user_id: str = "", workspace: str = "") -> None:
        """
        Atomically decrement a counter in the KV store.

        Args:
            key: The key to decrement
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.DECREMENT,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    def kv_increment_if(self, key: str, delta: int = 1, ceiling: int = 0,
                        scope: str = "global",
                        user_id: str = "", workspace: str = "",
                        ttl: int = 0) -> None:
        """
        Fire-and-forget INCREMENT_IF: increment only if result would not exceed ceiling.

        Args:
            key: The key to increment
            delta: Amount to increment by
            ceiling: Maximum allowed value (guard); 0 means no guard
            scope: KV scope string
            user_id: Required for user-scoped operations
            workspace: Required for workspace-scoped operations
            ttl: Time-to-live in seconds on first write (0 = no expiration)
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.INCREMENT_IF,
            scope=_scope_to_proto(scope),
            key=key,
            int_value=delta,
            delta_value=int(delta),
            guard_value=ceiling,
            user_id=user_id,
            workspace=workspace,
            ttl=ttl,
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    def kv_decrement_if(self, key: str, delta: int = 1, floor: int = 0,
                        scope: str = "global",
                        user_id: str = "", workspace: str = "") -> None:
        """
        Fire-and-forget DECREMENT_IF: decrement only if result would not go below floor.

        Args:
            key: The key to decrement
            delta: Amount to decrement by
            floor: Minimum allowed value (guard)
            scope: KV scope string
            user_id: Required for user-scoped operations
            workspace: Required for workspace-scoped operations
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.DECREMENT_IF,
            scope=_scope_to_proto(scope),
            key=key,
            int_value=delta,
            delta_value=int(delta),
            guard_value=floor,
            user_id=user_id,
            workspace=workspace,
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    # =========================================================================
    # Task Creation (Phase 6)
    # =========================================================================

    def create_task(self, task_type: str, workspace: str,
                    target_agent_id: str = "",
                    target_implementation: str = "",
                    launch_param_overrides: Optional[Dict[str, str]] = None,
                    metadata: Optional[Dict[str, str]] = None,
                    payload: Optional[bytes] = None,
                    assignment_mode: int = SELF_ASSIGN,
                    context_id: str = "") -> None:
        """
        Create a new task.

        Args:
            task_type: The type of task to create
            workspace: The workspace for the task
            target_agent_id: For TARGETED mode, the agent to assign to
            target_implementation: For POOL mode, the agent implementation type to match
            launch_param_overrides: Optional parameter overrides for orchestration
            metadata: Optional task metadata
            payload: Optional binary payload for task input data (server-enforced size limit, default 512KB)
            assignment_mode: SELF_ASSIGN (0), TARGETED (1), or POOL (2) [automatically handled in most cases]
            context_id: Optional client-minted session identifier (A2A contextId). Tasks
                sharing a context_id are groupable via TaskFilter.context_id.
        """
        if target_agent_id and assignment_mode == SELF_ASSIGN:
            assignment_mode = TARGETED
        if target_implementation and assignment_mode == SELF_ASSIGN:
            assignment_mode = POOL
        req = aether_pb2.CreateTaskRequest(
            task_type=task_type,
            workspace=workspace,
            assignment_mode=assignment_mode,  # type: ignore[arg-type]
            target_agent_id=target_agent_id,
            target_implementation=target_implementation,
            launch_param_overrides=launch_param_overrides or {},
            metadata=metadata or {},
            payload=payload or b"",
            context_id=context_id,
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(create_task=req))

    def create_task_sync(self, task_type: str, workspace: str,
                         target_agent_id: str = "",
                         target_implementation: str = "",
                         launch_param_overrides: Optional[Dict[str, str]] = None,
                         metadata: Optional[Dict[str, str]] = None,
                         payload: Optional[bytes] = None,
                         assignment_mode: int = SELF_ASSIGN,
                         authorization: Optional[aether_pb2.AuthorizationContext] = None,
                         context_id: str = "",
                         timeout: float = 10.0) -> Optional[aether_pb2.CreateTaskResponse]:
        """
        Create a new task and wait for the server's response containing the task_id.

        Unlike create_task (fire-and-forget), this variant generates a request_id,
        sends it with the CreateTaskRequest, and waits for the correlated
        CreateTaskResponse from the server.

        Args:
            task_type: The type of task to create
            workspace: The workspace for the task
            target_agent_id: For TARGETED mode, the agent to assign to
            target_implementation: For POOL mode, the agent implementation type to match
            launch_param_overrides: Optional parameter overrides for orchestration
            metadata: Optional task metadata
            payload: Optional binary payload for task input data (server-enforced size limit, default 512KB)
            assignment_mode: SELF_ASSIGN (0), TARGETED (1), or POOL (2)
            authorization: Optional on-behalf-of authorization context. When
                present, the created task is associated with the subject's
                authority and the gateway ACL check is evaluated against the
                delegated grant (instead of the raw caller identity).
            context_id: Optional client-minted session identifier (A2A contextId). Tasks
                sharing a context_id are groupable via TaskFilter.context_id.
            timeout: Timeout in seconds (default 10.0)

        Returns:
            CreateTaskResponse with task_id, status, etc., or None on timeout
        """
        if target_agent_id and assignment_mode == SELF_ASSIGN:
            assignment_mode = TARGETED
        if target_implementation and assignment_mode == SELF_ASSIGN:
            assignment_mode = POOL
        request_id = str(uuid.uuid4())
        req = aether_pb2.CreateTaskRequest(
            task_type=task_type,
            workspace=workspace,
            assignment_mode=assignment_mode,  # type: ignore[arg-type]
            target_agent_id=target_agent_id,
            target_implementation=target_implementation,
            launch_param_overrides=launch_param_overrides or {},
            metadata=metadata or {},
            payload=payload or b"",
            authorization=authorization,
            context_id=context_id,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(create_task=req), request_id, timeout,
        )

    # =========================================================================
    # Progress Reporting
    # =========================================================================

    def report_progress(self, task_id: str, state: str = "running",
                        completion: float = -1.0, summary: str = "",
                        step_name: str = "", step_detail: str = "",
                        step_sequence: int = 0, step_total: int = 0,
                        step_type: str = "",
                        recipient: str = "", request_id: str = "",
                        metadata: Optional[Dict[str, str]] = None,
                        kind: int = 0) -> None:
        """
        Report progress on a task or work item.

        Progress is supplemental to the task lifecycle — it describes what
        the agent/task is doing while it is running. Connection liveness
        handles death detection separately.

        Args:
            task_id: Task or correlation ID this progress relates to
            state: Current state (e.g., "running", "finishing", "idle")
            completion: Completion fraction 0.0-1.0, or -1 for indeterminate
            summary: Human-readable summary of current activity
            step_name: Name of the current step (for multi-step progress)
            step_detail: Detail description of the current step
            step_sequence: Step number (1-based)
            step_total: Total number of steps (0 = unknown)
            step_type: Step type hint for UI rendering (e.g., "llm_call", "processing")
            recipient: Target identity topic to receive updates (empty = broadcast to all workspace subscribers)
            request_id: Correlation ID for the originating request
            metadata: Arbitrary key-value metadata
        """
        step = None
        if step_name:
            step = aether_pb2.ProgressStep(
                name=step_name,
                detail=step_detail,
                sequence=step_sequence,
                total_steps=step_total,
                step_type=step_type,
            )

        report = aether_pb2.ProgressReport(
            task_id=task_id,
            state=state,
            completion=completion,
            summary=summary,
            step=step,
            recipient=recipient,
            request_id=request_id,
            metadata=metadata or {},
            kind=kind,
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(progress=report))

    # =========================================================================
    # Checkpoint Operations
    # =========================================================================

    def checkpoint_save(self, data: bytes, key: str = "", ttl: int = -1) -> None:
        """
        Save checkpoint data.

        Checkpoints allow agents/tasks to persist arbitrary state that survives
        restarts. This is separate from message offset tracking (handled automatically).

        Args:
            data: The checkpoint data to save (bytes)
            key: Optional checkpoint key (default: "default"). Allows multiple named checkpoints.
            ttl: Time-to-live in seconds (-1 = server default, 0 = no expiration, >0 = specific TTL)
        """
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.SAVE,
            key=key,
            data=data,
            ttl=ttl
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    def checkpoint_save_sync(self, data: bytes, key: str = "", ttl: int = -1,
                             timeout: float = 5.0) -> Optional[aether_pb2.CheckpointResponse]:
        """
        Save checkpoint data and wait for the response.

        Args:
            data: The checkpoint data to save (bytes)
            key: Optional checkpoint key (default: "default"). Allows multiple named checkpoints.
            ttl: Time-to-live in seconds (-1 = server default, 0 = no expiration, >0 = specific TTL)
            timeout: Timeout in seconds

        Returns:
            CheckpointResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.SAVE,
            key=key, data=data, ttl=ttl,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(checkpoint_op=op),
            request_id, timeout,
        )

    def checkpoint_load(self, key: str = "") -> None:
        """
        Request checkpoint data. Response will arrive via on_checkpoint_response callback.

        Args:
            key: Optional checkpoint key (default: "default")
        """
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.LOAD,
            key=key
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    def checkpoint_load_sync(self, key: str = "",
                             timeout: float = 5.0) -> Optional[aether_pb2.CheckpointResponse]:
        """
        Load checkpoint data and wait for the response.

        Args:
            key: Optional checkpoint key (default: "default")
            timeout: Timeout in seconds

        Returns:
            CheckpointResponse or None if timeout. Check response.data for the checkpoint data.
            If the checkpoint doesn't exist, response.success=True but response.data will be empty.
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.LOAD,
            key=key,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(checkpoint_op=op),
            request_id, timeout,
        )

    def checkpoint_delete(self, key: str = "") -> None:
        """
        Delete a checkpoint.

        Args:
            key: Optional checkpoint key (default: "default")
        """
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.DELETE,
            key=key
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    def checkpoint_delete_sync(self, key: str = "",
                               timeout: float = 5.0) -> Optional[aether_pb2.CheckpointResponse]:
        """
        Delete a checkpoint and wait for the response.

        Args:
            key: Optional checkpoint key (default: "default")
            timeout: Timeout in seconds

        Returns:
            CheckpointResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.DELETE,
            key=key,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(checkpoint_op=op),
            request_id, timeout,
        )

    def checkpoint_list(self) -> None:
        """
        Request list of checkpoint keys. Response will arrive via on_checkpoint_response callback.
        """
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.LIST
        )
        self.request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    def checkpoint_list_sync(self, timeout: float = 5.0) -> Optional[aether_pb2.CheckpointResponse]:
        """
        List all checkpoint keys and wait for the response.

        Args:
            timeout: Timeout in seconds

        Returns:
            CheckpointResponse or None if timeout. Check response.keys for the list of checkpoint keys.
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.LIST,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(checkpoint_op=op),
            request_id, timeout,
        )

    # =========================================================================
    # Task Management Operations
    # =========================================================================

    # Map short status names to proto enum values for task queries
    _TASK_STATUS_MAP = {
        'queued': aether_pb2.TASK_STATUS_QUEUED,
        'pending': aether_pb2.TASK_STATUS_QUEUED,  # alias
        'running': aether_pb2.TASK_STATUS_RUNNING,
        'completed': aether_pb2.TASK_STATUS_COMPLETED,
        'failed': aether_pb2.TASK_STATUS_FAILED,
        'cancelled': aether_pb2.TASK_STATUS_CANCELLED,
    }

    def query_tasks(self, workspace: str = "", status: str = "",
                    statuses: Optional[List[str]] = None,
                    task_type: str = "", limit: int = 0, offset: int = 0,
                    task_class: int = 0,
                    exclude_task_classes: Optional[List[int]] = None,
                    creator_actor: Optional[aether_pb2.PrincipalRef] = None,
                    status_timestamp_after_unix_ms: int = 0,
                    page_token: str = "",
                    include_descendants: bool = False,
                    timeout: float = 10.0) -> Optional[aether_pb2.TaskQueryResponse]:
        """
        List tasks with optional filters and wait for the response.

        Args:
            workspace: Filter by workspace
            status: Filter by single task status (deprecated — use statuses)
            statuses: Filter by multiple task statuses (e.g. ["pending", "running"])
            task_type: Filter by task type
            limit: Maximum number of results (0 = server default)
            offset: Offset for pagination
            task_class: Filter by task class integer value
            exclude_task_classes: Exclude tasks of these class values
            creator_actor: Phase 4 (Stage A) — filter by the principal that
                created the task (lineage).
            status_timestamp_after_unix_ms: Phase 4 (Stage A) — only return
                tasks whose last status change occurred at or after this
                unix-ms timestamp.
            page_token: Phase 4 (Stage A) — cursor for pagination; pass the
                value of TaskQueryResponse.next_page_token from a prior call.
            include_descendants: Phase 4 (Stage A) — when True and
                parent_task_id is set in the filter, return all descendants
                recursively (not just direct children).
            timeout: Timeout in seconds

        Returns:
            TaskQueryResponse or None if timeout. When page_token is set in
            the response, pass it back as page_token to retrieve the next page.
        """
        request_id = str(uuid.uuid4())

        # Build repeated statuses from list, falling back to singular status
        proto_statuses = []
        if statuses:
            for s in statuses:
                enum_val = self._TASK_STATUS_MAP.get(s.lower())
                if enum_val is not None:
                    proto_statuses.append(enum_val)

        status_enum = 0
        if not proto_statuses and status:
            status_enum = self._TASK_STATUS_MAP.get(status.lower(), 0)

        task_filter = aether_pb2.TaskFilter(
            status=status_enum,
            statuses=proto_statuses,
            workspace=workspace,
            task_type=task_type,
            task_class=task_class,  # type: ignore[arg-type]
            exclude_task_classes=exclude_task_classes or [],  # type: ignore[arg-type]
            limit=limit,
            offset=offset,
            creator_actor=creator_actor,
            status_timestamp_after_unix_ms=status_timestamp_after_unix_ms,
            page_token=page_token,
            include_descendants=include_descendants,
        )
        query = aether_pb2.TaskQuery(
            op=aether_pb2.TaskQuery.LIST,
            filter=task_filter,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_query=query),
            request_id, timeout,
        )

    def register_task_event_handler(
            self, handler: Callable[[aether_pb2.TaskEvent], None]) -> None:
        """
        Register a callback invoked for each TaskEvent received via
        subscribe_to_task().

        The handler replaces any previously registered handler (mirrors the
        single-handler convention used by on_progress, on_task_assignment,
        etc.). To fan-out to multiple consumers, wrap them in one callable.

        The handler runs on the SDK's I/O thread. It should be fast; use a
        queue if heavy processing is needed.

        Args:
            handler: Callable that accepts a TaskEvent proto message.
        """
        self.on_task_event = handler

    def subscribe_to_task(
            self, task_id: str, recursive: bool = False,
            start_timestamp_unix_ms: int = 0,
            timeout: float = 10.0) -> Optional[aether_pb2.TaskSubscriptionOperationResponse]:
        """
        Subscribe to the per-task event stream for ``task_id``.

        Events arrive on the downstream channel as TaskEvent messages. Register
        a handler with :meth:`register_task_event_handler` (or set
        ``on_task_event`` directly) before subscribing to consume them.

        Args:
            task_id: The task to subscribe to.
            recursive: If True, also stream events for descendant tasks.
                Snapshot taken at subscribe-time; tasks born after the
                subscription are picked up via child-lifecycle events on the
                parent stream.
            start_timestamp_unix_ms: Cold-start cursor. 0 = live-only (no
                replay of historical events).
            timeout: Gateway RPC timeout in seconds.

        Returns:
            TaskSubscriptionOperationResponse carrying the server-issued
            subscription_id. Pass that id to :meth:`unsubscribe_from_task`.
            Returns None on timeout.

        Raises:
            ValueError: if task_id is empty.
        """
        if not task_id:
            raise ValueError("task_id must not be empty")
        client_request_id = str(uuid.uuid4())
        op = aether_pb2.TaskSubscriptionOperation(
            op=aether_pb2.TaskSubscriptionOperation.SUBSCRIBE,
            task_id=task_id,
            recursive=recursive,
            start_timestamp_unix_ms=start_timestamp_unix_ms,
            client_request_id=client_request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_subscription_op=op),
            client_request_id, timeout,
        )

    def unsubscribe_from_task(
            self, subscription_id: str,
            timeout: float = 10.0) -> Optional[aether_pb2.TaskSubscriptionOperationResponse]:
        """
        Stop streaming events for a previously-created subscription.

        Args:
            subscription_id: The id returned by :meth:`subscribe_to_task`.
            timeout: Gateway RPC timeout in seconds.

        Returns:
            TaskSubscriptionOperationResponse or None on timeout.

        Raises:
            ValueError: if subscription_id is empty.
        """
        if not subscription_id:
            raise ValueError("subscription_id must not be empty")
        client_request_id = str(uuid.uuid4())
        op = aether_pb2.TaskSubscriptionOperation(
            op=aether_pb2.TaskSubscriptionOperation.UNSUBSCRIBE,
            subscription_id=subscription_id,
            client_request_id=client_request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_subscription_op=op),
            client_request_id, timeout,
        )

    def get_task(self, task_id: str,
                 timeout: float = 10.0) -> Optional[aether_pb2.TaskQueryResponse]:
        """
        Get a specific task by ID and wait for the response.

        Args:
            task_id: The task ID to retrieve
            timeout: Timeout in seconds

        Returns:
            TaskQueryResponse or None if timeout. Check response.task for the task info.
        """
        request_id = str(uuid.uuid4())
        query = aether_pb2.TaskQuery(
            op=aether_pb2.TaskQuery.GET,
            task_id=task_id,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_query=query),
            request_id, timeout,
        )

    def cancel_task(self, task_id: str, reason: str = "",
                    timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Cancel a running or queued task and wait for the response.

        Args:
            task_id: The task ID to cancel
            reason: Optional reason for cancellation
            timeout: Timeout in seconds

        Returns:
            TaskOperationResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.CANCEL,
            task_id=task_id,
            reason=reason,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def retry_task(self, task_id: str,
                   timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Retry a failed or cancelled task and wait for the response.

        Args:
            task_id: The task ID to retry
            timeout: Timeout in seconds

        Returns:
            TaskOperationResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.RETRY,
            task_id=task_id,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def complete_task(self, task_id: str,
                      timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Mark a task as completed.

        Used by POOL workers to signal successful task execution back to
        Aether so the task transitions from assigned/running to completed.

        Args:
            task_id: The task ID to mark as completed
            timeout: Timeout in seconds

        Returns:
            TaskOperationResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.COMPLETE,
            task_id=task_id,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def fail_task(self, task_id: str, reason: str = "",
                  timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Mark a task as failed.

        Used by POOL workers to signal task failure back to Aether so
        the task transitions from assigned/running to failed.

        Args:
            task_id: The task ID to mark as failed
            reason: Error message or failure reason
            timeout: Timeout in seconds

        Returns:
            TaskOperationResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.FAIL,
            task_id=task_id,
            reason=reason,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def pause_task(self, task_id: str, wait_spec: aether_pb2.WaitSpec,
                   reason: str = "",
                   timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Pause a running task into a waiting state described by wait_spec.

        ``wait_spec.reason`` determines the target status (INPUT, AUTHORITY,
        DEPENDENCY, HIBERNATION). ``reason`` is a free-text narrative for the
        audit trail.

        Args:
            task_id: The task ID to pause
            wait_spec: Describes the wait condition. Must be non-None and
                ``wait_spec.reason`` must be set (not WAIT_REASON_UNSPECIFIED).
                Use :func:`make_wait_spec` to build this conveniently.
            reason: Optional human-readable narrative for the audit trail
            timeout: Timeout in seconds (default 10.0)

        Returns:
            TaskOperationResponse or None if timeout

        Raises:
            ValueError: If ``wait_spec`` is None or its reason is unspecified.
        """
        if wait_spec is None:
            raise ValueError("pause_task: wait_spec must be provided")
        if wait_spec.reason == aether_pb2.WAIT_REASON_UNSPECIFIED:
            raise ValueError("pause_task: wait_spec.reason must not be WAIT_REASON_UNSPECIFIED")
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.PAUSE,
            task_id=task_id,
            reason=reason,
            wait_spec=wait_spec,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def wait_for_task(self, task_id: str, depends_on: List[str],
                      wake_on_any: bool = False, timeout_ms: int = 0,
                      scheduled_wake_unix_ms: int = 0,
                      reason: str = "",
                      timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Specialization of pause_task for spontaneous task dependencies.

        Forces ``wait_spec.reason = DEPENDENCY``. ``depends_on`` must be
        non-empty. ``wake_on_any=False`` (default) waits for ALL listed tasks
        to terminate; ``True`` wakes on the first.

        Args:
            task_id: The task ID to pause into a dependency wait
            depends_on: Non-empty list of task IDs whose termination satisfies
                the wait condition.
            wake_on_any: If True, wake when any listed task terminates;
                if False (default), wake only when all have terminated.
            timeout_ms: Maximum wait duration in milliseconds (0 = no timeout).
            scheduled_wake_unix_ms: Unix timestamp (ms) for a forced wake-up
                regardless of dependency resolution (0 = disabled).
            reason: Optional human-readable narrative for the audit trail
            timeout: Timeout in seconds for the RPC call (default 10.0)

        Returns:
            TaskOperationResponse or None if timeout

        Raises:
            ValueError: If ``depends_on`` is empty.
        """
        if not depends_on:
            raise ValueError("wait_for_task: depends_on must be non-empty")
        wait_spec = aether_pb2.WaitSpec(
            reason=aether_pb2.WAIT_REASON_DEPENDENCY,
            depends_on=depends_on,
            wake_on_any=wake_on_any,
            timeout_ms=timeout_ms,
            scheduled_wake_unix_ms=scheduled_wake_unix_ms,
        )
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.WAIT_FOR,
            task_id=task_id,
            reason=reason,
            wait_spec=wait_spec,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def resume_task(self, task_id: str,
                    timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Force-resume a paused task back to running.

        Normally the server's task_waker does this automatically when wake
        conditions are satisfied; ``resume_task`` is for manual / admin paths.

        Args:
            task_id: The task ID to resume
            timeout: Timeout in seconds (default 10.0)

        Returns:
            TaskOperationResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.RESUME,
            task_id=task_id,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def reject_task(self, task_id: str, reason: str = "",
                    timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Reject a task (terminal). Used when an agent declines before processing.

        Distinct from ``fail_task`` — REJECT means "I won't try this", not
        "I tried and broke".

        Args:
            task_id: The task ID to reject
            reason: Optional reason for rejection
            timeout: Timeout in seconds (default 10.0)

        Returns:
            TaskOperationResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.REJECT,
            task_id=task_id,
            reason=reason,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    # =========================================================================
    # Phase 2 Stage C: AuthorityRequest ("sudo") lifecycle
    # =========================================================================

    def request_authority(self,
                          desired_workspace_scope: Optional[List[str]] = None,
                          desired_resource_scope: Optional[List[aether_pb2.AuthorityRequestResourceScopeEntry]] = None,
                          desired_operation_scope: Optional[List[str]] = None,
                          requested_access_level: int = aether_pb2.ACCESS_LEVEL_READWRITE,
                          requested_duration_seconds: int = 1800,
                          audience_type: str = "",
                          audience_id: str = "",
                          routing_principal: Optional[aether_pb2.PrincipalRef] = None,
                          routing_capability: str = "",
                          reason: str = "",
                          task_id: str = "",
                          metadata: Optional[Dict[str, str]] = None,
                          requesting_actor: Optional[aether_pb2.PrincipalRef] = None,
                          target_subject: Optional[aether_pb2.PrincipalRef] = None,
                          timeout: float = 10.0) -> Optional[aether_pb2.AuthorityRequestOperationResponse]:
        """
        Submit a new authority request (the "sudo" handshake).

        ``requesting_actor`` defaults to the caller's session identity when
        left ``None``. Exactly one of (``routing_principal``,
        ``routing_capability``) must be provided.

        On success the gateway echoes back an
        :class:`aether_pb2.AuthorityRequestOperationResponse` with the
        created request (server-assigned ``request_id``, ``status=PENDING``,
        and ``expires_at`` populated).

        Args:
            desired_workspace_scope: Workspaces the resulting grant should
                authorize.
            desired_resource_scope: Resource-type + glob pattern entries.
                Build via :func:`make_authority_request_resource_scope_entry`.
            desired_operation_scope: Operation strings (e.g. ``["send_message"]``).
            requested_access_level: Proto :class:`aether_pb2.AccessLevel`
                enum value (default ``READWRITE``).
            requested_duration_seconds: Lifetime of the resulting grant.
                The server clamps to ``MaxAuthorityRequestDurationSeconds``.
            audience_type: Optional audience binding (session / task / agent / service).
            audience_id: Concrete audience id (must match the caller's
                associated context unless cross-binding is permitted).
            routing_principal: Specific approver. Mutually exclusive with
                ``routing_capability``.
            routing_capability: Capability gate, e.g.
                ``"capability/approve/admin"``. Mutually exclusive with
                ``routing_principal``.
            reason: Human-readable narrative for the audit trail.
            task_id: Optional task id the requester is parked on; the waker
                uses this to reconcile WAITING_AUTHORITY tasks.
            metadata: Free-form key/value annotations propagated to the
                eventual grant.
            requesting_actor: Override the requester identity; defaults to
                the caller.
            target_subject: On-behalf-of escalation target; leave ``None``
                for the common "asking for myself" case.
            timeout: Timeout in seconds for the RPC.

        Returns:
            ``AuthorityRequestOperationResponse`` or ``None`` on timeout.

        Raises:
            ValueError: If routing_principal and routing_capability are both
                set, or both empty.
        """
        routing = make_authority_request_routing(
            principal=routing_principal,
            capability=routing_capability,
        )
        payload = aether_pb2.CreateAuthorityRequestPayload(
            desired_workspace_scope=list(desired_workspace_scope or []),
            desired_resource_scope=list(desired_resource_scope or []),
            desired_operation_scope=list(desired_operation_scope or []),
            requested_access_level=requested_access_level,  # type: ignore[arg-type]
            requested_duration_seconds=requested_duration_seconds,
            audience_type=audience_type,
            audience_id=audience_id,
            routing_target=routing,
            reason=reason,
            task_id=task_id,
            metadata=metadata or {},
        )
        if requesting_actor is not None:
            payload.requesting_actor.CopyFrom(requesting_actor)
        if target_subject is not None:
            payload.target_subject.CopyFrom(target_subject)

        request_id = str(uuid.uuid4())
        op = aether_pb2.AuthorityRequestOperation(
            op=aether_pb2.AuthorityRequestOperation.CREATE,
            create=payload,
            client_request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            request_id, timeout,
        )

    def list_pending_authority_requests(self,
                                        workspace: str = "",
                                        matching_capabilities: Optional[List[str]] = None,
                                        limit: int = 100,
                                        offset: int = 0,
                                        timeout: float = 10.0) -> Optional[aether_pb2.AuthorityRequestOperationResponse]:
        """
        List pending authority requests the caller can resolve.

        The server returns requests whose routing addresses the caller's
        identity OR whose ``routing_capability`` matches any entry in
        ``matching_capabilities``. The server does NOT auto-discover the
        caller's full capability set in Stage C — the caller must enumerate
        which gates it is representing.

        Args:
            workspace: Optional workspace filter; empty = no filter.
            matching_capabilities: List of capability-gate strings the
                caller holds. Empty list = no capability-gate matches
                (principal-routed requests still returned).
            limit: Max rows to return.
            offset: Pagination offset.
            timeout: RPC timeout in seconds.

        Returns:
            ``AuthorityRequestOperationResponse`` with ``requests`` populated.
        """
        list_filter = aether_pb2.AuthorityRequestListFilter(
            workspace=workspace,
            limit=int(limit),
            offset=int(offset),
            matching_capabilities=list(matching_capabilities or []),
        )
        request_id = str(uuid.uuid4())
        op = aether_pb2.AuthorityRequestOperation(
            op=aether_pb2.AuthorityRequestOperation.LIST_PENDING,
            list_filter=list_filter,
            client_request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            request_id, timeout,
        )

    def resolve_authority_request(self,
                                  request_id: str,
                                  approve: bool = True,
                                  reason: str = "",
                                  granted_workspace_scope: Optional[List[str]] = None,
                                  granted_resource_scope: Optional[List[aether_pb2.AuthorityRequestResourceScopeEntry]] = None,
                                  granted_operation_scope: Optional[List[str]] = None,
                                  granted_access_level: int = 0,
                                  granted_duration_seconds: int = 0,
                                  may_delegate: bool = False,
                                  remaining_hops: int = 0,
                                  timeout: float = 10.0) -> Optional[aether_pb2.AuthorityRequestOperationResponse]:
        """
        Resolve a pending authority request (approve or deny).

        ``approve=True`` mints a grant via the standard CreateAuthorityGrant
        path. ``approve=False`` flips the row to DENIED with the supplied
        reason; the granted_* fields are ignored.

        Approvers may NARROW scope: each ``granted_*`` field is intersected
        with the corresponding desired_* field from the original request.
        Anything in the granted set that is NOT in the request is silently
        dropped server-side (approvers cannot broaden).

        Args:
            request_id: Server-assigned request id from CREATE.
            approve: True to APPROVE, False to DENY.
            reason: Human-readable resolution explanation.
            granted_workspace_scope: Optional narrower workspace scope.
            granted_resource_scope: Optional narrower resource scope.
            granted_operation_scope: Optional narrower operation scope.
            granted_access_level: Optional access-level cap (proto enum
                value; 0 = inherit).
            granted_duration_seconds: Optional shorter duration; 0 = inherit.
            may_delegate: Whether the resulting grant carries delegate-on
                authority.
            remaining_hops: Initial hop count for delegation chains.
            timeout: RPC timeout in seconds.
        """
        decision = (
            aether_pb2.ResolveAuthorityRequestPayload.APPROVE
            if approve else aether_pb2.ResolveAuthorityRequestPayload.DENY
        )
        payload = aether_pb2.ResolveAuthorityRequestPayload(
            decision=decision,
            granted_workspace_scope=list(granted_workspace_scope or []),
            granted_resource_scope=list(granted_resource_scope or []),
            granted_operation_scope=list(granted_operation_scope or []),
            granted_access_level=granted_access_level,  # type: ignore[arg-type]
            granted_duration_seconds=int(granted_duration_seconds),
            reason=reason,
            may_delegate=may_delegate,
            remaining_hops=int(remaining_hops),
        )
        client_request_id = str(uuid.uuid4())
        op = aether_pb2.AuthorityRequestOperation(
            op=aether_pb2.AuthorityRequestOperation.RESOLVE,
            request_id=request_id,
            resolve=payload,
            client_request_id=client_request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            client_request_id, timeout,
        )

    def cancel_authority_request(self, request_id: str, reason: str = "",
                                 timeout: float = 10.0) -> Optional[aether_pb2.AuthorityRequestOperationResponse]:
        """
        Withdraw a pending authority request. Only the original requester
        may call this — the gateway returns a not-found-style error to
        non-requesters (info hiding).

        Args:
            request_id: Server-assigned request id.
            reason: Optional reason for withdrawal (recorded as
                ``resolution_reason``).
            timeout: RPC timeout in seconds.
        """
        client_request_id = str(uuid.uuid4())
        op = aether_pb2.AuthorityRequestOperation(
            op=aether_pb2.AuthorityRequestOperation.CANCEL,
            request_id=request_id,
            reason=reason,
            client_request_id=client_request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            client_request_id, timeout,
        )

    # =========================================================================
    # Phase 3 Stage C: Hibernation lifecycle
    # =========================================================================

    def hibernate_until(self, task_id: str,
                        checkpoint_key: str,
                        scheduled_wake_unix_ms: int = 0,
                        timeout_ms: int = 0,
                        resume_session_id: str = "",
                        wake_event_types: Optional[List[str]] = None,
                        escalation_policy: str = "",
                        reason: str = "",
                        timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Park a task into TASK_STATUS_HIBERNATED so its worker can be released
        and compute reclaimed. The task wakes when ``scheduled_wake_unix_ms``
        is reached (set to 0 for "wait indefinitely until external wake"), or
        fails if ``timeout_ms`` elapses (subject to ``escalation_policy``).

        Precondition: a checkpoint with key ``checkpoint_key`` MUST already be
        SAVE'd for this worker's identity — the gateway validates checkpoint
        existence before allowing the transition.

        Args:
            task_id: The task ID to hibernate.
            checkpoint_key: The checkpoint key the worker SAVE'd before this
                call.
            scheduled_wake_unix_ms: Absolute wake time (unix ms). 0 = no
                scheduled wake.
            timeout_ms: Max duration in hibernation before
                ``escalation_policy`` fires. 0 = no timeout.
            resume_session_id: Optional session id to resume; empty = fresh
                session.
            wake_event_types: Reserved for future use; current waker only
                honors ``scheduled_wake_unix_ms``.
            escalation_policy: ``""`` / ``"fail"`` (default) — fail the task
                on timeout. ``"retry"`` — re-queue for fresh worker spawn.
                ``"alert"`` — stay hibernated, log warning.
            reason: Free-text narrative for audit.
            timeout: Gateway RPC timeout in seconds.

        Returns:
            ``TaskOperationResponse`` on success.

        Raises:
            ValueError: if ``checkpoint_key`` is empty (required
                precondition).
        """
        if not checkpoint_key:
            raise ValueError("hibernate_until: checkpoint_key is required")

        hibernation = make_hibernation_descriptor(
            checkpoint_key=checkpoint_key,
            resume_session_id=resume_session_id,
            wake_event_types=wake_event_types,
            escalation_policy=escalation_policy,
        )
        wait_spec = aether_pb2.WaitSpec(
            reason=aether_pb2.WAIT_REASON_HIBERNATION,
            scheduled_wake_unix_ms=scheduled_wake_unix_ms,
            timeout_ms=timeout_ms,
            hibernation=hibernation,
        )
        request_id = str(uuid.uuid4())
        op = aether_pb2.TaskOperation(
            op=aether_pb2.TaskOperation.PAUSE,
            task_id=task_id,
            reason=reason,
            wait_spec=wait_spec,
            request_id=request_id,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    def _send_lockable_op(self, lock: threading.Lock, response_queue: queue.Queue,
                          message: aether_pb2.UpstreamMessage, timeout: float = 10.0):
        """
        Send an operation that requires exclusive lock, drain stale responses, send, wait for response.

        Used for operation types (workspace, agent, ACL) that do not support request_id correlation
        and must be serialized per-type to avoid response mismatches.

        Args:
            lock: Per-operation-type threading lock
            response_queue: Queue where the listen loop deposits the response
            message: UpstreamMessage to send
            timeout: Timeout in seconds

        Returns:
            The response proto, or None on timeout
        """
        with lock:
            while not response_queue.empty():
                try:
                    response_queue.get_nowait()
                except queue.Empty:
                    break

            self.request_queue.put(message)

            try:
                return response_queue.get(timeout=timeout)
            except queue.Empty:
                return None

    def workspace_op(self, op: aether_pb2.WorkspaceOperation,
                     timeout: float = 10.0):
        """
        Send a workspace operation and wait for the response.

        Uses a per-type lock to serialize concurrent callers since
        WorkspaceOperation does not support request_id correlation.

        Args:
            op: WorkspaceOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            Workspace response or None if timeout
        """
        return self._send_lockable_op(
            self._workspace_op_lock,
            self._workspace_response_queue,
            aether_pb2.UpstreamMessage(workspace_op=op),
            timeout,
        )

    def agent_op(self, op: aether_pb2.AgentOperation,
                 timeout: float = 10.0):
        """
        Send an agent operation and wait for the response.

        Uses a per-type lock to serialize concurrent callers since
        AgentOperation does not support request_id correlation.

        Args:
            op: AgentOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            Agent response or None if timeout
        """
        return self._send_lockable_op(
            self._agent_op_lock,
            self._agent_response_queue,
            aether_pb2.UpstreamMessage(agent_op=op),
            timeout,
        )

    def acl_op(self, op: aether_pb2.ACLOperation,
               timeout: float = 10.0):
        """
        Send an ACL operation and wait for the response.

        Uses a per-type lock to serialize concurrent callers since
        ACLOperation does not support request_id correlation.

        Args:
            op: ACLOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            ACL response or None if timeout
        """
        return self._send_lockable_op(
            self._acl_op_lock,
            self._acl_response_queue,
            aether_pb2.UpstreamMessage(acl_op=op),
            timeout,
        )

    def workflow_op(self, op: aether_pb2.WorkflowOperation,
                    timeout: float = 10.0):
        """
        Send a workflow operation and wait for the response.

        Args:
            op: WorkflowOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            Workflow response or None if timeout
        """
        request_id = str(uuid.uuid4())
        op.request_id = request_id
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(workflow_op=op),
            request_id, timeout,
        )

    def _send_token_op(self, op: aether_pb2.TokenOperation,
                       timeout: float = 10.0):
        """Send a TokenOperation with request ID correlation and wait for response."""
        request_id = str(uuid.uuid4())
        op.request_id = request_id
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(token_op=op),
            request_id, timeout,
        )

    def token_op(self, op: aether_pb2.TokenOperation,
                 timeout: float = 10.0):
        """
        Send a TokenOperation and wait for the response (low-level escape hatch).

        Args:
            op: aether_pb2.TokenOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            TokenResponse or None if timeout
        """
        return self._send_token_op(op, timeout)

    def _send_authority_grant_op(self, op: aether_pb2.AuthorityGrantOperation,
                                 timeout: float = 10.0):
        """Send an AuthorityGrantOperation with request ID correlation and wait for response."""
        request_id = str(uuid.uuid4())
        op.request_id = request_id
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_grant_op=op),
            request_id, timeout,
        )

    def authority_grant_op(self, op: aether_pb2.AuthorityGrantOperation,
                           timeout: float = 10.0):
        """
        Send an AuthorityGrantOperation and wait for the response.

        Args:
            op: AuthorityGrantOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            AuthorityGrantResponse or None if timeout
        """
        return self._send_authority_grant_op(op, timeout)

    def exchange_authority_grant(self,
                                 source_session_id: str = "",
                                 workspace_scope: Optional[List[str]] = None,
                                 resource_scope: Optional[Dict[str, List[str]]] = None,
                                 operation_scope: Optional[List[str]] = None,
                                 max_access_level: int = 0,
                                 audience_type: str = "",
                                 audience_id: str = "",
                                 valid_while_audience_active: bool = False,
                                 expires_at: int = 0,
                                 renewable_until: int = 0,
                                 may_delegate: bool = False,
                                 remaining_hops: int = 0,
                                 reason: str = "",
                                 metadata: Optional[Dict[str, str]] = None,
                                 timeout: float = 10.0):
        """Exchange a runtime authority grant for the current actor."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.EXCHANGE,
            exchange_request=aether_pb2.AuthorityGrantExchangeRequest(
                source_session_id=source_session_id,
                workspace_scope=workspace_scope or [],
                resource_scope=_authority_resource_scope_entries(resource_scope),
                operation_scope=operation_scope or [],
                max_access_level=max_access_level,
                audience_type=audience_type,
                audience_id=audience_id,
                valid_while_audience_active=valid_while_audience_active,
                expires_at=expires_at,
                renewable_until=renewable_until,
                may_delegate=may_delegate,
                remaining_hops=remaining_hops,
                reason=reason,
                metadata=metadata or {},
            ),
        )
        return self._send_authority_grant_op(op, timeout)

    def derive_authority_grant(self,
                               parent_grant_id: str,
                               delegate_type: str,
                               delegate_id: str,
                               workspace_scope: Optional[List[str]] = None,
                               resource_scope: Optional[Dict[str, List[str]]] = None,
                               operation_scope: Optional[List[str]] = None,
                               max_access_level: int = 0,
                               audience_type: str = "",
                               audience_id: str = "",
                               valid_while_audience_active: bool = False,
                               expires_at: int = 0,
                               renewable_until: int = 0,
                               may_delegate: bool = False,
                               remaining_hops: int = 0,
                               reason: str = "",
                               metadata: Optional[Dict[str, str]] = None,
                               timeout: float = 10.0):
        """Derive a child authority grant from an existing parent grant."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.DERIVE,
            derive_request=aether_pb2.AuthorityGrantDeriveRequest(
                parent_grant_id=parent_grant_id,
                delegate=_principal_ref(delegate_type, delegate_id),
                workspace_scope=workspace_scope or [],
                resource_scope=_authority_resource_scope_entries(resource_scope),
                operation_scope=operation_scope or [],
                max_access_level=max_access_level,
                audience_type=audience_type,
                audience_id=audience_id,
                valid_while_audience_active=valid_while_audience_active,
                expires_at=expires_at,
                renewable_until=renewable_until,
                may_delegate=may_delegate,
                remaining_hops=remaining_hops,
                reason=reason,
                metadata=metadata or {},
            ),
        )
        return self._send_authority_grant_op(op, timeout)

    def get_authority_grant(self, grant_id: str, timeout: float = 10.0):
        """Get a runtime authority grant by ID."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.GET,
            grant_id=grant_id,
        )
        return self._send_authority_grant_op(op, timeout)

    def renew_authority_grant(self, grant_id: str, expires_at: int,
                              timeout: float = 10.0):
        """Renew a runtime authority grant lease."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.RENEW,
            grant_id=grant_id,
            renew_request=aether_pb2.ACLRenewAuthorityGrantRequest(
                grant_id=grant_id,
                expires_at=expires_at,
            ),
        )
        return self._send_authority_grant_op(op, timeout)

    def submit_audit_event(self,
                           event_type: str,
                           operation: str = "",
                           resource_type: str = "",
                           resource_id: str = "",
                           workspace: str = "",
                           success: bool = True,
                           error_message: str = "",
                           metadata: Optional[Dict[str, str]] = None,
                           timeout: float = 10.0) -> Optional[AuditSubmitResponse]:
        """Submit a foreign audit event to the gateway and wait for the ack.

        Mirrors :meth:`AuthorityGrantOps.derive_authority_grant`'s correlation
        pattern: mints a fresh ``client_request_id``, registers a pending
        queue, sends the upstream ``SubmitAuditEventRequest``, and blocks until
        the matching downstream ``SubmitAuditEventResponse`` arrives or the
        timeout fires.

        The gateway always stamps actor identity from the authenticated session
        and validates ``event_type`` against the foreign whitelist
        (``message`` / ``kv`` / ``task`` / ``custom``); gateway-truth
        categories (connection / auth / admin / acl) are reserved for server
        emission and will be rejected. Metadata is sanitized for credential-
        shaped keys regardless of audit verbosity.

        Args:
            event_type: Required. One of ``message`` / ``kv`` / ``task`` /
                ``custom``.
            operation: Free-form operation name (e.g. ``send``, ``get``,
                ``complete``).
            resource_type: Optional resource classifier (e.g. ``kv_key``).
            resource_id: Optional resource identifier.
            workspace: Optional workspace override. Cross-workspace submissions
                require ``capability/audit_submit`` on the target workspace.
            success: Whether the audited operation itself succeeded.
            error_message: Optional error context when ``success=False``.
            metadata: Additional context map (credential-shaped keys are
                redacted server-side).
            timeout: Response timeout in seconds.

        Returns:
            An :class:`AuditSubmitResponse` on response, or ``None`` on
            timeout.
        """
        client_request_id = uuid.uuid4().hex
        req = aether_pb2.SubmitAuditEventRequest(
            event_type=event_type,
            operation=operation,
            resource_type=resource_type,
            resource_id=resource_id,
            workspace=workspace,
            success=success,
            error_message=error_message,
            metadata=metadata or {},
            client_request_id=client_request_id,
        )
        resp = self._send_sync_op(
            aether_pb2.UpstreamMessage(submit_audit_event=req),
            client_request_id, timeout,
        )
        if resp is None:
            return None
        return AuditSubmitResponse(
            success=resp.success,
            error_code=resp.error_code,
            error_message=resp.error_message,
            client_request_id=resp.client_request_id,
        )

    def revoke_authority_grant(self, grant_id: str,
                               timeout: float = 10.0):
        """Revoke a runtime authority grant by ID."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.REVOKE,
            grant_id=grant_id,
        )
        return self._send_authority_grant_op(op, timeout)

    def list_tokens(self, limit: int = 0, offset: int = 0,
                    timeout: float = 10.0):
        """List API tokens."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.LIST,
            filter=aether_pb2.TokenFilter(limit=limit, offset=offset),
        )
        return self._send_token_op(op, timeout)

    def get_token(self, token_id: str, timeout: float = 10.0):
        """Get a specific API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.GET,
            token_id=token_id,
        )
        return self._send_token_op(op, timeout)

    def create_token(self, name: str, principal_type: str,
                     workspace_patterns: Optional[List[str]] = None,
                     scopes: Optional[List[str]] = None,
                     expires_in_hours: int = 0,
                     created_by: str = "",
                     timeout: float = 10.0):
        """Create a new API token."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.CREATE,
            create_request=aether_pb2.TokenCreateRequest(
                name=name,
                principal_type=principal_type,
                workspace_patterns=workspace_patterns or [],
                scopes=scopes or [],
                expires_in_hours=expires_in_hours,
                created_by=created_by,
            ),
        )
        return self._send_token_op(op, timeout)

    def delete_token(self, token_id: str, timeout: float = 10.0):
        """Delete an API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.DELETE,
            token_id=token_id,
        )
        return self._send_token_op(op, timeout)

    def revoke_token(self, token_id: str, timeout: float = 10.0):
        """Revoke an API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.REVOKE,
            token_id=token_id,
        )
        return self._send_token_op(op, timeout)

    # ---- ACL convenience methods ----

    def acl_list_rules(self, principal_type: str = "", principal_id: str = "",
                       resource_type: str = "", resource_id: str = "",
                       limit: int = 0, offset: int = 0,
                       timeout: float = 10.0):
        """List ACL rules, optionally filtered.

        Args:
            principal_type: Filter by principal type (e.g. "user", "agent").
            principal_id: Filter by principal ID.
            resource_type: Filter by resource type (e.g. "workspace", "dataset").
            resource_id: Filter by resource ID.
            limit: Maximum number of results (0 = server default).
            offset: Offset for pagination.
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.LIST_RULES,
            rule_filter=aether_pb2.ACLRuleFilter(
                principal_type=principal_type,
                principal_id=principal_id,
                resource_type=resource_type,
                resource_id=resource_id,
                limit=limit,
                offset=offset,
            ),
        )
        return self.acl_op(op, timeout)

    def acl_get_rule(self, rule_id: str, timeout: float = 10.0):
        """Get a specific ACL rule by ID.

        Args:
            rule_id: The rule identifier.
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.GET_RULE,
            rule_id=rule_id,
        )
        return self.acl_op(op, timeout)

    def acl_check_access(self, principal_type: str, principal_id: str,
                         resource_type: str, resource_id: str,
                         required_level: int,
                         timeout: float = 10.0) -> bool:
        """Check whether a principal has at least the required access level on a resource.

        Queries matching ACL rules. If any rule grants access_level >= required_level,
        returns True. Otherwise falls back to the fallback policy for the category
        "{principal_type}_{resource_type}".

        Args:
            principal_type: Principal type (e.g. "user").
            principal_id: Principal identifier.
            resource_type: Resource type (e.g. "workspace").
            resource_id: Resource identifier.
            required_level: Minimum access level required.
            timeout: Timeout in seconds.

        Returns:
            True if access is granted, False otherwise.
        """
        resp = self.acl_list_rules(
            principal_type=principal_type,
            principal_id=principal_id,
            resource_type=resource_type,
            resource_id=resource_id,
            timeout=timeout,
        )
        if resp is not None and resp.rules:
            for rule in resp.rules:
                if rule.access_level >= required_level:
                    return True

        # Check fallback policy
        category = f"{principal_type}_{resource_type}"
        fallback_resp = self.acl_get_fallback_policy(category, timeout=timeout)
        if fallback_resp is not None and fallback_resp.fallback_policy:
            if fallback_resp.fallback_policy.fallback_access_level >= required_level:
                return True

        return False

    def acl_grant(self, principal_type: str, principal_id: str,
                  resource_type: str, resource_id: str,
                  access_level: int, granted_by: str,
                  reason: str = "", expires_at: int = 0,
                  timeout: float = 10.0):
        """Grant an ACL rule.

        Args:
            principal_type: Principal type (e.g. "user", "agent").
            principal_id: Principal identifier.
            resource_type: Resource type (e.g. "workspace").
            resource_id: Resource identifier.
            access_level: Access level to grant.
            granted_by: Identity of the granter.
            reason: Optional reason for the grant.
            expires_at: Optional Unix timestamp for expiry (0 = no expiry).
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.GRANT,
            grant_request=aether_pb2.ACLGrantRequest(
                principal_type=principal_type,
                principal_id=principal_id,
                resource_type=resource_type,
                resource_id=resource_id,
                access_level=access_level,
                granted_by=granted_by,
                reason=reason,
                expires_at=expires_at,
            ),
        )
        return self.acl_op(op, timeout)

    def acl_revoke(self, rule_id: str, timeout: float = 10.0):
        """Revoke an ACL rule by ID.

        Args:
            rule_id: The rule identifier to revoke.
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.REVOKE,
            rule_id=rule_id,
        )
        return self.acl_op(op, timeout)

    def acl_get_fallback_policy(self, rule_category: str,
                                timeout: float = 10.0):
        """Get the fallback policy for a rule category.

        Args:
            rule_category: Category string (e.g. "user_workspace").
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.GET_FALLBACK_POLICY,
            rule_category=rule_category,
        )
        return self.acl_op(op, timeout)

    def acl_set_fallback_policy(self, rule_category: str,
                                fallback_access_level: int,
                                updated_by: str = "",
                                timeout: float = 10.0):
        """Set or update the fallback policy for a rule category.

        Args:
            rule_category: Category string (e.g. "user_workspace").
            fallback_access_level: Default access level when no rules match.
            updated_by: Identity of the updater.
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.SET_FALLBACK_POLICY,
            fallback_request=aether_pb2.ACLSetFallbackRequest(
                rule_category=rule_category,
                fallback_access_level=fallback_access_level,
                updated_by=updated_by,
            ),
        )
        return self.acl_op(op, timeout)

    def acl_query_audit(self, start_time: int = 0, end_time: int = 0,
                        principal_type: str = "", workspace: str = "",
                        limit: int = 0, timeout: float = 10.0):
        """Query ACL audit logs.

        Args:
            start_time: Filter start time (Unix timestamp, 0 = no lower bound).
            end_time: Filter end time (Unix timestamp, 0 = no upper bound).
            principal_type: Filter by principal type.
            workspace: Filter by workspace.
            limit: Maximum number of results (0 = server default).
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.QUERY_AUDIT,
            audit_filter=aether_pb2.ACLAuditFilter(
                start_time=start_time,
                end_time=end_time,
                principal_type=principal_type,
                workspace=workspace,
                limit=limit,
            ),
        )
        return self.acl_op(op, timeout)

    # NOTE: acl_{list,get,create,renew,revoke}_authority_grant helpers were
    # removed. The streaming ACLOperation no longer carries authority-grant
    # ops; use the runtime AuthorityGrantOperation surface (see Phase 4 SDK
    # cache helpers) or the REST admin endpoints for management.

    def acl_cleanup_expired_rules(self, timeout: float = 10.0):
        """Clean up expired ACL rules.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.CLEANUP_EXPIRED,
        )
        return self.acl_op(op, timeout)

    def acl_cleanup_audit_logs(self, retention_days: int = 90,
                               timeout: float = 10.0):
        """Clean up old audit log entries.

        Args:
            retention_days: Number of days of logs to retain (default 90).
            timeout: Timeout in seconds.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.CLEANUP_AUDIT_LOGS,
            retention_days=retention_days,
        )
        return self.acl_op(op, timeout)

    # ------------------------------------------------------------------
    # Agent Registry — Pythonic helpers
    # ------------------------------------------------------------------

    def register_agent(self, implementation: str,
                       profile: str = "local",
                       description: str = "",
                       launch_params: Optional[Dict[str, str]] = None,
                       resource_schema: Optional[List[aether_pb2.AgentResourceSchemaEntry]] = None,
                       capabilities: Optional[Dict[str, bool]] = None,
                       extensions: Optional[List[str]] = None,
                       timeout: float = 10.0):
        """Register an agent implementation for orchestration.

        Args:
            implementation: Unique agent implementation name (e.g. "scitrera/falcon-chat-v2").
            profile: Orchestrator profile that handles this agent (e.g. "local", "k8s").
            description: Human-readable description.
            launch_params: Default launch parameters (string key-value pairs).
            resource_schema: Phase 5 — list of
                :class:`aether_pb2.AgentResourceSchemaEntry` declaring the
                ``resource_type_prefix`` values this agent owns. Build entries
                with :func:`make_resource_schema_entry`. The gateway rejects
                registrations that claim a prefix already declared by another
                active registration (error code ``ERR_PREFIX_CONFLICT``).
            capabilities: Phase 5 — free-form capability flags
                (e.g. ``{"streaming": True}``).
            extensions: Phase 5 — list of extension URIs the agent supports.
            timeout: Response timeout in seconds.

        Returns:
            AgentResponse or None on timeout. On ``ERR_PREFIX_CONFLICT`` the
            response carries ``success=False`` and ``error`` is prefixed with
            ``"ERR_PREFIX_CONFLICT: "``.
        """
        params = dict(launch_params or {})
        params.setdefault("profile", profile)

        agent = aether_pb2.AgentRegistrationInfo(
            implementation=implementation,
            orchestrator_profile=profile,
            description=description,
            launch_params=params,
            capabilities=capabilities or {},
            extensions=list(extensions or []),
        )
        if resource_schema:
            agent.resource_schema.extend(resource_schema)

        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.REGISTER,
            agent=agent,
        )
        return self.agent_op(op, timeout=timeout)

    def update_agent(self, implementation: str,
                     profile: str = "local",
                     description: str = "",
                     launch_params: Optional[Dict[str, str]] = None,
                     resource_schema: Optional[List[aether_pb2.AgentResourceSchemaEntry]] = None,
                     capabilities: Optional[Dict[str, bool]] = None,
                     extensions: Optional[List[str]] = None,
                     timeout: float = 10.0):
        """Update an existing agent registration (UPDATE op).

        Wire-level ``UPDATE`` is upsert-shaped: it replaces every column on
        the existing row with the supplied values (including clearing
        ``resource_schema`` if not provided). Use this when you want
        explicit-update semantics; :meth:`register_agent` is the equivalent
        REGISTER op.

        See :meth:`register_agent` for the Phase 5 kwargs.

        Returns:
            AgentResponse or None on timeout.
        """
        params = dict(launch_params or {})
        params.setdefault("profile", profile)

        agent = aether_pb2.AgentRegistrationInfo(
            implementation=implementation,
            orchestrator_profile=profile,
            description=description,
            launch_params=params,
            capabilities=capabilities or {},
            extensions=list(extensions or []),
        )
        if resource_schema:
            agent.resource_schema.extend(resource_schema)

        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.UPDATE,
            implementation=implementation,
            agent=agent,
        )
        return self.agent_op(op, timeout=timeout)

    def get_agent(self, implementation: str, timeout: float = 10.0):
        """Get an agent registration by implementation name.

        Returns:
            AgentResponse or None on timeout.
        """
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.GET,
            implementation=implementation,
        )
        return self.agent_op(op, timeout=timeout)

    def list_agents(self, profile: str = "",
                    limit: int = 0, offset: int = 0,
                    timeout: float = 10.0):
        """List registered agent implementations.

        Args:
            profile: Optional filter by orchestrator profile.
            limit: Maximum results (0 = default).
            offset: Pagination offset.
            timeout: Response timeout in seconds.

        Returns:
            AgentResponse or None on timeout.
        """
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.LIST,
            filter=aether_pb2.AgentFilter(
                orchestrator_profile=profile,
                limit=limit,
                offset=offset,
            ),
        )
        return self.agent_op(op, timeout=timeout)

    def delete_agent(self, implementation: str, timeout: float = 10.0):
        """Remove an agent registration.

        Returns:
            AgentResponse or None on timeout.
        """
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.DELETE,
            implementation=implementation,
        )
        return self.agent_op(op, timeout=timeout)

    def launch_agent(self, implementation: str,
                     workspace: str = "",
                     specifier: str = "",
                     param_overrides: Optional[Dict[str, str]] = None,
                     timeout: float = 10.0):
        """Launch an agent via the orchestrator.

        Args:
            implementation: Agent implementation to launch.
            workspace: Target workspace.
            specifier: Instance specifier for the new agent.
            param_overrides: Override default launch parameters.
            timeout: Response timeout in seconds.

        Returns:
            AgentResponse (with launch_result) or None on timeout.
        """
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.LAUNCH,
            implementation=implementation,
            launch_params=aether_pb2.AgentLaunchParams(
                specifier=specifier,
                workspace=workspace,
                param_overrides=param_overrides or {},
            ),
        )
        return self.agent_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Audit Query
    # ------------------------------------------------------------------

    def audit_query_sync(self,
                         event_type: str = "",
                         actor_type: str = "",
                         actor_id: str = "",
                         workspace: str = "",
                         operation: str = "",
                         resource_type: str = "",
                         resource_id: str = "",
                         start_time: int = 0,
                         end_time: int = 0,
                         only_failures: bool = False,
                         limit: int = 100,
                         offset: int = 0,
                         timeout: float = 10.0,
                         subject_type: str = "",
                         subject_id: str = "",
                         authority_mode: str = "",
                         authority_grant_id: str = "",
                         authorization: Optional[aether_pb2.AuthorizationContext] = None,
                         exclude_actor_types: Optional[List[str]] = None,
                         exclude_workspaces: Optional[List[str]] = None,
                         exclude_service_direct: bool = False):
        """Query the comprehensive audit log (blocking).

        Args:
            event_type: Filter by event type (connection, auth, message, kv, admin, acl)
            actor_type: Filter by actor type (agent, task, user, system, service)
            actor_id: Filter by specific actor identity
            workspace: Filter by workspace
            operation: Filter by operation name
            resource_type: Filter by resource type
            resource_id: Filter by resource ID
            start_time: Unix timestamp (seconds) lower bound
            end_time: Unix timestamp (seconds) upper bound
            only_failures: If True, only return failed operations
            limit: Max results (default 100, max 500)
            offset: Pagination offset
            timeout: Timeout in seconds
            subject_type: Filter by subject principal type (on-behalf-of entries)
            subject_id: Filter by subject principal id
            authority_mode: Filter by authority mode ("direct" or "on_behalf_of")
            authority_grant_id: Filter by authority grant id
            authorization: Optional on-behalf-of context.
            exclude_actor_types: Actor types to exclude from results.
            exclude_workspaces: Workspaces to exclude from results.
            exclude_service_direct: If True, exclude direct service entries.

        Returns:
            AuditQueryResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        query = aether_pb2.AuditQuery(
            request_id=request_id,
            event_type=event_type,
            actor_type=actor_type,
            actor_id=actor_id,
            workspace=workspace,
            operation=operation,
            resource_type=resource_type,
            resource_id=resource_id,
            start_time=start_time,
            end_time=end_time,
            only_failures=only_failures,
            limit=limit,
            offset=offset,
            subject_type=subject_type,
            subject_id=subject_id,
            authority_mode=authority_mode,
            authority_grant_id=authority_grant_id,
            authorization=authorization,
            exclude_actor_types=exclude_actor_types or [],
            exclude_workspaces=exclude_workspaces or [],
            exclude_service_direct=exclude_service_direct,
        )
        return self._send_sync_op(
            aether_pb2.UpstreamMessage(audit_query=query),
            request_id, timeout,
        )

    # ------------------------------------------------------------------
    # Scheduling convenience methods
    # ------------------------------------------------------------------

    def create_schedule_sync(self, schedule_id: str, name: str,
                             schedule_type: str, schedule_expr: str,
                             action: Optional[dict] = None,
                             workflow_id: str = "",
                             workspace: str = "*",
                             miss_policy: str = "skip",
                             max_concurrent: int = 0,
                             timeout: float = 10.0):
        """Create a new schedule (blocking).

        Args:
            schedule_id: Unique schedule identifier.
            name: Human-readable schedule name.
            schedule_type: "cron", "interval", or "once".
            schedule_expr: Cron expression, Go duration (e.g. "21600s"), or RFC3339 timestamp.
            action: Action definition dict (required if no workflow_id).
            workflow_id: Workflow ID to trigger (required if no action).
            workspace: Workspace scope (default "*").
            miss_policy: "skip", "fire_once", or "fire_all" (default "skip").
            max_concurrent: Max concurrent executions; 0=unlimited, 1=no overlap (default 0).
            timeout: RPC timeout in seconds.

        Returns:
            WorkflowResponse protobuf or None on timeout.
        """
        import json as _json
        data = {
            "id": schedule_id,
            "name": name,
            "schedule_type": schedule_type,
            "schedule_expr": schedule_expr,
            "workspace": workspace,
            "miss_policy": miss_policy,
            "max_concurrent": max_concurrent,
        }
        if action is not None:
            data["action"] = action
        if workflow_id:
            data["workflow_id"] = workflow_id

        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.CREATE_SCHEDULE,
            data=_json.dumps(data).encode(),
        )
        return self.workflow_op(op, timeout=timeout)

    def upsert_schedule_sync(self, schedule_id: str, name: str,
                             schedule_type: str, schedule_expr: str,
                             action: Optional[dict] = None,
                             workflow_id: str = "",
                             workspace: str = "*",
                             miss_policy: str = "skip",
                             max_concurrent: int = 0,
                             timeout: float = 10.0):
        """Create or update a schedule idempotently (blocking).

        Same parameters as create_schedule_sync. If a schedule with the given ID
        already exists, its configuration is updated but next_fire_at and
        last_fired_at are preserved.

        Returns:
            WorkflowResponse protobuf or None on timeout.
        """
        import json as _json
        data = {
            "id": schedule_id,
            "name": name,
            "schedule_type": schedule_type,
            "schedule_expr": schedule_expr,
            "workspace": workspace,
            "miss_policy": miss_policy,
            "max_concurrent": max_concurrent,
        }
        if action is not None:
            data["action"] = action
        if workflow_id:
            data["workflow_id"] = workflow_id

        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.UPSERT_SCHEDULE,
            data=_json.dumps(data).encode(),
        )
        return self.workflow_op(op, timeout=timeout)

    def delete_schedule_sync(self, schedule_id: str, timeout: float = 10.0):
        """Delete a schedule by ID (blocking).

        Returns:
            WorkflowResponse protobuf or None on timeout.
        """
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.DELETE_SCHEDULE,
            id=schedule_id,
        )
        return self.workflow_op(op, timeout=timeout)

    def list_schedules_sync(self, workspace: str = "*", timeout: float = 10.0):
        """List all schedules for a workspace (blocking).

        Returns:
            WorkflowResponse protobuf or None on timeout.
        """
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.LIST_SCHEDULES,
            workspace=workspace,
        )
        return self.workflow_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def wait_until_disconnected(self) -> None:
        """Block until the client disconnects (useful for keeping the connection alive)."""
        self._stop_event.wait()

    def close(self):
        """Close the connection."""
        self._stop_event.set()
        self.request_queue.put(None)  # Wake up generator
        if self._stream_thread:
            self._stream_thread.join(timeout=1.0)
        if self.channel:
            self.channel.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# =============================================================================
# Client Classes
#
# NOTE: Each subclass repeats the same 10 retry/TLS constructor parameters
# (max_retries, initial_backoff, max_backoff, auto_reconnect, tls_enabled,
# tls_root_cert, tls_root_cert_path, tls_client_cert, tls_client_cert_path,
# tls_client_key, tls_client_key_path) forwarded to BaseAetherClient.__init__.
# Consolidating these into a **kwargs pass-through would break explicit named-arg
# call sites and lose IDE type inference. A ClientConfig dataclass would be the
# correct fix but requires a public API change; deferred to a future major version.
# =============================================================================

class AgentClient(BaseAetherClient):
    """Client for agent connections to the Aether gateway."""

    def __init__(self, workspace: str, implementation: str, specifier: str,
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None,
                 extensions: Optional[List[aether_pb2.ExtensionDeclaration]] = None):
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.workspace = workspace
        self.implementation = implementation
        self.specifier = specifier
        self.init = create_agent_init(workspace, implementation, specifier, credentials,
                                      extensions=extensions)

    def connect(self, target: str = "localhost:50051"):
        super()._connect(self.init, target)

    def switch_workspace(self, new_workspace: str):
        """Switch to a different workspace."""
        self.workspace = new_workspace
        self._switch_workspace(new_workspace)

    def send_message_to_agent(self, workspace: str, implementation: str, specifier: str, payload: bytes,
                              message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_agent(workspace, implementation, specifier), payload,
                                  message_type=message_type)

    def send_message_to_task(self, workspace: str, implementation: str, payload: bytes, unique_specifier: str = "",
                             message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_task(workspace, implementation, unique_specifier), payload,
                                  message_type=message_type)

    def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                     message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_user(user_id, window_id), payload, message_type=message_type)

    def send_broadcast_to_agents(self, workspace: str, payload: bytes, message_type: int = aether_pb2.OPAQUE):
        """Send a broadcast message to all agents in a workspace."""
        return self._send_message(create_topic_global_agents(workspace), payload, message_type=message_type)

    def send_broadcast_to_users(self, workspace: str, payload: bytes, message_type: int = aether_pb2.OPAQUE):
        """Send a broadcast to all users in a workspace."""
        return self._send_message(create_topic_global_users(workspace), payload, message_type=message_type)

    def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                        message_type: int = aether_pb2.OPAQUE):
        """Send a message to a user's workspace-scoped topic."""
        return self._send_message(create_topic_user_workspace(user_id, workspace), payload, message_type=message_type)

    def send_event(self, payload: bytes):
        """Send an event to the workflow engine."""
        return self._send_message("event::*", payload, message_type=aether_pb2.EVENT)

    def send_metric(self, metric):
        """Send a metric to the metrics bridge.

        All entries are interpreted as additive deltas. Entries with negative
        qty values require the ``capability/metric_credit`` ACL permission on the
        sender.

        Args:
            metric: An ``aether_pb2.Metric`` instance, typically constructed
                via ``new_metric()`` from ``scitrera_aether_client.metrics``.

        Raises:
            TypeError: If ``metric`` is not an ``aether_pb2.Metric`` instance.
        """
        if not isinstance(metric, aether_pb2.Metric):
            raise TypeError(f"metric must be an aether_pb2.Metric instance, got {type(metric)!r}")
        buf = metric.SerializeToString()
        return self._send_message("metric::*", buf, message_type=aether_pb2.METRIC)


class TaskClient(BaseAetherClient):
    """Client for task connections to the Aether gateway."""

    def __init__(self, workspace: str, implementation: str, unique_specifier: str = "",
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None,
                 extensions: Optional[List[aether_pb2.ExtensionDeclaration]] = None):
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.workspace = workspace
        self.implementation = implementation
        self.unique_specifier = unique_specifier
        self.init = create_task_init(workspace, implementation, unique_specifier, credentials,
                                     extensions=extensions)

    def connect(self, target: str = "localhost:50051"):
        super()._connect(self.init, target)

    def switch_workspace(self, new_workspace: str):
        """Switch to a different workspace."""
        self.workspace = new_workspace
        self._switch_workspace(new_workspace)

    def send_message_to_agent(self, workspace: str, implementation: str, specifier: str, payload: bytes,
                              message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_agent(workspace, implementation, specifier), payload,
                                  message_type=message_type)

    def send_message_to_task(self, workspace: str, implementation: str, payload: bytes, unique_specifier: str = "",
                             message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_task(workspace, implementation, unique_specifier), payload,
                                  message_type=message_type)

    def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                     message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_user(user_id, window_id), payload, message_type=message_type)

    def send_event(self, payload: bytes):
        """Send an event to the workflow engine."""
        return self._send_message("event::*", payload, message_type=aether_pb2.EVENT)

    def send_metric(self, metric):
        """Send a metric to the metrics bridge.

        All entries are interpreted as additive deltas. Entries with negative
        qty values require the ``capability/metric_credit`` ACL permission on the
        sender.

        Args:
            metric: An ``aether_pb2.Metric`` instance, typically constructed
                via ``new_metric()`` from ``scitrera_aether_client.metrics``.

        Raises:
            TypeError: If ``metric`` is not an ``aether_pb2.Metric`` instance.
        """
        if not isinstance(metric, aether_pb2.Metric):
            raise TypeError(f"metric must be an aether_pb2.Metric instance, got {type(metric)!r}")
        buf = metric.SerializeToString()
        return self._send_message("metric::*", buf, message_type=aether_pb2.METRIC)


class UserClient(BaseAetherClient):
    """Client for user connections to the Aether gateway."""

    def __init__(self, user_id: str, window_id: str,
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None,
                 extensions: Optional[List[aether_pb2.ExtensionDeclaration]] = None):
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.user_id = user_id
        self.window_id = window_id
        self.init = create_user_init(user_id, window_id, credentials, extensions=extensions)

    def connect(self, target: str = "localhost:50051"):
        super()._connect(self.init, target)

    def send_message_to_agent(self, workspace: str, implementation: str, specifier: str, payload: bytes,
                              message_type: int = aether_pb2.OPAQUE,
                              app_workspace: str = ""):
        """Send a message to a specific agent.

        ``app_workspace`` is an optional hint carrying the user's active app
        workspace (e.g. ``"default"``). When set, the gateway stamps it into
        the task-authority grant's WorkspaceScope at triggerOrchestration time
        so the spawned agent can create resources in the user's workspace.
        """
        return self._send_message(create_topic_agent(workspace, implementation, specifier), payload,
                                  message_type=message_type, app_workspace=app_workspace)

    def send_message_to_task(self, workspace: str, implementation: str, payload: bytes, unique_specifier: str = "",
                             message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_task(workspace, implementation, unique_specifier), payload,
                                  message_type=message_type)

    def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                     message_type: int = aether_pb2.OPAQUE):
        return self._send_message(create_topic_user(user_id, window_id), payload, message_type=message_type)

    def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                        message_type: int = aether_pb2.OPAQUE):
        """Send a message to a user's workspace-scoped topic."""
        return self._send_message(create_topic_user_workspace(user_id, workspace), payload, message_type=message_type)

    def switch_workspace(self, new_workspace: str):
        """Declare the user's active app workspace to the gateway.

        The user identity (``us::<user_id>::<window_id>``) does not encode a
        workspace — it lives in session state on the gateway side. Without
        issuing this op after connect, ``client.Identity.Workspace`` remains
        empty for user sessions, which breaks any server-side logic that
        relies on ``sender.Workspace`` (e.g. task authority grant scoping
        in ``mintTaskGrantForSender``). Callers should call this right
        after ``connect()`` (and on every app-workspace change) to keep
        server-side session state in sync with the user's current workspace.
        """
        self._switch_workspace(new_workspace)


class OrchestratorClient(BaseAetherClient):
    """
    Client for orchestrator connections to the Aether gateway.

    Orchestrators are responsible for managing agent/task lifecycle:
    - Receiving startup requests when targeted agents are offline
    - Launching compute resources (containers, VMs, etc.)
    - Managing agent pools and scaling
    """

    def __init__(self, implementation: str,
                 supported_profiles: List[str],
                 specifier: Optional[str] = None,
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None,
                 extensions: Optional[List[aether_pb2.ExtensionDeclaration]] = None):
        """
        Create an orchestrator client.

        Args:
            implementation: Orchestrator implementation type (e.g., "kubernetes-orchestrator", "docker-orchestrator")
            supported_profiles: The profiles that this orchestrator can handle
            specifier: Unique specifier for this orchestrator instance (generated if not provided)
            credentials: Optional authentication credentials
            max_retries: Maximum connection retry attempts
            initial_backoff: Initial backoff delay in seconds
            max_backoff: Maximum backoff delay in seconds
            auto_reconnect: Whether to automatically reconnect on connection loss
            tls_enabled: Whether to use TLS for the connection
            tls_root_cert: Root CA certificate bytes for server verification
            tls_root_cert_path: Path to root CA certificate file
            tls_client_cert: Client certificate bytes for mTLS
            tls_client_cert_path: Path to client certificate file
            tls_client_key: Client private key bytes for mTLS
            tls_client_key_path: Path to client private key file
        """
        if not implementation:
            raise InvalidArgumentError(
                message="Orchestrator must have an implementation identifier",
                argument="implementation"
            )
        if not supported_profiles:
            raise InvalidArgumentError(
                message="An orchestrator must support at least one profile",
                argument="supported_profiles"
            )
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.implementation = implementation
        self.specifier = specifier or str(uuid.uuid4())[:8]  # Generate a specifier for this instance if not given
        self.supported_profiles = supported_profiles
        self.init = create_orchestrator_init(self.implementation, self.specifier,
                                             self.supported_profiles, credentials,
                                             extensions=extensions)

    def connect(self, target: str = "localhost:50051"):
        super()._connect(self.init, target)

    def send_status_to_agent(self, workspace: str, implementation: str, specifier: str,
                             payload: bytes, message_type: int = aether_pb2.CONTROL):
        """Send a status/control message to an agent."""
        return self._send_message(create_topic_agent(workspace, implementation, specifier), payload,
                                  message_type=message_type)

    def send_status_to_task(self, workspace: str, implementation: str, payload: bytes,
                            unique_specifier: str = "", message_type: int = aether_pb2.CONTROL):
        """Send a status/control message to a task."""
        return self._send_message(create_topic_task(workspace, implementation, unique_specifier), payload,
                                  message_type=message_type)


class WorkflowEngineClient(BaseAetherClient):
    """
    Client for workflow engine connections to the Aether gateway.

    The workflow engine is the sole subscriber to event.* topics and
    processes broadcast events to trigger downstream actions by sending
    commands to agents/tasks.
    """

    def __init__(self, credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None,
                 extensions: Optional[List[aether_pb2.ExtensionDeclaration]] = None):
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.init = create_workflow_engine_init(credentials, extensions=extensions)

    def connect(self, target: str = "localhost:50051"):
        super()._connect(self.init, target)

    def send_command_to_agent(self, workspace: str, implementation: str, specifier: str,
                              payload: bytes, message_type: int = aether_pb2.CONTROL):
        """Send a command to a specific agent."""
        return self._send_message(create_topic_agent(workspace, implementation, specifier), payload,
                                  message_type=message_type)

    def send_command_to_task(self, workspace: str, implementation: str, payload: bytes,
                             unique_specifier: str = "", message_type: int = aether_pb2.CONTROL):
        """Send a command to a specific task."""
        return self._send_message(create_topic_task(workspace, implementation, unique_specifier), payload,
                                  message_type=message_type)

    def send_broadcast_to_agents(self, workspace: str, payload: bytes, message_type: int = aether_pb2.CONTROL):
        """Send a broadcast to all agents in a workspace."""
        return self._send_message(create_topic_global_agents(workspace), payload, message_type=message_type)

    def send_broadcast_to_users(self, workspace: str, payload: bytes, message_type: int = aether_pb2.OPAQUE):
        """Send a broadcast to all users in a workspace."""
        return self._send_message(create_topic_global_users(workspace), payload, message_type=message_type)

    def send_message_to_user(self, user_id: str, window_id: str, payload: bytes,
                             message_type: int = aether_pb2.OPAQUE):
        """Send a message to a specific user session."""
        return self._send_message(create_topic_user(user_id, window_id), payload, message_type=message_type)

    def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                        message_type: int = aether_pb2.OPAQUE):
        """Send a message to a user's workspace-scoped topic."""
        return self._send_message(create_topic_user_workspace(user_id, workspace), payload, message_type=message_type)

    def send_metric(self, metric):
        """Send a metric to the metrics bridge.

        All entries are interpreted as additive deltas. Entries with negative
        qty values require the ``capability/metric_credit`` ACL permission on the
        sender.

        Args:
            metric: An ``aether_pb2.Metric`` instance, typically constructed
                via ``new_metric()`` from ``scitrera_aether_client.metrics``.

        Raises:
            TypeError: If ``metric`` is not an ``aether_pb2.Metric`` instance.
        """
        if not isinstance(metric, aether_pb2.Metric):
            raise TypeError(f"metric must be an aether_pb2.Metric instance, got {type(metric)!r}")
        buf = metric.SerializeToString()
        return self._send_message("metric::*", buf, message_type=aether_pb2.METRIC)


class MetricsBridgeClient(BaseAetherClient):
    """
    Client for metrics bridge connections to the Aether gateway.

    The metrics bridge is a receive-only client that subscribes to metric.*
    topics to collect telemetry data from agents and tasks.
    """

    def __init__(self, credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None,
                 extensions: Optional[List[aether_pb2.ExtensionDeclaration]] = None):
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.init = create_metrics_bridge_init(credentials, extensions=extensions)

    def connect(self, target: str = "localhost:50051"):
        super()._connect(self.init, target)

    # Metrics bridge is primarily receive-only but can acknowledge or respond
    def send_acknowledgment(self, target_topic: str, payload: bytes):
        """Send an acknowledgment to a source topic."""
        return self._send_message(target_topic, payload, message_type=aether_pb2.CONTROL)


class ServiceClient(BaseAetherClient):
    """Client for Service principal connections (trusted backend intermediaries).

    Service principals are workspace-less and are intended for app/websocket
    backends that authenticate as themselves and perform privileged work on
    behalf of users via ``AuthorizationContext`` (on-behalf-of mode).

    Canonical identity string: ``sv::{implementation}::{specifier}``.

    Typical usage::

        sv = ServiceClient(
            implementation='platform-server',
            specifier='pod-abc',
            credentials=Credentials.api_key(api_key),
        )
        sv.connect(target='aether:50151')
        # Exchange a user session into an on-behalf-of grant
        resp = sv.exchange_authority_grant(
            source_session_id=user_session_id,
            audience_type='session', audience_id=browser_sid,
            valid_while_audience_active=True,
        )
        auth = aether_pb2.AuthorizationContext(
            authority_mode='on_behalf_of',
            subject=aether_pb2.PrincipalRef(principal_type='user', principal_id='alice'),
            grant_id=resp.grant.grant_id,
        )
        # Then any op: sv.kv_get(...), sv.audit_query_sync(..., authorization=auth), etc.
    """

    def __init__(self, implementation: str, specifier: str,
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: bool = False,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None,
                 extensions: Optional[List[aether_pb2.ExtensionDeclaration]] = None):
        if not implementation:
            raise InvalidArgumentError(
                message="Service principal requires an implementation identifier",
                argument="implementation",
            )
        if not specifier:
            raise InvalidArgumentError(
                message="Service principal requires a specifier (instance id)",
                argument="specifier",
            )
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         **_env_tls_kwargs_filter(
                             tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                             tls_root_cert_path=tls_root_cert_path,
                             tls_client_cert=tls_client_cert,
                             tls_client_cert_path=tls_client_cert_path,
                             tls_client_key=tls_client_key,
                             tls_client_key_path=tls_client_key_path,
                         ))
        self.implementation = implementation
        self.specifier = specifier
        self.init = create_service_init(implementation, specifier, credentials,
                                        extensions=extensions)

    def connect(self, target: str = "localhost:50051"):
        super()._connect(self.init, target)

    # --- Message sending ----------------------------------------------------
    # Service principals are workspace-less; callers must supply the target
    # workspace explicitly. Any user-scoped operation should pass
    # ``authorization`` to invoke on-behalf-of semantics.

    def send_message_to_agent(self, workspace: str, implementation: str, specifier: str,
                              payload: bytes, message_type: int = aether_pb2.OPAQUE):
        """Send a message to a specific agent."""
        self._send_message(
            create_topic_agent(workspace, implementation, specifier),
            payload, message_type=message_type,
        )

    def send_message_to_task(self, workspace: str, implementation: str, payload: bytes,
                             unique_specifier: str = "", message_type: int = aether_pb2.OPAQUE):
        """Send a message to a specific task."""
        self._send_message(
            create_topic_task(workspace, implementation, unique_specifier),
            payload, message_type=message_type,
        )

    def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                     message_type: int = aether_pb2.OPAQUE):
        """Send a message to a specific user session."""
        self._send_message(
            create_topic_user(user_id, window_id),
            payload, message_type=message_type,
        )

    def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                       message_type: int = aether_pb2.OPAQUE):
        """Send a message to a user's workspace-scoped topic."""
        self._send_message(
            create_topic_user_workspace(user_id, workspace),
            payload, message_type=message_type,
        )
