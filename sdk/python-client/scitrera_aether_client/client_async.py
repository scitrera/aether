"""
Async variant of the Aether client using grpc.aio and asyncio.

This module provides async versions of all client types for use in asyncio applications.
"""
import asyncio
import logging
import random
import uuid
from dataclasses import dataclass
from typing import Callable, Optional, Dict, List, Awaitable, Union

import grpc
from grpc import aio as grpc_aio

logger = logging.getLogger("aether.client.async")

from ._common import (
    create_agent_init,
    create_task_init,
    create_user_init,
    create_orchestrator_init,
    create_workflow_engine_init,
    create_metrics_bridge_init,
    create_service_init,
    create_topic_agent,
    create_topic_service,
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
# Phase 2 Stage C authority-request helpers live in client.py (the sync side)
# so we re-import them here for the async surface. Avoid duplicating the
# proto-fiddling logic across both files.
from .client import make_authority_request_routing
# Phase 3 Stage C: hibernation helper — same re-import pattern.
from .client import make_hibernation_descriptor
from .exceptions import (
    AetherError,
    ConnectionError,
    ReconnectionError,
    InvalidArgumentError,
    from_grpc_error,
    is_recoverable_error,
)
from .client import AuditSubmitResponse, make_wait_spec
from .proto import aether_pb2
from .proto import aether_pb2_grpc

# Type alias for callbacks - can be sync or async
MessageCallback = Union[Callable[[aether_pb2.IncomingMessage], None],
Callable[[aether_pb2.IncomingMessage], Awaitable[None]]]
ConfigCallback = Union[Callable[[aether_pb2.ConfigSnapshot], None],
Callable[[aether_pb2.ConfigSnapshot], Awaitable[None]]]
SignalCallback = Union[Callable[[aether_pb2.Signal], None],
Callable[[aether_pb2.Signal], Awaitable[None]]]
ErrorCallback = Union[Callable[[aether_pb2.ErrorResponse], None],
Callable[[aether_pb2.ErrorResponse], Awaitable[None]]]
KVResponseCallback = Union[Callable[[aether_pb2.KVResponse], None],
Callable[[aether_pb2.KVResponse], Awaitable[None]]]
TaskAssignmentCallback = Union[Callable[[aether_pb2.TaskAssignment], None],
Callable[[aether_pb2.TaskAssignment], Awaitable[None]]]
CheckpointResponseCallback = Union[Callable[[aether_pb2.CheckpointResponse], None],
Callable[[aether_pb2.CheckpointResponse], Awaitable[None]]]
ProgressCallback = Union[Callable[[aether_pb2.ProgressUpdate], None],
Callable[[aether_pb2.ProgressUpdate], Awaitable[None]]]
ConnectCallback = Union[Callable[[], None], Callable[[], Awaitable[None]]]
DisconnectCallback = Union[Callable[[str], None], Callable[[str], Awaitable[None]]]


def _principal_ref(principal_type: str, principal_id: str) -> aether_pb2.PrincipalRef:
    return aether_pb2.PrincipalRef(
        principal_type=principal_type,
        principal_id=principal_id,
    )


@dataclass
class ResolvedIdentity:
    """The gateway identity this client connected with.

    ``principal_type`` is the Aether principal kind: ``"service"`` or
    ``"agent"`` (others may be added in future).

    ``actor_id`` is the canonical Aether identity string used for
    ``X-Auth-Actor-ID``:
    - service: ``sv::{implementation}::{specifier}``
    - agent:   ``ag::{workspace}::{implementation}::{specifier}``
    """

    principal_type: str
    actor_id: str
    implementation: str
    specifier: str
    workspace: str  # empty for service principals


# REST-form (lowercase) principal type → proto enum. Matches the workspace_handler
# convention on the server side. Empty/unknown maps to UNSPECIFIED so callers can
# leave a filter unset.
_PRINCIPAL_TYPE_FROM_STRING = {
    "agent": aether_pb2.PrincipalType.PRINCIPAL_AGENT,
    "task": aether_pb2.PrincipalType.PRINCIPAL_TASK,
    "user": aether_pb2.PrincipalType.PRINCIPAL_USER,
    "orchestrator": aether_pb2.PrincipalType.PRINCIPAL_ORCHESTRATOR,
    "workflow_engine": aether_pb2.PrincipalType.PRINCIPAL_WORKFLOW_ENGINE,
    "metrics_bridge": aether_pb2.PrincipalType.PRINCIPAL_METRICS_BRIDGE,
    "bridge": aether_pb2.PrincipalType.PRINCIPAL_BRIDGE,
    "service": aether_pb2.PrincipalType.PRINCIPAL_SERVICE,
}


def _principal_type_from_string(t: str) -> "aether_pb2.PrincipalType":
    return _PRINCIPAL_TYPE_FROM_STRING.get((t or "").lower(),
                                            aether_pb2.PrincipalType.PRINCIPAL_TYPE_UNSPECIFIED)


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


async def _maybe_await(func, *args):
    """Call a function and await it if it's a coroutine."""
    result = func(*args)
    if asyncio.iscoroutine(result):
        return await result
    return result


async def _safe_callback(fn, *args):
    """Run a callback in a task, catching exceptions to prevent unhandled task errors.

    Used by _listen_loop to dispatch message handlers concurrently so
    the loop can continue processing response messages (ACL, KV, etc.)
    without being blocked by long-running handlers.
    """
    try:
        result = fn(*args)
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            await result
    except Exception as e:
        logger.error("Callback error in %s: %s", getattr(fn, '__name__', fn), e, exc_info=True)


class BaseAsyncAetherClient:
    """Base class for all async Aether client types."""

    def __init__(self,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 backoff_multiplier: float = 2.0,
                 auto_reconnect: bool = True,
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
        """
        Initialize the base async client.

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
        self.channel: Optional[grpc_aio.Channel] = None
        self.stub: Optional[aether_pb2_grpc.AetherGatewayStub] = None
        self._request_queue: asyncio.Queue = asyncio.Queue()

        # TLS configuration
        tls = _env_tls_kwargs_filter(
            tls_enabled, tls_root_cert, tls_root_cert_path, tls_client_cert, tls_client_cert_path, tls_client_key, tls_client_key_path
        )
        self.tls_enabled = tls['tls_enabled']
        # Use the env-resolved bytes from the filter, NOT the raw constructor
        # args — when the caller relies on AETHER_TLS_* env vars, the
        # constructor args are None but the filter resolves them into bytes.
        # Storing the constructor args here would silently drop env-driven
        # TLS material and cause grpc to fall back to the system trust
        # store ("unable to get local issuer certificate" at handshake).
        self._tls_root_cert = tls['tls_root_cert']
        self._tls_root_cert_path = tls['tls_root_cert_path']
        self._tls_client_cert = tls['tls_client_cert']
        self._tls_client_cert_path = tls['tls_client_cert_path']
        self._tls_client_key = tls['tls_client_key']
        self._tls_client_key_path = tls['tls_client_key_path']

        # Retry configuration
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        self.auto_reconnect = auto_reconnect

        # Message callbacks (can be sync or async functions)
        self.on_message: Optional[MessageCallback] = None
        self.on_config: Optional[ConfigCallback] = None
        self.on_signal: Optional[SignalCallback] = None
        self.on_error: Optional[ErrorCallback] = None
        self.on_kv_response: Optional[KVResponseCallback] = None
        self.on_task_assignment: Optional[TaskAssignmentCallback] = None
        self.on_checkpoint_response: Optional[CheckpointResponseCallback] = None
        self.on_progress: Optional[ProgressCallback] = None
        self.on_connect: Optional[ConnectCallback] = None
        self.on_disconnect: Optional[DisconnectCallback] = None

        # Typed message callbacks (SDK routes by message_type)
        self.on_chat_message: Optional[MessageCallback] = None
        self.on_control_message: Optional[MessageCallback] = None
        self.on_tool_call: Optional[MessageCallback] = None
        self.on_event: Optional[MessageCallback] = None
        self.on_metric: Optional[MessageCallback] = None

        self._stop_event = asyncio.Event()
        self._listen_task: Optional[asyncio.Task] = None
        self._request_task: Optional[asyncio.Task] = None
        # Set by _request_generator's finally block when it exits. Used by
        # close()/_attempt_reconnect() to wait for the generator to drain
        # before tearing down the channel, so gRPC's hidden
        # _consume_request_iterator task can finish _done_writing() cleanly
        # instead of being cancelled mid-send_receive_close().
        self._request_generator_done: Optional[asyncio.Event] = None
        self._force_disconnect = False
        self._task_revoked = False
        self._reconnecting = False
        self._reconnect_attempt = 0
        self._connection_confirmed = False
        self._connection_generation: int = 0
        self._session_id: Optional[str] = None
        self._init_msg: Optional[aether_pb2.InitConnection] = None

        # Unified pending request registry: request_id -> Future
        # Used for all response types that support request_id correlation
        self._pending_requests: Dict[str, asyncio.Future] = {}

        # Fallback queues for when no request_id match (backward compat)
        self._kv_response_queue: asyncio.Queue = asyncio.Queue()
        self._checkpoint_response_queue: asyncio.Queue = asyncio.Queue()
        self._task_query_response_queue: asyncio.Queue = asyncio.Queue()
        self._task_op_response_queue: asyncio.Queue = asyncio.Queue()
        self._workspace_response_queue: asyncio.Queue = asyncio.Queue()
        self._agent_response_queue: asyncio.Queue = asyncio.Queue()
        self._session_response_queue: asyncio.Queue = asyncio.Queue()
        self._acl_response_queue: asyncio.Queue = asyncio.Queue()
        self._workflow_response_queue: asyncio.Queue = asyncio.Queue()
        self._authority_grant_response_queue: asyncio.Queue = asyncio.Queue()

        # Per-operation-type locks for ops without request_id (workspace, agent, session, ACL)
        self._workspace_op_lock = asyncio.Lock()
        self._agent_op_lock = asyncio.Lock()
        self._session_op_lock = asyncio.Lock()
        self._acl_op_lock = asyncio.Lock()

        # Workspace/agent/ACL/workflow response callbacks
        self.on_workspace_response = None
        self.on_agent_response = None
        self.on_acl_response = None
        self.on_workflow_response = None
        self.on_token_response = None
        self.on_authority_grant_response = None
        # Push-event callback for AuthorityGrantRevocation messages — the
        # gateway sends one of these when a grant the connected delegate
        # holds (or one of its parents in a delegation chain) is revoked.
        # The installed AuthorityGrantCache (if any) is notified first.
        self.on_authority_grant_revocation = None
        # Optional AuthorityGrantCache — wired by make_authority_cache().
        self._authority_grant_cache = None

        # Stream reference for bidirectional communication
        self._stream: Optional[grpc_aio.StreamStreamCall] = None

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set() or self._reconnecting

    @property
    def identity(self) -> Optional["ResolvedIdentity"]:
        """Return the resolved gateway identity for this connection, or ``None``.

        Populated after ``_do_connect`` receives the ``InitConnection``
        acknowledgement (i.e., once ``_init_msg`` is set). Returns ``None``
        before the first successful connection or for principal types that
        do not carry a structured identity (user, task, orchestrator, etc.).
        """
        init_msg = self._init_msg
        if init_msg is None:
            return None
        kind = init_msg.WhichOneof("client_type")
        if kind == "service":
            svc = init_msg.service
            return ResolvedIdentity(
                principal_type="service",
                actor_id=f"sv::{svc.implementation}::{svc.specifier}",
                implementation=svc.implementation,
                specifier=svc.specifier,
                workspace="",
            )
        if kind == "agent":
            ag = init_msg.agent
            return ResolvedIdentity(
                principal_type="agent",
                actor_id=f"ag::{ag.workspace}::{ag.implementation}::{ag.specifier}",
                implementation=ag.implementation,
                specifier=ag.specifier,
                workspace=ag.workspace,
            )
        return None

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate backoff delay with jitter.

        First attempt is zero-delay: in deployments where the gateway
        rotates connections (e.g. periodic freshness cuts every ~2h), a
        full second of wait before reconnecting is pure dead time —
        nothing has gone wrong, the previous stream just ended cleanly.
        Subsequent attempts use the configured exponential backoff so a
        genuinely-broken endpoint still gets the usual rate limiting.
        """
        if attempt == 0:
            return 0.0
        delay = min(self.initial_backoff * (self.backoff_multiplier ** attempt), self.max_backoff)
        # Add jitter (+/- 25%)
        jitter = delay * 0.25 * (random.random() * 2 - 1)
        return delay + jitter

    def _is_recoverable_error(self, error: Union[grpc.RpcError, AetherError]) -> bool:
        """Check if an error is recoverable (should trigger reconnection)."""
        return is_recoverable_error(error)

    @property
    def task_revoked(self) -> bool:
        """True if the gateway rejected reconnection because the per-task token
        was revoked. Workers should treat their associated work as permanently
        dead when this flips True. Set by the reconnect loop on PERMISSION_DENIED
        / UNAUTHENTICATED responses; reset only on a fresh connect()."""
        return self._task_revoked

    async def _on_error(self, error: Union[grpc.RpcError, aether_pb2.ErrorResponse, AetherError],
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
            await _maybe_await(self.on_error, error)
        else:
            logger.error("Error: %s - %s", error.code, error.message)

        return aether_error

    async def _request_generator(self):
        """Async generator that yields requests from the queue.

        Tracks connection generation so stale generators from previous
        connections exit cleanly instead of interfering with new ones.

        Swallows ``asyncio.CancelledError``: gRPC's
        ``_consume_request_iterator`` calls ``aclose()`` on the iterator
        during stream teardown, which cancels any pending ``wait_for`` on
        the queue. That's a normal shutdown signal, not an error — the
        traceback bubbling up was just noise.

        Sets ``_request_generator_done`` in the ``finally`` block so
        ``close()``/``_attempt_reconnect()`` can wait for this generator
        to drain before tearing down the channel.
        """
        gen = self._connection_generation
        done_event = self._request_generator_done
        try:
            while not self._stop_event.is_set() and self._connection_generation == gen:
                try:
                    # Use wait_for to allow checking stop_event periodically
                    request = await asyncio.wait_for(self._request_queue.get(), timeout=0.1)
                    if request is None:  # Sentinel for closing
                        break
                    yield request
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            # Graceful close (close()/reconnect/switch_workspace) — exit
            # without re-raising so gRPC's iterator-consumer doesn't log
            # a misleading "Client request_iterator raised exception".
            return
        finally:
            # Signal close()/reconnect that the iterator is exiting so
            # they can sequence channel teardown after gRPC has finished
            # its half-close.
            if done_event is not None and self._connection_generation == gen:
                done_event.set()

    async def _listen_loop(self):
        """Listen for responses from the server."""
        should_reconnect = False
        try:
            async for response in self._stream:
                if self._stop_event.is_set():
                    break

                # Connection confirmed when we receive first response from server
                if not self._connection_confirmed:
                    self._connection_confirmed = True
                    if self._reconnecting:
                        logger.info("Reconnected to %s", self.target)
                    else:
                        logger.info("Connected to %s", self.target)
                    self._reconnect_attempt = 0
                    if self.on_connect:
                        await _maybe_await(self.on_connect)

                payload_type = response.WhichOneof("payload")
                if payload_type == "msg":
                    msg = response.msg

                    # Route to typed callback if registered.
                    # Handlers are dispatched as tasks so the listen loop can
                    # continue processing response messages (ACL, KV, checkpoint)
                    # without being blocked by long-running message handlers.
                    if msg.message_type == aether_pb2.CHAT and self.on_chat_message:
                        asyncio.create_task(_safe_callback(self.on_chat_message, msg))
                    elif msg.message_type == aether_pb2.CONTROL and self.on_control_message:
                        asyncio.create_task(_safe_callback(self.on_control_message, msg))
                    elif msg.message_type == aether_pb2.TOOL_CALL and self.on_tool_call:
                        asyncio.create_task(_safe_callback(self.on_tool_call, msg))
                    elif msg.message_type == aether_pb2.EVENT and self.on_event:
                        asyncio.create_task(_safe_callback(self.on_event, msg))
                    elif msg.message_type == aether_pb2.METRIC and self.on_metric:
                        asyncio.create_task(_safe_callback(self.on_metric, msg))

                    # Always call catch-all if registered
                    if self.on_message:
                        asyncio.create_task(_safe_callback(self.on_message, msg))
                elif payload_type == "config":
                    if self.on_config:
                        await _maybe_await(self.on_config, response.config)
                elif payload_type == "signal":
                    # Handle FORCE_DISCONNECT signal
                    if response.signal.type == aether_pb2.Signal.FORCE_DISCONNECT:
                        logger.warning("Received FORCE_DISCONNECT: %s", response.signal.reason)
                        self._force_disconnect = True
                        self._stop_event.set()
                        if self.on_disconnect:
                            await _maybe_await(self.on_disconnect, response.signal.reason)
                        break
                    elif response.signal.type == aether_pb2.Signal.GRACEFUL_DISCONNECT:
                        logger.info("Received GRACEFUL_DISCONNECT: %s", response.signal.reason)
                        # Do not set _force_disconnect — allow auto-reconnect.
                        # Do NOT set _stop_event either — the post-loop reconnect
                        # dispatcher (line ~547) gates on `not _stop_event.is_set()`,
                        # so setting it would silently disable reconnection. Instead
                        # mark should_reconnect=True and break, mirroring the
                        # transport-error path.
                        if self.on_disconnect:
                            await _maybe_await(self.on_disconnect, response.signal.reason)
                        if self.auto_reconnect and not self._force_disconnect:
                            should_reconnect = True
                        break
                    if self.on_signal:
                        await _maybe_await(self.on_signal, response.signal)
                elif payload_type == "error":
                    err = response.error
                    req_id = err.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending is not None and not pending.done():
                        from scitrera_aether_client.exceptions import error_response_to_aether_error
                        pending.set_exception(error_response_to_aether_error(err))
                    else:
                        await self._on_error(err)
                elif payload_type == "kv":
                    resp = response.kv
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    else:
                        await self._kv_response_queue.put(resp)
                        if self.on_kv_response:
                            await _maybe_await(self.on_kv_response, resp)
                elif payload_type == "task_assignment":
                    if self.on_task_assignment:
                        await _maybe_await(self.on_task_assignment, response.task_assignment)
                elif payload_type == "checkpoint":
                    resp = response.checkpoint
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    else:
                        await self._checkpoint_response_queue.put(resp)
                        if self.on_checkpoint_response:
                            await _maybe_await(self.on_checkpoint_response, resp)
                elif payload_type == "task_query":
                    resp = response.task_query
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    else:
                        await self._task_query_response_queue.put(resp)
                elif payload_type == "task_op":
                    resp = response.task_op
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    else:
                        await self._task_op_response_queue.put(resp)
                elif payload_type == "workspace":
                    await self._workspace_response_queue.put(response.workspace)
                    if self.on_workspace_response:
                        await _maybe_await(self.on_workspace_response, response.workspace)
                elif payload_type == "agent":
                    await self._agent_response_queue.put(response.agent)
                    if self.on_agent_response:
                        await _maybe_await(self.on_agent_response, response.agent)
                elif payload_type == "session_response":
                    resp = response.session_response
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    else:
                        await self._session_response_queue.put(resp)
                elif payload_type == "acl":
                    await self._acl_response_queue.put(response.acl)
                    if self.on_acl_response:
                        await _maybe_await(self.on_acl_response, response.acl)
                elif payload_type == "workflow_response":
                    resp = response.workflow_response
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    else:
                        await self._workflow_response_queue.put(resp)
                        if self.on_workflow_response:
                            await _maybe_await(self.on_workflow_response, resp)
                elif payload_type == "token":
                    resp = response.token
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    elif self.on_token_response:
                        await _maybe_await(self.on_token_response, resp)
                elif payload_type == "authority_grant":
                    resp = response.authority_grant
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                    else:
                        await self._authority_grant_response_queue.put(resp)
                        if self.on_authority_grant_response:
                            await _maybe_await(self.on_authority_grant_response, resp)
                elif payload_type == "audit_response":
                    resp = response.audit_response
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                elif payload_type == "resolve_authority_response":
                    resp = response.resolve_authority_response
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                elif payload_type == "connection_status_response":
                    resp = response.connection_status_response
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                elif payload_type == "create_task":
                    resp = response.create_task
                    req_id = resp.request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                elif payload_type == "submit_audit_event_response":
                    resp = response.submit_audit_event_response
                    req_id = resp.client_request_id
                    pending = self._pending_requests.pop(req_id, None) if req_id else None
                    if pending and not pending.done():
                        pending.set_result(resp)
                elif payload_type == "progress_update":
                    if self.on_progress:
                        await _maybe_await(self.on_progress, response.progress_update)
                elif payload_type == "proxy_http_request":
                    # Dispatch directly here rather than relying on the
                    # ``proxy_terminator``/``proxy`` modules' stream-filter
                    # monkey-patch.  That patch wraps ``self._stream`` only on
                    # ``_do_connect`` calls that happen AFTER the patch is
                    # installed; consumers that import the terminator module
                    # only after ``connect()`` (e.g. via a delayed import in
                    # an async lifecycle hook) end up iterating the unwrapped
                    # stream and silently drop ``proxy_http_*`` frames.
                    # Looking the dispatcher up here is import-ordering-
                    # independent: ``ProxyHttpTerminator.__init__`` always
                    # sets ``client._proxy_terminator_dispatcher`` regardless
                    # of when the connection was opened.
                    dispatcher = getattr(self, "_proxy_terminator_dispatcher", None)
                    if dispatcher is not None and getattr(dispatcher, "_terminators", None):
                        try:
                            await dispatcher.handle_request(response.proxy_http_request)
                        except Exception:  # noqa: BLE001
                            logger.exception("proxy_http_request dispatch failed")
                elif payload_type == "proxy_http_body_chunk":
                    chunk = response.proxy_http_body_chunk
                    if chunk.is_request:
                        # Terminator-bound (request-direction) chunk.
                        dispatcher = getattr(self, "_proxy_terminator_dispatcher", None)
                        if dispatcher is not None and getattr(dispatcher, "_terminators", None):
                            try:
                                await dispatcher.handle_body_chunk(chunk)
                            except Exception:  # noqa: BLE001
                                logger.exception("proxy_http_body_chunk (request) dispatch failed")
                    else:
                        # Initiator-bound (response-direction) chunk.
                        dispatcher = getattr(self, "_proxy_dispatcher", None)
                        if dispatcher is not None:
                            try:
                                dispatcher.handle_body_chunk(chunk)
                            except Exception:  # noqa: BLE001
                                logger.exception("proxy_http_body_chunk (response) dispatch failed")
                elif payload_type == "proxy_http_response":
                    dispatcher = getattr(self, "_proxy_dispatcher", None)
                    if dispatcher is not None:
                        try:
                            dispatcher.handle_response(response.proxy_http_response)
                        except Exception:  # noqa: BLE001
                            logger.exception("proxy_http_response dispatch failed")
                elif payload_type == "connection_ack":
                    self._session_id = response.connection_ack.session_id
                    if response.connection_ack.resumed:
                        logger.info("Session resumed (session_id=%s...)", self._session_id[:8])
                elif payload_type == "authority_grant_revocation":
                    # Push event: gateway is telling us a grant we hold (or
                    # one of its parents) was revoked. Notify any installed
                    # AuthorityGrantCache plus the optional callback.
                    evt = response.authority_grant_revocation
                    cache = getattr(self, "_authority_grant_cache", None)
                    if cache is not None:
                        try:
                            cache.handle_revocation_event(evt)
                        except Exception:  # noqa: BLE001
                            logger.exception("AuthorityGrantCache revocation handling failed")
                    if self.on_authority_grant_revocation:
                        await _maybe_await(self.on_authority_grant_revocation, evt)

        except grpc.RpcError as e:
            if not self._stop_event.is_set():
                await self._on_error(e, from_listen_loop=True)
                if self.auto_reconnect and self._is_recoverable_error(e) and not self._force_disconnect:
                    should_reconnect = True
        finally:
            # Don't fire disconnect handler if we're about to reconnect —
            # the new connection will fire on_connect when it's established.
            if self.on_disconnect and not self._force_disconnect and not should_reconnect:
                await _maybe_await(self.on_disconnect, "connection lost")

        # Trigger reconnection if needed
        if should_reconnect and not self._stop_event.is_set():
            await self._attempt_reconnect()

    async def _attempt_reconnect(self):
        """
        Attempt to reconnect with exponential backoff.

        This method manages reconnection attempts internally. If the maximum
        number of retries is exceeded, it logs a message, sets the internal
        stop event, and returns without raising an exception.
        """
        if self._reconnecting:
            return

        self._reconnecting = True

        try:
            while not self._force_disconnect:
                # Check max retries
                if self.max_retries > 0 and self._reconnect_attempt >= self.max_retries:
                    logger.error("Max reconnection attempts (%d) exceeded, giving up", self.max_retries)
                    self._stop_event.set()
                    # Don't raise here - this is called from listen loop context
                    return

                backoff = self._calculate_backoff(self._reconnect_attempt)
                logger.info("Reconnecting in %.1fs (attempt %d)...", backoff, self._reconnect_attempt + 1)

                self._reconnect_attempt += 1

                # Wait for backoff period
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    return

                if self._force_disconnect:
                    return

                try:
                    # Signal old tasks to stop
                    self._stop_event.set()

                    # Send sentinel to stop old request generator cleanly
                    try:
                        self._request_queue.put_nowait(None)
                    except asyncio.QueueFull:
                        pass

                    # Wait for the old request generator to drain so gRPC
                    # can finish _done_writing() before we close the
                    # channel — same rationale as close(). Best-effort
                    # with a tight timeout because reconnect should be
                    # responsive.
                    if self._request_generator_done is not None:
                        try:
                            await asyncio.wait_for(
                                self._request_generator_done.wait(), timeout=1.0
                            )
                        except asyncio.TimeoutError:
                            pass
                        await asyncio.sleep(0)

                    # Cancel and await old listen task to prevent two listeners
                    if self._listen_task and not self._listen_task.done():
                        self._listen_task.cancel()
                        try:
                            await self._listen_task
                        except asyncio.CancelledError:
                            pass
                    self._listen_task = None

                    # Close old channel
                    if self.channel:
                        try:
                            await self.channel.close()
                        except Exception:
                            pass

                    # Bump connection generation so any stale generators exit
                    self._connection_generation += 1

                    # Reset state for new connection
                    self._stop_event.clear()
                    self._force_disconnect = False
                    self._connection_confirmed = False
                    self._request_queue = asyncio.Queue()
                    self._kv_response_queue = asyncio.Queue()
                    self._checkpoint_response_queue = asyncio.Queue()
                    self._task_query_response_queue = asyncio.Queue()
                    self._task_op_response_queue = asyncio.Queue()
                    self._workspace_response_queue = asyncio.Queue()
                    self._agent_response_queue = asyncio.Queue()
                    self._session_response_queue = asyncio.Queue()
                    self._acl_response_queue = asyncio.Queue()
                    self._workflow_response_queue = asyncio.Queue()
                    self._pending_requests = {}

                    # Attempt reconnection
                    await self._do_connect(self._init_msg, self.target)
                    return

                except grpc.RpcError as e:
                    code = e.code()
                    # Auth failure on reconnect signals a revoked per-task token,
                    # which means the associated task transitioned to terminal
                    # while we were away (e.g. disconnect reaper fired). No
                    # amount of retry will fix this — surface it so the worker
                    # can clean up.
                    if code in (grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAUTHENTICATED):
                        logger.error("Reconnect rejected (auth): task token revoked — surfacing TaskRevokedError")
                        self._task_revoked = True
                        self._force_disconnect = True
                        self._stop_event.set()
                        return
                    if not self._is_recoverable_error(e):
                        logger.error("Non-recoverable error during reconnect: %s", code.name if code else 'UNKNOWN')
                        self._stop_event.set()
                        return
                except Exception as e:
                    logger.error("Reconnection error: %s", e)
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

    async def _do_connect(self, init_msg: aether_pb2.InitConnection, target: str):
        """Internal method to establish connection (no retry logic)."""
        if self.tls_enabled:
            credentials = self._build_tls_credentials()
            self.channel = grpc_aio.secure_channel(target, credentials)
        else:
            self.channel = grpc_aio.insecure_channel(target)
        self.stub = aether_pb2_grpc.AetherGatewayStub(self.channel)

        # Fresh event for this connection's request generator. Must be
        # created before _request_generator() is invoked so the generator
        # captures the right event for its current generation.
        self._request_generator_done = asyncio.Event()

        # Create the bidirectional stream
        self._stream = self.stub.Connect(self._request_generator())

        # Send init message (with session ID if resuming)
        if self._session_id:
            init_with_resume = aether_pb2.InitConnection()
            init_with_resume.CopyFrom(init_msg)
            init_with_resume.resume_session_id = self._session_id
            await self._request_queue.put(aether_pb2.UpstreamMessage(init=init_with_resume))
        else:
            await self._request_queue.put(aether_pb2.UpstreamMessage(init=init_msg))

        # Start listen task
        self._listen_task = asyncio.create_task(self._listen_loop())

    async def _connect(self, init_msg: aether_pb2.InitConnection, target: str = "localhost:50051"):
        """Initializes the connection with retry logic."""
        self.target = target
        self._init_msg = init_msg
        self._stop_event.clear()
        self._force_disconnect = False
        self._task_revoked = False
        self._reconnecting = False
        self._reconnect_attempt = 0
        self._connection_confirmed = False
        self._request_queue = asyncio.Queue()
        self._kv_response_queue = asyncio.Queue()
        self._checkpoint_response_queue = asyncio.Queue()
        self._task_query_response_queue = asyncio.Queue()
        self._task_op_response_queue = asyncio.Queue()
        self._workspace_response_queue = asyncio.Queue()
        self._agent_response_queue = asyncio.Queue()
        self._session_response_queue = asyncio.Queue()
        self._acl_response_queue = asyncio.Queue()
        self._workflow_response_queue = asyncio.Queue()
        self._pending_requests = {}

        attempt = 0
        last_error = None

        while attempt < self.max_retries or self.max_retries == 0:
            try:
                await self._do_connect(init_msg, target)
                # The gRPC channel is opened lazily — secure_channel returns
                # before the TCP/TLS handshake completes. Don't claim
                # "Connected" here; the listen loop will set
                # _connection_confirmed once the gateway acks.
                logger.info("Connecting to %s (channel opened, awaiting handshake/ack)", target)
                return

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
                await asyncio.sleep(backoff)

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
                await asyncio.sleep(backoff)

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

    async def _send_sync_op(self, message: aether_pb2.UpstreamMessage, request_id: str,
                            timeout: float = 10.0):
        """
        Send an upstream message and wait for the correlated response.

        Uses the unified _pending_requests registry to route the response
        back to this caller by request_id, making concurrent async ops safe.

        Args:
            message: The UpstreamMessage to send
            request_id: The request_id set on the inner operation proto
            timeout: Timeout in seconds

        Returns:
            The response proto, or None on timeout
        """
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_requests[request_id] = fut
        try:
            await self._request_queue.put(message)
            try:
                return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            except asyncio.TimeoutError:
                return None
        finally:
            self._pending_requests.pop(request_id, None)

    async def _send_message(self, target_topic: str, payload: bytes,
                            message_type: int = aether_pb2.OPAQUE,
                            authorization: Optional[aether_pb2.AuthorizationContext] = None,
                            app_workspace: str = ""):
        """Send a message to a target topic.

        If ``authorization`` is provided, the message is authorized against the
        subject's ACL via the referenced authority grant (on-behalf-of mode).

        ``app_workspace`` is an optional hint carrying the user's active app
        workspace (e.g. ``"default"``). When set, the gateway includes it in
        the task-authority grant's WorkspaceScope at triggerOrchestration time
        so spawned agents can create resources in the user's workspace. Ignored
        for non-user principals.
        """
        msg = aether_pb2.SendMessage(
            target_topic=target_topic,
            payload=payload,
            message_type=message_type,  # type: ignore[arg-type]
            authorization=authorization,
            app_workspace=app_workspace,
        )
        await self._request_queue.put(aether_pb2.UpstreamMessage(send=msg))

    async def _switch_workspace(self, new_workspace_id: str):
        """Switch to a different workspace."""
        sw = aether_pb2.SwitchWorkspace(new_workspace_id=new_workspace_id)
        await self._request_queue.put(aether_pb2.UpstreamMessage(switch_workspace=sw))

    # =========================================================================
    # KV Operations with full scope support
    # =========================================================================

    # in principle, these (commented out) functions aren't useful because any caller would want the response
    # --------------------------------------------------------------------------------
    # async def kv_get(self, key: str, scope: str = "global",
    #                  user_id: str = "", workspace: str = "") -> None:
    #
    # async def kv_list(self, key_prefix: str = "", scope: str = "global",
    #                       user_id: str = "", workspace: str = "") -> None:

    async def kv_put_nowait(self, key: str, value: bytes, scope: str = "global",
                            user_id: str = "", workspace: str = "", ttl: int = 0,
                            authorization: Optional[aether_pb2.AuthorizationContext] = None) -> None:
        """
        Put a value in the KV store. Does not wait for a response from the server before returning.

        Args:
            key: The key to store
            value: The value to store (bytes)
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            ttl: Time-to-live in seconds (0 = no expiration)
            authorization: Optional on-behalf-of authorization context
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.PUT,
            scope=_scope_to_proto(scope),
            key=key,
            value=value,
            user_id=user_id,
            workspace=workspace,
            ttl=ttl,
            authorization=authorization,
        )
        await self._request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    async def kv_delete_nowait(self, key: str, scope: str = "global",
                               user_id: str = "", workspace: str = "",
                               authorization: Optional[aether_pb2.AuthorizationContext] = None) -> None:
        """
        Delete a key from the KV store. Does not wait for a response from the server before returning.

        Args:
            key: The key to delete
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            authorization: Optional on-behalf-of authorization context
        """
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.DELETE,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace,
            authorization=authorization,
        )
        await self._request_queue.put(aether_pb2.UpstreamMessage(kv_op=kv))

    async def kv_get(self, key: str, scope: str = "global",
                     user_id: str = "", workspace: str = "",
                     timeout: float = 5.0,
                     authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[aether_pb2.KVResponse]:
        """
        Get a value from the KV store and wait for the response.

        Args:
            key: The key to retrieve
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            KVResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.GET,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace,
            request_id=request_id,
            authorization=authorization,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )

    async def kv_put(self, key: str, value: bytes, scope: str = "global",
                     user_id: str = "", workspace: str = "", ttl: int = 0,
                     timeout: float = 5.0,
                     authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[aether_pb2.KVResponse]:
        """
        Put a value in the KV store and wait for the response.

        Args:
            key: The key to store
            value: The value to store (bytes)
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            ttl: Time-to-live in seconds (0 = no expiration)
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            KVResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.PUT,
            scope=_scope_to_proto(scope),
            key=key,
            value=value,
            user_id=user_id,
            workspace=workspace,
            ttl=ttl,
            request_id=request_id,
            authorization=authorization,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )

    async def kv_list(self, key_prefix: str = "", scope: str = "global",
                      user_id: str = "", workspace: str = "",
                      timeout: float = 5.0,
                      authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[aether_pb2.KVResponse]:
        """
        List keys from the KV store and wait for the response.

        Args:
            key_prefix: Prefix to filter keys (empty for all)
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            KVResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.LIST,
            scope=_scope_to_proto(scope),
            key=key_prefix,
            user_id=user_id,
            workspace=workspace,
            request_id=request_id,
            authorization=authorization,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )

    async def kv_delete(self, key: str, scope: str = "global",
                        user_id: str = "", workspace: str = "",
                        timeout: float = 5.0,
                        authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[aether_pb2.KVResponse]:
        """
        Delete a key from the KV store and wait for the response.

        Args:
            key: The key to delete
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            KVResponse or None if timeout
        """
        request_id = str(uuid.uuid4())
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.DELETE,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace,
            request_id=request_id,
            authorization=authorization,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )

    async def kv_increment(self, key: str, scope: str = "global",
                           user_id: str = "", workspace: str = "",
                           ttl: int = 0,
                           timeout: float = 5.0,
                           authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[aether_pb2.KVResponse]:
        """
        Atomically increment a counter in the KV store and wait for the response.

        Args:
            key: The key to increment
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            ttl: Time-to-live in seconds applied on first increment (0 = no expiration)
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            KVResponse with counter_value set, or None if timeout
        """
        request_id = str(uuid.uuid4())
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.INCREMENT,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace,
            ttl=ttl,
            request_id=request_id,
            authorization=authorization,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )

    async def kv_decrement(self, key: str, scope: str = "global",
                           user_id: str = "", workspace: str = "",
                           timeout: float = 5.0,
                           authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[aether_pb2.KVResponse]:
        """
        Atomically decrement a counter in the KV store and wait for the response.

        Args:
            key: The key to decrement
            scope: One of "global", "workspace", "user", "user-workspace"
            user_id: Required for "user" and "user-workspace" scopes
            workspace: Required for "workspace" and "user-workspace" scopes
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            KVResponse with counter_value set, or None if timeout
        """
        request_id = str(uuid.uuid4())
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.DECREMENT,
            scope=_scope_to_proto(scope),
            key=key,
            user_id=user_id,
            workspace=workspace,
            request_id=request_id,
            authorization=authorization,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )

    async def kv_increment_if(self, key: str, delta: int = 1, ceiling: int = 0,
                              scope: str = "global",
                              user_id: str = "", workspace: str = "",
                              ttl: int = 0,
                              timeout: float = 5.0,
                              authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[tuple]:
        """
        Atomically increment a counter only if the result would not exceed ceiling.

        Args:
            key: The key to increment
            delta: Amount to increment by
            ceiling: Maximum allowed value (guard); 0 means no guard
            scope: KV scope string
            user_id: Required for user-scoped operations
            workspace: Required for workspace-scoped operations
            ttl: Time-to-live in seconds on first write (0 = no expiration)
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            (counter_value: int, applied: bool) tuple, or None if timeout
        """
        request_id = str(uuid.uuid4())
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
            request_id=request_id,
            authorization=authorization,
        )
        resp = await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )
        if resp is None:
            return None
        return (resp.counter_value, resp.applied)

    async def kv_decrement_if(self, key: str, delta: int = 1, floor: int = 0,
                              scope: str = "global",
                              user_id: str = "", workspace: str = "",
                              timeout: float = 5.0,
                              authorization: Optional[aether_pb2.AuthorizationContext] = None) -> Optional[tuple]:
        """
        Atomically decrement a counter only if the result would not go below floor.

        Args:
            key: The key to decrement
            delta: Amount to decrement by
            floor: Minimum allowed value (guard)
            scope: KV scope string
            user_id: Required for user-scoped operations
            workspace: Required for workspace-scoped operations
            timeout: Timeout in seconds
            authorization: Optional on-behalf-of authorization context

        Returns:
            (counter_value: int, applied: bool) tuple, or None if timeout
        """
        request_id = str(uuid.uuid4())
        kv = aether_pb2.KVOperation(
            op=aether_pb2.KVOperation.DECREMENT_IF,
            scope=_scope_to_proto(scope),
            key=key,
            int_value=delta,
            delta_value=int(delta),
            guard_value=floor,
            user_id=user_id,
            workspace=workspace,
            request_id=request_id,
            authorization=authorization,
        )
        resp = await self._send_sync_op(
            aether_pb2.UpstreamMessage(kv_op=kv),
            request_id, timeout,
        )
        if resp is None:
            return None
        return (resp.counter_value, resp.applied)

    # =========================================================================
    # Task Creation (Phase 6)
    # =========================================================================

    async def create_task(self, task_type: str, workspace: str,
                          target_agent_id: str = "",
                          target_implementation: str = "",
                          launch_param_overrides: Optional[Dict[str, str]] = None,
                          metadata: Optional[Dict[str, str]] = None,
                          payload: Optional[bytes] = None,
                          assignment_mode: int = SELF_ASSIGN,
                          authorization: Optional[aether_pb2.AuthorizationContext] = None,
                          task_class: int = 0,
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
            authorization: Optional on-behalf-of authorization context. When present,
                the created task is associated with the subject's authority and
                automatic child grants are minted for assigned workers.
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
            authorization=authorization,
            task_class=task_class,  # type: ignore[arg-type]
            context_id=context_id,
        )
        await self._request_queue.put(aether_pb2.UpstreamMessage(create_task=req))

    async def create_task_sync(self, task_type: str, workspace: str,
                               target_agent_id: str = "",
                               target_implementation: str = "",
                               launch_param_overrides: Optional[Dict[str, str]] = None,
                               metadata: Optional[Dict[str, str]] = None,
                               payload: Optional[bytes] = None,
                               assignment_mode: int = SELF_ASSIGN,
                               authorization: Optional[aether_pb2.AuthorizationContext] = None,
                               target_identity: str = "",
                               task_class: int = 0,
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
            authorization: Optional on-behalf-of authorization context
            target_identity: Optional canonical identity string of the worker the
                caller will spawn for this task (e.g., "ag::ws::impl::spec"). When
                set AND the gateway's issue-token ACL gate passes, the response's
                ``task_token`` carries a fresh per-task auth token the worker can
                present at connection init. Empty string disables token issuance.
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
            target_identity=target_identity,
            task_class=task_class,  # type: ignore[arg-type]
            context_id=context_id,
            request_id=request_id,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(create_task=req), request_id, timeout,
        )

    # =========================================================================
    # Progress Reporting
    # =========================================================================

    async def report_progress(self, task_id: str, state: str = "running",
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
            kind: ProgressKind enum value classifying the UI surface/consumer
                (0=UNSPECIFIED, 1=CHAT, 2=APP, 3=TASK). See aether.proto.
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
        await self._request_queue.put(aether_pb2.UpstreamMessage(progress=report))

    # =========================================================================
    # Checkpoint Operations
    # =========================================================================

    async def checkpoint_save_nowait(self, data: bytes, key: str = "",
                                     ttl: int = -1) -> None:
        """
        Save checkpoint data. Does not wait for a response from the server.

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
        await self._request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    async def checkpoint_save(self, data: bytes, key: str = "",
                              ttl: int = -1,
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(checkpoint_op=op),
            request_id, timeout,
        )

    async def checkpoint_load_nowait(self, key: str = "") -> None:
        """
        Request checkpoint data. Response will arrive via on_checkpoint_response callback.

        Args:
            key: Optional checkpoint key (default: "default")
        """
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.LOAD,
            key=key
        )
        await self._request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    async def checkpoint_load(self, key: str = "",
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(checkpoint_op=op),
            request_id, timeout,
        )

    async def checkpoint_delete_nowait(self, key: str = "") -> None:
        """
        Delete a checkpoint. Does not wait for a response from the server.

        Args:
            key: Optional checkpoint key (default: "default")
        """
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.DELETE,
            key=key
        )
        await self._request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    async def checkpoint_delete(self, key: str = "",
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(checkpoint_op=op),
            request_id, timeout,
        )

    async def checkpoint_list_nowait(self) -> None:
        """
        Request list of checkpoint keys. Response will arrive via on_checkpoint_response callback.
        """
        op = aether_pb2.CheckpointOperation(
            op=aether_pb2.CheckpointOperation.LIST
        )
        await self._request_queue.put(aether_pb2.UpstreamMessage(checkpoint_op=op))

    async def checkpoint_list(self, timeout: float = 5.0) -> Optional[aether_pb2.CheckpointResponse]:
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
        return await self._send_sync_op(
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

    async def query_tasks(self, workspace: str = "", status: str = "",
                          statuses: Optional[List[str]] = None,
                          task_type: str = "", limit: int = 0, offset: int = 0,
                          task_class: int = 0,
                          exclude_task_classes: Optional[List[int]] = None,
                          timeout: float = 10.0) -> Optional[aether_pb2.TaskQueryResponse]:
        """
        List tasks with optional filters.

        Args:
            workspace: Filter by workspace
            status: Filter by single task status (deprecated — use statuses)
            statuses: Filter by multiple task statuses (e.g. ["pending", "running"])
            task_type: Filter by task type
            limit: Maximum number of results (0 = server default)
            offset: Offset for pagination
            timeout: Timeout in seconds

        Returns:
            TaskQueryResponse or None if timeout
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
        )
        query = aether_pb2.TaskQuery(
            op=aether_pb2.TaskQuery.LIST,
            filter=task_filter,
            request_id=request_id,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_query=query),
            request_id, timeout,
        )

    async def get_task(self, task_id: str,
                       timeout: float = 10.0) -> Optional[aether_pb2.TaskQueryResponse]:
        """
        Get a specific task by ID.

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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_query=query),
            request_id, timeout,
        )

    async def cancel_task(self, task_id: str, reason: str = "",
                          timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Cancel a running or queued task.

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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def retry_task(self, task_id: str,
                         timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Retry a failed or cancelled task.

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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def complete_task(self, task_id: str,
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def fail_task(self, task_id: str, reason: str = "",
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def pause_task(self, task_id: str, wait_spec: aether_pb2.WaitSpec,
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
                Use :func:`make_wait_spec` from ``client.py`` to build this conveniently.
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def wait_for_task(self, task_id: str, depends_on: List[str],
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def resume_task(self, task_id: str,
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def reject_task(self, task_id: str, reason: str = "",
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    # =========================================================================
    # Phase 2 Stage C: AuthorityRequest ("sudo") lifecycle (async)
    # =========================================================================

    async def request_authority(self,
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
        Async counterpart to :meth:`BaseAetherClient.request_authority`. See
        the sync docstring for the parameter contract.
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            request_id, timeout,
        )

    async def list_pending_authority_requests(self,
                                              workspace: str = "",
                                              matching_capabilities: Optional[List[str]] = None,
                                              limit: int = 100,
                                              offset: int = 0,
                                              timeout: float = 10.0) -> Optional[aether_pb2.AuthorityRequestOperationResponse]:
        """
        Async counterpart to
        :meth:`BaseAetherClient.list_pending_authority_requests`.
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            request_id, timeout,
        )

    async def resolve_authority_request(self,
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
        Async counterpart to
        :meth:`BaseAetherClient.resolve_authority_request`.
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            client_request_id, timeout,
        )

    async def cancel_authority_request(self, request_id: str, reason: str = "",
                                       timeout: float = 10.0) -> Optional[aether_pb2.AuthorityRequestOperationResponse]:
        """
        Async counterpart to
        :meth:`BaseAetherClient.cancel_authority_request`.
        """
        client_request_id = str(uuid.uuid4())
        op = aether_pb2.AuthorityRequestOperation(
            op=aether_pb2.AuthorityRequestOperation.CANCEL,
            request_id=request_id,
            reason=reason,
            client_request_id=client_request_id,
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_request_op=op),
            client_request_id, timeout,
        )

    # =========================================================================
    # Phase 3 Stage C: Hibernation lifecycle (async)
    # =========================================================================

    async def hibernate_until(self, task_id: str,
                              checkpoint_key: str,
                              scheduled_wake_unix_ms: int = 0,
                              timeout_ms: int = 0,
                              resume_session_id: str = "",
                              wake_event_types: Optional[List[str]] = None,
                              escalation_policy: str = "",
                              reason: str = "",
                              timeout: float = 10.0) -> Optional[aether_pb2.TaskOperationResponse]:
        """
        Async counterpart to :meth:`BaseAetherClient.hibernate_until`. See
        the sync docstring for the full parameter contract.
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(task_op=op),
            request_id, timeout,
        )

    async def workspace_op(self, op: aether_pb2.WorkspaceOperation,
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
        async with self._workspace_op_lock:
            while not self._workspace_response_queue.empty():
                try:
                    self._workspace_response_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

            await self._request_queue.put(aether_pb2.UpstreamMessage(workspace_op=op))

            try:
                return await asyncio.wait_for(self._workspace_response_queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                return None

    async def agent_op(self, op: aether_pb2.AgentOperation,
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
        async with self._agent_op_lock:
            while not self._agent_response_queue.empty():
                try:
                    self._agent_response_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

            await self._request_queue.put(aether_pb2.UpstreamMessage(agent_op=op))

            try:
                return await asyncio.wait_for(self._agent_response_queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                return None

    # ------------------------------------------------------------------
    # Agent Registry — Pythonic helpers
    # ------------------------------------------------------------------

    async def register_agent(self, implementation: str,
                             profile: str = "local",
                             description: str = "",
                             launch_params: dict[str, str] | None = None,
                             timeout: float = 10.0):
        """Register an agent implementation for orchestration.

        Args:
            implementation: Unique agent implementation name (e.g. "scitrera/falcon-chat-v2").
            profile: Orchestrator profile that handles this agent (e.g. "local", "k8s").
            description: Human-readable description.
            launch_params: Default launch parameters (string key-value pairs).
            timeout: Response timeout in seconds.

        Returns:
            AgentResponse or None on timeout.
        """
        # The gateway's agent registry requires "profile" inside launch_params
        params = dict(launch_params or {})
        params.setdefault("profile", profile)

        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.REGISTER,
            agent=aether_pb2.AgentRegistrationInfo(
                implementation=implementation,
                orchestrator_profile=profile,
                description=description,
                launch_params=params,
            ),
        )
        return await self.agent_op(op, timeout=timeout)

    async def get_agent(self, implementation: str,
                        timeout: float = 10.0):
        """Get an agent registration by implementation name.

        Returns:
            AgentResponse or None on timeout.
        """
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.GET,
            implementation=implementation,
        )
        return await self.agent_op(op, timeout=timeout)

    async def list_agents(self, profile: str = "",
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
        return await self.agent_op(op, timeout=timeout)

    async def delete_agent(self, implementation: str,
                           timeout: float = 10.0):
        """Remove an agent registration.

        Returns:
            AgentResponse or None on timeout.
        """
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.DELETE,
            implementation=implementation,
        )
        return await self.agent_op(op, timeout=timeout)

    async def launch_agent(self, implementation: str,
                           workspace: str = "",
                           specifier: str = "",
                           param_overrides: dict[str, str] | None = None,
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
        return await self.agent_op(op, timeout=timeout)

    async def session_op(self, op: aether_pb2.SessionOperation,
                         timeout: float = 10.0):
        """
        Send a SessionOperation and wait for the response.

        Uses request_id correlation when set; falls back to a per-type lock
        otherwise. Mirrors agent_op / workspace_op.

        Args:
            op: SessionOperation protobuf message. Set ``request_id`` for
                concurrent-call correlation; the server echoes it back.
            timeout: Timeout in seconds.

        Returns:
            SessionOperationResponse or None on timeout.
        """
        # Fast path: correlate by request_id when set, supporting concurrent callers.
        if op.request_id:
            future: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending_requests[op.request_id] = future
            try:
                await self._request_queue.put(aether_pb2.UpstreamMessage(session_op=op))
                return await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                self._pending_requests.pop(op.request_id, None)
                return None

        async with self._session_op_lock:
            while not self._session_response_queue.empty():
                try:
                    self._session_response_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

            await self._request_queue.put(aether_pb2.UpstreamMessage(session_op=op))

            try:
                return await asyncio.wait_for(self._session_response_queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                return None

    # ------------------------------------------------------------------
    # Sessions — Pythonic helpers
    # ------------------------------------------------------------------

    async def list_sessions(self, *, principal_type: str = "",
                            workspace: str = "",
                            limit: int = 0, offset: int = 0,
                            authorization: Optional[aether_pb2.AuthorizationContext] = None,
                            timeout: float = 10.0):
        """List active gateway sessions.

        Args:
            principal_type: Optional filter, lowercase REST form
                ("agent", "task", "user", "orchestrator", ...).
            workspace: Optional workspace filter.
            limit: Maximum number of results (0 = server default).
            offset: Pagination offset.
            authorization: Optional on-behalf-of authority context. When set,
                the gateway runs the admin ACL check against the subject
                (the user) rather than the actor (the platform-server).
            timeout: Response timeout in seconds.

        Returns:
            SessionOperationResponse (with ``connections`` and
            ``total_count``) or None on timeout.
        """
        op = aether_pb2.SessionOperation(
            op=aether_pb2.SessionOperation.LIST,
            request_id=str(uuid.uuid4()),
            filter=aether_pb2.ConnectionFilter(
                type=_principal_type_from_string(principal_type),
                workspace=workspace,
                limit=limit,
                offset=offset,
            ),
            authorization=authorization,
        )
        return await self.session_op(op, timeout=timeout)

    async def get_session(self, session_id: str,
                          authorization: Optional[aether_pb2.AuthorizationContext] = None,
                          timeout: float = 10.0):
        """Get a single active session by id."""
        op = aether_pb2.SessionOperation(
            op=aether_pb2.SessionOperation.GET,
            session_id=session_id,
            request_id=str(uuid.uuid4()),
            authorization=authorization,
        )
        return await self.session_op(op, timeout=timeout)

    async def disconnect_session(self, session_id: str, reason: str = "",
                                 authorization: Optional[aether_pb2.AuthorizationContext] = None,
                                 timeout: float = 10.0):
        """Forcibly disconnect an active session.

        Equivalent to the legacy REST ``DELETE /api/connections/{id}``.
        """
        op = aether_pb2.SessionOperation(
            op=aether_pb2.SessionOperation.DISCONNECT,
            session_id=session_id,
            reason=reason,
            request_id=str(uuid.uuid4()),
            authorization=authorization,
        )
        return await self.session_op(op, timeout=timeout)

    async def acl_op(self, op: aether_pb2.ACLOperation,
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
        async with self._acl_op_lock:
            while not self._acl_response_queue.empty():
                try:
                    self._acl_response_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

            await self._request_queue.put(aether_pb2.UpstreamMessage(acl_op=op))

            try:
                return await asyncio.wait_for(self._acl_response_queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                return None

    async def audit_query(self,
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
        """
        Query the comprehensive audit log.

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
            authorization: Optional on-behalf-of context. When present, the
                query is evaluated against the subject's ACL via the grant;
                required for non-admin callers.

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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(audit_query=query),
            request_id, timeout,
        )

    async def workflow_op(self, op: aether_pb2.WorkflowOperation,
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
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(workflow_op=op),
            request_id, timeout,
        )

    async def _send_token_op(self, op: aether_pb2.TokenOperation,
                             timeout: float = 10.0):
        """Send a TokenOperation with request ID correlation and wait for response."""
        request_id = str(uuid.uuid4())
        op.request_id = request_id
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(token_op=op),
            request_id, timeout,
        )

    async def token_op(self, op: aether_pb2.TokenOperation,
                       timeout: float = 10.0):
        """
        Send a TokenOperation and wait for the response (low-level escape hatch).

        Args:
            op: aether_pb2.TokenOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            TokenResponse or None if timeout
        """
        return await self._send_token_op(op, timeout)

    async def _send_authority_grant_op(self, op: aether_pb2.AuthorityGrantOperation,
                                       timeout: float = 10.0):
        """Send an AuthorityGrantOperation with request ID correlation and wait for response."""
        request_id = str(uuid.uuid4())
        op.request_id = request_id
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(authority_grant_op=op),
            request_id, timeout,
        )

    async def authority_grant_op(self, op: aether_pb2.AuthorityGrantOperation,
                                 timeout: float = 10.0):
        """
        Send an AuthorityGrantOperation and wait for the response.

        Args:
            op: AuthorityGrantOperation protobuf message
            timeout: Timeout in seconds

        Returns:
            AuthorityGrantResponse or None if timeout
        """
        return await self._send_authority_grant_op(op, timeout)

    async def exchange_authority_grant(self,
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
        return await self._send_authority_grant_op(op, timeout)

    async def derive_authority_grant(self,
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
        return await self._send_authority_grant_op(op, timeout)

    async def get_authority_grant(self, grant_id: str, timeout: float = 10.0):
        """Get a runtime authority grant by ID."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.GET,
            grant_id=grant_id,
        )
        return await self._send_authority_grant_op(op, timeout)

    async def renew_authority_grant(self, grant_id: str, expires_at: int = 0,
                                    extend_seconds: int = 0,
                                    timeout: float = 10.0):
        """Renew a runtime authority grant lease.

        Args:
            grant_id: ID of the grant to renew.
            expires_at: Absolute new expiry (unix-ms). When set to 0 the
                gateway uses ``extend_seconds`` if provided.
            extend_seconds: Renewal-sugar — extend the current expiry by
                this many seconds. Server clamps against ``renewable_until``.
                Ignored when ``expires_at`` is non-zero.
            timeout: Timeout in seconds.
        """
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.RENEW,
            grant_id=grant_id,
            renew_request=aether_pb2.ACLRenewAuthorityGrantRequest(
                grant_id=grant_id,
                expires_at=expires_at,
                extend_seconds=extend_seconds,
            ),
        )
        return await self._send_authority_grant_op(op, timeout)

    async def revoke_authority_grant(self, grant_id: str,
                                     timeout: float = 10.0):
        """Revoke a runtime authority grant by ID."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.REVOKE,
            grant_id=grant_id,
        )
        return await self._send_authority_grant_op(op, timeout)

    async def submit_audit_event(self,
                                 event_type: str,
                                 operation: str = "",
                                 resource_type: str = "",
                                 resource_id: str = "",
                                 workspace: str = "",
                                 success: bool = True,
                                 error_message: str = "",
                                 metadata: Optional[Dict[str, str]] = None,
                                 timeout: float = 10.0) -> Optional[AuditSubmitResponse]:
        """Submit a foreign audit event and await the gateway ack.

        Async equivalent of :meth:`BaseAetherClient.submit_audit_event`. Mints
        a fresh ``client_request_id``, registers an ``asyncio.Future`` in
        ``_pending_requests``, sends the upstream ``SubmitAuditEventRequest``,
        and awaits the matching downstream response or timeout.

        See the sync sibling for argument semantics (event_type whitelist,
        actor-identity stamping, cross-workspace ACL, metadata sanitization).

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
        resp = await self._send_sync_op(
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

    async def list_my_authority_grants(self,
                                       audience_type: str = "",
                                       audience_id: str = "",
                                       include_revoked: bool = False,
                                       limit: int = 0,
                                       offset: int = 0,
                                       timeout: float = 10.0):
        """List grants where the actor is delegate or subject.

        Returns the response with ``grants`` populated; callers may
        page using ``limit``/``offset`` and the ``total`` field on
        the response.
        """
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.LIST_MY_GRANTS,
            list_request=aether_pb2.AuthorityGrantListRequest(
                audience_type=audience_type,
                audience_id=audience_id,
                include_revoked=include_revoked,
                limit=limit,
                offset=offset,
            ),
        )
        return await self._send_authority_grant_op(op, timeout)

    async def list_authority_grants_on_me(self,
                                          audience_type: str = "",
                                          audience_id: str = "",
                                          include_revoked: bool = False,
                                          limit: int = 0,
                                          offset: int = 0,
                                          timeout: float = 10.0):
        """List grants where the actor is the subject (i.e. grants OTHERS hold on me)."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.LIST_GRANTS_ON_ME,
            list_request=aether_pb2.AuthorityGrantListRequest(
                audience_type=audience_type,
                audience_id=audience_id,
                include_revoked=include_revoked,
                limit=limit,
                offset=offset,
            ),
        )
        return await self._send_authority_grant_op(op, timeout)

    async def batch_exchange_authority_grants(self,
                                              requests: List[aether_pb2.AuthorityGrantExchangeRequest],
                                              stop_on_first_error: bool = False,
                                              timeout: float = 10.0):
        """Exchange multiple authority grants in a single round-trip."""
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.BATCH_EXCHANGE,
            batch_exchange_request=aether_pb2.AuthorityGrantBatchExchangeRequest(
                requests=requests,
                stop_on_first_error=stop_on_first_error,
            ),
        )
        return await self._send_authority_grant_op(op, timeout)

    async def derive_authority_grant_for_target(self,
                                                parent_grant_id: str,
                                                target_type: str,
                                                target_id: str,
                                                audience_type: str = "",
                                                audience_id: str = "",
                                                operation_scope: Optional[List[str]] = None,
                                                max_access_level: int = 0,
                                                expires_at: int = 0,
                                                renewable_until: int = 0,
                                                may_delegate: bool = False,
                                                remaining_hops: int = 0,
                                                reason: str = "",
                                                timeout: float = 10.0):
        """Idempotent derive: returns existing visible grant or mints a new one.

        Server-side this op is the safe-to-call-repeatedly variant of
        DERIVE — useful for orchestrators handing out per-task grants
        without leaking new grants on every retry.
        """
        op = aether_pb2.AuthorityGrantOperation(
            op=aether_pb2.AuthorityGrantOperation.DERIVE_FOR_TARGET,
            derive_for_target_request=aether_pb2.AuthorityGrantDeriveForTargetRequest(
                parent_grant_id=parent_grant_id,
                target=_principal_ref(target_type, target_id),
                audience_type=audience_type,
                audience_id=audience_id,
                operation_scope=operation_scope or [],
                max_access_level=max_access_level,
                expires_at=expires_at,
                renewable_until=renewable_until,
                may_delegate=may_delegate,
                remaining_hops=remaining_hops,
                reason=reason,
            ),
        )
        return await self._send_authority_grant_op(op, timeout)

    def make_authority_cache(self, **kwargs):
        """Create an :class:`AuthorityGrantCache` wired into this client.

        Subsequent ``AuthorityGrantRevocation`` push events on the
        downstream stream are dispatched to the cache automatically. Only
        one cache may be installed per client — calling this twice
        replaces the prior cache (the prior one stops receiving events).
        """
        # Local import avoids a circular module-load — authority_cache
        # imports the proto bindings, not this module.
        from .authority_cache import AuthorityGrantCache
        cache = AuthorityGrantCache(self, **kwargs)
        self._authority_grant_cache = cache
        return cache

    # =========================================================================
    # Authority resolution & connection status (Phase 3.5b)
    # =========================================================================

    async def resolve_authority(
        self,
        grant_id: str,
        subject_type: str,
        subject_id: str,
        *,
        actor_type: Optional[str] = None,
        actor_id: Optional[str] = None,
        audience_type: str = "",
        audience_id: str = "",
        request_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Optional[aether_pb2.ResolveAuthorityResponse]:
        """Resolve a runtime authority grant for a (subject, actor) pair.

        Returns the raw ``ResolveAuthorityResponse`` proto regardless of
        ``ok``; callers (e.g. :class:`~scitrera_aether_client.authority.AsyncAuthorityResolver`)
        are responsible for interpreting ``ok=false`` and surfacing or
        suppressing the embedded ``error``.

        Args:
            grant_id: ID of the runtime authority grant to resolve.
            subject_type: REST-form principal type of the grant subject (e.g. ``"user"``).
            subject_id: ID of the grant subject.
            actor_type: Optional override for the actor principal type. When ``None``,
                the gateway uses the connection identity.
            actor_id: Optional override for the actor principal ID. When ``None``,
                the gateway uses the connection identity.
            audience_type: Optional expected audience principal type to validate against.
            audience_id: Optional expected audience principal ID.
            request_id: Optional explicit request_id; defaults to a fresh ``uuid4``.
            timeout: Timeout in seconds; defaults to 10.0 if ``None``.

        Returns:
            ``ResolveAuthorityResponse`` proto, or ``None`` on timeout.
        """
        if timeout is None:
            timeout = 10.0
        req_id = request_id or str(uuid.uuid4())
        req = aether_pb2.ResolveAuthorityRequest(
            request_id=req_id,
            grant_id=grant_id,
            subject=_principal_ref(subject_type, subject_id),
            audience_type=audience_type,
            audience_id=audience_id,
        )
        if actor_type is not None or actor_id is not None:
            req.actor.CopyFrom(_principal_ref(actor_type or "", actor_id or ""))
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(resolve_authority_request=req),
            req_id, timeout,
        )

    async def connection_status(
        self,
        principal_type: str,
        principal_id: str,
        *,
        request_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Optional[aether_pb2.ConnectionStatusResponse]:
        """Query connection status for a principal.

        Returns the raw ``ConnectionStatusResponse`` proto regardless of
        ``ok``; callers interpret ``ok=false`` (e.g. unknown principal,
        permission denied) and the embedded ``error``.

        Args:
            principal_type: REST-form principal type (e.g. ``"agent"``, ``"task"``).
            principal_id: Principal ID to query.
            request_id: Optional explicit request_id; defaults to a fresh ``uuid4``.
            timeout: Timeout in seconds; defaults to 10.0 if ``None``.

        Returns:
            ``ConnectionStatusResponse`` proto, or ``None`` on timeout.
        """
        if timeout is None:
            timeout = 10.0
        req_id = request_id or str(uuid.uuid4())
        req = aether_pb2.ConnectionStatusRequest(
            request_id=req_id,
            principal=_principal_ref(principal_type, principal_id),
        )
        return await self._send_sync_op(
            aether_pb2.UpstreamMessage(connection_status_request=req),
            req_id, timeout,
        )

    async def list_tokens(self, limit: int = 0, offset: int = 0,
                          include_revoked: bool = False,
                          timeout: float = 10.0):
        """List API tokens.

        Args:
            limit: Maximum number of results (0 = default 100).
            offset: Offset for pagination.
            include_revoked: If False (default), excludes revoked tokens.
            timeout: Timeout in seconds.
        """
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.LIST,
            filter=aether_pb2.TokenFilter(
                limit=limit, offset=offset,
                include_revoked=include_revoked,
            ),
        )
        return await self._send_token_op(op, timeout)

    async def get_token(self, token_id: str, timeout: float = 10.0):
        """Get a specific API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.GET,
            token_id=token_id,
        )
        return await self._send_token_op(op, timeout)

    async def create_token(self, name: str, principal_type: str,
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
        return await self._send_token_op(op, timeout)

    async def delete_token(self, token_id: str, timeout: float = 10.0):
        """Delete an API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.DELETE,
            token_id=token_id,
        )
        return await self._send_token_op(op, timeout)

    async def revoke_token(self, token_id: str, timeout: float = 10.0):
        """Revoke an API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.REVOKE,
            token_id=token_id,
        )
        return await self._send_token_op(op, timeout)

    # ---- ACL convenience methods ----

    async def acl_list_rules(self, principal_type: str = "", principal_id: str = "",
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
        return await self.acl_op(op, timeout)

    async def acl_get_rule(self, rule_id: str, timeout: float = 10.0):
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
        return await self.acl_op(op, timeout)

    async def acl_check_access(self, principal_type: str, principal_id: str,
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
        resp = await self.acl_list_rules(
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
        fallback_resp = await self.acl_get_fallback_policy(category, timeout=timeout)
        if fallback_resp is not None and fallback_resp.fallback_policy:
            if fallback_resp.fallback_policy.fallback_access_level >= required_level:
                return True

        return False

    async def acl_grant(self, principal_type: str, principal_id: str,
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
        return await self.acl_op(op, timeout)

    async def acl_revoke(self, rule_id: str, timeout: float = 10.0):
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
        return await self.acl_op(op, timeout)

    async def acl_get_fallback_policy(self, rule_category: str,
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
        return await self.acl_op(op, timeout)

    async def acl_set_fallback_policy(self, rule_category: str,
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
        return await self.acl_op(op, timeout)

    async def acl_query_audit(self, start_time: int = 0, end_time: int = 0,
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
        return await self.acl_op(op, timeout)

    # NOTE: acl_{list,get,create,renew,revoke}_authority_grant helpers were
    # removed. The streaming ACLOperation no longer carries authority-grant
    # ops; use the runtime AuthorityGrantOperation surface (see Phase 4 SDK
    # cache helpers) or the REST admin endpoints for management.

    async def acl_cleanup_expired_rules(self, timeout: float = 10.0):
        """Clean up expired ACL rules.

        Returns:
            ACLResponse or None if timeout.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.CLEANUP_EXPIRED,
        )
        return await self.acl_op(op, timeout)

    async def acl_cleanup_audit_logs(self, retention_days: int = 90,
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
        return await self.acl_op(op, timeout)

    # ---- Schedule convenience methods ----

    async def create_schedule(self, schedule_id: str, name: str,
                              schedule_type: str, schedule_expr: str,
                              action: Optional[dict] = None,
                              workflow_id: str = "",
                              workspace: str = "*",
                              miss_policy: str = "skip",
                              max_concurrent: int = 0,
                              timeout: float = 10.0):
        """Create a new schedule.

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
        return await self.workflow_op(op, timeout=timeout)

    async def upsert_schedule(self, schedule_id: str, name: str,
                              schedule_type: str, schedule_expr: str,
                              action: Optional[dict] = None,
                              workflow_id: str = "",
                              workspace: str = "*",
                              miss_policy: str = "skip",
                              max_concurrent: int = 0,
                              timeout: float = 10.0):
        """Create or update a schedule idempotently.

        Same parameters as create_schedule. If a schedule with the given ID
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
        return await self.workflow_op(op, timeout=timeout)

    async def delete_schedule(self, schedule_id: str, timeout: float = 10.0):
        """Delete a schedule by ID.

        Returns:
            WorkflowResponse protobuf or None on timeout.
        """
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.DELETE_SCHEDULE,
            id=schedule_id,
        )
        return await self.workflow_op(op, timeout=timeout)

    async def list_schedules(self, workspace: str = "*", timeout: float = 10.0):
        """List all schedules for a workspace.

        Returns:
            WorkflowResponse protobuf or None on timeout.
        """
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.LIST_SCHEDULES,
            workspace=workspace,
        )
        return await self.workflow_op(op, timeout=timeout)

    async def close(self):
        """Close the connection.

        Sequencing matters here: gRPC's hidden ``_consume_request_iterator``
        task calls ``_done_writing()`` (which awaits ``send_receive_close``)
        as soon as our generator returns. Closing the channel before that
        await completes cancels the consumer task mid-flight and produces a
        spurious ``CancelledError`` traceback in gRPC's logger. We:

          1. set ``_stop_event`` and put the sentinel,
          2. wait for ``_request_generator_done`` (best-effort, with timeout),
          3. yield once so gRPC's consumer can finish ``_done_writing()``,
          4. cancel the listen task and close the channel.
        """
        self._stop_event.set()
        await self._request_queue.put(None)  # Sentinel to wake generator

        # Step 2: let the request generator drain.
        if self._request_generator_done is not None:
            try:
                await asyncio.wait_for(self._request_generator_done.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            # Step 3: yield to the loop so gRPC's consumer task can run
            # _done_writing() before we tear the channel down.
            await asyncio.sleep(0)

        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self.channel:
            await self.channel.close()

    async def wait_until_disconnected(self):
        """Wait until the client is fully disconnected.

        With ``auto_reconnect=True`` the SDK installs a NEW ``_listen_task``
        on every successful reconnect (see ``_attempt_reconnect`` →
        ``_do_connect``).  Awaiting just the original task reference would
        therefore return after the first disconnect even though the SDK is
        actively reconnecting in the background — leaving long-running
        callers (e.g. ``Agent2.run``) to fall out of their main loop and
        terminate the process at the gateway's first ``MaxConnectionAge``
        graceful disconnect (typically every 2h).

        This method loops over successive listen tasks until the SDK
        signals it is fully done — ``_stop_event`` set AND no reconnect in
        flight.  Polls briefly between tasks so the new listen task
        installed by ``_do_connect`` is observed.
        """
        # Tight loop: await whatever listen task is current; when it ends,
        # decide whether the SDK is fully done or just between reconnects.
        while True:
            task = self._listen_task
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Fully done = stop event set AND no reconnect attempt in progress.
            # _attempt_reconnect sets _reconnecting=True for the duration of
            # its retry loop; on giving up it sets _stop_event and unsets
            # _reconnecting (in the finally block).
            if self._stop_event.is_set() and not self._reconnecting:
                return

            # Either reconnecting (new listen task incoming) or transient
            # absence of a task. Yield briefly so the new task can be installed.
            await asyncio.sleep(0.1)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


# =============================================================================
# Async Client Classes
# =============================================================================

class AsyncAgentClient(BaseAsyncAetherClient):
    """Async client for agent connections to the Aether gateway."""

    def __init__(self, workspace: str, implementation: str, specifier: str,
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
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
        self.init = create_agent_init(workspace, implementation, specifier, credentials)

    async def connect(self, target: str = "localhost:50051"):
        await self._connect(self.init, target)

    async def switch_workspace(self, new_workspace: str):
        """Switch to a different workspace."""
        self.workspace = new_workspace
        await self._switch_workspace(new_workspace)

    async def send_message_to_agent(self, workspace: str, implementation: str, specifier: str,
                                    payload: bytes, message_type: int = aether_pb2.OPAQUE,
                                    authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_agent(workspace, implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_service(self, implementation: str, specifier: str,
                                      payload: bytes, message_type: int = aether_pb2.OPAQUE,
                                      authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a message to a service principal (``sv::{impl}::{specifier}``).

        Service principals are workspace-less; the gateway routes inbound
        traffic on the service topic to the registered service connection
        (typically a proxy-sidecar relay+terminator). Useful when an agent
        replies to an RPC originated from a service identity.
        """
        await self._send_message(
            create_topic_service(implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_task(self, workspace: str, implementation: str, payload: bytes,
                                   unique_specifier: str = "", message_type: int = aether_pb2.OPAQUE,
                                   authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_task(workspace, implementation, unique_specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                           message_type: int = aether_pb2.OPAQUE,
                                           authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_user(user_id, window_id),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_broadcast_to_agents(self, workspace: str, payload: bytes,
                                       message_type: int = aether_pb2.OPAQUE,
                                       authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a broadcast message to all agents in a workspace."""
        await self._send_message(
            create_topic_global_agents(workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_broadcast_to_users(self, workspace: str, payload: bytes,
                                      message_type: int = aether_pb2.OPAQUE,
                                      authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a broadcast to all users in a workspace."""
        await self._send_message(
            create_topic_global_users(workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                             message_type: int = aether_pb2.OPAQUE,
                                             authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a message to a user's workspace-scoped topic."""
        await self._send_message(
            create_topic_user_workspace(user_id, workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_event(self, payload: bytes):
        """Send an event to the workflow engine."""
        await self._send_message("event::*", payload, message_type=aether_pb2.EVENT)

    async def send_metric(self, metric):
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
        await self._send_message("metric::*", buf, message_type=aether_pb2.METRIC)


class AsyncServiceClient(BaseAsyncAetherClient):
    """Async client for Service principal connections (trusted backend intermediaries).

    Service principals are workspace-less and are intended for app/websocket
    backends that authenticate as themselves and perform privileged work on
    behalf of users via ``AuthorizationContext`` (on-behalf-of mode).

    Canonical identity string: ``sv.{implementation}.{specifier}``.

    Typical usage::

        sv = AsyncServiceClient(
            implementation='platform-server',
            specifier='pod-abc',
            credentials=Credentials.api_key(api_key),
        )
        await sv.connect(target='aether:50151')
        # Exchange a user session into an on-behalf-of grant
        resp = await sv.exchange_authority_grant(
            source_session_id=user_session_id,
            audience_type='session', audience_id=browser_sid,
            valid_while_audience_active=True,
        )
        auth = aether_pb2.AuthorizationContext(
            authority_mode='on_behalf_of',
            subject=aether_pb2.PrincipalRef(principal_type='user', principal_id='alice'),
            grant_id=resp.grant.grant_id,
        )
        # Then any op: sv.kv_get(..., authorization=auth), sv.audit_query(..., authorization=auth), etc.
    """

    def __init__(self, implementation: str, specifier: str,
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
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
        self.init = create_service_init(implementation, specifier, credentials)

    async def connect(self, target: str = "localhost:50051"):
        await self._connect(self.init, target)

    # --- Message sending ----------------------------------------------------
    # Service principals are workspace-less; callers must supply the target
    # workspace explicitly. Any user-scoped operation should pass
    # ``authorization`` to invoke on-behalf-of semantics.

    async def send_message_to_agent(self, workspace: str, implementation: str, specifier: str,
                                    payload: bytes, message_type: int = aether_pb2.OPAQUE,
                                    authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_agent(workspace, implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_service(self, implementation: str, specifier: str,
                                      payload: bytes, message_type: int = aether_pb2.OPAQUE,
                                      authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a message to another service principal."""
        await self._send_message(
            create_topic_service(implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_task(self, workspace: str, implementation: str, payload: bytes,
                                   unique_specifier: str = "", message_type: int = aether_pb2.OPAQUE,
                                   authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_task(workspace, implementation, unique_specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                           message_type: int = aether_pb2.OPAQUE,
                                           authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_user(user_id, window_id),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                             message_type: int = aether_pb2.OPAQUE,
                                             authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_user_workspace(user_id, workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_metric(self, metric):
        """Send a metric to the metrics bridge.

        Service principals can publish metrics for their own resource use
        (e.g. transient services that incur cost on each invocation).  All
        entries are interpreted as additive deltas; entries with negative
        ``qty`` values require the ``capability/metric_credit`` ACL permission on
        the sender.

        Args:
            metric: An ``aether_pb2.Metric`` instance, typically constructed
                via ``new_metric()`` from ``scitrera_aether_client.metrics``.

        Raises:
            TypeError: If ``metric`` is not an ``aether_pb2.Metric`` instance.
        """
        if not isinstance(metric, aether_pb2.Metric):
            raise TypeError(f"metric must be an aether_pb2.Metric instance, got {type(metric)!r}")
        buf = metric.SerializeToString()
        await self._send_message("metric::*", buf, message_type=aether_pb2.METRIC)


class AsyncTaskClient(BaseAsyncAetherClient):
    """Async client for task connections to the Aether gateway."""

    def __init__(self, workspace: str, implementation: str, unique_specifier: str = "",
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
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
        self.init = create_task_init(workspace, implementation, unique_specifier, credentials)

    async def connect(self, target: str = "localhost:50051"):
        await self._connect(self.init, target)

    async def switch_workspace(self, new_workspace: str):
        """Switch to a different workspace."""
        self.workspace = new_workspace
        await self._switch_workspace(new_workspace)

    async def send_message_to_agent(self, workspace: str, implementation: str, specifier: str,
                                    payload: bytes, message_type: int = aether_pb2.OPAQUE,
                                    authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_agent(workspace, implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_task(self, workspace: str, implementation: str, payload: bytes,
                                   unique_specifier: str = "", message_type: int = aether_pb2.OPAQUE,
                                   authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_task(workspace, implementation, unique_specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                           message_type: int = aether_pb2.OPAQUE,
                                           authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_user(user_id, window_id),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_event(self, payload: bytes):
        """Send an event to the workflow engine."""
        await self._send_message("event::*", payload, message_type=aether_pb2.EVENT)

    async def send_metric(self, metric):
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
        await self._send_message("metric::*", buf, message_type=aether_pb2.METRIC)


class AsyncUserClient(BaseAsyncAetherClient):
    """Async client for user connections to the Aether gateway."""

    def __init__(self, user_id: str, window_id: str,
                 credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
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
        self.user_id = user_id
        self.window_id = window_id
        self.init = create_user_init(user_id, window_id, credentials)

    async def connect(self, target: str = "localhost:50051"):
        await self._connect(self.init, target)

    async def send_message_to_agent(self, workspace: str, implementation: str, specifier: str,
                                    payload: bytes, message_type: int = aether_pb2.OPAQUE,
                                    authorization: Optional[aether_pb2.AuthorizationContext] = None,
                                    app_workspace: str = ""):
        """Send a message to a specific agent.

        ``app_workspace`` is an optional hint carrying the user's active app
        workspace (e.g. ``"default"``). When set, the gateway stamps it into
        the task-authority grant's WorkspaceScope at triggerOrchestration time
        so the spawned agent can create resources in the user's workspace.
        """
        await self._send_message(
            create_topic_agent(workspace, implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
            app_workspace=app_workspace,
        )

    async def send_message_to_task(self, workspace: str, implementation: str, payload: bytes,
                                   unique_specifier: str = "", message_type: int = aether_pb2.OPAQUE,
                                   authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_task(workspace, implementation, unique_specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_session(self, user_id: str, window_id: str, payload: bytes,
                                           message_type: int = aether_pb2.OPAQUE,
                                           authorization: Optional[aether_pb2.AuthorizationContext] = None):
        await self._send_message(
            create_topic_user(user_id, window_id),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                             message_type: int = aether_pb2.OPAQUE,
                                             authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a message to a user's workspace-scoped topic."""
        await self._send_message(
            create_topic_user_workspace(user_id, workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def switch_workspace(self, new_workspace: str):
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
        await self._switch_workspace(new_workspace)


class AsyncOrchestratorClient(BaseAsyncAetherClient):
    """
    Async client for orchestrator connections to the Aether gateway.

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
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
        """
        Create an async orchestrator client.

        Args:
            implementation: Orchestrator implementation type
            supported_profiles: The profiles that this orchestrator can handle
            specifier: Unique specifier for this instance (generated if not provided)
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
        self.specifier = specifier or str(uuid.uuid4())[:8]
        self.supported_profiles = supported_profiles
        self.init = create_orchestrator_init(
            self.implementation, self.specifier,
            self.supported_profiles, credentials
        )

    async def connect(self, target: str = "localhost:50051"):
        await self._connect(self.init, target)

    async def send_status_to_agent(self, workspace: str, implementation: str, specifier: str,
                                   payload: bytes, message_type: int = aether_pb2.CONTROL,
                                   authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a status/control message to an agent."""
        await self._send_message(
            create_topic_agent(workspace, implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_status_to_task(self, workspace: str, implementation: str, payload: bytes,
                                  unique_specifier: str = "", message_type: int = aether_pb2.CONTROL,
                                  authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a status/control message to a task."""
        await self._send_message(
            create_topic_task(workspace, implementation, unique_specifier),
            payload, message_type=message_type, authorization=authorization,
        )


class AsyncWorkflowEngineClient(BaseAsyncAetherClient):
    """
    Async client for workflow engine connections to the Aether gateway.

    The workflow engine is the sole subscriber to event.* topics and
    processes broadcast events to trigger downstream actions.
    """

    def __init__(self, credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.init = create_workflow_engine_init(credentials)

    async def connect(self, target: str = "localhost:50051"):
        await self._connect(self.init, target)

    async def send_command_to_agent(self, workspace: str, implementation: str, specifier: str,
                                    payload: bytes, message_type: int = aether_pb2.CONTROL,
                                    authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a command to a specific agent."""
        await self._send_message(
            create_topic_agent(workspace, implementation, specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_command_to_task(self, workspace: str, implementation: str, payload: bytes,
                                   unique_specifier: str = "", message_type: int = aether_pb2.CONTROL,
                                   authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a command to a specific task."""
        await self._send_message(
            create_topic_task(workspace, implementation, unique_specifier),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_broadcast_to_agents(self, workspace: str, payload: bytes,
                                       message_type: int = aether_pb2.CONTROL,
                                       authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a broadcast to all agents in a workspace."""
        await self._send_message(
            create_topic_global_agents(workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_broadcast_to_users(self, workspace: str, payload: bytes,
                                      message_type: int = aether_pb2.OPAQUE,
                                      authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a broadcast to all users in a workspace."""
        await self._send_message(
            create_topic_global_users(workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user(self, user_id: str, window_id: str, payload: bytes,
                                   message_type: int = aether_pb2.OPAQUE,
                                   authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a message to a specific user session."""
        await self._send_message(
            create_topic_user(user_id, window_id),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_message_to_user_workspace(self, user_id: str, workspace: str, payload: bytes,
                                             message_type: int = aether_pb2.OPAQUE,
                                             authorization: Optional[aether_pb2.AuthorizationContext] = None):
        """Send a message to a user's workspace-scoped topic."""
        await self._send_message(
            create_topic_user_workspace(user_id, workspace),
            payload, message_type=message_type, authorization=authorization,
        )

    async def send_metric(self, metric):
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
        await self._send_message("metric::*", buf, message_type=aether_pb2.METRIC)


class AsyncMetricsBridgeClient(BaseAsyncAetherClient):
    """
    Async client for metrics bridge connections to the Aether gateway.

    The metrics bridge is a receive-only client that subscribes to metric.*
    topics to collect telemetry data from agents and tasks.
    """

    def __init__(self, credentials: Optional[Dict[str, str]] = None,
                 max_retries: int = 5,
                 initial_backoff: float = 1.0,
                 max_backoff: float = 30.0,
                 auto_reconnect: bool = True,
                 tls_enabled: Optional[bool] = None,
                 tls_root_cert: Optional[bytes] = None,
                 tls_root_cert_path: Optional[str] = None,
                 tls_client_cert: Optional[bytes] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key: Optional[bytes] = None,
                 tls_client_key_path: Optional[str] = None):
        super().__init__(max_retries=max_retries, initial_backoff=initial_backoff,
                         max_backoff=max_backoff, auto_reconnect=auto_reconnect,
                         tls_enabled=tls_enabled, tls_root_cert=tls_root_cert,
                         tls_root_cert_path=tls_root_cert_path,
                         tls_client_cert=tls_client_cert,
                         tls_client_cert_path=tls_client_cert_path,
                         tls_client_key=tls_client_key,
                         tls_client_key_path=tls_client_key_path)
        self.init = create_metrics_bridge_init(credentials)

    async def connect(self, target: str = "localhost:50051"):
        await self._connect(self.init, target)

    async def send_acknowledgment(self, target_topic: str, payload: bytes):
        """Send an acknowledgment to a source topic."""
        await self._send_message(target_topic, payload, message_type=aether_pb2.CONTROL)
