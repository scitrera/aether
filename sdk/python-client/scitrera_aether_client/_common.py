"""
Common constants, types, and helper functions shared between sync and async clients.
"""
import logging
import platform
from typing import Dict, List, Optional

import grpc
import sys

from .proto import aether_pb2

# =============================================================================
# Client version metadata (InitConnection versioning spec)
# =============================================================================

# Identifies this SDK in audit rows. Fixed at "python" — distinct from
# user-supplied version strings so the gateway can group connections by
# language without parsing the version string.
_CLIENT_SDK_NAME = "python"


def _resolve_client_version() -> str:
    """Best-effort version lookup.

    Prefers the package distribution version (set at build/install time)
    so editable installs report the real shipping version; falls back to
    the ``__version__`` constant when the distribution metadata is
    unavailable (e.g. running from a source checkout without an install).
    """
    try:
        from importlib.metadata import version, PackageNotFoundError  # type: ignore
        try:
            return version("scitrera_aether_client")
        except PackageNotFoundError:
            pass
    except ImportError:
        pass
    try:
        from . import __version__ as fallback_version
        return fallback_version
    except ImportError:
        return "unknown"


# Memoized so every InitConnection construction is cheap.
_CLIENT_VERSION = _resolve_client_version()
_CLIENT_RUNTIME = (
    f"python{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
)
_CLIENT_OS = f"{platform.system().lower()}/{platform.machine()}"


def _apply_client_version_meta(init_msg: "aether_pb2.InitConnection") -> "aether_pb2.InitConnection":
    """Populate the SDK version + build-info fields on an InitConnection.

    Called from every ``create_*_init`` helper so the gateway always
    receives consistent version metadata regardless of which client
    type is connecting. Idempotent: re-applying does not change the
    encoded value.
    """
    init_msg.client_version = _CLIENT_VERSION
    init_msg.client_sdk = _CLIENT_SDK_NAME
    init_msg.client_build_info.runtime = _CLIENT_RUNTIME
    init_msg.client_build_info.os = _CLIENT_OS
    return init_msg


# =============================================================================
# Error codes that should not trigger reconnection
# =============================================================================

NON_RECOVERABLE_CODES = {
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.ALREADY_EXISTS,
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.UNIMPLEMENTED,
}


# =============================================================================
# Helper functions for creating identity messages
# =============================================================================

def create_agent_init(workspace: str, implementation: str, specifier: str,
                      credentials: Optional[Dict[str, str]] = None,
                      resume_session_id: str = "",
                      extensions: Optional[List["aether_pb2.ExtensionDeclaration"]] = None,
                      ) -> aether_pb2.InitConnection:
    """Create an InitConnection message for an agent.

    Args:
        workspace: Agent workspace.
        implementation: Agent implementation name.
        specifier: Agent instance specifier.
        credentials: Optional auth credentials map.
        resume_session_id: Optional previous session ID to resume.
        extensions: Optional list of ``ExtensionDeclaration`` values
            describing Phase 6 extensions the client wants negotiated at
            connect time. Pass values built via :func:`make_extension`.
    """
    return _apply_client_version_meta(aether_pb2.InitConnection(
        agent=aether_pb2.AgentIdentity(
            workspace=workspace,
            implementation=implementation,
            specifier=specifier
        ),
        credentials=credentials or {},
        resume_session_id=resume_session_id,
        extensions=list(extensions) if extensions else [],
    ))


def create_task_init(workspace: str, implementation: str, unique_specifier: str = "",
                     credentials: Optional[Dict[str, str]] = None,
                     resume_session_id: str = "",
                     extensions: Optional[List["aether_pb2.ExtensionDeclaration"]] = None,
                     ) -> aether_pb2.InitConnection:
    """Create an InitConnection message for a task.

    See :func:`create_agent_init` for the ``extensions`` argument shape.
    """
    return _apply_client_version_meta(aether_pb2.InitConnection(
        task=aether_pb2.TaskIdentity(
            workspace=workspace,
            implementation=implementation,
            unique_specifier=unique_specifier
        ),
        credentials=credentials or {},
        resume_session_id=resume_session_id,
        extensions=list(extensions) if extensions else [],
    ))


def create_user_init(user_id: str, window_id: str,
                     credentials: Optional[Dict[str, str]] = None,
                     resume_session_id: str = "",
                     extensions: Optional[List["aether_pb2.ExtensionDeclaration"]] = None,
                     ) -> aether_pb2.InitConnection:
    """Create an InitConnection message for a user."""
    return _apply_client_version_meta(aether_pb2.InitConnection(
        user=aether_pb2.UserIdentity(user_id=user_id, window_id=window_id),
        credentials=credentials or {},
        resume_session_id=resume_session_id,
        extensions=list(extensions) if extensions else [],
    ))


def create_orchestrator_init(implementation: str, specifier: str,
                             supported_profiles: Optional[List[str]] = None,
                             credentials: Optional[Dict[str, str]] = None,
                             resume_session_id: str = "",
                             extensions: Optional[List["aether_pb2.ExtensionDeclaration"]] = None,
                             ) -> aether_pb2.InitConnection:
    """Create an InitConnection message for an orchestrator."""
    return _apply_client_version_meta(aether_pb2.InitConnection(
        orchestrator=aether_pb2.OrchestratorIdentity(
            implementation=implementation,
            specifier=specifier,
            supported_profiles=supported_profiles or []
        ),
        credentials=credentials or {},
        resume_session_id=resume_session_id,
        extensions=list(extensions) if extensions else [],
    ))


def create_workflow_engine_init(credentials: Optional[Dict[str, str]] = None,
                                resume_session_id: str = "",
                                extensions: Optional[List["aether_pb2.ExtensionDeclaration"]] = None,
                                ) -> aether_pb2.InitConnection:
    """Create an InitConnection message for a workflow engine."""
    return _apply_client_version_meta(aether_pb2.InitConnection(
        workflow_engine=aether_pb2.WorkflowEngineIdentity(),
        credentials=credentials or {},
        resume_session_id=resume_session_id,
        extensions=list(extensions) if extensions else [],
    ))


def create_metrics_bridge_init(credentials: Optional[Dict[str, str]] = None,
                               resume_session_id: str = "",
                               extensions: Optional[List["aether_pb2.ExtensionDeclaration"]] = None,
                               ) -> aether_pb2.InitConnection:
    """Create an InitConnection message for a metrics bridge."""
    return _apply_client_version_meta(aether_pb2.InitConnection(
        metrics_bridge=aether_pb2.MetricsBridgeIdentity(),
        credentials=credentials or {},
        resume_session_id=resume_session_id,
        extensions=list(extensions) if extensions else [],
    ))


def create_service_init(implementation: str, specifier: str,
                        credentials: Optional[Dict[str, str]] = None,
                        resume_session_id: str = "",
                        extensions: Optional[List["aether_pb2.ExtensionDeclaration"]] = None,
                        ) -> aether_pb2.InitConnection:
    """Create an InitConnection message for a Service principal (workspace-less).

    Service principals are for trusted backend intermediaries (app backends,
    websocket servers, etc.) that authenticate as themselves but perform most
    privileged work on behalf of users via ``AuthorizationContext``.

    Canonical identity string: ``sv::{implementation}::{specifier}``.
    """
    return _apply_client_version_meta(aether_pb2.InitConnection(
        service=aether_pb2.ServiceIdentity(
            implementation=implementation,
            specifier=specifier,
        ),
        credentials=credentials or {},
        resume_session_id=resume_session_id,
        extensions=list(extensions) if extensions else [],
    ))


# =============================================================================
# Topic creation helpers
# =============================================================================

# Segment separator for all identity / topic strings. Must match
# server/pkg/models/topics.go::IdentitySep and the TypeScript SDK.
# Using "::" (not ".") lets field values legitimately contain "." — e.g. Python
# FQN implementations ("scitrera_ai_intelligence.cowork.aether_bridge.CoworkAgent")
# or email-style user_ids ("alice@example.com") — without the parser ambiguity
# that a dotted separator would create.
IDENTITY_SEP = "::"


def create_topic_agent(workspace: str, implementation: str, specifier: str) -> str:
    """Create a topic string for a specific agent."""
    return f"ag{IDENTITY_SEP}{workspace}{IDENTITY_SEP}{implementation}{IDENTITY_SEP}{specifier}"


def create_topic_service(implementation: str, specifier: str) -> str:
    """Create a topic string for a specific service principal.

    Service principals are workspace-less (canonical identity is
    ``sv::{implementation}::{specifier}``); the topic mirrors that shape.
    Mirrors :func:`models.topics.ServiceTopic` on the gateway side.
    """
    return f"sv{IDENTITY_SEP}{implementation}{IDENTITY_SEP}{specifier}"


def create_topic_task(workspace: str, implementation: str, specifier: str) -> str:
    """Create a topic string for a task."""
    if specifier:
        return f"tu{IDENTITY_SEP}{workspace}{IDENTITY_SEP}{implementation}{IDENTITY_SEP}{specifier}"
    return f"ta{IDENTITY_SEP}{workspace}{IDENTITY_SEP}{implementation}{IDENTITY_SEP}"


def create_topic_task_broadcast(workspace: str, implementation: str) -> str:
    """Create a broadcast topic for task load balancing."""
    return f"tb{IDENTITY_SEP}{workspace}{IDENTITY_SEP}{implementation}"


def create_topic_user(user_id: str, window_id: str) -> str:
    """Create a topic string for a user session."""
    return f"us{IDENTITY_SEP}{user_id}{IDENTITY_SEP}{window_id}"


def create_topic_user_workspace(user_id: str, workspace: str) -> str:
    """Create a topic string for user workspace messages."""
    return f"uw{IDENTITY_SEP}{user_id}{IDENTITY_SEP}{workspace}"


def create_topic_global_agents(workspace: str) -> str:
    """Create a global broadcast topic for all agents in a workspace."""
    return f"ga{IDENTITY_SEP}{workspace}"


def create_topic_global_users(workspace: str) -> str:
    """Create a global broadcast topic for all users in a workspace."""
    return f"gu{IDENTITY_SEP}{workspace}"


# =============================================================================
# Constants for convenience
# =============================================================================

# Message types
MESSAGE_TYPE_UNSPECIFIED = aether_pb2.MESSAGE_TYPE_UNSPECIFIED
OPAQUE = aether_pb2.OPAQUE
CHAT = aether_pb2.CHAT
CONTROL = aether_pb2.CONTROL
TOOL_CALL = aether_pb2.TOOL_CALL
EVENT = aether_pb2.EVENT
METRIC = aether_pb2.METRIC

# Task assignment modes
SELF_ASSIGN = aether_pb2.SELF_ASSIGN
TARGETED = aether_pb2.TARGETED
POOL = aether_pb2.POOL

# KV operation types
KV_GET = aether_pb2.KVOperation.GET
KV_PUT = aether_pb2.KVOperation.PUT
KV_LIST = aether_pb2.KVOperation.LIST
KV_DELETE = aether_pb2.KVOperation.DELETE

# KV scopes
KV_SCOPE_GLOBAL = "global"
KV_SCOPE_WORKSPACE = "workspace"
KV_SCOPE_USER = "user"
KV_SCOPE_USER_WORKSPACE = "user-workspace"
KV_SCOPE_GLOBAL_EXCLUSIVE = "global-exclusive"
KV_SCOPE_WORKSPACE_EXCLUSIVE = "workspace-exclusive"
KV_SCOPE_USER_SHARED = "user-shared"
KV_SCOPE_USER_WORKSPACE_SHARED = "user-workspace-shared"

_SCOPE_MAP = {
    "global": aether_pb2.KVOperation.GLOBAL,
    "workspace": aether_pb2.KVOperation.WORKSPACE,
    "user": aether_pb2.KVOperation.USER,
    "user-workspace": aether_pb2.KVOperation.USER_WORKSPACE,
    "global-exclusive": aether_pb2.KVOperation.GLOBAL_EXCLUSIVE,
    "workspace-exclusive": aether_pb2.KVOperation.WORKSPACE_EXCLUSIVE,
    "user-shared": aether_pb2.KVOperation.USER_SHARED,
    "user-workspace-shared": aether_pb2.KVOperation.USER_WORKSPACE_SHARED,
}


def _scope_to_proto(scope: str) -> int:
    """Convert a scope string to the corresponding KVOperation.Scope enum value."""
    return _SCOPE_MAP.get(scope, aether_pb2.KVOperation.SCOPE_UNSPECIFIED)


# =============================================================================
# Credentials helper class
# =============================================================================

class Credentials(dict):
    """Convenience builder for authentication credentials.

    Wraps a dict[str, str] with builder methods for common auth modes.
    Can be passed directly to any client constructor's credentials parameter.

    Examples:
        # API key auth
        creds = Credentials.api_key("my-api-key-here")
        agent = AgentClient("localhost:50051", "workspace", "impl", "spec", credentials=creds)

        # Task token auth (for orchestrated agents)
        creds = Credentials.task_token(token)
        agent = AgentClient("localhost:50051", "workspace", "impl", "spec", credentials=creds)

        # OAuth bearer token
        creds = Credentials.bearer_token(jwt_token)

        # Combine multiple
        creds = Credentials().with_api_key("key").with_tenant("tenant-id")
    """

    @classmethod
    def api_key(cls, key: str) -> "Credentials":
        """Create credentials with an API key."""
        return cls({"x-api-key": key})

    @classmethod
    def task_token(cls, token: str) -> "Credentials":
        """Create credentials with a task authentication token."""
        return cls({"token": token})

    @classmethod
    def bearer_token(cls, token: str) -> "Credentials":
        """Create credentials with an OAuth/JWT bearer token."""
        return cls({"authorization": f"Bearer {token}"})

    def with_api_key(self, key: str) -> "Credentials":
        """Add an API key credential."""
        self["x-api-key"] = key
        return self

    def with_task_token(self, token: str) -> "Credentials":
        """Add a task authentication token."""
        self["token"] = token
        return self

    def with_bearer_token(self, token: str) -> "Credentials":
        """Add an OAuth/JWT bearer token."""
        self["authorization"] = f"Bearer {token}"
        return self

    def with_tenant(self, tenant_id: str) -> "Credentials":
        """Add a tenant ID."""
        self["x-tenant-id"] = tenant_id
        return self


def _fix_pem_format(data: str) -> str:
    """Fix PEM formatting issues from env var transport (spaces instead of newlines, etc.)."""
    import re
    # Ensure newline after BEGIN line
    data = re.sub(
        r"(-----BEGIN [^-]+-----)\s*",
        r"\1\n",
        data
    )
    # Ensure newline before END line
    data = re.sub(
        r"\s*(-----END [^-]+-----)",
        r"\n\1",
        data
    )
    return data.strip() + "\n"


def _to_bytes_pem(data) -> Optional[bytes]:
    """Normalize PEM input (str|bytes|None) to bytes.

    If PEM data is given as a str, also fix formatting issues that occur when
    transitioning through environment variable mechanisms (spaces instead of newlines).
    """
    if data is None:
        return None
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return _fix_pem_format(data).encode('utf-8')
    return None


def _resolve_cert(explicit: Optional[bytes], path: Optional[str], env_var: str) -> tuple[
    Optional[bytes], Optional[str]]:
    """Resolve a certificate from explicit bytes, a file path, or an env var.

    The env var value is treated as a file path if it exists on disk,
    otherwise as inline PEM data.

    Returns (cert_bytes, cert_path) — at most one is set.
    """
    import os

    if explicit is not None:
        return (_to_bytes_pem(explicit), None)

    env_val = path or os.environ.get(env_var) or None
    if env_val is None:
        return (None, None)

    # If it looks like a file path and exists, return as path
    if os.path.isfile(env_val):
        return (None, env_val)

    # Otherwise treat as inline PEM data (e.g., from K8s secret env injection)
    cert_bytes = _to_bytes_pem(env_val)
    if cert_bytes and b"-----BEGIN" in cert_bytes:
        return (cert_bytes, None)

    # Doesn't look like PEM — treat as path (may not exist yet)
    return (None, env_val)


def _env_tls_kwargs_filter(
        tls_enabled: Optional[bool] = None,
        tls_root_cert: Optional[bytes] = None,
        tls_root_cert_path: Optional[str] = None,
        tls_client_cert: Optional[bytes] = None,
        tls_client_cert_path: Optional[str] = None,
        tls_client_key: Optional[bytes] = None,
        tls_client_key_path: Optional[str] = None
) -> dict:
    """Resolve TLS configuration from explicit args and/or environment variables.

    For each cert (CA, client cert, client key), resolution order is:
      1. Explicit bytes arg (tls_root_cert, tls_client_cert, tls_client_key)
      2. Explicit path arg (tls_root_cert_path, tls_client_cert_path, tls_client_key_path)
      3. Environment variable (AETHER_TLS_CA_CERT, AETHER_TLS_CLIENT_CERT, AETHER_TLS_CLIENT_KEY)

    Env var values are auto-detected as either file paths (if the file exists)
    or inline PEM data (with automatic formatting fixes for env var transport).

    ``tls_enabled`` is a tri-state: ``None`` (default) auto-detects from
    ``AETHER_TLS_ENABLED`` and the CA cert presence; ``True`` / ``False``
    are authoritative and skip the env-based auto-enable. Callers that
    need to opt OUT of TLS in an environment that otherwise advertises it
    (e.g. a sidecar process dialing a plaintext loopback relay while the
    container env is configured for upstream mTLS) MUST pass
    ``tls_enabled=False`` explicitly — the env auto-enable will not
    override an explicit decision.
    """
    import os

    ca_bytes, ca_path = _resolve_cert(tls_root_cert, tls_root_cert_path, "AETHER_TLS_CA_CERT")
    cert_bytes, cert_path = _resolve_cert(tls_client_cert, tls_client_cert_path, "AETHER_TLS_CLIENT_CERT")
    key_bytes, key_path = _resolve_cert(tls_client_key, tls_client_key_path, "AETHER_TLS_CLIENT_KEY")

    # Resolve the tri-state. Only fall back to env-based auto-detect when the
    # caller leaves tls_enabled at the None sentinel; explicit True/False is
    # authoritative. Auto-enable fires when AETHER_TLS_ENABLED is truthy OR a
    # CA cert is configured (server verification).
    if tls_enabled is None:
        has_ca = ca_bytes is not None or ca_path is not None
        tls_enabled = (
                os.environ.get("AETHER_TLS_ENABLED", "").lower() in ("true", "1", "yes") or
                has_ca
        )

    # Only include client cert/key if TLS is actually enabled
    if not tls_enabled:
        cert_bytes = cert_path = key_bytes = key_path = None

    return {
        'tls_enabled': tls_enabled,
        'tls_root_cert': ca_bytes,
        'tls_root_cert_path': ca_path,
        'tls_client_cert': cert_bytes,
        'tls_client_cert_path': cert_path,
        'tls_client_key': key_bytes,
        'tls_client_key_path': key_path,
    }


def _logging_lite_hook():
    """ Limit gRPC cython internal logging to INFO level """
    # limit logging from grpc internals to INFO level to avoid noisy DEBUG logs
    logging.getLogger('grpc._cython.cygrpc').setLevel(logging.INFO)
