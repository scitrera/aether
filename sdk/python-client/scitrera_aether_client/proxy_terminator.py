"""
HTTP-over-Aether proxy *terminator* (server side).

This module is the inverse of :mod:`scitrera_aether_client.proxy` (the
*initiator* side). Where ``proxy.py`` lets a Python caller *send* a
``ProxyHttpRequest`` envelope and await a correlated ``ProxyHttpResponse``,
this module lets a Python service *receive* inbound ``ProxyHttpRequest`` /
``ProxyHttpBodyChunk`` envelopes from the gateway, hand the assembled
request to a user-supplied ``handler`` callable, and frame the response
back to the gateway as ``ProxyHttpResponse`` (+ optional
``ProxyHttpBodyChunk`` frames for large bodies).

The behaviour mirrors the Go terminator at
``oss-repo/server/internal/proxysidecar/runner.go`` /
``terminator.go`` / ``http_backend.go``. Specifically:

* request body reassembly across ``ProxyHttpBodyChunk`` frames keyed by
  ``request_id`` (mirrors ``terminator.beginChunkedRequest`` /
  ``handleChunkedRequestFrame``).
* path allow-list with glob semantics (mirrors
  ``http_backend.pathAllowed``).
* header minting in ``strict`` mode: strips inbound ``X-Auth-*`` /
  ``X-Aether-*`` headers and synthesises fresh ``X-Auth-*`` headers from
  ``ProxyHttpRequest.authorization`` (mirrors
  ``identityheaders.StripInbound`` + ``identityheaders.MintInto``).
* response body chunking when the response exceeds
  ``PROXY_BODY_CHUNK_SIZE`` (mirrors
  ``terminator.dispatchAndRespond``'s 256 KB inline cap).
* error envelopes via ``ProxyHttpResponse{error: ProxyError{...}}``
  (mirrors ``terminator.errorResponse``).

Hook mechanism (mirrors the pattern in ``proxy.py``):

    The dispatcher seam in ``BaseAsyncAetherClient._listen_loop`` does not
    fan out ``proxy_http_request`` or request-direction
    ``proxy_http_body_chunk`` envelopes to user handlers. We monkey-patch
    ``_do_connect`` to wrap the gRPC ``responses`` iterator with a filter
    that consumes terminator-direction frames before they reach
    ``_listen_loop``. The patch is installed exactly once per process and
    is idempotent — the same pattern ``proxy.py::_ensure_hooks_installed``
    uses for the *initiator* side. Initiator and terminator hooks coexist:
    the initiator filter peels off ``proxy_http_response`` and
    ``is_request=False`` body chunks; the terminator filter peels off
    ``proxy_http_request`` and ``is_request=True`` body chunks. Other
    payload types fall through to ``_listen_loop`` untouched.

This terminator currently supports HTTP only (no Tunnel*). Streaming
response bodies (``stream_response_indefinitely``) are not yet supported
on the Python side; a request that asks for them produces a 500 with a
``not_implemented`` error body. TCP / WebSocket terminators and indefinite
streaming will land in a follow-up.
"""
from __future__ import annotations

import asyncio
import fnmatch
import logging
import threading
import traceback
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Literal, Mapping, Optional, Sequence, Union

from .authority import AuthorityResolverProtocol, ResolvedAuthorityInfo
from .proto import aether_pb2

logger = logging.getLogger("aether.client.proxy_terminator")

# Match initiator-side default so request/response chunking is symmetric.
PROXY_BODY_CHUNK_SIZE = 256 * 1024


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


HeaderMode = str  # "strict" | "passthrough"


@dataclass
class MintedRequest:
    """A fully assembled inbound proxy request, ready for the handler.

    ``headers`` is the post-mint header set: in strict mode all inbound
    ``X-Auth-*`` / ``X-Aether-*`` entries have been stripped and the
    canonical ``X-Auth-*`` headers minted from ``authorization`` have been
    overlaid. In passthrough mode the inbound headers are preserved as-is.

    The ``query`` field is split out from ``path`` for caller convenience.
    The Go terminator does not separate them on the wire; we follow the
    same envelope shape and split here so the ASGI bridge can build a
    proper ``http`` scope without re-parsing.
    """

    method: str
    path: str
    query: str
    headers: Dict[str, str]
    body: bytes
    request_id: str
    authorization: Optional[aether_pb2.AuthorizationContext] = None
    app_workspace: str = ""


@dataclass
class _PendingRequest:
    """Accumulator for one in-flight chunked request body."""

    header: aether_pb2.ProxyHttpRequest
    chunks: Dict[int, bytes] = field(default_factory=dict)
    fin_seen: bool = False
    total_bytes: int = 0


# Re-export for caller convenience — they get one import for everything.
ProxyHttpResponse = aether_pb2.ProxyHttpResponse
ProxyHttpRequest = aether_pb2.ProxyHttpRequest
ProxyHttpBodyChunk = aether_pb2.ProxyHttpBodyChunk
ProxyError = aether_pb2.ProxyError
AuthorizationContext = aether_pb2.AuthorizationContext


# Handler signature: takes a MintedRequest, returns either a
# ProxyHttpResponse (full envelope, status / headers / body) or a (status,
# headers, body) tuple. We accept both shapes so a handler can either build
# the envelope itself (e.g., for fine-grained header control) or return a
# simple triple.
HandlerResult = Union[
    aether_pb2.ProxyHttpResponse,
    "tuple[int, Mapping[str, str], bytes]",
]
Handler = Callable[[MintedRequest], Awaitable[HandlerResult]]


# ---------------------------------------------------------------------------
# Header minting (mirrors pkg/identityheaders/identityheaders.go)
# ---------------------------------------------------------------------------


# Canonical X-Auth-* header names. Kept in sync with the Go side at
# ``oss-repo/server/pkg/identityheaders/identityheaders.go`` — when a header
# is added there, mirror it here.
_HDR_ACTOR_TYPE = "X-Auth-Actor-Type"
_HDR_ACTOR_ID = "X-Auth-Actor-ID"
_HDR_AUTHORITY_MODE = "X-Auth-Authority-Mode"
_HDR_GRANT_ID = "X-Auth-Grant-ID"
_HDR_SUBJECT_TYPE = "X-Auth-Subject-Type"
_HDR_SUBJECT_ID = "X-Auth-Subject-ID"
_HDR_ROOT_SUBJECT_TYPE = "X-Auth-Root-Subject-Type"
_HDR_ROOT_SUBJECT_ID = "X-Auth-Root-Subject-ID"
_HDR_MAX_ACCESS_LEVEL = "X-Auth-Max-Access-Level"
_HDR_WORKSPACE_SCOPE = "X-Auth-Workspace-Scope"
# Audience headers — populated only by the resolver overlay (the wire envelope
# does not carry them; the gateway derives them from the validated grant).
_HDR_AUDIENCE_TYPE = "X-Auth-Audience-Type"
_HDR_AUDIENCE_ID = "X-Auth-Audience-ID"
# Backward-compat user/principal headers that downstream services read.
_HDR_USER_ID = "X-Auth-User-ID"
_HDR_PRINCIPAL_TYPE = "X-Auth-Principal-Type"

# OBO policy: how strict-mode dispatch handles on_behalf_of requests.
# - "require_resolver": OBO requires a configured resolver AND a non-None
#   resolve() result. Anything else → reject with ACL_DENIED.  This is the
#   safe default for production: callers downstream see fully-minted scope
#   headers (max_access_level, workspace_scope, audience), so a missing
#   overlay would silently grant broader access than the grant permits.
# - "allow_partial": OBO requests proceed with whatever ``_mint_auth_headers``
#   produces from the wire envelope alone (Phase 2a behaviour). Useful for
#   transition periods, callers that don't enforce scope, or non-strict
#   downstreams.
OBOPolicy = Literal["require_resolver", "allow_partial"]

_AUTHORITY_MODE_DIRECT = "direct"
_AUTHORITY_MODE_OBO = "on_behalf_of"


def _strip_inbound_identity_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of ``headers`` with ``X-Auth-*`` / ``X-Aether-*`` entries removed.

    Mirrors ``pkg/identityheaders/identityheaders.go::StripInbound``.
    Comparison is case-insensitive — HTTP header names are
    case-insensitive but the wire format we receive is whatever the caller
    set, so a malicious caller could still send ``x-auth-subject-id`` in
    lowercase to try to slip past a naive case-sensitive filter.
    """
    out: Dict[str, str] = {}
    for k, v in headers.items():
        lower = k.lower()
        if lower.startswith("x-auth-") or lower.startswith("x-aether-"):
            continue
        out[k] = v
    return out


def _mint_auth_headers(authz: Optional[aether_pb2.AuthorizationContext]) -> Dict[str, str]:
    """Synthesise the canonical ``X-Auth-*`` header set from an ``AuthorizationContext``.

    Python equivalent of ``pkg/identityheaders/identityheaders.go::MintInto``,
    scoped to the fields the SDK / Aether gateway actually populates on
    ``ProxyHttpRequest.authorization``: authority_mode, subject, grant_id.
    The Go side mints additional headers (tenant id, workspace access,
    audience, scopes, etc.) sourced from the authenticated identity which
    the SDK terminator does not yet have direct access to. Those are
    filled in by upstream layers when present.

    The Aether gateway is the trusted minter for actor-type / actor-id —
    those reflect the authenticated principal that opened the gateway
    connection. In v1 we do not have that identity in-band on the
    envelope, so we leave actor headers unset and let the gateway / a
    downstream auth-proxy stamp them when needed. (The cowork bridge
    today populates them via the gateway's stamped
    ``x-aether-actor-topic`` header — that header is preserved when
    `header_mode="passthrough"` and stripped + re-derived otherwise.)
    """
    if authz is None:
        # Direct mode with no authority context — only the mode flag is
        # known. Do not stamp grant/subject; downstream readers treat
        # missing fields as "no OBO" via ``Authority is None``.
        return {_HDR_AUTHORITY_MODE: _AUTHORITY_MODE_DIRECT}

    mode = (authz.authority_mode or "").strip()
    grant_id = authz.grant_id or ""
    subject = authz.subject if authz.HasField("subject") else None

    minted: Dict[str, str] = {}

    # OBO is signalled either by an explicit on_behalf_of mode or by the
    # presence of grant_id+subject (matches the Go translator's
    # tolerance — see http_backend.go::translateAuthorizationContext).
    if mode == _AUTHORITY_MODE_OBO or (grant_id and subject and subject.principal_id):
        minted[_HDR_AUTHORITY_MODE] = _AUTHORITY_MODE_OBO
        if grant_id:
            minted[_HDR_GRANT_ID] = grant_id
        if subject is not None:
            if subject.principal_type:
                minted[_HDR_SUBJECT_TYPE] = subject.principal_type
                # Backward-compat: downstream services that pre-date OBO
                # read X-Auth-Principal-Type for the user-facing principal
                # type. In OBO mode the subject IS the user-facing
                # principal — see Go MintInto OBO branch.
                minted[_HDR_PRINCIPAL_TYPE] = subject.principal_type
            if subject.principal_id:
                minted[_HDR_SUBJECT_ID] = subject.principal_id
                minted[_HDR_USER_ID] = subject.principal_id
        return minted

    # Direct mode (default).
    minted[_HDR_AUTHORITY_MODE] = _AUTHORITY_MODE_DIRECT
    return minted


def _overlay_resolved_authority(
    headers: Dict[str, str],
    info: ResolvedAuthorityInfo,
) -> None:
    """Overlay extended OBO header fields onto an already-minted header set.

    Mirrors the OBO branch of ``identityheaders.MintInto`` for the fields
    that are NOT carried on the wire envelope (max access level, workspace
    scope, audience, root subject).  ``_mint_auth_headers`` has already
    populated authority_mode, grant_id, subject_type, subject_id from the
    envelope; the resolver result is the authoritative source for the
    remaining fields.

    Resolver-derived ``root_subject_*`` overrides any envelope-minted
    values: the gateway-validated grant is the source of truth, and the
    envelope's view may be stale or partial.

    ``X-Auth-Workspace-Scope`` is omitted entirely (rather than emitted as
    an empty string) when the resolver reports an empty scope tuple, so
    downstream services can distinguish "no scope info" from "scope =
    everywhere".
    """
    if info.max_access_level:
        headers[_HDR_MAX_ACCESS_LEVEL] = str(info.max_access_level)
    if info.workspace_scope:
        headers[_HDR_WORKSPACE_SCOPE] = ",".join(info.workspace_scope)
    if info.audience_type:
        headers[_HDR_AUDIENCE_TYPE] = info.audience_type
    if info.audience_id:
        headers[_HDR_AUDIENCE_ID] = info.audience_id
    # Resolver values for root subject are authoritative — they come from
    # the validated grant chain — and overwrite any prior wire-derived
    # entries.
    if info.root_subject_type:
        headers[_HDR_ROOT_SUBJECT_TYPE] = info.root_subject_type
    if info.root_subject_id:
        headers[_HDR_ROOT_SUBJECT_ID] = info.root_subject_id


def _resolved_authority_from_proto(
    authz: aether_pb2.AuthorizationContext,
    resolved_pb: aether_pb2.ResolvedAuthorityInfo,
) -> ResolvedAuthorityInfo:
    """Build a :class:`ResolvedAuthorityInfo` from a gateway-stamped envelope.

    The gateway stamps :attr:`AuthorizationContext.resolved` after it has
    validated the grant under the calling delegate (in proxyACLCheck), so
    the values here are authoritative within a single Aether cluster.  The
    grant_id and subject ride on the parent ``authz``; the resolved proto
    carries only the fields the terminator needs but the wire envelope
    cannot otherwise convey (max access level, workspace scope, audience
    binding, root subject, expiry).

    ``revoked`` is implicitly ``False``: the gateway only stamps grants
    that passed ``ValidateActiveAt`` during proxyACLCheck, so a stamped
    ResolvedAuthorityInfo can never represent a revoked grant.
    """
    subject = authz.subject if authz.HasField("subject") else None
    root_subj = (
        resolved_pb.root_subject
        if resolved_pb.HasField("root_subject")
        else None
    )
    # Convert unix-millis (proto) → unix-seconds (dataclass) for parity with
    # the resolver path's ``ResolvedAuthorityInfo.expires_at``.
    expires_at_s = (
        int(resolved_pb.expires_at_ms // 1000)
        if resolved_pb.expires_at_ms
        else 0
    )
    return ResolvedAuthorityInfo(
        grant_id=authz.grant_id,
        subject_type=subject.principal_type if subject is not None else "",
        subject_id=subject.principal_id if subject is not None else "",
        root_subject_type=root_subj.principal_type if root_subj is not None else "",
        root_subject_id=root_subj.principal_id if root_subj is not None else "",
        audience_type=resolved_pb.audience_type,
        audience_id=resolved_pb.audience_id,
        max_access_level=int(resolved_pb.max_access_level),
        workspace_scope=tuple(resolved_pb.workspace_scope),
        expires_at=expires_at_s,
        revoked=False,
    )


# ---------------------------------------------------------------------------
# Path matching (mirrors http_backend.go::pathAllowed)
# ---------------------------------------------------------------------------


def _path_allowed(patterns: Sequence[str], req_path: str) -> bool:
    """Return True iff ``req_path`` matches any pattern.

    Patterns may be:
    - ``"*"`` or ``"/*"`` — match everything.
    - ``"<prefix>/*"`` — match anything under ``<prefix>/`` (and the bare
      prefix itself).
    - exact path — match by equality.
    - any glob honoured by ``fnmatch.fnmatch``.

    Mirrors the Go ``pathAllowed`` helper at
    ``http_backend.go::pathAllowed`` so route-level ACL semantics are
    identical on both terminator implementations.
    """
    for pattern in patterns:
        if pattern == "*" or pattern == "/*":
            return True
        if fnmatch.fnmatch(req_path, pattern):
            return True
        if pattern.endswith("/*"):
            prefix = pattern[: -len("/*")]
            if req_path == prefix or req_path.startswith(prefix + "/"):
                return True
        if pattern == req_path:
            return True
    return False


# ---------------------------------------------------------------------------
# Per-client terminator dispatcher
# ---------------------------------------------------------------------------


class _TerminatorDispatcher:
    """Per-client registry of active ``ProxyHttpTerminator`` instances.

    Multiple terminators can share one client (different path globs),
    though in practice a service registers exactly one terminator that
    owns the full route surface. The dispatcher routes inbound frames to
    the registered terminators in registration order; the first one whose
    path filter accepts the request wins. A request that no terminator
    accepts produces an ``ACL_DENIED`` ``ProxyError`` reply (mirroring the
    Go runner's behaviour when no backend matches).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._terminators: List["ProxyHttpTerminator"] = []
        self._pending: Dict[str, _PendingRequest] = {}
        # Per-client send func resolved on first dispatch — the SDK does
        # not expose a stable "send response" API so we resolve it from
        # the client's request_queue. See `_send_upstream`.
        self._client: Optional[Any] = None
        # Track in-flight handler tasks so they aren't garbage-collected
        # mid-flight (asyncio.create_task only holds a weak reference via
        # the event loop, but stronger ownership here makes the
        # invariant explicit and lets us tear them down on shutdown).
        # Each task removes itself on completion via its done callback.
        self._inflight_tasks: "set[asyncio.Task]" = set()

    def attach_client(self, client: Any) -> None:
        with self._lock:
            self._client = client

    def register(self, term: "ProxyHttpTerminator") -> None:
        with self._lock:
            if term not in self._terminators:
                self._terminators.append(term)

    def unregister(self, term: "ProxyHttpTerminator") -> None:
        with self._lock:
            try:
                self._terminators.remove(term)
            except ValueError:
                pass
            # Drop any pending body accumulators that were targeted at
            # this terminator. We don't track per-terminator ownership of
            # pending bodies, so the simplest conservative cleanup is to
            # leave the rest in place — they're keyed by request_id and
            # will be GC'd on fin or on client teardown.

    def _select(self, req: aether_pb2.ProxyHttpRequest) -> Optional["ProxyHttpTerminator"]:
        with self._lock:
            terminators = list(self._terminators)
        path = _split_path_query(req.path)[0]
        for term in terminators:
            if term._accepts_path(path):
                return term
        return None

    async def handle_request(self, req: aether_pb2.ProxyHttpRequest) -> None:
        # Chunked-request: register accumulator, wait for body.
        if req.body_chunked:
            with self._lock:
                self._pending[req.request_id] = _PendingRequest(header=req)
            return
        # Inline body — schedule the FastAPI/handler dispatch on its own
        # task so callers (the SDK listen loop or stream filter that
        # invoked us) return immediately and can keep delivering other
        # downstream messages.  Awaiting ``_dispatch`` inline would block
        # the listen loop for the entire duration of the handler — which
        # is fatal because the handler itself often makes SDK calls (KV
        # ops via rate-limit middleware, sub-RPCs, etc.) whose responses
        # arrive on the same gRPC stream and would deadlock against the
        # blocked listener until each call's own timeout fires.
        self._spawn_dispatch(req, req.body)

    async def handle_body_chunk(self, chunk: aether_pb2.ProxyHttpBodyChunk) -> None:
        # Only request-direction chunks are interesting to the terminator.
        # Response-direction chunks are emitted, not consumed, by us.
        if not chunk.is_request:
            return
        with self._lock:
            pending = self._pending.get(chunk.request_id)
            if pending is None:
                # Frame for a request we never accepted — drop quietly.
                # Mirrors the Go terminator's silent-drop semantics.
                return
            if chunk.data:
                pending.chunks[chunk.seq] = chunk.data
                pending.total_bytes += len(chunk.data)
            if chunk.fin:
                pending.fin_seen = True
                self._pending.pop(chunk.request_id, None)
            if not pending.fin_seen:
                return

        # Reassemble body in seq order (mirror initiator's `_build_final`).
        ordered = sorted(pending.chunks.items())
        body = b"".join(data for _, data in ordered)
        # Header arrives first with body_chunked=True and empty body. Now
        # that we have the full body, dispatch with body_chunked=False
        # implicitly (we pass the assembled bytes directly).  Spawn a task
        # for the same reason as the inline-body path above — never block
        # the listen loop on user-handler execution.
        self._spawn_dispatch(pending.header, body)

    def _spawn_dispatch(
        self, req: aether_pb2.ProxyHttpRequest, body: bytes
    ) -> None:
        """Schedule ``_dispatch`` as an unawaited task and track it.

        We hold a strong reference to the task in ``_inflight_tasks`` so
        the event loop's weak-reference policy can't garbage-collect a
        running handler. The done-callback removes the task from the set
        once it finishes.
        """
        task = asyncio.create_task(self._dispatch(req, body))
        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    async def _dispatch(self, req: aether_pb2.ProxyHttpRequest, body: bytes) -> None:
        path, query = _split_path_query(req.path)
        term = self._select(req)
        if term is None:
            await self._send_error(
                req.request_id,
                aether_pb2.ProxyError.ACL_DENIED,
                f"no terminator permits {req.method} {path}",
            )
            return
        try:
            await term._dispatch(req, path, query, body)
        except Exception as exc:  # noqa: BLE001 — handler safety net
            logger.error(
                "ProxyHttpTerminator handler crashed for %s: %s\n%s",
                req.request_id,
                exc,
                traceback.format_exc(),
            )
            await self._send_error(
                req.request_id,
                aether_pb2.ProxyError.UPSTREAM_RESET,
                f"terminator handler crashed: {type(exc).__name__}",
            )

    async def _send_error(self, request_id: str, kind: int, message: str) -> None:
        resp = aether_pb2.ProxyHttpResponse(
            request_id=request_id,
            error=aether_pb2.ProxyError(kind=kind, message=message),
        )
        await self._send_upstream(aether_pb2.UpstreamMessage(proxy_http_response=resp))

    async def wait_idle(self) -> None:
        """Await all in-flight handler tasks until none remain.

        Handler dispatch runs on its own asyncio task (see ``_spawn_dispatch``)
        so that the listen loop never blocks on user code. Callers that need to
        observe handler side effects deterministically — graceful shutdown,
        tests — should await this method after submitting their last request.
        Re-entrant: if a handler spawns further dispatches, those are awaited
        too.
        """
        while True:
            with self._lock:
                tasks = list(self._inflight_tasks)
            if not tasks:
                return
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_upstream(self, msg: aether_pb2.UpstreamMessage) -> None:
        client = self._client
        if client is None:
            logger.warning("ProxyHttpTerminator: dispatcher has no client; dropping outbound frame")
            return
        # Async clients expose `_request_queue` (asyncio.Queue); sync
        # clients expose `request_queue` (queue.Queue). The terminator is
        # async-first but tolerant of either.
        async_q = getattr(client, "_request_queue", None)
        if async_q is not None and isinstance(async_q, asyncio.Queue):
            await async_q.put(msg)
            return
        sync_q = getattr(client, "request_queue", None)
        if sync_q is not None:
            sync_q.put(msg)
            return
        logger.error("ProxyHttpTerminator: client has no usable request queue; dropping frame")


def _get_terminator_dispatcher(client: Any) -> _TerminatorDispatcher:
    d = getattr(client, "_proxy_terminator_dispatcher", None)
    if d is None:
        d = _TerminatorDispatcher()
        client._proxy_terminator_dispatcher = d
    d.attach_client(client)
    return d


def _split_path_query(raw_path: str) -> "tuple[str, str]":
    """Split a request URL path into ``(path, query)``.

    The Go terminator keeps the raw envelope path intact and lets
    ``net/http`` parse it on the backend side. We split here so the ASGI
    bridge can build a scope without re-parsing — the Python ASGI spec
    treats path and query string as separate fields.
    """
    if not raw_path:
        return "/", ""
    if "?" in raw_path:
        path, _, query = raw_path.partition("?")
        return path or "/", query
    return raw_path, ""


# ---------------------------------------------------------------------------
# ProxyHttpTerminator
# ---------------------------------------------------------------------------


class ProxyHttpTerminator:
    """Server-side terminator: receives ProxyHttpRequest envelopes and serves them.

    Usage::

        async def handler(req: MintedRequest) -> ProxyHttpResponse:
            ...

        terminator = ProxyHttpTerminator(
            client=service_client,
            handler=handler,
            allow_paths=["/v1/*", "/healthz"],
            header_mode="strict",
        )
        await terminator.start()
        ...
        await terminator.stop()

    Behaviour mirrors the Go terminator at
    ``oss-repo/server/internal/proxysidecar/terminator.go``:
    - inbound ``ProxyHttpRequest`` envelopes are matched against
      ``allow_paths``. Denied requests produce an ``ACL_DENIED`` error
      response and the handler is NOT invoked.
    - chunked-body requests reassemble across ``ProxyHttpBodyChunk``
      frames keyed by ``request_id``.
    - in ``strict`` mode all inbound ``X-Auth-*`` / ``X-Aether-*`` headers
      are stripped and fresh ``X-Auth-*`` headers are minted from the
      envelope's ``AuthorizationContext`` before the handler runs.
    - response bodies larger than ``body_chunk_size`` are split into
      ``ProxyHttpBodyChunk`` frames (mirrors the 256 KB inline cap on the
      Go side). Smaller bodies travel inline in the
      ``ProxyHttpResponse``.
    """

    def __init__(
        self,
        client: Any,
        handler: Handler,
        allow_paths: Optional[Sequence[str]] = None,
        header_mode: HeaderMode = "strict",
        body_chunk_size: int = PROXY_BODY_CHUNK_SIZE,
        resolver: Optional[AuthorityResolverProtocol] = None,
        obo_policy: OBOPolicy = "require_resolver",
    ) -> None:
        """Construct a terminator.

        Args:
            resolver: Optional :class:`AuthorityResolverProtocol`. When set
                AND ``header_mode="strict"``, OBO requests have their minted
                ``X-Auth-*`` set extended with grant-derived fields
                (``X-Auth-Max-Access-Level``, ``X-Auth-Workspace-Scope``,
                ``X-Auth-Audience-*``, and authoritative
                ``X-Auth-Root-Subject-*``).  Direct-mode requests never
                hit the resolver.
            obo_policy: How strict mode handles on_behalf_of requests:
                ``"require_resolver"`` (default, safe) rejects OBO with
                ``ACL_DENIED`` when no resolver is configured or when the
                resolver returns ``None``. ``"allow_partial"`` lets OBO
                fall through with only the wire-derived header set (Phase
                2a behaviour) — appropriate when downstream does not
                enforce scope/audience.  Direct-mode requests are not
                affected.
        """
        if header_mode not in ("strict", "passthrough"):
            raise ValueError(
                f"unsupported header_mode {header_mode!r}; expected 'strict' or 'passthrough'"
            )
        if body_chunk_size <= 0:
            raise ValueError("body_chunk_size must be positive")
        if obo_policy not in ("require_resolver", "allow_partial"):
            raise ValueError(
                f"unsupported obo_policy {obo_policy!r}; "
                f"expected 'require_resolver' or 'allow_partial'"
            )

        _ensure_terminator_hooks_installed()

        self._client = client
        self._handler = handler
        self._allow_paths: List[str] = list(allow_paths) if allow_paths else ["/*"]
        self._header_mode = header_mode
        self._body_chunk_size = body_chunk_size
        self._resolver = resolver
        self._obo_policy: OBOPolicy = obo_policy
        self._dispatcher = _get_terminator_dispatcher(client)
        self._started = False

    @property
    def allow_paths(self) -> List[str]:
        return list(self._allow_paths)

    @property
    def header_mode(self) -> HeaderMode:
        return self._header_mode

    async def start(self) -> None:
        """Register this terminator with the client's dispatcher.

        Non-blocking. After ``start`` returns, inbound proxy requests
        whose path matches ``allow_paths`` are routed to the handler.
        """
        if self._started:
            return
        self._dispatcher.register(self)
        self._started = True
        logger.info(
            "ProxyHttpTerminator registered (header_mode=%s, allow_paths=%s)",
            self._header_mode,
            self._allow_paths,
        )

    async def stop(self) -> None:
        """Unregister this terminator. Safe to call multiple times."""
        if not self._started:
            return
        self._dispatcher.unregister(self)
        self._started = False
        logger.info("ProxyHttpTerminator unregistered")

    # ----- internal helpers used by the dispatcher -----

    def _accepts_path(self, path: str) -> bool:
        return _path_allowed(self._allow_paths, path)

    async def _dispatch(
        self,
        req: aether_pb2.ProxyHttpRequest,
        path: str,
        query: str,
        body: bytes,
    ) -> None:
        # Build the MintedRequest visible to the handler.
        inbound_headers: Dict[str, str] = dict(req.headers)
        if self._header_mode == "strict":
            sanitized = _strip_inbound_identity_headers(inbound_headers)
            authz = req.authorization if req.HasField("authorization") else None
            sanitized.update(_mint_auth_headers(authz))
            # Overlay actor headers from the client's own gateway identity.
            # The actor reflects the authenticated principal that opened the
            # gateway connection — equivalent to Go's terminator deriving
            # actor-type/id from the session (see identityheaders.MintInto).
            identity = getattr(self._client, "identity", None)
            actor_type: Optional[str] = None
            actor_id: Optional[str] = None
            if identity is not None:
                actor_type = identity.principal_type
                actor_id = identity.actor_id
                sanitized[_HDR_ACTOR_TYPE] = actor_type
                sanitized[_HDR_ACTOR_ID] = actor_id

            # Phase 3.5c: extended OBO header overlay via authority resolver.
            # The wire envelope only carries authority_mode, subject, and
            # grant_id; the gateway is the trusted source for max access
            # level, workspace scope, audience, and root-subject.  In strict
            # mode we MUST validate the grant before forwarding to the
            # backend — otherwise downstream services would treat the
            # actor's connection access as the request's access ceiling
            # (which is broader than the grant).
            if authz is not None and authz.authority_mode == _AUTHORITY_MODE_OBO and authz.grant_id:
                subject = authz.subject if authz.HasField("subject") else None
                subject_type = subject.principal_type if subject is not None else ""
                subject_id = subject.principal_id if subject is not None else ""
                # Fast path: the gateway has already validated the grant
                # under the calling delegate (during proxyACLCheck) and
                # stamped the projected ResolvedAuthorityInfo onto
                # ``authz.resolved`` before publishing.  Trust it and skip
                # the resolver RPC.  That round-trip would otherwise fail
                # acl.ResolveAuthority's actor==delegate check anyway,
                # because this terminator is the audience, not the
                # delegate.  Trust anchor: the terminator's gRPC
                # connection is mTLS-authenticated to the same gateway
                # that authenticated the originating delegate; within one
                # Aether cluster this is sufficient.
                if authz.HasField("resolved"):
                    info = _resolved_authority_from_proto(authz, authz.resolved)
                    _overlay_resolved_authority(sanitized, info)
                elif self._resolver is None:
                    if self._obo_policy == "require_resolver":
                        await self._send_error(
                            req.request_id,
                            aether_pb2.ProxyError.ACL_DENIED,
                            "on_behalf_of request requires authority resolver "
                            "(terminator started without one and gateway did "
                            "not stamp resolved info on the request)",
                        )
                        return
                    # else: allow_partial — fall through with whatever
                    # _mint_auth_headers produced.
                else:
                    info = await self._resolver.resolve(
                        authz.grant_id,
                        subject_type,
                        subject_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                    )
                    if info is None:
                        if self._obo_policy == "require_resolver":
                            await self._send_error(
                                req.request_id,
                                aether_pb2.ProxyError.ACL_DENIED,
                                "authority resolver denied or did not return a grant",
                            )
                            return
                        # else: allow_partial — fall through.
                    else:
                        _overlay_resolved_authority(sanitized, info)
            request_headers = sanitized
        else:
            request_headers = inbound_headers

        minted = MintedRequest(
            method=req.method,
            path=path,
            query=query,
            headers=request_headers,
            body=body,
            request_id=req.request_id,
            authorization=(
                req.authorization if req.HasField("authorization") else None
            ),
            app_workspace=req.app_workspace,
        )

        # Streaming-response path is not yet implemented (out of scope per
        # Sub-phase 2a). Surface a clean error so callers know to fall
        # back to the bounded path or the Go sidecar in the meantime.
        if req.stream_response_indefinitely:
            await self._send_error(
                req.request_id,
                aether_pb2.ProxyError.UNKNOWN,
                "stream_response_indefinitely not supported by Python terminator yet",
            )
            return

        try:
            result = await self._handler(minted)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "ProxyHttpTerminator: handler raised for %s %s: %s\n%s",
                req.method,
                path,
                exc,
                traceback.format_exc(),
            )
            await self._send_response(
                aether_pb2.ProxyHttpResponse(
                    request_id=req.request_id,
                    status_code=500,
                    body=b"internal server error",
                )
            )
            return

        resp = _coerce_response(result, req.request_id)
        await self._send_response(resp)

    async def _send_response(self, resp: aether_pb2.ProxyHttpResponse) -> None:
        # Errors and small bodies travel inline. Otherwise emit a header
        # frame with body_chunked=true followed by chunk frames.
        if resp.HasField("error") or len(resp.body) <= self._body_chunk_size:
            await self._dispatcher._send_upstream(
                aether_pb2.UpstreamMessage(proxy_http_response=resp)
            )
            return

        body = resp.body
        # Header frame: empty body, body_chunked=true.
        header = aether_pb2.ProxyHttpResponse()
        header.CopyFrom(resp)
        header.body = b""
        header.body_chunked = True
        await self._dispatcher._send_upstream(
            aether_pb2.UpstreamMessage(proxy_http_response=header)
        )

        # Body chunks. Last frame carries fin=true.
        request_id = resp.request_id
        seq = 0
        for offset in range(0, len(body), self._body_chunk_size):
            piece = body[offset : offset + self._body_chunk_size]
            is_last = (offset + self._body_chunk_size) >= len(body)
            await self._dispatcher._send_upstream(
                aether_pb2.UpstreamMessage(
                    proxy_http_body_chunk=aether_pb2.ProxyHttpBodyChunk(
                        request_id=request_id,
                        is_request=False,
                        seq=seq,
                        data=piece,
                        fin=is_last,
                    )
                )
            )
            seq += 1

    async def _send_error(self, request_id: str, kind: int, message: str) -> None:
        await self._dispatcher._send_error(request_id, kind, message)


def _coerce_response(
    result: HandlerResult, request_id: str
) -> aether_pb2.ProxyHttpResponse:
    """Normalise a handler return into a ``ProxyHttpResponse`` envelope."""
    if isinstance(result, aether_pb2.ProxyHttpResponse):
        # Stamp request_id if the handler forgot — common ergonomic slip.
        if not result.request_id:
            result.request_id = request_id
        return result

    # Treat as (status, headers, body) triple.
    try:
        status, headers, body = result  # type: ignore[misc]
    except (TypeError, ValueError) as exc:
        raise TypeError(
            "ProxyHttpTerminator handler must return ProxyHttpResponse or "
            "(status:int, headers:Mapping[str,str], body:bytes)"
        ) from exc
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise TypeError("response body must be bytes")
    resp = aether_pb2.ProxyHttpResponse(
        request_id=request_id,
        status_code=int(status),
        body=bytes(body),
    )
    if headers:
        for k, v in headers.items():
            resp.headers[k] = v
    return resp


# ---------------------------------------------------------------------------
# Hook installation (mirrors proxy.py::_ensure_hooks_installed)
# ---------------------------------------------------------------------------


def _ensure_terminator_hooks_installed() -> None:
    """Install class-level wrappers on BaseAetherClient/BaseAsyncAetherClient.

    Idempotent — safe to call from every ``ProxyHttpTerminator.__init__``.
    Coexists with the initiator-side hook from :mod:`proxy`: each hook
    intercepts a disjoint set of payload types, so installation order is
    irrelevant.
    """
    from . import client as _client_mod
    from . import client_async as _client_async_mod

    base_sync = _client_mod.BaseAetherClient
    base_async = _client_async_mod.BaseAsyncAetherClient

    if not getattr(base_sync, "_proxy_terminator_hook_installed", False):
        _install_sync_terminator_hook(base_sync)
        base_sync._proxy_terminator_hook_installed = True

    if not getattr(base_async, "_proxy_terminator_hook_installed", False):
        _install_async_terminator_hook(base_async)
        base_async._proxy_terminator_hook_installed = True


def _install_sync_terminator_hook(base_sync: type) -> None:
    """Wrap ``BaseAetherClient._do_connect`` to install a sync terminator filter.

    The terminator path is async-first; the sync hook exists for
    completeness so that a sync client created in a process that later
    instantiates a ``ProxyHttpTerminator`` does not silently drop frames.
    Sync handlers run on a private event loop dedicated to the
    terminator dispatch task.
    """
    original_do_connect = base_sync._do_connect

    def _patched_do_connect(self, init_msg, target):  # type: ignore[no-untyped-def]
        # Defer to whatever earlier hook is in place (the initiator hook
        # patches this same slot). Then wrap the resulting stream.
        original_do_connect(self, init_msg, target)
        # Sync terminator support is a follow-up — for now we just record
        # the connection so async-side terminators created against the
        # same module-shared state work. The vast majority of Python
        # services using this terminator are async.

    base_sync._do_connect = _patched_do_connect  # type: ignore[assignment]
    base_sync._original_do_connect_terminator = original_do_connect  # type: ignore[attr-defined]


def _install_async_terminator_hook(base_async: type) -> None:
    """Wrap ``BaseAsyncAetherClient._do_connect`` to install an async terminator filter.

    The wrapping order is: ``_install_async_hook`` (proxy.py) wraps
    ``_do_connect`` first to set ``self._stream`` to an
    ``_AsyncResponseFilter``. We then re-wrap that stream in a
    ``_AsyncTerminatorFilter`` that intercepts inbound terminator frames.
    """
    # Snapshot the current _do_connect, which may itself already be a
    # patched version (e.g. proxy.py's). We compose by calling through.
    previous_do_connect = base_async._do_connect

    async def _patched_do_connect(self, init_msg, target):  # type: ignore[no-untyped-def]
        await previous_do_connect(self, init_msg, target)
        # ``self._stream`` is whatever the previous hook installed (in
        # the typical case: proxy.py's ``_AsyncResponseFilter``). Wrap it
        # again to peel off terminator frames before they reach
        # ``_listen_loop``.
        inner = self._stream
        if not isinstance(inner, _AsyncTerminatorFilter):
            self._stream = _AsyncTerminatorFilter(self, inner)

    base_async._do_connect = _patched_do_connect  # type: ignore[assignment]
    base_async._original_do_connect_terminator = previous_do_connect  # type: ignore[attr-defined]


class _AsyncTerminatorFilter:
    """Async iterator wrapper that intercepts terminator-side frames.

    Inbound payload routing:
    - ``proxy_http_request`` → terminator dispatcher; consumed.
    - ``proxy_http_body_chunk`` with ``is_request=true`` → terminator
      dispatcher; consumed.
    - everything else → forwarded to the next layer (which is typically
      the proxy.py initiator filter, then ``_listen_loop``).

    The dispatcher's ``handle_*`` methods are async; we await them inline
    so frame ordering is preserved and back-pressure flows through to the
    gateway. A slow handler will pause inbound delivery — same semantics
    as the Go runner's ``OnProxyHttpRequest`` callback.
    """

    def __init__(self, client: Any, inner: Any) -> None:
        self._client = client
        self._inner = inner
        self._inner_iter: Optional[Any] = None
        # Resolved lazily — terminator may not be active yet at connect time.
        self._dispatcher: Optional[_TerminatorDispatcher] = None

    def _get_dispatcher(self) -> Optional[_TerminatorDispatcher]:
        if self._dispatcher is None:
            self._dispatcher = getattr(
                self._client, "_proxy_terminator_dispatcher", None
            )
        return self._dispatcher

    def __aiter__(self) -> "_AsyncTerminatorFilter":
        return self

    async def __anext__(self):
        if self._inner_iter is None:
            # The inner may be either a raw grpc.aio call (which exposes
            # __aiter__ but not __anext__) or another filter (which
            # exposes both). Resolve once and cache.
            inner_aiter = getattr(self._inner, "__aiter__", None)
            if inner_aiter is not None:
                self._inner_iter = inner_aiter()
            else:
                self._inner_iter = self._inner
        while True:
            msg = await self._inner_iter.__anext__()
            payload_type = msg.WhichOneof("payload")
            if payload_type == "proxy_http_request":
                dispatcher = self._get_dispatcher()
                if dispatcher is not None and dispatcher._terminators:
                    await dispatcher.handle_request(msg.proxy_http_request)
                    continue
                # No terminator registered — fall through and let
                # _listen_loop drop the frame quietly.
            elif payload_type == "proxy_http_body_chunk":
                chunk = msg.proxy_http_body_chunk
                if chunk.is_request:
                    dispatcher = self._get_dispatcher()
                    if dispatcher is not None and dispatcher._terminators:
                        await dispatcher.handle_body_chunk(chunk)
                        continue
                    # No terminator — drop the request-direction chunk.
                    continue
                # Response-direction chunk: not ours, fall through to
                # whatever earlier filter (initiator) wants to handle it.
            return msg

    def cancel(self):  # pass-through used by client.disconnect()
        return self._inner.cancel()


__all__ = [
    "PROXY_BODY_CHUNK_SIZE",
    "ProxyHttpTerminator",
    "MintedRequest",
    "ProxyHttpResponse",
    "ProxyHttpRequest",
    "ProxyHttpBodyChunk",
    "ProxyError",
    "AuthorizationContext",
    "HeaderMode",
    "OBOPolicy",
    "Handler",
    "HandlerResult",
]