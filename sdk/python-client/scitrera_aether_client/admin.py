"""
Synchronous AdminClient for the Aether Python SDK.

``AdminClient`` wraps a connected ``BaseAetherClient`` (typically an
:class:`AgentClient`, :class:`UserClient`, or :class:`ServiceClient`) and
exposes named helper methods for the administrative operations available
over the gRPC streaming protocol:

* Token management (create / list / revoke)
* ACL rules and fallback policies
* Workspace CRUD
* Agent registry inspection
* Session operations (disconnect)

The underlying client **must already be connected** before calling any
method on :class:`AdminClient`. Methods block until the gateway sends the
correlated response or the per-call ``timeout`` elapses (in which case the
underlying primitive returns ``None``).

This module mirrors the TypeScript ``AdminClient`` surface from
``sdk/typescript/src/admin.ts`` using Python naming conventions
(``snake_case``).
"""

from __future__ import annotations

from typing import Dict, List, Optional

from .client import BaseAetherClient
from .proto import aether_pb2


class AdminClient:
    """Administrative client for managing an Aether gateway.

    ``AdminClient`` is a thin facade over a connected
    :class:`BaseAetherClient` instance. It does not own a connection of its
    own — pass in any already-connected sync client (typically an
    :class:`AgentClient`, :class:`UserClient`, or :class:`ServiceClient`)
    whose authenticated identity has admin permissions on the gateway.

    Example::

        from scitrera_aether_client import AgentClient, AdminClient, Credentials

        agent = AgentClient(
            workspace="default",
            implementation="admin-agent",
            specifier="ops-1",
            credentials=Credentials.api_key("my-admin-key"),
        )
        agent.connect("localhost:50051")

        admin = AdminClient(agent)

        workspaces = admin.list_workspaces()
        token = admin.create_token(name="ci-token", principal_type="agent")
        print(token.plaintext_token)
    """

    def __init__(self, client: BaseAetherClient):
        """Create an AdminClient backed by a connected sync client.

        Args:
            client: A connected ``BaseAetherClient`` instance.
        """
        self._client = client

    # ------------------------------------------------------------------
    # Token Operations
    # ------------------------------------------------------------------

    def create_token(self,
                     name: str,
                     principal_type: str = "",
                     workspace_patterns: Optional[List[str]] = None,
                     scopes: Optional[List[str]] = None,
                     expires_in_hours: int = 0,
                     created_by: str = "",
                     timeout: float = 10.0):
        """Create a new API token.

        Returns a ``TokenResponse`` whose ``plaintext_token`` field contains
        the freshly minted token. The plaintext is only returned at creation
        time.
        """
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
        return self._client.token_op(op, timeout=timeout)

    def revoke_token(self, token_id: str, timeout: float = 10.0):
        """Revoke an API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.REVOKE,
            token_id=token_id,
        )
        return self._client.token_op(op, timeout=timeout)

    def list_tokens(self,
                    include_revoked: bool = False,
                    limit: int = 0,
                    offset: int = 0,
                    timeout: float = 10.0):
        """List API tokens with optional filters.

        Note:
            The TS ``listTokens`` accepts a ``principalType`` filter, but the
            underlying ``TokenFilter`` proto does not currently expose that
            field. It is therefore not honored here (server-side filtering
            is not available).
        """
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.LIST,
            filter=aether_pb2.TokenFilter(
                limit=limit,
                offset=offset,
                include_revoked=include_revoked,
            ),
        )
        return self._client.token_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # ACL Operations
    # ------------------------------------------------------------------

    def create_acl_rule(self,
                        principal_type: str,
                        principal_id: str,
                        resource_type: str,
                        resource_id: str,
                        access_level: int,
                        granted_by: str = "",
                        reason: str = "",
                        expires_at: int = 0,
                        timeout: float = 10.0):
        """Create an ACL rule granting access.

        ``access_level`` uses the numeric tiers from ``internal/acl/types.go``:
        ``0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN, 50=SUPERADMIN``.

        Note:
            The TS surface uses a string ``permission`` and a ``metadata``
            map. The Python proto uses ``access_level`` (int) and ``granted_by``
            / ``reason`` fields. ``metadata`` is not supported by the
            ``ACLGrantRequest`` proto and is omitted here.
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
        return self._client.acl_op(op, timeout=timeout)

    def delete_acl_rule(self, rule_id: str, timeout: float = 10.0):
        """Delete (revoke) an ACL rule by ID."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.REVOKE,
            rule_id=rule_id,
        )
        return self._client.acl_op(op, timeout=timeout)

    def list_acl_rules(self,
                       principal_type: str = "",
                       principal_id: str = "",
                       resource_type: str = "",
                       resource_id: str = "",
                       limit: int = 0,
                       offset: int = 0,
                       timeout: float = 10.0):
        """List ACL rules with optional filters."""
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
        return self._client.acl_op(op, timeout=timeout)

    def get_fallback_policy(self, rule_category: str, timeout: float = 10.0):
        """Read the fallback policy for a rule category.

        ``rule_category`` is the canonical ``{principal_type}_{resource_type}``
        slug (e.g., ``"agent_kv_scope"``).
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.GET_FALLBACK_POLICY,
            rule_category=rule_category,
        )
        return self._client.acl_op(op, timeout=timeout)

    def set_fallback_policy(self,
                            rule_category: str,
                            fallback_access_level: int,
                            updated_by: str = "",
                            timeout: float = 10.0):
        """Upsert the fallback policy for a rule category.

        Use ``fallback_access_level=0`` to flip a category to default-deny.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.SET_FALLBACK_POLICY,
            fallback_request=aether_pb2.ACLSetFallbackRequest(
                rule_category=rule_category,
                fallback_access_level=fallback_access_level,
                updated_by=updated_by,
            ),
        )
        return self._client.acl_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Workspace Operations
    # ------------------------------------------------------------------

    def list_workspaces(self,
                        limit: int = 0,
                        offset: int = 0,
                        timeout: float = 10.0):
        """List workspaces."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.LIST,
            filter=aether_pb2.WorkspaceFilter(limit=limit, offset=offset),
        )
        return self._client.workspace_op(op, timeout=timeout)

    def create_workspace(self,
                         workspace_id: str,
                         display_name: str = "",
                         description: str = "",
                         metadata: Optional[Dict[str, str]] = None,
                         timeout: float = 10.0):
        """Create a new workspace."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.CREATE,
            workspace=aether_pb2.WorkspaceInfo(
                workspace_id=workspace_id,
                display_name=display_name,
                description=description,
                metadata=metadata or {},
            ),
        )
        return self._client.workspace_op(op, timeout=timeout)

    def update_workspace(self,
                         workspace_id: str,
                         display_name: str = "",
                         description: str = "",
                         metadata: Optional[Dict[str, str]] = None,
                         timeout: float = 10.0):
        """Update an existing workspace."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.UPDATE,
            workspace_id=workspace_id,
            workspace=aether_pb2.WorkspaceInfo(
                workspace_id=workspace_id,
                display_name=display_name,
                description=description,
                metadata=metadata or {},
            ),
        )
        return self._client.workspace_op(op, timeout=timeout)

    def delete_workspace(self, workspace_id: str, timeout: float = 10.0):
        """Delete a workspace by ID."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.DELETE,
            workspace_id=workspace_id,
        )
        return self._client.workspace_op(op, timeout=timeout)

    def get_workspace(self, workspace_id: str, timeout: float = 10.0):
        """Get a single workspace by ID."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.GET,
            workspace_id=workspace_id,
        )
        return self._client.workspace_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Agent Registry Operations
    # ------------------------------------------------------------------

    def list_agents(self,
                    orchestrator_profile: str = "",
                    limit: int = 0,
                    offset: int = 0,
                    timeout: float = 10.0):
        """List registered agent implementations.

        Note:
            The TS ``listAgents`` accepts a ``workspace`` filter, but the
            underlying ``AgentFilter`` proto only exposes
            ``orchestrator_profile``. ``workspace`` is therefore not
            honored here.
        """
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.LIST,
            filter=aether_pb2.AgentFilter(
                orchestrator_profile=orchestrator_profile,
                limit=limit,
                offset=offset,
            ),
        )
        return self._client.agent_op(op, timeout=timeout)

    def get_agent(self, implementation: str, timeout: float = 10.0):
        """Get the registration details for a specific agent implementation."""
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.GET,
            implementation=implementation,
        )
        return self._client.agent_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Workflow Operations (admin-flavored)
    # ------------------------------------------------------------------

    def list_workflow_rules(self,
                            workspace: str = "",
                            timeout: float = 10.0):
        """List workflow engine rules."""
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.LIST_RULES,
            workspace=workspace,
        )
        return self._client.workflow_op(op, timeout=timeout)

    def get_workflow_rule(self,
                          rule_id: str,
                          workspace: str = "",
                          timeout: float = 10.0):
        """Get a workflow rule by ID."""
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.GET_RULE,
            id=rule_id,
            workspace=workspace,
        )
        return self._client.workflow_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Skipped TS surface (no Python primitive)
    # ------------------------------------------------------------------
    #
    # The TypeScript AdminClient also exposes ``getHealth``, ``getConnections``,
    # and ``disconnectSession``. Those route through ``sendWorkspaceOperation``
    # with non-proto sentinel fields (``_adminQuery`` / ``_sessionOp``) that
    # the streaming gateway does not actually decode against ``WorkspaceOperation``.
    # The proper proto paths are ``AdminQuery`` and ``SessionOperation`` —
    # primitives that the sync Python client does not yet expose. Use the
    # async client (which has ``session_op`` / ``list_sessions`` /
    # ``disconnect_session`` helpers) or the REST admin endpoints for those
    # operations until the sync primitives are added.
