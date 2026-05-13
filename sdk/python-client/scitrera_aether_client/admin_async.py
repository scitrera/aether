"""
Asynchronous AdminClient for the Aether Python SDK.

``AsyncAdminClient`` mirrors :class:`scitrera_aether_client.admin.AdminClient`
but composes an already-connected ``BaseAsyncAetherClient`` (e.g.
:class:`AsyncAgentClient`, :class:`AsyncUserClient`,
:class:`AsyncServiceClient`) and exposes coroutine methods for all admin
operations supported over the gRPC streaming protocol.

The underlying client **must already be connected** before calling any
method on :class:`AsyncAdminClient`. Methods return either the response
proto or ``None`` if the per-call ``timeout`` elapses.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from .client_async import BaseAsyncAetherClient
from .proto import aether_pb2


class AsyncAdminClient:
    """Asynchronous administrative client.

    Example::

        import asyncio
        from scitrera_aether_client import (
            AsyncAgentClient,
            AsyncAdminClient,
            Credentials,
        )

        async def main():
            agent = AsyncAgentClient(
                workspace="default",
                implementation="admin-agent",
                specifier="ops-1",
                credentials=Credentials.api_key("my-admin-key"),
            )
            await agent.connect("localhost:50051")

            admin = AsyncAdminClient(agent)
            workspaces = await admin.list_workspaces()
            token = await admin.create_token(name="ci-token", principal_type="agent")
            print(token.plaintext_token)

        asyncio.run(main())
    """

    def __init__(self, client: BaseAsyncAetherClient):
        """Create an AsyncAdminClient backed by a connected async client."""
        self._client = client

    # ------------------------------------------------------------------
    # Token Operations
    # ------------------------------------------------------------------

    async def create_token(self,
                           name: str,
                           principal_type: str = "",
                           workspace_patterns: Optional[List[str]] = None,
                           scopes: Optional[List[str]] = None,
                           expires_in_hours: int = 0,
                           created_by: str = "",
                           timeout: float = 10.0):
        """Create a new API token. See :meth:`AdminClient.create_token`."""
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
        return await self._client.token_op(op, timeout=timeout)

    async def revoke_token(self, token_id: str, timeout: float = 10.0):
        """Revoke an API token by ID."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.REVOKE,
            token_id=token_id,
        )
        return await self._client.token_op(op, timeout=timeout)

    async def list_tokens(self,
                          include_revoked: bool = False,
                          limit: int = 0,
                          offset: int = 0,
                          timeout: float = 10.0):
        """List API tokens. See :meth:`AdminClient.list_tokens` for caveats."""
        op = aether_pb2.TokenOperation(
            op=aether_pb2.TokenOperation.LIST,
            filter=aether_pb2.TokenFilter(
                limit=limit,
                offset=offset,
                include_revoked=include_revoked,
            ),
        )
        return await self._client.token_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # ACL Operations
    # ------------------------------------------------------------------

    async def create_acl_rule(self,
                              principal_type: str,
                              principal_id: str,
                              resource_type: str,
                              resource_id: str,
                              access_level: int,
                              granted_by: str = "",
                              reason: str = "",
                              expires_at: int = 0,
                              timeout: float = 10.0):
        """Create an ACL rule granting access. See :meth:`AdminClient.create_acl_rule`."""
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
        return await self._client.acl_op(op, timeout=timeout)

    async def delete_acl_rule(self, rule_id: str, timeout: float = 10.0):
        """Delete (revoke) an ACL rule by ID."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.REVOKE,
            rule_id=rule_id,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def list_acl_rules(self,
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
        return await self._client.acl_op(op, timeout=timeout)

    async def get_fallback_policy(self, rule_category: str, timeout: float = 10.0):
        """Read the fallback policy for a rule category."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.GET_FALLBACK_POLICY,
            rule_category=rule_category,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def set_fallback_policy(self,
                                  rule_category: str,
                                  fallback_access_level: int,
                                  updated_by: str = "",
                                  timeout: float = 10.0):
        """Upsert the fallback policy for a rule category."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.SET_FALLBACK_POLICY,
            fallback_request=aether_pb2.ACLSetFallbackRequest(
                rule_category=rule_category,
                fallback_access_level=fallback_access_level,
                updated_by=updated_by,
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Workspace Operations
    # ------------------------------------------------------------------

    async def list_workspaces(self,
                              limit: int = 0,
                              offset: int = 0,
                              timeout: float = 10.0):
        """List workspaces."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.LIST,
            filter=aether_pb2.WorkspaceFilter(limit=limit, offset=offset),
        )
        return await self._client.workspace_op(op, timeout=timeout)

    async def create_workspace(self,
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
        return await self._client.workspace_op(op, timeout=timeout)

    async def update_workspace(self,
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
        return await self._client.workspace_op(op, timeout=timeout)

    async def delete_workspace(self, workspace_id: str, timeout: float = 10.0):
        """Delete a workspace by ID."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.DELETE,
            workspace_id=workspace_id,
        )
        return await self._client.workspace_op(op, timeout=timeout)

    async def get_workspace(self, workspace_id: str, timeout: float = 10.0):
        """Get a single workspace by ID."""
        op = aether_pb2.WorkspaceOperation(
            op=aether_pb2.WorkspaceOperation.GET,
            workspace_id=workspace_id,
        )
        return await self._client.workspace_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Agent Registry Operations
    # ------------------------------------------------------------------

    async def list_agents(self,
                          orchestrator_profile: str = "",
                          limit: int = 0,
                          offset: int = 0,
                          timeout: float = 10.0):
        """List registered agent implementations."""
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.LIST,
            filter=aether_pb2.AgentFilter(
                orchestrator_profile=orchestrator_profile,
                limit=limit,
                offset=offset,
            ),
        )
        return await self._client.agent_op(op, timeout=timeout)

    async def get_agent(self, implementation: str, timeout: float = 10.0):
        """Get the registration details for a specific agent implementation."""
        op = aether_pb2.AgentOperation(
            op=aether_pb2.AgentOperation.GET,
            implementation=implementation,
        )
        return await self._client.agent_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Workflow Operations (admin-flavored)
    # ------------------------------------------------------------------

    async def list_workflow_rules(self,
                                  workspace: str = "",
                                  timeout: float = 10.0):
        """List workflow engine rules."""
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.LIST_RULES,
            workspace=workspace,
        )
        return await self._client.workflow_op(op, timeout=timeout)

    async def get_workflow_rule(self,
                                rule_id: str,
                                workspace: str = "",
                                timeout: float = 10.0):
        """Get a workflow rule by ID."""
        op = aether_pb2.WorkflowOperation(
            op=aether_pb2.WorkflowOperation.GET_RULE,
            id=rule_id,
            workspace=workspace,
        )
        return await self._client.workflow_op(op, timeout=timeout)

    # ------------------------------------------------------------------
    # Session Operations
    # ------------------------------------------------------------------

    async def list_connections(self,
                               principal_type: str = "",
                               workspace: str = "",
                               limit: int = 0,
                               offset: int = 0,
                               timeout: float = 10.0):
        """List active gateway sessions (connections).

        Thin wrapper over the async client's :meth:`list_sessions` helper.
        """
        return await self._client.list_sessions(
            principal_type=principal_type,
            workspace=workspace,
            limit=limit,
            offset=offset,
            timeout=timeout,
        )

    async def disconnect_session(self,
                                 session_id: str,
                                 reason: str = "",
                                 timeout: float = 10.0):
        """Forcibly disconnect a session by session ID.

        Thin wrapper over the async client's :meth:`disconnect_session` helper.
        """
        return await self._client.disconnect_session(
            session_id=session_id,
            reason=reason,
            timeout=timeout,
        )

    # ------------------------------------------------------------------
    # Skipped TS surface
    # ------------------------------------------------------------------
    #
    # ``getHealth`` from the TypeScript AdminClient is not portable as-is:
    # it relies on a ``_adminQuery`` sentinel routed through
    # ``sendWorkspaceOperation`` that the gateway does not decode against
    # ``WorkspaceOperation``. The proto path (``AdminQuery``) is not yet
    # exposed as a primitive on either Python client. Use the REST admin
    # endpoints (or wait for an ``admin_query`` primitive) for health
    # queries.
