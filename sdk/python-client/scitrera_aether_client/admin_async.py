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
    # Role / Group Operations
    # ------------------------------------------------------------------
    #
    # A group is a named collection of principals; a role is a named
    # permission bundle (its permissions are granted with
    # :meth:`create_acl_rule` using ``principal_type="role"``,
    # ``principal_id=<role name>``). Membership/assignment edges are resolved
    # transitively and combined additively at evaluation time. These mirror
    # the Go SDK helpers in ``sdk/go/aether/admin_roles.go``.

    async def create_role(self,
                          name: str,
                          description: str = "",
                          created_by: str = "",
                          metadata: Optional[Dict[str, str]] = None,
                          timeout: float = 10.0):
        """Create a named role.

        Grant its permissions with :meth:`create_acl_rule` using
        ``principal_type="role"``, ``principal_id=<role name>``.

        Note:
            Creating a role that already exists returns a response with
            ``success=False`` and an ``error`` like ``ErrRoleExists`` rather
            than raising; callers decide how to treat that.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.CREATE_ROLE,
            role_request=aether_pb2.ACLRoleRequest(
                name=name,
                description=description,
                created_by=created_by,
                metadata=metadata or {},
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def delete_role(self, name: str, timeout: float = 10.0):
        """Delete a role (and its permission rules) by name."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.DELETE_ROLE,
            name=name,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def get_role(self, name: str, timeout: float = 10.0):
        """Fetch a role by name."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.GET_ROLE,
            name=name,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def list_roles(self, timeout: float = 10.0):
        """List all roles."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.LIST_ROLES,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def assign_role(self,
                          name: str,
                          assignee_type: str,
                          assignee_id: str,
                          granted_by: str = "",
                          expires_at: int = 0,
                          timeout: float = 10.0):
        """Assign (or refresh) a role to a principal or group.

        ``assignee_type`` is a principal type or ``"group"``. ``expires_at``
        is Unix seconds; ``0`` means no expiry.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.ASSIGN_ROLE,
            name=name,
            assignment_request=aether_pb2.ACLRoleAssignmentRequest(
                assignee_type=assignee_type,
                assignee_id=assignee_id,
                granted_by=granted_by,
                expires_at=expires_at,
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def unassign_role(self,
                            name: str,
                            assignee_type: str,
                            assignee_id: str,
                            timeout: float = 10.0):
        """Remove a role assignment from a principal or group."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.UNASSIGN_ROLE,
            name=name,
            principal=aether_pb2.PrincipalRef(
                principal_type=assignee_type,
                principal_id=assignee_id,
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def list_role_assignments(self, name: str, timeout: float = 10.0):
        """List the direct assignees of a role."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.LIST_ROLE_ASSIGNMENTS,
            name=name,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def list_principal_roles(self,
                                   principal_type: str,
                                   principal_id: str,
                                   timeout: float = 10.0):
        """List the roles directly assigned to a principal."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.LIST_PRINCIPAL_ROLES,
            principal=aether_pb2.PrincipalRef(
                principal_type=principal_type,
                principal_id=principal_id,
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def create_group(self,
                           name: str,
                           description: str = "",
                           created_by: str = "",
                           metadata: Optional[Dict[str, str]] = None,
                           timeout: float = 10.0):
        """Create a named group.

        Note:
            Creating a group that already exists returns a response with
            ``success=False`` and an ``error`` rather than raising; callers
            decide how to treat that.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.CREATE_GROUP,
            group_request=aether_pb2.ACLGroupRequest(
                name=name,
                description=description,
                created_by=created_by,
                metadata=metadata or {},
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def delete_group(self, name: str, timeout: float = 10.0):
        """Delete a group by name."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.DELETE_GROUP,
            name=name,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def get_group(self, name: str, timeout: float = 10.0):
        """Fetch a group by name."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.GET_GROUP,
            name=name,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def list_groups(self, timeout: float = 10.0):
        """List all groups."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.LIST_GROUPS,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def add_group_member(self,
                               name: str,
                               member_type: str,
                               member_id: str,
                               granted_by: str = "",
                               expires_at: int = 0,
                               timeout: float = 10.0):
        """Add (or refresh) a member of a group.

        ``member_type`` is a principal type or ``"group"`` (for nesting).
        ``expires_at`` is Unix seconds; ``0`` means no expiry.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.ADD_GROUP_MEMBER,
            name=name,
            member_request=aether_pb2.ACLGroupMemberRequest(
                member_type=member_type,
                member_id=member_id,
                granted_by=granted_by,
                expires_at=expires_at,
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def remove_group_member(self,
                                  name: str,
                                  member_type: str,
                                  member_id: str,
                                  timeout: float = 10.0):
        """Remove a member from a group."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.REMOVE_GROUP_MEMBER,
            name=name,
            principal=aether_pb2.PrincipalRef(
                principal_type=member_type,
                principal_id=member_id,
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def list_group_members(self, name: str, timeout: float = 10.0):
        """List the direct members of a group."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.LIST_GROUP_MEMBERS,
            name=name,
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def list_principal_groups(self,
                                    principal_type: str,
                                    principal_id: str,
                                    timeout: float = 10.0):
        """List the groups a principal is a direct member of."""
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.LIST_PRINCIPAL_GROUPS,
            principal=aether_pb2.PrincipalRef(
                principal_type=principal_type,
                principal_id=principal_id,
            ),
        )
        return await self._client.acl_op(op, timeout=timeout)

    async def explain_access(self,
                             principal_type: str,
                             principal_id: str,
                             resource_type: str,
                             resource_id: str,
                             required_level: int = 0,
                             timeout: float = 10.0):
        """Explain how a principal's effective access to a resource is decided.

        Resolves the subject set (self + transitive groups/roles), every rule
        that matched, and the resulting decision. This does not gate access,
        but the gateway records an ``explain_access`` audit event attributing
        the call to the connected principal. ``required_level`` is the
        threshold the decision is compared against (``0`` = NONE).

        Read the result from the response's ``explanation`` field
        (:class:`~aether_pb2.ACLAccessExplanationInfo`), whose ``allowed`` and
        ``effective_access_level`` fields carry the resolved decision.
        """
        op = aether_pb2.ACLOperation(
            op=aether_pb2.ACLOperation.EXPLAIN_ACCESS,
            principal=aether_pb2.PrincipalRef(
                principal_type=principal_type,
                principal_id=principal_id,
            ),
            resource_type=resource_type,
            resource_id=resource_id,
            required_level=required_level,
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
