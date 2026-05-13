"""
Tests for the sync and async AdminClient surfaces.

These exercise one happy-path per category (workspace, agent, ACL, token,
workflow) for both clients. They mirror the patterns used in
``test_client.py`` / ``test_client_async.py``: prime the per-op response
queue with a sentinel object so the underlying primitive returns
immediately, then inspect the request queue for the correct
``UpstreamMessage`` payload.

The session-op helpers on the async surface are routed through the
existing async ``session_op`` primitive; the request queue check confirms
the upstream tag, not the response.
"""
from __future__ import annotations

import asyncio
import queue as _queue

import pytest

from scitrera_aether_client import AdminClient, AsyncAdminClient
from scitrera_aether_client.client import AgentClient
from scitrera_aether_client.client_async import AsyncAgentClient
from scitrera_aether_client.proto import aether_pb2


# =============================================================================
# Smoke
# =============================================================================

class TestAdminClientSmoke:
    """Sanity checks for class shape and exports."""

    def test_admin_client_exported(self):
        from scitrera_aether_client import AdminClient as Exported
        assert Exported is AdminClient

    def test_async_admin_client_exported(self):
        from scitrera_aether_client import AsyncAdminClient as Exported
        assert Exported is AsyncAdminClient

    def test_admin_client_wraps_client(self, agent_client: AgentClient):
        admin = AdminClient(agent_client)
        assert admin._client is agent_client

    def test_async_admin_client_wraps_client(self, async_agent_client: AsyncAgentClient):
        admin = AsyncAdminClient(async_agent_client)
        assert admin._client is async_agent_client


# =============================================================================
# Sync happy-path: one per category
# =============================================================================

class TestSyncAdminClientHappyPath:
    """One happy-path test per category against the sync AdminClient."""

    def test_create_workspace_queues_upstream(self, agent_client: AgentClient):
        agent_client.request_queue = _queue.Queue()
        agent_client._workspace_response_queue.put(object())

        admin = AdminClient(agent_client)
        admin.create_workspace(
            workspace_id="ws-1",
            display_name="Workspace One",
            metadata={"env": "test"},
            timeout=1.0,
        )

        msg = agent_client.request_queue.get_nowait()
        assert msg.HasField("workspace_op")
        assert msg.workspace_op.op == aether_pb2.WorkspaceOperation.CREATE
        assert msg.workspace_op.workspace.workspace_id == "ws-1"
        assert msg.workspace_op.workspace.display_name == "Workspace One"
        assert dict(msg.workspace_op.workspace.metadata) == {"env": "test"}

    def test_get_agent_queues_upstream(self, agent_client: AgentClient):
        agent_client.request_queue = _queue.Queue()
        agent_client._agent_response_queue.put(object())

        admin = AdminClient(agent_client)
        admin.get_agent(implementation="my-impl", timeout=1.0)

        msg = agent_client.request_queue.get_nowait()
        assert msg.HasField("agent_op")
        assert msg.agent_op.op == aether_pb2.AgentOperation.GET
        assert msg.agent_op.implementation == "my-impl"

    def test_create_acl_rule_queues_upstream(self, agent_client: AgentClient):
        agent_client.request_queue = _queue.Queue()
        agent_client._acl_response_queue.put(object())

        admin = AdminClient(agent_client)
        admin.create_acl_rule(
            principal_type="user",
            principal_id="alice",
            resource_type="workspace",
            resource_id="ws-1",
            access_level=20,
            granted_by="ops",
            timeout=1.0,
        )

        msg = agent_client.request_queue.get_nowait()
        assert msg.HasField("acl_op")
        assert msg.acl_op.op == aether_pb2.ACLOperation.GRANT
        gr = msg.acl_op.grant_request
        assert gr.principal_type == "user"
        assert gr.principal_id == "alice"
        assert gr.resource_type == "workspace"
        assert gr.resource_id == "ws-1"
        assert gr.access_level == 20
        assert gr.granted_by == "ops"

    def test_create_token_queues_upstream(self, agent_client: AgentClient):
        import threading

        agent_client._stop_event = threading.Event()
        agent_client.request_queue = _queue.Queue()

        admin = AdminClient(agent_client)

        def call():
            admin.create_token(
                name="ci-token",
                principal_type="agent",
                scopes=["read"],
                timeout=0.2,
            )

        t = threading.Thread(target=call)
        t.start()
        msg = agent_client.request_queue.get(timeout=0.5)
        t.join(timeout=1)

        assert msg.HasField("token_op")
        assert msg.token_op.op == aether_pb2.TokenOperation.CREATE
        assert msg.token_op.create_request.name == "ci-token"
        assert msg.token_op.create_request.principal_type == "agent"
        assert list(msg.token_op.create_request.scopes) == ["read"]
        assert msg.token_op.request_id != ""

    def test_list_workflow_rules_queues_upstream(self, agent_client: AgentClient):
        import threading

        agent_client._stop_event = threading.Event()
        agent_client.request_queue = _queue.Queue()

        admin = AdminClient(agent_client)

        def call():
            admin.list_workflow_rules(workspace="ws-1", timeout=0.2)

        t = threading.Thread(target=call)
        t.start()
        msg = agent_client.request_queue.get(timeout=0.5)
        t.join(timeout=1)

        assert msg.HasField("workflow_op")
        assert msg.workflow_op.op == aether_pb2.WorkflowOperation.LIST_RULES
        assert msg.workflow_op.workspace == "ws-1"
        assert msg.workflow_op.request_id != ""


# =============================================================================
# Async happy-path: one per category
# =============================================================================

class TestAsyncAdminClientHappyPath:
    """One happy-path test per category against the async AdminClient."""

    @pytest.mark.asyncio
    async def test_list_workspaces_queues_upstream(self, async_agent_client: AsyncAgentClient):
        await async_agent_client._workspace_response_queue.put(object())

        admin = AsyncAdminClient(async_agent_client)
        await admin.list_workspaces(limit=5, offset=10, timeout=1.0)

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("workspace_op")
        assert msg.workspace_op.op == aether_pb2.WorkspaceOperation.LIST
        assert msg.workspace_op.filter.limit == 5
        assert msg.workspace_op.filter.offset == 10

    @pytest.mark.asyncio
    async def test_list_agents_queues_upstream(self, async_agent_client: AsyncAgentClient):
        await async_agent_client._agent_response_queue.put(object())

        admin = AsyncAdminClient(async_agent_client)
        await admin.list_agents(orchestrator_profile="k8s", limit=3, timeout=1.0)

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("agent_op")
        assert msg.agent_op.op == aether_pb2.AgentOperation.LIST
        assert msg.agent_op.filter.orchestrator_profile == "k8s"
        assert msg.agent_op.filter.limit == 3

    @pytest.mark.asyncio
    async def test_list_acl_rules_queues_upstream(self, async_agent_client: AsyncAgentClient):
        await async_agent_client._acl_response_queue.put(object())

        admin = AsyncAdminClient(async_agent_client)
        await admin.list_acl_rules(
            principal_type="user", resource_type="workspace", timeout=1.0,
        )

        msg = async_agent_client._request_queue.get_nowait()
        assert msg.HasField("acl_op")
        assert msg.acl_op.op == aether_pb2.ACLOperation.LIST_RULES
        assert msg.acl_op.rule_filter.principal_type == "user"
        assert msg.acl_op.rule_filter.resource_type == "workspace"

    @pytest.mark.asyncio
    async def test_revoke_token_queues_upstream(self, async_agent_client: AsyncAgentClient):
        admin = AsyncAdminClient(async_agent_client)

        # Use a background task so timeout terminates quickly even without a response.
        task = asyncio.create_task(admin.revoke_token(token_id="tok-xyz", timeout=0.2))
        msg = await asyncio.wait_for(async_agent_client._request_queue.get(), timeout=0.5)
        await task

        assert msg.HasField("token_op")
        assert msg.token_op.op == aether_pb2.TokenOperation.REVOKE
        assert msg.token_op.token_id == "tok-xyz"
        assert msg.token_op.request_id != ""

    @pytest.mark.asyncio
    async def test_get_workflow_rule_queues_upstream(self, async_agent_client: AsyncAgentClient):
        admin = AsyncAdminClient(async_agent_client)

        task = asyncio.create_task(
            admin.get_workflow_rule(rule_id="rule-1", workspace="ws-1", timeout=0.2)
        )
        msg = await asyncio.wait_for(async_agent_client._request_queue.get(), timeout=0.5)
        await task

        assert msg.HasField("workflow_op")
        assert msg.workflow_op.op == aether_pb2.WorkflowOperation.GET_RULE
        assert msg.workflow_op.id == "rule-1"
        assert msg.workflow_op.workspace == "ws-1"
