# Aether Python SDK

Aether Python SDK — client library for the Aether distributed control plane. Aether is a system for routing structured messages, tracking tasks, and managing connection lifecycles for agents, tasks, users, and other principals.

## Features

- **Sync and Async Support**: Both synchronous (threading-based) and asynchronous (asyncio-based) client implementations
- **Multiple Client Types**: Agent, Task, User, Orchestrator, WorkflowEngine, and MetricsBridge clients
- **Key-Value Store**: Hierarchical configuration store with multiple scopes (global, workspace, user, user-workspace)
- **Task Management**: Create and manage tasks with different assignment modes
- **Checkpointing**: Persist and restore agent/task state across restarts
- **Auto-Reconnection**: Configurable exponential backoff with automatic reconnection
- **TLS/mTLS Support**: Secure connections with optional mutual TLS authentication
- **Typed + Catch-all Handlers**: Register `on_chat_message`, `on_control_message`, etc. alongside `on_message` — both fire for matching messages

## Installation

```bash
pip install scitrera-aether-client
```

For development:

```bash
pip install scitrera-aether-client[dev]
```

## Quick Start

### Synchronous Client

```python
from scitrera_aether_client import AgentClient, CHAT

# Create an agent client
client = AgentClient(
    workspace="default",
    implementation="my-agent",
    specifier="agent-01"
)

# Set up message callback
def on_message(msg):
    print(f"Received from {msg.source_topic}: {msg.payload.decode()}")

client.on_message = on_message

# Connect to the gateway
client.connect("localhost:50051")

# Send a message to another agent
client.send_message_to_agent(
    workspace="default",
    implementation="other-agent",
    specifier="01",
    payload=b"Hello!"
)

# Keep running until interrupted
try:
    while True:
        import time
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    client.close()
```

### Asynchronous Client

```python
import asyncio
from scitrera_aether_client import AsyncAgentClient

async def main():
    client = AsyncAgentClient(
        workspace="default",
        implementation="my-async-agent",
        specifier="agent-01"
    )

    async def on_message(msg):
        print(f"Received: {msg.payload.decode()}")

    client.on_message = on_message

    await client.connect("localhost:50051")

    await client.send_message_to_agent(
        workspace="default",
        implementation="my-async-agent",
        specifier="agent-01",
        payload=b"Hello from async!"
    )

    # Wait until disconnected
    await client.wait_until_disconnected()

asyncio.run(main())
```

### Using Async Context Manager

```python
async with AsyncAgentClient("default", "my-agent", "01") as client:
    await client.connect("localhost:50051")
    await client.send_message_to_agent("default", "other", "01", b"Hello!")
    await asyncio.sleep(1)
# Connection automatically closed
```

## Client Types

### AgentClient / AsyncAgentClient

For long-running agent processes that need unique identities.

```python
from scitrera_aether_client import AgentClient

client = AgentClient(
    workspace="default",
    implementation="python-worker",
    specifier="worker-01"
)
```

### TaskClient / AsyncTaskClient

For task execution. Supports both unique (named) and non-unique (pooled) tasks.

```python
from scitrera_aether_client import TaskClient

# Unique task (named)
unique_task = TaskClient(
    workspace="default",
    implementation="data-processor",
    unique_specifier="job-123"
)

# Non-unique task (pooled, server assigns ID)
pooled_task = TaskClient(
    workspace="default",
    implementation="worker"
)
```

### UserClient / AsyncUserClient

For user session connections (e.g., from browser windows).

```python
from scitrera_aether_client import UserClient

client = UserClient(
    user_id="user-123",
    window_id="window-abc"
)
```

### OrchestratorClient / AsyncOrchestratorClient

For managing agent/task lifecycle and compute resources.

```python
from scitrera_aether_client import OrchestratorClient

client = OrchestratorClient(
    implementation="kubernetes-orchestrator",
    supported_profiles=["docker", "kubernetes"]
)
```

### WorkflowEngineClient / AsyncWorkflowEngineClient

For processing events and triggering downstream actions.

```python
from scitrera_aether_client import WorkflowEngineClient

client = WorkflowEngineClient()
```

### MetricsBridgeClient / AsyncMetricsBridgeClient

For collecting telemetry data from agents and tasks.

```python
from scitrera_aether_client import MetricsBridgeClient

client = MetricsBridgeClient()
```

### ServiceClient / AsyncServiceClient

For trusted backend intermediaries (e.g., app servers or WebSocket backends) that act on behalf of users.

```python
from scitrera_aether_client import ServiceClient, Credentials

client = ServiceClient(
    implementation="platform-server",
    specifier="pod-abc",
    credentials=Credentials.api_key(api_key)
)
```

`ServiceClient` differs from `AgentClient` in that it is workspace-less — it authenticates as itself and uses `AuthorizationContext` (on-behalf-of mode) to perform operations scoped to a user. Use `AgentClient` for long-running autonomous agents; use `ServiceClient` for backend services that proxy user actions.

---

## Principal Types

Aether defines the following principal types. Each maps to a dedicated client class in the Python SDK.

| Principal Type | Python SDK Class | Identity Format | Description |
|---------------|-----------------|-----------------|-------------|
| **Agent** | `AgentClient` / `AsyncAgentClient` | `ag::{workspace}::{impl}::{spec}` | Long-running autonomous process with a globally unique identity. Receives messages, can create tasks, and persists state via KV and checkpoints. |
| **Task** | `TaskClient` / `AsyncTaskClient` | `tu::{workspace}::{impl}::{spec}` (unique) or `ta::{workspace}::{impl}::{id}` (pooled) | Short-lived compute unit. Unique tasks have named identities; non-unique (pooled) tasks receive a server-assigned ID and participate in load-balanced dispatch via `tb.*` topics. |
| **User** | `UserClient` / `AsyncUserClient` | `us::{user_id}::{window_id}` | Represents a human user session (e.g., a browser window). Multiple tabs may connect simultaneously using different `window_id` values. Can also receive workspace-scoped messages via `uw::{user_id}::{workspace}`. |
| **Orchestrator** | `OrchestratorClient` / `AsyncOrchestratorClient` | `orc::{implementation}[::{specifier}]` | Manages compute lifecycle — receives `TaskAssignment` messages and launches agents or tasks on demand. Registers supported profiles to receive matching assignments. |
| **WorkflowEngine** | `WorkflowEngineClient` / `AsyncWorkflowEngineClient` | `wfe::shard0` | Singleton invariant; `Implementation` reserved for future multi-shard. Processes `EVENT`-type messages and triggers downstream automation. Has the broadest send permissions — can target any principal type. |
| **MetricsBridge** | `MetricsBridgeClient` / `AsyncMetricsBridgeClient` | `metrics::shard0` | Singleton invariant matching WFE sharding model; `Implementation` reserved for future multi-shard. Receive-only telemetry sink. Collects `METRIC`-type messages from agents and tasks; cannot send messages. |
| **Service** | `ServiceClient` / `AsyncServiceClient` | `sv::{impl}::{spec}` | Trusted backend intermediary. Workspace-less; authenticates as itself and performs privileged operations on behalf of users via `AuthorizationContext`. Use for app/WebSocket backends proxying user actions. |
| **Bridge** | *(not yet in Python SDK)* | `br::{impl}::{spec}` | Cross-workspace messaging integration (e.g., Discord, Teams, Email). Has no workspace component and can send to any workspace. Implemented as a standalone server (`cmd/msgbridge`). |

---

## Callbacks

All clients support the following callbacks:

| Callback | Description | Signature |
|----------|-------------|-----------|
| `on_message` | Every incoming message (catch-all) | `(msg: IncomingMessage) -> None` |
| `on_chat_message` | CHAT-typed messages | `(msg: IncomingMessage) -> None` |
| `on_control_message` | CONTROL-typed messages | `(msg: IncomingMessage) -> None` |
| `on_tool_call` | TOOL_CALL-typed messages | `(msg: IncomingMessage) -> None` |
| `on_event` | EVENT-typed messages | `(msg: IncomingMessage) -> None` |
| `on_metric` | METRIC-typed messages | `(msg: IncomingMessage) -> None` |
| `on_config` | Configuration snapshot received | `(config: ConfigSnapshot) -> None` |
| `on_signal` | Signal received | `(signal: Signal) -> None` |
| `on_error` | Error occurred | `(error: ErrorResponse) -> None` |
| `on_kv_response` | Async KV operation response | `(kv: KVResponse) -> None` |
| `on_task_assignment` | Task assigned (Orchestrators) | `(assignment: TaskAssignment) -> None` |
| `on_checkpoint_response` | Async checkpoint response | `(response: CheckpointResponse) -> None` |
| `on_connect` | Connection established | `() -> None` |
| `on_disconnect` | Connection lost | `(reason: str) -> None` |

Typed handlers (`on_chat_message`, etc.) and the catch-all `on_message` are independent. If both are registered, the typed handler fires first, then `on_message` fires as well. This matches the behavior of the Go and TypeScript SDKs.

For async clients, callbacks can be either sync or async functions:

```python
# Sync callback
def on_message(msg):
    print(msg.payload.decode())

# Async callback
async def on_message(msg):
    await process_message(msg)
```

## Messaging

### Message Types

```python
from scitrera_aether_client import CHAT, CONTROL, TOOL_CALL, EVENT, METRIC

# CHAT - Regular chat messages
client.send_message_to_agent(..., message_type=CHAT)

# CONTROL - Control/command messages
client.send_message_to_agent(..., message_type=CONTROL)

# EVENT - Events for workflow engine
client.send_event(payload)

# METRIC - Telemetry data for metrics bridge
client.send_metric(payload)
```

### Sending Messages

```python
# To a specific agent
client.send_message_to_agent(
    workspace="default",
    implementation="worker",
    specifier="01",
    payload=b"Hello!"
)

# To a task
client.send_message_to_task(
    workspace="default",
    implementation="processor",
    payload=b"Process this",
    unique_specifier="task-123"  # Optional for unique tasks
)

# To a user session
client.send_message_to_user_session(
    user_id="user-123",
    window_id="window-abc",
    payload=b"Notification"
)

# Broadcast to all agents in workspace
client.send_broadcast_to_agents(
    workspace="default",
    payload=b"Broadcast message"
)

# Send event (agents/tasks only)
client.send_event(b'{"event": "completed"}')

# Send metric (agents/tasks only)
client.send_metric(b'{"metric": "latency", "value": 42}')
```

## KV Operations

The KV store supports multiple scopes:

| Scope | Description | Required Parameters |
|-------|-------------|---------------------|
| `global` | Global configuration | None |
| `workspace` | Workspace-specific | `workspace` |
| `user` | User-specific | `user_id` |
| `user-workspace` | User + workspace scoped | `user_id`, `workspace` |

### Synchronous Client KV

```python
# Store a value (fire-and-forget)
client.kv_put(
    key="config/setting",
    value=b"value",
    scope="global"
)

# Store with workspace scope
client.kv_put(
    key="team/setting",
    value=b"team-value",
    scope="workspace",
    workspace="default"
)

# Get a value (response via callback)
client.kv_get(key="config/setting", scope="global")

# List keys
client.kv_list(key_prefix="config/", scope="global")

# Delete a key
client.kv_delete(key="config/old", scope="global")
```

### Async Client KV

Async clients support both fire-and-forget (`_nowait`) and awaitable operations:

```python
# Fire-and-forget
await client.kv_put_nowait(
    key="setting",
    value=b"value",
    scope="global"
)

# Await response
response = await client.kv_get(
    key="setting",
    scope="global",
    timeout=5.0
)
if response:
    print(f"Value: {response.value}")

# Put and await confirmation
response = await client.kv_put(
    key="setting",
    value=b"new-value",
    scope="global",
    timeout=5.0
)
```

## Task Creation

Create tasks with different assignment modes:

```python
from scitrera_aether_client import SELF_ASSIGN, TARGETED, POOL

# Self-assigned task (creator handles it)
client.create_task(
    task_type="data-processing",
    workspace="default",
    assignment_mode=SELF_ASSIGN,
    metadata={"priority": "high"}
)

# Targeted task (assign to specific agent)
client.create_task(
    task_type="specialized-work",
    workspace="default",
    assignment_mode=TARGETED,
    target_agent_id="ag.default.worker.specialist-01",
    launch_param_overrides={"memory": "4G"}
)

# Pool task (load-balanced to available workers)
client.create_task(
    task_type="batch-job",
    workspace="default",
    assignment_mode=POOL
)
```

## Checkpointing

Save and restore agent/task state:

### Synchronous

```python
# Save checkpoint
client.checkpoint_save(data=b"state data", key="my-checkpoint")

# Save and wait for confirmation
response = client.checkpoint_save_sync(
    data=b"state data",
    key="my-checkpoint",
    timeout=5.0
)

# Load checkpoint
response = client.checkpoint_load_sync(key="my-checkpoint", timeout=5.0)
if response and response.data:
    print(f"Restored state: {response.data}")

# List checkpoints
response = client.checkpoint_list_sync(timeout=5.0)
if response:
    print(f"Checkpoints: {response.keys}")

# Delete checkpoint
client.checkpoint_delete_sync(key="my-checkpoint", timeout=5.0)
```

### Asynchronous

```python
# Save and wait
response = await client.checkpoint_save(
    data=b"state data",
    key="my-checkpoint",
    timeout=5.0
)

# Load
response = await client.checkpoint_load(key="my-checkpoint", timeout=5.0)

# Fire-and-forget operations
await client.checkpoint_save_nowait(data=b"state", key="quick-save")
await client.checkpoint_delete_nowait(key="old-checkpoint")
```

## Admin Operations

The SDK exposes named admin helpers via `AdminClient` (sync) and
`AsyncAdminClient` (async). Both wrap an already-connected client whose
authenticated identity has admin permissions on the gateway. The wrappers
own no connection of their own — pass in any connected
`AgentClient` / `UserClient` / `ServiceClient` (or their async
equivalents).

Covered surface: workspace CRUD, agent registry inspection, ACL rule
management and fallback policies, API token lifecycle, and read-side
workflow rule queries. The async wrapper additionally forwards
`list_connections` / `disconnect_session` to the underlying session-op
primitive.

### Synchronous

```python
from scitrera_aether_client import AgentClient, AdminClient, Credentials

agent = AgentClient(
    workspace="default",
    implementation="admin-agent",
    specifier="ops-1",
    credentials=Credentials.api_key("my-admin-key"),
)
agent.connect("localhost:50051")

admin = AdminClient(agent)

# Workspaces
admin.create_workspace(workspace_id="ws-1", display_name="Workspace One")
workspaces = admin.list_workspaces(limit=50)

# Tokens
token = admin.create_token(name="ci-token", principal_type="agent")
print(token.plaintext_token)
admin.revoke_token(token_id=token.token.id)

# ACL
admin.create_acl_rule(
    principal_type="user",
    principal_id="alice",
    resource_type="workspace",
    resource_id="ws-1",
    access_level=20,  # READWRITE
    granted_by="ops",
)
rules = admin.list_acl_rules(principal_type="user")

# Agents (read-only inspection)
agents = admin.list_agents()
info = admin.get_agent(implementation="scitrera/echo-bot")

# Workflow rules (read-only)
admin.list_workflow_rules(workspace="ws-1")
```

### Asynchronous

```python
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

    await admin.create_workspace(workspace_id="ws-1", display_name="Workspace One")
    workspaces = await admin.list_workspaces(limit=50)

    token = await admin.create_token(name="ci-token", principal_type="agent")
    print(token.plaintext_token)

    # Session ops are forwarded to the underlying async client.
    conns = await admin.list_connections(workspace="ws-1")
    await admin.disconnect_session(session_id="abc-123", reason="ops cleanup")

asyncio.run(main())
```

> **Note**: A few methods on the TypeScript `AdminClient` (notably
> `getHealth`) rely on streaming pathways that are not yet exposed as
> primitives on the Python client. Use the REST admin endpoints for
> gateway health checks until the corresponding `admin_query` primitive
> is added.

## Connection Configuration

All clients support configurable connection behavior:

```python
client = AgentClient(
    workspace="default",
    implementation="worker",
    specifier="01",
    # Retry configuration
    max_retries=5,           # Max connection attempts (0 = infinite)
    initial_backoff=1.0,     # Initial retry delay in seconds
    max_backoff=30.0,        # Maximum retry delay
    auto_reconnect=True      # Auto-reconnect on connection loss
)
```

### Reconnection Behavior

- On connection loss, clients automatically attempt to reconnect (if `auto_reconnect=True`)
- Exponential backoff with jitter prevents thundering herd
- Non-recoverable errors (authentication failures, etc.) stop reconnection attempts
- Session IDs are preserved for session resumption when possible

## Workspace Switching

Agents and tasks can switch workspaces:

```python
client.switch_workspace("new-workspace")
```

## Constants

```python
from scitrera_aether_client import (
    # Message types
    CHAT,           # Regular messages
    CONTROL,        # Control/command messages
    TOOL_CALL,      # Tool invocations
    EVENT,          # Events for workflow engine
    METRIC,         # Telemetry data

    # Task assignment modes
    SELF_ASSIGN,    # Creator handles the task
    TARGETED,       # Assign to specific agent
    POOL,           # Load-balanced assignment

    # KV operations
    KV_GET,
    KV_PUT,
    KV_LIST,
    KV_DELETE,

    # KV scopes
    KV_SCOPE_GLOBAL,
    KV_SCOPE_WORKSPACE,
    KV_SCOPE_USER,
    KV_SCOPE_USER_WORKSPACE,
)
```

## Examples

See the `example.py` and `example_async.py` files for comprehensive examples including:

- Agent client with messaging, KV operations, and task creation
- Orchestrator for managing agent lifecycle
- Workflow engine for event processing
- Metrics bridge for telemetry collection
- Concurrent async clients
- Context manager usage

Run examples:

```bash
# Sync examples
python example.py agent
python example.py orchestrator
python example.py workflow
python example.py metrics

# Async examples
python example_async.py agent
python example_async.py concurrent
python example_async.py context
```

## TLS Configuration

```python
# Simple TLS (server authentication using system CA)
client = AgentClient(
    workspace="default", implementation="worker", specifier="w1",
    tls_enabled=True,
)

# Custom CA certificate
client = AgentClient(
    workspace="default", implementation="worker", specifier="w1",
    tls_enabled=True,
    tls_root_cert_path="/path/to/ca.pem",
)

# mTLS (mutual authentication)
client = AgentClient(
    workspace="default", implementation="worker", specifier="w1",
    tls_enabled=True,
    tls_root_cert_path="/path/to/ca.pem",
    tls_client_cert_path="/path/to/client.pem",
    tls_client_key_path="/path/to/client-key.pem",
)

# Pass certificate bytes directly instead of paths
client = AgentClient(
    workspace="default", implementation="worker", specifier="w1",
    tls_enabled=True,
    tls_root_cert=ca_cert_bytes,
    tls_client_cert=client_cert_bytes,
    tls_client_key=client_key_bytes,
)
```

## Proxy

Route HTTP requests through the Aether connection to a service principal using
the httpx transport adapters:

```python
import httpx
from scitrera_aether_client.httpx_transport import AetherHTTPXTransport

transport = AetherHTTPXTransport(aether_client, "sv::memorylayer::default")
with httpx.Client(transport=transport) as http:
    resp = http.get("http://ignored/v1/memories/abc")
```

Async variant:

```python
from scitrera_aether_client.httpx_transport import AetherAsyncHTTPXTransport

transport = AetherAsyncHTTPXTransport(aether_client, "sv::memorylayer::default")
async with httpx.AsyncClient(transport=transport) as http:
    resp = await http.get("http://ignored/v1/memories/abc")
```

For `requests` users, mount `AetherRequestsAdapter` on a session:

```python
import requests
from scitrera_aether_client.requests_adapter import AetherRequestsAdapter

session = requests.Session()
session.mount("aether+sv://", AetherRequestsAdapter(aether_client))
session.get("aether+sv://memorylayer--default/v1/memories/abc")
```

For full details on sidecar deployment, service addressing, ACL/OBO model,
limits, audit events, and failure modes, see
[server/docs/proxy.md](../../server/docs/proxy.md).

## License

Copyright 2025-2026 Scitrera LLC. Licensed under the Apache License, Version 2.0.

## Links

- [GitHub Repository](https://github.com/scitrera/aether)
- [Documentation](https://github.com/scitrera/aether#readme)
- [Issue Tracker](https://github.com/scitrera/aether/issues)
