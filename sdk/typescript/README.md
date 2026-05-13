# @scitrera/aether-client

TypeScript/JavaScript SDK for the [Aether](https://github.com/scitrera/aether) distributed control plane.

Aether is a distributed control plane for routing structured messages, tracking tasks, and managing connection lifecycles. This SDK provides TypeScript/JavaScript clients for agents, users, and other principal types.

## Installation

```bash
npm install @scitrera/aether-client
```

## Quick Start

### Agent Client

Agents are persistent entities with workspace/implementation/specifier identity. Each agent identity can only have one active connection at a time (Connection = Lock paradigm).

```typescript
import { AgentClient, MessageType } from "@scitrera/aether-client";

const agent = new AgentClient({
  address: "localhost:50051",
  workspace: "production",
  implementation: "data-processor",
  specifier: "instance-1",
});

// Register handlers
agent.onMessage((msg) => {
  const text = new TextDecoder().decode(msg.payload);
  console.log(`Received from ${msg.sourceTopic}: ${text}`);
});

agent.onConfig((config) => {
  console.log("Workspace KV keys:", Object.keys(config.kv));
  // values are Uint8Array; decode with msgpack/TextDecoder as needed
  console.log("Global KV keys:", Object.keys(config.globalKv));
});

agent.onConnect((ack) => {
  console.log(`Connected with session ${ack.sessionId} (resumed: ${ack.resumed})`);
});

agent.onDisconnect((reason) => {
  console.log(`Disconnected: ${reason}`);
});

// Connect to the gateway
await agent.connect();

// Send messages
const encoder = new TextEncoder();
agent.sendToAgent("production", "other-agent", "instance-2", encoder.encode("Hello!"));
agent.sendToUser("alice", "tab-1", encoder.encode(JSON.stringify({ status: "complete" })));

// Broadcast to all agents in workspace
agent.broadcastToAgents("production", encoder.encode("announcement"));

// Disconnect when done
await agent.disconnect();
```

### User Client

Users are identified by userId and windowId, allowing multiple browser tabs per user. Users can only send direct messages (no events or metrics).

```typescript
import { UserClient } from "@scitrera/aether-client";

const user = new UserClient({
  address: "localhost:50051",
  userId: "alice",
  windowId: "tab-1",
  workspace: "production",
});

user.onIncomingMessage((msg) => {
  const text = new TextDecoder().decode(msg.payload);
  console.log(`Message from ${msg.sourceTopic}: ${text}`);
});

await user.connect();

// Send a message to an agent
const encoder = new TextEncoder();
user.sendToAgent(
  "production",
  "data-processor",
  "instance-1",
  encoder.encode(JSON.stringify({ action: "process", data: [1, 2, 3] })),
);
```

### TaskClient

Tasks can be unique (named, persistent identity like agents) or non-unique (server-assigned ID, load-balanced):

```typescript
import { TaskClient } from "@scitrera/aether-client";

// Unique task — persistent identity, only one active connection
const uniqueTask = new TaskClient({
  address: "localhost:50051",
  workspace: "prod",
  implementation: "report-gen",
  uniqueSpecifier: "daily-report",
});

// Non-unique task — server-assigned ID, multiple instances allowed
const worker = new TaskClient({
  address: "localhost:50051",
  workspace: "prod",
  implementation: "data-processor",
  // no uniqueSpecifier — becomes a pool worker
});

worker.onMessage((msg) => {
  console.log(`Task received from ${msg.sourceTopic}:`, msg.payload);
});

await worker.connect();

// Send events and metrics
const encoder = new TextEncoder();
worker.sendEvent(encoder.encode(JSON.stringify({ type: "task.started" })));
worker.sendMetric(encoder.encode(JSON.stringify({ cpu: 0.4 })));

// Report progress
worker.reportProgress({
  taskId: "task-123",
  state: "running",
  completion: 0.5,
  summary: "Processing batch 50/100",
});
```

## Checkpoint API

Agents and tasks can persist state using the checkpoint store:

```typescript
const cp = agent.checkpoint();

// Save state
await cp.saveSync({
  key: "my-state",
  data: encoder.encode(JSON.stringify({ step: 5, results: [1, 2, 3] })),
  ttl: 3600, // seconds, 0 = no expiration
});

// Load state
const response = await cp.loadSync({ key: "my-state" });
if (response.success) {
  const state = JSON.parse(new TextDecoder().decode(response.data));
  console.log("Restored state:", state);
}

// List checkpoint keys
const listResp = await cp.listSync({});
console.log("Checkpoint keys:", listResp.keys);

// Delete a checkpoint
await cp.deleteSync({ key: "my-state" });
```

## Task Management API

Any connected client can query and manage tasks:

```typescript
// List tasks with optional filters
const listResp = await agent.queryTasks({
  workspace: "prod",
  status: "running",
  taskType: "data-processor",
  limit: 50,
  offset: 0,
  timeout: 10000, // ms
});
console.log(`Found ${listResp.totalCount} tasks`);

// Get a specific task
const getResp = await agent.getTask("task-abc123");
if (getResp.task) {
  console.log("Task status:", getResp.task.status);
}

// Cancel a task
await agent.cancelTask("task-abc123", "user requested cancellation");

// Retry a failed task
await agent.retryTask("task-abc123");

// Mark as complete (for pool workers)
await agent.completeTask("task-abc123");

// Mark as failed (for pool workers)
await agent.failTask("task-abc123", "processing error");
```

## Authentication

```typescript
import { AgentClient, withAPIKey, withToken, withTenant } from "@scitrera/aether-client";

// API Key authentication
const agent = new AgentClient({
  address: "gateway.example.com:50051",
  workspace: "production",
  implementation: "worker",
  specifier: "1",
  credentials: {
    ...withAPIKey("your-api-key"),
    ...withTenant("your-tenant-id"),
  },
});

// OAuth/JWT authentication
const agent2 = new AgentClient({
  address: "gateway.example.com:50051",
  workspace: "production",
  implementation: "worker",
  specifier: "2",
  credentials: withToken("your-jwt-token"),
});
```

## TLS Configuration

```typescript
import { AgentClient } from "@scitrera/aether-client";
import { readFileSync } from "fs";

const agent = new AgentClient({
  address: "gateway.example.com:50051",
  workspace: "production",
  implementation: "worker",
  specifier: "1",
  tls: {
    rootCerts: readFileSync("ca.pem"),
    // For mTLS:
    privateKey: readFileSync("client-key.pem"),
    certChain: readFileSync("client-cert.pem"),
  },
});
```

## KV Store Operations

Access the hierarchical key-value store through any client:

```typescript
import { KVScope } from "@scitrera/aether-client";

const kv = agent.kv();

// Async operations (fire-and-forget, responses via onKVResponse callback)
kv.putGlobal("my-key", encoder.encode("my-value"));
kv.getGlobal("my-key");

// Sync operations (Promise-based with timeout)
const response = await kv.getSync({
  key: "my-key",
  scope: KVScope.Global,
  timeout: 5000, // ms
});

if (response.success) {
  console.log("Value:", response.value);
}

// Workspace-scoped operations
await kv.putSync({
  key: "config",
  value: encoder.encode(JSON.stringify({ debug: true })),
  scope: KVScope.Workspace,
  workspace: "production",
  ttl: 3600, // seconds
});
```

## Progress Reporting

Agents and tasks report progress through the `pg.{workspace}` stream. Users subscribed to the workspace receive filtered updates:

```typescript
agent.reportProgress({
  taskId: "task-123",
  state: "running",           // e.g. "running", "finishing", "idle"
  completion: 0.5,            // 0.0–1.0, or -1 for indeterminate
  summary: "Processing batch 50/100",
  // Optional step info for multi-step operations:
  stepName: "Data validation",
  stepDetail: "Checking schema for 1000 records",
  stepSequence: 2,
  stepTotal: 4,
  stepType: "validation",
  // Optional targeting:
  recipient: "us.alice.tab-1",  // empty = broadcast to all workspace users
  requestId: "req-abc",
  metadata: { batchId: "b-99" },
});
```

## AdminClient

`AdminClient` wraps any connected `AetherClient` and exposes named methods for gateway administration: tokens, ACL rules, workspaces, agents, and connection management.

```typescript
import { AgentClient, AdminClient, withAPIKey } from "@scitrera/aether-client";

const agent = new AgentClient({
  address: "localhost:50051",
  workspace: "default",
  implementation: "admin-agent",
  specifier: "ops-1",
  credentials: withAPIKey("admin-api-key"),
});
await agent.connect();

const admin = new AdminClient(agent);

// --- Token management ---
const { plaintextToken } = await admin.createToken({
  name: "ci-token",
  principalType: "agent",
  workspacePatterns: ["production", "staging"],
  scopes: ["read", "write"],
  expiresInSeconds: 86400,
});
console.log("Token:", plaintextToken);

await admin.revokeToken({ tokenId: "tok-123" });
const { tokens } = await admin.listTokens({ principalType: "agent" });

// --- ACL rules ---
await admin.createACLRule({
  principalType: "user",
  principalId: "alice",
  resourceType: "workspace",
  resourceId: "production",
  permission: "write",
});

await admin.deleteACLRule({ ruleId: "rule-456" });
const aclResp = await admin.listACLRules({ principalType: "user" });

// --- Workspace management ---
await admin.createWorkspace({ workspaceId: "staging", displayName: "Staging" });
await admin.updateWorkspace({ workspaceId: "staging", displayName: "Staging Env" });
const wsr = await admin.listWorkspaces({ limit: 50 });
await admin.deleteWorkspace({ workspaceId: "old-workspace" });

// --- Agent registry ---
const agentsResp = await admin.listAgents({ workspace: "production" });
const agentInfo = await admin.getAgent({ implementation: "data-processor" });

// --- Connection management ---
const health = await admin.getHealth();
const conns = await admin.getConnections({ workspace: "production" });
await admin.disconnectSession({ sessionId: "sess-789", reason: "maintenance" });
```

## Auto-Reconnection

All clients support automatic reconnection with exponential backoff:

```typescript
const agent = new AgentClient({
  address: "localhost:50051",
  workspace: "production",
  implementation: "worker",
  specifier: "1",
  reconnect: true,          // default: true
  reconnectDelay: 1000,     // initial delay in ms (default: 1000)
  maxReconnectDelay: 30000, // max delay in ms (default: 30000)
  connection: {
    maxRetries: 10,          // 0 = infinite (default: 5)
    backoffMultiplier: 2.0,  // default: 2.0
  },
});

agent.onReconnecting((attempt) => {
  console.log(`Reconnection attempt ${attempt}...`);
});
```

## Retry on Duplicate Identity

When a previous instance crashes and reconnects before the distributed lock expires, the gateway returns `ALREADY_EXISTS`. Enable `retryOnDuplicate` to wait and retry automatically:

```typescript
const agent = new AgentClient({
  address: "localhost:50051",
  workspace: "production",
  implementation: "worker",
  specifier: "1",
  retryOnDuplicate: true,       // retry on ALREADY_EXISTS (default: false)
  retryOnDuplicateDelay: 5000,  // wait 5 s between retries (default: 5000)
  connection: {
    retryOnDuplicateMaxAttempts: 5,  // give up after 5 attempts (default: 5)
  },
});
```

## Error Handling

The SDK provides a structured error hierarchy:

```typescript
import {
  AetherError,
  ConnectionError,
  AuthenticationError,
  DuplicateIdentityError,
  TimeoutError,
  isRecoverable,
  isConnectionError,
} from "@scitrera/aether-client";

try {
  await agent.connect();
} catch (err) {
  if (err instanceof AuthenticationError) {
    console.error("Authentication failed:", err.message);
  } else if (err instanceof DuplicateIdentityError) {
    console.error("Identity already in use:", err.identity);
  } else if (err instanceof ConnectionError) {
    console.error("Connection failed:", err.message);
  }

  // Or use classification helpers
  if (!isRecoverable(err as Error)) {
    console.error("Non-recoverable error, will not retry");
  }
}
```

## Topic Schema

The SDK provides helpers for constructing topic strings:

```typescript
import {
  agentTopic,
  userTopic,
  uniqueTaskTopic,
  taskBroadcastTopic,
  globalAgentsTopic,
  eventTopic,
  bridgeTopic,
} from "@scitrera/aether-client";

agentTopic("prod", "worker", "inst-1");            // "ag.prod.worker.inst-1"
userTopic("alice", "tab-1");                        // "us.alice.tab-1"
uniqueTaskTopic("prod", "report", "daily");         // "tu.prod.report.daily"
taskBroadcastTopic("prod", "worker");               // "tb.prod.worker"
globalAgentsTopic("prod");                          // "ga.prod"
eventTopic("task.completed");                       // "event.task.completed"
bridgeTopic("aether-msgbridge", "discord-1");       // "br.aether-msgbridge.discord-1"
```

## Principal Types

Aether supports 8 principal types. The TypeScript SDK provides dedicated client classes for all of them except `Service`:

| Type | Client Class | Description | Topic Format |
|------|-------------|-------------|-------------|
| Agent | `AgentClient` | Persistent entity | `ag.{workspace}.{impl}.{spec}` |
| UniqueTask | `TaskClient` (with specifier) | Named task | `tu.{workspace}.{impl}.{spec}` |
| NonUniqueTask | `TaskClient` (no specifier) | Ephemeral task | `ta.{workspace}.{impl}.{id}` |
| User | `UserClient` | Browser session | `us.{userId}.{windowId}` |
| Orchestrator | `OrchestratorClient` | Compute provisioner | receives `TaskAssignment` |
| WorkflowEngine | `WorkflowEngineClient` | Event processor (singleton) | subscribes to `event.*` |
| MetricsBridge | `MetricsBridgeClient` | Telemetry collector (singleton) | subscribes to `metric.*` |
| Bridge | `BridgeClient` | Cross-workspace relay | `br.{impl}.{spec}` |
| Service | _(no dedicated client)_ | Sidecar service proxy | `sv.{impl}.{spec}` |

> **Known gap:** The TypeScript SDK does not currently have a dedicated `ServiceClient`. The `Service` principal type represents sidecar services addressable via the HTTP proxy feature. If you need to connect as a service principal, use `BridgeClient` (cross-workspace) or `AgentClient` (workspace-scoped) as a workaround and set your identity fields to match the service's `impl`/`spec`. A dedicated `ServiceClient` is planned for a future release.

### OrchestratorClient

Orchestrators receive task assignments when targeted agents are offline and launch compute resources:

```typescript
import { OrchestratorClient, BaseOrchestrator } from "@scitrera/aether-client";
import type { TaskAssignment } from "@scitrera/aether-client";

// Low-level: OrchestratorClient
const orch = new OrchestratorClient({
  address: "localhost:50051",
  implementation: "k8s-orchestrator",
  supportedProfiles: ["kubernetes", "docker"],
  specifier: "instance-1", // optional, auto-generated if omitted
});

orch.onTaskAssignment((assignment) => {
  console.log(`Launch ${assignment.targetImplementation} for task ${assignment.taskId}`);
  console.log("Profile:", assignment.profile);
  console.log("Params:", assignment.launchParams);
});

await orch.connect();

// High-level: extend BaseOrchestrator
class MyOrchestrator extends BaseOrchestrator {
  async launchTask(assignment: TaskAssignment): Promise<void> {
    // Start a container, subprocess, etc.
    console.log(`Starting ${assignment.targetImplementation}`);
  }
}

const myOrch = new MyOrchestrator({
  address: "localhost:50051",
  implementation: "my-orchestrator",
  supportedProfiles: ["my-profile"],
  logAssignments: true,
});
await myOrch.connect();
```

### WorkflowEngineClient

The workflow engine receives all events and can send commands to any principal:

```typescript
import { WorkflowEngineClient } from "@scitrera/aether-client";

const engine = new WorkflowEngineClient({
  address: "localhost:50051",
});

engine.onMessage((msg) => {
  const event = JSON.parse(new TextDecoder().decode(msg.payload));
  console.log(`Event from ${msg.sourceTopic}:`, event);

  // React to event: send commands to agents
  const encoder = new TextEncoder();
  engine.sendCommandToAgent("prod", "processor", "inst-1",
    encoder.encode(JSON.stringify({ action: "process", eventId: event.id })),
  );
});

await engine.connect();
```

### MetricsBridgeClient

The metrics bridge is receive-only — it subscribes to `metric.*` topics:

```typescript
import { MetricsBridgeClient } from "@scitrera/aether-client";

const bridge = new MetricsBridgeClient({
  address: "localhost:50051",
});

bridge.onMessage((msg) => {
  const metric = JSON.parse(new TextDecoder().decode(msg.payload));
  console.log(`Metric from ${msg.sourceTopic}:`, metric);
  // Forward to Prometheus, Datadog, etc.
});

await bridge.connect();
```

### BridgeClient

Bridges operate cross-workspace and can send to any topic in any workspace:

```typescript
import { BridgeClient, MessageType } from "@scitrera/aether-client";

const bridge = new BridgeClient({
  address: "localhost:50051",
  implementation: "aether-msgbridge",
  specifier: "discord-1",
});

bridge.onMessage((msg) => {
  // Receive messages addressed to this bridge
  console.log(`Received from ${msg.sourceTopic}:`, msg.payload);
});

await bridge.connect();

// Send to any workspace — bridges are cross-workspace by design
const encoder = new TextEncoder();
bridge.sendToAgent("prod", "my-agent", "instance-1",
  encoder.encode(JSON.stringify({ from: "discord", text: "Hello!" })),
);
bridge.sendToUser("alice", "tab-1",
  encoder.encode("Notification from Discord"),
);
bridge.broadcastToUsers("prod",
  encoder.encode("System announcement"),
  MessageType.Control,
);
```

## Proxy

Route HTTP requests through the Aether connection to a service principal using
`AetherFetchTransport`, which provides a Fetch-compatible interface:

```typescript
import { AetherFetchTransport } from "@scitrera/aether-client/proxy";

const transport = new AetherFetchTransport(agentClient, "sv::memorylayer::default");
const resp = await transport.fetch("/v1/memories/abc");
```

`AetherFetchTransport.fetch()` accepts the same signature as the Web Fetch API
(`string | URL | Request`, optional `RequestInit`). The URL hostname and
protocol are ignored — only the path and query string are forwarded.

For full details on sidecar deployment, service addressing, ACL/OBO model,
limits, audit events, and failure modes, see
[server/docs/proxy.md](../../server/docs/proxy.md).

## Foreign Audit Logging

Any connected principal can submit structured audit events directly to the gateway's audit pipeline using `submitAuditEvent`. This is useful for recording application-level actions (e.g. completed workflow steps, tool invocations, or policy decisions) alongside infrastructure events already captured by the gateway. The gateway accepts the event into its async audit queue and responds synchronously; `success: false` indicates the event was rejected (e.g. due to an ACL restriction or rate limit) but does not throw.

```typescript
await client.submitAuditEvent({
  eventType: "message",
  operation: "completed_workflow_step",
  metadata: { workflowId: "abc-123" },
});
```

## Workspace Switching

Agents, tasks, and users can switch their active workspace at runtime without reconnecting. The gateway updates the session's workspace subscription and returns a new `ConfigSnapshot` with the KV data for the new workspace.

```typescript
// AgentClient — updates the agent's workspace subscription
await agent.connect();
agent.switchWorkspace("staging");
// agent.workspace === "staging"

// UserClient — declares the user's active app workspace to the gateway.
// Users do not encode a workspace in their identity (topic: us.{userId}.{windowId}),
// so calling switchWorkspace right after connect() is recommended to ensure
// server-side session state has the correct workspace for task-authority scoping.
await user.connect();
user.switchWorkspace("production"); // call immediately after connect

// TaskClient — same pattern as AgentClient
await task.connect();
task.switchWorkspace("prod-v2");
```

**Signature** (same on `AgentClient`, `TaskClient`, `UserClient`):
```typescript
switchWorkspace(newWorkspace: string): void
```

- Fire-and-forget: the upstream `SwitchWorkspace` proto message is enqueued immediately; the local `workspace` property is updated synchronously.
- Throws `InvalidArgumentError` if `newWorkspace` is empty.
- No server ack is awaited — a new `ConfigSnapshot` downstream event will follow.

## Key Architectural Principle

The connection itself IS the distributed lock AND the heartbeat. When the gRPC stream closes, the identity lock is immediately released on the server. No separate heartbeat API exists. This means:

- Each agent/unique-task identity can only have one active connection
- Disconnection automatically releases the identity for reuse
- Auto-reconnect with session resumption preserves the lock

## API Reference

See the [Go SDK documentation](../go/aether/doc.go) and [Python SDK](../python-client/) for additional API patterns. This TypeScript SDK follows the same conventions.

## License

Apache License, Version 2.0
