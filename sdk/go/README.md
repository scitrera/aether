# Aether Go Client SDK

Go client SDK for the Scitrera Aether distributed control plane. Aether is a system for routing structured messages, tracking tasks, and managing connection lifecycles for agents, tasks, users, and other principals.

## Features

- **Idiomatic Go API**: Context-based cancellation, functional options, and error wrapping
- **Multiple Client Types**: Agent, Task, User, Orchestrator, WorkflowEngine, and MetricsBridge clients
- **Key-Value Store**: Hierarchical configuration store with multiple scopes (global, workspace, user, user-workspace)
- **Checkpointing**: Persist and restore agent/task state across restarts
- **Auto-Reconnection**: Configurable exponential backoff with automatic reconnection
- **Callback Handlers**: Event-driven message handling with context support
- **TLS/mTLS Support**: Secure connections with optional mutual TLS authentication
- **Concurrency Safe**: All clients are safe for concurrent use

## Installation

```bash
go get github.com/scitrera/aether/sdk/go
```

## Quick Start

### Agent Client

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"

    "github.com/scitrera/aether/sdk/go/aether"
)

func main() {
    // Create an agent client
    client, err := aether.NewAgentClient(aether.AgentOptions{
        ClientOptions: aether.ClientOptions{
            ServerAddr: "localhost:50051",
        },
        Workspace:      "default",
        Implementation: "my-agent",
        Specifier:      "agent-01",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Set up message handler
    client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
        fmt.Printf("Received from %s: %s\n", msg.SourceTopic, msg.Payload)
        return nil
    })

    // Set up connection handlers
    client.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
        log.Printf("Connected with session %s", ack.SessionID)
        return nil
    })

    client.OnDisconnect(func(ctx context.Context, reason string) error {
        log.Printf("Disconnected: %s", reason)
        return nil
    })

    // Create cancellable context
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle interrupt signal
    go func() {
        sigCh := make(chan os.Signal, 1)
        signal.Notify(sigCh, os.Interrupt)
        <-sigCh
        cancel()
    }()

    // Connect to the gateway
    if err := client.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Send a message to another agent
    err = client.SendToAgent("default", "other-agent", "01", []byte("Hello!"))
    if err != nil {
        log.Printf("Failed to send: %v", err)
    }

    // Run the message loop (blocks until disconnect)
    if err := client.Run(ctx); err != nil {
        log.Printf("Client stopped: %v", err)
    }
}
```

### Task Client

```go
package main

import (
    "context"
    "log"

    "github.com/scitrera/aether/sdk/go/aether"
)

func main() {
    // Unique task (named)
    uniqueTask, err := aether.NewTaskClient(aether.TaskOptions{
        ClientOptions: aether.ClientOptions{
            ServerAddr: "localhost:50051",
        },
        Workspace:      "default",
        Implementation: "data-processor",
        Specifier:      "job-123", // Unique task
    })
    if err != nil {
        log.Fatal(err)
    }

    // Non-unique task (pooled, server assigns ID)
    pooledTask, err := aether.NewTaskClient(aether.TaskOptions{
        ClientOptions: aether.ClientOptions{
            ServerAddr: "localhost:50051",
        },
        Workspace:      "default",
        Implementation: "worker",
        // Specifier omitted - server assigns ID
    })
    if err != nil {
        log.Fatal(err)
    }

    // Non-unique tasks subscribe to both their specific topic
    // and the broadcast topic for work claiming
    log.Printf("Pooled task broadcast topic: %s", pooledTask.BroadcastTopic())

    _ = uniqueTask
}
```

## Client Types

### AgentClient

For long-running agent processes that need unique identities.

```go
client, err := aether.NewAgentClient(aether.AgentOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "localhost:50051",
    },
    Workspace:      "default",
    Implementation: "python-worker",
    Specifier:      "worker-01",
})
```

### TaskClient

For task execution. Supports both unique (named) and non-unique (pooled) tasks.

```go
// Unique task
uniqueTask, _ := aether.NewTaskClient(aether.TaskOptions{
    ClientOptions: aether.ClientOptions{ServerAddr: "localhost:50051"},
    Workspace:      "default",
    Implementation: "processor",
    Specifier:      "task-123", // Named task
})

// Non-unique task (server assigns ID)
pooledTask, _ := aether.NewTaskClient(aether.TaskOptions{
    ClientOptions: aether.ClientOptions{ServerAddr: "localhost:50051"},
    Workspace:      "default",
    Implementation: "worker",
    // Specifier omitted
})
```

### UserClient

For user session connections (e.g., from browser windows).

```go
client, err := aether.NewUserClient(aether.UserOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "localhost:50051",
    },
    UserID:   "user-123",
    WindowID: "window-abc",
})
```

### OrchestratorClient

For managing agent/task lifecycle and compute resources.

```go
client, err := aether.NewOrchestratorClient(aether.OrchestratorOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "localhost:50051",
    },
    Implementation:    "kubernetes-orchestrator",
    SupportedProfiles: []string{"docker", "kubernetes"},
})
```

### WorkflowEngineClient

For processing events and triggering downstream actions.

```go
client, err := aether.NewWorkflowEngineClient(aether.WorkflowEngineOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "localhost:50051",
    },
})
```

### MetricsBridgeClient

For collecting telemetry data from agents and tasks.

```go
client, err := aether.NewMetricsBridgeClient(aether.MetricsBridgeOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "localhost:50051",
    },
})
```

## Handlers

All clients support the following handlers:

| Handler | Description | Signature |
|---------|-------------|-----------|
| `OnMessage` | Incoming message received | `func(ctx context.Context, msg *Message) error` |
| `OnConfig` | Configuration snapshot received | `func(ctx context.Context, config *ConfigSnapshot) error` |
| `OnSignal` | Signal received | `func(ctx context.Context, signal *Signal) error` |
| `OnError` | Error occurred | `func(ctx context.Context, err *ErrorInfo) error` |
| `OnKVResponse` | KV operation response | `func(ctx context.Context, resp *KVResponse) error` |
| `OnCheckpointResponse` | Checkpoint operation response | `func(ctx context.Context, resp *CheckpointResponse) error` |
| `OnTaskAssignment` | Task assigned (Orchestrators) | `func(ctx context.Context, task *TaskAssignment) error` |
| `OnConnect` | Connection established | `func(ctx context.Context, ack *ConnectionAck) error` |
| `OnDisconnect` | Connection lost | `func(ctx context.Context, reason string) error` |
| `OnReconnecting` | Reconnection attempt | `func(ctx context.Context, attempt int) error` |

Example handler registration:

```go
client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
    // Process message
    fmt.Printf("From %s: %s\n", msg.SourceTopic, msg.Payload)
    return nil
})

client.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
    log.Printf("Connected, session: %s, resumed: %v", ack.SessionID, ack.Resumed)
    return nil
})

client.OnReconnecting(func(ctx context.Context, attempt int) error {
    log.Printf("Reconnection attempt %d", attempt)
    if attempt > 10 {
        return errors.New("too many reconnection attempts")
    }
    return nil
})
```

## Handler dispatch model

> **TL;DR — if your handler ever calls back into the SDK (CreateTaskSync, KV
> ops, ProxyHTTP, derive_authority_grant, etc.), wrap it with
> `aether.Async` / `AsyncMessageHandler` / `AsyncTaskAssignmentHandler`.**

All handlers registered via the `On*` methods are invoked **synchronously on
the single receive-loop goroutine** that drains the bidirectional gRPC stream.
Each downstream frame triggers one handler call before the next frame is read.

This is fine for fire-and-forget logic. It is a deadlock for any handler that
itself makes a *synchronous* SDK call back to the gateway, because the
response that call waits for arrives on the very loop the handler is
holding. Symptom: `[DEADLINE_EXCEEDED]` on the nested call, even though the
gateway accepted and responded to it.

The trap is most likely to bite:

| Handler | Why callbacks happen |
|---------|----------------------|
| `OnProxyHttpRequest` | Service implementations almost always need to read state, mint downstream tasks, or call other services |
| `OnTaskAssignment` | Task workers update progress, complete/fail the task, derive grants |
| `OnMessage` | Routers / orchestrators that translate inbound messages into SDK ops |

### Recommended idiom: wrap with the async helpers

```go
client.OnProxyHttpRequest(aether.Async(func(ctx context.Context, req *pb.ProxyHttpRequest) error {
    // Free to call client.CreateTaskSync, KV ops, ProxyHTTP, etc.
    return nil
}))

client.OnMessage(aether.AsyncMessageHandler(func(ctx context.Context, msg *aether.Message) error {
    return nil
}))

client.OnTaskAssignment(aether.AsyncTaskAssignmentHandler(func(ctx context.Context, t *aether.TaskAssignment) error {
    return nil
}))
```

Each wrapped handler runs on its own goroutine with a fresh
`context.WithTimeout(context.Background(), aether.DefaultAsyncHandlerTimeout)`
(3 minutes; override with the `*WithTimeout` variants). Errors are logged at
warn; panics are recovered.

The wrappers trade in-order, single-threaded handler dispatch for the ability
to make nested calls. If your handler is purely fire-and-forget and you want
to preserve in-order semantics, leave it sync — you don't need the wrapper.

### Future work: built-in async dispatch (option C)

A follow-up will give the SDK a first-class async-dispatch mode for at least
`OnProxyHttpRequest` and `OnTaskAssignment` — the handler types where the
trap is real-world likely. The design will need:

- A bounded worker pool per session so unbounded fan-out can't OOM the
  process under load.
- Panic isolation per handler invocation.
- A configuration knob (per-client option, defaulting to "async") with an
  opt-out for callers that genuinely want strict in-order handling.
- A migration / deprecation path for the wrapper functions in this file
  (likely keep them as the explicit / per-handler escape hatch).

Until that lands, the `aether.Async*` wrappers are the canonical idiom.

## Messaging

### Message Types

```go
import pb "github.com/scitrera/aether/api/proto"

// CHAT - Regular chat messages (default)
client.SendToAgent("ws", "impl", "spec", payload)

// CONTROL - Control/command messages
client.SendToAgentWithType("ws", "impl", "spec", payload, pb.MessageType_CONTROL)

// TOOL_CALL - Tool invocation messages
client.SendToolCallMessage(topic, payload)

// EVENT - Events for workflow engine (agents/tasks only)
client.SendEvent(payload)

// METRIC - Telemetry data for metrics bridge (agents/tasks only)
client.SendMetric(payload)
```

### Sending Messages

```go
// To a specific agent
client.SendToAgent("default", "worker", "01", []byte("Hello!"))

// To a task (unique or broadcast)
client.SendToTask("default", "processor", "task-123", []byte("Process this"))
client.SendToTask("default", "worker", "", []byte("Broadcast to pool"))

// To a user session
client.SendToUser("user-123", "window-abc", []byte("Notification"))

// To a user's workspace scope
client.SendToUserWorkspace("user-123", "default", []byte("Workspace message"))

// Broadcast to all agents in workspace
client.BroadcastToAgents("default", []byte("Broadcast message"))

// Broadcast to all users in workspace
client.BroadcastToUsers("default", []byte("User broadcast"))

// Send event (agents/tasks only)
client.SendEvent([]byte(`{"event": "completed"}`))

// Send metric (agents/tasks only)
client.SendMetric([]byte(`{"metric": "latency", "value": 42}`))
```

## KV Operations

The KV store supports multiple scopes:

| Scope | Description | Constants |
|-------|-------------|-----------|
| `global` | Global configuration | `aether.KVScopeGlobal` |
| `workspace` | Workspace-specific | `aether.KVScopeWorkspace` |
| `user` | User-specific | `aether.KVScopeUser` |
| `user-workspace` | User + workspace scoped | `aether.KVScopeUserWorkspace` |

### Asynchronous Operations

```go
kv := client.KV()

// Store a value (fire-and-forget)
kv.Put("config/setting", []byte("value"), aether.KVScopeGlobal, "", "", 0)

// Store with workspace scope
kv.PutWorkspace("team/setting", []byte("team-value"), "default")

// Get a value (response via OnKVResponse handler)
kv.Get("config/setting", aether.KVScopeGlobal, "", "")

// List keys
kv.ListGlobal("config/")

// Delete a key
kv.DeleteGlobal("config/old")
```

### Synchronous Operations

```go
ctx := context.Background()
kv := client.KV()

// Get with timeout
resp, err := kv.GetSync(ctx, aether.KVGetOptions{
    Key:     "config/setting",
    Scope:   aether.KVScopeGlobal,
    Timeout: 5 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Value: %s\n", resp.Value)

// Put with TTL
resp, err = kv.PutSync(ctx, aether.KVPutOptions{
    Key:     "session/data",
    Value:   []byte("session-value"),
    Scope:   aether.KVScopeWorkspace,
    Workspace: "default",
    TTL:     time.Hour,
    Timeout: 5 * time.Second,
})

// List keys
listResp, err := kv.ListSync(ctx, aether.KVListOptions{
    KeyPrefix: "config/",
    Scope:     aether.KVScopeGlobal,
    Timeout:   5 * time.Second,
})
if err == nil {
    for _, key := range listResp.Keys {
        fmt.Println(key)
    }
}
```

### Convenience Methods

```go
// Global scope shortcuts
kv.GetGlobal("key")
kv.PutGlobal("key", []byte("value"))
kv.DeleteGlobal("key")
kv.ListGlobal("prefix/")

// Workspace scope shortcuts
kv.GetWorkspace("key", "my-workspace")
kv.PutWorkspace("key", []byte("value"), "my-workspace")

// User scope shortcuts
kv.GetUser("key", "user-123")
kv.PutUser("key", []byte("value"), "user-123")

// User-workspace scope shortcuts
kv.GetUserWorkspace("key", "user-123", "my-workspace")
kv.PutUserWorkspace("key", []byte("value"), "user-123", "my-workspace")
```

## Checkpointing

Save and restore agent/task state:

### Asynchronous Operations

```go
cp := client.Checkpoint()

// Save checkpoint (fire-and-forget)
cp.Save([]byte("state data"), "my-checkpoint", -1) // -1 = server default TTL

// Save with specific TTL
cp.SaveWithTTL([]byte("state"), "checkpoint-key", time.Hour)

// Save with no expiration
cp.SavePermanent([]byte("state"), "permanent-checkpoint")

// Load checkpoint (response via OnCheckpointResponse handler)
cp.Load("my-checkpoint")

// List checkpoints
cp.List()

// Delete checkpoint
cp.Delete("my-checkpoint")
```

### Synchronous Operations

```go
ctx := context.Background()
cp := client.Checkpoint()

// Save and wait for confirmation
resp, err := cp.SaveSync(ctx, aether.CheckpointSaveOptions{
    Data:    []byte("state data"),
    Key:     "my-checkpoint",
    TTL:     time.Hour, // 0 = no expiration, -1 = server default
    Timeout: 5 * time.Second,
})

// Load checkpoint
resp, err = cp.LoadSync(ctx, aether.CheckpointLoadOptions{
    Key:     "my-checkpoint",
    Timeout: 5 * time.Second,
})
if err == nil && resp.Success {
    fmt.Printf("Restored state: %s\n", resp.Data)
}

// List all checkpoints
listResp, err := cp.ListSync(ctx, 5*time.Second)
if err == nil {
    fmt.Printf("Checkpoints: %v\n", listResp.Keys)
}

// Delete checkpoint
_, err = cp.DeleteSync(ctx, aether.CheckpointDeleteOptions{
    Key:     "my-checkpoint",
    Timeout: 5 * time.Second,
})
```

## Task Creation

Create tasks with different assignment modes:

```go
// Self-assigned task (creator handles it)
client.CreateTask(aether.CreateTaskOptions{
    TaskType:       "data-processing",
    Workspace:      "default",
    AssignmentMode: aether.TaskAssignmentSelfAssign,
    Metadata:       map[string]string{"priority": "high"},
})

// Targeted task (assign to specific agent)
client.CreateTask(aether.CreateTaskOptions{
    TaskType:       "specialized-work",
    Workspace:      "default",
    AssignmentMode: aether.TaskAssignmentTargeted,
    TargetAgentID:  "ag.default.worker.specialist-01",
    LaunchParamOverrides: map[string]string{"memory": "4G"},
})

// Pool task (load-balanced to available workers)
client.CreateTask(aether.CreateTaskOptions{
    TaskType:       "batch-job",
    Workspace:      "default",
    AssignmentMode: aether.TaskAssignmentPool,
})
```

## Connection Configuration

All clients support configurable connection behavior:

```go
client, err := aether.NewAgentClient(aether.AgentOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "localhost:50051",
        Connection: aether.ConnectionOptions{
            MaxRetries:        5,              // Max connection attempts (0 = infinite)
            InitialBackoff:    time.Second,    // Initial retry delay
            MaxBackoff:        30 * time.Second, // Maximum retry delay
            BackoffMultiplier: 2.0,            // Exponential backoff multiplier
            AutoReconnect:     true,           // Auto-reconnect on connection loss
            ConnectTimeout:    30 * time.Second, // Connection timeout
            KeepAliveInterval: 30 * time.Second, // Keepalive ping interval
        },
    },
    Workspace:      "default",
    Implementation: "worker",
    Specifier:      "01",
})
```

### Functional Options

```go
opts := aether.DefaultConnectionOptions()
aether.ApplyConnectionOptions(&opts,
    aether.WithMaxRetries(10),
    aether.WithInitialBackoff(2*time.Second),
    aether.WithMaxBackoff(time.Minute),
    aether.WithAutoReconnect(true),
)
```

### Reconnection Behavior

- On connection loss, clients automatically attempt to reconnect (if `AutoReconnect=true`)
- Exponential backoff with jitter prevents thundering herd
- Non-recoverable errors (authentication failures, duplicate identity, etc.) stop reconnection attempts
- Session IDs are preserved for session resumption when possible

### WithRetryOnDuplicate

By default, a `DuplicateIdentityError` is treated as non-recoverable and stops reconnection. Enable `RetryOnDuplicate` to treat it as recoverable instead — useful when restarting a container or process before the previous connection's Redis lock (30 s TTL) has expired:

```go
opts := aether.DefaultConnectionOptions()
aether.ApplyConnectionOptions(&opts,
    aether.WithRetryOnDuplicate(true),  // retry until lock expires (~30 s)
    aether.WithMaxRetries(0),           // infinite retries
    aether.WithMaxBackoff(10*time.Second),
)

client, err := aether.NewAgentClient(aether.AgentOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "localhost:50051",
        Connection: opts,
    },
    Workspace:      "default",
    Implementation: "my-agent",
    Specifier:      "agent-01",
})
```

Or set it directly in the struct:

```go
Connection: aether.ConnectionOptions{
    AutoReconnect:    true,
    RetryOnDuplicate: true,
    MaxRetries:       0,   // 0 = infinite
    InitialBackoff:   time.Second,
    MaxBackoff:       10 * time.Second,
    BackoffMultiplier: 2.0,
},
```

## TLS Configuration

### Simple TLS (Server Authentication)

```go
client, err := aether.NewAgentClient(aether.AgentOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "secure.example.com:50051",
        TLS: &aether.TLSConfig{
            Enabled:    true,
            ServerName: "secure.example.com",
        },
    },
    // ...
})
```

### mTLS (Mutual Authentication)

```go
// Load certificates from files
tlsConfig, err := aether.LoadTLSConfigFromFiles(
    "/path/to/ca.pem",
    "/path/to/client.pem",
    "/path/to/client-key.pem",
)
if err != nil {
    log.Fatal(err)
}

client, err := aether.NewAgentClient(aether.AgentOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "secure.example.com:50051",
        TLS:        tlsConfig,
    },
    // ...
})
```

### TLS Config from Bytes

```go
client, err := aether.NewAgentClient(aether.AgentOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr: "secure.example.com:50051",
        TLS: &aether.TLSConfig{
            Enabled:    true,
            RootCAs:    caCertPEM,    // []byte
            ClientCert: clientCertPEM, // []byte
            ClientKey:  clientKeyPEM,  // []byte
            ServerName: "secure.example.com",
        },
    },
    // ...
})
```

## Credentials

```go
creds := aether.NewCredentials().
    WithAPIKey("your-api-key").
    WithTenant("tenant-id")

client, err := aether.NewAgentClient(aether.AgentOptions{
    ClientOptions: aether.ClientOptions{
        ServerAddr:  "localhost:50051",
        Credentials: creds,
    },
    // ...
})
```

## Workspace Switching

Agents and tasks can switch workspaces:

```go
err := client.SwitchWorkspace("new-workspace")
```

## Error Types

The SDK provides typed errors for specific error conditions:

```go
import "github.com/scitrera/aether/sdk/go/aether"

// Connection errors
var connErr *aether.ConnectionError
var closedErr *aether.ConnectionClosedError
var reconnErr *aether.ReconnectionError

// Auth errors
var authErr *aether.AuthenticationError
var permErr *aether.PermissionDeniedError

// Identity errors
var dupErr *aether.DuplicateIdentityError

// Timeout errors
var timeoutErr *aether.TimeoutError

// Request errors
var argErr *aether.InvalidArgumentError
var notFoundErr *aether.NotFoundError

// Example error handling
if err := client.Connect(ctx); err != nil {
    if errors.As(err, &dupErr) {
        log.Printf("Identity already connected: %s", dupErr.Identity)
    } else if errors.As(err, &authErr) {
        log.Printf("Authentication failed: %s", authErr.Message)
    } else if aether.IsRecoverable(err) {
        log.Printf("Recoverable error, will retry: %v", err)
    } else {
        log.Fatal(err)
    }
}
```

### Error Classification Helpers

```go
// Check if error is recoverable (should trigger reconnection)
if aether.IsRecoverable(err) {
    // Client will auto-reconnect
}

// Check if error is connection-related
if aether.IsConnectionError(err) {
    // Handle connection issues
}

// Check if error is a timeout
if aether.IsTimeoutError(err) {
    // Handle timeout
}
```

## Topic Helpers

The SDK provides helpers for constructing topic addresses:

```go
// Agent topics
topic := aether.AgentTopic("workspace", "impl", "spec")
// Result: "ag.workspace.impl.spec"

// Task topics
uniqueTopic := aether.UniqueTaskTopic("workspace", "impl", "spec")
// Result: "tu.workspace.impl.spec"

nonUniqueTopic := aether.TaskTopic("workspace", "impl", "id")
// Result: "ta.workspace.impl.id"

broadcastTopic := aether.TaskBroadcastTopic("workspace", "impl")
// Result: "tb.workspace.impl"

// User topics
userTopic := aether.UserTopic("user-id", "window-id")
// Result: "us.user-id.window-id"

userWsTopic := aether.UserWorkspaceTopic("user-id", "workspace")
// Result: "uw.user-id.workspace"

// Broadcast topics
agentBroadcast := aether.GlobalAgentsTopic("workspace")
// Result: "ga.workspace"

userBroadcast := aether.GlobalUsersTopic("workspace")
// Result: "gu.workspace"

// Event and metric topics
eventTopic := aether.EventWildcardTopic()
// Result: "event.*"

metricTopic := aether.MetricWildcardTopic()
// Result: "metric.*"
```

## Constants

```go
import "github.com/scitrera/aether/sdk/go/aether"

// Message types
aether.MessageTypeChat     // "CHAT"
aether.MessageTypeControl  // "CONTROL"
aether.MessageTypeToolCall // "TOOL_CALL"
aether.MessageTypeEvent    // "EVENT"
aether.MessageTypeMetric   // "METRIC"

// Task assignment modes
aether.TaskAssignmentSelfAssign // "SELF_ASSIGN"
aether.TaskAssignmentTargeted   // "TARGETED"
aether.TaskAssignmentPool       // "POOL"

// KV scopes
aether.KVScopeGlobal        // "global"
aether.KVScopeWorkspace     // "workspace"
aether.KVScopeUser          // "user"
aether.KVScopeUserWorkspace // "user-workspace"

// Signal types
aether.SignalForceDisconnect
```

## Admin Operations

`AdminClient` is a thin facade over a connected client that exposes named
helper methods for every administrative operation supported by the gateway:
token management, ACL rules, workspace and agent CRUD, gateway health /
info / stats queries, and forced session disconnects.

The underlying client must already be connected before any `AdminClient`
method is invoked. The recommended pattern is to wrap an existing
connected `AgentClient` or `UserClient` via `NewAdminClientFromBase`;
`NewAdminClient` is also available for cases where the caller wants
`AdminClient` to manage its own internal `UserClient`.

```go
package main

import (
    "context"
    "log"

    "github.com/scitrera/aether/sdk/go/aether"
)

func main() {
    ctx := context.Background()

    // 1. Connect any client (here, an Agent acting as an admin operator).
    agent, err := aether.NewAgentClient(aether.AgentOptions{
        ClientOptions: aether.ClientOptions{
            ServerAddr:  "localhost:50051",
            Credentials: aether.NewCredentials().WithAPIKey("admin-api-key"),
        },
        Workspace:      "ops",
        Implementation: "admin-agent",
        Specifier:      "ops-1",
    })
    if err != nil {
        log.Fatal(err)
    }
    if err := agent.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer agent.Close()

    // 2. Wrap it in an AdminClient.
    admin := aether.NewAdminClientFromBase(agent.BaseClient)

    // 3a. Workspace operation.
    wsr, err := admin.CreateWorkspace(ctx, aether.CreateWorkspaceOptions{
        WorkspaceID: "staging",
        DisplayName: "Staging Environment",
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("workspace created: success=%v", wsr.Success)

    // 3b. ACL operation: grant READWRITE on a workspace to a user.
    aclResp, err := admin.CreateACLRule(ctx, aether.CreateACLRuleOptions{
        PrincipalType: "user",
        PrincipalID:   "user-123",
        ResourceType:  "workspace",
        ResourceID:    "staging",
        AccessLevel:   20, // 0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN, 50=SUPERADMIN
        GrantedBy:     "ops",
        Reason:        "onboarding",
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("ACL granted: rule_id=%s", aclResp.Rule.RuleID)

    // 4. Token, agent, health, and session helpers are similarly typed.
    _, _ = admin.CreateToken(ctx, aether.CreateTokenOptions{
        Name:             "ci-token",
        PrincipalType:    "agent",
        ExpiresInSeconds: 24 * 60 * 60,
    })
    _, _ = admin.GetHealth(ctx, 0)
    _, _ = admin.DisconnectSession(ctx, aether.DisconnectSessionOptions{
        SessionID: "sess-789",
        Reason:    "maintenance",
    })
}
```

`AdminClient` mirrors the TypeScript SDK's `AdminClient` method-for-method,
with two Go-idiomatic adjustments forced by the proto contract:

- **`CreateACLRule.AccessLevel`** is an integer tier (`0/10/20/30/40/50`)
  rather than the free-form `permission` string used by the TS SDK. The
  numeric tiers match the server's `internal/acl/types.go`.
- **`ListTokens` / `ListAgents`** omit fields the TS SDK accepts but the
  server's proto filters do not actually support (`PrincipalType` on
  `TokenFilter`, `Workspace` on `AgentFilter`). See doc comments in
  `admin.go` for the relevant proto cross-references.

The lower-level surface is also available directly on `BaseClient` when a
typed `AdminClient` facade is unwanted:

```go
// Workspace, Agent, ACL, Token, Workflow, Admin (queries), Session.
bc := agent.BaseClient
bc.Workspace().Create(ctx, &pb.WorkspaceInfo{WorkspaceId: "staging"})
bc.ACL().Grant(ctx, &pb.ACLGrantRequest{ /* ... */ })
bc.Tokens().Create(ctx, "ci", "agent", []string{"*"}, []string{"connect"}, 24, "ops")
bc.Admin().SendOpSync(ctx, &pb.AdminQuery{Op: pb.AdminQuery_GET_HEALTH}, 0)
bc.Session().Disconnect(ctx, "sess-789", "maintenance")
```

## Proxy

Route HTTP requests through the Aether connection to a service principal using
`AetherRoundTripper`, which implements `http.RoundTripper`:

```go
rt := &aether.AetherRoundTripper{
    Client: agentClient,
    Target: "sv::memorylayer::default",
}
httpClient := &http.Client{Transport: rt}
resp, err := httpClient.Get("http://ignored/v1/memories/abc")
```

OBO authorization is passed via context:

```go
ctx = aether.WithOBOAuthorization(ctx, authContext)
req, _ = http.NewRequestWithContext(ctx, "GET", "http://ignored/v1/memories/abc", nil)
resp, err = httpClient.Do(req)
```

For full details on sidecar deployment, service addressing, ACL/OBO model,
limits, audit events, and failure modes, see
[server/docs/proxy.md](../../server/docs/proxy.md).

## Progress Reporting

Agents and tasks can report fine-grained progress to the gateway. Progress is
supplemental to the task lifecycle — connection liveness handles death detection
separately. The gateway fans progress out to the `pg.{workspace}` stream for
subscribers (users, orchestrators, other agents).

```go
import pb "github.com/scitrera/aether/api/proto"

// Basic indeterminate progress
err = client.ReportProgress(aether.ReportProgressOptions{
    TaskID:  "task-abc",
    State:   "running",
    Completion: -1.0,   // -1 = indeterminate; 0.0–1.0 for a known fraction
    Summary: "Analyzing document...",
})

// Multi-step progress with recipient routing
err = client.ReportProgress(aether.ReportProgressOptions{
    TaskID:       "task-abc",
    State:        "running",
    Completion:   0.5,
    Summary:      "Extracting text",
    StepName:     "Extracting text",
    StepDetail:   "Processing page 2 of 4",
    StepSequence: 2,
    StepTotal:    4,
    StepType:     "processing",       // UI rendering hint
    Recipient:    "us::user-1",       // route to a specific user (all windows)
    RequestID:    "req-xyz",          // correlate with originating request
    Metadata:     map[string]string{"thread_id": "thread-1"},
    Kind:         pb.ProgressKind_PROGRESS_KIND_CHAT, // surface in chat thread
})
```

### ProgressKind values

| Constant | Meaning |
|---|---|
| `pb.ProgressKind_PROGRESS_KIND_UNSPECIFIED` (0) | Unclassified; receivers apply legacy heuristics |
| `pb.ProgressKind_PROGRESS_KIND_CHAT` (1) | Chat-thread progress bar |
| `pb.ProgressKind_PROGRESS_KIND_APP` (2) | App-dashboard long-running job |
| `pb.ProgressKind_PROGRESS_KIND_TASK` (3) | Task lifecycle events (orchestrator/agent consumers) |

## Workspace Switching

Agents, tasks, and users can switch to a different workspace on an active
connection. The gateway updates the entity's topic subscription and may send a
fresh `ConfigSnapshot` for the new workspace.

```go
// Switch to a new workspace (updates both gateway-side subscription and local state)
err = client.SwitchWorkspace("new-workspace")
```

For **user clients**, call `SwitchWorkspace` immediately after `Connect()` to
declare the user's active app workspace. User identity does not encode a
workspace, so without this call `Identity.Workspace` remains empty on the
server side — which breaks features like task-authority grant scoping.

```go
userClient.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
    return userClient.SwitchWorkspace("default")
})
```

## Principal Types

Aether supports eight principal types that map to distinct identity and routing
semantics:

| Type | Constructor | Topic format | Notes |
|---|---|---|---|
| **Agent** | `NewAgentClient` | `ag.{ws}.{impl}.{spec}` | Persistent singleton; globally unique per identity |
| **Task** | `NewTaskClient` | `tu.{ws}.{impl}.{spec}` or `ta.{ws}.{impl}.{id}` | Unique task (`tu`) or server-assigned non-unique (`ta`/`tb`) |
| **User** | `NewUserClient` | `us.{user}.{window}` | Unique per window; call `SwitchWorkspace` after connect |
| **Orchestrator** | `NewOrchestratorClient` | — | Receives `TaskAssignment` for offline agents/tasks; launches compute |
| **WorkflowEngine** | `NewWorkflowEngineClient` | — | Can send to all topic types; receives `event.*` stream |
| **MetricsBridge** | `NewMetricsBridgeClient` | — | Receive-only; collects `metric.*` stream for telemetry |
| **Service** | `NewServiceClient` | `sv.{impl}.{spec}` | Sidecar/utility principals exposing HTTP-over-Aether proxy endpoints; workspace-scoped |
| **Bridge** | `NewBridgeClient` | `br.{impl}.{spec}` | Cross-workspace integration (Discord, Teams, Email, etc.); not workspace-scoped, checks ACL per message against target workspace |

**Service vs Bridge:** A `Service` principal lives within a single workspace
and is addressable via the proxy HTTP transport (`AetherRoundTripper`). A
`Bridge` has no workspace component in its identity and may route messages
across any workspace — it performs per-message ACL checks against the target
workspace rather than a single upfront check at connect time.

## Key Architectural Principle

**Connection = Lock = Heartbeat**: The connection itself IS the distributed lock AND the heartbeat. When the gRPC stream closes, the identity lock is immediately released. No separate heartbeat API exists.

## Requirements

- Go 1.21+
- gRPC 1.78.0+
- Protobuf 1.36.0+

## License

Copyright 2024-2025 Scitrera. Licensed under the Apache License, Version 2.0.

## Links

- [GitHub Repository](https://github.com/scitrera/aether)
- [Documentation](https://github.com/scitrera/aether#readme)
- [Issue Tracker](https://github.com/scitrera/aether/issues)
