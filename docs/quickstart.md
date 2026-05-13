# Quickstart: Hello World in 15 Minutes

This guide walks you through starting a local Aether Gateway and connecting two agents that exchange a message. No prior knowledge of the system is required. By the end you will have a working local environment and a foundation to build on.

There are two paths: **Option A** uses AetherLite (no external services, zero setup), and **Option B** uses the full stack with Docker-managed Redis and RabbitMQ.

## Prerequisites

- **Go 1.25+** — [Install Go](https://go.dev/dl/)
- **Python 3.12+** (optional) — only needed if you want to use the Python client

Verify Go is installed:

```bash
go version
# go version go1.25.x linux/amd64
```

---

## Option A: AetherLite Quickstart (no external dependencies)

AetherLite is a single-binary deployment mode that replaces Redis, RabbitMQ, and PostgreSQL with embedded in-process equivalents (Badger KV and SQLite). There is nothing to install beyond Go itself.

### Step A1: Build AetherLite

```bash
cd server
go build -o aetherlite ./cmd/aetherlite
```

### Step A2: Start AetherLite

```bash
AETHER_ALLOW_DEV_MODE=true ./aetherlite --dev --insecure-admin
```

AetherLite creates a `./aether-lite-data/` directory on first run and stores all state there. It listens on the same default ports as the full gateway:

| Setting | Default |
|---|---|
| gRPC port | `50051` |
| Admin UI port | `31880` |
| Data directory | `./aether-lite-data/` |

You should see output like:

```
AetherLite v0.1.0 — embedded single-binary server
INF AetherLite gRPC gateway listening addr=:50051
INF admin UI listening port=31880
INF AetherLite is ready
```

The admin UI is available at `http://localhost:31880`. Skip ahead to [Step 3](#step-3-connect-two-agents-using-the-go-sdk) — the SDK examples work identically against AetherLite.

For more information about AetherLite, see [aetherlite.md](aetherlite.md).

---

## Option B: Full Aether with External Services

The full deployment uses Docker-managed RabbitMQ and Redis and exposes the complete production feature set.

### Prerequisites (Option B only)

- **Docker** — to run RabbitMQ and Redis via the provided scripts

## Step 1: Start Dependencies

Aether requires RabbitMQ Streams for messaging and Redis (Valkey) for session state. The repository includes Docker scripts for both.

Open two terminal windows (or run both in the background):

```bash
# Terminal 1 — RabbitMQ with Streams plugin
# Starts on: stream port 55552, AMQP port 55672, management UI at http://localhost:15672
./scripts/docker_rmq_test.sh

# Terminal 2 — Redis (Valkey)
# Starts on: port 56379 (cluster ports 56379-56381)
./scripts/docker_valkey_test.sh
```

Wait a few seconds until both containers report that they are ready. You can verify RabbitMQ by opening `http://localhost:15672` (credentials: `guest` / `guest`).

## Step 2: Build and Start the Gateway

```bash
# Build the gateway binary
go build -o gateway ./cmd/gateway

# Start with development defaults (no config file required)
./gateway --dev --insecure-admin
```

The `--dev` flag allows the gateway to start without a `configs/dev.yaml` file, using hardcoded development defaults:

| Setting | Default (dev mode) |
|---|---|
| gRPC port | `50051` |
| Admin UI port | `31880` |
| Redis | `localhost:56379` |
| RabbitMQ Stream | `rabbitmq-stream://guest:guest@localhost:55552` |
| RabbitMQ AMQP | `amqp://guest:guest@localhost:55672/` |

The `--insecure-admin` flag permits the admin UI to run without an API key. This is only appropriate for local development.

You should see output similar to:

```
Aether Gateway v0.1.0
Aether Gateway starting...
...
INF Aether Gateway listening addr=:50051
INF admin server starting (no TLS) addr=:31880
```

The admin UI is now available at `http://localhost:31880`.

If you have a `configs/dev.yaml` file (see `README.md` for an example), omit `--dev` and run:

```bash
./gateway --config configs/dev.yaml --insecure-admin
```

## Step 3: Connect Two Agents Using the Go SDK

The Go SDK lives in `sdk/go/`. Each principal type has a dedicated client constructor. Agents use `aether.NewAgentClient`.

Below is a minimal example that connects two agents and sends a message from one to the other. You can also run the full-featured example directly:

```bash
# In one terminal — start agent-a
go run ./sdk/go/examples/agent/main.go \
  -server=localhost:50051 \
  -workspace=hello \
  -impl=greeter \
  -spec=agent-a

# In another terminal — start agent-b
go run ./sdk/go/examples/agent/main.go \
  -server=localhost:50051 \
  -workspace=hello \
  -impl=greeter \
  -spec=agent-b
```

## Step 4: Send a Message Between Agents

The minimal code below connects an agent, registers a message handler, and sends one message to another agent in the same workspace. Both agents must be connected for message delivery.

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"

    "github.com/scitrera/aether/sdk/go/aether"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() { <-sigChan; cancel() }()

    client, err := aether.NewAgentClient(aether.AgentOptions{
        ClientOptions: aether.ClientOptions{
            ServerAddr: "localhost:50051",
        },
        Workspace:      "hello",
        Implementation: "greeter",
        Specifier:      "agent-a",
    })
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to create client: %v\n", err)
        os.Exit(1)
    }

    // Register handler for incoming messages
    client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
        fmt.Printf("received from %s: %s\n", msg.SourceTopic, string(msg.Payload))
        return nil
    })

    if err := client.Connect(ctx); err != nil {
        fmt.Fprintf(os.Stderr, "connect failed: %v\n", err)
        os.Exit(1)
    }

    // Send a message to agent-b in the same workspace
    if err := client.SendToAgent("hello", "greeter", "agent-b", []byte("hello from agent-a")); err != nil {
        fmt.Fprintf(os.Stderr, "send failed: %v\n", err)
    }

    // Block until interrupted
    client.Run(ctx)
}
```

Run a mirror of this with `Specifier: "agent-b"` (and a `SendToAgent` targeting `agent-a`) in a second terminal. When both are connected you will see the message printed in each terminal.

The topic that `agent-a` subscribes to is `ag.hello.greeter.agent-a`. The `SendToAgent` call routes to `ag.hello.greeter.agent-b`. All routing happens inside the gateway over RabbitMQ Streams — the clients never communicate directly.

## Step 5: Verify in the Admin UI

Open `http://localhost:31880` in a browser. The Connections view shows both active agent sessions, their session IDs, and their topics. The KV Browser lets you inspect any workspace configuration pushed to connecting clients.

## What's Next

- **Full SDK examples** — `sdk/go/examples/agent/`, `sdk/go/examples/task/`, `sdk/go/examples/orchestrator/`
- **System specification** — [`specification.md`](specification.md) covers every principal type, topic schema, permission matrix, and the orchestration / lazy-loading flow
- **Horizontal scaling** — [`docs/horizontal-scaling.md`](horizontal-scaling.md) explains how to run multiple gateway instances with Redis-based distributed locking
- **Admin UI reference** — [`docs/admin-ui.md`](admin-ui.md) documents all REST endpoints and the WebSocket monitoring interface
- **Error codes** — [`docs/error-codes.md`](error-codes.md)
- **Python client** — `sdk/python-client/` with examples in `sdk/python-client/tests/`

## Troubleshooting

**`DuplicateIdentityError` on connect** — another process is connected with the same workspace/implementation/specifier triple. Either stop the other process or change the specifier.

**Connection refused on port 50051** — the gateway is not running or bound to a different port. Check the gateway log for the `listening addr` line.

**`Config file not found`** — you started the gateway without `--dev` and no `configs/dev.yaml` exists. Add `--dev` for local development, or create the config file (see `README.md`).

**RabbitMQ not ready** — the Docker container needs a few seconds to start. Wait for the management UI at `http://localhost:15672` to respond before starting the gateway.
