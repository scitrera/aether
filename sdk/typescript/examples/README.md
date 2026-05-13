# Aether TypeScript SDK — Examples

Minimal runnable examples demonstrating the core Aether client types. Each example
is self-contained, reads configuration from environment variables, and shuts down
gracefully on `SIGINT` / `SIGTERM`.

## Prerequisites

```bash
cd sdk/typescript
npm install
```

You'll also need a running Aether gateway. The easiest local setup:

```bash
cd ../../server
go run ./cmd/aetherlite --insecure-admin
```

## Running an example

The examples import from the local SDK source (`../../src/index.js`), so use `tsx`
to run them directly:

```bash
npx tsx examples/agent/index.ts
npx tsx examples/task/index.ts
npx tsx examples/orchestrator/index.ts
```

Configure with environment variables:

| Variable             | Default            | Used by         |
|----------------------|--------------------|------------------|
| `AETHER_SERVER`      | `localhost:50051`  | all              |
| `AETHER_WORKSPACE`   | `default`          | agent, task      |
| `AETHER_API_KEY`     | _(unset)_          | all (optional)   |
| `AETHER_TENANT`      | _(unset)_          | all (optional)   |
| `AETHER_TASK_UNIQUE` | `true`             | task             |
| `AETHER_PROFILES`    | `docker,kubernetes`| orchestrator     |

## What each example shows

### `agent/`
Agent client: connect, register handlers, send a message to itself, perform KV
operations across scopes (`Global` put/get-sync), publish an event and a metric,
create a self-assigned task, then idle waiting for messages.

### `task/`
Task client: supports both unique (`AETHER_TASK_UNIQUE=true`) and non-unique
worker-pool mode. Sends messages to an agent, to a unique task, to a task pool
(broadcast), and to a user. Publishes an event and a metric.

### `orchestrator/`
Orchestrator client: registers `onTaskAssignment` to receive lazy-startup requests
from the gateway, dispatches by profile (`docker` / `kubernetes`), and sends a
status message back to the target agent. Demonstrates the orchestration lifecycle
the gateway uses when a targeted identity is offline.

## Mapping to Go SDK examples

These mirror the canonical Go examples under `sdk/go/examples/{agent,task,orchestrator}/main.go`
but are intentionally shorter. The Go examples include richer demos (TLS config
loading, full error-type matching). Consult them for production-grade error
handling patterns.
