# scitrera-aether-ag2

ag2 (AutoGen 2) adapter for Scitrera Aether, the distributed control plane for
multi-agent systems. Wrap any `ConversableAgent`, host it on Aether, and reach
it from another process as if it were local — or let Aether spin it up on demand
when a message arrives for an offline agent.

## Install

```bash
# from the sdk/python-ag2 directory
pip install -e ../python-client   # Aether Python client
pip install -e ".[dev]"           # this package + dev deps
```

## Build aetherlite

The examples and E2E tests need the `aetherlite` binary (a single-binary
all-in-one Aether gateway for local development):

```bash
cd ../../server
go build -o aetherlite ./cmd/aetherlite
```

The binary is expected at `server/aetherlite` relative to the repo root.
Override with `AETHERLITE_BIN=/path/to/binary`.

## Quickstart

The `two_agent_remote` example spawns its own gateway, hosts an EchoAgent,
and calls it over Aether in under 10 seconds:

```bash
cd sdk/python-ag2
./.venv/bin/python -m scitrera_aether_ag2.examples.two_agent_remote
```

Expected output:

```
Reply from remote agent: echo: hi
```

Source: `scitrera_aether_ag2/examples/two_agent_remote.py`

## Concepts

- **AetherAgentHost** — server side. Wraps a `ConversableAgent`, connects to
  Aether as an agent principal, and dispatches incoming `RequestEnvelope`
  messages to ag2's `AgentService`. Optionally persists conversation history
  via checkpoints and emits telemetry metrics.

- **AetherRemoteAgent** — client side. Implements the ag2 `Agent` protocol
  locally; on `a_receive()` it serialises the conversation turn into a
  `RequestEnvelope`, sends it over Aether to the host, streams responses back,
  and handles tool-call continuations and HITL prompts.

- **AetherAg2Orchestrator** — lazy spin-up. Extends the Python SDK's
  `MultiprocessOrchestrator`. When Aether's dispatcher sends a `TaskAssignment`
  for an offline agent, the orchestrator reads `launch_params["factory"]`
  (a `module:callable` string), builds an env dict, and calls `spawn_module()`
  to launch `python -m scitrera_aether_ag2.host_entrypoint` as a subprocess.

## HITL, tool-calls, streaming

**HITL**: pass `hitl_mode="sender"` to `AetherRemoteAgent` to forward
human-input prompts to `sender.get_human_input()`, or `"auto_skip"` to
silently skip them. Default `"raise"` raises `HITLRequired`.

**Tool calls**: `AetherRemoteAgent` detects tool calls in the remote reply that
name tools registered on the sender, executes them locally, and sends a
continuation request automatically (up to `max_continuations=8`).

**Streaming**: set `iostream_streaming=True` (default) to forward streaming
text chunks to ag2's `IOStream` as `StreamEvent`s as they arrive.

## Tests

```bash
# Unit tests only (no gateway needed):
cd sdk/python-ag2
./.venv/bin/python -m pytest

# Full E2E suite (spawns aetherlite):
AETHER_RUN_E2E=1 ./.venv/bin/python -m pytest -v
```

## Status

Alpha. The v1 surface is stable enough for experimentation. Known gaps and
deferred items are tracked in `.slop/ag2-adapter-tech-debt.md`.
