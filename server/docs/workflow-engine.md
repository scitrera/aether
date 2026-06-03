# Aether Workflow Engine (WFE)

The Workflow Engine is Aether's event-triggered automation layer. It consumes a
shared event plane, matches events against declarative **rules**, and dispatches
**destinations** — sending a tool-call to an agent, spawning a POOL task,
emitting another event, or driving a **join** (fan-in / barrier / coalesce). It
also runs cron-style **schedules**, a **DAG engine**, and **state machines**.

This document covers the full engine: the rule/event model, every destination
kind (with joins covered in depth), the scheduler, the admin API, configuration,
and the design decisions behind the join subsystem.

---

## 1. Architecture

The WFE is a **standalone binary** (`cmd/workflow`) that connects to the Aether
gateway as a `WorkflowEngineClient` (one of the seven principal types). It holds
no Redis connection of its own and owns no database driver: persistence is
injected as a `WorkflowStore` (Postgres or SQLite), and all distributed
coordination runs over the gateway's KV via the SDK.

```
                    ┌──────────────────────── Workflow Engine ────────────────────────┐
   event::receiver0 │  Router ──┬─► Executor ─► (message | create_task | emit_event)   │
   (gateway fan-in) │           └─► JoinEngine ─► fan-in / barrier / coalesce          │
                    │  Scheduler ─► due schedules + due join deadlines (leader-gated)  │
                    │  DAGEngine · StateMachineEngine · AdminServer (REST :31881)       │
                    │  LeaderElector (coord lease over gateway KV)                      │
                    └──────────────────────────────────────────────────────────────────┘
                         │ WorkflowEngineClient (gRPC, single bidi stream)
                         ▼
                    Aether Gateway  ──  KV (Redis | Badger | NATS JetStream)
```

**Components** (`internal/workflow/`):

| Component | Responsibility |
|---|---|
| `Router` | Consumes the event plane, matches rules, renders destinations, routes to the Executor or JoinEngine. |
| `Executor` | Dispatches actions: tool-call `message`, POOL `create_task`, `emit_event`. |
| `JoinEngine` | Fan-in / barrier / coalesce joins (this document's focus). |
| `Scheduler` | Leader-gated poll loop: fires due schedules and sweeps due join deadlines. |
| `DAGEngine` | Multi-step workflow definitions with intra-definition step ordering. |
| `StateMachineEngine` | Event-driven state machines with entry/exit actions and timeouts. |
| `LeaderElector` | Elects one active replica (coord lease over gateway KV) so only one runs the scheduler/monitors. |
| `AdminServer` | REST API (default `:31881`) for CRUD + observability. |

**Leadership.** Election runs on the shared coord primitive (atomic
`SetNX`/`CompareAndSet` lease) over a `coord.Locker` backed by the gateway KV, on
the global scope under the reserved key `_sys/coord/workflow/leader` (30s TTL, 10s
renew). One leader is elected across replicas in **every** backend mode; the
scheduler and monitors run only on the leader.

**Mode portability.** Everything the engine does — leader election, the join
arrival counter, dedup ledger, fire-markers, set membership — is built on the
portable `KVReadWriter` primitives (`IncrementIf`, `SetNX`, `CompareAndSet`,
`CompareAndDelete`, `SetAdd`/`SetCard`), which are implemented natively in all
three backends: **Redis** (Lua), **Badger** (optimistic-CC retry), **NATS
JetStream** (revision-guarded CAS). The engine code is identical across
aether-full, aetherlite-single, and aetherlite-cluster.

---

## 2. Events, rules, and the dispatch path

### 2.1 The event plane

Clients publish events with `SendEvent(payload)` → topic `event.*`, which the
gateway rewrites to a fan-in shard (`event::receiver0` today). The payload is a
JSON `EventPayload`:

```json
{ "source_agent": "memorylayer", "workspace": "ws-1",
  "event_names": ["memorylayer.ingest_complete"], "data": { "...": "..." } }
```

`source_agent` and `workspace` are back-filled from the topic segments when
omitted. The Router processes each name in `event_names` independently.

### 2.2 Rules

A `Rule` (persisted in `workflow_rules`) is:

| Field | Meaning |
|---|---|
| `source_event` | Event name to match (exact). |
| `source_agent` | Match a specific agent or `*`. |
| `workspace` | Match a specific workspace or `*`. |
| `trigger_condition` | Optional **expr-lang** boolean over the arrival env. |
| `destination_template` | Go `text/template` → YAML describing the destination. |
| `priority`, `active` | Ordering (desc) and on/off. |

`GetMatchingRules(agent, event, workspace)` selects `active = true AND
source_event = $event AND (source_agent = $agent OR '*') AND (workspace =
$workspace OR '*')` ordered by `priority DESC, id ASC`, cached with a TTL
(invalidated on any rule CRUD).

### 2.3 Evaluation env

For each matched rule the Router builds:

```
{ input:  <event.data>,
  source: { agent: <source_agent>, workspace: <workspace>, event: <event_name> } }
```

- `trigger_condition` is evaluated by **expr-lang** (`github.com/expr-lang/expr`)
  — not CEL. Empty ⇒ always matches.
- `destination_template` is rendered by Go `text/template` (with
  `missingkey=zero`), then parsed as YAML into a `TransformResult`.

> **One expression dialect.** Every expression field in the engine — trigger
> conditions and all join expressions (`correlation_key`, `expected_count`,
> `dedup_key`, `member_key`, `expected_set`) — uses **expr-lang**. Template
> *substitution* (`{{ .source.workspace }}`) is Go `text/template`, applied to
> the destination before YAML parsing.

### 2.4 Destination kinds

The rendered `type` field selects the destination:

| `type` | Action |
|---|---|
| _empty_ / `message` | Tool-call to an agent (`agent`, `tool_name`, `arguments`). |
| `create_task` | Spawn a POOL task (`task_type`, `target_implementation`, `payload`, `metadata`, `retry`). |
| `emit_event` | Publish a synthetic event back onto `event.*` (`event_name`, `payload`) — enables chaining. |
| `join` | Fan-in / barrier / coalesce (see §3). |

Existing `message`/`create_task` rules are unchanged by the join work; `join`
and `emit_event` are additive.

**Event chaining** (the pre-existing multi-step idiom): task A completes →
worker emits an event → a rule spawns task B → … This handles *linear/sequential*
flows. Joins add the one thing chaining cannot express: **parallel fan-out
followed by a barrier.**

---

## 3. Joins (fan-in / barrier / coalesce)

A **join** answers: *"after the last of N correlated things finishes (or a
deadline elapses), do X — exactly once."* It is declared as a rule whose rendered
destination is `type: join`.

### 3.1 The join destination schema

```yaml
type: join
join:
  name: ingest-decompose-barrier      # logical join id (per workspace)
  correlation_key: "input.job_id"      # expr-lang → the barrier/group id (REQUIRED)
  mode: count                          # count | coalesce | set
  # --- count mode ---
  expected_count: "input.expected_count"  # expr-lang → integer (literal or off the arm event)
  arm_on_event: memorylayer.ingest_complete  # event that supplies expected_count (dynamic-N)
  # --- set mode ---
  expected_set: '["page:1","page:2","page:3"]'  # expr-lang → list; completes at full membership
  member_key: "input.memory_id"                  # expr-lang → this arrival's member id
  # --- common ---
  dedup_key: "input.memory_id"         # expr-lang → per-arrival dedup id (count/coalesce)
  window: "5s"                         # coalesce debounce/cooldown
  timeout: "15m"                       # barrier deadline (drives the sweep)
  linger: "1m"                         # post-terminal late-arrival retention
  on_partial_failure: proceed_degraded # proceed | proceed_degraded | abort
  on_complete:                         # action fired when the barrier completes
    type: create_task
    task_type: memorylayer-task.kb_update
    target_implementation: memorylayer
    workspace: "{{ .source.workspace }}"
    payload: { workspace_id: "{{ .source.workspace }}" }
  on_timeout:                          # action fired on deadline (falls back to on_complete)
    type: create_task
    task_type: memorylayer-task.kb_update
    target_implementation: memorylayer
    workspace: "{{ .source.workspace }}"
    payload: { workspace_id: "{{ .source.workspace }}", degraded: true }
```

`on_complete` / `on_timeout` are ordinary actions (`create_task` or
`emit_event`). Their `text/template` fields are rendered when the enclosing
destination is transformed, so the engine sees concrete values.

### 3.2 Fan-in matching — the correlation contract

A join does **not** fire because two events arrived. It fires because two events
arrived that **resolve to the same correlation-key value.** The join instance —
and its KV keys — are keyed by `(workspace, join_name, correlation_key_value)`.
Different values ⇒ different instances ⇒ they never cross.

```
correlation_key: "input.job_id"

decompose_complete {job_id:"A", memory_id:"m1"} → key "A" → counter …:A = 1
decompose_complete {job_id:"A", memory_id:"m2"} → key "A" → counter …:A = 2   ✓ same origin
decompose_complete {job_id:"B", memory_id:"m9"} → key "B" → counter …:B = 1   ✗ separate barrier
```

So "same origin" is **entirely** determined by whether the producer stamps the
same correlation id on every member of a logical group. Two hard producer-side
requirements:

1. **Every member event carries a shared correlation id.** No id ⇒ a degenerate
   key that never completes (the engine rejects an empty correlation key).
2. **N (or the member set) is supplied** — a literal, an expression evaluable on
   every arrival, or a manifest/arm event (dynamic-N, §3.4).

The engine guarantees, given a correct id: per-origin isolation, single-firer
election, idempotent downstream, and a deadline backstop.

### 3.3 Count mode

Each **member** arrival atomically bumps a per-correlation counter
(`IncrementIf(+1, ceiling=2^40)` — a sentinel ceiling, so it always applies and
returns the arrival's sequence number) and mirrors `arrived_count` to the SQL
row. When the count reaches the known `expected_count`, a single firer is
elected and `on_complete` runs once.

### 3.4 Dynamic-N arming

When the count is not known at first arrival, a designated **arm event**
(`arm_on_event`) supplies it later. An arming arrival is **not** counted as a
member (arming typically rides a *different* event, e.g. a manifest); it records
`expected_count` and re-reads the counter, so an arm that lands *after* all
members still completes the barrier. (Register two rules pointing at the same
join `name`/`correlation_key`: one for the member event, one for the arm event.)

### 3.5 Coalesce mode

Collapses a burst into one firing — the server-side generalization of
MemoryLayer's hand-rolled `kb_update` lease. The first arrival acquires the
active/cooldown gate (TTL = `window`) and fires; arrivals while the gate is held
set the `dirty` flag (a trailing run is driven by the next post-cooldown arrival
or the deadline sweep). With `window` empty, each arrival fires (no debounce).
Use coalesce when the downstream **self-heals** (re-running corrects a slightly
stale result), e.g. KB regeneration.

### 3.6 Set mode

Completes when a known **set** of member ids has each reported at least once. Each
arrival adds `member_key` to a KV set via the atomic `SetAdd` primitive (the set
inherently dedups, so set mode needs no separate `dedup_key`); the join fires when
the set cardinality reaches `len(expected_set)`. The member whose add completes
the set is the unique firer.

### 3.7 Dedup (count / coalesce)

When `dedup_key` is set, the first arrival with a given dedup id wins a per-id
`TryAcquire` (`SetNX`) slot under the join base; repeats are dropped and counted
as late. Dedup id source: the event id for explicit events. (Set mode dedups via
set membership instead.)

### 3.8 Deadline / timeout sweep and partial failure

Each instance with a `timeout` carries a `deadline_at`. The **leader-gated
scheduler** sweeps `GetDueJoinDeadlines(now)` and calls `HandleDeadline` per open,
past-deadline instance:

- `on_partial_failure: abort` → mark `timed_out`, fire nothing.
- `proceed` / `proceed_degraded` / _empty_ → fire the persisted `on_timeout`
  action (falling back to `on_complete`), then mark `timed_out`.

The actions are **persisted** on the row (`on_complete`/`on_timeout` JSON) so the
sweep can fire them with no live event. A never-completing barrier is GC'd at its
deadline rather than leaking.

### 3.9 Exactly-once firing

Two layers guarantee the downstream runs exactly once:

1. **Single-firer election** — completeness is detected atomically, and a
   `fire-marker` (`TryAcquire base+"/fired"`) ensures exactly one caller (a
   completing arrival *or* the deadline sweep) runs the action. The marker also
   prevents a deadline-fire and a late completion from both firing.
2. **Idempotent `create_task`** — the firer stamps a stable
   `idempotency_key = join:{name}:{workspace}:{correlation_key}` (identical across
   the `on_complete` and `on_timeout` paths). The gateway dedups creation on that
   key via a `SetNX` ledger under `_sys/idem/task/…`: the first create proceeds and
   records its task_id; duplicates (retries, engine restart, a timeout racing a
   completion) create no second task. Fail-open on KV outage (a rare duplicate is
   preferable to a dropped task).

### 3.10 KV keys and storage

The hot state lives in KV under the reserved infra namespace (granted to the WFE
via the gateway fast-path), keyed by a hash of `(name, workspace, correlation_key)`:

```
_sys/coord/joins/{hash}            # count: arrival counter
_sys/coord/joins/{hash}/members    # set:   member set
_sys/coord/joins/{hash}/d/{hash}   # dedup ledger (per dedup id)
_sys/coord/joins/{hash}/fired      # fire-marker (exactly-once gate)
_sys/coord/joins/{hash}/active     # coalesce cooldown gate
```

All carry a TTL of `timeout + linger` so abandoned instances self-GC even if the
sweep never runs. The durable mirror is the **`workflow_joins`** table (one row
per `(join_name, workspace, correlation_key)`): mode, expected/arrived counts,
dirty, status (`open|fired|timed_out|cancelled`), `deadline_at`, `linger_until`,
the persisted actions, and timestamps. The SQL row is the observability surface
and deadline driver; **KV is the firing arbiter**, the row a mirror.

### 3.11 Observability and operator control

| Op | Admin REST | Effect |
|---|---|---|
| `LIST_JOINS` | `GET /joins[?workspace=]` | List in-flight + recently-terminal instances. |
| `GET_JOIN` | `GET /joins/{name}/{corr}` | Inspect one (404 if absent). |
| `CANCEL_JOIN` | `POST /joins/{name}/{corr}/cancel` | Mark a wedged instance `cancelled`. |

The `joinView` surface: `join_name, workspace, correlation_key, mode, arrived,
expected, dirty, status, deadline_at, linger_until, created_at, updated_at`.
Internal action JSON is not exposed.

### 3.12 Worked example: ingest → decompose → kb_update

The headline barrier (a *future* MemoryLayer architecture; today ingestion is
synchronous and `kb_update` is coalesced by a hand-rolled lease):

- **Producer contract:** each async `decompose_facts` task emits
  `memorylayer.decompose_complete` stamped with the shared `job_id`; the
  finalize step emits `memorylayer.ingest_complete` carrying
  `expected_count = total_memories_created`.
- **Rule M** (member): `decompose_complete → join` (count mode,
  `correlation_key: input.job_id`, `dedup_key: input.memory_id`).
- **Rule A** (arm): `ingest_complete → join` (same `name`/`correlation_key`,
  `arm_on_event: ingest_complete`, `expected_count: input.expected_count`).
- On completion (or timeout), one `kb_update` task is created — exactly once.

**Simpler alternative (recommended until strict completeness is required):** skip
the count barrier and register `decompose_complete → join` in **coalesce** mode,
relying on `kb_update`'s self-healing. Use count mode only when the downstream
does **not** self-heal (batch-import summary, scatter/gather aggregation).

### 3.13 Feed B — gathering over task completions

A join can gather over **task completions** without the worker emitting its own
event. A task opts in via a `completion_event` config (persisted on the task);
when it reaches a selected terminal status the server emits a JSON `EventPayload`
onto `event::*`:

```json
{ "source_agent": "orchestrator", "workspace": "ws-1",
  "event_names": ["memorylayer.decompose_complete"],
  "data": { "task_id": "...", "status": "completed", "task_type": "...",
            "correlation_id": "job-42", "root_task_id": "...", "metadata": {…} } }
```

A **fan-out rule** tags the member tasks it spawns by setting `correlation_id`
and `completion_event` on its `create_task` destination:

```yaml
type: create_task
task_type: decompose_facts
target_implementation: memorylayer
correlation_id: "{{ .input.job_id }}"
completion_event:
  enabled: true
  event_name: memorylayer.decompose_complete   # empty ⇒ task.completed/failed/cancelled
```

The **join's member rule** then matches `memorylayer.decompose_complete` with
`correlation_key: input.correlation_id`. No worker code changes — the member task
just completes.

- `completion_event.on_statuses` (proto) restricts emission to specific terminal
  statuses; empty ⇒ all (`completed`, `failed`, `cancelled`). The emitted event
  name defaults to `task.<status>` when `event_name` is empty.
- Emission is best-effort and non-fatal: a publish failure never blocks the task
  transition.
- `correlation_id` / `root_task_id` are persisted on the task (both stores) and
  queryable via `TaskFilter`; `root_task_id` defaults to the task's own id when a
  task has no supplied root (it is its own flow root).

**Feed A vs feed B.** Feed A (the worker emits a domain event) gives full control
over the event name and payload. Feed B (the server emits on terminal status)
needs no worker code — useful for POOL tasks you don't own. Both land on
`event::*` as `EventPayload`s and are matched identically.

---

## 4. Scheduler

A leader-gated ticker (`scheduler.go`) polls on `GetSchedulerPollInterval()`:

1. **Due schedules** — `GetDueSchedules(now)` → fire each (cron or interval),
   update `last_fired_at`/`next_fire_at`.
2. **Due join deadlines** — `GetDueJoinDeadlines(now)` → `HandleDeadline` per
   open, past-deadline join (§3.8).

Both run only on the elected leader (`IsLeader()`), so deadline sweeps fire once
cluster-wide.

---

## 5. DAG engine & state machines (existing)

- **DAG engine** (`dag.go`): `WorkflowDefinition`s with steps and intra-definition
  `depends_on` ordering, executed against `workflow_executions`/`step_states`,
  concurrency-capped. Expresses *defined* multi-step workflows — not runtime fan-in
  over an externally-produced set (that's what joins add). The join destination is
  a strict subset a future full-DAG AND-join can absorb; its `correlation_id` /
  `flow_id` design (§7) is forward-compatible as the eventual DAG run id.
- **State machines** (`statemachine.go`): definitions with states, transitions,
  entry/exit actions, and timeouts; instances advance on events.

---

## 6. Admin REST API

Default port `:31881` (gorilla/mux, API-key auth). CRUD for `rules`, `workflows`,
`schedules`, `executions`, `state machines`, plus the join observability
endpoints (§3.11). The same operations are reachable as `WorkflowOperation`s over
the gateway (forwarded to the WFE).

---

## 7. Design decisions & rationale

- **Join destination first, full DAG later.** A declarative join reuses the
  production rule path, the portable KV primitives, and the event plane with ~one
  table and one action case. The DAG engine's `depends_on` is *intra-definition*;
  it cannot natively join a runtime-discovered, externally-spawned set without
  re-inventing the correlation/expected-count machinery joins define. Joins are the
  right incremental primitive; the DAG AND-join is the eventual home.
- **KV is the firing arbiter; SQL is a mirror.** The atomic counter/set/markers in
  KV give exactly-once firing selection portably; the SQL row provides durability,
  observability, and the deadline driver. The split keeps the hot path atomic and
  the cold path queryable.
- **Portable primitives, not Lua.** Firing rides on `IncrementIf`/`SetNX`/`SetAdd`,
  each implemented natively in Redis/Badger/JetStream. No backend-specific firing
  logic. (The Redis Lua scripts are one *instance* of a portable contract.)
- **Exactly-once is a platform guarantee, not handler glue.** Idempotent
  `create_task` (gateway `SetNX` ledger) is why you'd use the WFE instead of
  re-deriving a per-handler KV barrier — it removes the ~270-line, fail-open,
  unobservable `kb_update.py`-style workaround.
- **Correlation id is distinct from task_id.** A barrier's identity must survive
  task retries, can have no parent task (event-originated fan-out), and a task may
  belong to several barriers. `correlation_id` + a propagated `flow_id`
  (≈ `root_task_id`, threaded like the existing Authority Root/Parent lineage) is
  the model; the join's `correlation_key` composes them per nesting level — which
  is forward-compatible as the DAG run id.
- **Two feeds, one plane.** Joins gather over **explicit domain events** (feed A —
  how MemoryLayer emits `ingest_complete`) **and** over **task completions** (feed
  B — a task with an enabled `completion_event` emits a synthetic event onto
  `event::*` at its terminal status, §3.13). Both arrive as JSON `EventPayload`s
  on the same plane, so the Router and join matching treat them identically; no
  per-task topic subscription is involved.

---

## 8. Forward / deferred work

Implemented in v1: feed B (§3.13), and `correlation_id`/`root_task_id`
persistence + `TaskFilter` predicates (§3.13). Still deferred:

- **Automatic `flow_id` propagation on spawn.** Today a task is its own flow root
  when no `root_task_id` is supplied, and a fan-out rule stamps `correlation_id`
  explicitly; full reserved-namespace inheritance of `flow_id` across nested
  spawns is future work.
- **Scatter destination & DAG AND-join** — a `scatter` destination that mints a
  `flow_id` and creates N tasks in one step, and AND-join *nodes* keyed on
  `(flow_id, upstream_node_set)`. The v1 `correlation_id`/`flow_id` becomes the
  DAG run id with no rework.

---

## 9. Proto reference (additive)

- `KVOperation.OpType`: `SET_ADD = 12` (member = `value`, `ttl`; reply
  `counter_value` = cardinality, `applied` = newly-added), `SET_CARD = 13`.
- `WorkflowOperation.OpType`: `LIST_JOINS = 24`, `GET_JOIN = 25`,
  `CANCEL_JOIN = 26` (`id` = join_name, `secondary_id` = correlation_key).
- `CreateTaskRequest`: `idempotency_key = 16`, `correlation_id = 17`,
  `root_task_id = 18`, `completion_event = 19`. `TaskInfo`: `correlation_id = 32`,
  `root_task_id = 33`, `completion_event = 34`. `TaskFilter`:
  `correlation_id = 23`, `root_task_id = 24`.
- `TaskCompletionEvent { bool enabled; string event_name; repeated TaskStatus
  on_statuses }` — the feed-B opt-in carried on a task.

All additive (new enum values / optional fields); existing wire semantics
unchanged.

---

## 10. Source map

- Rules / routing: `internal/workflow/router.go`, `expr.go`, `templates.go`.
- Actions: `internal/workflow/executor.go` (`message`/`create_task`/`emit_event`).
- Joins: `internal/workflow/join.go` (engine), `store.go` (`Join` + CRUD),
  `scheduler.go` (deadline sweep), `workflow_handler.go` + `admin.go` (ops/REST).
- Migrations: `internal/workflow/migrations/004_workflow_joins.sql`,
  `migrations/sqlite_workflow/002_workflow_joins.sql`.
- KV primitives: `internal/kv/{store,badger_store,jetstream_store}.go`,
  `internal/gateway/interfaces.go` (`KVReadWriter`), `internal/gateway/kv_handler.go`.
- Idempotent create: `internal/gateway/orchestration_integration.go`.
- SDK: `sdk/go/aether/kv.go` (`SetAddSync`/`SetCardSync`), `coordkv.go`.
