# Aether Agentic-Fabric Protocol Guide

> **Status**: Ships with Phases 1-6 of the enterprise-agentic-fabric upgrade.
> This document is the unified, cross-phase reference. For deep-dives on
> individual subsystems, see:
> [`agent-acl-integration.md`](agent-acl-integration.md) (Phase 5 ACL attribution),
> [`extension-protocol.md`](extension-protocol.md) (Phase 6 negotiation),
> [`on-behalf-of-acl-design.md`](on-behalf-of-acl-design.md) (authority-grant
> primitives), [`specification.md`](specification.md) (the master spec), and
> [`aetherlite.md`](aetherlite.md) (single-binary mode).

---

## Table of Contents

1. [Overview](#1-overview)
2. [Mental model: the agentic task lifecycle](#2-mental-model-the-agentic-task-lifecycle)
3. [Phase 1 — Stateful waiting + spontaneous dependencies](#3-phase-1--stateful-waiting--spontaneous-dependencies)
4. [Phase 2 — Authority requests (the "sudo" flow)](#4-phase-2--authority-requests-the-sudo-flow)
5. [Phase 3 — Hibernation](#5-phase-3--hibernation)
6. [Phase 4 — Task management surface](#6-phase-4--task-management-surface)
7. [Phase 5 — Registry unification + ACL attribution](#7-phase-5--registry-unification--acl-attribution)
8. [Phase 6 — Extension protocol](#8-phase-6--extension-protocol)
9. [End-to-end walkthrough: a multi-phase agentic workflow](#9-end-to-end-walkthrough-a-multi-phase-agentic-workflow)
10. [Clarifications: confusion and overlap with prior state](#10-clarifications-confusion-and-overlap-with-prior-state)
11. [Migration notes](#11-migration-notes)
12. [Future work / what was deferred](#12-future-work--what-was-deferred)

---

## 1. Overview

### Why these changes exist

Aether's task subsystem has always handled fire-and-forget worker dispatch
well. What was missing — and what these six phases introduce — is first-class
support for the *agentic* workload: a long-running task that, mid-flight,
realises it needs a human, another agent, an authority grant, or a sibling
task it spawned. Before these changes the consumer's workaround was either
(a) block the worker in-process burning a slot, or (b) emulate the
handshake on the side and lie to the orchestrator about what was happening.
Phases 1-6 bring all four flavours of "I'm waiting on something" into the
protocol surface, give them durable storage, audited approval flows, and
worker-release semantics where appropriate.

### What "enterprise agentic fabric" means in this codebase

"Enterprise agentic fabric" denotes the union of the following capabilities
delivered as **Aether protocol surface** (proto + gateway + SDK), not as
per-consumer glue:

- Tasks may pause into a **typed waiting state** that the server understands
  (`waiting_input`, `waiting_authority`, `waiting_dependency`, `hibernated`).
- A pause can be **driven by another task** (spontaneous dependency edges).
- Pausing can **release the worker** (hibernation) and re-spawn a fresh one
  on wake, with checkpoint hand-off.
- Tasks can ask for a **scoped, time-limited authority grant ("sudo")** that
  another principal (human or automated approver) resolves; the resulting
  grant feeds the existing on-behalf-of audit chain.
- Any actor that created or owns a task can **list, get, and subscribe** to
  it and its descendants via a uniform management surface.
- Agents register their **owned resource families**; the audit log
  attributes every ACL decision back to the owning agent.
- The wire protocol carries a **versioned, URI-keyed extension** mechanism
  so customer extensions are first-class without forking the proto.

### Reading guide

| If you are... | Start with | Then read |
|---|---|---|
| An agent developer adding a "wait for human" flow | [§2 Mental model](#2-mental-model-the-agentic-task-lifecycle), [§3 Phase 1](#3-phase-1--stateful-waiting--spontaneous-dependencies) | [§4 Phase 2](#4-phase-2--authority-requests-the-sudo-flow) |
| An agent developer wanting hibernation | [§5 Phase 3](#5-phase-3--hibernation) | [§9 Walkthrough](#9-end-to-end-walkthrough-a-multi-phase-agentic-workflow) |
| A UI / dashboard developer | [§6 Phase 4](#6-phase-4--task-management-surface) | [§2 Mental model](#2-mental-model-the-agentic-task-lifecycle) |
| A platform/operator integrating a new agent type | [§7 Phase 5](#7-phase-5--registry-unification--acl-attribution), [`agent-acl-integration.md`](agent-acl-integration.md) | [§10 Clarifications](#10-clarifications-confusion-and-overlap-with-prior-state) |
| A protocol implementer / SDK author | [§8 Phase 6](#8-phase-6--extension-protocol), [`extension-protocol.md`](extension-protocol.md) | [§11 Migration](#11-migration-notes) |
| An operator debugging a stuck task | [§2 Mental model](#2-mental-model-the-agentic-task-lifecycle), [§10 Clarifications](#10-clarifications-confusion-and-overlap-with-prior-state) | [§11 Migration](#11-migration-notes) |

### Quick-start snippets (one per phase)

Each is a minimal, idiomatic Python SDK call. Real-world flows compose
several of these — see [§9](#9-end-to-end-walkthrough-a-multi-phase-agentic-workflow).

```python
# Phase 1 — declare a spontaneous dependency on a sibling task
await client.wait_for_task(
    task_id=my_task_id,
    depends_on=[child_task_id],
    wake_on_any=False,        # wait for all listed deps
    timeout_ms=600_000,       # 10 minutes max
)

# Phase 2 — ask for elevated authority via a capability gate
resp = await client.request_authority(
    desired_workspace_scope=["sensitive-data"],
    requested_access_level=aether_pb2.ACCESS_LEVEL_READ,
    requested_duration_seconds=600,
    routing_capability="capability/approve/sensitive_read",
    reason="LLM needs read access to compliance docs",
    task_id=my_task_id,
)

# Phase 3 — checkpoint, release the worker, wake at a wall-clock time
await client.checkpoint_save(key="job-state-v1", data=serialized_state)
await client.hibernate_until(
    task_id=my_task_id,
    checkpoint_key="job-state-v1",
    scheduled_wake_unix_ms=tomorrow_morning_unix_ms,
    escalation_policy="retry",
)

# Phase 4 — subscribe to a task and its descendants
sub_id = await client.subscribe_to_task(
    task_id=parent_task_id,
    recursive=True,
)

# Phase 5 — register an agent declaring the resource families it owns
await client.register_agent(
    implementation="chat-orchestrator",
    profile="k8s",
    resource_schema=[
        make_resource_schema_entry(
            resource_type_prefix="chat/",
            permission_verbs=["read", "write"],
        ),
    ],
)

# Phase 6 — declare an extension at connect time
await agent_client.connect(
    extensions=[
        make_extension(uri="https://example.com/aether-ext/chat-attachments/v1",
                       required=False),
    ],
)
```

---

## 2. Mental model: the agentic task lifecycle

### The unified state diagram

The Go-side `TaskStatus` enum (`server/pkg/tasks/types.go:11-27`) defines
ten lifecycle states. The transitions allowed between them are listed in
`validTransitions` (`server/pkg/tasks/types.go:397-458`) and gate-checked by
`ValidateTransition` on every state mutation.

```
                       +-------------+
                       |  REJECTED   |   (terminal; A2A semantic — declined)
                       +-------------+
                              ^ REJECT op
                              |
       CreateTask  +-----------+----------+   AssignTask    +------------+
   --------------> |        PENDING       | --------------> |  ASSIGNED  |
                   +----------------------+                 +------------+
                       |              |                          |
                       | RETRY        | CANCEL                   | StartingTask
                       v              v                          v
                  +----------+   +-----------+              +-----------+
                  | <retry>  |   | CANCELLED |              | STARTING  |
                  +----------+   +-----------+              +-----------+
                                                                  |
                                                                  | StartTask
                                                                  v
                                                            +-------------+
                                                            |   RUNNING   |<------+
                                                            +-------------+       |
                                                              |   |  |   |        |
                                                       PAUSE  |   |  |   |        | ResumeTask
                                                              v   |  |   v        |
            +------------------+  +-----------------+   +--------+----+    +-------+
            |  WAITING_INPUT   |  | WAITING_AUTHORITY|   | WAITING_DEP |    |       |
            +------------------+  +-----------------+   +-------------+    |       |
                  ^   |                  ^   |               ^   |         |       |
                  +---+ ResumeTask       +---+ ResumeTask    +---+         |       |
                  | RUN                  | RUN               | RUN/PEND    |       |
                  +---------- timeout ---+-----------------------+         |       |
                                       (fail)                              |       |
                                                                           |       |
                                                  +----------------+       |       |
                                                  |   HIBERNATED   | ------+-------+
                                                  +----------------+ Wake = re-queue
                                                          |          to PENDING
                                                          v (timeout)
                                                       FAILED        +------------+
                                                                     | COMPLETED  |
                                                                     +------------+
                                                                     | FAILED     |
                                                                     +------------+
                                                                     | DLQ        |
                                                                     +------------+
                                                                       (terminal)
```

### Three flavours of pause, plus one

A **paused** task is one whose `Status` is one of the `waitingStatuses`
(`server/pkg/tasks/types.go:378-383`). The four pause states encode
*why* the task is paused:

| State | `WaitReason` | What's owed | Worker retained? |
|---|---|---|---|
| `waiting_input` | `input` | a typed input message matching `WaitSpec.InputMatch` | **yes** |
| `waiting_authority` | `authority` | resolution of `WaitSpec.AuthorityRequestID` | **yes** |
| `waiting_dependency` | `dependency` | terminal transition of one or all `WaitSpec.DependsOn` tasks | **yes** |
| `hibernated` | `hibernation` | scheduled-wake / timeout firing | **no — worker is released** |

The first three are "park the cursor"; the fourth (`hibernated`) is "park
the cursor and free the compute". Wake on `hibernated` re-routes through
`pending` (`WakeHibernatedTask`, `task_assignment.go:833-916`) so the
orchestrator spawns a *fresh* worker carrying the saved
`checkpoint_key` / `resume_session_id` on its `TaskAssignment`.

### Wake paths

There are four ways a paused task can wake:

1. **Scheduled timer** — `WaitSpec.ScheduledWakeUnixMs` fires; the
   `task_waker` resumes the task at the next scan tick
   (`task_waker.go:200-224`).
2. **Dependency satisfied** — a child task reaches a terminal state and
   `TaskAssignmentService.wakeDependents` fans out to every waiting parent
   (`task_assignment.go:980-1022`), with the `task_waker` as the safety net
   (`task_waker.go:231-247`).
3. **Authority resolved** — the Phase 2 lifecycle service flips the
   `AuthorityRequest` row; the waker observes the new status on its next
   scan and either resumes (on `APPROVED`) or fails (on
   `DENIED`/`EXPIRED`/`CANCELLED`) the bound task
   (`task_waker.go:254-303`).
4. **Inbound input message** — *not yet automated server-side*. The waker
   handles timer / scheduled / dependency / authority wakes
   (`task_waker.go:32-48`); INPUT wake is documented as event-driven and is
   deferred (see [§12](#12-future-work--what-was-deferred)).

### Terminal states

The terminal set is enumerated in `terminalStatuses`
(`server/pkg/tasks/types.go:369-375`):

| State | Meaning |
|---|---|
| `completed` | finished successfully |
| `failed` | tried to process, broke; may be retried |
| `cancelled` | user/system aborted; may be retried |
| `dlq` | dead-letter — exhausted retries or poison |
| `rejected` | **agent declined before processing** (new in Phase 1; distinct from `failed`) |

The distinction between `rejected` and `failed` is intentional and is the
A2A-aligned semantic: "I refused this task" vs "I tried and broke".
`rejected` has **no outgoing transitions** — it's a permanent decision —
whereas `failed` and `cancelled` can re-enter `pending` for retry
(`validTransitions`, `server/pkg/tasks/types.go:451-457`).

### Where the background workers fit

Three goroutines sit alongside the request path and reconcile state on a
ticker:

| Worker | What it does | Skip rules |
|---|---|---|
| `disconnect_reaper` (`server/internal/orchestration/disconnect_reaper.go`) | Fails `running`/`assigned`/`starting` tasks whose worker disconnect exceeds the per-task grace window | **Phase 3:** skips any task where `IsWaiting(t.Status)` is true (`disconnect_reaper.go:83`) — waiting/hibernated workers can disconnect intentionally |
| `task_waker` (`server/internal/orchestration/task_waker.go`) | Reconciles every waiting task: dependency wakes, timer wakes, authority-request wakes, timeout-to-fail | Skips terminal rows; double-checks `IsWaiting` before acting (`task_waker.go:130-132`) |
| Authority-request expirer | Atomically flips PENDING authority-request rows whose `expires_at` is past; runs once per `task_waker` tick (`task_waker.go:107-116`, `authority_request_lifecycle.go:396-411`) | Bounded by `sweepLimit` (256 per tick, `task_waker.go:60-61`) |

The two scanners are siblings: identical lifecycle shape (ticker loop,
cancelled via `Run(ctx)`'s context, per-row errors logged and skipped so
one bad row never aborts the scan).

### The two-level hierarchy

When designing or debugging an agentic flow, two questions sit on top of
each other:

1. **Per-phase**: which `TaskOperation.OpType` did the agent invoke, and
   which target `TaskStatus` did the gateway transition to? This is the
   wire-level decision.
2. **Overall**: where in the lifecycle did the task land, and which
   background worker (if any) will move it next?

Throughout this guide we hold both views simultaneously: §3-§8 cover the
per-phase mechanics, §2 above is the overall view, and §9 ties them
together through a worked example.

---

## 3. Phase 1 — Stateful waiting + spontaneous dependencies

### Wire-level state additions

The proto `TaskStatus` enum (`api/proto/aether.proto:380-392`) gained five
new values; the Go-side `TaskStatus` const block
(`server/pkg/tasks/types.go:22-27`) tracks them as string literals:

| Proto enum | Go const | String value |
|---|---|---|
| `TASK_STATUS_WAITING_INPUT` (6) | `TaskStatusWaitingInput` | `"waiting_input"` |
| `TASK_STATUS_WAITING_AUTHORITY` (7) | `TaskStatusWaitingAuthority` | `"waiting_authority"` |
| `TASK_STATUS_WAITING_DEPENDENCY` (8) | `TaskStatusWaitingDependency` | `"waiting_dependency"` |
| `TASK_STATUS_HIBERNATED` (9) | `TaskStatusHibernated` | `"hibernated"` |
| `TASK_STATUS_REJECTED` (10) | `TaskStatusRejected` | `"rejected"` |

The proto enum collapses `pending`/`assigned`/`starting` into a single
wire-level `TASK_STATUS_QUEUED` (see `taskStatusToProto` in
`server/internal/orchestration/task_event_publisher.go:37-62`); the new
waiting/hibernated/rejected values are *not* collapsed because clients
need to react to them directly.

### `WaitSpec`

`WaitSpec` is the universal "why am I paused, and what will wake me"
descriptor. The proto shape is in `api/proto/aether.proto:1157-1189`; the
Go struct is in `server/pkg/tasks/types.go:63-73`.

```go
// server/pkg/tasks/types.go:63-73
type WaitSpec struct {
    Reason              WaitReason             `json:"reason,omitempty"`
    ExpectedPrincipal   string                 `json:"expected_principal,omitempty"`
    InputMatch          map[string]string      `json:"input_match,omitempty"`
    AuthorityRequestID  string                 `json:"authority_request_id,omitempty"`
    DependsOn           []string               `json:"depends_on,omitempty"`
    WakeOnAny           bool                   `json:"wake_on_any,omitempty"`
    TimeoutMs           int64                  `json:"timeout_ms,omitempty"`
    ScheduledWakeUnixMs int64                  `json:"scheduled_wake_unix_ms,omitempty"`
    Hibernation         *HibernationDescriptor `json:"hibernation,omitempty"`
}
```

| Field | Meaningful when `Reason ==` | Semantics |
|---|---|---|
| `Reason` | always | Discriminator; pairs 1:1 with the target `TaskStatus`. The pairing is documented in `api/proto/aether.proto:1138-1148`. |
| `ExpectedPrincipal` | `input` | Canonical principal expected to send the wake message. Empty = any principal in the workspace. |
| `InputMatch` | `input` | Key/value pairs the inbound message metadata must match. |
| `AuthorityRequestID` | `authority` | Correlation id of the Phase 2 authority request being awaited; populated by the Phase 2 flow. |
| `DependsOn` | `dependency` | Task IDs that must terminate before wake. Mirrored onto `Task.DependsOn` for shortcut queries (`api/proto/aether.proto:1064-1067`). |
| `WakeOnAny` | `dependency` | False (default) = wait for all; true = wake on first. |
| `TimeoutMs` | any | Hard upper bound on time in this waiting state. On expiry the waker forcibly fails the task (`task_waker.go:145-189`) unless overridden by hibernation escalation policy. |
| `ScheduledWakeUnixMs` | any | Absolute wall-clock wake time. Independent of `TimeoutMs`. |
| `Hibernation` | `hibernation` | Nested descriptor — see [§5](#5-phase-3--hibernation). |

`WaitSpec` is stored as JSONB (Postgres) / JSON text (SQLite) so existing
rows without the field deserialise as `nil` and new sub-objects can be
added without migrations. The doc comment on
`server/pkg/tasks/types.go:53-62` explains the schema-evolution contract.

### New `TaskOperation` ops

`TaskOperation.OpType` (`api/proto/aether.proto:1108-1119`) added four
operations:

| Op | Wire enum | Effect | Notes |
|---|---|---|---|
| `PAUSE` | 4 | `running → waiting_*` according to `wait_spec.reason` | `wait_spec` required |
| `WAIT_FOR` | 5 | `running → waiting_dependency` | Specialisation of `PAUSE`; convenience wrapper. `wait_spec.reason` is forced to `DEPENDENCY` |
| `RESUME` | 6 | `waiting_* → running` (or `pending` for hibernated; see [§5](#5-phase-3--hibernation)) | Force-wake; normally the `task_waker` triggers this automatically |
| `REJECT` | 7 | `* → rejected` | Terminal. Side-effects mirror `CancelTask`: token revoke, task-grant revoke, queue retirement, dependency-wake fan-out (`task_assignment.go:943-969`) |

When to use which:

- **`PAUSE`** — anytime an agent realises it must wait for something
  (input, authority, dependency, hibernation). The shape of `WaitSpec`
  determines what the waker watches for.
- **`WAIT_FOR`** — only when waiting on sibling/child tasks. Strictly a
  convenience wrapper; semantically identical to `PAUSE` with
  `wait_spec.reason = DEPENDENCY`.
- **`RESUME`** — admin/manual unblocking. The vast majority of resumes
  come from the `task_waker` autonomously.
- **`REJECT`** — first response to an unprocessable task. Distinct from
  `FAIL` (which means "I tried and broke"). `rejected` is terminal with no
  outgoing transitions.

### The resume contract (A2A-aligned)

Resume of a paused task **preserves task identity**: same `task_id`, new
message. The Go-side `ResumeTask`
(`server/internal/orchestration/task_assignment.go:797-808`) clears the
`WaitSpec` and transitions to a target status that the caller passes in
(typically `running`). No new task is created.

For hibernation specifically, the wake path differs — see
[§5](#5-phase-3--hibernation).

### `ContextID` — grouping multi-task interactions

The A2A `contextId` lands on `Task.ContextID`
(`server/pkg/tasks/types.go:210-211`), the wire field
`CreateTaskRequest.context_id` (`api/proto/aether.proto:613-616`), and
the projection `TaskInfo.context_id`
(`api/proto/aether.proto:1068-1071`). It is also a filter dimension on
`TaskFilter.context_id` (`api/proto/aether.proto:991-993`).

Aether treats `context_id` as **opaque**: any client-minted identifier
that the caller wants to use to bundle related tasks across turns of a
conversation, retries of a job, or sub-agent fan-out. The server stores
it, filters by it, and never interprets it. It is independent of and
orthogonal to `workspace` (which scopes resources and ACL).

### The `task_waker`

`server/internal/orchestration/task_waker.go` is the periodic scan that
keeps waiting tasks moving. It's a sibling of the
`disconnect_reaper.go` — same construction shape (`NewTaskWaker` →
`Run(ctx)`), same ticker pattern (10 second interval by default,
`task_waker.go:75`), same per-row error tolerance.

What it does per scan (`task_waker.go:98-305`):

1. Sweep expired authority requests (lifecycle service; non-blocking, best-effort).
2. List up to 500 waiting tasks (`batchLimit`, `task_waker.go:76`).
3. Per task, in priority order:
   - **Timeout wake-to-fail** — if `TimeoutMs > 0` and `now - PausedAt >= TimeoutMs`, fail the task (or apply the hibernation escalation policy, see [§5](#5-phase-3--hibernation)).
   - **Scheduled wake** — if `ScheduledWakeUnixMs` has passed: resume (or `WakeHibernatedTask` for hibernated rows).
   - **Dependency reconciliation** — for `waiting_dependency`, re-evaluate `WaitSpec.DependsOn` via `shouldWakeParent` (`task_assignment.go:1029-1065`). The event-driven `wakeDependents` path normally catches this first; the waker covers races.
   - **Authority reconciliation** — for `waiting_authority`, look up the bound `AuthorityRequest` row and resume (on `APPROVED`) or fail (on `DENIED`/`EXPIRED`/`CANCELLED`).

INPUT wake is intentionally absent from the waker — see [§12](#12-future-work--what-was-deferred) on event-driven INPUT wake.

### SDK helpers

The Python SDK exposes these on `BaseAetherClient` /
`BaseAsyncAetherClient`. Public helpers in `client.py` are reused by both
the sync and async variants:

| Helper | Defined in | Wire op |
|---|---|---|
| `make_wait_spec(...)` | `client.py:46-95` | builder for `aether_pb2.WaitSpec` |
| `pause_task(task_id, wait_spec, reason, ...)` | `client_async.py:2083-2122` | `TaskOperation.PAUSE` |
| `wait_for_task(task_id, depends_on, wake_on_any=False, ...)` | `client_async.py:2124-2174` | `TaskOperation.WAIT_FOR` |
| `resume_task(task_id, ...)` | `client_async.py:2176-2200` | `TaskOperation.RESUME` |
| `reject_task(task_id, reason, ...)` | `client_async.py:2202-2228` | `TaskOperation.REJECT` |

### Mini-example: dependency wait

```python
# Agent A creates a child task and waits on it.
child = await client.create_task(
    task_type="sub-computation",
    workspace="my-workspace",
    assignment_mode="pool",
    target_implementation="compute-worker",
    context_id=my_context_id,
)

await client.wait_for_task(
    task_id=parent_task_id,
    depends_on=[child.task_id],
    wake_on_any=False,            # wait for child to terminate
    timeout_ms=900_000,           # 15 min hard ceiling
    reason="awaiting sub-computation result",
)
# parent_task_id is now WAITING_DEPENDENCY with WaitSpec.DependsOn=[child.task_id]
```

**What happens server-side**:

1. Gateway routes the upstream `TaskOperation{WAIT_FOR}` through
   `handleTaskOp` (`routing.go` around line 1850-1937).
2. `authorizeTaskOp` validates the caller may act on `parent_task_id`
   (creator-match, assignee-match, or workspace `Manage`).
3. `taskStore.PauseTask` performs the validated transition
   `running → waiting_dependency`, persists `WaitSpec`, stamps `PausedAt`.
4. `publishStatusChange` emits a `TaskStatusChangedEvent` on the
   `tk::{workspace}::{task_id}::events` topic for any Phase 4 subscribers.
5. When `child` reaches a terminal status, `TaskAssignmentService.wakeDependents`
   (`task_assignment.go:980-1022`) scans `ListTasksWaitingOnDependency(child.task_id)`,
   evaluates `shouldWakeParent` (`task_assignment.go:1029-1065`), and on
   wake calls `ResumeTask(parent, running)` — which clears `WaitSpec`
   and re-emits a status-changed event.
6. If the child's terminal transition somehow happened before the parent
   reached `waiting_dependency` (race), the next `task_waker` tick
   catches it via `taskService.shouldWakeParent`.

---

## 4. Phase 2 — Authority requests (the "sudo" flow)

### Why a separate flow rather than direct grant minting

Aether already had a sophisticated authority-grant primitive
(`server/internal/acl/authority_grants.go`,
`authority_context.go`) — static rules + delegated grants, scope
intersection, audience binding, parent chains, expiry +
renewable-until, all wired through `comprehensive_audit_log`. What was
missing was the **request/approve handshake**: a way for an agent to
*ask* for a grant and park until someone resolves it. Every primitive
needed to mint a grant existed; the gap was the lifecycle row.

`AuthorityRequest` fills that gap. It is the typed handshake; the
**approval terminates in `CreateAuthorityGrant`** — the existing path —
so the audit chain works for free and no parallel grant type exists.

### Lifecycle states

`AuthorityRequestStatus` (proto: `api/proto/aether.proto:2006-2013`,
Go: `server/internal/acl/authority_request_types.go:31-46`):

| State | When |
|---|---|
| `pending` | Created; awaiting approver action |
| `approved` | Resolved approved; a corresponding `AuthorityGrant` has been minted (`GrantedGrantID`) |
| `denied` | Resolved denied; `ResolutionReason` populated |
| `expired` | Not resolved before `ExpiresAt`; `SweepExpiredAuthorityRequests` flipped it (`authority_request_lifecycle.go:388-411`) |
| `cancelled` | Requester withdrew it |

Lifecycle transitions (`AuthorityRequestStatus.IsTerminal` —
`server/internal/acl/authority_request_types.go:48-60`):

```
                  +-----------+
                  |  PENDING  |
                  +-----------+
                    |    |     |    |
              APPROVE   DENY  EXPIRE  CANCEL
                  |    |     |    |
        +-----------+ +--------+ +--------+ +-----------+
        | APPROVED  | | DENIED | | EXPIRED| | CANCELLED |
        +-----------+ +--------+ +--------+ +-----------+
              (terminal — no further transitions)
```

### Routing: principal-targeted vs capability-gate

Each `AuthorityRequest` carries an `AuthorityRequestRoutingTarget` (proto:
`api/proto/aether.proto:2017-2024`, Go:
`server/internal/acl/authority_request_types.go:69-86`):

- **Principal target** (`AuthorityRequestRoutingTarget.Principal`) — a
  specific approver identity (user, role, group). Only that bound
  principal may resolve. Examples: `user::alice`, `role::ops-on-call`.
- **Capability target** (`AuthorityRequestRoutingTarget.Capability`) — a
  capability-gate string like `"capability/approve/<action>"`. Any actor
  whose ACL `CheckAccess` against the gate succeeds may resolve.

Exactly one of the two must be populated; the storage layer rejects an
empty routing target on insert
(`server/internal/acl/authority_requests.go:33-34`,
`authority_request_lifecycle.go:138-146`).

### The five op types

`AuthorityRequestOperation.OpType` (`api/proto/aether.proto:2125-2132`):

| Op | Payload | Purpose |
|---|---|---|
| `CREATE` | `CreateAuthorityRequestPayload` | Submit a new request; server assigns `request_id` |
| `GET` | (`request_id` only) | Fetch a single request by id |
| `LIST_PENDING` | `AuthorityRequestListFilter` | List pending requests visible to the caller (matches resolver principal/capabilities) |
| `RESOLVE` | `ResolveAuthorityRequestPayload` | Approver decides: approve (mints grant) or deny |
| `CANCEL` | (`request_id` + `reason`) | Requester withdraws |

### Approver scope refinement (intersection-only)

At `RESOLVE` time the approver may supply a *narrower* scope via
`ResolveAuthorityRequestPayload.granted_*` fields
(`api/proto/aether.proto:2090-2108`). The lifecycle service intersects
the requested and granted sets — broadening is silently dropped:

| Refinement | Effect |
|---|---|
| `granted_workspace_scope` | Intersected with `req.WorkspaceScope` (`authority_request_lifecycle.go:424-448`) |
| `granted_resource_scope` | Per-key intersection on inner slices; keys missing from the granted map are dropped (`authority_request_lifecycle.go:458-474`) |
| `granted_operation_scope` | Intersected with `req.OperationScope` |
| `granted_access_level` | `min(requested, granted)` with 0=inherit semantics (`authority_request_lifecycle.go:242-249`) |
| `granted_duration_seconds` | `min(requested, granted, MaxAuthorityRequestDurationSeconds)` (`authority_request_lifecycle.go:251-258`) |

`MaxAuthorityRequestDurationSeconds = 3600` (1 hour) is the
policy ceiling on any minted grant
(`authority_request_lifecycle.go:40-46`).
`DefaultAuthorityRequestTimeoutSeconds = 1800` (30 minutes) is the
default for how long a `PENDING` row may sit before the sweeper flips
it to `EXPIRED` (`authority_request_lifecycle.go:48-53`).

### How a task ties into a request

When an agent calls `request_authority(..., task_id=my_task_id)`:

1. Submit creates the request row with `task_id = my_task_id`.
2. The agent's SDK helper (or its own logic) transitions the task to
   `waiting_authority` via a `PAUSE` op with
   `WaitSpec.Reason=authority` and
   `WaitSpec.AuthorityRequestID=<new request id>`.
3. The `task_waker` reconciles `waiting_authority` rows against the
   bound request: on `APPROVED` it resumes the task to `running`; on
   `DENIED`/`EXPIRED`/`CANCELLED` it fails the task with a structured
   reason (`task_waker.go:254-303`, `authorityRequestFailureReason`
   `task_waker.go:311-319`).

### Audit

Every lifecycle event is an `EventTypeAuthorityRequest` audit row.
The lifecycle service emits via `s.audit.LogAuthorityRequestEvent`
(`authority_request_lifecycle.go:183`, `:327`, `:355`, `:384`, `:408`)
with `operation` set to one of the constants:

```go
// server/internal/acl/authority_request_lifecycle.go:59-63
OpAuthorityRequestCreated   = "authority_request_created"
OpAuthorityRequestApproved  = "authority_request_approved"
OpAuthorityRequestDenied    = "authority_request_denied"
OpAuthorityRequestExpired   = "authority_request_expired"
OpAuthorityRequestCancelled = "authority_request_cancelled"
```

The audit row records the actor (requester for CREATE/CANCEL, approver
for APPROVED/DENIED), the request id, and on APPROVED includes the
`granted_grant_id` in metadata so the audit chain links to the minted
`AuthorityGrant`.

### SDK helpers

| Helper | Defined in | Wire op |
|---|---|---|
| `make_authority_request_routing(principal=, capability=)` | `client.py:151-190` | `AuthorityRequestRoutingTarget` builder |
| `make_authority_request_resource_scope_entry(rt, patterns)` | `client.py:193-203` | `AuthorityRequestResourceScopeEntry` builder |
| `request_authority(...)` | `client_async.py:2234-2285` | `AuthorityRequestOperation{CREATE}` |
| `list_pending_authority_requests(workspace, matching_capabilities, ...)` | `client_async.py:2287-2312` | `AuthorityRequestOperation{LIST_PENDING}` |
| `resolve_authority_request(request_id, approve, granted_*, ...)` | `client_async.py:2314-2355` | `AuthorityRequestOperation{RESOLVE}` |
| `cancel_authority_request(request_id, reason)` | `client_async.py:2357-2373` | `AuthorityRequestOperation{CANCEL}` |

### Mini-example: step-up access via capability gate

```python
# Agent submits an authority request, parking until the policy-bot approves.
req_resp = await chat_agent.request_authority(
    desired_workspace_scope=["compliance-vault"],
    desired_operation_scope=["read"],
    requested_access_level=aether_pb2.ACCESS_LEVEL_READ,
    requested_duration_seconds=600,
    routing_capability="capability/approve/compliance_read",
    reason="user asked for a summary of the Q4 audit memo",
    task_id=my_task_id,
)
request_id = req_resp.request.request_id

# Then park the task on the request:
await chat_agent.pause_task(
    task_id=my_task_id,
    wait_spec=make_wait_spec(
        reason=aether_pb2.WAIT_REASON_AUTHORITY,
        authority_request_id=request_id,
        timeout_ms=20 * 60 * 1000,  # 20-minute ceiling on the wait
    ),
)

# Elsewhere — a deterministic policy-bot processes the request:
policy_bot_pending = await policy_bot.list_pending_authority_requests(
    matching_capabilities=["capability/approve/compliance_read"],
)
for r in policy_bot_pending.requests:
    if is_allowed_by_policy(r):
        await policy_bot.resolve_authority_request(
            request_id=r.request_id,
            approve=True,
            granted_duration_seconds=300,   # narrower than requested
            reason="policy: read-only compliance summaries auto-approved",
        )

# task_waker observes the APPROVED status on its next scan and resumes
# my_task_id to running. The minted grant is in
# AuthorityRequest.granted_grant_id and is fully visible in the audit log.
```

---

## 5. Phase 3 — Hibernation

### The contract

Hibernation is the "fourth waiting state": same `WaitSpec` shape, but the
worker is **released**. Re-wake spawns a *fresh* worker carrying the
saved checkpoint key and (optionally) a resume session id on its
`TaskAssignment`.

The full contract is documented in the proto comment on
`api/proto/aether.proto:1191-1210` and the Go-side
`HibernationDescriptor` struct doc (`server/pkg/tasks/types.go:39-47`).

### `HibernationDescriptor` fields

```go
// server/pkg/tasks/types.go:42-47
type HibernationDescriptor struct {
    CheckpointKey    string   `json:"checkpoint_key,omitempty"`
    ResumeSessionID  string   `json:"resume_session_id,omitempty"`
    WakeEventTypes   []string `json:"wake_event_types,omitempty"`
    EscalationPolicy string   `json:"escalation_policy,omitempty"`
}
```

| Field | Required? | Effect |
|---|---|---|
| `CheckpointKey` | **yes** | The checkpoint key the worker `SAVE`d before requesting hibernation. The gateway validates this checkpoint exists before allowing the `HIBERNATE` transition (Stage A validator). On wake, the next `TaskAssignment` carries this key in `TaskAssignment.checkpoint_key` so the spawned worker can `LOAD` it (`api/proto/aether.proto:680-683`). |
| `ResumeSessionID` | no | If set, the new worker should resume that session rather than create a fresh one. Threaded onto `TaskAssignment.resume_session_id` (`api/proto/aether.proto:685-686`). |
| `WakeEventTypes` | no | Reserved for future use; the current waker honours `scheduled_wake_unix_ms` / `timeout_ms` already. |
| `EscalationPolicy` | no | How the waker handles a timeout: `""` / `"fail"` (default), `"retry"`, `"alert"`. See below. |

### The precondition: checkpoint must exist

Stage A enforces — at the gateway, before allowing the HIBERNATE
transition — that the named checkpoint exists. Without this guarantee a
wake would have nothing to load. Validation lives alongside the
gateway-side `PAUSE` handler; the integration test that exercises this
explicitly is `internal/gateway/hibernation_precondition_test.go`.

### Hand-off mechanics

`WakeHibernatedTask` is the canonical wake path
(`server/internal/orchestration/task_assignment.go:833-916`). Step by step:

1. **Reload the task** — if it's not still `hibernated` (race against a
   concurrent waker / admin op), log + no-op.
2. **Merge hibernation hand-off into metadata** — before clearing the
   `WaitSpec`, copy `Hibernation.CheckpointKey` to
   `Task.Metadata[MetadataKeyHibernationCheckpointKey]` and
   `Hibernation.ResumeSessionID` to
   `Task.Metadata[MetadataKeyHibernationResumeSessionID]`. The reserved
   keys are defined in `server/pkg/tasks/types.go:79-82`:
   ```go
   MetadataKeyHibernationCheckpointKey   = "_hibernation_checkpoint_key"
   MetadataKeyHibernationResumeSessionID = "_hibernation_resume_session_id"
   ```
   The "_" prefix denotes server-managed metadata (`server/pkg/tasks/types.go:58-62`).
3. **`ResumeTask(task, pending)`** — flip `hibernated → pending` and
   clear `WaitSpec`. Note: this is `pending`, not `running`, because the
   worker has been released and a fresh one must be spawned.
4. **Re-insert into `orchestrated_task_queue`** — using the existing
   `InsertQueueEntry` path (the same one `createOrchestratedStartupTask`
   uses). The orchestrator picks up the row via its usual NOTIFY / poll
   loop and produces a new `TaskAssignment`.
5. The orchestration delivery path reads the reserved metadata keys and
   populates `TaskAssignment.checkpoint_key` /
   `TaskAssignment.resume_session_id` so the fresh worker rehydrates.

When the task transitions to `hibernated`, the gateway also emits a
`TaskHibernated` downstream message (`api/proto/aether.proto:105-114`)
to the assigned worker. Workers SHOULD close their gRPC stream cleanly
after receiving it; the `disconnect_reaper` skips `hibernated` tasks so
the disconnect is non-fatal.

### Escalation policies

When `WaitSpec.TimeoutMs` fires on a `hibernated` task, the waker
honours `HibernationDescriptor.EscalationPolicy`
(`task_waker.go:148-176`):

| Policy | Behaviour |
|---|---|
| `""` / `"fail"` (default) | `FailTask(taskID, "wait timeout")` |
| `"retry"` | Re-route through `WakeHibernatedTask` so the task is re-queued for a fresh worker |
| `"alert"` | Stay hibernated; emit a warning log line. No external alerting integration in Stage B — the log line is the entry point. |

### Disconnect-reaper guard

The disconnect reaper iterates all `running`/`starting`/`assigned`
tasks with a stamped `DisconnectedAt`, but explicitly skips any task
whose status is in the `waitingStatuses` set
(`server/internal/orchestration/disconnect_reaper.go:83`). This is
the Phase 3 hook that lets hibernation release the worker without the
reaper failing the row.

### SDK helpers

| Helper | Defined in | Behavior |
|---|---|---|
| `make_hibernation_descriptor(checkpoint_key, resume_session_id, wake_event_types, escalation_policy)` | `client.py:118-148` | Builder; rejects empty `checkpoint_key` |
| `hibernate_until(task_id, checkpoint_key, scheduled_wake_unix_ms, timeout_ms, ...)` | `client_async.py:2379-2418` | Composes a `WaitSpec{HIBERNATION}` and submits a `TaskOperation{PAUSE}` |

`hibernate_until` is intentionally a thin wrapper over `pause_task` with
`reason=HIBERNATION`. It refuses an empty `checkpoint_key`
(`client_async.py:2392-2393`).

### Mini-example: save → hibernate → wake

```python
# Agent serializes its in-progress state and parks for the night.
state_bytes = msgpack.packb(my_in_progress_state)
await client.checkpoint_save(key="ingest-batch-3-v1", data=state_bytes)

await client.hibernate_until(
    task_id=my_task_id,
    checkpoint_key="ingest-batch-3-v1",
    scheduled_wake_unix_ms=tomorrow_8am_unix_ms,
    timeout_ms=18 * 3600 * 1000,    # 18 hour ceiling
    escalation_policy="retry",      # if we miss the wake, re-queue
)
# Task is now HIBERNATED with the checkpoint key. Worker disconnects.

# === Eight hours later ===
# task_waker observes ScheduledWakeUnixMs is in the past, calls
# WakeHibernatedTask → metadata gets the checkpoint key, status goes
# hibernated → pending, queue entry is inserted.
# Orchestrator spawns a fresh worker. On TaskAssignment delivery:
#   - assignment.checkpoint_key = "ingest-batch-3-v1"
#   - assignment.resume_session_id = ""
# The new worker LOADs the checkpoint and continues from saved state.
```

---

## 6. Phase 4 — Task management surface

### Idiomatic Aether: bidi-stream, not unary RPCs

The Aether external API surface is a single bidi-stream
`Connect (stream UpstreamMessage) returns (stream DownstreamMessage)`
(`api/proto/aether.proto:7-12`). All operations are oneof variants
inside `UpstreamMessage` / `DownstreamMessage`
(`api/proto/aether.proto:14-103`). Adding named unary RPCs for task
management would be out-of-pattern; Phase 4 keeps the management
surface idiomatic by shipping new payloads inside the existing oneofs.

### `TaskQuery` filter extensions

`TaskFilter` (`api/proto/aether.proto:967-1020`) added the following
fields in Phase 4. The Go-side mirror is in
`server/pkg/tasks/types.go:340-362`.

| Field | Wire | Effect |
|---|---|---|
| `creator_actor` | `PrincipalRef` (`proto:1006`) | Filter by the actor that created the task. `principal_id` is matched against the task's stored `parent_agent_id` column (Phase 5 audit notes the storage detail). `principal_type` is informational. |
| `status_timestamp_after_unix_ms` | `int64` (`proto:1009`) | Returns tasks whose most recent status transition (`updated_at`) is at or after this unix-ms timestamp. 0 = no filter. |
| `page_token` | `string` (`proto:1015`) | Cursor-based pagination. Takes priority over `Limit`/`Offset` when supplied. |
| `include_descendants` | `bool` (`proto:1019`) | When true and `parent_task_id` is set, recursively walks the full subtree (not just direct children). Default false preserves the existing single-level behaviour. |

Phase 1 also added:

| Field | Wire | Effect |
|---|---|---|
| `context_id` | `string` (`proto:993`) | Filter by client-minted session identifier |
| `exclude_statuses` | `repeated TaskStatus` (`proto:997`) | Inverted status filter; combine with `status`/`statuses` |

### Cursor pagination

Cursors are opaque to clients. Internally they encode a `(updated_at,
task_id)` tuple ordered DESCENDING. The encoding format is documented in
the Go-side `TaskFilter.PageToken` doc comment
(`server/pkg/tasks/types.go:352-356`):

> The cursor format is base64url("\<unix_micros\>|\<task_id\>") and
> orders by (updated_at DESC, task_id DESC).

The server returns `TaskQueryResponse.next_page_token`
(`api/proto/aether.proto:1097-1100`) when more rows may exist; pass it
back as `TaskFilter.page_token` to fetch the next page. An empty
`next_page_token` indicates the last page has been served.

The recursive descendant walk is bounded at **10,000 rows** to keep
memory and latency predictable. The `ListTasksPage` interface on
`taskstore.Store` is the entry point.

### Authorization model

`authorizeTaskOp` (`server/internal/gateway/routing.go:1451-1466`) is
the helper invoked on every Phase 4 task lookup / op. The check is a
cheap-first chain:

1. **Creator match** — the calling session's identity matches the task's
   stored creator identity.
2. **Assignee match** — the calling session is the task's assignee.
3. **Workspace `Manage`** — the caller holds `AccessManage` on the
   task's workspace.

ACL lookup failures convert to "deny" — fail-closed for a missing ACL
surface.

For `TaskQuery.LIST` specifically, the gateway scopes results to
workspaces the caller can read (the existing pre-Phase-4 behaviour);
recursive descendant walks and `creator_actor` filters compose on top
of that scope.

### Info-hiding: "task not found"

Both "task does not exist" and "you are not authorized for this task"
return the identical error string `ErrTaskNotFoundOrUnauthorized = "task
not found"` (`server/internal/gateway/routing.go:1444-1449`). This is
the A2A info-hiding principle: auth-fail must be indistinguishable from
not-found, otherwise the error code itself becomes a resource-enumeration
oracle.

Audit logs still record the underlying decision (workspace-mismatch,
permission denied, etc.) — the info-hiding is purely on the wire.

### `TaskSubscription` primitive

`TaskSubscriptionOperation` (`api/proto/aether.proto:2842-2863`)
introduces two ops:

- `SUBSCRIBE` — start streaming `TaskEvent`s for `task_id`. Returns a
  `subscription_id` on `TaskSubscriptionOperationResponse`
  (`api/proto/aether.proto:2865-2874`).
- `UNSUBSCRIBE` — stop streaming. The server-issued `subscription_id`
  is the canonical reference; an empty id falls back to `(task_id,
  recursive)` matching.

Optional fields on SUBSCRIBE:

- `recursive` (`bool`) — when true, also stream events for descendant
  tasks. **Snapshot-at-subscribe**: descendants known at subscribe time
  are tracked; tasks born *after* subscribe are NOT picked up
  automatically (proto comment, `api/proto/aether.proto:2851-2853`).
- `start_timestamp_unix_ms` — cold-start cursor; empty / 0 = live-only.

### `TaskEvent` variants

`TaskEvent` (`api/proto/aether.proto:2876-2892`) has a oneof body:

| Variant | Fires on |
|---|---|
| `status_changed: TaskStatusChangedEvent` | Every lifecycle transition (emitted from `publishStatusChange` in `task_event_publisher.go:64-89`) |
| `progress: TaskProgressEvent` | `ProgressUpdate` whose `task_id` matches the subscription |
| `child_lifecycle: TaskChildLifecycleEvent` | A direct child's lifecycle transition (relayed by `publishChildLifecycle`, `task_event_publisher.go:94-114`) |
| `authority_request: TaskAuthorityRequestEventRelay` | A Phase 2 `AuthorityRequestEvent` whose `task_id` matches the subscription |

`TaskEvent` envelope fields include `task_id`, `emitted_at_unix_ms`,
`workspace`, `parent_task_id`, and `subscription_id`. The gateway
stamps `subscription_id` on send so clients with multiple overlapping
subscriptions can demultiplex.

### Topic taxonomy

The per-task event topic is `tk::{workspace}::{task_id}::events`. The
gateway publishes via the `TaskEventPublisher` interface
(`server/internal/orchestration/task_event_publisher.go:21-26`); a
nil publisher is treated as no-op so test harnesses and aetherlite
single-binary mode work unchanged.

This is **distinct from** the legacy workspace-wide progress topic
`pg::{workspace}`, which fans out `ProgressUpdate`s. The new
`tk::{workspace}::{task_id}::events` is per-task and carries the full
lifecycle / progress / child / authority axis.

### SDK helpers

| Helper | Defined in |
|---|---|
| `query_tasks(workspace, status, statuses, context_id, parent_task_id, include_descendants, creator_actor_id, page_token, ...)` | `client.py:1524` (sync) / `client_async.py:1791` (async) |
| `subscribe_to_task(task_id, recursive, start_timestamp_unix_ms, ...)` | `client.py:1618` / `client_async.py:1885` |
| `unsubscribe_from_task(subscription_id, ...)` | `client.py:1662` / `client_async.py:1929` |
| `register_task_event_handler(handler)` | `client.py:1600` |

Subscriptions are auto-cleaned on session disconnect — the
`task_subscription_handler` (`internal/gateway/task_subscription_handler.go`)
tracks per-session subscriptions and reaps them when the client stream
closes.

### Mini-example: subscribe recursive, observe child fan-out

```python
# UI dashboard subscribes to a parent task and all known descendants.
def on_event(evt: aether_pb2.TaskEvent):
    if evt.HasField("status_changed"):
        print(f"{evt.task_id}: {evt.status_changed.from_status} -> {evt.status_changed.to_status}")
    elif evt.HasField("child_lifecycle"):
        cl = evt.child_lifecycle
        print(f"child {cl.child_task_id}: {cl.lifecycle} -> {cl.child_status}")

client.register_task_event_handler(on_event)
sub_id = await client.subscribe_to_task(task_id=parent_task_id, recursive=True)

# When parent_task_id spawns child_a and child_b, then child_a completes,
# the dashboard sees:
#   parent: QUEUED -> RUNNING
#   child_a: QUEUED -> RUNNING
#   child_b: QUEUED -> RUNNING
#   child_a: RUNNING -> COMPLETED  (status_changed on child's own stream)
#   parent: child_lifecycle{ child_a, COMPLETED, "completed" }  (relayed)
```

---

## 7. Phase 5 — Registry unification + ACL attribution

> This section is the cross-phase summary. The deep-dive lives in
> [`agent-acl-integration.md`](agent-acl-integration.md). Read this for
> the protocol overview; read the deep-dive for the audit-log shape,
> migration mechanics, and the multi-gateway caveats in full.

### Three orthogonal concerns on one registration

Phase 5 unifies what used to be two separate stories — orchestration
("how do I spawn this agent?") and authorization ("what does this agent
own?") — into a single extended `AgentRegistration`. The proto is at
`api/proto/aether.proto:1404-1429`; the Go-side type extension lives in
`server/internal/registry/agent_registry.go`.

The new fields:

| Field | Proto | Purpose |
|---|---|---|
| `ResourceSchema` (`repeated AgentResourceSchemaEntry`) | `proto:1418` | Declares the resource families the agent owns. |
| `Capabilities` (`map<string, bool>`) | `proto:1423` | Free-form capability flag bag. A2A-aligned (e.g. `"streaming"`, `"hibernation_aware"`, `"extensions_supported"`). |
| `Extensions` (`repeated string`) | `proto:1428` | URI list of extensions the agent supports. Phase 6 hooks into this. |

These three are **orthogonal**: a registration can declare a schema
without extensions, capabilities without a schema, etc.

### `AgentResourceSchemaEntry`

```proto
// api/proto/aether.proto:1432-1445
message AgentResourceSchemaEntry {
  string resource_type_prefix = 1;
  repeated string permission_verbs = 2;
  string resource_id_schema = 3;
}
```

- `resource_type_prefix` — e.g. `"chat/"`, `"docmgmt/document"`,
  `"workflow/run"`. Uniqueness is enforced **across active
  registrations**.
- `permission_verbs` — informational; Aether's `CheckAccess` routes
  through the existing operation/access-level model unchanged. Used by
  tooling, future AgentCard projections.
- `resource_id_schema` — optional JSON Schema (as a string)
  describing the `resource_id` shape under this prefix. Informational.

### Uniqueness enforcement

No two agents can claim overlapping `resource_type_prefix` values. The
gateway enforces this on `AgentOperation.REGISTER` and `UPDATE`:

- On collision the gateway returns `ErrCodePrefixConflict = "ERR_PREFIX_CONFLICT"`
  (`server/internal/gateway/agent_handler.go:19`) — the error string is
  prefixed `"ERR_PREFIX_CONFLICT: ..."` so SDK callers can pattern-match
  on it. Test:
  `server/internal/gateway/registry_acl_audit_test.go:14, 498, 563, 621`.

### `PrefixIndex` — in-memory routing

`server/internal/registry/prefix_index.go:24-31`:

```go
type PrefixIndex struct {
    mu sync.RWMutex
    prefixes map[string]string
}
```

The index maps `resource_type_prefix → implementation`. It is consulted
on the ACL `CheckAccess` hot path to identify the owning agent for any
resource access.

`Lookup` (`prefix_index.go:49-102`) does a right-to-left "/"-walk: for
`resource_type="docmgmt/document/abc"`, it tries (in order)
`"docmgmt/document/"`, `"docmgmt/document"`, `"docmgmt/"`, `"docmgmt"` —
first hit wins (longest match). Both trailing-slash and bare forms are
tolerated so an agent declaring `"chat/"` matches a query for `"chat"`
and vice versa.

The index is refreshed on every `Register` / `Delete` (`Set` /
`Delete`, `prefix_index.go:113-147`), plus a full `Rebuild`
(`prefix_index.go:157-176`) at gateway startup. It is **not** a source
of truth — the `agent_registry` table is — and if the index drifts,
audit attribution may be stale until the next `Rebuild`. ACL decisions
themselves never depend on the index — they always go through the
existing `CheckAccess` rules.

### Audit attribution

When an ACL `CheckAccess` runs on a resource type that maps to a
registered agent prefix, the audit row's metadata is enriched with two
keys: `owning_agent` and `owning_agent_prefix`. See
`server/internal/acl/audit.go` and the deep-dive
[`agent-acl-integration.md`](agent-acl-integration.md) for the full
metadata-bag schema.

`PrefixIndex.Lookup` returns both the implementation name and the
matched prefix so the audit row records *which* prefix from the
agent's declaration caught the access — useful for debugging when an
agent declares multiple prefixes (`prefix_index.go:41-48`).

### Multi-gateway caveat

The `PrefixIndex` is per-gateway, rebuilt at boot from the central
`agent_registry` table. There is no cross-gateway broadcast yet. In a
multi-gateway deployment, a `REGISTER` on one gateway is visible to
others on their next `Rebuild`. The deep-dive
[`agent-acl-integration.md`](agent-acl-integration.md) covers the
implications.

### SDK helpers

| Helper | Defined in | Effect |
|---|---|---|
| `make_resource_schema_entry(resource_type_prefix, permission_verbs, resource_id_schema)` | `client.py:206-244` | Builder; rejects empty prefix |
| `register_agent(implementation, profile, resource_schema=, capabilities=, extensions=, ...)` | `client_async.py:2482-2529` | `AgentOperation.REGISTER` |
| `update_agent(implementation, ..., resource_schema=, capabilities=, extensions=)` | `client_async.py:2531-2562` | `AgentOperation.UPDATE` |

`ERR_PREFIX_CONFLICT` is surfaced via `AgentResponse.error` prefixed
with `"ERR_PREFIX_CONFLICT: "` (`client.py:2940-2949`).

---

## 8. Phase 6 — Extension protocol

> This section is the cross-phase summary. The deep-dive lives in
> [`extension-protocol.md`](extension-protocol.md). Read this for the
> protocol overview; read the deep-dive for negotiation semantics in
> full and the test matrix.

### Why URIs

The A2A v1.0 extension model uses URIs as the global identity for
extensions. Aether borrows this for forward-compat: customers (or
future Aether-internal phases) can declare extensions without forking
the proto.

### `ExtensionDeclaration`

```proto
// api/proto/aether.proto:177-192
message ExtensionDeclaration {
  string uri = 1;          // globally unique extension URI
  string version = 2;      // optional version
  bool   required = 3;     // reject connection if unsupported
  string json_schema = 4;  // informational
}
```

### Connect-time negotiation

The client declares the extensions it wants active in **one of two
places**:

1. **`InitConnection.extensions`** — the authoritative proto field
   (`api/proto/aether.proto:155-164`). Carries the `required` flag.
2. **`Aether-Extensions` gRPC metadata header** — a comma-separated
   URI list. The gateway constant
   `extensionMetadataHeader = "aether-extensions"`
   (`server/internal/gateway/extensions.go:17`). Header-sourced
   declarations are always **non-required**. The proto field remains
   authoritative for `required` semantics.

The gateway unions both and runs `negotiateExtensions`
(`server/internal/gateway/extensions.go:117-186`) which computes:

- **Supported** when the URI is in `KnownExtensions` OR (for agent
  callers) appears in the agent's `AgentRegistration.Extensions`.
- **Unsupported and required** → connection rejected with
  `codes.FailedPrecondition` ("ERR_EXTENSION_UNSUPPORTED").
- **Unsupported and non-required** → still negotiated; surfaced in
  `ConnectionAck.negotiated_extensions` with `supported=false` and a
  `rejection_reason` for diagnostics.

### `KnownExtensions` registry

`server/internal/gateway/extensions.go:19-33`:

```go
var KnownExtensions = map[string]bool{
    // (intentionally empty in Phase 6)
}
```

Phase 6 ships with `KnownExtensions` empty. The wire + negotiation
machinery is in place; concrete server-blessed extensions land in
future phases as they're specified. Adding an entry here is the only
required action to declare gateway support.

### Per-envelope `active_extensions`

Both `UpstreamMessage` and `DownstreamMessage` carry a
`repeated string active_extensions` field
(`api/proto/aether.proto:48-55`, `:98-102`). It is per-envelope, not
per-payload, so any oneof variant can opt into extension semantics
without proto-side rewrites of every payload type.

Receivers MUST reject a message when an URI in `active_extensions` is
listed AND was declared `required` at connect AND is unsupported by the
receiver (error: `ERR_EXTENSION_UNSUPPORTED`). Otherwise unknown URIs
are ignored.

### Agent-declared extensions widen the set

When an agent's `AgentRegistration.Extensions` carries URIs that
`KnownExtensions` does not, those URIs are **supported on sessions
targeting that agent**. This is the link between Phase 5 and Phase 6:
customer-deployed agents can ship their own extensions without
gateway-side changes.

### `ConnectionAck` projection

`ConnectionAck` (`api/proto/aether.proto:118-137`) returns:

- `negotiated_extensions: repeated NegotiatedExtension` — one entry per
  client declaration, in declaration order. The
  `NegotiatedExtension` envelope carries `uri`, `version`, `supported`,
  and `rejection_reason` (`api/proto/aether.proto:198-211`).
- `server_supported_extensions: repeated string` — URIs the gateway
  natively supports that the client did NOT declare. Lets clients
  discover what optional extensions are available without a separate
  descriptor endpoint.

### SDK helpers

| Helper | Defined in | Behavior |
|---|---|---|
| `make_extension(uri, version, required, json_schema)` | `client.py:247-286` | Builder; rejects empty URI |
| `negotiated_extensions()` | `client.py:431` | After connect, returns the URIs the gateway accepted |
| `extensions=` kwarg | client constructors | Carried into `InitConnection.extensions` |

---

## 9. End-to-end walkthrough: a multi-phase agentic workflow

> Scenario: a user-facing chat agent (registered with
> `resource_schema=[{prefix:"chat/"}]`) receives a request that requires
> elevated permissions to read a sensitive workspace. The agent submits
> an authority request, a policy-bot approves it with a narrower scope,
> the agent hibernates the parent task across a long-running
> sub-computation, and a UI dashboard observes the entire chain.

### Setup (deployment-time)

```python
# Operator registers the chat agent. Phase 5: declare what it owns.
await admin.register_agent(
    implementation="chat-orchestrator",
    profile="k8s",
    description="user-facing chat agent",
    resource_schema=[
        make_resource_schema_entry(
            resource_type_prefix="chat/",
            permission_verbs=["read", "write"],
        ),
    ],
    capabilities={"streaming": True, "hibernation_aware": True},
)
# Gateway PrefixIndex now has "chat/" -> "chat-orchestrator". Any ACL
# CheckAccess on a resource_type matching "chat/..." will be audited
# with owning_agent=chat-orchestrator.

# Same for the policy-bot:
await admin.register_agent(
    implementation="policy-bot",
    profile="k8s",
    capabilities={"deterministic_approver": True},
)
```

### Step 1 — User starts a chat turn; agent spawns a sub-task

```python
# Chat agent connects (Phase 6 — declares an extension it cares about):
await chat_client.connect_as_agent(
    workspace="my-tenant",
    implementation="chat-orchestrator",
    specifier="pod-7",
    extensions=[
        make_extension(uri="https://aether.scitrera.com/ext/chat-attachments/v1"),
    ],
)
# ConnectionAck.negotiated_extensions reports supported=True for the URI
# (because the agent registration declared it), or supported=False if not.

# When the user message arrives, the agent creates a child task to do
# the sensitive read. context_id groups the multi-turn conversation.
child = await chat_client.create_task(
    task_type="compliance-summary",
    workspace="compliance-vault",       # sensitive workspace
    assignment_mode="pool",
    target_implementation="reader-worker",
    context_id=conversation_id,         # Phase 1: groups across tasks
    metadata={"thread_id": conversation_id, "turn": "5"},
)
```

### Step 2 — Submit an authority request, park the parent

```python
# The agent's own session does not hold read on compliance-vault.
# Submit an authority request gated by a capability that the policy-bot
# holds. Phase 2.
req = await chat_client.request_authority(
    desired_workspace_scope=["compliance-vault"],
    desired_resource_scope=[
        make_authority_request_resource_scope_entry(
            "chat/conversations", ["*"],
        ),
    ],
    desired_operation_scope=["read"],
    requested_access_level=aether_pb2.ACCESS_LEVEL_READ,
    requested_duration_seconds=600,                        # 10 minutes
    routing_capability="capability/approve/compliance_read",
    reason=f"summarising memo for user {user_id}",
    task_id=parent_task_id,                                # so the waker can wake us
)
auth_request_id = req.request.request_id

# Park the parent on the authority request. Phase 1 PAUSE.
await chat_client.pause_task(
    task_id=parent_task_id,
    wait_spec=make_wait_spec(
        reason=aether_pb2.WAIT_REASON_AUTHORITY,
        authority_request_id=auth_request_id,
        timeout_ms=15 * 60 * 1000,    # 15 min ceiling on the wait
    ),
    reason="awaiting compliance approval",
)
# parent_task_id is now WAITING_AUTHORITY.
# Audit log: row "authority_request_created", actor=chat-orchestrator,
# request_id=auth_request_id.
```

### Step 3 — Policy-bot resolves with narrower scope

```python
# Elsewhere, the policy-bot polls pending requests it can resolve.
pending = await policy_bot.list_pending_authority_requests(
    matching_capabilities=["capability/approve/compliance_read"],
)

for r in pending.requests:
    if r.requesting_actor.principal_id.startswith("ag.my-tenant.chat-orchestrator"):
        # Approve with a narrower duration than requested.
        await policy_bot.resolve_authority_request(
            request_id=r.request_id,
            approve=True,
            granted_duration_seconds=300,     # 5 min — narrower than 600
            reason="auto-approved by policy: chat read of compliance",
        )

# Server-side:
#  - ApproveAuthorityRequest intersects scopes, clamps duration to min(300, 600, 3600).
#  - CreateAuthorityGrant mints a fresh grant; granted_grant_id is recorded.
#  - acl_authority_requests row flips PENDING -> APPROVED.
#  - Audit log: row "authority_request_approved", actor=policy-bot,
#    metadata.granted_grant_id=<new grant id>.
#  - Audit chain links: action_audit -> grant (mint) -> request (approved).
```

### Step 4 — Parent wakes, hibernates over the slow sub-task

```python
# The task_waker observed APPROVED on its next 10-second tick and
# called ResumeTask(parent, running). On reconnect, the agent fetches
# its task and sees Status=RUNNING.

# The compute task is going to take 6 hours. Hibernate the parent.
state_bytes = msgpack.packb({"conversation_id": conversation_id,
                             "child_task_id": child.task_id})
await chat_client.checkpoint_save(key=f"chat-{parent_task_id}-v1",
                                   data=state_bytes)

await chat_client.hibernate_until(
    task_id=parent_task_id,
    checkpoint_key=f"chat-{parent_task_id}-v1",
    timeout_ms=8 * 3600 * 1000,                # 8 hour ceiling
    escalation_policy="retry",
)
# parent_task_id is now HIBERNATED.
# Gateway sends TaskHibernated{descriptor} downstream to the assigned worker.
# Worker closes its stream cleanly. disconnect_reaper sees IsWaiting=true
# and skips this task — the disconnect is intentional.
```

### Step 5 — Child completes; UI dashboard observes via subscription

```python
# Separately, a UI dashboard process subscribed at session start:
sub_id = await ui_client.subscribe_to_task(
    task_id=parent_task_id,
    recursive=True,                # also stream child events
)

# When the child finishes:
#   compute-worker -> CompleteTask(child.task_id)
#
# Phase 4 emits a TaskStatusChangedEvent on
# tk.compliance-vault.{child.task_id}.events AND a
# TaskChildLifecycleEvent on tk.compliance-vault.{parent_task_id}.events.
# Dashboard sees both (recursive=True flows the child status, parent
# relays its child_lifecycle).
#
# Also: TaskAssignmentService.wakeDependents would have fired
# (parent had no DependsOn here, only authority-and-then-hibernation,
# so this step is informational — the parent is HIBERNATED, not
# WAITING_DEPENDENCY).
```

### Step 6 — Hibernation timer fires; fresh worker rehydrates

```python
# Hours later, the parent's HibernationDescriptor escalation policy is
# "retry"; the task_waker sees the WaitSpec.TimeoutMs hasn't elapsed
# yet — but suppose ScheduledWakeUnixMs WAS set instead (alternative
# wake path). Either way:
#
# WakeHibernatedTask:
#   1. Reload parent, confirm Status=HIBERNATED.
#   2. Merge into metadata:
#        _hibernation_checkpoint_key = "chat-{parent_task_id}-v1"
#        _hibernation_resume_session_id = ""
#   3. ResumeTask(parent, pending) — NOT running.
#   4. InsertQueueEntry(... implementation="chat-orchestrator", profile="k8s",
#                       launch_params=...)
#
# Orchestrator picks up the queue entry. New worker is spawned. Its
# TaskAssignment carries:
#   TaskAssignment.task_id = parent_task_id
#   TaskAssignment.checkpoint_key = "chat-{parent_task_id}-v1"
#   TaskAssignment.resume_session_id = ""
#
# The fresh worker's on-task-assignment handler:
async def on_task_assignment(self, assignment: aether_pb2.TaskAssignment):
    if assignment.checkpoint_key:
        ck = await self.checkpoint_load(key=assignment.checkpoint_key)
        state = msgpack.unpackb(ck.data)
        # ... continue from saved state ...
```

### Step 7 — Audit trail

The audit log (`comprehensive_audit_log` table, queryable via
`AuditQuery`) now contains an unbroken chain:

- **ACL check** rows for every resource access on `chat/...`,
  attributed in metadata to `owning_agent=chat-orchestrator,
  owning_agent_prefix="chat/"` (Phase 5).
- **`authority_request_created`** by `chat-orchestrator` for
  `auth_request_id`.
- **`authority_request_approved`** by `policy-bot`, metadata
  `granted_grant_id=<g>`.
- **Authority-grant** rows: the new grant `<g>`'s creation row.
- **Subsequent ACL checks** using grant `<g>` show the full
  `actor → subject → root_subject` chain (existing OBO infra).
- **Task lifecycle** rows for parent: `running → waiting_authority →
  running → hibernated → pending → assigned → running → completed`.

---

## 10. Clarifications: confusion and overlap with prior state

This section catalogues the places where new (Phase 1-6) concepts
look like older ones, and explains how to tell them apart. Skim the
left two columns first; read the "Rule of thumb" column if you find
yourself confused in code.

### 10.1 `ResolveAuthorityRequest` (existing) vs `ResolveAuthorityRequestPayload` (Phase 2)

| | Old | New (Phase 2) |
|---|---|---|
| Message | `ResolveAuthorityRequest` (`api/proto/aether.proto:2761-2772`) | `ResolveAuthorityRequestPayload` (`api/proto/aether.proto:2090-2108`) |
| What it does | Asks the gateway to **validate an existing grant** against the caller and audience; returns a public-safe projection | Carries the approver's **decision** (APPROVE / DENY) when resolving an open authority request |
| Used by | Trusted callers like the Python `ProxyHttpTerminator` to mint `X-Auth-*` headers | An approver submitting `AuthorityRequestOperation{RESOLVE}` |
| Wire field | `UpstreamMessage.resolve_authority_request` (tag 27) | `AuthorityRequestOperation.resolve` (oneof body of tag 30) |

**Rule of thumb**: If you're *checking* an existing grant, you want the
top-level message. If you're *approving or denying* a pending request,
you want the payload. The proto comment on the payload (proto:2087-2089)
explicitly calls out the naming collision.

### 10.2 `AuthorityRequest` (Phase 2) vs `AuthorityGrant` (existing)

| | `AuthorityGrant` (existing) | `AuthorityRequest` (Phase 2) |
|---|---|---|
| What it is | A **minted, active authorization** that can be checked against | The **ask-for-a-grant lifecycle row** |
| Lifetime | Active until expiry / revocation | PENDING until APPROVED / DENIED / EXPIRED / CANCELLED |
| Outcome | Used directly to enforce access | Approval **creates an `AuthorityGrant`** |
| Storage | `acl_authority_grants` | `acl_authority_requests` |

**Rule of thumb**: Requests model the conversation; grants model the
authorization itself. There is no parallel grant type — the approval
path terminates in `CreateAuthorityGrant`
(`authority_request_lifecycle.go:301-304`).

### 10.3 `WaitSpec.AuthorityRequestID` (Phase 1) vs Phase 2 actual id

In the Phase 1 PR `WaitSpec.AuthorityRequestID` shipped as a placeholder
(`server/pkg/tasks/types.go:67`), waiting for the Phase 2 lifecycle to
populate it. In Phase 2 it becomes the **actual** correlation id of the
`AuthorityRequest` row. Modules built against Phase 1 alone saw it as a
free-form string; modules running against Phase 2+ should treat it as a
foreign key.

**Rule of thumb**: If you read code that puts a literal value into
`AuthorityRequestID`, it's old Phase 1 scaffolding. Today's flow always
populates it from `AuthorityRequestOperationResponse.request.request_id`.

### 10.4 `waiting_input` vs session resume

| | Session resume | `waiting_input` |
|---|---|---|
| What it does | Reconnects a client to a live task in any status — the lock is taken over | Pauses the task explicitly into a wait state |
| Task status | Unchanged (still `running`, etc.) | Becomes `waiting_input` |
| Triggered by | `InitConnection.resume_session_id` | `TaskOperation.PAUSE` with `WaitSpec.Reason=input` |
| Wake | Implicit — the worker is back online | Inbound matching input message (today: event-driven path planned, see [§12](#12-future-work--what-was-deferred)) |

**Rule of thumb**: Session resume is transport-layer (reconnect a stream).
`waiting_input` is application-layer (the agent is paused on a logical
input event).

### 10.5 Hibernation vs the other waiting states

| | `waiting_input` / `waiting_authority` / `waiting_dependency` | `hibernated` |
|---|---|---|
| Worker | **Retained** — gRPC stream stays open | **Released** — worker disconnects after `TaskHibernated` |
| Resume target | `running` directly | **`pending`** — orchestrator spawns a fresh worker |
| Disconnect reaper | Skips waiting states (`disconnect_reaper.go:83`) | Same; skipped |
| Checkpoint required | No | **Yes** — gateway validates checkpoint exists before allowing HIBERNATE |
| Hand-off | None — same worker resumes | `TaskAssignment.checkpoint_key` + `resume_session_id` |

**Rule of thumb**: If the wait is expected to be short and the worker
has cheap state, use one of the first three. If the wait could be
hours-to-days and you want to reclaim compute, hibernate.

### 10.6 `ResumeTask` (non-hibernation) vs `WakeHibernatedTask`

| | `ResumeTask` | `WakeHibernatedTask` |
|---|---|---|
| Defined in | `task_assignment.go:797-808` | `task_assignment.go:833-916` |
| Target status | `running` (typically) | `pending` |
| Side effects | Clears `WaitSpec`, emits event | Merges hibernation hand-off into metadata, clears `WaitSpec`, re-queues into `orchestrated_task_queue` |
| Used by | `task_waker` for non-hibernation; `TaskOperation{RESUME}` | `task_waker` for hibernated rows only |

**Rule of thumb**: `RESUME` op resumes anyone. The waker uses
`WakeHibernatedTask` internally on `hibernated` rows because the worker
is gone and the orchestrator must spawn fresh.

### 10.7 `TaskOperation.PAUSE` vs `TaskOperation.WAIT_FOR`

| | `PAUSE` (`OpType=4`) | `WAIT_FOR` (`OpType=5`) |
|---|---|---|
| Accepts | Any `WaitSpec.Reason` | Forces `WaitSpec.Reason=DEPENDENCY` |
| Status outcome | `waiting_input` / `waiting_authority` / `waiting_dependency` / `hibernated` | Always `waiting_dependency` |
| SDK helper | `pause_task(task_id, wait_spec, ...)` | `wait_for_task(task_id, depends_on, wake_on_any, ...)` |

**Rule of thumb**: `WAIT_FOR` is a specialisation of `PAUSE`. The two
land at the same `taskStore.PauseTask` path; the SDK split is for
ergonomics.

### 10.8 `task_waker` vs `disconnect_reaper`

| | `task_waker` | `disconnect_reaper` |
|---|---|---|
| Concern | Drive **wake** transitions on waiting tasks | Fail **disconnected** tasks past their grace window |
| Scans | `waitingStatuses` rows | `running`/`starting`/`assigned` rows with `DisconnectedAt` stamped |
| Phase 3 hook | New scanner | Added `IsWaiting` guard so it skips intentional disconnects (`disconnect_reaper.go:83`) |
| Cadence | 10 second ticker (default) | Configurable; similar shape |

**Rule of thumb**: Waker drives forward progress. Reaper enforces
liveness. They are explicitly separated so each can be reasoned about
independently.

### 10.9 `_hibernation_checkpoint_key` metadata vs `WaitSpec.Hibernation.CheckpointKey`

Both fields hold the same value at different moments:

| Field | Lives in | Populated when |
|---|---|---|
| `WaitSpec.Hibernation.CheckpointKey` | The task's persisted `WaitSpec` while hibernated | Set when the agent calls `hibernate_until(checkpoint_key=...)` |
| `Task.Metadata["_hibernation_checkpoint_key"]` | The task's metadata bag after wake | Set by `WakeHibernatedTask` *before* clearing the `WaitSpec`; the reserved-key constants live at `server/pkg/tasks/types.go:79-82` |

The metadata copy exists because `ResumeTask` (called inside
`WakeHibernatedTask`) clears the `WaitSpec`. Without the metadata
hand-off, the `TaskAssignment` produced for the fresh worker would have
no checkpoint key.

**Rule of thumb**: `WaitSpec.Hibernation.CheckpointKey` is the durable
record. The metadata key is the wake-time hand-off carrier. Both
intentional; the metadata copy is left in place as audit history.

### 10.10 `creator_actor_id` proto field vs `parent_agent_id` storage column

The Phase 4 filter `TaskFilter.creator_actor.principal_id`
(`api/proto/aether.proto:1006`) is matched against the storage column
`parent_agent_id` on the `tasks` table (Phase 5 audit notes confirm
no separate `creator_actor_id` column exists today). The Go-side
`TaskFilter.CreatorActorID` field comment makes this explicit
(`server/pkg/tasks/types.go:340-345`).

**Rule of thumb**: The proto field is the wire-friendly name; the
storage column is the underlying schema. Filter by `creator_actor` on
the wire, expect `parent_agent_id` in audit-log reasoning.

### 10.11 `AgentRegistration.Extensions` (Phase 5) vs `ExtensionDeclaration` (Phase 6)

| | `AgentRegistration.Extensions` | `ExtensionDeclaration` |
|---|---|---|
| Where | Stored on the agent's row | Sent at connect / per-envelope |
| What it says | "I, the agent, statically support these URIs" | "I, this connection, want these active right now" |
| When evaluated | Loaded by the negotiator at connect time of any session targeting this agent | Negotiated on every `InitConnection` |
| Phase | 5 (registry unification) | 6 (negotiation protocol) |

**Rule of thumb**: Phase 5 is the static manifest; Phase 6 is the
runtime handshake. They compose: an agent's static declarations widen
the supported set for that agent's sessions.

### 10.12 `AgentRegistration.Capabilities` vs `ConnectionAck.negotiated_extensions`

| | `Capabilities` | `negotiated_extensions` |
|---|---|---|
| Shape | `map<string, bool>` of free-form flags | `repeated NegotiatedExtension` |
| Purpose | Forward-looking capability flags (`streaming`, `hibernation_aware`, ...) | Outcome of one specific session's extension negotiation |
| Phase | 5 | 6 |

**Rule of thumb**: `Capabilities` is the agent's self-description,
useful for tooling and future AgentCard. `negotiated_extensions` is
this-session-only.

### 10.13 `tk::{workspace}::{task_id}::events` vs `pg::{workspace}` topic

| | `pg::{workspace}` (old) | `tk::{workspace}::{task_id}::events` (Phase 4) |
|---|---|---|
| Scope | Workspace-wide firehose | Per-task |
| Carries | `ProgressUpdate` only | `TaskStatusChangedEvent`, `TaskProgressEvent`, `TaskChildLifecycleEvent`, `TaskAuthorityRequestEventRelay` |
| Filtering | Server-side recipient filter | Topic itself is per-task; subscribers pick which tasks |

**Rule of thumb**: `pg::{workspace}` is for "show me all progress in
this workspace". `tk::{workspace}::{task_id}::events` is for "show me
everything happening to this specific task".

### 10.14 ACL `CheckAccess` (existing) vs `PrefixIndex` (Phase 5)

| | `CheckAccess` | `PrefixIndex` |
|---|---|---|
| Decision authority | **Yes** — grants or denies | **No** — never grants or denies |
| Used for | Routing the existing rule/grant machinery | Audit attribution only — enrich rows with `owning_agent` / `owning_agent_prefix` |
| If it fails / drifts | Authorization breaks | Audit attribution is stale until next `Rebuild`; decisions unaffected |

**Rule of thumb**: PrefixIndex is **purely advisory**. Authorization
still goes through the existing ACL rules system unchanged.

### 10.15 `context_id` (Phase 1) vs `workspace`

| | `workspace` | `context_id` (Phase 1) |
|---|---|---|
| Scope | Resource container (ACL, KV partition, topic dimension) | Multi-task interaction grouping (conversation, retry batch) |
| Server interpretation | Heavy — scoping, ACL, topic routing | None — opaque pass-through |
| Client interpretation | Heavy | Heavy (client decides what it means) |

**Rule of thumb**: workspace = where the resources live. context_id =
which logical interaction the task is part of. They never substitute
for each other.

### 10.16 `TaskFilter.exclude_statuses` vs `TaskFilter.statuses`

Both can be combined:

- `statuses` — positive set: include only tasks whose status is in
  this list.
- `exclude_statuses` — negative set: omit tasks whose status is in
  this list.

**Rule of thumb**: Use `statuses` for "interactive + running"; use
`exclude_statuses` for "all non-terminal" (without enumerating every
non-terminal value).

### 10.17 Phase 4 `authorizeTaskOp` vs pre-Phase-4 workspace match

Pre-Phase-4 task ops were authorised purely by workspace membership.
`authorizeTaskOp` (`server/internal/gateway/routing.go:1466-1466`)
widened this to **creator OR assignee OR workspace-Manage**.

**Rule of thumb**: If you used to expect "only workspace admins can
cancel tasks", today task creators can cancel their own tasks too.
This is intentional — agents shouldn't need workspace-admin to manage
the tasks they created.

### 10.18 `Aether-Extensions` header vs `InitConnection.extensions`

| | gRPC metadata header | Proto field |
|---|---|---|
| Key | `aether-extensions` (`server/internal/gateway/extensions.go:17`) | `InitConnection.extensions` (`api/proto/aether.proto:164`) |
| Value | Comma-separated URI list | `repeated ExtensionDeclaration` |
| `required` semantics | Always non-required (header sources can't express `required=true`) | Authoritative; carries `required` per declaration |

**Rule of thumb**: Both work. The header is a lighter-weight option for
clients that can't easily re-encode `InitConnection`. If you need
`required=true`, you must use the proto field.

### 10.19 `AgentRegistration.LaunchParams.profile` vs `Capabilities`

| | `LaunchParams.profile` | `Capabilities` |
|---|---|---|
| When read | Orchestration time (which orchestrator profile spawns the agent — `"local"`, `"k8s"`, `"docker"`, ...) | Forward-looking: feature flags consumed by tooling, future AgentCard |
| Shape | Single string inside `LaunchParams` | `map<string, bool>` |

**Rule of thumb**: `profile` answers "how do I spawn this?".
`Capabilities` answers "what features does this agent support?".

### 10.20 `TaskHibernated` downstream message vs `TaskEvent.status_changed(hibernated)`

| | `TaskHibernated` (`api/proto/aether.proto:105-114`) | `TaskEvent.status_changed{to=HIBERNATED}` |
|---|---|---|
| Target | The **assigned worker** specifically | All **subscribers** of the task's event topic |
| Purpose | Signal the worker to disconnect cleanly | Broadcast the lifecycle transition |
| Carries | `task_id` + `HibernationDescriptor` echo | `from_status` + `to_status` + reason |

**Rule of thumb**: `TaskHibernated` is the targeted command to the
worker. `status_changed` is the public broadcast that something
happened.

---

## 11. Migration notes

For consumers upgrading from pre-Phase-1:

### Proto wire compatibility

- All new fields and messages are **additive**. Existing fields keep
  their tags. Backward compat is preserved on the wire.
- New `TaskStatus` enum values (`WAITING_*`, `HIBERNATED`, `REJECTED`)
  are appended. Old clients ignore them; new clients must be ready to
  see them.
- New `TaskOperation.OpType` values (`PAUSE`/`WAIT_FOR`/`RESUME`/`REJECT`)
  are appended.
- New `UpstreamMessage` / `DownstreamMessage` oneof variants
  (`authority_request_op`, `task_subscription_op`, `authority_request_response`,
  `authority_request_event`, `task_hibernated`,
  `task_subscription_response`, `task_event`) are appended with new
  tags (>=30).
- `active_extensions` is a new `repeated string` on both envelope
  messages; absent on old wire = empty.

### Storage

- New columns on the `tasks` table (`wait_spec`, `depends_on`,
  `context_id`, `paused_at`) are all nullable. Existing rows
  deserialise with `nil`/`""`/`0` for the new fields.
- New table `acl_authority_requests` is added in the relevant
  migration. The existing `acl_authority_grants` and
  `comprehensive_audit_log` schemas are unaffected.
- New columns on `agent_registry` (`resource_schema`,
  `capabilities`, `extensions`) are nullable.

### SDK

- All new SDK kwargs are **optional** with safe defaults. Old call
  signatures continue to work unchanged.
- New helpers (`pause_task`, `wait_for_task`, `resume_task`,
  `reject_task`, `hibernate_until`, `request_authority`,
  `list_pending_authority_requests`, `resolve_authority_request`,
  `cancel_authority_request`, `subscribe_to_task`,
  `unsubscribe_from_task`, `register_task_event_handler`) are
  additive.
- New module-level builders: `make_wait_spec`,
  `make_hibernation_descriptor`,
  `make_authority_request_routing`,
  `make_authority_request_resource_scope_entry`,
  `make_resource_schema_entry`, `make_extension`.

### New typed error codes

| Code | Where | Meaning |
|---|---|---|
| `ERR_PREFIX_CONFLICT:` | Wire-prefixed on `AgentResponse.error` from REGISTER/UPDATE | Another active registration already claims the supplied `resource_type_prefix` (`server/internal/gateway/agent_handler.go:19`) |
| `ERR_EXTENSION_UNSUPPORTED` | `codes.FailedPrecondition` on connect; per-message reject | The client declared a `required=true` extension URI the gateway and target agent (if any) do not support |

### Behavior changes worth flagging

1. **`disconnect_reaper` skip on waiting states** — pre-Phase-3, a
   worker disconnect always raced against the grace window. Phase 3
   added `IsWaiting` (`disconnect_reaper.go:83`); waiting/hibernated
   tasks are now immune. Any monitoring keyed off "disconnected task
   failed" may now see waiting tasks survive their worker dying.
2. **Task-op error info-hiding** — pre-Phase-4, workspace-mismatch
   returned a distinct "not authorized" error. Phase 4 collapses both
   "not authorized" and "not found" into the single string
   `"task not found"` (`server/internal/gateway/routing.go:1449`).
   Clients pattern-matching on the old error text must update.
3. **`ResumeTask` on HIBERNATED is special** — Phase 3 introduced
   `WakeHibernatedTask` which routes `hibernated → pending` instead of
   the conventional `hibernated → running`. The orchestrator must
   re-spawn. Anything that called `RESUME` directly on a hibernated
   task before should still work via the same op — the storage layer
   routes to the special path.
4. **Audit metadata enrichment** — any code consuming
   `comprehensive_audit_log.metadata` may now see additional keys:
   `owning_agent`, `owning_agent_prefix` on rows where the resource
   type matched a registered prefix. Existing keys are unchanged.
5. **`TaskOperation.RESUME` semantics on hibernated rows** — direct
   admin resume of a hibernated task now goes through `pending`, not
   `running`, because the worker has been released. Old admin
   tooling that assumed `running` should be updated.

---

## 12. Future work / what was deferred

Captured here so the gaps don't get lost. Each item is intentionally
*not* in Phases 1-6.

### AgentCard generation (Phase 6 deferred)

The A2A spec describes an `AgentCard` JSON descriptor with
`AgentSkill` JSON-Schema I/O, signed cards, well-known endpoint
hosting. Phase 6 deferred this entirely — Phase 5's
`AgentRegistration` (now carrying `ResourceSchema` + `Capabilities` +
`Extensions`) already encodes everything an AgentCard would need.
When external A2A interop becomes a priority, an `AgentCard` becomes a
**projection** over the existing registry, not new state.

### Multi-gateway `PrefixIndex` broadcast

`PrefixIndex` is per-gateway, rebuilt at boot. A future iteration
would push `Set` / `Delete` deltas across the cluster so a
`REGISTER` on one gateway updates attribution on all peers
immediately. The deep-dive
[`agent-acl-integration.md`](agent-acl-integration.md) covers the
implications of the current design.

### Late agent self-registration

Today, only **orchestrators** register late (at connect via
`OrchestratorIdentity.supported_profiles`); agents are
pre-registered admin entries. A future iteration could let agents
self-register their `ResourceSchema` on startup. The Phase 5 schema
additions leave the door open without locking in a particular shape.

### Workflow-level DAG redesign

`workflow/dag.go` is intentionally out of scope. The pre-declared DAG
handles its use cases; Phase 1 added *spontaneous* dependencies
(`WAIT_FOR`) as an orthogonal capability, not a replacement.

### First-class `owning_agent` column on audit log

Today `owning_agent` and `owning_agent_prefix` are stored in the
`comprehensive_audit_log.metadata` JSON bag. A future migration could
promote them to top-level columns for indexable queries. For now,
JSON-path queries on metadata are the supported access pattern.

### Concrete server-blessed extensions

`KnownExtensions` is empty in Phase 6 (`server/internal/gateway/extensions.go:31-33`).
The wire + negotiation foundation is in place; concrete extensions
land in later phases as they're specified.

### Worker-side rehydration hooks

The Phase 3 SDK exposes `hibernate_until`; the wake side delivers a
`TaskAssignment` with `checkpoint_key` + `resume_session_id`. The SDK
intentionally does **not** auto-invoke a checkpoint-load on
assignment delivery — agent code reads the assignment, decides what
checkpoint semantics to apply, and calls `checkpoint_load`
explicitly. Future SDK iterations may add an opt-in auto-rehydration
hook, but Phase 3 keeps the contract explicit.

### Event-driven `WAITING_INPUT` wake

Today's `task_waker` reconciles timer / scheduled / dependency /
authority wakes (`task_waker.go:32-48`). Input-arrival wake is
documented as event-driven and is deferred. Until it lands, the agent
must use one of:
- timer / scheduled wake to come back and check,
- the SDK's existing message-delivery hooks to manually `resume_task`,
- a sibling task that the input flow will mark terminal (then use
  `wait_for_task`).

### Subscription to "new child spawned" events

`TaskSubscription` with `recursive=True` is a **snapshot-at-subscribe**
model: descendants known at subscribe time are tracked; tasks born
after subscribe are NOT picked up automatically
(`api/proto/aether.proto:2851-2853`). A future iteration could add a
"spawned" lifecycle event so subscribers see newly-created
descendants in real-time. Today, subscribers must `LIST` periodically
to discover new children.

### Audit log retention policies for the new event types

The new Phase 2 lifecycle events (`authority_request_*`) and Phase 4
task events ride the existing `comprehensive_audit_log` and topic
infrastructure. Per-event-type retention policies (e.g. keep
`authority_request_approved` longer than `authority_request_expired`)
are a future operator-facing feature; today retention is uniform
across event types via the existing `CLEANUP_AUDIT_LOGS` admin op.

---

*End of guide. For corrections or expansion, see the file headers in
`server/pkg/tasks/types.go`, `api/proto/aether.proto`,
`server/internal/orchestration/task_waker.go`, and
`server/internal/acl/authority_request_lifecycle.go` — those four
files are the canonical sources for the lifecycle described here.*
