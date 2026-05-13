# Aether Error Codes

This document catalogs all error codes that the Aether gateway emits to clients via `ErrorResponse` messages on the gRPC stream. Each code is a stable string constant that SDKs and clients can inspect programmatically.

## Format

Error responses include:
- `code` — the string constant listed here
- `message` — human-readable detail (may vary per invocation)
- `retryable` — whether the client should retry the operation
- `retry_after_ms` — hint for how long to wait before retrying (when `retryable` is true)

---

## Connection / Session Errors

| Code | gRPC Status | Retryable | Meaning | Typical Cause | Client Action |
|------|-------------|-----------|---------|---------------|---------------|
| `ERR_SESSION_001` | AlreadyExists | No | Duplicate identity — another connection with the same identity is already active | Two processes starting with the same `workspace/implementation/specifier` | Terminate the duplicate, or choose a unique specifier |

> **Note:** `ERR_SESSION_001` is surfaced as a gRPC `AlreadyExists` status error during the `InitConnection` handshake, before the stream enters the message loop. SDKs typically expose this as `DuplicateIdentityError`.

---

## Messaging / Routing Errors

| Code | gRPC Status | Retryable | Meaning | Typical Cause | Client Action |
|------|-------------|-----------|---------|---------------|---------------|
| `ERR_INVALID_TOPIC` | InvalidArgument | No | Target topic has an invalid prefix or is malformed | Misformatted topic string passed to `SendMessage` | Fix the topic address; check topic schema documentation |
| `ERR_PERMISSION_DENIED` | PermissionDenied | No | Sender's principal type or ACL rules forbid sending to this topic | Sending to an off-limits topic (e.g., agent→orchestrator, cross-workspace) | Check the permission matrix and ACL rules for this principal |
| `ERR_PAYLOAD_TOO_LARGE` | InvalidArgument | No | Message payload exceeds the configured maximum size | Payload larger than gateway's `max_message_size` setting | Split payload or compress before sending |
| `ERR_RATE_LIMITED` | ResourceExhausted | Yes | Per-client message rate limit exceeded | Sending too many messages too quickly | Back off and retry after `retry_after_ms` |
| `ERR_WORKSPACE_RATE_LIMITED` | ResourceExhausted | Yes | Per-workspace message rate limit exceeded | Aggregate workspace traffic exceeds configured threshold | Back off and retry after `retry_after_ms` |
| `ERR_QUOTA_001` | ResourceExhausted | Yes | Per-workspace connection or resource quota exceeded | Workspace has hit its connection count or other quota ceiling | Reduce connections or request a quota increase |
| `ERR_CIRCUIT_OPEN` | Unavailable | Yes | Message broker circuit breaker is open; RabbitMQ publish rejected | RabbitMQ is temporarily unavailable or unhealthy | Retry with exponential backoff; monitor broker health |
| `ERR_PUBLISH_FAILED` | Internal | Yes | Message delivery to RabbitMQ failed for a non-circuit-breaker reason | Transient broker error | Retry; escalate if persistent |
| `ERR_INVALID_PRINCIPAL` | InvalidArgument | No | Operation is not permitted for this principal type | Attempting a workspace switch as a non-user principal, or sending a progress report as a non-task | Use the correct principal type for this operation |
| `ERR_WORKSPACE_SWITCH_FAILED` | Internal | No | Workspace switch failed during ACL check or topic subscription | ACL system error or Redis subscription failure during `SwitchWorkspace` | Reconnect; if persistent, check gateway logs |
| `ERR_NOT_IMPLEMENTED` | Unimplemented | No | Operation is not supported over the streaming gRPC API | Attempting an admin-only operation (e.g., `AgentOperation`) via the stream | Use the REST admin API for this operation |

---

## Metric Payload Errors

These codes are only emitted when a `METRIC`-type message fails server-side validation. See specification §4.5 for the full metric payload contract.

| Code | gRPC Status | Retryable | Meaning | Typical Cause | Client Action |
|------|-------------|-----------|---------|---------------|---------------|
| `ERR_METRIC_INVALID` | InvalidArgument | No | Payload did not unmarshal as a valid `Metric` proto, or `metadata` exceeds 64 keys | Payload is not a serialized `Metric` message, or metadata map is too large | Serialize the payload as a `Metric` proto; trim metadata |
| `ERR_METRIC_EMPTY` | InvalidArgument | No | `Metric.entries` is empty, or exceeds the maximum of 1024 entries | Sending a metric with zero entries, or batching too many entries | Include at least one entry; split large batches |
| `ERR_METRIC_INVALID_ENTRY` | InvalidArgument | No | An entry has an empty `name`, or a non-finite `qty` (NaN, +Inf, -Inf) | Unnamed metric entry or floating-point overflow in the qty field | Validate all entry names and qty values before sending |
| `ERR_METRIC_NEGATIVE_FORBIDDEN` | PermissionDenied | No | A negative `qty` entry was sent but the principal lacks the `capability/metric_credit` ACL permission | Sending a credit (negative delta) without the required ACL grant | Request the `capability/metric_credit` permission for this principal, or only send non-negative deltas |

---

## Orchestration Errors

These codes appear in `TaskOperationResponse` or `ErrorResponse` messages related to task creation and orchestration.

| Code | gRPC Status | Retryable | Meaning | Typical Cause | Client Action |
|------|-------------|-----------|---------|---------------|---------------|
| `ERR_TASKS_NOT_ENABLED` | Unavailable | No | Task/orchestration subsystem is not enabled (no PostgreSQL configured) | Gateway started without a PostgreSQL connection | Enable the task subsystem by configuring `postgres.*` settings |
| `ERR_INVALID_ARGUMENT` | InvalidArgument | No | A required field in a task request is missing or invalid | `task_workspace` is empty in a `CreateTaskRequest` | Inspect the error message for the specific missing field |
| `ERR_TASK_CREATE_FAILED` | Internal | No | Task record could not be persisted in PostgreSQL | Database write failure during task creation | Check database connectivity; retry |
| `ERR_ORCH_001` | NotFound | No | Agent implementation not found in the orchestration registry | Requesting a task for an agent implementation that no orchestrator has registered | Ensure an orchestrator has registered the implementation |
| `ERR_ORCH_002` | Unavailable | No | No active orchestrator is available for the requested profile | All orchestrators for the workspace/profile combination are offline | Wait for an orchestrator to connect; retry |
| `ERR_ORCH_003` | Internal | No | Task assignment to orchestrator failed | Internal error during claim-and-assign flow | Retry; check gateway and orchestrator logs |
| `ERR_ORCH_004` | AlreadyExists | No | Duplicate orchestrator registration for the same implementation | Two orchestrators registering the same implementation in the same workspace | Ensure only one orchestrator registers each implementation |
| `ERR_INTERNAL` | Internal | No | Unclassified internal server error | Unexpected failure during ACL check, task creation, or other operation | Check gateway logs; report if recurring |

---

## KV / Checkpoint Errors

KV and checkpoint failures are returned in `KVResponse` and `CheckpointResponse` messages respectively, not as `ErrorResponse` codes. Inspect the `error` field in those response messages for details.

---

## Notes

- All error codes are stable across patch releases within a major version. New codes may be added in minor releases; clients should handle unknown codes gracefully.
- The `retryable` flag is authoritative. When `false`, retrying the same request without modification will not succeed.
- When `retry_after_ms` is present and positive, clients should wait at least that many milliseconds before retrying.
- SDKs map several of these codes to typed exceptions (e.g., `DuplicateIdentityError`, `PermissionDeniedError`, `RateLimitedError`). Refer to the SDK documentation for the exception hierarchy.
