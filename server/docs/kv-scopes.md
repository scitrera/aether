# Aether KV Store Scopes

Aether's KV store exposes an **8-cell scope matrix** combining two orthogonal axes: **identity scope** (global/workspace/user/user-workspace) and **sharing** (shared/exclusive). This document explains the matrix, access control, privileged-data conventions, and guarded counter operations.

## Overview: The 4×2 Matrix

KV scopes determine where data lives in Redis and who can access it. The matrix combines:

- **Identity Scope** (rows): Global, workspace, user, user-workspace
- **Sharing** (columns): Shared (cross-agent) or exclusive (per-agent)

| Identity Scope | Shared | Canonical Name | Redis Key Shape | ConfigSnapshot | Use Case |
|---|---|---|---|---|---|
| Global | Yes | `global` | `kv:global` | No | Tenant-wide shared data (config, constants) |
| Global | No | `global-exclusive` | `kv:agent:{impl}\|{spec}:global` | Yes | Per-agent tenant-wide state |
| Workspace | Yes | `workspace` | `kv:ws:{workspace}` | No | Workspace-shared data (settings, policies) |
| Workspace | No | `workspace-exclusive` | `kv:agent:{impl}\|{spec}:ws:{workspace}` | Yes | Per-agent workspace state |
| User | Yes | `user-shared` | `kv:user:{uid}` | No | Cross-agent per-user data (by OBO only) |
| User | No | `user` | `kv:agent:{impl}\|{spec}:user:{uid}` | No | Per-agent per-user state |
| User-Workspace | Yes | `user-workspace-shared` | `kv:user:{uid}:ws:{workspace}` | No | Cross-agent per-user-per-workspace (by OBO only) |
| User-Workspace | No | `user-workspace` | `kv:agent:{impl}\|{spec}:user:{uid}:ws:{workspace}` | No | Per-agent per-user-per-workspace state |

### Key Properties

**Shared scopes** drop the agent identity prefix—every agent in the same tenant or workspace sees the same Redis namespace. This is how agents rendezvous on shared data.

**Exclusive scopes** embed the agent identity (`impl|spec`) in the Redis key. Storage layout automatically isolates one agent's data from another's, enabling fast-path ACL bypass.

**ConfigSnapshot eager-load**: Only `global-exclusive` and `workspace-exclusive` are populated at connection time via `sendBaselineConfig()`. All other scopes are queried on demand.

## Access Control Model

KV access is checked in two tiers:

1. **Key-level rule** (`kv_key/{key}`) — if an explicit rule exists, use it; otherwise fall through.
2. **Scope-level rule** (`kv_scope/{scope}`) — default policy for the scope.

See `/server/internal/gateway/kv_handler.go:checkKeyPermission()` for implementation.

### Owner Fast-Path

For **exclusive scopes** with direct authority (no On-Behalf-Of), the gateway skips the ACL database entirely. The storage layout already isolates the namespace to the caller's `agent:{impl}|{spec}` segment — ownership is implicit. Cross-agent access via OBO still requires an explicit ACL grant.

```go
// Fast-path for caller's own exclusive scope
if scope.IsExclusive() && authority == nil {
    return nil  // ALLOW — ownership by storage layout
}
```

### Default Fallback Policies

Two layers of "default" exist:

1. **Wildcard `acl_rules` rows** — concrete rules naming `principal_id='_any_authenticated'`. Used to lock down specific scope/key combinations to default-deny (e.g., the new shared user scopes ship with these — see `server/migrations/019_kv_new_scope_fallbacks.sql`).
2. **`acl_fallback_policies` rows** — keyed by `RuleCategory()` (`{principal_type}_{resource_type}` such as `agent_kv_scope`, `task_kv_scope`, `service_kv_scope`). The DB is consulted when no explicit `acl_rules` row matches; the fallback decides the default access level for that whole category.

Default behavior in this revamp:

- **`user-shared` and `user-workspace-shared`**: DEFAULT-DENY (wildcard rule from migration 019). Cross-agent reads of per-user data are privacy-sensitive. Requires explicit grant or OBO authority.
- **`global-exclusive` and `workspace-exclusive`**: No explicit fallback rule. The owner fast-path covers self-access; cross-agent requires OBO.
- **Legacy scopes** (`global`, `workspace`, `user`, `user-workspace`): Unchanged fallback behavior — `agent_kv_scope` / `task_kv_scope` / `service_kv_scope` default to READ-WRITE in development; tighten via `SetFallbackPolicy` for production.

#### Managing fallback policies via SDK

All three SDKs expose ACL admin operations on the standard client. The caller principal needs `admin/acl` (or `admin/*` globally) to mutate.

**Go:**
```go
import (
    pb "github.com/scitrera/aether/api/proto"
    "github.com/scitrera/aether/internal/acl"
)

// Read the current fallback for agents accessing kv_scope resources.
resp, err := client.ACL().GetFallbackPolicy(ctx, "agent_kv_scope")
fmt.Println(resp.FallbackPolicy.FallbackAccessLevel) // 20 (READ-WRITE) by default

// Tighten production: deny by default for service principals.
_, err = client.ACL().SetFallbackPolicy(ctx, &pb.ACLSetFallbackRequest{
    RuleCategory:        "service_kv_scope",
    FallbackAccessLevel: int32(acl.AccessNone),
})
```

**Python:**
```python
# Read fallback for the (agent, kv_scope) category.
resp = await client.acl_get_fallback_policy("agent_kv_scope")
print(resp.fallback_policy.fallback_access_level)  # 20 by default

# Tighten production: deny by default for service principals.
await client.acl_set_fallback_policy(
    rule_category="service_kv_scope",
    fallback_access_level=0,  # AccessNone
)
```

**TypeScript:**
```typescript
// Read fallback for the (agent, kv_scope) category.
const resp = await client.admin.getFallbackPolicy({ ruleCategory: "agent_kv_scope" });
console.log(resp.fallbackPolicy?.fallbackAccessLevel); // 20 by default

// Tighten production: deny by default for service principals.
await client.admin.setFallbackPolicy({
  ruleCategory: "service_kv_scope",
  fallbackAccessLevel: 0,  // AccessNone
});
```

## Privileged-Data Convention

For sensitive data in shared scopes (billing balances, API keys, OAuth tokens), use this three-layer pattern:

### 1. Key Prefix Convention

Choose a descriptive prefix and document it. Common prefixes in the scitrera platform:

- `billing:*` — Financial records (credits, debits, rates)
- `api_key:*` — OAuth tokens, integration secrets
- `credits:*` — Debit ledger entries

```go
// Example: store billing balance in user-shared scope
key := "billing:user-alice:balance"
value := "1000"  // cents
s.Set(ctx, agent, kv.ScopeUserShared, key, value, "alice", "ws-prod", 0)
```

### 2. Tight ACL Rules

Create wildcard ACL rules that DEFAULT-DENY by prefix, then grant only to specific principals. Use the SDK for live tenants; SQL is a convenient bootstrap for dev/seed migrations.

**Go SDK:**
```go
import pb "github.com/scitrera/aether/api/proto"

// Default DENY any read/write on billing:*
_, err := client.ACL().Grant(ctx, &pb.ACLGrantRequest{
    PrincipalType: "wildcard",
    PrincipalId:   "_any_authenticated",
    ResourceType:  "kv_key",
    ResourceId:    "billing:*",
    Permission:    "AccessNone",  // 0 — explicit deny
})

// Grant read-write to the billing service
_, err = client.ACL().Grant(ctx, &pb.ACLGrantRequest{
    PrincipalType: "agent",
    PrincipalId:   "billing-service|v1",
    ResourceType:  "kv_key",
    ResourceId:    "billing:*",
    Permission:    "AccessReadWrite",
})

// Inspect what's in place
resp, err := client.ACL().ListRules(ctx, &pb.ACLRuleFilter{
    ResourceType: "kv_key",
    ResourceId:   "billing:*",
})
for _, rule := range resp.Rules {
    fmt.Printf("%s/%s -> %s (level=%d)\n",
        rule.PrincipalType, rule.PrincipalId, rule.ResourceId, rule.AccessLevel)
}
```

**Python SDK:**
```python
# Default DENY on billing:* — uses the convenience method on the async client
await client.acl_grant(
    principal_type="wildcard",
    principal_id="_any_authenticated",
    resource_type="kv_key",
    resource_id="billing:*",
    permission="AccessNone",  # 0
)

# Grant read-write to the billing service
await client.acl_grant(
    principal_type="agent",
    principal_id="billing-service|v1",
    resource_type="kv_key",
    resource_id="billing:*",
    permission="AccessReadWrite",
)

# Inspect rules
resp = await client.acl_list_rules(resource_type="kv_key", resource_id="billing:*")
for rule in resp.rules:
    print(rule.principal_type, rule.principal_id, "->", rule.access_level)
```

**TypeScript SDK:**
```typescript
// Default DENY on billing:*
await client.admin.createACLRule({
  principalType: "wildcard",
  principalId: "_any_authenticated",
  resourceType: "kv_key",
  resourceId: "billing:*",
  permission: "AccessNone",
});

// Grant read-write to the billing service
await client.admin.createACLRule({
  principalType: "agent",
  principalId: "billing-service|v1",
  resourceType: "kv_key",
  resourceId: "billing:*",
  permission: "AccessReadWrite",
});

// Inspect rules
const resp = await client.admin.listACLRules({
  resourceType: "kv_key",
  resourceId: "billing:*",
});
for (const rule of resp.rules ?? []) {
  console.log(rule.principalType, rule.principalId, "->", rule.accessLevel);
}
```

<details>
<summary>SQL equivalent (deploy-time bootstrap)</summary>

```sql
-- Default DENY any read/write on billing:*
INSERT INTO acl_rules (principal_type, principal_id, resource_type, resource_id, access_level)
VALUES ('wildcard', '_any_authenticated', 'kv_key', 'billing:*', 0);

-- Grant read-write to the billing service
INSERT INTO acl_rules (principal_type, principal_id, resource_type, resource_id, access_level)
VALUES ('agent', 'billing-service|v1', 'kv_key', 'billing:*', 20);
```
</details>

### 3. At-Rest Encryption via `enc:` Prefix

The Store automatically encrypts values when the key starts with `enc:`:

```go
// Store encrypted API token
key := "enc:api_key:integration-slack"
value := "xoxp-1234567890-..."
s.Set(ctx, agent, kv.ScopeGlobalExclusive, key, value, "", "", 0)
// Value is encrypted in Redis, decrypted on retrieval
```

See `/server/internal/kv/store.go:encryptValue()` for details.

## On-Behalf-Of (OBO) Authority for Shared Access

When agent A needs to read a user's data via `user-shared` from agent B's storage rendezvous, agent A must hold an OBO authority grant naming the user as subject. The grant is attached to KV requests via `KVOperation.authorization` (see `api/proto/aether.proto` `AuthorizationContext`).

**Auto-OBO Promotion** (gateway-side): If the caller is a task whose grant lineage names the user, the gateway automatically promotes the task's grant to authority for the request—no explicit `AuthorizationContext` needed. See `/server/internal/gateway/routing.go:handleKVOp()` (lines 760-800 ish).

### Example: Agent B Reads Agent A's User Data

```
Agent A (user alice):
  → Stores "note:todo-list" in kv:user:alice via scope=user-shared
     (storage: key=kv:user:alice:note:todo-list)

Agent B (wants to read alice's notes):
  1. Holds OBO grant: { subject: alice, principal: agent-b|spec }
  2. Sends KVOperation.Get with authorization=grant
  3. Gateway checks:
     - scope=user-shared (cross-agent per-user)
     - authority names alice
     → Proceeds with ACL check using OBO authority
```

## Guarded Counter Operations: INCREMENT_IF / DECREMENT_IF

Two atomic counter operations with bound checks prevent race conditions in shared counters (especially billing debits).

### INCREMENT_IF

```go
func (s *Store) IncrementIf(
    ctx context.Context,
    agent models.Identity,
    scope KVScope,
    key string,
    userID, workspace string,
    delta, ceiling int64,
) (value int64, applied bool, error)
```

Increments counter by `delta` if the result would not exceed `ceiling`. Returns:
- `value`: Current counter value (after mutation if applied, unchanged if rejected)
- `applied`: `true` if mutation happened, `false` if guard rejected

**Semantics**: Useful for rate-limit counters that must not exceed a quota.

```go
// Increment request count, reject if it would exceed 1000
newCount, applied, err := s.IncrementIf(ctx, agent, kv.ScopeWorkspace,
    "request-count", "", "ws-prod", 1, 1000)
if applied {
    log.Info().Int64("count", newCount).Msg("request allowed")
} else {
    log.Warn().Msg("rate limit exceeded")
}
```

### DECREMENT_IF

```go
func (s *Store) DecrementIf(
    ctx context.Context,
    agent models.Identity,
    scope KVScope,
    key string,
    userID, workspace string,
    delta, floor int64,
) (value int64, applied bool, error)
```

Decrements counter by `delta` if the result would not drop below `floor`. Returns same structure as `IncrementIf`.

**Semantics**: Useful for billing debits that must not go negative.

```go
// Debit 25 cents, reject if balance would drop below 0
balance, applied, err := s.DecrementIf(ctx, agent, kv.ScopeUserShared,
    "billing:balance", "alice", "ws-prod", 25, 0)
if applied {
    log.Info().Int64("balance", balance).Msg("debit succeeded")
} else {
    log.Warn().Msg("insufficient balance")
}
```

### Implementation

Both operations are backed by Lua scripts in Redis (atomic) and BadgerDB transactions with retry-on-conflict (see `/server/internal/kv/store.go` and `/server/internal/kv/badger_store.go`). Under contention, the implementation retries up to 20 times before returning an error.

**No negative delta allowed**: Both operations reject negative deltas at the API level to prevent bypassing the guard by using the opposite operation.

```go
// This is rejected
_, _, err := s.IncrementIf(ctx, agent, scope, key, uid, ws, -5, 100)
// error: IncrementIf delta must be non-negative
```

## Naming Asymmetry Note

For wire-compatibility, the legacy enum values `USER` and `USER_WORKSPACE` kept their original semantics (per-agent / exclusive). The new `USER_SHARED` and `USER_WORKSPACE_SHARED` cover the cross-agent variants.

This asymmetry is intentional: changing tag numbers or default semantics would break old SDK clients. The canonical scope names reflect the actual semantics:

- `user` (enum: per-agent)
- `user-shared` (enum: cross-agent)
- `user-workspace` (enum: per-agent)
- `user-workspace-shared` (enum: cross-agent)

## SDK Usage Patterns

### Go SDK

```go
import "github.com/scitrera/aether/internal/kv"

// Shared workspace config (read by any agent)
config, err := store.Get(ctx, agent, kv.ScopeWorkspace, "app-config", "", "ws-prod")

// Per-agent workspace state
state, err := store.Get(ctx, agent, kv.ScopeWorkspaceExclusive, "state", "", "ws-prod")

// Cross-agent per-user data (via OBO authority)
notes, err := store.Get(ctx, agent, kv.ScopeUserShared, "note:todos", "alice", "ws-prod")

// Guarded billing debit
balance, applied, err := store.DecrementIf(ctx, agent, kv.ScopeUserShared,
    "billing:balance", "alice", "ws-prod", 100, 0)
```

### Python SDK

```python
from aether import kv

# Shared workspace config
config = store.get(agent, kv.KVScope.WORKSPACE, "app-config", "", "ws-prod")

# Per-agent workspace state
state = store.get(agent, kv.KVScope.WORKSPACE_EXCLUSIVE, "state", "", "ws-prod")

# Cross-agent per-user data (via OBO authority)
notes = store.get(agent, kv.KVScope.USER_SHARED, "note:todos", "alice", "ws-prod")

# Guarded billing debit
balance, applied = store.decrement_if(agent, kv.KVScope.USER_SHARED,
    "billing:balance", "alice", "ws-prod", 100, 0)
```

### TypeScript SDK

```typescript
import { KVScope } from '@scitrera/aether';

// Shared workspace config
const config = await store.get(agent, KVScope.WORKSPACE, "app-config", "", "ws-prod");

// Per-agent workspace state
const state = await store.get(agent, KVScope.WORKSPACE_EXCLUSIVE, "state", "", "ws-prod");

// Cross-agent per-user data (via OBO authority)
const notes = await store.get(agent, KVScope.USER_SHARED, "note:todos", "alice", "ws-prod");

// Guarded billing debit
const [balance, applied] = await store.decrementIf(agent, KVScope.USER_SHARED,
    "billing:balance", "alice", "ws-prod", 100, 0);
```

## Testing Guarded Counters

The KV store includes comprehensive tests for guard semantics and concurrency:

```bash
cd /home/drew/scitrera-aether3-go/oss-repo/server
go test ./internal/kv -run TestIncrementIf -v
go test ./internal/kv -run TestDecrementIf -v
go test ./internal/kv -run TestDecrementIf_ConcurrentRace -v
```

Key test scenarios:
- Guard allows mutation (below ceiling, above floor)
- Guard rejects mutation (at/above ceiling, at/below floor)
- Concurrent mutations respect guard under contention
- Negative delta is rejected

## References

- **Core namespace logic**: `/server/internal/kv/namespace.go`
- **ACL checking**: `/server/internal/gateway/kv_handler.go:checkKeyPermission()`
- **Store implementation**: `/server/internal/kv/store.go`
- **BadgerDB implementation**: `/server/internal/kv/badger_store.go`
- **Baseline config at connect**: `/server/internal/gateway/routing.go:sendBaselineConfig()`
- **ACL fallback policies**: `/server/migrations/019_kv_new_scope_fallbacks.sql`
- **Integration tests**: `/server/internal/gateway/connect_test.go` (sendBaselineConfig tests)
- **Counter tests**: `/server/internal/kv/if_counters_test.go`
