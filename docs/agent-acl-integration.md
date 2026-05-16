# Agent ACL Integration

> Phase 5 of the Aether enterprise-agentic-fabric upgrade adds a centralized,
> opt-in ACL surface where agents declare the resource types they own and the
> permission verbs that apply. Aether routes `CheckAccess` decisions through
> those declarations to enrich the audit log with the owning agent's identity.

## Why

Before Phase 5, every agent invented its own ad-hoc ACL. There was no canonical
place to record which agent "owns" which resource family, and audit rows carried
no attribution to the agent whose schema covered the access.

Phase 5 adds three things:

1. **ResourceSchema** — each agent registration declares the resource-type
   prefixes it owns and the permission verbs it recognizes.
2. **Uniqueness enforcement** — no two active registrations may declare
   overlapping prefixes. Aether enforces this transactionally at registration
   time.
3. **Audit attribution** — when `CheckAccess` is called for any resource_type,
   the gateway looks up which agent declared the matching prefix and stamps the
   owning agent's identity into the audit row's metadata bag.

The result is a single audit log that carries the full attribution chain for
every access check: who called, what they accessed, and which agent owns that
resource family.

---

## Concepts

### AgentRegistration.ResourceSchema

A list of resource families the agent owns. Each entry has three fields:

| Field | Required | Description |
|-------|----------|-------------|
| `resource_type_prefix` | yes | A prefix that covers the agent's resource family, e.g. `chat/` or `docmgmt/document`. All resource_types that start with this prefix (after slash-boundary matching) are attributed to this agent. |
| `permission_verbs` | no | Verbs the agent recognizes for this family, e.g. `["read", "write", "admin"]`. Informational today; used by the Phase 6 AgentCard generator. |
| `resource_id_schema` | no | Optional JSON Schema string describing the shape of resource IDs within this family. Also informational today. |

**Prefix matching semantics**: Aether uses longest-match, right-to-left slash
boundary resolution. For `resource_type="docmgmt/document/abc"` the lookup
walks:

```
docmgmt/document/   → exact match wins (if declared)
docmgmt/document    → exact match (trailing-slash variant)
docmgmt/            → parent prefix
docmgmt             → bare parent
```

The first hit wins. Exact matches always beat parent prefixes because the
right-to-left walk finds longer candidates first.

### Uniqueness

No two active registrations may declare the same (or overlapping) prefix.
Aether enforces this transactionally on every `Register` and `Update`
operation. Attempting to claim an already-owned prefix fails with a
`ERR_PREFIX_CONFLICT:` error code on the wire (see
[Wire error codes](#wire-error-codes) below).

Registrations without any `resource_schema` entries skip the uniqueness check
entirely — backward-compatible with pre-Phase-5 agents.

### AgentRegistration.Capabilities

A map of capability flag names to booleans. Informational today; Phase 6 will
use these to construct AgentCards for external A2A interop.

Example values: `{"streaming": true, "hibernation_aware": true}`.

### AgentRegistration.Extensions

A list of URI strings indicating extensions the agent supports, e.g.
`["https://a2a-protocol.org/extensions/v1/threading"]`. Also informational
today (Phase 6+).

---

## Registering an agent's resource schema (Python SDK)

```python
from scitrera_aether_client import make_resource_schema_entry

client.register_agent(
    implementation="chat-agent",
    profile="docker",
    description="Conversational chat agent",
    resource_schema=[
        make_resource_schema_entry(
            resource_type_prefix="chat/conversations",
            permission_verbs=["read", "write", "admin"],
        ),
        make_resource_schema_entry(
            resource_type_prefix="chat/messages",
            permission_verbs=["read", "write"],
        ),
    ],
    capabilities={"streaming": True, "hibernation_aware": True},
    extensions=["https://a2a-protocol.org/extensions/v1/threading"],
)
```

If another agent already owns `chat/conversations` (or any prefix that
overlaps), the call fails:

```
ERR_PREFIX_CONFLICT: resource_type_prefix "chat/conversations" already
declared by agent "other-chat-agent"
```

### Wire error codes

The `ERR_PREFIX_CONFLICT:` prefix is prepended to the human-readable error
message in `AgentResponse.error`. SDK callers that need to distinguish a prefix
conflict from other registration errors should check:

```python
if response.error.startswith("ERR_PREFIX_CONFLICT:"):
    # Handle conflict
```

A future proto revision may add a typed error field; the string prefix is the
stable surface for now.

---

## Updating an agent's resource schema

Pass the full updated `resource_schema` to `update_agent`. The uniqueness check
treats the update as "release all old prefixes owned by this agent, then claim
the new set". This means:

- Narrowing the schema (removing a prefix) immediately releases it for other
  agents to claim.
- Expanding the schema (adding a prefix) runs the conflict check against all
  other active registrations.
- Re-registering with an empty schema clears all prefix claims.

```python
# Release "chat/conversations", keep "chat/messages".
client.update_agent(
    implementation="chat-agent",
    resource_schema=[
        make_resource_schema_entry(
            resource_type_prefix="chat/messages",
            permission_verbs=["read", "write"],
        ),
    ],
)
```

---

## How CheckAccess attribution works

When the gateway's ACL service receives a `CheckAccess` call it:

1. Evaluates the decision (allow/deny) via the Casbin enforcer and fallback
   policies — this is unchanged from pre-Phase-5.
2. Queries the **PrefixIndex** (an in-memory map, rebuilt at gateway startup
   and updated on every Register/Delete) for the `resource_type` argument.
3. If a match is found, the implementation name and matched prefix are passed
   to `LogDecisionWithAttribution`, which stamps them into the audit event's
   metadata bag before enqueuing it on the shared audit writer.

The decision itself (allow/deny) is never influenced by the prefix
attribution — it is advisory metadata only.

### Lookup algorithm

Given `resource_type`:

1. Exact match: is `resource_type` itself a key in the index? → hit.
2. Trailing-slash variant: if `resource_type` ends in `/`, try without it (and
   vice versa) → hit.
3. Walk right-to-left along `/` boundaries, trying `head/` and `head` at each
   step. The first hit is the longest match.
4. No match → attribution fields are omitted from the audit row.

---

## Audit log shape

When attribution is available, the audit row's `metadata` JSON bag carries two
extra keys:

```json
{
  "decision": "ALLOW",
  "access_level": 2,
  "fallback_applied": false,
  "owning_agent": "chat-agent",
  "owning_agent_prefix": "chat/messages",
  ...other existing metadata fields...
}
```

| Key | Type | Description |
|-----|------|-------------|
| `owning_agent` | string | The `implementation` name of the agent whose declared prefix covers this access. |
| `owning_agent_prefix` | string | The literal prefix string from that agent's `resource_schema` that matched. |

These keys are absent when no prefix covers the accessed resource_type
(pre-Phase-5 agents, or resource types outside any declared family).

---

## Querying audit attribution

### SQLite (AetherLite)

```sql
-- All access checks attributed to "chat-agent":
SELECT
    timestamp,
    actor_type,
    actor_id,
    resource_type,
    resource_id,
    operation,
    json_extract(metadata, '$.decision') AS decision,
    json_extract(metadata, '$.owning_agent') AS owning_agent,
    json_extract(metadata, '$.owning_agent_prefix') AS owning_agent_prefix
FROM comprehensive_audit_log
WHERE event_type = 'authorization'
  AND json_extract(metadata, '$.owning_agent') = 'chat-agent'
ORDER BY timestamp DESC
LIMIT 100;
```

```sql
-- Access checks across all agents in the "chat/" family:
SELECT
    timestamp,
    actor_id,
    resource_type,
    json_extract(metadata, '$.decision') AS decision,
    json_extract(metadata, '$.owning_agent') AS owning_agent
FROM comprehensive_audit_log
WHERE event_type = 'authorization'
  AND json_extract(metadata, '$.owning_agent') IS NOT NULL
ORDER BY timestamp DESC;
```

### PostgreSQL (full mode)

```sql
-- All access checks attributed to "chat-agent":
SELECT
    timestamp,
    actor_type,
    actor_id,
    resource_type,
    resource_id,
    operation,
    metadata->>'decision' AS decision,
    metadata->>'owning_agent' AS owning_agent,
    metadata->>'owning_agent_prefix' AS owning_agent_prefix
FROM comprehensive_audit_log
WHERE event_type = 'authorization'
  AND metadata->>'owning_agent' = 'chat-agent'
ORDER BY timestamp DESC
LIMIT 100;
```

```sql
-- Access checks across all registered prefixes, grouped by agent:
SELECT
    metadata->>'owning_agent' AS agent,
    metadata->>'owning_agent_prefix' AS prefix,
    metadata->>'decision' AS decision,
    COUNT(*) AS check_count
FROM comprehensive_audit_log
WHERE event_type = 'authorization'
  AND metadata->>'owning_agent' IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY check_count DESC;
```

---

## Operational concerns

### Multi-gateway deployments

The **PrefixIndex** is per-gateway: each gateway instance rebuilds it from the
registry DB at startup and updates it incrementally on every Register/Delete
that arrives at that gateway. Cross-gateway propagation is not yet implemented.

Operators running multi-gateway deployments should plan for a brief
attribution-lag window after a registration change: another gateway will carry
stale attribution data until it restarts or processes the next Register/Delete
on its own connection. Access decisions themselves are always correct — only
the audit attribution metadata may lag.

Expected lag: seconds to minutes, bounded by the next gateway restart or
registration event on that instance. A future phase will add a broadcast hook.

### Empty schema is fine

Registrations without any `resource_schema` entries:
- Skip the uniqueness check entirely.
- Produce audit rows without `owning_agent` attribution.
- Are fully backward-compatible with pre-Phase-5 behavior.

Existing agents that have not been updated to declare a schema continue to work
unchanged.

### Capabilities and Extensions are informational today

`capabilities` and `extensions` fields are stored and round-trip through the
proto, but are not acted upon by the gateway in Phase 5. Phase 6 will use them
to generate **AgentCards** for external A2A (Agent-to-Agent) protocol interop.
Populating them now ensures agents are ready for Phase 6 without a separate
re-registration step.

---

## Python SDK helper reference

### `make_resource_schema_entry`

```python
from scitrera_aether_client import make_resource_schema_entry

entry = make_resource_schema_entry(
    resource_type_prefix="chat/conversations",   # required
    permission_verbs=["read", "write", "admin"], # optional
    resource_id_schema='{"type":"string"}',      # optional JSON Schema string
)
```

Returns an `AgentResourceSchemaEntry` proto message suitable for inclusion in
the `resource_schema` list passed to `register_agent` / `update_agent`.

### `register_agent` / `update_agent` keyword arguments

| Kwarg | Type | Description |
|-------|------|-------------|
| `resource_schema` | `list[AgentResourceSchemaEntry]` | Resource families this agent owns. |
| `capabilities` | `dict[str, bool]` | Capability flags. |
| `extensions` | `list[str]` | Extension URIs. |

All three are optional and default to empty/None, which preserves pre-Phase-5
behavior.

---

## Future work (deferred from Phase 5)

- **Late agent self-registration on connect**: today agents are
  admin-registered before they connect. A future phase will allow agents to
  declare their schema on `InitConnection` so the PrefixIndex can be
  bootstrapped from live connections.
- **Cross-gateway PrefixIndex broadcast**: propagate Register/Delete events to
  all gateways in a cluster so attribution is consistent across instances
  without requiring a restart.
- **First-class `owning_agent` column** on `comprehensive_audit_log`: the
  current shape stores attribution in the metadata JSON bag. Promoting it to a
  dedicated column enables cheaper SQL filtering without `json_extract`.
- **AgentCard generation** from ResourceSchema + Capabilities + Extensions
  (Phase 6): the A2A-protocol AgentCard format will be auto-generated from a
  registration's declared schema for external agent discovery.

---

## See also

- [aetherlite.md](aetherlite.md) — single-binary deployment mode; all ACL and
  audit features described here are fully supported in AetherLite.
- [on-behalf-of-acl-design.md](on-behalf-of-acl-design.md) — authority grant
  (on-behalf-of delegation) design, which intersects with the attribution
  chain in multi-hop access scenarios.
- [audit_testing_guide.md](audit_testing_guide.md) — guidance for writing tests
  against the audit pipeline, including the flush mechanism used by the Stage C
  gateway integration tests.
