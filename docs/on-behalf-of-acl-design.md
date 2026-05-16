# On-Behalf-Of ACL And Authority Grants

## Status

Phase 0 foundation is implemented. Phase 1 core request-time enforcement is implemented for `SendMessage`, `KVOperation`, `CreateTaskRequest`, and comprehensive `AuditQuery`.

Implemented foundation:

- canonical principal references for ACL/audit/grants
- first-class `Service` principal support
- authority grant persistence with lineage, bounded transitivity, lifecycle binding, renewable leases, and scope fields
- actor/subject/root-subject/grant-lineage audit columns and logging

Implemented phase 1 enforcement:

- `AuthorizationContext` on message send, KV, task creation, and audit query
- request-time `on_behalf_of` resolution against persisted authority grants
- subject ACL evaluation intersected with grant constraints
- actor/subject/root-subject/grant lineage recorded in comprehensive audit events

Implemented admin lifecycle surface:

- authority-grant create/list/get/renew/revoke over the ACL admin REST API
- authority-grant create/list/get/renew/revoke over the streaming gRPC `ACLOperation` surface

Implemented runtime lifecycle surface:

- non-admin streaming `AuthorityGrantOperation` with `EXCHANGE`, `DERIVE`, `GET`, `RENEW`, and `REVOKE`
- trusted-session exchange backed by active session lookup in the gateway session registry
- focused TypeScript and Python client support for runtime and admin authority-grant operations
- audit events for grant exchange, derivation, inspection, renewal, and revocation

Implemented orchestration propagation:

- task creation now mints task authority grants when `CreateTask` runs on behalf of a subject
- targeted and self-assigned tasks receive root grants bound to the assignee's `agent` audience
- pool tasks receive task-bound anchor grants and derive assignee-specific child grants on delivery
- queued and immediate delivery paths now attach usable task grant metadata to `TaskAssignment`
- task completion, failure, and cancellation revoke the task's root authority grant tree
- task records now persist first-class authority lineage fields alongside compatibility metadata
- task/agent liveness can renew task grants conservatively within the existing lease window and `renewable_until`

Still pending after this phase:

- richer task/admin query surfacing for first-class task authority fields
- compatibility cleanup for legacy delegation-chain APIs

## Summary

The current ACL model is workspace-centric and can optionally consult a stored delegation chain for a derived principal. That is not sufficient for the next stage of Aether:

- a websocket or app backend needs to stay connected as itself while acting for the currently logged-in user
- some legacy paths will continue to flow through a system/service connection during migration
- automated workers may need to act for multiple users concurrently
- audit must always answer both "which authenticated actor performed the action" and "whose authority was exercised"

The target model is:

- every connection authenticates as exactly one actor principal
- any privileged operation may optionally include an `AuthorizationContext`
- `AuthorizationContext` may carry an `on_behalf_of` subject plus a server-issued authority grant
- effective authorization is computed from the subject's ACL, constrained by the grant
- audit always records actor, subject, root subject, and grant lineage

`on_behalf_of` becomes the primary request-time model. Delegation chains become a derived lineage/compatibility concept, not the primary enforcement primitive.

## Goals

- Support trusted backend services acting for users without losing actor identity.
- Support multi-hop automation where child workers act for one or more users.
- Support multiple active user grants on one worker/session.
- Keep grants renewable, audience-bound, revocable, and short-lived.
- Preserve direct user connections as the simplest path for new code.
- Keep authorization decisions queryable and explainable in audit logs.
- Avoid overloading existing bridge behavior for service/backend use cases.

## Non-Goals

- Hidden impersonation where the actor disappears from audit.
- Long-lived bearer-style delegation with no audience binding.
- Reusing the current delegation-chain table as the long-term enforcement model.
- Making all admin/ACL mutation operations available through `on_behalf_of` in phase 1.

## Key Decisions

### 1. Introduce A First-Class `Service` Principal

Aether should add a new workspace-less `Service` principal for app/websocket backends and similar trusted platform services.

Why:

- this matches the actual use case better than `Bridge`
- it avoids overloading bridge semantics with backend application semantics
- it gives ACL, audit, auth, and SDK code a clean type for trusted intermediaries

Proposed identity shape:

- `implementation`
- `specifier`

Proposed canonical identity string:

- `sv::{implementation}::{specifier}`

Proposed behavior:

- may connect as itself
- may act directly under its own ACL
- may act for a user or other subject only via `AuthorizationContext` + valid authority grant
- may hold multiple active grants concurrently

## 2. Make `on_behalf_of` A Request-Time Authorization Context

Connection-time ACL remains actor/session admission.

Per-operation authorization becomes:

- direct actor authorization when no `AuthorizationContext` is present
- subject-constrained authorization when `AuthorizationContext` is present

This is the right fit for:

- one service connection acting for many users
- one worker handling multiple user-driven tasks
- mixed direct and delegated behavior on the same connection

## 3. Use Persisted, Server-Issued Authority Grants

Authority is represented by a persisted server-side grant, not by a client-synthesized chain.

Why:

- revocation is straightforward
- renewal is straightforward
- audience binding is enforceable
- parent/child lineage is queryable
- grants can be used across process boundaries without trusting the caller to mint authority

Opaque export tokens may be added later, but persisted grants remain the source of truth.

## 4. All Grants Are Bounded And Renewable

Every grant must have:

- `expires_at`
- `renewable_until`
- explicit audience binding
- explicit workspace/resource/operation scope

Renewal is required because some legitimate flows will outlive a very short TTL. The system should support short active TTLs with bounded renewal instead of long-lived open-ended grants.

## 5. One Actor May Hold Multiple Grants

This is required for the "one agent acting for three users" case.

The actor remains the authenticated connection identity. Each operation selects the grant and subject it is using. Grants are not stored as a single derived identity override on the connection.

## 6. Trusted Services Use Exchange APIs, Not Local Minting

A trusted service may exchange an authenticated user context for a bounded authority grant issued by Aether. Services do not mint grants locally.

This keeps the source of authority inside the control plane and keeps audit, revocation, and renewal coherent.

### 6.1 Trust Model: `capability/exchange_authority_grants` Is User-Impersonation

`capability/exchange_authority_grants` is a **user-impersonation capability**. A service holding it can mint a grant where `subject` is a user whose session is currently active in this Aether tenant. Aether validates only that:

1. the service holds the capability (broad rule, OR a narrow rule scoped to subject workspace + canonical id), AND
2. the supplied `source_session_id` resolves to an active user session.

Aether does **not** verify that the user authorized the service to act on their behalf — the trust model assumes the service has independently authenticated the user (e.g., a WebSocket server validating a browser session cookie). This is the standard RFC 8693 trusted-intermediary pattern.

**Operational guidance:**

- Treat `capability/exchange_authority_grants` as if it were "may impersonate any active user in this tenant" when granted as the broad rule (`(svc::X, capability, capability/exchange_authority_grants, AccessManage)`).
- Prefer the narrow rule form: `(svc::X, capability, capability/exchange_authority_grants/{subject_workspace}/{subject_canonical_id_pattern}, AccessManage)`. Subject workspace and canonical id support glob (`*`, `?`). The gateway tries the narrow rule first and falls back to the broad rule, so granular operators can lock down without a coordinated migration.
- Trusted exchanges (those using `source_session_id`) are forced to track the source session lifecycle: `valid_while_audience_active=true` is set server-side regardless of the request value, so the grant cannot outlive the user's session even if RENEW is called.
- Grant a service the broad capability only if you accept that any compromise of that service entails impersonation of any user in the tenant. For multi-application tenants, prefer narrow rules per (service, subject-population).

### 6.2 Future Hardening (post-v1.0)

The v1.0 model is "trusted intermediary without proof-of-possession." Stronger structural defenses planned for v1.1+:

- **Issuer-of-session binding** — store `created_by_actor` in the session record so only the connection that minted a user's session can EXCHANGE for it.
- **Time-window the EXCHANGE** — only allow EXCHANGE within N seconds of source-session creation (configurable).
- **Subject-side discoverability** — `LIST_GRANTS_ON_ME` (already shipping) plus `REVOKE_GRANT_ON_ME` so users can audit and revoke active OBO grants on themselves.

## 7. Audit Must Carry Actor And Subject Separately

Every relevant audit row should carry:

- `actor_type`
- `actor_id`
- `subject_type`
- `subject_id`
- `root_subject_type`
- `root_subject_id`
- `grant_id`
- `parent_grant_id`
- `authority_mode` (`direct` or `on_behalf_of`)

Without this split, Aether cannot reliably answer "who did what on whose behalf."

## Current Problems In The Existing Model

The current delegation-chain approach has structural issues that make it unsuitable as the primary long-term model:

- chains are looked up by derived principal, not by operation context
- one derived principal effectively gets one current chain, which breaks concurrent multi-user use
- chains are not audience-bound to session/task/worker
- chains are not renewable
- chains are not propagated through task creation and orchestration as first-class authorization context
- audit records the ACL decision plus optional chain ID, but not a first-class actor/subject split
- identity handling is inconsistent for agents/tasks because some code uses canonical identity strings and some uses `Identity.ID`

These issues are not patch-level problems. They point to the wrong core abstraction for the target use cases.

## Target Data Model

### Principal Reference

Introduce a canonical principal reference structure used consistently in ACL, grants, audit, and protobufs.

Proposed fields:

- `principal_type`
- `principal_id`

Rules:

- `principal_id` is always the canonical string identity used by ACL and audit
- do not use bare `Identity.ID` for agents, unique tasks, services, bridges, or orchestrators

### Authority Grants Table

Add a new table, for example `acl_authority_grants`.

Suggested columns:

- `grant_id UUID PRIMARY KEY`
- `root_grant_id UUID`
- `subject_type VARCHAR`
- `subject_id VARCHAR`
- `delegate_type VARCHAR`
- `delegate_id VARCHAR`
- `issued_by_type VARCHAR`
- `issued_by_id VARCHAR`
- `root_subject_type VARCHAR`
- `root_subject_id VARCHAR`
- `parent_grant_id UUID NULL`
- `may_delegate BOOLEAN`
- `remaining_hops INTEGER`
- `workspace_scope JSONB`
- `resource_scope JSONB`
- `operation_scope JSONB`
- `max_access_level INTEGER`
- `audience_type VARCHAR`
- `audience_id VARCHAR`
- `valid_while_audience_active BOOLEAN`
- `expires_at TIMESTAMP`
- `renewable_until TIMESTAMP`
- `renewed_at TIMESTAMP NULL`
- `revoked BOOLEAN`
- `revoked_at TIMESTAMP NULL`
- `reason TEXT`
- `metadata JSONB`
- `created_at TIMESTAMP`

Suggested audience types:

- `session`
- `task`
- `agent`
- `service`

The current implementation uses this richer shape so transitive derivation, task-bound lifetime, and future Casbin integration can evolve without another schema rewrite.

Suggested indexes:

- active grants by delegate
- active grants by subject
- active grants by audience
- parent-child lookup
- expiry cleanup

### Task Lineage Fields

Task records should explicitly carry execution authority lineage instead of only creator/parent identity.

Add fields such as:

- `creator_actor_id`
- `subject_id`
- `root_subject_id`
- `authority_grant_id`
- `parent_authority_grant_id`

This keeps task inspection and recovery aligned with the authorization model.

## Protobuf And API Changes

### New Shared Messages

Add a canonical `PrincipalRef` message and an `AuthorizationContext` message.

Suggested shape:

```protobuf
message PrincipalRef {
  string principal_type = 1;
  string principal_id = 2;
}

message AuthorizationContext {
  string authority_mode = 1;          // "direct" or "on_behalf_of"
  PrincipalRef subject = 2;           // required for on_behalf_of
  string grant_id = 3;                // required for on_behalf_of
}
```

The actor is never taken from the request body. The actor always comes from the authenticated connection/session.

### Phase 1 Operation Surfaces

Add optional `AuthorizationContext` to:

- `SendMessage`
- `CreateTaskRequest`
- `KVOperation`
- audit query/view operations

For audit, phase 1 should split the current behavior into two categories:

- `AdminAuditQuery`: existing admin/system path, not `on_behalf_of`
- `OperationalAuditQuery`: subject-capable read path, usable with `on_behalf_of`

This avoids accidentally turning the current admin audit endpoint into a user-scoped delegated endpoint.

Current implementation detail:

- the existing comprehensive `AuditQuery` request now accepts `AuthorizationContext`
- direct/system callers still use the existing direct/admin path
- `on_behalf_of` callers use the same endpoint, but authorization is evaluated through the subject ACL plus grant constraints
- filter/query fields now include `subject_type`, `subject_id`, `authority_mode`, and `authority_grant_id`

### Grant Management APIs

Add service methods / admin methods for:

- `ExchangeAuthorityGrant`
- `RenewAuthorityGrant`
- `RevokeAuthorityGrant`
- `GetAuthorityGrant`
- `ListAuthorityGrants`

Current implementation status:

- `RenewAuthorityGrant`, `RevokeAuthorityGrant`, `GetAuthorityGrant`, and `ListAuthorityGrants` are implemented in the ACL service and exposed through admin REST and gRPC ACL operations
- generic admin-side `CreateAuthorityGrant` is implemented
- runtime `AuthorityGrantOperation` is implemented over the ordinary streaming connection with:
  - `EXCHANGE` for user self-exchange or trusted actor exchange from an active user session
  - `DERIVE` for child-grant issuance from a currently valid parent grant
  - `GET`, `RENEW`, and `REVOKE` for visible grants
- trusted exchange from `source_session_id` requires `capability/exchange_authority_grants`
- `workspace_scope` accepts the magic value `_subject_workspaces` (constant `acl.WorkspaceScopeSubjectInherited`) to explicitly inherit the workspace decision from the subject's own ACL — preferred over a bare `["*"]` because it documents intent and audit dashboards can distinguish it from accidentally over-broad wildcards. The subject's own ACL still gates each request, so the security ceiling is unchanged.
- runtime exchange is intentionally narrower than admin-side `CreateAuthorityGrant`; it always issues to the current actor and binds exchange audiences to the current actor context

Recommended issuance flows:

- user session -> self grant, then derive child grants as needed
- trusted service -> exchange from active user session to service grant
- trusted service/worker -> child grant for task/agent/service, derived from parent grant

## Enforcement Model

### Direct Mode

If `AuthorizationContext` is absent:

1. authenticate actor connection
2. evaluate actor ACL against the requested resource/operation
3. audit with `authority_mode=direct`

### On-Behalf-Of Mode

If `AuthorizationContext` is present:

1. authenticate actor connection
2. resolve and load the grant by `grant_id`
3. verify `grant.delegate == actor`
4. verify grant is active and not expired
5. verify audience binding matches the current session/task/agent
6. verify requested operation/resource/workspace is within grant scope
7. evaluate the subject's current ACL against the requested resource/operation
8. cap the result by `grant.max_access_level`
9. authorize if the capped result satisfies the required level
10. audit with actor + subject + root subject + grant lineage

Suggested effective access formula:

- `effective_access = min(subject_acl_level, grant.max_access_level)`

The actor's own resource ACL is not intersected into the subject decision. The actor's authority is represented by being the named delegate on a valid grant. This keeps the model understandable and prevents service ACLs from unexpectedly shrinking subject rights after a grant has already been issued.

### Casbin Split

Casbin remains the base ACL engine. Authority grants are enforced in Aether's service layer.

Request-time evaluation is:

1. resolve grant and validate lifecycle state
2. validate actor/delegate match and audience binding
3. validate grant scope constraints
4. evaluate the subject's ACL through Casbin
5. allow only if both the grant envelope and Casbin ACL permit the action

This keeps grant lifecycle, derivation, renewal, and revocation logic out of Casbin while still using Casbin for the underlying policy evaluation.

## Renewal Model

Short TTL is acceptable only if renewal is built in.

Recommended behavior:

- grants have a short `expires_at`
- grants may be renewed until `renewable_until`
- only the current delegate or Aether itself may renew
- audience binding must remain unchanged on renewal
- renewal is denied if the parent grant is no longer valid
- revocation of a parent grant cascades to all child grants

Suggested defaults:

- service-facing grants: short active TTL, renewable for the duration of the user session
- task/worker child grants: short active TTL, renewable for the duration of the task

Task-bound grants should also support lifecycle binding independent of session lifetime:

- a task-execution grant may outlive the initiating session
- the grant remains valid only while the bound task is active
- task completion/failure/cancelation revokes the task grant and descendants immediately

## Orchestration Model

The existing task token remains an actor-authentication primitive. It proves that an agent or worker may connect as itself. It should not also carry the subject's authorization.

The new model is:

- task token authenticates the launched actor identity
- authority grant carries the subject authority
- child grants are minted when work is assigned or launched

Example:

1. user `alice` authorizes service `sv.frontend.api-1`
2. service creates task on behalf of `alice` using grant `G1`
3. Aether stores task with actor `sv.frontend.api-1`, subject `alice`, grant `G1`
4. when a worker is assigned, Aether creates child grant `G2` for that worker
5. worker connects as itself using task token, then uses `G2` for operations on behalf of `alice`

This cleanly separates authentication from delegated authorization.

## Audit Model

### Schema

Extend `comprehensive_audit_log` with first-class authority columns:

- `subject_type`
- `subject_id`
- `root_subject_type`
- `root_subject_id`
- `authority_mode`
- `authority_grant_id`
- `parent_authority_grant_id`

Keep `actor_*` as the authenticated actor.

### Semantics

Examples:

- direct user action: actor=user, subject=user, authority_mode=direct
- service acting for user: actor=service, subject=user, authority_mode=on_behalf_of
- worker acting under child grant: actor=agent/task/service, subject=user, root_subject=user, authority_mode=on_behalf_of

### ACL Decision Audit

ACL decision logs should record the same split. `grant_id` is not enough on its own.

## Compatibility With Existing Delegation Chains

> **Removed (2026-04):** The `acl_delegation_chains` table and all related code (ACL service methods, gateway hot-path checks, admin REST endpoints, SDK wrappers) were removed as part of the OBO migration. Migration 018 drops the table and removes `delegation_chain_id` from the `acl_audit_log` view. OBO authority grants (`acl_authority_grants`) are the sole mechanism for principal-to-principal authority lineage. The transitional strategy described below is preserved for historical context only.

~~Transitional strategy:~~

- ~~keep existing chain read APIs for now~~
- ~~stop expanding delegation-chain semantics~~
- ~~derive lineage views from authority grants where useful~~
- ~~eventually replace `acl_delegation_chains` with either:~~
  - ~~a compatibility view over authority grant lineage, or~~
  - ~~a read-only historical table populated from grants~~

~~What should not happen:~~

- ~~adding more policy meaning to the current chain table~~
- ~~using the current chain table for multi-user service sessions~~
- ~~using chain lookup as the primary authorization context selector~~

## Phase Plan

### Phase 0: Foundation

- status: complete
- normalize canonical principal references in ACL/audit code
- add `Service` principal to models, auth, ACL, audit, SDKs, and proto
- add audit schema columns for actor/subject/grant lineage
- add authority grant schema and service layer

### Phase 1: On-Behalf-Of For Core Operations

Scope:

- `SendMessage`
- `CreateTask`
- `KVOperation`
- operational audit query/view

Deliverables:

- status: implemented for core operations, runtime grant lifecycle, and task assignment propagation
- `AuthorizationContext` in proto and SDKs
- request-time OBO enforcement
- actor/subject/root-subject audit recording
- root task grants created from delegated `CreateTask` requests
- metadata propagation of task authority lineage into created and delivered tasks
- first-class task-schema persistence for authority lineage and current delivery binding
- runtime authority-grant `EXCHANGE` / `DERIVE` / `GET` / `RENEW` / `REVOKE` over the ordinary stream
- admin authority-grant lifecycle over REST and gRPC ACL admin operations
- automatic child-grant minting for pool-task delivery and reconnect-safe targeted delivery
- conservative task-grant renewal on task connect/progress heartbeat
- broader client SDK ergonomics beyond the base streaming authority-grant operations

### Phase 2: Orchestration Propagation

- status: partial implementation landed
- persist actor/subject/grant lineage on tasks in first-class columns instead of metadata-only storage
- renew task/root delivery grants for long-running active tasks within `renewable_until`
- expand automatic derivation beyond task assignment into deeper launched-worker / subagent flows

### Phase 3: Compatibility And Cleanup

- convert delegation-chain endpoints into compatibility wrappers or views
- remove chain-based enforcement from hot paths
- tighten fallback policies for production defaults
- document direct vs OBO semantics in public SDK docs

## Repo Areas Expected To Change

- `server/pkg/models/identity.go`
- `api/proto/aether.proto`
- `server/internal/gateway/auth_handler.go`
- `server/internal/gateway/connect.go`
- `server/internal/gateway/routing.go`
- `server/internal/gateway/kv_handler.go`
- `server/internal/gateway/orchestration_integration.go`
- `server/internal/orchestration/task_assignment.go`
- `server/internal/acl/`
- `server/internal/audit/`
- `server/migrations/`
- `sdk/go/`
- `sdk/typescript/`

## Recommended Direction

For sustainable long-term growth of Aether:

- treat `on_behalf_of` as the operation-time authority model
- treat persisted authority grants as the portable, renewable proof of delegated authority
- ~~treat delegation chains as a historical/derived lineage representation, not the enforcement core~~ (delegation chains were removed; OBO authority grants are the sole lineage mechanism)

That model fits:

- direct user connections
- trusted backend services
- partial migration of legacy system-connection paths
- automated multi-user workloads
- strong audit requirements

---

## Status: Delegation Chains Removed

The deprecation plan documented in the "Compatibility With Existing Delegation Chains" and "Phase Plan" sections above was completed in 2026-04. The `acl_delegation_chains` table, all related ACL service methods (`CheckAccessWithDelegation`, `CreateDelegationChain`, `GetDelegationChain`, `DeleteDelegationChain`), gateway hot-path delegation logic, admin REST endpoints (`/api/acl/delegation-chains`), and SDK wrappers were removed from the codebase. Migration 018 (`018_drop_delegation_chains.sql`) drops the table and removes the `delegation_chain_id` derived column from the `acl_audit_log` view. OBO authority grants (`acl_authority_grants`) are the sole mechanism for principal-to-principal authority lineage going forward.
