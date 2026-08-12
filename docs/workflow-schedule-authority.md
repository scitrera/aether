# Workflow schedule authority

Aether can attach a private, bounded authority grant to a WorkflowEngine
schedule. This is intended for scheduled `create_task` actions that must run on
behalf of a user after the operation that created the schedule has returned.

The schedule definition stays portable. Schedule-authority grant IDs are not written to action
JSON, task payloads, schedule responses, or ecosystem messages. The gateway
mints the grant from an authenticated `WorkflowOperation`; WorkflowEngine
stores it in private schedule columns and supplies it only on the later
`CreateTaskRequest` transport envelope.

## Authorization boundary

Schedule list/create/upsert/delete operations require an exact workspace.
Create/upsert/delete also require an exact schedule ID matching the JSON
definition. The gateway checks the canonical resource
`workflow/schedule:workspaces/{workspace}/schedules/{schedule}` at read or
manage level. Production deployments must grant this resource explicitly.
AetherLite and the full gateway grant user schedule management only when their
explicit `--dev` mode is enabled.

Clients may send:

- `WorkflowOperation.authorization`: optional direct/OBO authority used to
  authorize schedule management;
- `WorkflowOperation.schedule_authority_scope`: requested workspace, resource,
  operation, access, expiry, hop, lifetime, and policy-version ceiling; and
- action fields `require_task_authority` and
  `required_downstream_authority_hops`.

The current `policy_version` is `1`. Unknown versions fail closed. The gateway
records a deterministic SHA-256 digest of the complete scope. A targeted task
requires one derivation edge after the schedule grant; a pooled task requires
two because the pool task anchor must derive to the selected worker.

## Lifetime modes

`WORKFLOW_AUTHORITY_LIFETIME_SOURCE_BOUND` derives under the caller's source
grant. Source expiry/revocation cascades normally, and a source grant bound to a
live session or task is rechecked on every scheduled task creation.

`WORKFLOW_AUTHORITY_LIFETIME_DURABLE` creates a new bounded root. A direct user
may request it; an OBO intermediary additionally needs manage access to
`capability/schedule_authority`, and the requested scope/hops must attenuate the
source. Durable authority has a fixed expiry of at most 90 days, cannot be
auto-renewed by WorkflowEngine, and must be replaced by authenticated upsert.

## Failure and replacement

Upsert stores the replacement, revokes the prior grant cascade, and restores
the prior row if revocation fails. Delete revokes before deleting. Gateway
forwarding failures, negative WorkflowEngine responses, request timeouts, and
shutdown revoke provisional grants.

A transient task-creation error leaves the occurrence due for its existing
idempotent retry. Expiry, revocation, missing authority, or authority denial
records a no-task `authority_invalid` skip and blocks the schedule until an
authenticated upsert installs fresh authority.

Direct schedules without `require_task_authority` remain supported. This keeps
standalone/system scheduling available while allowing enterprise compositions
to fail closed for user-authorized scheduled work.
