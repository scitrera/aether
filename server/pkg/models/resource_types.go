package models

// ResourceType constants shared across ACL and audit subsystems.
const (
	ResourceTypeWorkspace = "workspace"
	ResourceTypeAgent     = "agent"
	ResourceTypeTask      = "task"
	ResourceTypeUser      = "user"
	ResourceTypeSession   = "session"
	ResourceTypeTopic     = "topic"
	ResourceTypeKVKey     = "kv_key"
	ResourceTypeKVScope   = "kv_scope"
	// ResourceTypePermission is the legacy "permission" resource type used by
	// the original `_perm:*` capability gates. It is retained ONLY for the
	// alias/translation layer in package acl that rewrites legacy
	// (permission, "_perm:foo") rules to the typed admin/* and capability/*
	// families. New code should use ResourceTypeAdmin or ResourceTypeCapability.
	// Deprecated: use ResourceTypeAdmin or ResourceTypeCapability.
	ResourceTypePermission = "permission"
	// ResourceTypeAdmin gates global administrative operations (ACL, tokens,
	// workspaces, agents, sessions). Resource IDs follow the form
	// "admin/<category>" (e.g. "admin/acl") or "admin/*" for the umbrella.
	ResourceTypeAdmin = "admin"
	// ResourceTypeCapability gates runtime capabilities that are not
	// workspace/agent/task scoped (e.g. "capability/metric_credit",
	// "capability/resolve_authority", "capability/create_workspace").
	ResourceTypeCapability = "capability"
	// ResourceTypeServiceImpl gates capabilities scoped to a Service
	// principal's implementation identifier. Services are workspace-less
	// (sv::impl::spec) so workspace ACLs do not apply directly. The
	// canonical resource_id is the implementation name (e.g.
	// "sandbox-sidecar"); specifier wildcards are implicit. Today this is
	// used by the gateway's task-token-issue gate to authorize an actor to
	// mint per-task tokens for service principals (preventing arbitrary
	// agents from forging tokens for impls they don't own).
	ResourceTypeServiceImpl = "service_impl"
)
