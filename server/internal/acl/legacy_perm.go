package acl

import (
	"sync"

	"github.com/scitrera/aether/internal/logging"
)

// legacyPermMap maps legacy "_perm:*" resource IDs (paired with the
// "permission" resource type) to their canonical typed equivalents under the
// "admin/*" and "capability/*" resource families. Migration
// 020_permission_namespace_refactor.sql backfills the rule table to the new
// shape; this map provides forward-compatible translation for callers and
// (more importantly) for any rule rows that escape the migration.
var legacyPermMap = map[string]struct {
	resourceType string
	resourceID   string
}{
	"_perm:create_workspace":          {ResourceTypeCapability, "capability/create_workspace"},
	"_perm:admin_operations":          {ResourceTypeAdmin, "admin/*"},
	"_perm:admin_acl":                 {ResourceTypeAdmin, "admin/acl"},
	"_perm:admin_tokens":              {ResourceTypeAdmin, "admin/tokens"},
	"_perm:admin_workspaces":          {ResourceTypeAdmin, "admin/workspaces"},
	"_perm:admin_agents":              {ResourceTypeAdmin, "admin/agents"},
	"_perm:exchange_authority_grants": {ResourceTypeCapability, "capability/exchange_authority_grants"},
	"_perm:authority_intermediary":    {ResourceTypeCapability, "capability/authority_intermediary"},
	"_perm:metric_credit":             {ResourceTypeCapability, "capability/metric_credit"},
	"_perm:resolve_authority":         {ResourceTypeCapability, "capability/resolve_authority"},
	"_perm:query_connections":         {ResourceTypeCapability, "capability/query_connections"},
}

// legacyPermLogOnce ensures we log a single notice per process whenever a
// legacy translation actually fires. Spamming the log on every CheckAccess
// would be noisy; once-per-process is enough to flag that the alias layer is
// still being exercised.
var legacyPermLogOnce sync.Once

// rewriteLegacyPermission converts a (resourceType, resourceID) pair from the
// legacy "permission" + "_perm:*" form to the typed "admin"/"capability"
// families. Returns the rewritten pair and true if a translation occurred,
// otherwise the inputs and false. The rewrite only fires when resourceType is
// exactly ResourceTypePermission ("permission") AND resourceID matches a known
// legacy capability name; unknown "_perm:*" strings are passed through
// unchanged so any operator-defined custom permissions remain addressable.
func rewriteLegacyPermission(resourceType, resourceID string) (string, string, bool) {
	if resourceType != ResourceTypePermission {
		return resourceType, resourceID, false
	}
	mapped, ok := legacyPermMap[resourceID]
	if !ok {
		return resourceType, resourceID, false
	}
	legacyPermLogOnce.Do(func() {
		logging.Logger.Info().
			Str("legacy_resource_type", resourceType).
			Str("legacy_resource_id", resourceID).
			Str("new_resource_type", mapped.resourceType).
			Str("new_resource_id", mapped.resourceID).
			Msg("acl: translating legacy _perm:* permission to typed admin/capability resource (logged once per process)")
	})
	return mapped.resourceType, mapped.resourceID, true
}
