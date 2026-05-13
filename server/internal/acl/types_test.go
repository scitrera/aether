package acl

import (
	"testing"
)

func TestACLDecision_Denied(t *testing.T) {
	allowed := &ACLDecision{Allowed: true}
	if allowed.Denied() {
		t.Error("allowed decision should not be denied")
	}

	denied := &ACLDecision{Allowed: false}
	if !denied.Denied() {
		t.Error("denied decision should be denied")
	}
}

func TestACLDecision_HasLevel(t *testing.T) {
	d := &ACLDecision{Allowed: true, EffectiveAccessLevel: AccessReadWrite}

	if !d.HasLevel(AccessRead) {
		t.Error("ReadWrite should satisfy Read")
	}
	if !d.HasLevel(AccessReadWrite) {
		t.Error("ReadWrite should satisfy ReadWrite")
	}
	if d.HasLevel(AccessManage) {
		t.Error("ReadWrite should not satisfy Manage")
	}
}

func TestACLDecision_HasLevel_Denied(t *testing.T) {
	d := &ACLDecision{Allowed: false, EffectiveAccessLevel: AccessAdmin}
	if d.HasLevel(AccessRead) {
		t.Error("denied decision should never HasLevel even with high effective level")
	}
}

func TestAccessLevelName(t *testing.T) {
	tests := []struct {
		level int
		want  string
	}{
		{AccessNone, "NONE"},
		{AccessRead, "READ"},
		{AccessReadWrite, "READWRITE"},
		{AccessManage, "MANAGE"},
		{AccessAdmin, "ADMIN"},
		{AccessSuperAdmin, "SUPERADMIN"},
		{99, "UNKNOWN"},
	}

	for _, tt := range tests {
		got := AccessLevelName(tt.level)
		if got != tt.want {
			t.Errorf("AccessLevelName(%d) = %q, want %q", tt.level, got, tt.want)
		}
	}
}

func TestValidateAccessLevel(t *testing.T) {
	valid := []int{AccessNone, AccessRead, AccessReadWrite, AccessManage, AccessAdmin, AccessSuperAdmin}
	for _, level := range valid {
		if err := ValidateAccessLevel(level); err != nil {
			t.Errorf("ValidateAccessLevel(%d) = %v, want nil", level, err)
		}
	}

	invalid := []int{-1, 5, 15, 25, 55, 60, 100}
	for _, level := range invalid {
		if err := ValidateAccessLevel(level); err == nil {
			t.Errorf("ValidateAccessLevel(%d) = nil, want error", level)
		}
	}
}

func TestIsWildcard(t *testing.T) {
	if !IsWildcard(WildcardAnyAuthenticatedUser) {
		t.Error("_any_authenticated_user should be wildcard")
	}
	if !IsWildcard(WildcardAnyAgent) {
		t.Error("_any_agent should be wildcard")
	}
	if !IsWildcard(WildcardAnyTask) {
		t.Error("_any_task should be wildcard")
	}
	if IsWildcard("regular-user") {
		t.Error("regular-user should not be wildcard")
	}
	if IsWildcard("") {
		t.Error("empty string should not be wildcard")
	}
}

func TestRuleCategory(t *testing.T) {
	got := RuleCategory(PrincipalTypeUser, ResourceTypeWorkspace)
	if got != "user_workspace" {
		t.Errorf("RuleCategory(%s, %s) = %q, want user_workspace", PrincipalTypeUser, ResourceTypeWorkspace, got)
	}

	got = RuleCategory(PrincipalTypeAgent, ResourceTypePermission)
	if got != "agent_permission" {
		t.Errorf("RuleCategory(%s, %s) = %q, want agent_permission", PrincipalTypeAgent, ResourceTypePermission, got)
	}
}

// TestRewriteLegacyPermission_KnownNames verifies that the alias layer
// translates each of the 11 legacy "_perm:*" capability resource IDs to the
// canonical typed admin/* and capability/* form. Inputs paired with any other
// resource type are passed through unchanged.
func TestRewriteLegacyPermission_KnownNames(t *testing.T) {
	cases := []struct {
		legacyID string
		newType  string
		newID    string
	}{
		{"_perm:create_workspace", ResourceTypeCapability, "capability/create_workspace"},
		{"_perm:admin_operations", ResourceTypeAdmin, "admin/*"},
		{"_perm:admin_acl", ResourceTypeAdmin, "admin/acl"},
		{"_perm:admin_tokens", ResourceTypeAdmin, "admin/tokens"},
		{"_perm:admin_workspaces", ResourceTypeAdmin, "admin/workspaces"},
		{"_perm:admin_agents", ResourceTypeAdmin, "admin/agents"},
		{"_perm:exchange_authority_grants", ResourceTypeCapability, "capability/exchange_authority_grants"},
		{"_perm:authority_intermediary", ResourceTypeCapability, "capability/authority_intermediary"},
		{"_perm:metric_credit", ResourceTypeCapability, "capability/metric_credit"},
		{"_perm:resolve_authority", ResourceTypeCapability, "capability/resolve_authority"},
		{"_perm:query_connections", ResourceTypeCapability, "capability/query_connections"},
	}

	for _, c := range cases {
		t.Run(c.legacyID, func(t *testing.T) {
			gotType, gotID, ok := rewriteLegacyPermission(ResourceTypePermission, c.legacyID)
			if !ok {
				t.Fatalf("expected rewrite to fire for %q", c.legacyID)
			}
			if gotType != c.newType {
				t.Errorf("resource_type = %q, want %q", gotType, c.newType)
			}
			if gotID != c.newID {
				t.Errorf("resource_id = %q, want %q", gotID, c.newID)
			}
		})
	}
}

// TestRewriteLegacyPermission_PassthroughOnNonMatch verifies that the alias
// layer does NOT rewrite when the resource type isn't "permission" or when
// the resource_id is not a known legacy name. This keeps custom operator-
// defined permissions (and rules already on the new shape) addressable.
func TestRewriteLegacyPermission_PassthroughOnNonMatch(t *testing.T) {
	cases := []struct {
		name         string
		resourceType string
		resourceID   string
	}{
		{"already-typed admin", ResourceTypeAdmin, "admin/acl"},
		{"already-typed capability", ResourceTypeCapability, "capability/metric_credit"},
		{"unknown _perm: name with permission type", ResourceTypePermission, "_perm:operator_custom"},
		{"workspace resource", ResourceTypeWorkspace, "default"},
		{"empty pair", "", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotType, gotID, ok := rewriteLegacyPermission(c.resourceType, c.resourceID)
			if ok {
				t.Errorf("expected no rewrite, got (%q, %q)", gotType, gotID)
			}
			if gotType != c.resourceType || gotID != c.resourceID {
				t.Errorf("inputs were mutated: got (%q, %q), want (%q, %q)", gotType, gotID, c.resourceType, c.resourceID)
			}
		})
	}
}

// TestPermissionConstants_HaveTypedValues locks the post-migration string
// values for the public Permission* constants. Migration
// 020_permission_namespace_refactor.sql + the alias layer assume these
// values; if they drift, in-flight rules and call sites diverge.
func TestPermissionConstants_HaveTypedValues(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"PermissionCreateWorkspace", PermissionCreateWorkspace, "capability/create_workspace"},
		{"PermissionAdminOperations", PermissionAdminOperations, "admin/*"},
		{"PermissionAdminACL", PermissionAdminACL, "admin/acl"},
		{"PermissionAdminTokens", PermissionAdminTokens, "admin/tokens"},
		{"PermissionAdminWorkspaces", PermissionAdminWorkspaces, "admin/workspaces"},
		{"PermissionAdminAgents", PermissionAdminAgents, "admin/agents"},
		{"PermissionExchangeAuthorityGrants", PermissionExchangeAuthorityGrants, "capability/exchange_authority_grants"},
		{"PermissionAuthorityIntermediary", PermissionAuthorityIntermediary, "capability/authority_intermediary"},
		{"PermissionMetricCredit", PermissionMetricCredit, "capability/metric_credit"},
		{"PermissionResolveAuthority", PermissionResolveAuthority, "capability/resolve_authority"},
		{"PermissionQueryConnections", PermissionQueryConnections, "capability/query_connections"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

// TestValidateGlobPattern_PerIdentityPrefix locks the rule that 4-segment
// identity types (ag/tu/ta) require 3 concrete segments before a wildcard,
// while 3-segment types (sv/br/us/etc.) require only 2. The intent is
// "everything except the LAST segment must be concrete".
func TestValidateGlobPattern_PerIdentityPrefix(t *testing.T) {
	cases := []struct {
		name    string
		id      string
		wantErr bool
	}{
		// 4-segment agent identities — last-segment wildcard allowed
		{"agent impl-locked spec-wild", "ag::_system::platform-server::*", false},
		{"agent impl-wild rejected", "ag::_system::*", true},
		{"agent type-only rejected", "ag::*", true},

		// 3-segment service identities — last-segment wildcard allowed
		{"service impl-locked spec-wild", "sv::billing-sync::*", false},
		{"service impl-locked spec-wild platform", "sv::platform-server::*", false},
		{"service type-only rejected", "sv::*", true},

		// 3-segment bridge identities
		{"bridge impl-locked spec-wild", "br::discord::*", false},
		{"bridge type-only rejected", "br::*", true},

		// 3-segment user identities (us::user_id::window_id)
		{"user user-locked window-wild", "us::alice::*", false},
		{"user type-only rejected", "us::*", true},

		// Named-wildcard constants always pass
		{"named wildcard _any_agent", WildcardAnyAgent, false},
		{"named wildcard _any_service", WildcardAnyService, false},

		// Non-glob IDs always pass
		{"plain agent identity", "ag::_system::platform-server::default", false},
		{"plain service identity", "sv::billing-sync::default", false},

		// Non-identity-style IDs skip the segment guard. The hierarchy in
		// these formats lives in `/` or `:` separators that the guard wasn't
		// designed for; operators take responsibility for breadth.
		{"typed admin umbrella", "admin/*", false},
		{"typed admin category-glob", "admin/agen*", false},
		{"typed capability narrow path with ::", "capability/exchange_authority_grants/prod/user::cust-*", false},
		{"workspace glob", "prod-*", false},
		{"service_impl glob", "sandbox-*", false},
		{"kv key glob", "billing/cred*", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateGlobPattern(c.id, "principal_id")
			if (err != nil) != c.wantErr {
				t.Errorf("validateGlobPattern(%q) err = %v, wantErr = %v", c.id, err, c.wantErr)
			}
		})
	}
}

// TestRequiredGlobSegmentsForID covers the per-prefix minimum that backs
// validateGlobPattern. 3-segment identity types use 2; everything else
// falls back to the global default of 3.
func TestRequiredGlobSegmentsForID(t *testing.T) {
	cases := []struct {
		id   string
		want int
	}{
		{"sv::impl::*", 2},
		{"br::impl::*", 2},
		{"us::uid::*", 2},
		{"orc::impl::*", 2},
		{"wfe::impl", 2},
		{"metric::ws", 2},
		{"event::ws", 2},
		// 4-segment types fall through to the default
		{"ag::ws::impl::*", minGlobSegments},
		{"tu::ws::impl::*", minGlobSegments},
		{"ta::ws::impl::*", minGlobSegments},
		// Non-identity strings (resource IDs, etc.) — default
		{"billing:*", minGlobSegments},
		{"kv_key/foo:*", minGlobSegments},
	}

	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			got := requiredGlobSegmentsForID(c.id)
			if got != c.want {
				t.Errorf("requiredGlobSegmentsForID(%q) = %d, want %d", c.id, got, c.want)
			}
		})
	}
}
