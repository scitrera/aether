package kv

import (
	"testing"

	"github.com/scitrera/aether/pkg/models"
)

func TestBuildNamespace(t *testing.T) {
	agent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "production",
		Implementation: "python-worker",
		Specifier:      "instance-1",
	}

	tests := []struct {
		name      string
		scope     KVScope
		userID    string
		workspace string
		expected  string
	}{
		// Shared cells: agent identity is intentionally NOT in the key.
		{
			name:     "global (shared)",
			scope:    ScopeGlobal,
			expected: "kv:global",
		},
		{
			name:      "workspace (shared)",
			scope:     ScopeWorkspace,
			workspace: "production",
			expected:  "kv:ws:production",
		},
		{
			name:     "user-shared",
			scope:    ScopeUserShared,
			userID:   "alice",
			expected: "kv:user:alice",
		},
		{
			name:      "user-workspace-shared",
			scope:     ScopeUserWorkspaceShared,
			userID:    "alice",
			workspace: "production",
			expected:  "kv:user:alice:ws:production",
		},
		// Exclusive cells: agent impl|spec embedded.
		{
			name:     "global-exclusive",
			scope:    ScopeGlobalExclusive,
			expected: "kv:agent:python-worker|instance-1:global",
		},
		{
			name:      "workspace-exclusive",
			scope:     ScopeWorkspaceExclusive,
			workspace: "production",
			expected:  "kv:agent:python-worker|instance-1:ws:production",
		},
		{
			name:     "user (exclusive)",
			scope:    ScopeUser,
			userID:   "alice",
			expected: "kv:agent:python-worker|instance-1:user:alice",
		},
		{
			name:      "user-workspace (exclusive)",
			scope:     ScopeUserWorkspace,
			userID:    "alice",
			workspace: "production",
			expected:  "kv:agent:python-worker|instance-1:user:alice:ws:production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildNamespace(agent, tt.scope, tt.userID, tt.workspace)
			if result != tt.expected {
				t.Errorf("BuildNamespace() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestBuildNamespace_SharedScopesCrossAgent enforces the central
// invariant of the eight-cell matrix:
//
//   - SHARED scopes produce IDENTICAL keys for two distinct agents
//     (cross-agent visibility by storage layout).
//   - EXCLUSIVE scopes produce DIFFERENT keys for two distinct agents
//     (storage-layer isolation, even when the userID/workspace match).
func TestBuildNamespace_SharedScopesCrossAgent(t *testing.T) {
	agentA := models.Identity{
		Type: models.PrincipalAgent, Workspace: "production",
		Implementation: "python-worker", Specifier: "instance-1",
	}
	agentB := models.Identity{
		Type: models.PrincipalAgent, Workspace: "production",
		Implementation: "java-worker", Specifier: "instance-9",
	}

	// Shared cells must collide.
	if BuildNamespace(agentA, ScopeGlobal, "", "") != BuildNamespace(agentB, ScopeGlobal, "", "") {
		t.Error("global scope must produce the same key regardless of caller (cross-agent shared)")
	}
	if BuildNamespace(agentA, ScopeWorkspace, "", "production") != BuildNamespace(agentB, ScopeWorkspace, "", "production") {
		t.Error("workspace scope must produce the same key regardless of caller (cross-agent shared)")
	}
	if BuildNamespace(agentA, ScopeUserShared, "alice", "") != BuildNamespace(agentB, ScopeUserShared, "alice", "") {
		t.Error("user-shared scope must produce the same key regardless of caller (cross-agent shared)")
	}
	if BuildNamespace(agentA, ScopeUserWorkspaceShared, "alice", "production") != BuildNamespace(agentB, ScopeUserWorkspaceShared, "alice", "production") {
		t.Error("user-workspace-shared scope must produce the same key regardless of caller (cross-agent shared)")
	}

	// Exclusive cells must diverge.
	if BuildNamespace(agentA, ScopeUser, "alice", "") == BuildNamespace(agentB, ScopeUser, "alice", "") {
		t.Error("user scope must REMAIN per-agent — different agents must produce different keys")
	}
	if BuildNamespace(agentA, ScopeUserWorkspace, "alice", "production") == BuildNamespace(agentB, ScopeUserWorkspace, "alice", "production") {
		t.Error("user-workspace scope must REMAIN per-agent")
	}
	if BuildNamespace(agentA, ScopeGlobalExclusive, "", "") == BuildNamespace(agentB, ScopeGlobalExclusive, "", "") {
		t.Error("global-exclusive scope must diverge cross-agent")
	}
	if BuildNamespace(agentA, ScopeWorkspaceExclusive, "", "production") == BuildNamespace(agentB, ScopeWorkspaceExclusive, "", "production") {
		t.Error("workspace-exclusive scope must diverge cross-agent")
	}
}

func TestParseNamespace(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		wantErr       bool
		wantAgentImpl string
		wantAgentSpec string
		wantScope     KVScope
		wantUserID    string
		wantWorkspace string
	}{
		// Shared shapes
		{
			name:      "global (shared)",
			key:       "kv:global",
			wantScope: ScopeGlobal,
		},
		{
			name:          "workspace (shared)",
			key:           "kv:ws:production",
			wantScope:     ScopeWorkspace,
			wantWorkspace: "production",
		},
		{
			name:       "user-shared",
			key:        "kv:user:alice",
			wantScope:  ScopeUserShared,
			wantUserID: "alice",
		},
		{
			name:          "user-workspace-shared",
			key:           "kv:user:alice:ws:production",
			wantScope:     ScopeUserWorkspaceShared,
			wantUserID:    "alice",
			wantWorkspace: "production",
		},
		// Exclusive shapes
		{
			name:          "global-exclusive",
			key:           "kv:agent:python-worker|v1:global",
			wantAgentImpl: "python-worker",
			wantAgentSpec: "v1",
			wantScope:     ScopeGlobalExclusive,
		},
		{
			name:          "workspace-exclusive",
			key:           "kv:agent:python-worker|v1:ws:production",
			wantAgentImpl: "python-worker",
			wantAgentSpec: "v1",
			wantScope:     ScopeWorkspaceExclusive,
			wantWorkspace: "production",
		},
		{
			name:          "user (exclusive)",
			key:           "kv:agent:python-worker|v1:user:alice",
			wantAgentImpl: "python-worker",
			wantAgentSpec: "v1",
			wantScope:     ScopeUser,
			wantUserID:    "alice",
		},
		{
			name:          "user-workspace (exclusive)",
			key:           "kv:agent:python-worker|v1:user:alice:ws:production",
			wantAgentImpl: "python-worker",
			wantAgentSpec: "v1",
			wantScope:     ScopeUserWorkspace,
			wantUserID:    "alice",
			wantWorkspace: "production",
		},
		// Errors
		{
			name:    "invalid format",
			key:     "invalid:key",
			wantErr: true,
		},
		{
			name:    "missing agent spec",
			key:     "kv:agent:python-worker:user:alice",
			wantErr: true,
		},
		{
			name:       "user-workspace-shared missing workspace",
			key:        "kv:user:alice:ws:",
			wantErr:    false, // empty workspace is parseable; storage layer rejects via validation
			wantScope:  ScopeUserWorkspaceShared,
			wantUserID: "alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agentImpl, agentSpec, scope, userID, workspace, err := ParseNamespace(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if agentImpl != tt.wantAgentImpl {
					t.Errorf("agentImpl = %v, want %v", agentImpl, tt.wantAgentImpl)
				}
				if agentSpec != tt.wantAgentSpec {
					t.Errorf("agentSpec = %v, want %v", agentSpec, tt.wantAgentSpec)
				}
				if scope != tt.wantScope {
					t.Errorf("scope = %v, want %v", scope, tt.wantScope)
				}
				if userID != tt.wantUserID {
					t.Errorf("userID = %v, want %v", userID, tt.wantUserID)
				}
				if workspace != tt.wantWorkspace {
					t.Errorf("workspace = %v, want %v", workspace, tt.wantWorkspace)
				}
			}
		})
	}
}

// TestParseNamespace_BuildRoundTrip ensures every BuildNamespace output
// round-trips through ParseNamespace back to the same scope.
func TestParseNamespace_BuildRoundTrip(t *testing.T) {
	agent := models.Identity{
		Type: models.PrincipalAgent, Workspace: "production",
		Implementation: "python-worker", Specifier: "instance-1",
	}
	cases := []struct {
		scope     KVScope
		userID    string
		workspace string
	}{
		{ScopeGlobal, "", ""},
		{ScopeWorkspace, "", "production"},
		{ScopeUserShared, "alice", ""},
		{ScopeUserWorkspaceShared, "alice", "production"},
		{ScopeGlobalExclusive, "", ""},
		{ScopeWorkspaceExclusive, "", "production"},
		{ScopeUser, "alice", ""},
		{ScopeUserWorkspace, "alice", "production"},
	}
	for _, c := range cases {
		t.Run(string(c.scope), func(t *testing.T) {
			ns := BuildNamespace(agent, c.scope, c.userID, c.workspace)
			_, _, gotScope, _, _, err := ParseNamespace(ns)
			if err != nil {
				t.Fatalf("ParseNamespace(%q): %v", ns, err)
			}
			if gotScope != c.scope {
				t.Errorf("round-trip scope mismatch: got %s, want %s (ns=%q)", gotScope, c.scope, ns)
			}
		})
	}
}

func TestIsOwner(t *testing.T) {
	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "production",
		Implementation: "python-worker",
		Specifier:      "instance-1",
	}

	tests := []struct {
		name      string
		namespace string
		expected  bool
	}{
		// Owner of all four exclusive shapes
		{
			name:      "owner: user (exclusive)",
			namespace: "kv:agent:python-worker|instance-1:user:alice",
			expected:  true,
		},
		{
			name:      "owner: user-workspace (exclusive)",
			namespace: "kv:agent:python-worker|instance-1:user:alice:ws:production",
			expected:  true,
		},
		{
			name:      "owner: global-exclusive",
			namespace: "kv:agent:python-worker|instance-1:global",
			expected:  true,
		},
		{
			name:      "owner: workspace-exclusive",
			namespace: "kv:agent:python-worker|instance-1:ws:production",
			expected:  true,
		},
		// Not owner: different impl or spec
		{
			name:      "different implementation",
			namespace: "kv:agent:java-worker|instance-1:user:alice:ws:production",
			expected:  false,
		},
		{
			name:      "different specifier",
			namespace: "kv:agent:python-worker|instance-2:user:alice:ws:production",
			expected:  false,
		},
		// Shared shapes have no owner
		{
			name:      "shared global is not owned by anyone",
			namespace: "kv:global",
			expected:  false,
		},
		{
			name:      "shared workspace is not owned by anyone",
			namespace: "kv:ws:production",
			expected:  false,
		},
		{
			name:      "shared user is not owned by anyone",
			namespace: "kv:user:alice",
			expected:  false,
		},
		{
			name:      "shared user-workspace is not owned by anyone",
			namespace: "kv:user:alice:ws:production",
			expected:  false,
		},
		{
			name:      "invalid namespace",
			namespace: "invalid:namespace",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsOwner(identity, tt.namespace)
			if result != tt.expected {
				t.Errorf("IsOwner() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestScopeFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected KVScope
	}{
		{"global", ScopeGlobal},
		{"workspace", ScopeWorkspace},
		{"user", ScopeUser},
		{"user-workspace", ScopeUserWorkspace},
		{"global-exclusive", ScopeGlobalExclusive},
		{"workspace-exclusive", ScopeWorkspaceExclusive},
		{"user-shared", ScopeUserShared},
		{"user-workspace-shared", ScopeUserWorkspaceShared},
		{"unknown", ScopeGlobal}, // fallback
		{"", ScopeGlobal},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ScopeFromString(tt.input)
			if result != tt.expected {
				t.Errorf("ScopeFromString(%s) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValidateScopeConfig(t *testing.T) {
	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "production",
		Implementation: "python-worker",
		Specifier:      "instance-1",
	}
	emptyIdentity := models.Identity{
		Type: models.PrincipalService,
	}

	tests := []struct {
		name      string
		scope     KVScope
		identity  models.Identity
		userID    string
		workspace string
		wantErr   bool
	}{
		{name: "global always valid", scope: ScopeGlobal, identity: identity},
		{name: "workspace requires workspace", scope: ScopeWorkspace, identity: identity, workspace: "production"},
		{name: "workspace without workspace fails", scope: ScopeWorkspace, identity: identity, wantErr: true},
		{name: "user requires user ID", scope: ScopeUser, identity: identity, userID: "alice"},
		{name: "user without user ID fails", scope: ScopeUser, identity: identity, wantErr: true},
		{name: "user-workspace requires both", scope: ScopeUserWorkspace, identity: identity, userID: "alice", workspace: "production"},
		{name: "user-workspace missing user ID", scope: ScopeUserWorkspace, identity: identity, workspace: "production", wantErr: true},
		{name: "user-workspace missing workspace", scope: ScopeUserWorkspace, identity: identity, userID: "alice", wantErr: true},
		// New scope coverage
		{name: "user-shared requires user ID", scope: ScopeUserShared, identity: identity, userID: "alice"},
		{name: "user-shared without user ID fails", scope: ScopeUserShared, identity: identity, wantErr: true},
		{name: "user-workspace-shared requires both", scope: ScopeUserWorkspaceShared, identity: identity, userID: "alice", workspace: "production"},
		{name: "global-exclusive valid for agent", scope: ScopeGlobalExclusive, identity: identity},
		{name: "workspace-exclusive valid for agent", scope: ScopeWorkspaceExclusive, identity: identity, workspace: "production"},
		// Exclusive scopes reject empty-identity callers
		{name: "global-exclusive rejects empty identity", scope: ScopeGlobalExclusive, identity: emptyIdentity, wantErr: true},
		{name: "workspace-exclusive rejects empty identity", scope: ScopeWorkspaceExclusive, identity: emptyIdentity, workspace: "production", wantErr: true},
		{name: "user (exclusive) rejects empty identity", scope: ScopeUser, identity: emptyIdentity, userID: "alice", wantErr: true},
		// Unknown scope
		{name: "unknown scope rejected", scope: KVScope("not-a-scope"), identity: identity, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateScopeConfig(tt.scope, tt.identity, tt.userID, tt.workspace)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateScopeConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestKVScope_IsExclusive locks the predicate used by the gateway
// owner fast-path. Exclusive cells return true; shared cells false.
func TestKVScope_IsExclusive(t *testing.T) {
	exclusive := []KVScope{ScopeGlobalExclusive, ScopeWorkspaceExclusive, ScopeUser, ScopeUserWorkspace}
	shared := []KVScope{ScopeGlobal, ScopeWorkspace, ScopeUserShared, ScopeUserWorkspaceShared}

	for _, s := range exclusive {
		if !s.IsExclusive() {
			t.Errorf("%s.IsExclusive() = false, want true", s)
		}
	}
	for _, s := range shared {
		if s.IsExclusive() {
			t.Errorf("%s.IsExclusive() = true, want false", s)
		}
	}
}
