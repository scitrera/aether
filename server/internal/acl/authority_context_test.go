package acl

import (
	"testing"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

func TestValidateGrantAudience(t *testing.T) {
	sessionID := uuid.New()
	actor := models.Identity{Type: models.PrincipalService, Implementation: "frontend", Specifier: "api-1"}

	tests := []struct {
		name     string
		grant    AuthorityGrant
		audience GrantAudienceContext
		wantErr  error
	}{
		{
			name: "session audience matches",
			grant: AuthorityGrant{
				AudienceType: AuthorityAudienceSession,
				AudienceID:   sessionID.String(),
			},
			audience: GrantAudienceContext{
				SessionID: sessionID,
				Actor:     actor,
			},
		},
		{
			name: "task audience mismatch",
			grant: AuthorityGrant{
				AudienceType: AuthorityAudienceTask,
				AudienceID:   "task-123",
			},
			audience: GrantAudienceContext{
				AssociatedTaskID: "task-999",
				Actor:            actor,
			},
			wantErr: ErrAuthorityGrantAudienceMismatch,
		},
		{
			name: "service audience matches actor",
			grant: AuthorityGrant{
				AudienceType: AuthorityAudienceService,
				AudienceID:   actor.CanonicalPrincipalID(),
			},
			audience: GrantAudienceContext{
				Actor: actor,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGrantAudience(&tt.grant, actor, tt.audience)
			if err != tt.wantErr {
				t.Fatalf("validateGrantAudience() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateGrantConstraints(t *testing.T) {
	grant := &AuthorityGrant{
		MaxAccessLevel: AccessReadWrite,
		WorkspaceScope: []string{"ws-a"},
		OperationScope: []string{"kv_*", "task_create"},
		ResourceScope: map[string][]string{
			ResourceTypeKVKey: {"ws-a.config.*"},
		},
	}

	if decision := validateGrantConstraints(grant, ResourceTypeKVKey, "ws-a.config.key1", "kv_put", "ws-a", AccessReadWrite); decision != nil {
		t.Fatalf("validateGrantConstraints() unexpected deny decision: %+v", decision)
	}

	if decision := validateGrantConstraints(grant, ResourceTypeKVKey, "ws-a.config.key1", "kv_delete", "ws-a", AccessAdmin); decision == nil || decision.Reason != ErrAuthorityGrantScopeEscalation.Error() {
		t.Fatalf("validateGrantConstraints() scope escalation = %+v", decision)
	}

	if decision := validateGrantConstraints(grant, ResourceTypeKVKey, "ws-b.config.key1", "kv_put", "ws-b", AccessReadWrite); decision == nil || decision.Reason != ErrAuthorityGrantWorkspaceDenied.Error() {
		t.Fatalf("validateGrantConstraints() workspace deny = %+v", decision)
	}

	if decision := validateGrantConstraints(grant, ResourceTypeKVKey, "ws-a.other.key1", "kv_put", "ws-a", AccessReadWrite); decision == nil || decision.Reason != ErrAuthorityGrantResourceDenied.Error() {
		t.Fatalf("validateGrantConstraints() resource deny = %+v", decision)
	}
}

func TestMatchesWorkspaceConstraint(t *testing.T) {
	tests := []struct {
		name      string
		workspace string
		patterns  []string
		want      bool
	}{
		{"empty patterns matches all", "alpha", nil, true},
		{"exact match", "alpha", []string{"alpha"}, true},
		{"glob match", "alpha", []string{"a*"}, true},
		{"no match", "alpha", []string{"beta", "gamma"}, false},
		{"wildcard star", "alpha", []string{"*"}, true},
		{"subject-inherited magic value", "alpha", []string{WorkspaceScopeSubjectInherited}, true},
		{"subject-inherited mixed with explicit", "alpha", []string{"beta", WorkspaceScopeSubjectInherited}, true},
		{"subject-inherited matches anything", "any-workspace", []string{WorkspaceScopeSubjectInherited}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesWorkspaceConstraint(tt.workspace, tt.patterns); got != tt.want {
				t.Errorf("matchesWorkspaceConstraint(%q, %v) = %v, want %v", tt.workspace, tt.patterns, got, tt.want)
			}
		})
	}
}
