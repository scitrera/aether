package gateway

import (
	"testing"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/models"
)

func TestResolveExchangeAudience(t *testing.T) {
	sessionID := uuid.New()
	client := &ClientSession{
		SessionUUID:      sessionID,
		AssociatedTaskID: "task-123",
	}

	t.Run("defaults to current session", func(t *testing.T) {
		audienceType, audienceID, err := resolveExchangeAudience(client, models.Identity{Type: models.PrincipalUser, ID: "alice"}, "", "", "")
		if err != nil {
			t.Fatalf("resolveExchangeAudience() error = %v", err)
		}
		if audienceType != acl.AuthorityAudienceSession {
			t.Fatalf("audienceType = %q, want %q", audienceType, acl.AuthorityAudienceSession)
		}
		if audienceID != sessionID.String() {
			t.Fatalf("audienceID = %q, want %q", audienceID, sessionID.String())
		}
	})

	t.Run("rejects mismatched session", func(t *testing.T) {
		if _, _, err := resolveExchangeAudience(client, models.Identity{Type: models.PrincipalUser, ID: "alice"}, acl.AuthorityAudienceSession, uuid.New().String(), ""); err == nil {
			t.Fatal("expected error for mismatched session audience")
		}
	})

	t.Run("accepts current task audience", func(t *testing.T) {
		audienceType, audienceID, err := resolveExchangeAudience(client, models.Identity{Type: models.PrincipalTask, Workspace: "ws", Implementation: "job", ID: "task-actor"}, acl.AuthorityAudienceTask, "", "")
		if err != nil {
			t.Fatalf("resolveExchangeAudience() error = %v", err)
		}
		if audienceType != acl.AuthorityAudienceTask || audienceID != "task-123" {
			t.Fatalf("resolveExchangeAudience() = (%q, %q), want (%q, %q)", audienceType, audienceID, acl.AuthorityAudienceTask, "task-123")
		}
	})

	t.Run("accepts service audience for current service", func(t *testing.T) {
		actor := models.Identity{Type: models.PrincipalService, Implementation: "frontend", Specifier: "api-1"}
		audienceType, audienceID, err := resolveExchangeAudience(client, actor, acl.AuthorityAudienceService, "", "")
		if err != nil {
			t.Fatalf("resolveExchangeAudience() error = %v", err)
		}
		if audienceType != acl.AuthorityAudienceService {
			t.Fatalf("audienceType = %q, want %q", audienceType, acl.AuthorityAudienceService)
		}
		if audienceID != actor.CanonicalPrincipalID() {
			t.Fatalf("audienceID = %q, want %q", audienceID, actor.CanonicalPrincipalID())
		}
	})
}

func TestIdentityTopicFromAuthorityPrincipal(t *testing.T) {
	tests := []struct {
		name          string
		principalType string
		principalID   string
		want          string
	}{
		{
			name:          "user yields bare-user topic",
			principalType: acl.PrincipalTypeUser,
			principalID:   "alice",
			want:          (models.Identity{Type: models.PrincipalUser, ID: "alice"}).String(),
		},
		{
			name:          "service yields canonical topic",
			principalType: acl.PrincipalTypeService,
			principalID:   "sv::frontend::api-1",
			want:          (models.Identity{Type: models.PrincipalService, Implementation: "frontend", Specifier: "api-1"}).String(),
		},
		{
			name:          "empty type returns empty",
			principalType: "",
			principalID:   "x",
			want:          "",
		},
		{
			name:          "empty id returns empty",
			principalType: acl.PrincipalTypeUser,
			principalID:   "",
			want:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := identityTopicFromAuthorityPrincipal(tt.principalType, tt.principalID); got != tt.want {
				t.Fatalf("identityTopicFromAuthorityPrincipal(%q, %q) = %q, want %q", tt.principalType, tt.principalID, got, tt.want)
			}
		})
	}
}

func TestIdentityFromAuthorityPrincipal(t *testing.T) {
	tests := []struct {
		name          string
		principalType string
		principalID   string
		wantType      models.PrincipalType
		wantID        string
	}{
		{
			name:          "user principal",
			principalType: acl.PrincipalTypeUser,
			principalID:   "alice",
			wantType:      models.PrincipalUser,
			wantID:        "alice",
		},
		{
			name:          "service principal",
			principalType: acl.PrincipalTypeService,
			principalID:   "sv::frontend::api-1",
			wantType:      models.PrincipalService,
			wantID:        "sv::frontend::api-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := identityFromAuthorityPrincipal(tt.principalType, tt.principalID)
			if err != nil {
				t.Fatalf("identityFromAuthorityPrincipal() error = %v", err)
			}
			if got.Type != tt.wantType {
				t.Fatalf("Type = %v, want %v", got.Type, tt.wantType)
			}
			if got.CanonicalPrincipalID() != tt.wantID {
				t.Fatalf("CanonicalPrincipalID() = %q, want %q", got.CanonicalPrincipalID(), tt.wantID)
			}
		})
	}
}
