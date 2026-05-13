package gateway

import (
	"testing"
	"time"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/models"
)

func TestApplyTaskAuthorityGrantToMetadataPreservesCreatorLineage(t *testing.T) {
	metadata := map[string]interface{}{
		"authority_mode":          "on_behalf_of",
		"subject_type":            "User",
		"subject_id":              "alice",
		"authority_grant_id":      "creator-grant",
		"root_authority_grant_id": "creator-root",
	}

	grant := &acl.AuthorityGrant{
		GrantID:         "task-grant",
		RootGrantID:     "task-root",
		SubjectType:     acl.PrincipalTypeUser,
		SubjectID:       "alice",
		DelegateType:    acl.PrincipalTypeAgent,
		DelegateID:      "ag::ws::worker::a1",
		RootSubjectType: acl.PrincipalTypeUser,
		RootSubjectID:   "alice",
		AudienceType:    acl.AuthorityAudienceAgent,
		AudienceID:      "ag::ws::worker::a1",
	}

	updated := applyTaskAuthorityGrantToMetadata(metadata, grant)

	if got := metadataString(updated, "creator_authority_grant_id"); got != "creator-grant" {
		t.Fatalf("creator_authority_grant_id = %q, want %q", got, "creator-grant")
	}
	if got := metadataString(updated, "authority_grant_id"); got != "task-grant" {
		t.Fatalf("authority_grant_id = %q, want %q", got, "task-grant")
	}
	if got := metadataString(updated, authorityAudienceTypeKey); got != acl.AuthorityAudienceAgent {
		t.Fatalf("authority_audience_type = %q, want %q", got, acl.AuthorityAudienceAgent)
	}
}

func TestTaskAuthorityGrantUsableForDelegate(t *testing.T) {
	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws",
		Implementation: "worker",
		Specifier:      "a1",
	}

	usableGrant := &acl.AuthorityGrant{
		GrantID:      "grant-1",
		DelegateType: acl.PrincipalTypeAgent,
		DelegateID:   delegate.CanonicalPrincipalID(),
		AudienceType: acl.AuthorityAudienceAgent,
		AudienceID:   delegate.CanonicalPrincipalID(),
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if !taskAuthorityGrantUsableForDelegate(usableGrant, delegate, "") {
		t.Fatal("expected agent-bound grant to be usable for matching delegate")
	}

	taskBoundGrant := &acl.AuthorityGrant{
		GrantID:      "grant-2",
		DelegateType: acl.PrincipalTypeTask,
		DelegateID:   "ta::ws::worker::task-1",
		AudienceType: acl.AuthorityAudienceTask,
		AudienceID:   "task-1",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if taskAuthorityGrantUsableForDelegate(taskBoundGrant, delegate, "task-1") {
		t.Fatal("expected task-bound anchor grant to be unusable directly for agent delegate")
	}
}

func TestTaskGrantRenewalTarget(t *testing.T) {
	now := time.Now()

	t.Run("renews when close to expiry", func(t *testing.T) {
		createdAt := now.Add(-25 * time.Minute)
		grant := &acl.AuthorityGrant{
			GrantID:                  "grant-1",
			CreatedAt:                createdAt,
			ExpiresAt:                now.Add(2 * time.Minute),
			RenewableUntil:           now.Add(30 * time.Minute),
			ValidWhileAudienceActive: true,
		}

		target, ok := taskGrantRenewalTarget(grant, now)
		if !ok {
			t.Fatal("expected renewal to be needed near expiry")
		}
		expected := now.Add(27 * time.Minute)
		if target.Before(expected.Add(-time.Second)) || target.After(expected.Add(time.Second)) {
			t.Fatalf("renewal target = %s, want about %s", target, expected)
		}
	})

	t.Run("does not renew when not near expiry", func(t *testing.T) {
		grant := &acl.AuthorityGrant{
			GrantID:        "grant-2",
			CreatedAt:      now.Add(-5 * time.Minute),
			ExpiresAt:      now.Add(20 * time.Minute),
			RenewableUntil: now.Add(40 * time.Minute),
		}

		if _, ok := taskGrantRenewalTarget(grant, now); ok {
			t.Fatal("expected no renewal when grant is not close to expiry")
		}
	})

	t.Run("caps renewal at renewable_until", func(t *testing.T) {
		grant := &acl.AuthorityGrant{
			GrantID:        "grant-3",
			CreatedAt:      now.Add(-20 * time.Minute),
			ExpiresAt:      now.Add(1 * time.Minute),
			RenewableUntil: now.Add(10 * time.Minute),
		}

		target, ok := taskGrantRenewalTarget(grant, now)
		if !ok {
			t.Fatal("expected renewal to be possible")
		}
		if !target.Equal(grant.RenewableUntil) {
			t.Fatalf("renewal target = %s, want %s", target, grant.RenewableUntil)
		}
	})
}
