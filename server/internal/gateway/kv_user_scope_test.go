package gateway

import (
	"testing"

	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/internal/kv"
	"github.com/scitrera/aether/server/pkg/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The Identity axis of the scope taxonomy ("whose data is this?") had no
// enforcement: op.UserId is client-supplied and ValidateScopeSpec only checks
// it is non-empty, so a caller could name any user and address their namespace.
// An ACL default-deny on the shared user scopes stood in for it, which is a
// mitigation at the wrong layer — it blocks legitimate same-user reads just as
// hard as cross-user ones. These pin the axis itself.

func oboAuthority(subjectUser string) *acl.ResolvedAuthority {
	return &acl.ResolvedAuthority{
		Actor:   agentIdentity,
		Subject: models.Identity{Type: models.PrincipalUser, ID: subjectUser},
		Grant:   &acl.AuthorityGrant{GrantID: "grant-1", RootSubjectType: "user", RootSubjectID: subjectUser},
	}
}

func TestResolveUserScopeSubject_rejects_a_foreign_user(t *testing.T) {
	// The whole point: an OBO grant for user A must not reach user B's
	// namespace, on either shared user scope.
	for _, scope := range []kv.KVScope{kv.ScopeUserShared, kv.ScopeUserWorkspaceShared, kv.ScopeUser, kv.ScopeUserWorkspace} {
		got, err := resolveUserScopeSubject(scope, agentIdentity, oboAuthority("alice@example.com"), "bob@example.com")
		if err == nil {
			t.Fatalf("scope %s: expected denial, got user_id=%q", scope, got)
		}
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("scope %s: expected PermissionDenied, got %v", scope, status.Code(err))
		}
	}
}

func TestResolveUserScopeSubject_allows_the_subjects_own_namespace(t *testing.T) {
	got, err := resolveUserScopeSubject(
		kv.ScopeUserWorkspaceShared, agentIdentity, oboAuthority("alice@example.com"), "alice@example.com")
	if err != nil {
		t.Fatalf("same-user access must be allowed: %v", err)
	}
	if got != "alice@example.com" {
		t.Fatalf("user_id = %q, want alice@example.com", got)
	}
}

func TestResolveUserScopeSubject_derives_an_omitted_user_from_the_subject(t *testing.T) {
	// Under OBO the user axis is derivable rather than assertable, so an
	// omitted user_id is filled in instead of failing scope validation.
	got, err := resolveUserScopeSubject(
		kv.ScopeUserShared, agentIdentity, oboAuthority("alice@example.com"), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "alice@example.com" {
		t.Fatalf("user_id = %q, want it derived from the subject", got)
	}
}

func TestResolveUserScopeSubject_ignores_non_user_scopes(t *testing.T) {
	// Global and workspace scopes have no user axis; a user_id on them is
	// meaningless and must not be rewritten or rejected here.
	for _, scope := range []kv.KVScope{kv.ScopeGlobal, kv.ScopeWorkspace, kv.ScopeGlobalExclusive, kv.ScopeWorkspaceExclusive} {
		got, err := resolveUserScopeSubject(scope, agentIdentity, oboAuthority("alice@example.com"), "bob@example.com")
		if err != nil {
			t.Fatalf("scope %s: unexpected error %v", scope, err)
		}
		if got != "bob@example.com" {
			t.Fatalf("scope %s: user_id = %q, want it untouched", scope, got)
		}
	}
}

func TestResolveUserScopeSubject_leaves_own_authority_callers_alone(t *testing.T) {
	// platform-server writes per-user session state for the browser's user
	// under its OWN service authority; its reach is bounded by the explicit
	// kv_scope grants it holds, not by this pin. Breaking that would silently
	// drop session state.
	svc := models.Identity{Type: models.PrincipalService, Implementation: "platform-server", Specifier: "a"}
	got, err := resolveUserScopeSubject(kv.ScopeUserWorkspaceShared, svc, nil, "alice@example.com")
	if err != nil {
		t.Fatalf("service under its own authority must be untouched: %v", err)
	}
	if got != "alice@example.com" {
		t.Fatalf("user_id = %q, want it preserved", got)
	}
}

func TestResolveUserScopeSubject_rejects_a_subject_with_no_user_id(t *testing.T) {
	// A user-typed subject with an empty ID cannot identify a namespace;
	// proceeding would fall back to whatever the caller supplied.
	authority := oboAuthority("")
	if _, err := resolveUserScopeSubject(
		kv.ScopeUserShared, agentIdentity, authority, "bob@example.com"); err == nil {
		t.Fatal("expected denial when the OBO subject carries no user id")
	}
}

func TestResolveUserScopeSubject_ignores_non_user_subjects(t *testing.T) {
	// Service-to-service OBO (a grant whose subject is not a user) has no user
	// axis to pin; leave it to the ACL.
	authority := &acl.ResolvedAuthority{
		Actor:   agentIdentity,
		Subject: models.Identity{Type: models.PrincipalService, Implementation: "memorylayer", Specifier: "a"},
		Grant:   &acl.AuthorityGrant{GrantID: "grant-2"},
	}
	got, err := resolveUserScopeSubject(kv.ScopeUserShared, agentIdentity, authority, "bob@example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "bob@example.com" {
		t.Fatalf("user_id = %q, want it untouched", got)
	}
}
