package identityheaders

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/models"
)

func TestMint_DirectMode(t *testing.T) {
	h := Mint(context.Background(), "tenant-1", Identity{
		UserID:          "alice",
		PrincipalType:   "User",
		WorkspaceAccess: 20,
		Scopes:          "read,write",
		APIKeyID:        "key-123",
	})

	checks := map[string]string{
		HeaderTenantID:        "tenant-1",
		HeaderUserID:          "alice",
		HeaderPrincipalType:   "User",
		HeaderWorkspaceAccess: "20",
		HeaderScopes:          "read,write",
		HeaderAPIKeyID:        "key-123",
		HeaderActorType:       "User",
		HeaderActorID:         "alice",
		HeaderAuthorityMode:   AuthorityModeDirect,
	}
	for header, want := range checks {
		if got := h.Get(header); got != want {
			t.Errorf("%s: want %q, got %q", header, want, got)
		}
	}

	for _, header := range []string{HeaderGrantID, HeaderSubjectID, HeaderWorkspaceScope, HeaderMaxAccessLevel} {
		if got := h.Get(header); got != "" {
			t.Errorf("%s should be empty in direct mode, got %q", header, got)
		}
	}
}

func TestMint_OBOMode(t *testing.T) {
	h := Mint(context.Background(), "tenant-1", Identity{
		UserID:          "sv::foo::bar",
		PrincipalType:   "Service",
		WorkspaceAccess: 20,
		Authority: &AuthenticatedAuthority{
			ActorType:       "Service",
			ActorID:         "sv::foo::bar",
			GrantID:         "g1",
			SubjectType:     "User",
			SubjectID:       "alice",
			RootSubjectType: "User",
			RootSubjectID:   "alice",
			AudienceType:    "service",
			AudienceID:      "sv::foo::bar",
			MaxAccessLevel:  20,
			WorkspaceScope:  []string{"ws1", "ws2"},
		},
	})

	checks := map[string]string{
		HeaderUserID:          "alice",
		HeaderPrincipalType:   "User",
		HeaderActorType:       "Service",
		HeaderActorID:         "sv::foo::bar",
		HeaderAuthorityMode:   AuthorityModeOnBehalfOf,
		HeaderGrantID:         "g1",
		HeaderSubjectType:     "User",
		HeaderSubjectID:       "alice",
		HeaderRootSubjectType: "User",
		HeaderRootSubjectID:   "alice",
		HeaderAudienceType:    "service",
		HeaderAudienceID:      "sv::foo::bar",
		HeaderMaxAccessLevel:  "20",
		HeaderWorkspaceScope:  "ws1,ws2",
	}
	for header, want := range checks {
		if got := h.Get(header); got != want {
			t.Errorf("%s: want %q, got %q", header, want, got)
		}
	}
}

func TestMint_OBOMode_EmptyWorkspaceScopeOmitsHeader(t *testing.T) {
	h := Mint(context.Background(), "t", Identity{
		Authority: &AuthenticatedAuthority{
			ActorType:      "Service",
			ActorID:        "sv::foo::bar",
			GrantID:        "g1",
			SubjectType:    "User",
			SubjectID:      "alice",
			AudienceType:   "service",
			AudienceID:     "sv::foo::bar",
			MaxAccessLevel: 10,
			WorkspaceScope: nil,
		},
	})
	if got := h.Get(HeaderWorkspaceScope); got != "" {
		t.Errorf("WorkspaceScope should be absent when scope is empty, got %q", got)
	}
}

func TestStripInbound(t *testing.T) {
	h := http.Header{}
	h.Set("X-Auth-User-ID", "spoofed")
	h.Set("X-Aether-Grant-ID", "spoofed-grant")
	h.Set("Content-Type", "application/json")

	StripInbound(h)

	if h.Get("X-Auth-User-ID") != "" {
		t.Error("X-Auth-User-ID should have been stripped")
	}
	if h.Get("X-Aether-Grant-ID") != "" {
		t.Error("X-Aether-Grant-ID should have been stripped")
	}
	if h.Get("Content-Type") != "application/json" {
		t.Error("Content-Type should be preserved")
	}
}

func TestMintIntoMap_DirectMode(t *testing.T) {
	m := map[string]string{}
	MintIntoMap(m, "tenant-1", Identity{
		UserID:          "alice",
		PrincipalType:   "User",
		WorkspaceAccess: 20,
		Scopes:          "read,write",
		APIKeyID:        "key-123",
		CallerTopic:     "uw::alice::ws1",
	})

	checks := map[string]string{
		// Literal constant keys must survive verbatim — a bare map performs no
		// canonicalization, so "-ID" is NOT folded to "-Id".
		HeaderTenantID:           "tenant-1",
		HeaderWorkspaceAccess:    "20",
		HeaderUserID:             "alice",
		HeaderPrincipalType:      "User",
		HeaderActorType:          "User",
		HeaderActorID:            "alice",
		HeaderAuthorityMode:      AuthorityModeDirect,
		HeaderScopes:             "read,write",
		HeaderAPIKeyID:           "key-123",
		HeaderXAetherCallerTopic: "uw::alice::ws1",
	}
	for key, want := range checks {
		if got := m[key]; got != want {
			t.Errorf("%s: want %q, got %q", key, want, got)
		}
	}

	// Guard against http.Header-style canonicalization sneaking back in.
	if _, ok := m["X-Auth-Tenant-Id"]; ok {
		t.Error("MintIntoMap must not canonicalize -ID to -Id (found X-Auth-Tenant-Id)")
	}

	for _, key := range []string{HeaderGrantID, HeaderSubjectID, HeaderWorkspaceScope, HeaderMaxAccessLevel} {
		if _, ok := m[key]; ok {
			t.Errorf("%s should be absent in direct mode", key)
		}
	}
}

func TestMintIntoMap_OBOMode(t *testing.T) {
	m := map[string]string{}
	MintIntoMap(m, "tenant-1", Identity{
		UserID:          "sv::foo::bar",
		PrincipalType:   "Service",
		WorkspaceAccess: 20,
		Authority: &AuthenticatedAuthority{
			ActorType:       "Service",
			ActorID:         "sv::foo::bar",
			GrantID:         "g1",
			SubjectType:     "User",
			SubjectID:       "alice",
			RootSubjectType: "User",
			RootSubjectID:   "alice",
			AudienceType:    "service",
			AudienceID:      "sv::foo::bar",
			MaxAccessLevel:  20,
			WorkspaceScope:  []string{"ws1", "ws2"},
		},
	})

	checks := map[string]string{
		HeaderTenantID:        "tenant-1",
		HeaderWorkspaceAccess: "20",
		HeaderUserID:          "alice",
		HeaderPrincipalType:   "User",
		HeaderActorType:       "Service",
		HeaderActorID:         "sv::foo::bar",
		HeaderAuthorityMode:   AuthorityModeOnBehalfOf,
		HeaderGrantID:         "g1",
		HeaderSubjectType:     "User",
		HeaderSubjectID:       "alice",
		HeaderRootSubjectType: "User",
		HeaderRootSubjectID:   "alice",
		HeaderAudienceType:    "service",
		HeaderAudienceID:      "sv::foo::bar",
		HeaderMaxAccessLevel:  "20",
		HeaderWorkspaceScope:  "ws1,ws2",
	}
	for key, want := range checks {
		if got := m[key]; got != want {
			t.Errorf("%s: want %q, got %q", key, want, got)
		}
	}
}

func TestStripInboundMap(t *testing.T) {
	m := map[string]string{
		"X-Auth-User-ID":    "spoofed",
		"x-auth-tenant-id":  "spoofed-lower",
		"X-Aether-Grant-ID": "spoofed-grant",
		"Content-Type":      "application/json",
	}

	StripInboundMap(m)

	if _, ok := m["X-Auth-User-ID"]; ok {
		t.Error("X-Auth-User-ID should have been stripped")
	}
	if _, ok := m["x-auth-tenant-id"]; ok {
		t.Error("x-auth-tenant-id should have been stripped (case-insensitive)")
	}
	if _, ok := m["X-Aether-Grant-ID"]; ok {
		t.Error("X-Aether-Grant-ID should have been stripped")
	}
	if m["Content-Type"] != "application/json" {
		t.Error("Content-Type should be preserved")
	}
}

// stubResolver lets us drive ResolveAndMint without standing up an ACL service.
type stubResolver struct {
	resolved *acl.ResolvedAuthority
	err      error
}

func (s *stubResolver) ResolveAuthority(_ context.Context, _ models.Identity, _ acl.RequestAuthorityContext, _ acl.GrantAudienceContext) (*acl.ResolvedAuthority, error) {
	return s.resolved, s.err
}

func TestResolveAndMint_DirectMode_NoResolverCall(t *testing.T) {
	resolver := &stubResolver{err: errors.New("should not be called")}
	actor := Identity{UserID: "alice", PrincipalType: "User", WorkspaceAccess: 30}

	h, authority, err := ResolveAndMint(
		context.Background(),
		resolver,
		"tenant-1",
		actor,
		models.Identity{Type: models.PrincipalUser, ID: "alice"},
		nil,
		acl.GrantAudienceContext{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if authority != nil {
		t.Errorf("expected nil authority in direct mode, got %+v", authority)
	}
	if got := h.Get(HeaderAuthorityMode); got != AuthorityModeDirect {
		t.Errorf("AuthorityMode: want %q, got %q", AuthorityModeDirect, got)
	}
	if got := h.Get(HeaderUserID); got != "alice" {
		t.Errorf("UserID: want alice, got %q", got)
	}
}

func TestResolveAndMint_OBOMode_PopulatesAuthorityHeaders(t *testing.T) {
	actorID := models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"}
	grant := &acl.AuthorityGrant{
		GrantID:        "g1",
		SubjectType:    "user",
		SubjectID:      "alice",
		AudienceType:   "service",
		AudienceID:     "sv::foo::bar",
		MaxAccessLevel: 20,
		WorkspaceScope: []string{"ws1"},
	}
	resolver := &stubResolver{
		resolved: &acl.ResolvedAuthority{
			Actor:   actorID,
			Subject: models.Identity{Type: models.PrincipalUser, ID: "alice"},
			Grant:   grant,
		},
	}

	actor := Identity{UserID: "sv::foo::bar", PrincipalType: "Service", WorkspaceAccess: 20}
	authCtx := &AuthorizationContext{
		Mode:    AuthorityModeOnBehalfOf,
		Subject: models.Identity{Type: models.PrincipalUser, ID: "alice"},
		GrantID: "g1",
	}

	h, authority, err := ResolveAndMint(
		context.Background(),
		resolver,
		"tenant-1",
		actor,
		actorID,
		authCtx,
		acl.GrantAudienceContext{Actor: actorID},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if authority == nil {
		t.Fatal("expected non-nil authority in OBO mode")
	}
	if authority.GrantID != "g1" {
		t.Errorf("authority.GrantID: want g1, got %q", authority.GrantID)
	}

	checks := map[string]string{
		HeaderAuthorityMode: AuthorityModeOnBehalfOf,
		HeaderActorID:       "sv::foo::bar",
		HeaderUserID:        "alice", // backward-compat: subject overrides UserID
		HeaderGrantID:       "g1",
		HeaderSubjectID:     "alice",
	}
	for header, want := range checks {
		if got := h.Get(header); got != want {
			t.Errorf("%s: want %q, got %q", header, want, got)
		}
	}
}

func TestResolveAndMint_ResolverError_PropagatesError(t *testing.T) {
	resolver := &stubResolver{err: acl.ErrAuthorityGrantNotFound}
	authCtx := &AuthorizationContext{
		Mode:    AuthorityModeOnBehalfOf,
		Subject: models.Identity{Type: models.PrincipalUser, ID: "alice"},
		GrantID: "g-missing",
	}
	_, _, err := ResolveAndMint(
		context.Background(),
		resolver,
		"t",
		Identity{},
		models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"},
		authCtx,
		acl.GrantAudienceContext{},
	)
	if !errors.Is(err, acl.ErrAuthorityGrantNotFound) {
		t.Fatalf("expected ErrAuthorityGrantNotFound, got %v", err)
	}
}

func TestMint_CallerTopic_DirectMode(t *testing.T) {
	h := Mint(context.Background(), "tenant-1", Identity{
		UserID:        "alice",
		PrincipalType: "User",
		CallerTopic:   "ag.ws.myagent.spec",
	})
	if got := h.Get(HeaderXAetherCallerTopic); got != "ag.ws.myagent.spec" {
		t.Errorf("CallerTopic: want %q, got %q", "ag.ws.myagent.spec", got)
	}
	if got := h.Get(HeaderXAetherCallerSubject); got != "" {
		t.Errorf("CallerSubject should be empty in direct mode, got %q", got)
	}
}

func TestMint_CallerTopic_OBOMode(t *testing.T) {
	h := Mint(context.Background(), "tenant-1", Identity{
		UserID:        "sv::foo::bar",
		PrincipalType: "Service",
		CallerTopic:   "ag.ws.myagent.spec",
		CallerSubject: "alice",
		Authority: &AuthenticatedAuthority{
			ActorType:      "Service",
			ActorID:        "sv::foo::bar",
			GrantID:        "g1",
			SubjectType:    "User",
			SubjectID:      "alice",
			AudienceType:   "service",
			AudienceID:     "sv::foo::bar",
			MaxAccessLevel: 20,
		},
	})
	if got := h.Get(HeaderXAetherCallerTopic); got != "ag.ws.myagent.spec" {
		t.Errorf("CallerTopic: want %q, got %q", "ag.ws.myagent.spec", got)
	}
	if got := h.Get(HeaderXAetherCallerSubject); got != "alice" {
		t.Errorf("CallerSubject: want %q, got %q", "alice", got)
	}
}

func TestMint_CallerTopic_Empty_OmitsHeaders(t *testing.T) {
	h := Mint(context.Background(), "t", Identity{
		UserID:        "alice",
		PrincipalType: "User",
	})
	if got := h.Get(HeaderXAetherCallerTopic); got != "" {
		t.Errorf("CallerTopic should be absent when empty, got %q", got)
	}
	if got := h.Get(HeaderXAetherCallerSubject); got != "" {
		t.Errorf("CallerSubject should be absent when empty, got %q", got)
	}
}

func TestStripInbound_StripsCallerHeaders(t *testing.T) {
	h := http.Header{}
	h.Set(HeaderXAetherCallerTopic, "ag.ws.myagent.spec")
	h.Set(HeaderXAetherCallerSubject, "alice")
	h.Set("Content-Type", "application/json")

	StripInbound(h)

	if got := h.Get(HeaderXAetherCallerTopic); got != "" {
		t.Errorf("CallerTopic should have been stripped, got %q", got)
	}
	if got := h.Get(HeaderXAetherCallerSubject); got != "" {
		t.Errorf("CallerSubject should have been stripped, got %q", got)
	}
	if got := h.Get("Content-Type"); got != "application/json" {
		t.Error("Content-Type should be preserved")
	}
}

func TestResolveAndMint_OBOMode_RejectsMissingGrantID(t *testing.T) {
	authCtx := &AuthorizationContext{
		Mode:    AuthorityModeOnBehalfOf,
		Subject: models.Identity{Type: models.PrincipalUser, ID: "alice"},
		// GrantID intentionally empty
	}
	_, _, err := ResolveAndMint(
		context.Background(),
		&stubResolver{},
		"t",
		Identity{},
		models.Identity{Type: models.PrincipalService, ID: "sv::foo::bar"},
		authCtx,
		acl.GrantAudienceContext{},
	)
	if err == nil {
		t.Fatal("expected error for missing grant_id")
	}
}

// TestExpandSubjectInheritedScope verifies the magic acl.WorkspaceScopeSubjectInherited
// value ("_subject_workspaces") is expanded to "*" for the minted
// X-Auth-Workspace-Scope header (terminators don't understand the magic value;
// minting it raw rejected every workspace). Concrete workspaces pass through.
func TestExpandSubjectInheritedScope(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"subject-inherited -> star", []string{"_subject_workspaces"}, []string{"*"}},
		{"concrete passthrough", []string{"ws1", "ws2"}, []string{"ws1", "ws2"}},
		{"mixed dedups star", []string{"ws1", "_subject_workspaces", "ws2"}, []string{"ws1", "*", "ws2"}},
		{"empty", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := expandSubjectInheritedScope(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len(got)=%d want %d (got=%v)", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("got[%d]=%q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}
