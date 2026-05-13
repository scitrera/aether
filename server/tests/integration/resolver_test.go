//go:build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/pkg/identityheaders"
	"github.com/scitrera/aether/pkg/models"
)

// stubResolver is a deterministic identityheaders.AuthorityResolver used in
// the OBO byte-equivalence test. Tests configure a single canonical authority;
// every ResolveAuthority call returns it (after sanity-checking the
// actor/grant/subject so we still exercise the same input shape the gateway
// would).
type stubResolver struct {
	mu        sync.RWMutex
	authority *identityheaders.AuthenticatedAuthority
}

func newStubResolver() *stubResolver {
	return &stubResolver{}
}

func (s *stubResolver) setAuthority(a *identityheaders.AuthenticatedAuthority) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authority = a
}

// ResolveAuthority implements identityheaders.AuthorityResolver. Returns a
// synthesized acl.ResolvedAuthority that, when converted via
// identityheaders.AuthorityFromResolved, produces the configured
// AuthenticatedAuthority.
func (s *stubResolver) ResolveAuthority(_ context.Context, actor models.Identity, req acl.RequestAuthorityContext, _ acl.GrantAudienceContext) (*acl.ResolvedAuthority, error) {
	s.mu.RLock()
	a := s.authority
	s.mu.RUnlock()
	if a == nil {
		return nil, fmt.Errorf("stubResolver: no authority configured")
	}
	if req.GrantID != a.GrantID {
		return nil, fmt.Errorf("stubResolver: grant_id mismatch: got %q want %q", req.GrantID, a.GrantID)
	}
	if req.Subject.ID != a.SubjectID || string(req.Subject.Type) != a.SubjectType {
		return nil, fmt.Errorf("stubResolver: subject mismatch")
	}

	// Build a minimal acl.AuthorityGrant carrying the audience/scope fields
	// AuthorityFromResolved reads. The fields not exercised here (parent,
	// delegate, expiry) are left zero — they don't influence header minting.
	grant := &acl.AuthorityGrant{
		GrantID:         a.GrantID,
		SubjectType:     a.SubjectType,
		SubjectID:       a.SubjectID,
		RootSubjectType: a.RootSubjectType,
		RootSubjectID:   a.RootSubjectID,
		AudienceType:    a.AudienceType,
		AudienceID:      a.AudienceID,
		MaxAccessLevel:  a.MaxAccessLevel,
		WorkspaceScope:  append([]string(nil), a.WorkspaceScope...),
	}
	return &acl.ResolvedAuthority{
		Actor:   actor,
		Subject: req.Subject,
		Grant:   grant,
	}, nil
}

// addTerminatorWithIdle is a variant of addTerminator that pins the backend's
// idle_timeout_ms (controls http.Client.Timeout). Used by the idle-timeout
// scenario.
func (h *harness) addTerminatorWithIdle(t testingT, implementation, specifier, backendURL, tenantID, headerMode string, idleMs int64) *terminatorEntry {
	t.Helper()
	cfg := &proxysidecar.Config{
		Service: proxysidecar.ServiceConfig{
			Implementation: implementation,
			Specifier:      specifier,
		},
		Gateway: proxysidecar.GatewayConfig{
			Address:  "localhost:0",
			Insecure: true,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "primary",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           backendURL,
				AllowMethods:  []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
				AllowPaths:    []string{"/*"},
				MaxBodyBytes:  20 << 20,
				IdleTimeoutMs: idleMs,
				HeaderMode:    headerMode,
			}},
		},
		TenantID: tenantID,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	term, err := proxysidecar.NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}
	entry := &terminatorEntry{
		implementation: implementation,
		specifier:      specifier,
		t:              term,
	}
	entry.online.Store(true)
	h.mu.Lock()
	h.terminators = append(h.terminators, entry)
	h.mu.Unlock()
	return entry
}

// testingT is the subset of *testing.T used by addTerminatorWithIdle. Defined
// here so the helper can also be reused by harness benchmark code (none yet).
type testingT interface {
	Helper()
	Fatalf(format string, args ...interface{})
}

// iToA is a tiny stable string-of-int helper so test names don't depend on
// strconv-imports inside the proxy_e2e_test.go file.
func iToA(i int) string {
	return strconv.Itoa(i)
}
