package gateway

// Tests for the anonymous-cert + scoped user API-key connection-auth path in
// authenticateCredentials.
//
// These cover the claim<->key binding security gate added so the platform
// server can open per-user Aether connections using an ANONYMOUS transport
// cert plus a per-user API key, authenticated AS the real user:
//
//   - anonymous-cert + matching user key        -> authenticated as the user
//   - anonymous-cert + mismatched key id        -> PermissionDenied
//   - anonymous-cert + mismatched type          -> PermissionDenied
//   - anonymous-cert + NO credentials (user)    -> Unauthenticated  [Critical #1]
//   - anonymous-cert + NO credentials (agent)   -> Unauthenticated  [Critical #1]
//   - in-process bufconn + NO credentials       -> allowed (exemption)
//   - isImpersonablePrincipal logic             -> direct unit test
//   - no-cert path applies the SAME enforcement (no divergence)
//   - non-anonymous cert path is UNCHANGED (key is ignored; cert is authority)

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/pkg/models"
)

// fakeAPIKeyAuthenticator returns a fixed AuthResult for any credentials that
// contain an api_key, and (nil, nil) otherwise. It lets us drive the binding
// logic in authenticateCredentials without a real token store.
type fakeAPIKeyAuthenticator struct {
	result *auth.AuthResult
}

func (f *fakeAPIKeyAuthenticator) Name() string { return "api_key" }

func (f *fakeAPIKeyAuthenticator) Authenticate(_ context.Context, creds map[string]string) (*auth.AuthResult, error) {
	if creds[auth.CredKeyAPIKey] == "" && creds[auth.CredKeyXAPIKey] == "" {
		return nil, nil
	}
	return f.result, nil
}

// userKeyResult builds an AuthResult shaped like the real api_key
// authenticator's output for a user principal.
func userKeyResult(userID string) *auth.AuthResult {
	return &auth.AuthResult{
		Authenticated: true,
		Identity:      models.Identity{Type: models.PrincipalUser, ID: userID},
		Method:        "api_key",
		Metadata: map[string]interface{}{
			"token_id":           "tok-1",
			"workspace_patterns": []string{"*"},
		},
	}
}

func newBindingAuthHandler(result *auth.AuthResult) *AuthHandler {
	composite := auth.NewCompositeAuthenticator(&fakeAPIKeyAuthenticator{result: result})
	// mtlsRequired=false, mode strict — irrelevant here since authenticateMTLS
	// is not exercised by these tests; we call authenticateCredentials directly.
	return newAuthHandler(composite, false, MTLSModeStrict, nil, nil)
}

func userInit() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_User{
			User: &pb.UserIdentity{UserId: "drew", WindowId: "win-1"},
		},
		Credentials: map[string]string{auth.CredKeyAPIKey: "secret"},
	}
}

func TestAuthCreds_AnonymousCert_MatchingUserKey_AuthenticatesAsUser(t *testing.T) {
	h := newBindingAuthHandler(userKeyResult("drew"))
	claim := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	_, ident, err := h.authenticateCredentials(context.Background(), userInit(), claim, true /*hasCertificate*/, true /*isAnonymous*/)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalUser || ident.ID != "drew" {
		t.Fatalf("expected authenticated user drew, got type=%s id=%s", ident.Type, ident.ID)
	}
	if ident.Specifier != "win-1" {
		t.Errorf("expected window specifier preserved from claim, got %q", ident.Specifier)
	}
}

func TestAuthCreds_AnonymousCert_MismatchedKeyID_Denied(t *testing.T) {
	// Key authenticates user "alice" but the connection claims user "drew".
	h := newBindingAuthHandler(userKeyResult("alice"))
	claim := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	_, _, err := h.authenticateCredentials(context.Background(), userInit(), claim, true, true)
	if err == nil {
		t.Fatal("expected PermissionDenied for mismatched user key, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", status.Code(err))
	}
}

func TestAuthCreds_AnonymousCert_MismatchedType_Denied(t *testing.T) {
	// Key authenticates a Service principal but the connection claims a User.
	result := &auth.AuthResult{
		Authenticated: true,
		Identity:      models.Identity{Type: models.PrincipalService, ID: "frontend"},
		Method:        "api_key",
	}
	h := newBindingAuthHandler(result)
	claim := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	_, _, err := h.authenticateCredentials(context.Background(), userInit(), claim, true, true)
	if err == nil {
		t.Fatal("expected PermissionDenied for principal-type mismatch, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", status.Code(err))
	}
}

func TestAuthCreds_NoCert_MatchingUserKey_AuthenticatesAsUser(t *testing.T) {
	// The no-cert path must apply the SAME binding as the anonymous-cert path.
	h := newBindingAuthHandler(userKeyResult("drew"))
	claim := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	_, ident, err := h.authenticateCredentials(context.Background(), userInit(), claim, false /*hasCertificate*/, false /*isAnonymous*/)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalUser || ident.ID != "drew" {
		t.Fatalf("expected authenticated user drew, got type=%s id=%s", ident.Type, ident.ID)
	}
}

func TestAuthCreds_NoCert_MismatchedKeyID_Denied(t *testing.T) {
	h := newBindingAuthHandler(userKeyResult("alice"))
	claim := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	_, _, err := h.authenticateCredentials(context.Background(), userInit(), claim, false, false)
	if err == nil {
		t.Fatal("expected PermissionDenied for mismatched user key on no-cert path, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", status.Code(err))
	}
}

func TestAuthCreds_NonAnonymousCert_KeyIgnored_IdentityUnchanged(t *testing.T) {
	// On the strict/semi-strict cert path (hasCertificate=true, isAnonymous=false)
	// the api_key must NOT rebind identity: the cert is authoritative. Even a
	// mismatched key must leave the cert-derived identity untouched and NOT
	// fail (the key is simply not consulted for binding).
	h := newBindingAuthHandler(userKeyResult("alice"))
	certIdentity := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	_, ident, err := h.authenticateCredentials(context.Background(), userInit(), certIdentity, true /*hasCertificate*/, false /*isAnonymous*/)
	if err != nil {
		t.Fatalf("unexpected error on non-anonymous cert path: %v", err)
	}
	if ident.Type != models.PrincipalUser || ident.ID != "drew" {
		t.Fatalf("expected cert identity drew unchanged, got type=%s id=%s", ident.Type, ident.ID)
	}
}

// --- Critical #1 regression tests: no-credential reject on anonymous path ---

// noCreds returns an InitConnection that carries a user claim but NO credentials
// at all — simulating a caller that omits the api_key field entirely.
func noCreds() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_User{
			User: &pb.UserIdentity{UserId: "drew", WindowId: "win-1"},
		},
		Credentials: map[string]string{}, // deliberately empty
	}
}

// noCredsAgent returns an InitConnection with an Agent claim and no credentials.
func noCredsAgent() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "prod",
				Implementation: "classifier",
				Specifier:      "v2",
			},
		},
		Credentials: map[string]string{},
	}
}

// TestAuthCreds_AnonymousCert_NoCreds_User_Rejected is the CORE regression for
// Critical #1: an external anonymous-cert connection claiming a User principal
// with no api_key credential must be rejected Unauthenticated, not succeed.
// Previously the composite returned (nil,nil), the binding block was skipped,
// and authenticateCredentials returned success with an unauthenticated identity.
func TestAuthCreds_AnonymousCert_NoCreds_User_Rejected(t *testing.T) {
	h := newBindingAuthHandler(userKeyResult("drew")) // authenticator is wired but no key presented
	claim := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	_, _, err := h.authenticateCredentials(context.Background(), noCreds(), claim, true /*hasCertificate*/, true /*isAnonymous*/)
	if err == nil {
		t.Fatal("expected Unauthenticated when no credential presented on anonymous-cert path, got nil")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", status.Code(err))
	}
}

// TestAuthCreds_AnonymousCert_NoCreds_Agent_Rejected confirms the reject is not
// user-only: an Agent claim on an anonymous-cert path with no credential is also
// rejected. All impersonable principal types must require a credential.
func TestAuthCreds_AnonymousCert_NoCreds_Agent_Rejected(t *testing.T) {
	h := newBindingAuthHandler(userKeyResult("drew"))
	claim := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "prod",
		Implementation: "classifier",
		Specifier:      "v2",
	}

	_, _, err := h.authenticateCredentials(context.Background(), noCredsAgent(), claim, true /*hasCertificate*/, true /*isAnonymous*/)
	if err == nil {
		t.Fatal("expected Unauthenticated when agent claim presented with no credential on anonymous-cert path, got nil")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", status.Code(err))
	}
}

// TestAuthCreds_InProcess_NoCreds_Allowed verifies the in-process bufconn
// exemption: IsInProcessConn(ctx)==true bypasses the credential requirement so
// the embedded workflow engine (which never carries api-key credentials) can
// connect without triggering Unauthenticated.
func TestAuthCreds_InProcess_NoCreds_Allowed(t *testing.T) {
	h := newBindingAuthHandler(userKeyResult("drew"))
	claim := models.Identity{Type: models.PrincipalUser, ID: "drew", Specifier: "win-1"}

	// Inject the in-process context marker the same way InProcessUnaryInterceptor does.
	inProcCtx := context.WithValue(context.Background(), inProcessConnKey{}, true)

	_, ident, err := h.authenticateCredentials(inProcCtx, noCreds(), claim, true /*hasCertificate*/, true /*isAnonymous*/)
	if err != nil {
		t.Fatalf("in-process connection without credentials must not be rejected: %v", err)
	}
	// Identity should be the unmodified claim (no key to bind from).
	if ident.Type != models.PrincipalUser || ident.ID != "drew" {
		t.Errorf("expected claim identity preserved for in-process conn, got type=%s id=%s", ident.Type, ident.ID)
	}
}

// TestIsImpersonablePrincipal_Coverage directly unit-tests the helper so that
// the exempt system types and the impersonable worker types are both pinned.
func TestIsImpersonablePrincipal_Coverage(t *testing.T) {
	cases := []struct {
		pt           models.PrincipalType
		impersonable bool
	}{
		// System principals — exempt
		{models.PrincipalWorkflowEngine, false},
		{models.PrincipalOrchestrator, false},
		{models.PrincipalMetricsBridge, false},
		// Empty string — not a real type, must not require a credential
		{"", false},
		// Impersonable worker/user principals — all must require a credential
		{models.PrincipalUser, true},
		{models.PrincipalAgent, true},
		{models.PrincipalService, true},
		{models.PrincipalTask, true},
		{models.PrincipalBridge, true},
	}
	for _, tc := range cases {
		got := isImpersonablePrincipal(tc.pt)
		if got != tc.impersonable {
			t.Errorf("isImpersonablePrincipal(%q) = %v, want %v", tc.pt, got, tc.impersonable)
		}
	}
}
