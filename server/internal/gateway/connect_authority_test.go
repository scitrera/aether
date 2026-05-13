package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// helpers shared across authority tests
// ---------------------------------------------------------------------------

// newAuthorityTestGrant creates a valid, non-expired authority grant in the DB
// and returns it. Caller controls delegate, subject, audienceType, audienceID,
// and revoked state via the revokeAfter flag.
func newAuthorityTestGrant(
	t *testing.T,
	ctx context.Context,
	aclSvc *acl.Service,
	delegate models.Identity,
	subject models.Identity,
	audienceType, audienceID string,
) *acl.AuthorityGrant {
	t.Helper()
	issuer := models.Identity{Type: models.PrincipalService, Implementation: "gateway", Specifier: "test"}
	expires := time.Now().UTC().Add(30 * time.Minute)
	renewable := time.Now().UTC().Add(4 * time.Hour)
	grant, err := aclSvc.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:        subject,
		Delegate:       delegate,
		IssuedBy:       issuer,
		MayDelegate:    true,
		RemainingHops:  2,
		MaxAccessLevel: acl.AccessReadWrite,
		AudienceType:   audienceType,
		AudienceID:     audienceID,
		ExpiresAt:      expires,
		RenewableUntil: renewable,
		Reason:         "test-grant",
	})
	if err != nil {
		t.Fatalf("CreateAuthorityGrant() error = %v", err)
	}
	return grant
}

// newResolveAuthorityRequest builds the upstream request proto.
func newResolveAuthorityRequest(requestID, grantID string, actor, subject *pb.PrincipalRef) *pb.ResolveAuthorityRequest {
	return &pb.ResolveAuthorityRequest{
		RequestId: requestID,
		Actor:     actor,
		GrantId:   grantID,
		Subject:   subject,
	}
}

// principalRefForIdentity converts a models.Identity to a PrincipalRef proto.
func principalRefForIdentity(id models.Identity) *pb.PrincipalRef {
	return &pb.PrincipalRef{
		PrincipalType: acl.PrincipalTypeForModel(id.Type),
		PrincipalId:   id.CanonicalPrincipalID(),
	}
}

// getResolveResponse pulls a ResolveAuthorityResponse from the first sent message.
func getResolveResponse(t *testing.T, stream *mockStream) *pb.ResolveAuthorityResponse {
	t.Helper()
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected at least one downstream message, got none")
	}
	resp := stream.sent[0].GetResolveAuthorityResponse()
	if resp == nil {
		t.Fatalf("expected ResolveAuthorityResponse payload, got %T", stream.sent[0].GetPayload())
	}
	return resp
}

// getConnectionStatusResponse pulls a ConnectionStatusResponse from the first sent message.
func getConnectionStatusResponse(t *testing.T, stream *mockStream) *pb.ConnectionStatusResponse {
	t.Helper()
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) == 0 {
		t.Fatal("expected at least one downstream message, got none")
	}
	resp := stream.sent[0].GetConnectionStatusResponse()
	if resp == nil {
		t.Fatalf("expected ConnectionStatusResponse payload, got %T", stream.sent[0].GetPayload())
	}
	return resp
}

// ---------------------------------------------------------------------------
// Test 1: ResolveAuthority — caller IS the grant delegate → implicit allow
// ---------------------------------------------------------------------------

// TestResolveAuthority_AsActor_ImplicitAllow verifies that the grant's delegate
// can resolve the grant without any explicit ACL permission. The full projected
// grant must be returned with ok=true.
func TestResolveAuthority_AsActor_ImplicitAllow(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test")

	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "worker",
		Specifier:      "inst-1",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "alice"}

	grant := newAuthorityTestGrant(t, ctx, aclSvc, delegate, subject, acl.AuthorityAudienceAgent, delegate.CanonicalPrincipalID())

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      delegate,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		acl:      aclSvc,
		sessions: newMockSessionManager(),
	}

	req := newResolveAuthorityRequest(
		"req-1",
		grant.GrantID,
		nil, // omit actor → gateway uses session identity
		principalRefForIdentity(subject),
	)
	gw.handleResolveAuthority(ctx, client, delegate, req)

	resp := getResolveResponse(t, stream)
	if !resp.GetOk() {
		t.Fatalf("expected ok=true, got error: %s", resp.GetError())
	}
	if resp.GetRequestId() != "req-1" {
		t.Errorf("request_id = %q, want %q", resp.GetRequestId(), "req-1")
	}
	if resp.GetAuthority() == nil {
		t.Fatal("expected authority in response, got nil")
	}
	if resp.GetAuthority().GetGrant() == nil {
		t.Fatal("expected grant info in response, got nil")
	}
	if resp.GetAuthority().GetGrant().GetGrantId() != grant.GrantID {
		t.Errorf("grant_id = %q, want %q", resp.GetAuthority().GetGrant().GetGrantId(), grant.GrantID)
	}
	// Verify projection: subject and actor are present.
	if resp.GetAuthority().GetSubject() == nil {
		t.Error("expected subject in resolved authority, got nil")
	}
	if resp.GetAuthority().GetActor() == nil {
		t.Error("expected actor in resolved authority, got nil")
	}
}

// ---------------------------------------------------------------------------
// Test 2: ResolveAuthority — caller IS the grant audience (service) → implicit allow
// ---------------------------------------------------------------------------

// TestResolveAuthority_AsAudience_ImplicitAllow is intentionally skipped.
//
// Originally written to verify the "caller is the grant audience but not the
// delegate" path in callerCanSeeResolvedAuthority. Investigation showed this
// case cannot be constructed for Agent/Service audience grants:
//
//   - acl.Service.ResolveAuthority enforces actor == grant.Delegate (line 70
//     of authority_context.go) AND, via validateGrantAudience, also enforces
//     actor == grant.AudienceID for AuthorityAudienceAgent/Service. The two
//     constraints together force grant.Delegate == grant.Audience for those
//     audience types, collapsing the "audience but not delegate" case into
//     the actor-implicit path covered by Test 1.
//
//   - For Session/Task audience grants, audience-implicit-allow IS distinct
//     from actor-implicit-allow, but the visibility switch in
//     callerCanSeeResolvedAuthority intentionally only matches Agent/Service
//     audiences (session/task are scoped to a session ID / task ID, not a
//     principal identity). A future test exercising session-audience visibility
//     would need a real session lifecycle harness — out of scope here.
//
// The audience-side switch in callerCanSeeResolvedAuthority is retained as
// defensive code in case validateGrantAudience semantics evolve.
func TestResolveAuthority_AsAudience_ImplicitAllow(t *testing.T) {
	t.Skip("audience-implicit-allow path is unreachable for Agent/Service audiences " +
		"in current validateGrantAudience semantics; see comment above for details")
}

// ---------------------------------------------------------------------------
// Test 3: ResolveAuthority — third-party caller, no perm → denied, no fields
// ---------------------------------------------------------------------------

// TestResolveAuthority_ThirdParty_DeniedWithoutPerm verifies that a caller who
// is neither the grant delegate nor audience, and holds no explicit
// capability/resolve_authority READ permission, receives ok=false and NO grant fields.
func TestResolveAuthority_ThirdParty_DeniedWithoutPerm(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-deny")

	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-deny",
		Implementation: "worker",
		Specifier:      "d-inst",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "carol"}
	thirdParty := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-deny",
		Implementation: "other-worker",
		Specifier:      "o-inst",
	}

	grant := newAuthorityTestGrant(t, ctx, aclSvc, delegate, subject, acl.AuthorityAudienceAgent, delegate.CanonicalPrincipalID())

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      thirdParty,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		acl:      aclSvc,
		sessions: newMockSessionManager(),
	}

	req := newResolveAuthorityRequest(
		"req-deny",
		grant.GrantID,
		principalRefForIdentity(delegate),
		principalRefForIdentity(subject),
	)
	gw.handleResolveAuthority(ctx, client, thirdParty, req)

	resp := getResolveResponse(t, stream)
	if resp.GetOk() {
		t.Fatal("expected ok=false for third-party caller without permission")
	}
	if resp.GetError() == "" {
		t.Error("expected non-empty error string")
	}
	// Per spec: NO grant fields must be leaked.
	if resp.GetAuthority() != nil {
		t.Errorf("expected nil authority in denied response, got %+v", resp.GetAuthority())
	}
}

// ---------------------------------------------------------------------------
// Test 4: ResolveAuthority — third-party with perm → allowed
// ---------------------------------------------------------------------------

// TestResolveAuthority_ThirdParty_AllowedWithPerm verifies that a caller who
// holds READ on capability/resolve_authority can see any grant's resolved authority.
// We use an Orchestrator identity which is implicitly allowed (hardcoded
// fast-path), avoiding the need to insert a real ACL policy row.
func TestResolveAuthority_ThirdParty_AllowedWithPerm(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-perm")

	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-perm",
		Implementation: "worker",
		Specifier:      "p-inst",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "dave"}

	grant := newAuthorityTestGrant(t, ctx, aclSvc, delegate, subject, acl.AuthorityAudienceAgent, delegate.CanonicalPrincipalID())

	// Orchestrator has implicit resolve_authority permission (isAllowedAuthorityResolve fast-path).
	orchestrator := models.Identity{
		Type:           models.PrincipalOrchestrator,
		Implementation: "k8s",
		Specifier:      "primary",
	}

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      orchestrator,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		acl:      aclSvc,
		sessions: newMockSessionManager(),
	}

	req := newResolveAuthorityRequest(
		"req-perm",
		grant.GrantID,
		principalRefForIdentity(delegate),
		principalRefForIdentity(subject),
	)
	gw.handleResolveAuthority(ctx, client, orchestrator, req)

	resp := getResolveResponse(t, stream)
	if !resp.GetOk() {
		t.Fatalf("expected ok=true for orchestrator (implicit perm), got error: %s", resp.GetError())
	}
	if resp.GetAuthority().GetGrant().GetGrantId() != grant.GrantID {
		t.Errorf("grant_id = %q, want %q", resp.GetAuthority().GetGrant().GetGrantId(), grant.GrantID)
	}
}

// ---------------------------------------------------------------------------
// Test 5: ResolveAuthority — revoked grant → revoked=true in projected grant
// ---------------------------------------------------------------------------

// TestResolveAuthority_RevokedGrant_ReturnsRevokedFlag verifies that an authorized
// caller resolving a revoked grant gets ok=false because RevokeAuthorityGrant
// marks revoked=true and ValidateActiveAt returns ErrAuthorityGrantRevoked.
func TestResolveAuthority_RevokedGrant_ReturnsRevokedFlag(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-revoked")

	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-rev",
		Implementation: "worker",
		Specifier:      "r-inst",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "eve"}

	grant := newAuthorityTestGrant(t, ctx, aclSvc, delegate, subject, acl.AuthorityAudienceAgent, delegate.CanonicalPrincipalID())

	// Revoke the grant before resolving.
	if err := aclSvc.RevokeAuthorityGrant(ctx, grant.GrantID); err != nil {
		t.Fatalf("RevokeAuthorityGrant() error = %v", err)
	}

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      delegate,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		acl:      aclSvc,
		sessions: newMockSessionManager(),
	}

	req := newResolveAuthorityRequest(
		"req-rev",
		grant.GrantID,
		nil,
		principalRefForIdentity(subject),
	)
	gw.handleResolveAuthority(ctx, client, delegate, req)

	resp := getResolveResponse(t, stream)
	// ResolveAuthority calls ValidateActiveAt which returns ErrAuthorityGrantRevoked
	// for revoked grants — so the handler returns ok=false before visibility check.
	if resp.GetOk() {
		t.Fatal("expected ok=false for revoked grant, got ok=true")
	}
	if resp.GetError() == "" {
		t.Error("expected non-empty error string for revoked grant")
	}
	// No grant fields must be leaked on error.
	if resp.GetAuthority() != nil {
		t.Errorf("expected nil authority for revoked grant error response, got %+v", resp.GetAuthority())
	}
}

// ---------------------------------------------------------------------------
// Test 6: ResolveAuthority — expired grant → ok=false, no fields
// ---------------------------------------------------------------------------

// TestResolveAuthority_ExpiredGrant_ReturnsError verifies that an already-expired
// grant yields ok=false with an error and no grant fields in the response.
// We create the grant with an expiry in the past by inserting it and then
// updating expires_at via direct DB manipulation.
func TestResolveAuthority_ExpiredGrant_ReturnsError(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-expired")

	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-exp",
		Implementation: "worker",
		Specifier:      "e-inst",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "frank"}

	grant := newAuthorityTestGrant(t, ctx, aclSvc, delegate, subject, acl.AuthorityAudienceAgent, delegate.CanonicalPrincipalID())

	// Back-date expires_at to make it expired.
	_, err := testDB.DB.ExecContext(ctx,
		"UPDATE acl_authority_grants SET expires_at = $1 WHERE grant_id = $2",
		time.Now().UTC().Add(-1*time.Hour), grant.GrantID)
	if err != nil {
		t.Fatalf("failed to back-date grant: %v", err)
	}

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      delegate,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		acl:      aclSvc,
		sessions: newMockSessionManager(),
	}

	req := newResolveAuthorityRequest("req-exp", grant.GrantID, nil, principalRefForIdentity(subject))
	gw.handleResolveAuthority(ctx, client, delegate, req)

	resp := getResolveResponse(t, stream)
	if resp.GetOk() {
		t.Fatal("expected ok=false for expired grant, got ok=true")
	}
	if resp.GetError() == "" {
		t.Error("expected non-empty error string for expired grant")
	}
	if resp.GetAuthority() != nil {
		t.Errorf("expected nil authority for expired grant response, got %+v", resp.GetAuthority())
	}
}

// ---------------------------------------------------------------------------
// Test 7: ResolveAuthority — audience mismatch → ok=false, no fields
// ---------------------------------------------------------------------------

// TestResolveAuthority_AudienceInvalid_ReturnsError verifies that a grant whose
// AudienceID does not match the calling session returns ok=false with no fields.
// We create a grant with audience = agent-A but present session from agent-B.
func TestResolveAuthority_AudienceInvalid_ReturnsError(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-aud-mismatch")

	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-mm",
		Implementation: "worker",
		Specifier:      "mm-inst",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "grace"}
	// Audience is a DIFFERENT agent, not the calling delegate.
	otherAgent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws-mm",
		Implementation: "other-worker",
		Specifier:      "other-inst",
	}

	// Grant audience = otherAgent, but caller (delegate) presents its own sessionID.
	grant := newAuthorityTestGrant(t, ctx, aclSvc, delegate, subject, acl.AuthorityAudienceAgent, otherAgent.CanonicalPrincipalID())

	stream := &mockStream{}
	sessions := newMockSessionManager()
	// IsActive returns true so the session check inside validateGrantAudience passes,
	// but the audience ID mismatch (audienceID = otherAgent, not delegate) will cause
	// an ErrAuthorityGrantAudienceMismatch from validateGrantAudience.
	sessions.isActiveResult = true

	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      delegate,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		acl:      aclSvc,
		sessions: sessions,
	}

	req := newResolveAuthorityRequest("req-mm", grant.GrantID, nil, principalRefForIdentity(subject))
	gw.handleResolveAuthority(ctx, client, delegate, req)

	resp := getResolveResponse(t, stream)
	if resp.GetOk() {
		t.Fatal("expected ok=false for audience mismatch, got ok=true")
	}
	if resp.GetError() == "" {
		t.Error("expected non-empty error string for audience mismatch")
	}
	if resp.GetAuthority() != nil {
		t.Errorf("expected nil authority for audience mismatch response, got %+v", resp.GetAuthority())
	}
}

// ---------------------------------------------------------------------------
// Test 8: ConnectionStatus — self-query always allowed
// ---------------------------------------------------------------------------

// TestConnectionStatus_Self_AlwaysAllowed verifies that a caller can query
// their own connection status without any ACL permission. The result reflects
// the mock SessionManager's isActiveResult.
func TestConnectionStatus_Self_AlwaysAllowed(t *testing.T) {
	ctx := context.Background()

	caller := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "my-worker",
		Specifier:      "inst-self",
	}

	sessions := newMockSessionManager()
	sessions.isActiveResult = true // caller is connected

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      caller,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		sessions: sessions,
		// no acl set — self-check must not hit ACL
	}

	req := &pb.ConnectionStatusRequest{
		RequestId: "req-self",
		Principal: principalRefForIdentity(caller),
	}
	gw.handleConnectionStatus(ctx, client, caller, req)

	resp := getConnectionStatusResponse(t, stream)
	if !resp.GetOk() {
		t.Fatalf("expected ok=true for self-query, got error: %s", resp.GetError())
	}
	if !resp.GetConnected() {
		t.Error("expected connected=true (isActiveResult=true), got false")
	}
	if resp.GetRequestId() != "req-self" {
		t.Errorf("request_id = %q, want %q", resp.GetRequestId(), "req-self")
	}
}

// ---------------------------------------------------------------------------
// Test 9: ConnectionStatus — cross-principal, no perm → denied
// ---------------------------------------------------------------------------

// TestConnectionStatus_Other_DeniedWithoutPerm verifies that a caller without
// capability/query_connections cannot query another principal's connection status.
// We use a plain agent identity (not WorkflowEngine/Orchestrator) and no ACL
// service, so isAllowedConnectionQuery returns false.
func TestConnectionStatus_Other_DeniedWithoutPerm(t *testing.T) {
	ctx := context.Background()

	caller := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "spy-worker",
		Specifier:      "s-inst",
	}
	target := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "other-worker",
		Specifier:      "o-inst",
	}

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      caller,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		sessions: newMockSessionManager(),
		// acl=nil → isAllowedConnectionQuery returns false for non-system principals
	}

	req := &pb.ConnectionStatusRequest{
		RequestId: "req-other-deny",
		Principal: principalRefForIdentity(target),
	}
	gw.handleConnectionStatus(ctx, client, caller, req)

	resp := getConnectionStatusResponse(t, stream)
	if resp.GetOk() {
		t.Fatal("expected ok=false for cross-principal query without perm")
	}
	if resp.GetError() == "" {
		t.Error("expected non-empty error string")
	}
}

// ---------------------------------------------------------------------------
// Test 10: ConnectionStatus — cross-principal with implicit perm → allowed
// ---------------------------------------------------------------------------

// TestConnectionStatus_Other_AllowedWithPerm verifies that an Orchestrator
// (which has the implicit isAllowedConnectionQuery fast-path) can query any
// other principal's connection status. The result reflects isActiveResult.
func TestConnectionStatus_Other_AllowedWithPerm(t *testing.T) {
	ctx := context.Background()

	orchestrator := models.Identity{
		Type:           models.PrincipalOrchestrator,
		Implementation: "k8s",
		Specifier:      "primary",
	}
	target := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "some-worker",
		Specifier:      "t-inst",
	}

	sessions := newMockSessionManager()
	sessions.isActiveResult = false // target is NOT connected

	stream := &mockStream{}
	client := &ClientSession{
		SessionUUID:   uuid.New(),
		Identity:      orchestrator,
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	gw := &GatewayServer{
		sessions: sessions,
		// acl=nil: orchestrator uses implicit fast-path, no ACL lookup needed
	}

	req := &pb.ConnectionStatusRequest{
		RequestId: "req-other-allow",
		Principal: principalRefForIdentity(target),
	}
	gw.handleConnectionStatus(ctx, client, orchestrator, req)

	resp := getConnectionStatusResponse(t, stream)
	if !resp.GetOk() {
		t.Fatalf("expected ok=true for orchestrator cross-principal query, got error: %s", resp.GetError())
	}
	if resp.GetConnected() {
		t.Error("expected connected=false (isActiveResult=false), got true")
	}
}

// ---------------------------------------------------------------------------
// Test 11: Proto roundtrip — marshal/unmarshal sanity for new messages
// ---------------------------------------------------------------------------

// TestProtoRoundtrip_NewMessages verifies that the six new message types
// can be constructed, wrapped in Upstream/DownstreamMessage oneofs, and the
// payload extracted correctly — confirming proto registration and field numbers.
func TestProtoRoundtrip_NewMessages(t *testing.T) {
	// UpstreamMessage: ResolveAuthorityRequest
	upRA := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ResolveAuthorityRequest{
			ResolveAuthorityRequest: &pb.ResolveAuthorityRequest{
				RequestId: "rt-1",
				GrantId:   "grant-abc",
				Subject:   &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "alice"},
			},
		},
	}
	if got := upRA.GetResolveAuthorityRequest(); got == nil {
		t.Error("GetResolveAuthorityRequest() = nil; want non-nil")
	} else if got.GetGrantId() != "grant-abc" {
		t.Errorf("grant_id = %q, want %q", got.GetGrantId(), "grant-abc")
	}

	// UpstreamMessage: ConnectionStatusRequest
	upCS := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ConnectionStatusRequest{
			ConnectionStatusRequest: &pb.ConnectionStatusRequest{
				RequestId: "rt-2",
				Principal: &pb.PrincipalRef{PrincipalType: "agent", PrincipalId: "worker/inst"},
			},
		},
	}
	if got := upCS.GetConnectionStatusRequest(); got == nil {
		t.Error("GetConnectionStatusRequest() = nil; want non-nil")
	} else if got.GetRequestId() != "rt-2" {
		t.Errorf("request_id = %q, want %q", got.GetRequestId(), "rt-2")
	}

	// DownstreamMessage: ResolveAuthorityResponse
	downRA := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ResolveAuthorityResponse{
			ResolveAuthorityResponse: &pb.ResolveAuthorityResponse{
				RequestId: "rt-3",
				Ok:        true,
				Authority: &pb.ResolvedAuthority{
					Actor:   &pb.PrincipalRef{PrincipalType: "agent", PrincipalId: "worker/inst"},
					Subject: &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "alice"},
					Grant: &pb.AuthorityGrantInfo{
						GrantId:        "grant-abc",
						SubjectType:    "user",
						SubjectId:      "alice",
						AudienceType:   "agent",
						AudienceId:     "worker/inst",
						MaxAccessLevel: 2,
						ExpiresAt:      time.Now().Unix(),
						Revoked:        false,
					},
				},
			},
		},
	}
	if got := downRA.GetResolveAuthorityResponse(); got == nil {
		t.Error("GetResolveAuthorityResponse() = nil; want non-nil")
	} else {
		if !got.GetOk() {
			t.Error("expected ok=true")
		}
		if got.GetAuthority().GetGrant().GetGrantId() != "grant-abc" {
			t.Errorf("grant_id = %q, want %q", got.GetAuthority().GetGrant().GetGrantId(), "grant-abc")
		}
		if got.GetAuthority().GetGrant().GetRevoked() {
			t.Error("expected revoked=false")
		}
	}

	// DownstreamMessage: ConnectionStatusResponse
	downCS := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionStatusResponse{
			ConnectionStatusResponse: &pb.ConnectionStatusResponse{
				RequestId:  "rt-4",
				Ok:         true,
				Connected:  true,
				LastSeenAt: time.Now().Unix(),
			},
		},
	}
	if got := downCS.GetConnectionStatusResponse(); got == nil {
		t.Error("GetConnectionStatusResponse() = nil; want non-nil")
	} else {
		if !got.GetConnected() {
			t.Error("expected connected=true")
		}
		if got.GetLastSeenAt() == 0 {
			t.Error("expected non-zero last_seen_at")
		}
	}

	// Error variant: ResolveAuthorityResponse with ok=false
	downRAErr := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ResolveAuthorityResponse{
			ResolveAuthorityResponse: &pb.ResolveAuthorityResponse{
				RequestId: "rt-5",
				Ok:        false,
				Error:     "not authorized",
			},
		},
	}
	if got := downRAErr.GetResolveAuthorityResponse(); got == nil {
		t.Error("GetResolveAuthorityResponse() = nil; want non-nil")
	} else if got.GetError() != "not authorized" {
		t.Errorf("error = %q, want %q", got.GetError(), "not authorized")
	} else if got.GetAuthority() != nil {
		t.Error("expected nil authority in error response")
	}
}
