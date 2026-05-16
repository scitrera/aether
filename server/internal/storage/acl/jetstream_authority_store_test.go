// Tests for JetStreamAuthorityStore — the gateway-facing Store decorator
// that routes authority-request lifecycle methods through the JetStream
// wrapper and passes every other Store method through to the inner.
//
// The tests use a real embedded NATS+JetStream server (so the event-publish
// side of the wrapper is exercised end-to-end), but the "inner Store" is a
// minimal fakeInnerStore that records every method invocation. Routing
// claims are then proven by:
//   - For lifecycle methods: a JetStream event MUST land on the authreq
//     stream's per-workspace subject (only the wrapper publishes events).
//   - For non-lifecycle methods: the fake inner's call counter MUST tick
//     up AND no JetStream side-effect must occur.

package acl_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/google/uuid"

	legacy "github.com/scitrera/aether/internal/acl"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// Compile-time interface assertion (Test 3)
// ---------------------------------------------------------------------------

// The decorator must satisfy aclstore.Store. This is also asserted inside
// the package itself; we keep a redundant one here so the test build catches
// drift even if the implementation file's assert is removed.
var _ aclstore.Store = (*aclstore.JetStreamAuthorityStore)(nil)

// ---------------------------------------------------------------------------
// Embedded NATS + JetStream helper (mirrors internal/acl test infra)
// ---------------------------------------------------------------------------

func startTestJS(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()

	opts := &natsserver.Options{
		Port:               -1,
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 64 * 1024 * 1024,
		JetStreamMaxStore:  256 * 1024 * 1024,
		NoSigs:             true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server new: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats server not ready")
	}

	conn, err := natsgo.Connect("", natsgo.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		t.Fatalf("jetstream new: %v", err)
	}

	stop := func() {
		_ = conn.Drain()
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	}
	return js, stop
}

// ---------------------------------------------------------------------------
// fakeInnerStore — minimal aclstore.Store implementation that tracks calls
// ---------------------------------------------------------------------------
//
// All ~40 methods are present (compile-time enforced via the interface assert
// at the top of the file). Most return zero values; only the lifecycle methods
// + a couple of non-lifecycle methods we care about for the passthrough test
// are wired to record + return synthetic data.

type fakeInnerStore struct {
	// Per-method call counters. Used by the passthrough test to verify
	// non-lifecycle methods hit the inner directly.
	checkAccessCalls         int
	getRuleCalls             int
	listAuthorityGrantCalls  int
	cleanupExpiredRulesCalls int

	// Per-lifecycle counters. Used by the routing test to verify lifecycle
	// methods also hit the inner (because the wrapper calls inner first
	// before doing the KV/event side-effect).
	submitCalls              int
	approveCalls             int
	denyCalls                int
	cancelCalls              int
	sweepCalls               int
	getAuthorityRequestCalls int

	// In-memory store keyed by request_id so the wrapper can read back the
	// pending row during Approve's snapshotStatus call.
	rows map[string]*aclstore.AuthorityRequest
}

func newFakeInnerStore() *fakeInnerStore {
	return &fakeInnerStore{rows: make(map[string]*aclstore.AuthorityRequest)}
}

// ----- Access checks -----

func (f *fakeInnerStore) CheckAccess(ctx context.Context, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*aclstore.Decision, error) {
	f.checkAccessCalls++
	// Return a non-nil decision so callers can branch on Allowed.
	return &aclstore.Decision{Allowed: true, EffectiveAccessLevel: requiredLevel}, nil
}

func (f *fakeInnerStore) CheckAccessWithAuthority(ctx context.Context, actor models.Identity, authority *aclstore.ResolvedAuthority, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*aclstore.Decision, error) {
	return &aclstore.Decision{Allowed: true, EffectiveAccessLevel: requiredLevel}, nil
}

func (f *fakeInnerStore) CanConnect(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return &aclstore.Decision{Allowed: true}, nil
}

func (f *fakeInnerStore) CanSendMessage(ctx context.Context, principal models.Identity, targetType, targetID, workspace string, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return &aclstore.Decision{Allowed: true}, nil
}

func (f *fakeInnerStore) CanManageWorkspace(ctx context.Context, principal models.Identity, workspace string, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return &aclstore.Decision{Allowed: true}, nil
}

func (f *fakeInnerStore) CanCreateWorkspace(ctx context.Context, principal models.Identity, sessionID uuid.UUID) (*aclstore.Decision, error) {
	return &aclstore.Decision{Allowed: true}, nil
}

// ----- Authority grants -----

func (f *fakeInnerStore) ResolveAuthority(ctx context.Context, actor models.Identity, req aclstore.RequestAuthorityContext, audience aclstore.GrantAudienceContext) (*aclstore.ResolvedAuthority, error) {
	return nil, nil
}

func (f *fakeInnerStore) CreateAuthorityGrant(ctx context.Context, req aclstore.CreateAuthorityGrantRequest) (*aclstore.AuthorityGrant, error) {
	return &aclstore.AuthorityGrant{}, nil
}

func (f *fakeInnerStore) GetAuthorityGrant(ctx context.Context, grantID string) (*aclstore.AuthorityGrant, error) {
	return &aclstore.AuthorityGrant{}, nil
}

func (f *fakeInnerStore) ListAuthorityGrants(ctx context.Context, filter aclstore.AuthorityGrantFilter) ([]*aclstore.AuthorityGrant, error) {
	f.listAuthorityGrantCalls++
	return nil, nil
}

func (f *fakeInnerStore) RenewAuthorityGrant(ctx context.Context, grantID string, expiresAt time.Time) (*aclstore.AuthorityGrant, error) {
	return &aclstore.AuthorityGrant{}, nil
}

func (f *fakeInnerStore) RenewAuthorityGrantOpts(ctx context.Context, grantID string, opts aclstore.RenewAuthorityGrantOpts) (*aclstore.AuthorityGrant, error) {
	return &aclstore.AuthorityGrant{}, nil
}

func (f *fakeInnerStore) RevokeAuthorityGrant(ctx context.Context, grantID string) error {
	return nil
}

func (f *fakeInnerStore) RevokeAuthorityGrantCascade(ctx context.Context, grantID string) ([]aclstore.RevokedAuthorityGrant, error) {
	return nil, nil
}

func (f *fakeInnerStore) ListVisibleGrants(ctx context.Context, filter aclstore.VisibleGrantsFilter) ([]*aclstore.AuthorityGrant, error) {
	return nil, nil
}

func (f *fakeInnerStore) FindVisibleDerivedGrant(ctx context.Context, parentGrantID string, target models.Identity, audienceType, audienceID string) (*aclstore.AuthorityGrant, error) {
	return nil, nil
}

// ----- Authority requests (Lifecycle + storage CRUD) -----

func (f *fakeInnerStore) CreateAuthorityRequest(ctx context.Context, req *aclstore.AuthorityRequest) error {
	if req.RequestID == "" {
		return errors.New("request_id required")
	}
	cp := *req
	f.rows[req.RequestID] = &cp
	return nil
}

func (f *fakeInnerStore) GetAuthorityRequest(ctx context.Context, requestID string) (*aclstore.AuthorityRequest, error) {
	f.getAuthorityRequestCalls++
	row, ok := f.rows[requestID]
	if !ok {
		return nil, aclstore.ErrAuthorityRequestNotFound
	}
	cp := *row
	return &cp, nil
}

func (f *fakeInnerStore) ListAuthorityRequests(ctx context.Context, filter aclstore.AuthorityRequestFilter) ([]*aclstore.AuthorityRequest, error) {
	return nil, nil
}

func (f *fakeInnerStore) ResolveAuthorityRequest(ctx context.Context, requestID string, status aclstore.AuthorityRequestStatus, resolvedBy models.Identity, resolutionReason string, grantedGrantID string, resolvedAt time.Time) error {
	row, ok := f.rows[requestID]
	if !ok {
		return aclstore.ErrAuthorityRequestNotFound
	}
	if row.Status != aclstore.AuthorityRequestStatusPending {
		return aclstore.ErrAuthorityRequestAlreadyResolved
	}
	row.Status = status
	row.ResolvedBy = resolvedBy
	row.ResolutionReason = resolutionReason
	row.GrantedGrantID = grantedGrantID
	row.ResolvedAt = &resolvedAt
	return nil
}

func (f *fakeInnerStore) CancelAuthorityRequest(ctx context.Context, requestID string, reason string) error {
	return f.ResolveAuthorityRequest(ctx, requestID, aclstore.AuthorityRequestStatusCancelled, models.Identity{}, reason, "", time.Now().UTC())
}

func (f *fakeInnerStore) ExpireAuthorityRequests(ctx context.Context, before time.Time, limit int) ([]*aclstore.AuthorityRequest, error) {
	return nil, nil
}

func (f *fakeInnerStore) SubmitAuthorityRequest(ctx context.Context, req *aclstore.AuthorityRequest) (*aclstore.AuthorityRequest, error) {
	f.submitCalls++
	if req.RequestID == "" {
		req.RequestID = "fake-" + time.Now().UTC().Format("150405.000000000")
	}
	if req.Status == "" {
		req.Status = aclstore.AuthorityRequestStatusPending
	}
	if req.CreatedAt.IsZero() {
		req.CreatedAt = time.Now().UTC()
	}
	if req.ExpiresAt.IsZero() {
		req.ExpiresAt = req.CreatedAt.Add(30 * time.Minute)
	}
	cp := *req
	f.rows[req.RequestID] = &cp
	out := cp
	return &out, nil
}

func (f *fakeInnerStore) ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *aclstore.ApproveDecision) (*aclstore.AuthorityRequest, error) {
	f.approveCalls++
	row, ok := f.rows[requestID]
	if !ok {
		return nil, aclstore.ErrAuthorityRequestNotFound
	}
	if row.Status != aclstore.AuthorityRequestStatusPending {
		return nil, aclstore.ErrAuthorityRequestAlreadyResolved
	}
	row.Status = aclstore.AuthorityRequestStatusApproved
	now := time.Now().UTC()
	row.ResolvedAt = &now
	row.ResolvedBy = approverIdentity
	row.GrantedGrantID = "grant-" + requestID
	if decision != nil {
		row.ResolutionReason = decision.Reason
	}
	out := *row
	return &out, nil
}

func (f *fakeInnerStore) DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*aclstore.AuthorityRequest, error) {
	f.denyCalls++
	row, ok := f.rows[requestID]
	if !ok {
		return nil, aclstore.ErrAuthorityRequestNotFound
	}
	if row.Status != aclstore.AuthorityRequestStatusPending {
		return nil, aclstore.ErrAuthorityRequestAlreadyResolved
	}
	row.Status = aclstore.AuthorityRequestStatusDenied
	now := time.Now().UTC()
	row.ResolvedAt = &now
	row.ResolvedBy = approverIdentity
	row.ResolutionReason = reason
	out := *row
	return &out, nil
}

func (f *fakeInnerStore) CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*aclstore.AuthorityRequest, error) {
	f.cancelCalls++
	row, ok := f.rows[requestID]
	if !ok {
		return nil, aclstore.ErrAuthorityRequestNotFound
	}
	if row.Status != aclstore.AuthorityRequestStatusPending {
		return nil, aclstore.ErrAuthorityRequestAlreadyResolved
	}
	row.Status = aclstore.AuthorityRequestStatusCancelled
	now := time.Now().UTC()
	row.ResolvedAt = &now
	row.ResolutionReason = reason
	out := *row
	return &out, nil
}

func (f *fakeInnerStore) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*aclstore.AuthorityRequest, error) {
	f.sweepCalls++
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var out []*aclstore.AuthorityRequest
	for _, r := range f.rows {
		if r.Status != aclstore.AuthorityRequestStatusPending {
			continue
		}
		if r.ExpiresAt.After(now) {
			continue
		}
		r.Status = aclstore.AuthorityRequestStatusExpired
		t := now
		r.ResolvedAt = &t
		cp := *r
		out = append(out, &cp)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// ----- Rules CRUD -----

func (f *fakeInnerStore) GrantAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) (*aclstore.Rule, error) {
	return &aclstore.Rule{}, nil
}

func (f *fakeInnerStore) RevokeAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string) error {
	return nil
}

func (f *fakeInnerStore) GetRule(ctx context.Context, principalType, principalID, resourceType, resourceID string) (*aclstore.Rule, error) {
	f.getRuleCalls++
	return &aclstore.Rule{
		PrincipalType: principalType,
		PrincipalID:   principalID,
		ResourceType:  resourceType,
		ResourceID:    resourceID,
	}, nil
}

func (f *fakeInnerStore) ListRules(ctx context.Context, filter aclstore.RuleFilter) ([]*aclstore.Rule, error) {
	return nil, nil
}

// ----- Fallback policy -----

func (f *fakeInnerStore) SetFallbackPolicy(ctx context.Context, ruleCategory string, fallbackAccessLevel int, updatedBy string) error {
	return nil
}

func (f *fakeInnerStore) GetFallbackPolicy(ctx context.Context, ruleCategory string) (*aclstore.FallbackPolicy, error) {
	return nil, aclstore.ErrFallbackPolicyNotFound
}

func (f *fakeInnerStore) InvalidateFallbackCache() {}

// ----- Cleanup -----

func (f *fakeInnerStore) CleanupExpiredRules(ctx context.Context) (int64, error) {
	f.cleanupExpiredRulesCalls++
	return 0, nil
}

// ----- Audit -----

func (f *fakeInnerStore) QueryAuditLog(ctx context.Context, filter aclstore.AuditLogFilter) ([]*aclstore.AuditLogEntry, error) {
	return nil, nil
}

func (f *fakeInnerStore) CleanupOldAuditLogs(ctx context.Context, retentionDays int) (int64, error) {
	return 0, nil
}

// ----- Lifecycle -----

func (f *fakeInnerStore) Close() error { return nil }

// ----- Prefix index hook -----

func (f *fakeInnerStore) SetPrefixIndex(p aclstore.PrefixLookup) {}

// Compile-time assertion: the fake inner satisfies aclstore.Store. If a method
// is added to Store and not added here, the test build fails — which is the
// signal to update the decorator/test in lockstep.
var _ aclstore.Store = (*fakeInnerStore)(nil)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func newTestRequest(workspace, requestID string) *aclstore.AuthorityRequest {
	requester := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	routingPrincipal := models.Identity{Type: models.PrincipalUser, ID: "bob"}
	return &aclstore.AuthorityRequest{
		RequestID:       requestID,
		RequestingActor: requester,
		WorkspaceScope:  []string{workspace},
		OperationScope:  []string{"read"},
		ResourceScope:   map[string][]string{"file": {"/x"}},
		RequestedAccess: 20,
		DurationSeconds: 60,
		AudienceType:    aclstore.AuthorityAudienceSession,
		AudienceID:      "sess-1",
		RoutingTarget: aclstore.AuthorityRequestRoutingTarget{
			Principal: &routingPrincipal,
		},
		Reason: "test",
	}
}

// drainEvents creates an ephemeral consumer filtered by the per-workspace
// subject and reports how many events landed within `maxWait`. Used by both
// the routing assertion (>=1 event after a lifecycle call) and the
// passthrough assertion (0 events after a non-lifecycle call).
func drainEvents(t *testing.T, ctx context.Context, js jetstream.JetStream, workspace string, want int, maxWait time.Duration) int {
	t.Helper()
	subject := legacy.WorkspaceEventSubject(workspace)
	stream, err := js.Stream(ctx, legacy.AuthorityRequestsStream)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	fetchCount := want
	if fetchCount == 0 {
		fetchCount = 1 // ensure Fetch isn't a no-op
	}
	batch, err := cons.Fetch(fetchCount, jetstream.FetchMaxWait(maxWait))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	count := 0
	for msg := range batch.Messages() {
		count++
		_ = msg.Ack()
	}
	return count
}

// ---------------------------------------------------------------------------
// Test 1: lifecycle methods route through the JetStream wrapper
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityStore_LifecycleMethods_RouteThroughWrapper exercises
// the 6 lifecycle methods through the Store decorator and asserts each one
// produced a JetStream event on the per-workspace subject. Events are only
// produced by the wrapper — the inner fake has no JetStream client. Therefore
// "event landed" ⇒ "the wrapper saw the call" ⇒ "the decorator routed
// correctly".
//
// Additionally we verify the inner counters tick up (the wrapper's contract
// is to call the inner first, then publish), confirming the wrapper still
// preserves the SQL/Postgres-side write path.
func TestJetStreamAuthorityStore_LifecycleMethods_RouteThroughWrapper(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeInnerStore()
	decorator, err := aclstore.NewJetStreamAuthorityStore(inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new decorator: %v", err)
	}

	// ----- Submit -----
	ws := "ws-route"
	req := newTestRequest(ws, "req-route-1")
	if _, err := decorator.SubmitAuthorityRequest(ctx, req); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if inner.submitCalls != 1 {
		t.Errorf("inner submit_calls = %d, want 1 (wrapper must invoke inner)", inner.submitCalls)
	}
	if got := drainEvents(t, ctx, js, ws, 1, 2*time.Second); got < 1 {
		t.Errorf("submit produced %d events on subject %s, want >= 1 (routing failed)", got, legacy.WorkspaceEventSubject(ws))
	}

	// ----- Approve -----
	wsApprove := "ws-route-approve"
	reqApprove := newTestRequest(wsApprove, "req-route-approve")
	if _, err := decorator.SubmitAuthorityRequest(ctx, reqApprove); err != nil {
		t.Fatalf("submit (approve setup): %v", err)
	}
	if _, err := decorator.ApproveAuthorityRequest(ctx, "req-route-approve", models.Identity{Type: models.PrincipalUser, ID: "carol"}, &aclstore.ApproveDecision{Reason: "lgtm"}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if inner.approveCalls != 1 {
		t.Errorf("inner approve_calls = %d, want 1", inner.approveCalls)
	}
	// 2 events expected on this workspace: created + approved.
	if got := drainEvents(t, ctx, js, wsApprove, 2, 2*time.Second); got < 2 {
		t.Errorf("approve produced %d events, want >= 2 (created+approved)", got)
	}

	// ----- Deny -----
	wsDeny := "ws-route-deny"
	if _, err := decorator.SubmitAuthorityRequest(ctx, newTestRequest(wsDeny, "req-route-deny")); err != nil {
		t.Fatalf("submit (deny setup): %v", err)
	}
	if _, err := decorator.DenyAuthorityRequest(ctx, "req-route-deny", models.Identity{Type: models.PrincipalUser, ID: "carol"}, "no"); err != nil {
		t.Fatalf("deny: %v", err)
	}
	if inner.denyCalls != 1 {
		t.Errorf("inner deny_calls = %d, want 1", inner.denyCalls)
	}
	if got := drainEvents(t, ctx, js, wsDeny, 2, 2*time.Second); got < 2 {
		t.Errorf("deny produced %d events, want >= 2 (created+denied)", got)
	}

	// ----- Cancel -----
	wsCancel := "ws-route-cancel"
	if _, err := decorator.SubmitAuthorityRequest(ctx, newTestRequest(wsCancel, "req-route-cancel")); err != nil {
		t.Fatalf("submit (cancel setup): %v", err)
	}
	if _, err := decorator.CancelOpenAuthorityRequest(ctx, "req-route-cancel", "changed mind"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if inner.cancelCalls != 1 {
		t.Errorf("inner cancel_calls = %d, want 1", inner.cancelCalls)
	}
	if got := drainEvents(t, ctx, js, wsCancel, 2, 2*time.Second); got < 2 {
		t.Errorf("cancel produced %d events, want >= 2 (created+cancelled)", got)
	}

	// ----- Sweep -----
	wsSweep := "ws-route-sweep"
	expiredReq := newTestRequest(wsSweep, "req-route-sweep")
	// Make it already-past-expiry so the sweep grabs it.
	expiredReq.CreatedAt = time.Now().Add(-2 * time.Hour).UTC()
	expiredReq.ExpiresAt = time.Now().Add(-1 * time.Hour).UTC()
	if _, err := decorator.SubmitAuthorityRequest(ctx, expiredReq); err != nil {
		t.Fatalf("submit (sweep setup): %v", err)
	}
	expired, err := decorator.SweepExpiredAuthorityRequests(ctx, time.Now().UTC(), 0)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(expired) < 1 {
		t.Fatalf("sweep returned %d rows, want >= 1", len(expired))
	}
	if inner.sweepCalls != 1 {
		t.Errorf("inner sweep_calls = %d, want 1", inner.sweepCalls)
	}
	if got := drainEvents(t, ctx, js, wsSweep, 2, 2*time.Second); got < 2 {
		t.Errorf("sweep produced %d events, want >= 2 (created+expired)", got)
	}

	// ----- Get -----
	// The wrapper passes Get through to the inner. We assert routing by
	// checking the inner's GetAuthorityRequest counter ticks up; no event is
	// emitted on Get (read-only path).
	wsGet := "ws-route-get"
	if _, err := decorator.SubmitAuthorityRequest(ctx, newTestRequest(wsGet, "req-route-get")); err != nil {
		t.Fatalf("submit (get setup): %v", err)
	}
	beforeGet := inner.getAuthorityRequestCalls
	if _, err := decorator.GetAuthorityRequest(ctx, "req-route-get"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if inner.getAuthorityRequestCalls != beforeGet+1 {
		t.Errorf("inner get_calls = %d, want %d (+1)", inner.getAuthorityRequestCalls, beforeGet+1)
	}
}

// ---------------------------------------------------------------------------
// Test 2: non-lifecycle methods pass through to the inner unchanged
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityStore_NonLifecycleMethods_PassthroughToInner picks
// three representative non-lifecycle methods (one access check, one rule
// read, one cleanup) and asserts:
//   - The inner Store's per-method counter ticks up by exactly one.
//   - NO JetStream event is emitted on any subject (the wrapper has no business
//     publishing for non-lifecycle traffic).
//
// This is the canonical "wrapper does not over-reach" guarantee.
func TestJetStreamAuthorityStore_NonLifecycleMethods_PassthroughToInner(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeInnerStore()
	decorator, err := aclstore.NewJetStreamAuthorityStore(inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new decorator: %v", err)
	}

	// ----- CheckAccess (representative access path) -----
	principal := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	if _, err := decorator.CheckAccess(ctx, principal, "file", "/x", "read", "ws-x", uuid.Nil, 10); err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if inner.checkAccessCalls != 1 {
		t.Errorf("inner check_access_calls = %d, want 1", inner.checkAccessCalls)
	}

	// ----- GetRule (representative rule-read path) -----
	if _, err := decorator.GetRule(ctx, "user", "alice", "file", "/x"); err != nil {
		t.Fatalf("GetRule: %v", err)
	}
	if inner.getRuleCalls != 1 {
		t.Errorf("inner get_rule_calls = %d, want 1", inner.getRuleCalls)
	}

	// ----- CleanupExpiredRules (representative maintenance path) -----
	if _, err := decorator.CleanupExpiredRules(ctx); err != nil {
		t.Fatalf("CleanupExpiredRules: %v", err)
	}
	if inner.cleanupExpiredRulesCalls != 1 {
		t.Errorf("inner cleanup_expired_rules_calls = %d, want 1", inner.cleanupExpiredRulesCalls)
	}

	// ----- ListAuthorityGrants (a 4th sanity check on the grant read path) -----
	if _, err := decorator.ListAuthorityGrants(ctx, aclstore.AuthorityGrantFilter{}); err != nil {
		t.Fatalf("ListAuthorityGrants: %v", err)
	}
	if inner.listAuthorityGrantCalls != 1 {
		t.Errorf("inner list_authority_grant_calls = %d, want 1", inner.listAuthorityGrantCalls)
	}

	// No lifecycle calls should have fired:
	if inner.submitCalls != 0 || inner.approveCalls != 0 || inner.denyCalls != 0 ||
		inner.cancelCalls != 0 || inner.sweepCalls != 0 {
		t.Errorf("lifecycle counters non-zero after non-lifecycle calls: submit=%d approve=%d deny=%d cancel=%d sweep=%d",
			inner.submitCalls, inner.approveCalls, inner.denyCalls, inner.cancelCalls, inner.sweepCalls)
	}

	// ----- No JetStream side-effects -----
	// Inspect the stream's last sequence: if any subject received an event,
	// the stream's State.Msgs > 0. The stream is freshly created by the
	// decorator's constructor, so a clean run has 0 messages.
	stream, err := js.Stream(ctx, legacy.AuthorityRequestsStream)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Errorf("stream has %d messages after non-lifecycle calls, want 0 (wrapper over-reached)", info.State.Msgs)
	}
}

// ---------------------------------------------------------------------------
// Test 3: compile-only interface assertion (covered at file top)
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityStore_InterfaceAssertion exists so `go test` shows
// the assertion as a passing test (the actual check is the package-level
// `var _ aclstore.Store = ...` at the top of this file — if the decorator
// stops satisfying Store, the test binary won't compile and the suite fails
// before this test runs).
func TestJetStreamAuthorityStore_InterfaceAssertion(t *testing.T) {
	// If we got here, the file compiled, which means the type-assertions
	// at file scope succeeded.
	_ = (aclstore.Store)((*aclstore.JetStreamAuthorityStore)(nil))
}

// ---------------------------------------------------------------------------
// Test 4: constructor input validation
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityStore_NilInputsRejected verifies the constructor
// rejects nil inputs explicitly so callers don't silently get a half-wired
// decorator that nil-deref's at the first lifecycle call.
func TestJetStreamAuthorityStore_NilInputsRejected(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	if _, err := aclstore.NewJetStreamAuthorityStore(nil, js, 1, nil); err == nil {
		t.Errorf("expected error for nil inner Store, got nil")
	}
	if _, err := aclstore.NewJetStreamAuthorityStore(newFakeInnerStore(), nil, 1, nil); err == nil {
		t.Errorf("expected error for nil js, got nil")
	}
}

// touch the json package via a no-op so go vet doesn't grumble about the
// unused import in case we strip a future assertion that consumed it.
var _ = json.Marshal
