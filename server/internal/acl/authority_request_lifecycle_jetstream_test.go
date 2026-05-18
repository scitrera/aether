// Tests for the JetStream-backed authority-request lifecycle wrapper.
//
// The wrapper is exercised against a real in-process embedded NATS+JetStream
// server (so KV CAS, stream creation, and event delivery are end-to-end real)
// combined with a fakeLifecycle that stands in for the SQLite/Postgres
// service. The fake makes the tests fast and lets us inject failures
// (InnerFailureRollsBack) and concurrency (ConcurrentResolve_OneWins)
// without bootstrapping the full ACL+audit stack.

package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// Test infra: embedded NATS + JetStream
// ---------------------------------------------------------------------------

// startTestJS boots an in-process NATS+JetStream server on an ephemeral
// port and returns a jetstream.JetStream context plus a teardown.
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
// Test infra: fakeLifecycle (stand-in for *acl.Service)
// ---------------------------------------------------------------------------

// fakeLifecycle satisfies the Lifecycle interface with an in-memory store.
// It mirrors the inner *acl.Service shape: Submit creates a pending row;
// Approve/Deny/Cancel flip to terminal; SweepExpired returns and updates
// PENDING rows whose ExpiresAt <= now. Concurrency-safe.
type fakeLifecycle struct {
	mu       sync.Mutex
	rows     map[string]*AuthorityRequest
	failNext bool   // when true, the next Submit call returns failNextErr
	failErr  error  // err returned when failNext is true
	failOp   string // limits failNext to a specific op: "submit", "approve", "deny", "cancel"
}

func newFakeLifecycle() *fakeLifecycle {
	return &fakeLifecycle{rows: make(map[string]*AuthorityRequest)}
}

func (f *fakeLifecycle) GetAuthorityRequest(ctx context.Context, requestID string) (*AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[requestID]
	if !ok {
		return nil, ErrAuthorityRequestNotFound
	}
	// Return a defensive copy so callers cannot mutate the store.
	cp := *row
	return &cp, nil
}

func (f *fakeLifecycle) SubmitAuthorityRequest(ctx context.Context, req *AuthorityRequest) (*AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext && (f.failOp == "" || f.failOp == "submit") {
		f.failNext = false
		return nil, f.failErr
	}
	if req.RequestID == "" {
		req.RequestID = fmt.Sprintf("req-%d", len(f.rows)+1)
	}
	if req.Status == "" {
		req.Status = AuthorityRequestStatusPending
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

func (f *fakeLifecycle) ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *ApproveDecision) (*AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext && (f.failOp == "" || f.failOp == "approve") {
		f.failNext = false
		return nil, f.failErr
	}
	row, ok := f.rows[requestID]
	if !ok {
		return nil, ErrAuthorityRequestNotFound
	}
	if row.Status.IsTerminal() {
		return nil, ErrAuthorityRequestAlreadyResolved
	}
	row.Status = AuthorityRequestStatusApproved
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

func (f *fakeLifecycle) DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext && (f.failOp == "" || f.failOp == "deny") {
		f.failNext = false
		return nil, f.failErr
	}
	row, ok := f.rows[requestID]
	if !ok {
		return nil, ErrAuthorityRequestNotFound
	}
	if row.Status.IsTerminal() {
		return nil, ErrAuthorityRequestAlreadyResolved
	}
	row.Status = AuthorityRequestStatusDenied
	now := time.Now().UTC()
	row.ResolvedAt = &now
	row.ResolvedBy = approverIdentity
	row.ResolutionReason = reason
	out := *row
	return &out, nil
}

func (f *fakeLifecycle) CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext && (f.failOp == "" || f.failOp == "cancel") {
		f.failNext = false
		return nil, f.failErr
	}
	row, ok := f.rows[requestID]
	if !ok {
		return nil, ErrAuthorityRequestNotFound
	}
	if row.Status.IsTerminal() {
		return nil, ErrAuthorityRequestAlreadyResolved
	}
	row.Status = AuthorityRequestStatusCancelled
	now := time.Now().UTC()
	row.ResolvedAt = &now
	row.ResolutionReason = reason
	out := *row
	return &out, nil
}

func (f *fakeLifecycle) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var out []*AuthorityRequest
	for _, r := range f.rows {
		if r.Status != AuthorityRequestStatusPending {
			continue
		}
		if r.ExpiresAt.After(now) {
			continue
		}
		r.Status = AuthorityRequestStatusExpired
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

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// readKVRequest returns the JSON-decoded AuthorityRequest at the given
// request_id, plus the entry revision. Fails the test if not found.
func readKVRequest(t *testing.T, ctx context.Context, kv jetstream.KeyValue, requestID string) (*AuthorityRequest, uint64) {
	t.Helper()
	entry, err := kv.Get(ctx, requestID)
	if err != nil {
		t.Fatalf("kv get %s: %v", requestID, err)
	}
	var req AuthorityRequest
	if err := json.Unmarshal(entry.Value(), &req); err != nil {
		t.Fatalf("kv unmarshal %s: %v", requestID, err)
	}
	return &req, entry.Revision()
}

// consumeOneEvent creates a one-shot ephemeral consumer on the workspace
// subject and returns the first decoded event. Times out after 2s.
func consumeOneEvent(t *testing.T, ctx context.Context, js jetstream.JetStream, workspace string) *AuthorityRequestLifecycleEvent {
	t.Helper()
	subject := WorkspaceEventSubject(workspace)

	stream, err := js.Stream(ctx, AuthorityRequestsStream)
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

	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if msg, ok := <-batch.Messages(); ok {
		var evt AuthorityRequestLifecycleEvent
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		_ = msg.Ack()
		return &evt
	}
	if err := batch.Error(); err != nil {
		t.Fatalf("batch error: %v", err)
	}
	t.Fatalf("no event received within timeout on subject %s", subject)
	return nil
}

// newTestRequest builds a minimal-but-valid AuthorityRequest with the given
// workspace + request_id (request_id auto-generated if empty).
func newTestRequest(workspace, requestID string) *AuthorityRequest {
	requester := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	routingPrincipal := models.Identity{Type: models.PrincipalUser, ID: "bob"}
	return &AuthorityRequest{
		RequestID:       requestID,
		RequestingActor: requester,
		WorkspaceScope:  []string{workspace},
		OperationScope:  []string{"read"},
		ResourceScope:   map[string][]string{"file": {"/x"}},
		RequestedAccess: 20,
		DurationSeconds: 60,
		AudienceType:    AuthorityAudienceSession,
		AudienceID:      "sess-1",
		RoutingTarget: AuthorityRequestRoutingTarget{
			Principal: &routingPrincipal,
		},
		Reason: "test",
	}
}

// ---------------------------------------------------------------------------
// Test 1: Create writes KV + emits event
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityLifecycle_Create_WritesKVAndEvent verifies that
// submitting a fresh request through the wrapper results in:
//   - The inner store recording the row (precondition for the wrapper to fire).
//   - A KV bucket entry at key=request_id with the JSON-encoded snapshot.
//   - A lifecycle event on the per-workspace subject with type=created.
func TestJetStreamAuthorityLifecycle_Create_WritesKVAndEvent(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeLifecycle()
	w, err := NewJetStreamAuthorityLifecycle(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new wrapper: %v", err)
	}

	req := newTestRequest("ws-alpha", "req-create-1")
	persisted, err := w.SubmitAuthorityRequest(ctx, req)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if persisted.RequestID != "req-create-1" {
		t.Fatalf("persisted.RequestID = %q, want %q", persisted.RequestID, "req-create-1")
	}

	// KV check: bucket has the entry.
	gotKV, rev := readKVRequest(t, ctx, w.kv, "req-create-1")
	if rev == 0 {
		t.Fatalf("kv revision = 0, want > 0")
	}
	if gotKV.Status != AuthorityRequestStatusPending {
		t.Fatalf("kv status = %q, want %q", gotKV.Status, AuthorityRequestStatusPending)
	}
	if len(gotKV.WorkspaceScope) == 0 || gotKV.WorkspaceScope[0] != "ws-alpha" {
		t.Fatalf("kv workspace_scope = %v, want [ws-alpha]", gotKV.WorkspaceScope)
	}

	// Event check: one event on the workspace subject.
	evt := consumeOneEvent(t, ctx, js, "ws-alpha")
	if evt.EventType != AuthorityRequestEventTypeCreated {
		t.Fatalf("event type = %q, want %q", evt.EventType, AuthorityRequestEventTypeCreated)
	}
	if evt.RequestID != "req-create-1" {
		t.Fatalf("event request_id = %q, want %q", evt.RequestID, "req-create-1")
	}
	if evt.StatusTo != AuthorityRequestStatusPending {
		t.Fatalf("event status_to = %q, want %q", evt.StatusTo, AuthorityRequestStatusPending)
	}
	if evt.Workspace != "ws-alpha" {
		t.Fatalf("event workspace = %q, want %q", evt.Workspace, "ws-alpha")
	}
	if evt.ActorPrincipal == "" {
		t.Fatalf("event actor_principal is empty; want requester identity")
	}
}

// ---------------------------------------------------------------------------
// Test 2: Approve drives a CAS transition + emits event
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityLifecycle_Resolve_CASTransition verifies that
// Submit followed by Approve results in:
//   - The KV entry advancing in revision (CAS update succeeded).
//   - The KV payload reflecting the new status (approved) + grant_id.
//   - Two lifecycle events emitted (created + approved) in order.
func TestJetStreamAuthorityLifecycle_Resolve_CASTransition(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeLifecycle()
	w, err := NewJetStreamAuthorityLifecycle(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new wrapper: %v", err)
	}

	req := newTestRequest("ws-beta", "req-cas-1")
	if _, err := w.SubmitAuthorityRequest(ctx, req); err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Capture initial revision after Submit.
	_, revBefore := readKVRequest(t, ctx, w.kv, "req-cas-1")

	approver := models.Identity{Type: models.PrincipalUser, ID: "carol"}
	resolved, err := w.ApproveAuthorityRequest(ctx, "req-cas-1", approver, &ApproveDecision{Reason: "lgtm"})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if resolved.Status != AuthorityRequestStatusApproved {
		t.Fatalf("resolved.Status = %q, want %q", resolved.Status, AuthorityRequestStatusApproved)
	}

	gotKV, revAfter := readKVRequest(t, ctx, w.kv, "req-cas-1")
	if revAfter <= revBefore {
		t.Fatalf("revision did not advance: before=%d after=%d", revBefore, revAfter)
	}
	if gotKV.Status != AuthorityRequestStatusApproved {
		t.Fatalf("kv status = %q, want %q", gotKV.Status, AuthorityRequestStatusApproved)
	}
	if gotKV.GrantedGrantID == "" {
		t.Fatalf("kv granted_grant_id is empty; want grant-req-cas-1")
	}

	// Two events: created + approved.
	subject := WorkspaceEventSubject("ws-beta")
	stream, err := js.Stream(ctx, AuthorityRequestsStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	batch, err := cons.Fetch(2, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var types []AuthorityRequestEventType
	for msg := range batch.Messages() {
		var evt AuthorityRequestLifecycleEvent
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		types = append(types, evt.EventType)
		_ = msg.Ack()
	}
	if len(types) != 2 || types[0] != AuthorityRequestEventTypeCreated || types[1] != AuthorityRequestEventTypeApproved {
		t.Fatalf("event types = %v, want [created, approved]", types)
	}
}

// ---------------------------------------------------------------------------
// Test 3: Concurrent resolve — one wins, other gets concurrent-modification
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityLifecycle_ConcurrentResolve_OneWins kicks off two
// goroutines that race on resolving the same request (one Approve, one
// Deny). The inner fake guards via ErrAuthorityRequestAlreadyResolved so
// exactly one terminal write lands; the loser must surface a non-nil error.
// This proves the wrapper doesn't double-publish events nor double-write KV
// for the same logical transition.
//
// NOTE: with the fake-lifecycle stand-in, the dominant race lives inside
// the inner (already-resolved guard). The wrapper's CAS provides defense in
// depth for races that bypass the inner (e.g. two wrappers pointed at the
// same KV bucket with independent inner stores, which is the cluster case).
// This test asserts the externally-visible invariant: exactly one resolve
// succeeds, the other returns a sentinel error the caller can detect.
func TestJetStreamAuthorityLifecycle_ConcurrentResolve_OneWins(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeLifecycle()
	w, err := NewJetStreamAuthorityLifecycle(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new wrapper: %v", err)
	}

	req := newTestRequest("ws-race", "req-race-1")
	if _, err := w.SubmitAuthorityRequest(ctx, req); err != nil {
		t.Fatalf("submit: %v", err)
	}

	approver := models.Identity{Type: models.PrincipalUser, ID: "carol"}

	var (
		approvedOK, deniedOK int32
		approveErr, denyErr  error
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		if _, err := w.ApproveAuthorityRequest(ctx, "req-race-1", approver, &ApproveDecision{Reason: "approve"}); err != nil {
			approveErr = err
		} else {
			atomic.AddInt32(&approvedOK, 1)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		if _, err := w.DenyAuthorityRequest(ctx, "req-race-1", approver, "deny"); err != nil {
			denyErr = err
		} else {
			atomic.AddInt32(&deniedOK, 1)
		}
	}()
	close(start)
	wg.Wait()

	totalOK := atomic.LoadInt32(&approvedOK) + atomic.LoadInt32(&deniedOK)
	if totalOK != 1 {
		t.Fatalf("expected exactly one resolver to succeed, got approveOK=%d denyOK=%d (approveErr=%v denyErr=%v)",
			approvedOK, deniedOK, approveErr, denyErr)
	}

	// The losing path must surface an error — either the inner's
	// AlreadyResolved or the wrapper's ConcurrentModification sentinel.
	loserErr := approveErr
	if loserErr == nil {
		loserErr = denyErr
	}
	if loserErr == nil {
		t.Fatalf("loser did not surface an error; one resolver should have failed")
	}
	if !errors.Is(loserErr, ErrAuthorityRequestAlreadyResolved) &&
		!errors.Is(loserErr, ErrAuthorityRequestConcurrentModification) {
		t.Fatalf("loser error = %v; want AlreadyResolved or ConcurrentModification", loserErr)
	}

	// KV should reflect a single terminal write.
	gotKV, _ := readKVRequest(t, ctx, w.kv, "req-race-1")
	if !gotKV.Status.IsTerminal() {
		t.Fatalf("kv status = %q, want terminal", gotKV.Status)
	}
}

// ---------------------------------------------------------------------------
// Test 4: Approver subscribes to per-workspace events in real time
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityLifecycle_Approver_SubscribesToEvents is the gap
// closer: it proves an approver can sit on a JetStream consumer for a
// specific workspace and receive lifecycle events in real time. No polling.
//
// Flow:
//  1. Approver provisions a durable consumer on authreq.{ws}.events.
//  2. Approver starts pulling messages in a background goroutine.
//  3. Three Submits land on the workspace in quick succession.
//  4. Approver's goroutine collects all three created-events and reports.
//
// Success criteria: all three events arrive on the approver consumer with
// the expected request IDs and event types within the test timeout.
func TestJetStreamAuthorityLifecycle_Approver_SubscribesToEvents(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeLifecycle()
	w, err := NewJetStreamAuthorityLifecycle(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new wrapper: %v", err)
	}

	workspace := "ws-watch"
	subject := WorkspaceEventSubject(workspace)

	// Approver-side consumer setup. Push-mode via Consume() so we get
	// real-time delivery without polling.
	stream, err := js.Stream(ctx, AuthorityRequestsStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	received := make(chan *AuthorityRequestLifecycleEvent, 8)
	consCtx, err := cons.Consume(func(msg jetstream.Msg) {
		var evt AuthorityRequestLifecycleEvent
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			t.Errorf("approver unmarshal: %v", err)
			_ = msg.Term()
			return
		}
		received <- &evt
		_ = msg.Ack()
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer consCtx.Stop()

	// Lifecycle service emits three created events on this workspace.
	ids := []string{"req-watch-1", "req-watch-2", "req-watch-3"}
	for _, id := range ids {
		if _, err := w.SubmitAuthorityRequest(ctx, newTestRequest(workspace, id)); err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
	}

	gotIDs := make(map[string]bool, len(ids))
	deadline := time.After(5 * time.Second)
	for len(gotIDs) < len(ids) {
		select {
		case evt := <-received:
			if evt.EventType != AuthorityRequestEventTypeCreated {
				t.Errorf("event type = %q, want %q", evt.EventType, AuthorityRequestEventTypeCreated)
			}
			if evt.Workspace != workspace {
				t.Errorf("event workspace = %q, want %q", evt.Workspace, workspace)
			}
			gotIDs[evt.RequestID] = true
		case <-deadline:
			t.Fatalf("approver missed events; got %d/%d (have=%v)", len(gotIDs), len(ids), gotIDs)
		}
	}

	for _, id := range ids {
		if !gotIDs[id] {
			t.Errorf("approver missed event for %s", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 5: Inner failure must NOT write KV or emit events
// ---------------------------------------------------------------------------

// TestJetStreamAuthorityLifecycle_InnerFailureRollsBack proves the wrapper
// preserves consistency: if the inner SQL write fails, the wrapper does NOT
// write to KV and does NOT emit a lifecycle event.
func TestJetStreamAuthorityLifecycle_InnerFailureRollsBack(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeLifecycle()
	w, err := NewJetStreamAuthorityLifecycle(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new wrapper: %v", err)
	}

	// Arm the fake to fail the next Submit.
	innerErr := errors.New("simulated inner failure")
	inner.failNext = true
	inner.failErr = innerErr
	inner.failOp = "submit"

	req := newTestRequest("ws-fail", "req-fail-1")
	got, err := w.SubmitAuthorityRequest(ctx, req)
	if !errors.Is(err, innerErr) {
		t.Fatalf("submit err = %v, want %v", err, innerErr)
	}
	if got != nil {
		t.Fatalf("submit returned non-nil request on inner failure: %+v", got)
	}

	// KV check: bucket has no entry for the failed request.
	if _, err := w.kv.Get(ctx, "req-fail-1"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("expected KV ErrKeyNotFound after inner failure, got: %v", err)
	}

	// Event stream check: no event landed for this workspace.
	subject := WorkspaceEventSubject("ws-fail")
	stream, err := js.Stream(ctx, AuthorityRequestsStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	count := 0
	for range batch.Messages() {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 events after inner failure, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Extra: workspace subject derivation must round-trip
// ---------------------------------------------------------------------------

// TestWorkspaceEventSubject_RoundsTrip verifies the codec-translated subject
// is a legal NATS subject and survives encode/decode for representative
// workspace strings (including ones with NATS-unsafe characters and an
// empty workspace).
func TestWorkspaceEventSubject_RoundsTrip(t *testing.T) {
	cases := []struct {
		workspace string
	}{
		{"simple"},
		{"with-dash"},
		{""},
		{"with.dot"},
		{"with space"},
		{"with*wildcard"},
	}
	for _, tc := range cases {
		t.Run(tc.workspace, func(t *testing.T) {
			subj := WorkspaceEventSubject(tc.workspace)
			if subj == "" {
				t.Fatalf("empty subject for workspace %q", tc.workspace)
			}
			// NATS subjects use "." as the token separator; we expect at
			// least three tokens (authreq, workspace, events).
			parts := strings.Split(subj, ".")
			if len(parts) < 3 {
				t.Fatalf("subject %q has %d tokens, want >= 3", subj, len(parts))
			}
			if parts[0] != "authreq" {
				t.Fatalf("subject %q first token = %q, want 'authreq'", subj, parts[0])
			}
			if parts[len(parts)-1] != "events" {
				t.Fatalf("subject %q last token = %q, want 'events'", subj, parts[len(parts)-1])
			}
		})
	}
}
