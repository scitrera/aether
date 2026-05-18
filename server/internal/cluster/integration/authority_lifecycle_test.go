package integration

// authority_lifecycle_test.go covers the full Phase 2 authority-request relay
// end-to-end through JetStream: the JetStreamAuthorityLifecycle wrapper
// (Submit / Approve / Deny / CancelOpen / SweepExpired) emits events on the
// "authreq" stream, the JetStreamTaskWaker subscribes to that stream, and
// translates each terminal event into ResumeTask or FailTask against the
// matching waiting_authority task.
//
// Production wiring under test:
//   internal/acl/authority_request_lifecycle_jetstream.go (the publisher)
//   internal/orchestration/task_waker_jetstream.go        (the consumer)
//
// This test sits at the cluster-integration seam: a single embedded NATS
// server hosts the "authreq" stream; the lifecycle wrapper writes to it via
// the public API, and the waker consumer reads via the production code path
// inside JetStreamTaskWaker. The collaborators we DON'T want to spin up
// (full *acl.Service + audit stack, full *TaskAssignmentService) are
// replaced with the same minimal fakes already used by the per-package unit
// tests (fakeLifecycle from acl/, recordingTaskWakerService from
// orchestration/) — REPLICATED here as integration_test-package locals
// because Go test packages cannot import _test.go files across packages.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/scitrera/aether/internal/acl"
	clusternats "github.com/scitrera/aether/internal/cluster/nats"
	"github.com/scitrera/aether/internal/orchestration"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// ---------------------------------------------------------------------------
// Fakes: minimal inner Lifecycle + minimal taskWakerService + waiting-task
// store. Each mirrors the per-package unit-test fake closely so the
// behavioral contract under test is identical to what the unit tests pin.
// ---------------------------------------------------------------------------

// fakeAuthLifecycle is the integration-test stand-in for *acl.Service.
// It satisfies acl.Lifecycle with an in-memory map, matching the shape of
// fakeLifecycle in internal/acl/authority_request_lifecycle_jetstream_test.go.
type fakeAuthLifecycle struct {
	mu   sync.Mutex
	rows map[string]*acl.AuthorityRequest
}

func newFakeAuthLifecycle() *fakeAuthLifecycle {
	return &fakeAuthLifecycle{rows: make(map[string]*acl.AuthorityRequest)}
}

func (f *fakeAuthLifecycle) GetAuthorityRequest(ctx context.Context, requestID string) (*acl.AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[requestID]
	if !ok {
		return nil, acl.ErrAuthorityRequestNotFound
	}
	cp := *row
	return &cp, nil
}

func (f *fakeAuthLifecycle) SubmitAuthorityRequest(ctx context.Context, req *acl.AuthorityRequest) (*acl.AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.RequestID == "" {
		req.RequestID = fmt.Sprintf("req-%d", len(f.rows)+1)
	}
	if req.Status == "" {
		req.Status = acl.AuthorityRequestStatusPending
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

func (f *fakeAuthLifecycle) ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *acl.ApproveDecision) (*acl.AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[requestID]
	if !ok {
		return nil, acl.ErrAuthorityRequestNotFound
	}
	if row.Status.IsTerminal() {
		return nil, acl.ErrAuthorityRequestAlreadyResolved
	}
	row.Status = acl.AuthorityRequestStatusApproved
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

func (f *fakeAuthLifecycle) DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*acl.AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[requestID]
	if !ok {
		return nil, acl.ErrAuthorityRequestNotFound
	}
	if row.Status.IsTerminal() {
		return nil, acl.ErrAuthorityRequestAlreadyResolved
	}
	row.Status = acl.AuthorityRequestStatusDenied
	now := time.Now().UTC()
	row.ResolvedAt = &now
	row.ResolvedBy = approverIdentity
	row.ResolutionReason = reason
	out := *row
	return &out, nil
}

func (f *fakeAuthLifecycle) CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*acl.AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[requestID]
	if !ok {
		return nil, acl.ErrAuthorityRequestNotFound
	}
	if row.Status.IsTerminal() {
		return nil, acl.ErrAuthorityRequestAlreadyResolved
	}
	row.Status = acl.AuthorityRequestStatusCancelled
	now := time.Now().UTC()
	row.ResolvedAt = &now
	row.ResolutionReason = reason
	out := *row
	return &out, nil
}

func (f *fakeAuthLifecycle) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*acl.AuthorityRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var out []*acl.AuthorityRequest
	for _, r := range f.rows {
		if r.Status != acl.AuthorityRequestStatusPending {
			continue
		}
		if r.ExpiresAt.After(now) {
			continue
		}
		r.Status = acl.AuthorityRequestStatusExpired
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
// recordingWakerService captures every ResumeTask / FailTask call. Mirrors
// the recordingTaskWakerService in
// internal/orchestration/task_waker_jetstream_test.go.
// ---------------------------------------------------------------------------

type recordingWakerService struct {
	mu     sync.Mutex
	resume []wakerResumeCall
	fail   []wakerFailCall
}

type wakerResumeCall struct {
	taskID string
	to     tasks.TaskStatus
}

type wakerFailCall struct {
	taskID string
	reason string
}

func (r *recordingWakerService) ResumeTask(ctx context.Context, taskID string, to tasks.TaskStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resume = append(r.resume, wakerResumeCall{taskID: taskID, to: to})
	return nil
}

func (r *recordingWakerService) FailTask(ctx context.Context, taskID, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = append(r.fail, wakerFailCall{taskID: taskID, reason: reason})
	return nil
}

func (r *recordingWakerService) resumeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.resume)
}

func (r *recordingWakerService) failCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.fail)
}

func (r *recordingWakerService) snapshotResumes() []wakerResumeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]wakerResumeCall, len(r.resume))
	copy(out, r.resume)
	return out
}

func (r *recordingWakerService) snapshotFails() []wakerFailCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]wakerFailCall, len(r.fail))
	copy(out, r.fail)
	return out
}

// ---------------------------------------------------------------------------
// waitingTaskStore is a minimal taskstore.Store that returns a fixed set of
// waiting_authority tasks from ListWaitingTasks. The waker's authority path
// only calls ListWaitingTasks (per task_waker_jetstream.go), so every other
// method panics to surface unexpected interactions.
// ---------------------------------------------------------------------------

type waitingTaskStore struct {
	*fakeTaskStore
	mu      sync.Mutex
	waiting map[string]*tasks.Task
}

func newWaitingTaskStore() *waitingTaskStore {
	return &waitingTaskStore{
		fakeTaskStore: newFakeTaskStore(),
		waiting:       make(map[string]*tasks.Task),
	}
}

// bindWaitingAuthority installs a fake "task in waiting_authority" row
// keyed on an authority request id. The waker will discover it via
// ListWaitingTasks.
func (s *waitingTaskStore) bindWaitingAuthority(taskID, workspace, requestID string) *tasks.Task {
	t := &tasks.Task{
		TaskID:    taskID,
		Workspace: workspace,
		Status:    tasks.TaskStatusWaitingAuthority,
		WaitSpec: &tasks.WaitSpec{
			Reason:             tasks.WaitReasonAuthority,
			AuthorityRequestID: requestID,
		},
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiting[taskID] = t
	return t
}

// ListWaitingTasks overrides the panicking parent method with a real return.
// The waker's tasksWaitingOnAuthorityRequest filter only keeps rows whose
// Status==waiting_authority AND WaitSpec.Reason==authority AND
// WaitSpec.AuthorityRequestID matches the incoming event.
func (s *waitingTaskStore) ListWaitingTasks(_ context.Context, limit int) ([]*tasks.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*tasks.Task, 0, len(s.waiting))
	for _, t := range s.waiting {
		cp := *t
		if t.WaitSpec != nil {
			specCp := *t.WaitSpec
			cp.WaitSpec = &specCp
		}
		out = append(out, &cp)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// Compile-time check that the override satisfies the interface.
var _ taskstore.Store = (*waitingTaskStore)(nil)

// ---------------------------------------------------------------------------
// Test infra: cluster bootstrap + assembled lifecycle/waker pair.
// ---------------------------------------------------------------------------

// authorityRelayHarness owns every collaborator the lifecycle+waker pair
// needs. setup() returns a fully-wired harness with the waker already
// running; tearDown() stops everything in the correct order.
type authorityRelayHarness struct {
	es        *clusternats.EmbeddedServer
	js        jetstream.JetStream
	inner     *fakeAuthLifecycle
	wrapper   *acl.JetStreamAuthorityLifecycle
	store     *waitingTaskStore
	svc       *recordingWakerService
	waker     *orchestration.JetStreamTaskWaker
	stopWaker func()
}

// setupAuthorityRelay brings up the single-node embedded NATS, the lifecycle
// wrapper (which provisions the authreq stream + KV bucket), the waiting-
// task store, the recording service, and the JetStreamTaskWaker. The waker
// is started in its own goroutine with a small settle to ensure its
// consumer is online before events fire (matches startJSWakerForTest in the
// orchestration package — DeliverNewPolicy drops messages that arrive
// before the consumer is created).
func setupAuthorityRelay(t *testing.T) *authorityRelayHarness {
	t.Helper()

	es := setupCluster1(t)
	js := es.JetStream()

	startCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	inner := newFakeAuthLifecycle()
	wrapper, err := acl.NewJetStreamAuthorityLifecycle(startCtx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("NewJetStreamAuthorityLifecycle: %v", err)
	}

	// The waker's Run() unconditionally starts BOTH an authority consumer
	// (on "authreq") AND an input consumer (on "tk"). If "tk" is missing,
	// startInputConsumer fails and Run() returns early — which silently
	// disables the authority path we are trying to test. Provision a no-op
	// "tk" stream here so the waker's input consumer comes up clean. The
	// input wake path is exercised separately in
	// internal/orchestration/task_waker_jetstream_test.go.
	if _, err := js.CreateOrUpdateStream(startCtx, jetstream.StreamConfig{
		Name:      "tk",
		Subjects:  []string{"tk.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
	}); err != nil {
		t.Fatalf("ensure tk stream: %v", err)
	}

	store := newWaitingTaskStore()
	svc := &recordingWakerService{}

	// Per-test durable consumer suffix prevents collisions across sub-tests.
	suffix := "authrelay_" + uuid.NewString()[:8]
	waker := orchestration.NewJetStreamTaskWaker(js, store, svc, suffix)

	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		waker.Run(runCtx)
	}()

	// Settle: give the push consumer time to subscribe at the stream leader
	// before the test publishes its first event.
	time.Sleep(200 * time.Millisecond)

	stop := func() {
		cancelRun()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("waker did not shut down within 5s")
		}
	}

	return &authorityRelayHarness{
		es:        es,
		js:        js,
		inner:     inner,
		wrapper:   wrapper,
		store:     store,
		svc:       svc,
		waker:     waker,
		stopWaker: stop,
	}
}

// newAuthorityRequestForTest builds a minimal-but-valid AuthorityRequest in
// the given workspace, bound to the given request id. The fields populated
// here are the same shape NewJetStreamAuthorityLifecycle.SubmitAuthorityRequest
// uses to compute the per-workspace event subject (firstWorkspace) and to
// derive the actor_principal field on the emitted event.
func newAuthorityRequestForTest(workspace, requestID string) *acl.AuthorityRequest {
	requester := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	routingPrincipal := models.Identity{Type: models.PrincipalUser, ID: "bob"}
	return &acl.AuthorityRequest{
		RequestID:       requestID,
		RequestingActor: requester,
		WorkspaceScope:  []string{workspace},
		OperationScope:  []string{"read"},
		ResourceScope:   map[string][]string{"file": {"/x"}},
		RequestedAccess: 20,
		DurationSeconds: 60,
		AudienceType:    acl.AuthorityAudienceSession,
		AudienceID:      "sess-1",
		RoutingTarget:   acl.AuthorityRequestRoutingTarget{Principal: &routingPrincipal},
		Reason:          "test",
	}
}

// waitForResume polls the recording service until it sees a ResumeTask call
// for the supplied task id, or fails the test on timeout.
func waitForResume(t *testing.T, svc *recordingWakerService, taskID string, timeout time.Duration) wakerResumeCall {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, c := range svc.snapshotResumes() {
			if c.taskID == taskID {
				return c
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for ResumeTask(%s) within %s (resumes=%d fails=%d)",
		taskID, timeout, svc.resumeCount(), svc.failCount())
	return wakerResumeCall{}
}

// waitForFail polls the recording service until it sees a FailTask call
// for the supplied task id, or fails the test on timeout.
func waitForFail(t *testing.T, svc *recordingWakerService, taskID string, timeout time.Duration) wakerFailCall {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, c := range svc.snapshotFails() {
			if c.taskID == taskID {
				return c
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for FailTask(%s) within %s (resumes=%d fails=%d)",
		taskID, timeout, svc.resumeCount(), svc.failCount())
	return wakerFailCall{}
}

// ---------------------------------------------------------------------------
// TestAuthorityLifecycle_EndToEnd is the parent test for the four
// Phase 2 sub-flows. We use a fresh embedded NATS / waker per sub-flow
// (via t.Run + setupAuthorityRelay) so durable-consumer state from one
// sub-flow does not leak into the next.
// ---------------------------------------------------------------------------

func TestAuthorityLifecycle_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("authority-relay integration test runs an embedded NATS server; skipped in -short")
	}

	// ----- Sub-flow 1: approve resumes a waiting_authority task -----
	t.Run("ApproveResumesTask", func(t *testing.T) {
		h := setupAuthorityRelay(t)
		defer h.stopWaker()

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		const (
			workspace = "ws-approve"
			requestID = "req-approve-1"
			taskID    = "task-approve-1"
		)
		// Bind a fake waiting_authority task to this request id BEFORE we
		// submit + approve. The waker discovers it via ListWaitingTasks
		// when the event arrives.
		h.store.bindWaitingAuthority(taskID, workspace, requestID)

		// Submit (CREATED) + Approve (APPROVED) via the wrapper. Each
		// emits an event on authreq::{workspace}::events.
		if _, err := h.wrapper.SubmitAuthorityRequest(ctx, newAuthorityRequestForTest(workspace, requestID)); err != nil {
			t.Fatalf("submit: %v", err)
		}
		approver := models.Identity{Type: models.PrincipalUser, ID: "carol"}
		resolved, err := h.wrapper.ApproveAuthorityRequest(ctx, requestID, approver, &acl.ApproveDecision{Reason: "lgtm"})
		if err != nil {
			t.Fatalf("approve: %v", err)
		}
		if resolved.Status != acl.AuthorityRequestStatusApproved {
			t.Fatalf("resolved status = %q want approved", resolved.Status)
		}

		got := waitForResume(t, h.svc, taskID, 5*time.Second)
		if got.to != tasks.TaskStatusRunning {
			t.Errorf("resume target = %q want %q", got.to, tasks.TaskStatusRunning)
		}
		if h.svc.failCount() != 0 {
			t.Errorf("expected no FailTask calls on approval; got %d", h.svc.failCount())
		}
	})

	// ----- Sub-flow 2: deny fails the task with a reason -----
	t.Run("DenyFailsTask", func(t *testing.T) {
		h := setupAuthorityRelay(t)
		defer h.stopWaker()

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		const (
			workspace = "ws-deny"
			requestID = "req-deny-1"
			taskID    = "task-deny-1"
		)
		h.store.bindWaitingAuthority(taskID, workspace, requestID)

		if _, err := h.wrapper.SubmitAuthorityRequest(ctx, newAuthorityRequestForTest(workspace, requestID)); err != nil {
			t.Fatalf("submit: %v", err)
		}
		approver := models.Identity{Type: models.PrincipalUser, ID: "carol"}
		if _, err := h.wrapper.DenyAuthorityRequest(ctx, requestID, approver, "policy-violation"); err != nil {
			t.Fatalf("deny: %v", err)
		}

		got := waitForFail(t, h.svc, taskID, 5*time.Second)
		// authorityEventFailureReason composes either
		//   "authority request denied: <reason>" (when ResolutionReason is
		//   present on the embedded request payload) or
		//   "authority request denied"
		// — either is acceptable; we just assert "denied" appears.
		if got.reason == "" {
			t.Errorf("expected non-empty fail reason on denial")
		}
		if !containsAll(got.reason, "denied") {
			t.Errorf("fail reason = %q want substring 'denied'", got.reason)
		}
		if h.svc.resumeCount() != 0 {
			t.Errorf("expected no ResumeTask calls on denial; got %d", h.svc.resumeCount())
		}
	})

	// ----- Sub-flow 3: cancel fails the task with a "cancelled" reason -----
	t.Run("CancelFailsTask", func(t *testing.T) {
		h := setupAuthorityRelay(t)
		defer h.stopWaker()

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		const (
			workspace = "ws-cancel"
			requestID = "req-cancel-1"
			taskID    = "task-cancel-1"
		)
		h.store.bindWaitingAuthority(taskID, workspace, requestID)

		if _, err := h.wrapper.SubmitAuthorityRequest(ctx, newAuthorityRequestForTest(workspace, requestID)); err != nil {
			t.Fatalf("submit: %v", err)
		}
		if _, err := h.wrapper.CancelOpenAuthorityRequest(ctx, requestID, "user-cancelled"); err != nil {
			t.Fatalf("cancel: %v", err)
		}

		got := waitForFail(t, h.svc, taskID, 5*time.Second)
		if !containsAll(got.reason, "cancelled") {
			t.Errorf("fail reason = %q want substring 'cancelled'", got.reason)
		}
		if h.svc.resumeCount() != 0 {
			t.Errorf("expected no ResumeTask calls on cancel; got %d", h.svc.resumeCount())
		}
	})

	// ----- Sub-flow 4: approver subscribes & receives every event variant -----
	// This is the gap-closer: an external approver sits on a JetStream
	// consumer for the per-workspace subject and gets every lifecycle
	// transition without polling. Without this consumer, the WIP guide
	// flagged approvers as falling back to SQL-polling.
	t.Run("ApproverSubscribesAndReceivesEvents", func(t *testing.T) {
		h := setupAuthorityRelay(t)
		defer h.stopWaker()

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		const workspace = "ws-watch"
		subject := acl.WorkspaceEventSubject(workspace)

		// Provision the approver's consumer directly on the authreq
		// stream — same code path an external service would use.
		stream, err := h.js.Stream(ctx, acl.AuthorityRequestsStream)
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:       "approver_" + uuid.NewString()[:8],
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		if err != nil {
			t.Fatalf("create approver consumer: %v", err)
		}

		received := make(chan *acl.AuthorityRequestLifecycleEvent, 16)
		consCtx, err := cons.Consume(func(msg jetstream.Msg) {
			var evt acl.AuthorityRequestLifecycleEvent
			if err := json.Unmarshal(msg.Data(), &evt); err != nil {
				t.Errorf("approver unmarshal: %v", err)
				_ = msg.Term()
				return
			}
			received <- &evt
			_ = msg.Ack()
		})
		if err != nil {
			t.Fatalf("approver consume: %v", err)
		}
		defer consCtx.Stop()

		// Brief settle so the approver consumer is live before publishes.
		time.Sleep(150 * time.Millisecond)

		// Drive every lifecycle transition on this workspace:
		//   reqA → SUBMIT + APPROVE   (created + approved events)
		//   reqB → SUBMIT + DENY      (created + denied events)
		//   reqC → SUBMIT + CANCEL    (created + cancelled events)
		approver := models.Identity{Type: models.PrincipalUser, ID: "carol"}

		const (
			reqA = "req-watch-approve"
			reqB = "req-watch-deny"
			reqC = "req-watch-cancel"
		)

		if _, err := h.wrapper.SubmitAuthorityRequest(ctx, newAuthorityRequestForTest(workspace, reqA)); err != nil {
			t.Fatalf("submit A: %v", err)
		}
		if _, err := h.wrapper.ApproveAuthorityRequest(ctx, reqA, approver, &acl.ApproveDecision{Reason: "ok"}); err != nil {
			t.Fatalf("approve A: %v", err)
		}
		if _, err := h.wrapper.SubmitAuthorityRequest(ctx, newAuthorityRequestForTest(workspace, reqB)); err != nil {
			t.Fatalf("submit B: %v", err)
		}
		if _, err := h.wrapper.DenyAuthorityRequest(ctx, reqB, approver, "nope"); err != nil {
			t.Fatalf("deny B: %v", err)
		}
		if _, err := h.wrapper.SubmitAuthorityRequest(ctx, newAuthorityRequestForTest(workspace, reqC)); err != nil {
			t.Fatalf("submit C: %v", err)
		}
		if _, err := h.wrapper.CancelOpenAuthorityRequest(ctx, reqC, "withdrawn"); err != nil {
			t.Fatalf("cancel C: %v", err)
		}

		// We expect 6 events total: 3 created + 1 approved + 1 denied + 1 cancelled.
		// Each event must carry the correct workspace + request_id +
		// event_type (per authority_request_lifecycle_jetstream.go's
		// publishEvent).
		type seen struct {
			reqID     string
			eventType acl.AuthorityRequestEventType
		}
		var got []seen
		deadline := time.After(8 * time.Second)
		for len(got) < 6 {
			select {
			case evt := <-received:
				if evt.Workspace != workspace {
					t.Errorf("event workspace = %q want %q", evt.Workspace, workspace)
				}
				if evt.RequestID == "" {
					t.Errorf("event has empty request_id")
				}
				got = append(got, seen{reqID: evt.RequestID, eventType: evt.EventType})
			case <-deadline:
				t.Fatalf("approver did not receive all 6 events within timeout; got %d: %+v", len(got), got)
			}
		}

		// Assert per-request event ordering. The created-event always
		// precedes the terminal-event for a given request id (single
		// stream-leader, monotonic publish order).
		perRequest := map[string][]acl.AuthorityRequestEventType{}
		for _, s := range got {
			perRequest[s.reqID] = append(perRequest[s.reqID], s.eventType)
		}
		expect := map[string][]acl.AuthorityRequestEventType{
			reqA: {acl.AuthorityRequestEventTypeCreated, acl.AuthorityRequestEventTypeApproved},
			reqB: {acl.AuthorityRequestEventTypeCreated, acl.AuthorityRequestEventTypeDenied},
			reqC: {acl.AuthorityRequestEventTypeCreated, acl.AuthorityRequestEventTypeCancelled},
		}
		for reqID, wantTypes := range expect {
			gotTypes := perRequest[reqID]
			if len(gotTypes) != len(wantTypes) {
				t.Errorf("request %s: got %d events %v, want %v", reqID, len(gotTypes), gotTypes, wantTypes)
				continue
			}
			for i, w := range wantTypes {
				if gotTypes[i] != w {
					t.Errorf("request %s event[%d] = %q want %q", reqID, i, gotTypes[i], w)
				}
			}
		}
	})
}

// ---------------------------------------------------------------------------
// TestAuthorityLifecycle_EventPayloadShape pins the on-the-wire shape of the
// lifecycle event so external Phase 2 consumers (Python SDK approver
// helper, Phase 2 gateway approval handler) can rely on it. The contract
// pinned here matches what JetStreamAuthorityLifecycle.publishEvent produces.
// ---------------------------------------------------------------------------

func TestAuthorityLifecycle_EventPayloadShape(t *testing.T) {
	if testing.Short() {
		t.Skip("authority-relay integration test runs an embedded NATS server; skipped in -short")
	}

	h := setupAuthorityRelay(t)
	defer h.stopWaker()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const workspace = "ws-shape"
	const requestID = "req-shape-1"

	// Provision the inspector's consumer directly so we can read the raw
	// payload bytes (not just the parsed struct) and confirm field names.
	stream, err := h.js.Stream(ctx, acl.AuthorityRequestsStream)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "shape_" + uuid.NewString()[:8],
		FilterSubject: acl.WorkspaceEventSubject(workspace),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("create inspector consumer: %v", err)
	}

	rawCreated := atomic.Value{}
	rawApproved := atomic.Value{}
	consCtx, err := cons.Consume(func(msg jetstream.Msg) {
		var probe acl.AuthorityRequestLifecycleEvent
		if err := json.Unmarshal(msg.Data(), &probe); err == nil {
			switch probe.EventType {
			case acl.AuthorityRequestEventTypeCreated:
				rawCreated.Store(append([]byte(nil), msg.Data()...))
			case acl.AuthorityRequestEventTypeApproved:
				rawApproved.Store(append([]byte(nil), msg.Data()...))
			}
		}
		_ = msg.Ack()
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer consCtx.Stop()
	time.Sleep(150 * time.Millisecond)

	if _, err := h.wrapper.SubmitAuthorityRequest(ctx, newAuthorityRequestForTest(workspace, requestID)); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := h.wrapper.ApproveAuthorityRequest(ctx, requestID, models.Identity{Type: models.PrincipalUser, ID: "carol"}, &acl.ApproveDecision{Reason: "ok"}); err != nil {
		t.Fatalf("approve: %v", err)
	}

	// Wait for both events to land at the inspector.
	deadline := time.Now().Add(5 * time.Second)
	for rawCreated.Load() == nil || rawApproved.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatalf("did not receive both created+approved within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Round-trip the created payload via map decode so we can assert
	// concrete JSON field names. The producer writes these via the
	// AuthorityRequestLifecycleEvent struct tags.
	var asMap map[string]interface{}
	if err := json.Unmarshal(rawCreated.Load().([]byte), &asMap); err != nil {
		t.Fatalf("inspector unmarshal created: %v", err)
	}
	requiredCreatedKeys := []string{
		"event_type", "request_id", "workspace", "status_to", "timestamp_ms",
	}
	for _, k := range requiredCreatedKeys {
		if _, ok := asMap[k]; !ok {
			t.Errorf("created event missing required key %q (payload=%v)", k, asMap)
		}
	}
	if asMap["event_type"] != string(acl.AuthorityRequestEventTypeCreated) {
		t.Errorf("created event_type = %v want %q", asMap["event_type"], acl.AuthorityRequestEventTypeCreated)
	}
	if asMap["request_id"] != requestID {
		t.Errorf("created request_id = %v want %q", asMap["request_id"], requestID)
	}
	if asMap["status_to"] != string(acl.AuthorityRequestStatusPending) {
		t.Errorf("created status_to = %v want %q", asMap["status_to"], acl.AuthorityRequestStatusPending)
	}

	var approvedMap map[string]interface{}
	if err := json.Unmarshal(rawApproved.Load().([]byte), &approvedMap); err != nil {
		t.Fatalf("inspector unmarshal approved: %v", err)
	}
	if approvedMap["event_type"] != string(acl.AuthorityRequestEventTypeApproved) {
		t.Errorf("approved event_type = %v want %q", approvedMap["event_type"], acl.AuthorityRequestEventTypeApproved)
	}
	if approvedMap["status_to"] != string(acl.AuthorityRequestStatusApproved) {
		t.Errorf("approved status_to = %v want %q", approvedMap["status_to"], acl.AuthorityRequestStatusApproved)
	}
	// grant_id is omitempty; should be present + non-empty on approved.
	if v, ok := approvedMap["grant_id"].(string); !ok || v == "" {
		t.Errorf("approved event grant_id = %v want non-empty string", approvedMap["grant_id"])
	}
}

// containsAll returns true iff s contains every substring in subs. Tiny
// helper to keep the assertion sites readable.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !contains(s, sub) {
			return false
		}
	}
	return true
}

// contains is the plain stdlib substring check, inlined so this file has no
// dependency on `strings` purely for assertion plumbing.
func contains(s, sub string) bool {
	if sub == "" {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
