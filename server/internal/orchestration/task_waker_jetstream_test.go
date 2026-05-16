// Tests for JetStreamTaskWaker (Slice 4B).
//
// The fakes here intentionally mirror task_waker_test.go's
// fakeAuthorityRequestSource shape (recording stub) so reviewing one set
// makes the other obvious. We use a real in-process NATS+JetStream server
// (via startTestNATSServer in jetstream_dispatcher_test.go) and a real
// sqlite task store (via newWakerStore in task_waker_test.go) so the
// integration story is end-to-end: real JetStream publish → real consumer
// → real task store transition. Only the wake-side service collaborators
// are stubbed.

package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/scitrera/aether/internal/acl"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/tasks"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

// recordingTaskWakerService is a lightweight stand-in for
// *TaskAssignmentService that captures every ResumeTask / FailTask call.
// We use a recording stub (rather than calling into the real service)
// because the waker contract is "trigger a wake transition" — the
// downstream side-effects (token revoke, queue retire, etc.) belong to
// task_assignment_test.go.
type recordingTaskWakerService struct {
	mu     sync.Mutex
	resume []resumeCall
	fail   []failCall
}

type resumeCall struct {
	taskID string
	to     tasks.TaskStatus
}

type failCall struct {
	taskID string
	reason string
}

func (r *recordingTaskWakerService) ResumeTask(ctx context.Context, taskID string, to tasks.TaskStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resume = append(r.resume, resumeCall{taskID: taskID, to: to})
	return nil
}

func (r *recordingTaskWakerService) FailTask(ctx context.Context, taskID, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = append(r.fail, failCall{taskID: taskID, reason: reason})
	return nil
}

func (r *recordingTaskWakerService) resumeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.resume)
}

func (r *recordingTaskWakerService) failCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.fail)
}

func (r *recordingTaskWakerService) lastResume() (resumeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.resume) == 0 {
		return resumeCall{}, false
	}
	return r.resume[len(r.resume)-1], true
}

func (r *recordingTaskWakerService) lastFail() (failCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fail) == 0 {
		return failCall{}, false
	}
	return r.fail[len(r.fail)-1], true
}

// ensureJSWakerStreams provisions the "authreq" and "tk" streams the way
// JetStreamAuthorityLifecycle and JetStreamRouter do in production. The
// waker assumes both exist; in tests we replicate the minimal config.
func ensureJSWakerStreams(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "authreq",
		Subjects:  []string{"authreq.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
	}); err != nil {
		t.Fatalf("create authreq stream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "tk",
		Subjects:  []string{"tk.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
	}); err != nil {
		t.Fatalf("create tk stream: %v", err)
	}
}

// startJSWakerForTest constructs a fully-wired JetStreamTaskWaker against
// the supplied store + recording service, starts Run in a goroutine, and
// returns a stop func. The suffix is the test name so parallel runs don't
// collide on durable consumer names inside the in-process JS.
func startJSWakerForTest(
	t *testing.T,
	js jetstream.JetStream,
	store taskstore.Store,
	svc taskWakerService,
) (*JetStreamTaskWaker, func()) {
	t.Helper()
	suffix := uniqueConsumerSuffix(t)
	waker := NewJetStreamTaskWaker(js, store, svc, suffix)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		waker.Run(ctx)
	}()

	// Give the consumers a moment to come online before tests start
	// publishing — otherwise DeliverNewPolicy may drop the first event.
	time.Sleep(150 * time.Millisecond)

	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("JetStreamTaskWaker did not shut down within 5s")
		}
	}
	return waker, stop
}

// uniqueConsumerSuffix builds a per-test durable suffix. We can't simply
// use t.Name() because subtests include "/" which is NATS-illegal in a
// consumer name; use uuid to be safe.
func uniqueConsumerSuffix(t *testing.T) string {
	t.Helper()
	return "test_" + uuid.New().String()[:8]
}

// publishAuthorityEvent JSON-encodes evt and publishes it to the per-
// workspace authreq subject (matching the producer in
// JetStreamAuthorityLifecycle.publishEvent).
func publishAuthorityEvent(t *testing.T, js jetstream.JetStream, evt *acl.AuthorityRequestLifecycleEvent) {
	t.Helper()
	payload, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal authority event: %v", err)
	}
	subject := acl.WorkspaceEventSubject(evt.Workspace)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subject, payload); err != nil {
		t.Fatalf("publish authority event to %s: %v", subject, err)
	}
}

// publishInputEvent JSON-encodes evt and publishes it on the input
// aether-subject convention "tk::{ws}::{task_id}::input".
func publishInputEvent(t *testing.T, js jetstream.JetStream, evt *TaskInputWakeEvent) {
	t.Helper()
	payload, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal input event: %v", err)
	}
	// natscodec.ToNATSSubject escapes tokens — but for plain ASCII test
	// workspaces / task ids it's a straight dot-translation.
	subject := fmt.Sprintf("tk.%s.%s.input", escapeTokenForTest(evt.Workspace), escapeTokenForTest(evt.TaskID))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subject, payload); err != nil {
		t.Fatalf("publish input event to %s: %v", subject, err)
	}
}

// escapeTokenForTest mirrors the natscodec rules for the few NATS-illegal
// characters that show up in test workspaces / task IDs. We don't want a
// cyclic import on the real natscodec package from a tests-only helper,
// and the test inputs are constrained to alnum + dash + underscore.
func escapeTokenForTest(s string) string {
	// Match natscodec.escapeToken behavior for the chars that appear in our tests.
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '_':
			out = append(out, []byte("_5F_")...)
		case '.':
			out = append(out, []byte("_2E_")...)
		default:
			out = append(out, c)
		}
	}
	return string(out)
}

// buildWaitingInputTaskWithMatch is the input-wake counterpart of
// buildWaitingAuthorityTask from task_waker_test.go.
func buildWaitingInputTaskWithMatch(
	t *testing.T,
	ctx context.Context,
	store taskstore.Store,
	hint string,
	inputMatch map[string]string,
) *tasks.Task {
	t.Helper()
	task := buildRunningTask(t, ctx, store, hint)
	spec := &tasks.WaitSpec{
		Reason:     tasks.WaitReasonInput,
		InputMatch: inputMatch,
	}
	if err := store.PauseTask(ctx, task.TaskID, tasks.TaskStatusWaitingInput, spec); err != nil {
		t.Fatalf("PauseTask(%s) waiting_input: %v", hint, err)
	}
	return task
}

// waitForResumeMatching polls the recording service until it observes a
// resume call with the expected task id, or fails the test on timeout.
func waitForResumeMatching(t *testing.T, svc *recordingTaskWakerService, taskID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		svc.mu.Lock()
		for _, c := range svc.resume {
			if c.taskID == taskID {
				svc.mu.Unlock()
				return
			}
		}
		svc.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for ResumeTask(%s) within %s (resumes=%d fails=%d)",
		taskID, timeout, svc.resumeCount(), svc.failCount())
}

// waitForFailMatching polls until a FailTask call for taskID is observed.
func waitForFailMatching(t *testing.T, svc *recordingTaskWakerService, taskID string, timeout time.Duration) failCall {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		svc.mu.Lock()
		for _, c := range svc.fail {
			if c.taskID == taskID {
				svc.mu.Unlock()
				return c
			}
		}
		svc.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for FailTask(%s) within %s (resumes=%d fails=%d)",
		taskID, timeout, svc.resumeCount(), svc.failCount())
	return failCall{}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestJetStreamWaker_AuthorityResolved_ResumesWaitingTask covers Phase 2
// wake path: a waiting_authority task hears about the approval via the
// authreq event stream and ResumeTask is called.
func TestJetStreamWaker_AuthorityResolved_ResumesWaitingTask(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()
	ensureJSWakerStreams(t, js)

	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	requestID := "ar-approved-" + uuid.New().String()[:8]
	task := buildWaitingAuthorityTask(t, ctx, store, "approve", requestID)

	svc := &recordingTaskWakerService{}
	_, stop := startJSWakerForTest(t, js, store, svc)
	defer stop()

	publishAuthorityEvent(t, js, &acl.AuthorityRequestLifecycleEvent{
		EventType:   acl.AuthorityRequestEventTypeApproved,
		RequestID:   requestID,
		Workspace:   task.Workspace,
		StatusFrom:  acl.AuthorityRequestStatusPending,
		StatusTo:    acl.AuthorityRequestStatusApproved,
		TimestampMs: time.Now().UnixMilli(),
		GrantID:     "grant-xyz",
	})

	waitForResumeMatching(t, svc, task.TaskID, 2*time.Second)

	got, ok := svc.lastResume()
	if !ok {
		t.Fatal("expected at least one resume call")
	}
	if got.to != tasks.TaskStatusRunning {
		t.Errorf("resume target status: got %q want running", got.to)
	}
	if svc.failCount() != 0 {
		t.Errorf("expected no FailTask calls on approval; got %d", svc.failCount())
	}
}

// TestJetStreamWaker_AuthorityDenied_FailsWaitingTask covers the negative
// branch — denied/expired/cancelled should call FailTask, not ResumeTask.
func TestJetStreamWaker_AuthorityDenied_FailsWaitingTask(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()
	ensureJSWakerStreams(t, js)

	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	requestID := "ar-denied-" + uuid.New().String()[:8]
	task := buildWaitingAuthorityTask(t, ctx, store, "deny", requestID)

	svc := &recordingTaskWakerService{}
	_, stop := startJSWakerForTest(t, js, store, svc)
	defer stop()

	publishAuthorityEvent(t, js, &acl.AuthorityRequestLifecycleEvent{
		EventType:   acl.AuthorityRequestEventTypeDenied,
		RequestID:   requestID,
		Workspace:   task.Workspace,
		StatusFrom:  acl.AuthorityRequestStatusPending,
		StatusTo:    acl.AuthorityRequestStatusDenied,
		TimestampMs: time.Now().UnixMilli(),
		Request: &acl.AuthorityRequest{
			RequestID:        requestID,
			Status:           acl.AuthorityRequestStatusDenied,
			ResolutionReason: "policy",
		},
	})

	got := waitForFailMatching(t, svc, task.TaskID, 2*time.Second)
	if got.reason == "" {
		t.Errorf("expected non-empty fail reason on denial; got empty")
	}
	if svc.resumeCount() != 0 {
		t.Errorf("expected no ResumeTask calls on denial; got %d", svc.resumeCount())
	}
}

// TestJetStreamWaker_InputMessage_ResumesWaitingInputTask exercises the
// deferred-gap closure: an INPUT message landing on
// tk::{ws}::{task_id}::input causes a waiting_input task to resume.
//
// HEADLINE: this is the test that proves the WIP-guide §12 deferred gap
// is closed. Without this push consumer, the only way out of waiting_input
// is a timer wake or a manual SDK call.
func TestJetStreamWaker_InputMessage_ResumesWaitingInputTask(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()
	ensureJSWakerStreams(t, js)

	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	match := map[string]string{"type": "foo"}
	task := buildWaitingInputTaskWithMatch(t, ctx, store, "input-wake", match)

	svc := &recordingTaskWakerService{}
	_, stop := startJSWakerForTest(t, js, store, svc)
	defer stop()

	publishInputEvent(t, js, &TaskInputWakeEvent{
		TaskID:          task.TaskID,
		Workspace:       task.Workspace,
		MessageType:     "CHAT",
		Metadata:        map[string]string{"type": "foo", "extra": "ok"},
		EmittedAtUnixMs: time.Now().UnixMilli(),
	})

	waitForResumeMatching(t, svc, task.TaskID, 2*time.Second)

	got, _ := svc.lastResume()
	if got.to != tasks.TaskStatusRunning {
		t.Errorf("input wake resume target: got %q want running", got.to)
	}
	if svc.failCount() != 0 {
		t.Errorf("expected no FailTask on input wake; got %d", svc.failCount())
	}
}

// TestJetStreamWaker_InputMessage_NoMatch_NoResume confirms the
// WaitSpec.InputMatch filter is honored: a non-matching inbound input is
// observed but does NOT trigger ResumeTask.
func TestJetStreamWaker_InputMessage_NoMatch_NoResume(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()
	ensureJSWakerStreams(t, js)

	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	match := map[string]string{"type": "foo"}
	task := buildWaitingInputTaskWithMatch(t, ctx, store, "input-nomatch", match)

	svc := &recordingTaskWakerService{}
	_, stop := startJSWakerForTest(t, js, store, svc)
	defer stop()

	// Publish a non-matching event (type=bar) and a control event that
	// SHOULD match. The control event is for a different task, so its
	// arrival proves the consumer is live and the first event's
	// no-resume decision is intentional, not a missed delivery.
	publishInputEvent(t, js, &TaskInputWakeEvent{
		TaskID:          task.TaskID,
		Workspace:       task.Workspace,
		MessageType:     "CHAT",
		Metadata:        map[string]string{"type": "bar"},
		EmittedAtUnixMs: time.Now().UnixMilli(),
	})

	// Control: distinct task to be sure the consumer is alive.
	controlMatch := map[string]string{} // empty matchSpec = "match any"
	controlTask := buildWaitingInputTaskWithMatch(t, ctx, store, "input-control", controlMatch)
	publishInputEvent(t, js, &TaskInputWakeEvent{
		TaskID:          controlTask.TaskID,
		Workspace:       controlTask.Workspace,
		MessageType:     "CHAT",
		Metadata:        map[string]string{"any": "meta"},
		EmittedAtUnixMs: time.Now().UnixMilli(),
	})

	// The control event proves the consumer is processing messages; once
	// we see it complete, any pending resume for the no-match task would
	// already have fired.
	waitForResumeMatching(t, svc, controlTask.TaskID, 2*time.Second)

	// Now assert the no-match task was NOT resumed.
	for _, c := range svc.resume {
		if c.taskID == task.TaskID {
			t.Fatalf("non-matching input event incorrectly triggered ResumeTask(%s)", task.TaskID)
		}
	}
}

// TestJetStreamWaker_GracefulShutdown asserts the waker exits cleanly
// within 2s of ctx cancellation and does not leak goroutines.
func TestJetStreamWaker_GracefulShutdown(t *testing.T) {
	js, stopNATS := startTestNATSServer(t)
	defer stopNATS()
	ensureJSWakerStreams(t, js)

	store, cleanup := newWakerStore(t)
	defer cleanup()

	svc := &recordingTaskWakerService{}
	waker := NewJetStreamTaskWaker(js, store, svc, uniqueConsumerSuffix(t))

	baselineGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		waker.Run(ctx)
	}()

	// Let the consumers come online.
	time.Sleep(150 * time.Millisecond)

	startTime := time.Now()
	cancel()
	select {
	case <-done:
		elapsed := time.Since(startTime)
		if elapsed > 2*time.Second {
			t.Fatalf("waker took %v to shut down; want <2s", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waker did not shut down within 2s of ctx cancellation")
	}

	// Give the runtime a moment for goroutine bookkeeping to settle.
	time.Sleep(100 * time.Millisecond)

	// Goroutine accounting is loose by design: the in-process NATS server,
	// JetStream replicators, and the Go runtime keep a variable number of
	// background goroutines alive that we don't control. The contract this
	// test enforces is "Run returned in <2s after cancel"; if a leak ever
	// surfaces it'll be 10-100s of leaked goroutines, not single-digit
	// noise. We assert a wide-but-finite ceiling rather than tight equality
	// so the test doesn't false-positive on innocuous runtime variance.
	current := runtime.NumGoroutine()
	if current > baselineGoroutines+50 {
		t.Errorf("goroutine leak suspected: baseline=%d current=%d (delta=%d)",
			baselineGoroutines, current, current-baselineGoroutines)
	}
}

// ---------------------------------------------------------------------------
// Internal helper tests
// ---------------------------------------------------------------------------

// TestInputMatchesWaitSpec is a pure-function table test for the matcher
// helper. Kept inline since the helper is unexported.
func TestInputMatchesWaitSpec(t *testing.T) {
	cases := []struct {
		name   string
		spec   map[string]string
		meta   map[string]string
		expect bool
	}{
		{"empty spec matches anything", nil, map[string]string{"k": "v"}, true},
		{"empty spec matches nil meta", nil, nil, true},
		{"single-key match", map[string]string{"a": "1"}, map[string]string{"a": "1"}, true},
		{"single-key mismatch", map[string]string{"a": "1"}, map[string]string{"a": "2"}, false},
		{"key missing in meta", map[string]string{"a": "1"}, map[string]string{"b": "1"}, false},
		{"meta has extras OK", map[string]string{"a": "1"}, map[string]string{"a": "1", "b": "2"}, true},
		{"multi-key all match", map[string]string{"a": "1", "b": "2"}, map[string]string{"a": "1", "b": "2"}, true},
		{"multi-key partial match", map[string]string{"a": "1", "b": "2"}, map[string]string{"a": "1"}, false},
		{"nil meta against non-empty spec", map[string]string{"a": "1"}, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := inputMatchesWaitSpec(c.spec, c.meta)
			if got != c.expect {
				t.Errorf("inputMatchesWaitSpec(%v, %v) = %v; want %v", c.spec, c.meta, got, c.expect)
			}
		})
	}
}

// TestAuthorityEventFailureReason confirms the reason formatter folds in
// the embedded resolution_reason when available.
func TestAuthorityEventFailureReason(t *testing.T) {
	cases := []struct {
		name   string
		evt    *acl.AuthorityRequestLifecycleEvent
		expect string
	}{
		{
			name:   "nil event",
			evt:    nil,
			expect: "authority request resolved without approval",
		},
		{
			name: "denied without reason",
			evt: &acl.AuthorityRequestLifecycleEvent{
				StatusTo: acl.AuthorityRequestStatusDenied,
			},
			expect: "authority request denied",
		},
		{
			name: "denied with reason",
			evt: &acl.AuthorityRequestLifecycleEvent{
				StatusTo: acl.AuthorityRequestStatusDenied,
				Request: &acl.AuthorityRequest{
					ResolutionReason: "rate limit",
				},
			},
			expect: "authority request denied: rate limit",
		},
		{
			name: "expired with reason",
			evt: &acl.AuthorityRequestLifecycleEvent{
				StatusTo: acl.AuthorityRequestStatusExpired,
				Request: &acl.AuthorityRequest{
					ResolutionReason: "ttl",
				},
			},
			expect: "authority request expired: ttl",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := authorityEventFailureReason(c.evt)
			if got != c.expect {
				t.Errorf("got %q want %q", got, c.expect)
			}
		})
	}
}

// _ marks atomic as deliberately imported even when no test currently
// references it (kept for future race-counter tests).
var _ = atomic.AddInt64
