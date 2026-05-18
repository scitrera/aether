// task_event_fanout_test.go exercises the JetStreamTaskEventPublisher
// end-to-end against an in-process aetherlite (single-node) cluster.
//
// This complements phase4_recursive_subscribe_test.go (which covers the
// cross-node post-subscribe-child path on a 3-node cluster). The headline
// test here drives the full status-changed lifecycle on a parent task,
// proves the post-subscribe-child gap closure from Phase 4 of
// docs/agentic-fabric-protocol-guide.md, and asserts cross-workspace
// isolation + codec-translated subject correctness.
//
// Production flow modeled:
//   - Single in-process embedded NATS + JetStream (setupCluster1 helper).
//   - JetStreamRouter created so the "tk" stream exists with subjects "tk.>".
//   - A JetStreamTaskEventPublisher publishes TaskEvent variants.
//   - A subscriber attaches a recursive workspace subscription
//     (SubscribeWorkspaceTaskEvents → filter "tk.{ws}.>") BEFORE the child
//     task exists.
//   - A raw js.Consumer on the same subject confirms that codec-translated
//     subjects appear under the "tk" stream with the expected shape.
//
// Run profile:
//   - Main (status-changed + post-subscribe child) gated on -short.
//   - Cross-workspace isolation runs in -short mode (cheap; setupCluster1
//     is intentionally fast).
//   - Total budget < 30s in non-short mode.

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/router"
	"github.com/scitrera/aether/internal/router/natscodec"
)

// newTaskEventTestRig spins up a single-node embedded NATS cluster and
// constructs the "tk" stream via JetStreamRouter (matching production wiring).
// Returns the JetStream context plus the publisher under test.
func newTaskEventTestRig(t *testing.T) (jetstream.JetStream, *orchestration.JetStreamTaskEventPublisher) {
	t.Helper()
	es := setupCluster1(t)
	js := es.JetStream()
	// Create the "tk" stream (and friends) the same way the production
	// gateway does. The constructor is idempotent.
	if _, err := router.NewJetStreamRouter(js, 1, nil); err != nil {
		t.Fatalf("NewJetStreamRouter: %v", err)
	}
	pub := orchestration.NewJetStreamTaskEventPublisher(js)
	return js, pub
}

// TestClusterIntegration_TaskEventFanout_StatusChangedRecursive is the
// headline test for task #28.
//
// Sequence:
//  1. Bring up the single-node aetherlite cluster + JetStreamTaskEventPublisher.
//  2. Subscribe recursively to the workspace ("tk.{ws}.>") BEFORE any child
//     task exists.
//  3. Publish a status-changed progression for the PARENT task using the
//     enum values the proto exposes (QUEUED → RUNNING → COMPLETED), with the
//     conceptual sub-steps ("assigning", "starting") carried in the Reason
//     field. The TaskStatus proto enum does not have separate ASSIGNED/STARTING
//     codes (see api/proto/aether.pb.go lines 166-176); see deferred TODO.
//  4. Publish a child_lifecycle event for a CHILD task whose subject did NOT
//     exist when the subscriber registered — this is the post-subscribe-child
//     gap closure.
//  5. Assert ordering of parent status-changed events, presence of the child
//     lifecycle event, and codec-translated subject shape via a raw consumer.
func TestClusterIntegration_TaskEventFanout_StatusChangedRecursive(t *testing.T) {
	if testing.Short() {
		t.Skip("task-event fan-out integration runs an embedded NATS server; skipped in -short")
	}

	js, pub := newTaskEventTestRig(t)

	const ws = "fanout-ws"
	const parentTaskID = "task-parent-fanout"
	const childTaskID = "task-child-post-subscribe"
	const consumerName = "fanout-recursive-sub"

	// ----- Subscriber attaches BEFORE any task event is published -----
	type recv struct {
		evt    *pb.TaskEvent
		seqIdx int64
	}
	received := make(chan recv, 32)
	var seq atomic.Int64
	cancelSub, err := pub.SubscribeWorkspaceTaskEvents(consumerName, ws, func(evt *pb.TaskEvent) {
		idx := seq.Add(1)
		received <- recv{evt: evt, seqIdx: idx}
	})
	if err != nil {
		t.Fatalf("SubscribeWorkspaceTaskEvents: %v", err)
	}
	t.Cleanup(cancelSub)

	// Also attach a raw js.Consumer on the same subject filter so we can
	// confirm the codec-translated subjects are well-formed. This is the
	// "out-of-band consumer verification" called out by the task.
	wsEsc := natscodec.EscapeForSubject(ws)
	rawFilter := fmt.Sprintf("tk.%s.>", wsEsc)
	rawCtx, rawCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer rawCancel()
	rawCons, err := js.CreateOrUpdateConsumer(rawCtx, "tk", jetstream.ConsumerConfig{
		Durable:       "fanout-raw-subject-verify",
		FilterSubject: rawFilter,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create raw verification consumer: %v", err)
	}
	rawMsgs, err := rawCons.Messages()
	if err != nil {
		t.Fatalf("raw consumer Messages: %v", err)
	}
	defer rawMsgs.Stop()

	var (
		rawMu       sync.Mutex
		rawSubjects []string
	)
	rawDone := make(chan struct{})
	go func() {
		defer close(rawDone)
		for {
			m, err := rawMsgs.Next()
			if err != nil {
				return
			}
			rawMu.Lock()
			rawSubjects = append(rawSubjects, m.Subject())
			rawMu.Unlock()
			_ = m.Ack()
		}
	}()

	// Give both consumer subscriptions a beat to register at the stream
	// leader. DeliverNewPolicy drops anything published before consumer
	// creation.
	time.Sleep(400 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// ----- Publish PARENT status-changed progression -----
	// Proto enum has UNSPECIFIED, QUEUED, RUNNING, COMPLETED, FAILED, ...;
	// ASSIGNED and STARTING are not first-class enum values, so we model the
	// conceptual transitions in Reason and step through the available codes.
	now := time.Now().UnixMilli()
	parentTransitions := []struct {
		from   pb.TaskStatus
		to     pb.TaskStatus
		reason string
	}{
		{pb.TaskStatus_TASK_STATUS_UNSPECIFIED, pb.TaskStatus_TASK_STATUS_QUEUED, "pending->queued"},
		{pb.TaskStatus_TASK_STATUS_QUEUED, pb.TaskStatus_TASK_STATUS_QUEUED, "assigning"},
		{pb.TaskStatus_TASK_STATUS_QUEUED, pb.TaskStatus_TASK_STATUS_RUNNING, "starting->running"},
		{pb.TaskStatus_TASK_STATUS_RUNNING, pb.TaskStatus_TASK_STATUS_COMPLETED, "completed"},
	}
	for i, tr := range parentTransitions {
		evt := &pb.TaskEvent{
			TaskId:          parentTaskID,
			EmittedAtUnixMs: now + int64(i),
			Workspace:       ws,
			Event: &pb.TaskEvent_StatusChanged{
				StatusChanged: &pb.TaskStatusChangedEvent{
					FromStatus: tr.from,
					ToStatus:   tr.to,
					Reason:     tr.reason,
				},
			},
		}
		if err := pub.PublishTaskEvent(ctx, ws, parentTaskID, evt); err != nil {
			t.Fatalf("publish parent transition %d (%s): %v", i, tr.reason, err)
		}
	}

	// ----- Publish CHILD lifecycle event whose subject did NOT exist at
	// subscribe time -----
	childEvt := &pb.TaskEvent{
		TaskId:          childTaskID,
		ParentTaskId:    parentTaskID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       ws,
		Event: &pb.TaskEvent_ChildLifecycle{
			ChildLifecycle: &pb.TaskChildLifecycleEvent{
				ChildTaskId: childTaskID,
				ChildStatus: pb.TaskStatus_TASK_STATUS_RUNNING,
				Lifecycle:   "spawned-post-subscribe",
			},
		},
	}
	if err := pub.PublishTaskEvent(ctx, ws, childTaskID, childEvt); err != nil {
		t.Fatalf("publish child lifecycle event: %v", err)
	}

	// ----- Collect events from the recursive subscriber -----
	// Expected: 4 parent status-changed + 1 child child-lifecycle = 5 total.
	const wantTotal = 5
	collected := make([]recv, 0, wantTotal)
	deadline := time.After(10 * time.Second)
	for len(collected) < wantTotal {
		select {
		case r := <-received:
			collected = append(collected, r)
		case <-deadline:
			t.Fatalf("timeout: only collected %d/%d events (recursive subscription may have missed the child)", len(collected), wantTotal)
		}
	}

	// Verify the parent status-changed events arrived in order. The recursive
	// subscriber receives parent + child events on the same handler, so we
	// extract the parent-only events first and check their reason sequence.
	var parentReasons []string
	var sawChild bool
	for _, r := range collected {
		switch ev := r.evt.GetEvent().(type) {
		case *pb.TaskEvent_StatusChanged:
			if r.evt.GetTaskId() == parentTaskID {
				parentReasons = append(parentReasons, ev.StatusChanged.GetReason())
			}
		case *pb.TaskEvent_ChildLifecycle:
			if r.evt.GetTaskId() == childTaskID {
				sawChild = true
				if got := ev.ChildLifecycle.GetLifecycle(); got != "spawned-post-subscribe" {
					t.Errorf("child lifecycle field: got %q, want %q", got, "spawned-post-subscribe")
				}
				if got := r.evt.GetParentTaskId(); got != parentTaskID {
					t.Errorf("child parent_task_id: got %q, want %q", got, parentTaskID)
				}
			}
		}
	}

	if len(parentReasons) != len(parentTransitions) {
		t.Fatalf("parent status_changed events: got %d, want %d (reasons=%v)", len(parentReasons), len(parentTransitions), parentReasons)
	}
	for i, want := range parentTransitions {
		if parentReasons[i] != want.reason {
			t.Errorf("parent transition %d reason: got %q, want %q (order violated)", i, parentReasons[i], want.reason)
		}
	}
	if !sawChild {
		t.Fatal("post-subscribe child lifecycle event was NOT delivered (Phase 4 gap regression)")
	}

	// ----- Verify codec-translated subjects via the raw consumer -----
	// Wait briefly for the raw consumer to have caught up with the publisher.
	if ok := waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		rawMu.Lock()
		defer rawMu.Unlock()
		return len(rawSubjects) >= wantTotal
	}); !ok {
		rawMu.Lock()
		t.Fatalf("raw consumer did not observe %d subjects (got %d: %v)", wantTotal, len(rawSubjects), rawSubjects)
		rawMu.Unlock()
	}
	rawMu.Lock()
	defer rawMu.Unlock()
	// Every subject must begin with "tk.{wsEsc}." and end with ".events".
	wantParentSubject := fmt.Sprintf("tk.%s.%s.events", wsEsc, natscodec.EscapeForSubject(parentTaskID))
	wantChildSubject := fmt.Sprintf("tk.%s.%s.events", wsEsc, natscodec.EscapeForSubject(childTaskID))
	var sawParentSubject, sawChildSubject bool
	for _, s := range rawSubjects {
		switch s {
		case wantParentSubject:
			sawParentSubject = true
		case wantChildSubject:
			sawChildSubject = true
		default:
			// Unexpected subject under our workspace filter — surface it
			// rather than silently ignore.
			t.Errorf("unexpected subject under filter %q: %q", rawFilter, s)
		}
	}
	if !sawParentSubject {
		t.Errorf("did not observe expected parent subject %q", wantParentSubject)
	}
	if !sawChildSubject {
		t.Errorf("did not observe expected child subject %q (post-subscribe-child subject did not land in the tk stream)", wantChildSubject)
	}

	t.Logf("verified codec subjects: parent=%q child=%q", wantParentSubject, wantChildSubject)
}

// TestClusterIntegration_TaskEventFanout_CrossWorkspaceIsolation asserts the
// recursive workspace subscription is filtered server-side: events published
// to workspace A must NOT leak to a subscriber on workspace B.
//
// Runs in -short on purpose: cheap (single-node setupCluster1) and the
// isolation guarantee is foundational enough that fast-CI lanes should keep
// verifying it.
func TestClusterIntegration_TaskEventFanout_CrossWorkspaceIsolation(t *testing.T) {
	_, pub := newTaskEventTestRig(t)

	const wsA = "iso-ws-alpha"
	const wsB = "iso-ws-beta"
	const taskA = "task-in-alpha"
	const taskB = "task-in-beta"

	// Subscriber on workspace B only.
	receivedB := make(chan *pb.TaskEvent, 8)
	cancelB, err := pub.SubscribeWorkspaceTaskEvents("iso-sub-beta", wsB, func(evt *pb.TaskEvent) {
		receivedB <- evt
	})
	if err != nil {
		t.Fatalf("SubscribeWorkspaceTaskEvents(beta): %v", err)
	}
	t.Cleanup(cancelB)

	// Subscriber on workspace A — sanity check that workspace A's events DO
	// reach an A subscriber (otherwise the isolation assertion is vacuous).
	receivedA := make(chan *pb.TaskEvent, 8)
	cancelA, err := pub.SubscribeWorkspaceTaskEvents("iso-sub-alpha", wsA, func(evt *pb.TaskEvent) {
		receivedA <- evt
	})
	if err != nil {
		t.Fatalf("SubscribeWorkspaceTaskEvents(alpha): %v", err)
	}
	t.Cleanup(cancelA)

	time.Sleep(400 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish to workspace A.
	if err := pub.PublishTaskEvent(ctx, wsA, taskA, &pb.TaskEvent{
		TaskId:          taskA,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       wsA,
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_QUEUED,
				ToStatus:   pb.TaskStatus_TASK_STATUS_RUNNING,
				Reason:     "alpha event",
			},
		},
	}); err != nil {
		t.Fatalf("publish to wsA: %v", err)
	}
	// Publish to workspace B (so we know the B subscriber wakes up at all).
	if err := pub.PublishTaskEvent(ctx, wsB, taskB, &pb.TaskEvent{
		TaskId:          taskB,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       wsB,
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_QUEUED,
				ToStatus:   pb.TaskStatus_TASK_STATUS_RUNNING,
				Reason:     "beta event",
			},
		},
	}); err != nil {
		t.Fatalf("publish to wsB: %v", err)
	}

	// Sanity: A subscriber must see exactly one event (the wsA one).
	select {
	case gotA := <-receivedA:
		if gotA.GetTaskId() != taskA || gotA.GetWorkspace() != wsA {
			t.Errorf("wsA subscriber got task=%q ws=%q, want task=%q ws=%q",
				gotA.GetTaskId(), gotA.GetWorkspace(), taskA, wsA)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wsA subscriber did not receive its own workspace event (sanity check failed)")
	}

	// Isolation: B subscriber must see ONLY the wsB event, never the wsA one.
	select {
	case gotB := <-receivedB:
		if gotB.GetWorkspace() == wsA || gotB.GetTaskId() == taskA {
			t.Fatalf("ISOLATION BREACH: wsB subscriber received wsA event (task=%q ws=%q)", gotB.GetTaskId(), gotB.GetWorkspace())
		}
		if gotB.GetTaskId() != taskB || gotB.GetWorkspace() != wsB {
			t.Errorf("wsB subscriber got task=%q ws=%q, want task=%q ws=%q",
				gotB.GetTaskId(), gotB.GetWorkspace(), taskB, wsB)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wsB subscriber did not receive its own workspace event")
	}

	// Cross-check: drain any further events from receivedB for a short window
	// and assert none of them are wsA leaks. The wildcard filter is enforced
	// server-side, so this is belt-and-suspenders, not a race-fixer.
	deadline := time.After(750 * time.Millisecond)
drain:
	for {
		select {
		case extra := <-receivedB:
			if extra.GetWorkspace() == wsA || extra.GetTaskId() == taskA {
				t.Fatalf("ISOLATION BREACH (drain): wsB subscriber received wsA event (task=%q ws=%q)", extra.GetTaskId(), extra.GetWorkspace())
			}
		case <-deadline:
			break drain
		}
	}
}
