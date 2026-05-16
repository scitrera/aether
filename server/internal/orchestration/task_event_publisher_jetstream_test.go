// Tests for JetStreamTaskEventPublisher.
//
// Headline test: TestJetStreamTaskEventPublisher_RecursiveSubscribe_PostSubscribeChild
// proves the WIP guide's snapshot-at-subscribe gap is closed — a child task
// spawned AFTER consumer creation is still delivered to a recursive workspace
// subscriber because the JetStream wildcard filter "tk.{ws}.>" covers all
// future subjects.

package orchestration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/scitrera/aether/api/proto"
)

// newTestJSPublisher spins up an in-process NATS server, ensures the "tk"
// stream exists (JetStreamRouter does this in production; we replicate it
// minimally here), and returns a ready JetStreamTaskEventPublisher + cleanup.
func newTestJSPublisher(t *testing.T) (*JetStreamTaskEventPublisher, func()) {
	t.Helper()
	js, stopNATS := startTestNATSServer(t)

	// Ensure the "tk" stream exists (mirrors what JetStreamRouter does at startup).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "tk",
		Subjects:  []string{"tk.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
	})
	if err != nil {
		stopNATS()
		t.Fatalf("create tk stream: %v", err)
	}

	pub := NewJetStreamTaskEventPublisher(js)
	return pub, stopNATS
}

// --- Test 1 ---

// TestJetStreamTaskEventPublisher_StatusChanged_PublishedToSubject publishes a
// status_changed event and asserts the subscriber on the same task topic
// receives it within the timeout.
func TestJetStreamTaskEventPublisher_StatusChanged_PublishedToSubject(t *testing.T) {
	pub, stop := newTestJSPublisher(t)
	defer stop()

	const ws = "test-ws"
	const taskID = "task-abc"
	const consumerName = "test-sub-status-changed"

	received := make(chan *pb.TaskEvent, 1)
	cancel, err := pub.SubscribeTaskEvents(consumerName, ws, taskID, func(evt *pb.TaskEvent) {
		received <- evt
	})
	if err != nil {
		t.Fatalf("SubscribeTaskEvents: %v", err)
	}
	defer cancel()

	evt := &pb.TaskEvent{
		TaskId:          taskID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       ws,
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_QUEUED,
				ToStatus:   pb.TaskStatus_TASK_STATUS_RUNNING,
				Reason:     "started",
			},
		},
	}

	ctx := context.Background()
	if err := pub.PublishTaskEvent(ctx, ws, taskID, evt); err != nil {
		t.Fatalf("PublishTaskEvent: %v", err)
	}

	select {
	case got := <-received:
		if got.GetTaskId() != taskID {
			t.Errorf("task_id: got %q, want %q", got.GetTaskId(), taskID)
		}
		sc := got.GetStatusChanged()
		if sc == nil {
			t.Fatal("expected status_changed event variant")
		}
		if sc.GetToStatus() != pb.TaskStatus_TASK_STATUS_RUNNING {
			t.Errorf("to_status: got %v, want RUNNING", sc.GetToStatus())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for task event delivery")
	}
}

// --- Test 2 (headline) ---

// TestJetStreamTaskEventPublisher_RecursiveSubscribe_PostSubscribeChild proves
// the snapshot-at-subscribe gap is closed: a subscriber using
// SubscribeWorkspaceTaskEvents (filter "tk.{ws}.>") receives events from a
// child task whose events subject did NOT exist at subscribe time.
func TestJetStreamTaskEventPublisher_RecursiveSubscribe_PostSubscribeChild(t *testing.T) {
	pub, stop := newTestJSPublisher(t)
	defer stop()

	const ws = "recursive-ws"
	const parentID = "parent-task-1"
	const childID = "child-task-spawned-later"
	const consumerName = "recursive-sub-test"

	var (
		mu       sync.Mutex
		received []*pb.TaskEvent
		gotChild = make(chan struct{})
	)

	// Subscribe to the full workspace BEFORE the child task subject exists.
	cancelSub, err := pub.SubscribeWorkspaceTaskEvents(consumerName, ws, func(evt *pb.TaskEvent) {
		mu.Lock()
		received = append(received, evt)
		if evt.GetTaskId() == childID {
			select {
			case <-gotChild:
			default:
				close(gotChild)
			}
		}
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("SubscribeWorkspaceTaskEvents: %v", err)
	}
	defer cancelSub()

	ctx := context.Background()

	// Publish a parent event (pre-exists at subscribe time — sanity check).
	parentEvt := &pb.TaskEvent{
		TaskId:          parentID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       ws,
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_QUEUED,
				ToStatus:   pb.TaskStatus_TASK_STATUS_RUNNING,
			},
		},
	}
	if err := pub.PublishTaskEvent(ctx, ws, parentID, parentEvt); err != nil {
		t.Fatalf("publish parent event: %v", err)
	}

	// Publish a child event AFTER consumer creation. With a snapshot-at-subscribe
	// implementation this would be missed; the JetStream wildcard captures it.
	childEvt := &pb.TaskEvent{
		TaskId:          childID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       ws,
		ParentTaskId:    parentID,
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_QUEUED,
				ToStatus:   pb.TaskStatus_TASK_STATUS_RUNNING,
				Reason:     "child started after subscribe",
			},
		},
	}
	if err := pub.PublishTaskEvent(ctx, ws, childID, childEvt); err != nil {
		t.Fatalf("publish child event: %v", err)
	}

	// The subscriber MUST see the child event even though the child's subject
	// did not exist when the consumer was created.
	select {
	case <-gotChild:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: child event not received — recursive wildcard subscription failed to close the post-subscribe gap")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) < 2 {
		t.Errorf("expected at least 2 events (parent + child), got %d", len(received))
	}
}

// --- Test 3 ---

// TestJetStreamTaskEventPublisher_AllEventVariants publishes one event of each
// variant (status_changed, progress, child_lifecycle, authority_request_relay)
// and asserts all four are received and correctly typed.
func TestJetStreamTaskEventPublisher_AllEventVariants(t *testing.T) {
	pub, stop := newTestJSPublisher(t)
	defer stop()

	const ws = "variants-ws"
	const taskID = "task-variants"
	const consumerName = "all-variants-sub"

	received := make(chan *pb.TaskEvent, 8)
	cancel, err := pub.SubscribeTaskEvents(consumerName, ws, taskID, func(evt *pb.TaskEvent) {
		received <- evt
	})
	if err != nil {
		t.Fatalf("SubscribeTaskEvents: %v", err)
	}
	defer cancel()

	ctx := context.Background()
	now := time.Now().UnixMilli()

	events := []*pb.TaskEvent{
		{
			TaskId:          taskID,
			EmittedAtUnixMs: now,
			Workspace:       ws,
			Event: &pb.TaskEvent_StatusChanged{
				StatusChanged: &pb.TaskStatusChangedEvent{
					FromStatus: pb.TaskStatus_TASK_STATUS_QUEUED,
					ToStatus:   pb.TaskStatus_TASK_STATUS_RUNNING,
				},
			},
		},
		{
			TaskId:          taskID,
			EmittedAtUnixMs: now,
			Workspace:       ws,
			Event: &pb.TaskEvent_Progress{
				Progress: &pb.TaskProgressEvent{
					Message:  "50% done",
					Progress: 0.5,
				},
			},
		},
		{
			TaskId:          taskID,
			EmittedAtUnixMs: now,
			Workspace:       ws,
			Event: &pb.TaskEvent_ChildLifecycle{
				ChildLifecycle: &pb.TaskChildLifecycleEvent{
					ChildTaskId: "child-xyz",
					ChildStatus: pb.TaskStatus_TASK_STATUS_COMPLETED,
					Lifecycle:   "completed",
				},
			},
		},
		{
			TaskId:          taskID,
			EmittedAtUnixMs: now,
			Workspace:       ws,
			Event: &pb.TaskEvent_AuthorityRequest{
				AuthorityRequest: &pb.TaskAuthorityRequestEventRelay{
					Event: &pb.AuthorityRequestEvent{
						EventType: pb.AuthorityRequestEvent_AUTHORITY_REQUEST_EVENT_CREATED,
					},
				},
			},
		},
	}

	for _, evt := range events {
		if err := pub.PublishTaskEvent(ctx, ws, taskID, evt); err != nil {
			t.Fatalf("PublishTaskEvent: %v", err)
		}
	}

	var (
		gotStatusChanged    bool
		gotProgress         bool
		gotChildLifecycle   bool
		gotAuthorityRequest bool
	)

	deadline := time.After(5 * time.Second)
	for i := 0; i < 4; i++ {
		select {
		case got := <-received:
			switch got.GetEvent().(type) {
			case *pb.TaskEvent_StatusChanged:
				gotStatusChanged = true
			case *pb.TaskEvent_Progress:
				gotProgress = true
			case *pb.TaskEvent_ChildLifecycle:
				gotChildLifecycle = true
			case *pb.TaskEvent_AuthorityRequest:
				gotAuthorityRequest = true
			}
		case <-deadline:
			t.Fatalf("timeout waiting for all event variants (received %d/4)", i)
		}
	}

	if !gotStatusChanged {
		t.Error("did not receive status_changed event")
	}
	if !gotProgress {
		t.Error("did not receive progress event")
	}
	if !gotChildLifecycle {
		t.Error("did not receive child_lifecycle event")
	}
	if !gotAuthorityRequest {
		t.Error("did not receive authority_request event")
	}
}
