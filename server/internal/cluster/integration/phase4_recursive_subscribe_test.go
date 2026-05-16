package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/router"
)

// TestClusterIntegration_Phase4_PostSubscribeChild_CrossNode is the HEADLINE
// test for the Phase 4 gap that motivated converting the task event publisher
// to JetStream subjects. The in-process router-backed publisher takes a
// snapshot of the parent task's children at subscribe time; any child task
// spawned AFTER the subscription was created is lost. JetStream wildcard
// filters ("tk.{ws}.>") close this gap because they cover the whole workspace
// subtree at the server side — new subjects appear in the consumer's filter
// automatically.
//
// Sequence:
//  1. Bring up a 3-node cluster + JetStreamRouter on every node (so the "tk"
//     stream gets created with Replicas=3).
//  2. Create a JetStreamTaskEventPublisher on node-0.
//  3. Subscribe recursively (SubscribeWorkspaceTaskEvents) on node-0 with the
//     wildcard tk.{ws}.>.
//  4. Publish a child task event from node-1 (different node from the
//     subscriber) AFTER the subscription was established.
//  5. Assert the subscriber receives the event within 3s.
//
// If this test fails, the cluster has NOT closed the Phase 4 gap.
func TestClusterIntegration_Phase4_PostSubscribeChild_CrossNode(t *testing.T) {
	c := setupCluster3(t)

	// Use NewJetStreamRouter on each node to ensure the "tk" stream is created
	// with Replicas=3. The router constructor is idempotent so we can call it
	// on multiple nodes without conflict.
	if _, err := router.NewJetStreamRouter(c.Node(0).JetStream(), 3, nil); err != nil {
		t.Fatalf("router on node-a: %v", err)
	}

	// Publisher / subscriber lives on node-0. The test publishes via node-1's
	// JetStream context to genuinely exercise the cross-node path.
	subscriberJS := c.Node(0).JetStream()
	publisherJS := c.Node(1).JetStream()

	subscriberPub := orchestration.NewJetStreamTaskEventPublisher(subscriberJS)
	publisherPub := orchestration.NewJetStreamTaskEventPublisher(publisherJS)

	const ws = "phase4-ws"
	const parentTaskID = "parent-001"
	const childTaskID = "child-001"
	const consumerName = "phase4-recursive-sub"

	// Establish the recursive workspace subscription BEFORE the child event
	// is published. The wildcard subject is tk.{ws}.> — any future task_id
	// matches without any further subscriber action.
	received := make(chan *pb.TaskEvent, 4)
	var receivedCount atomic.Int64
	cancelSub, err := subscriberPub.SubscribeWorkspaceTaskEvents(consumerName, ws, func(evt *pb.TaskEvent) {
		// Filter to just the child we care about so the test isn't sensitive
		// to other events appearing on the same workspace subject tree.
		if evt.GetTaskId() == childTaskID {
			receivedCount.Add(1)
			select {
			case received <- evt:
			default:
			}
		}
	})
	if err != nil {
		t.Fatalf("SubscribeWorkspaceTaskEvents: %v", err)
	}
	defer cancelSub()

	// Give the consumer a moment to register at the stream leader. Without
	// this, the publish below can occasionally land before the consumer's
	// filter is in place. DeliverNewPolicy means anything published before
	// the consumer is created is invisible.
	time.Sleep(500 * time.Millisecond)

	// Now publish the child event from node-1.
	childEvt := &pb.TaskEvent{
		TaskId:          childTaskID,
		ParentTaskId:    parentTaskID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       ws,
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: pb.TaskStatus_TASK_STATUS_QUEUED,
				ToStatus:   pb.TaskStatus_TASK_STATUS_RUNNING,
				Reason:     "child spawned post-subscribe",
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := publisherPub.PublishTaskEvent(ctx, ws, childTaskID, childEvt); err != nil {
		t.Fatalf("PublishTaskEvent: %v", err)
	}

	select {
	case got := <-received:
		if got.GetTaskId() != childTaskID {
			t.Errorf("task_id: got %q, want %q", got.GetTaskId(), childTaskID)
		}
		if got.GetParentTaskId() != parentTaskID {
			t.Errorf("parent_task_id: got %q, want %q", got.GetParentTaskId(), parentTaskID)
		}
		if sc := got.GetStatusChanged(); sc == nil || sc.GetToStatus() != pb.TaskStatus_TASK_STATUS_RUNNING {
			t.Errorf("unexpected event variant: %+v", got.GetEvent())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: cross-node post-subscribe child event was NOT delivered (Phase 4 gap regression)")
	}
}

// helper for sanity-checking stream presence; not used in this test directly
// but kept here as a documented reference for future tests that need to
// confirm the tk stream was created cluster-wide.
var _ = jetstream.StreamConfig{}
