package gateway

// Tests for Phase 2 "Design M": task-driven chat streaming on the per-task
// message lane (tk::{ws}::{task}::msg). Covers:
//   (a) the tk::…::msg topic passes validateTopicFormat + enforceTopicPermissions
//       and workspaceFromTopic extracts the workspace,
//   (b) a running PROGRESS_KIND_TASK subject-notice run through
//       createProgressFilterHandler subscribes the session to the task-message
//       lane (shared), and a terminal notice removes it,
//   (c) backfillRunningSubjectTaskMessages subscribes a running subject-task's
//       message lane (exclusive, from timestamp),
//   (d) a MessageEnvelope delivered through createMessageHandler on the msg
//       topic arrives at the session as DownstreamMessage_Msg.
//
// Reuses the native-sqlite harness (newTaskTestServerWithSQLiteStore),
// newTaskTestClient, callerIdentity, subjectUserIdentity, and the
// mockMessageRouter shared/exclusive topic assertions.

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
	"google.golang.org/protobuf/proto"
)

// (a) The task-message topic is a well-formed, user-publishable topic and its
// workspace is recoverable.
func TestTaskMessageTopic_ValidatesAndResolvesWorkspace(t *testing.T) {
	topic, err := models.TaskMessageTopic("ws1", "task1")
	if err != nil {
		t.Fatalf("TaskMessageTopic: %v", err)
	}
	if topic != "tk::ws1::task1::msg" {
		t.Fatalf("TaskMessageTopic = %q, want tk::ws1::task1::msg", topic)
	}

	if err := validateTopicFormat(topic); err != nil {
		t.Errorf("validateTopicFormat(%q) = %v, want nil", topic, err)
	}

	// A user principal must be allowed to publish to the lane (chat tokens are
	// produced by the agent, but the matrix gate must not block tk::).
	user := subjectUserIdentity("dev@example.com", "tab1")
	if err := enforceTopicPermissions(user, topic); err != nil {
		t.Errorf("enforceTopicPermissions(user, %q) = %v, want nil", topic, err)
	}
	// An agent principal (the actual producer) must also pass.
	agent := callerIdentity("worker", "alice")
	if err := enforceTopicPermissions(agent, topic); err != nil {
		t.Errorf("enforceTopicPermissions(agent, %q) = %v, want nil", topic, err)
	}

	if ws := workspaceFromTopic(topic); ws != "ws1" {
		t.Errorf("workspaceFromTopic(%q) = %q, want ws1", topic, ws)
	}
}

// runSubjectNoticeThroughFilter marshals a PROGRESS_KIND_TASK subject-notice
// and feeds it through the per-client progress filter handler, exactly as the
// pg::us::<user> stream consumer would.
func runSubjectNoticeThroughFilter(t *testing.T, s *GatewayServer, client *ClientSession, workspace, taskID, status string) {
	t.Helper()
	update := &pb.ProgressUpdate{
		Source:      s.gatewayID,
		TaskId:      taskID,
		State:       status,
		TimestampMs: time.Now().UnixMilli(),
		Workspace:   workspace,
		// Bare-user recipient → matches all of the user's windows.
		Recipient: "us" + models.IdentitySep + client.Identity.ID,
		Kind:      pb.ProgressKind_PROGRESS_KIND_TASK,
		Metadata:  map[string]string{"status": status},
	}
	b, err := proto.Marshal(update)
	if err != nil {
		t.Fatalf("marshal ProgressUpdate: %v", err)
	}
	s.createProgressFilterHandler(client)(b)
}

// (b) Lifecycle-driven subscribe/unsubscribe on the shared lane.
func TestProgressFilter_TaskLifecycle_SubscribesAndUnsubscribesMsgLane(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	router := s.router.(*mockMessageRouter)
	taskID := "task-lifecycle"
	subject := subjectUserIdentity("dev@example.com", "tab1")

	stream := &mockStream{}
	client := newTaskTestClient(stream, subject)
	client.deliveryCh = make(chan *pb.DownstreamMessage, 8)

	msgTopic := models.MustTaskMessageTopic("ws1", taskID)

	// Running notice → subscribe. The live lane now uses a per-(window,task)
	// named resume-or-tail consumer (was anonymous full-replay), so the mock
	// records it on the exclusive path.
	runSubjectNoticeThroughFilter(t, s, client, "ws1", taskID, "running")
	if !router.hasExclusiveTopic(msgTopic) {
		t.Fatalf("expected exclusive (resume-or-tail) subscription to %q after running notice", msgTopic)
	}
	if !client.HasSubscription(taskMsgSubKey(taskID)) {
		t.Errorf("expected client subscription under %q", taskMsgSubKey(taskID))
	}

	// Terminal notice → unsubscribe (subscription removed from the session).
	runSubjectNoticeThroughFilter(t, s, client, "ws1", taskID, "completed")
	if client.HasSubscription(taskMsgSubKey(taskID)) {
		t.Errorf("expected client subscription %q removed after terminal notice", taskMsgSubKey(taskID))
	}
}

// (c) Connect-time backfill of in-flight running subject tasks.
func TestBackfillRunningSubjectTaskMessages_SubscribesFromTimestamp(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	router := s.router.(*mockMessageRouter)
	ctx := context.Background()
	subjectID := "dev@example.com"
	taskID := "task-backfill-running"
	started := time.Now().Add(-1 * time.Minute)

	task := &tasks.Task{
		TaskID:    taskID,
		TaskType:  "chat_message",
		Workspace: "ws1",
		Status:    tasks.TaskStatusRunning,
		StartedAt: &started,
		Metadata: map[string]interface{}{
			"subject_participant": true,
		},
		Authority: tasks.TaskAuthorityInfo{
			SubjectType: string(models.PrincipalUser),
			SubjectID:   subjectID,
		},
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// A non-subject running task in the same workspace must NOT be picked up.
	otherID := "task-other-subject"
	otherTask := &tasks.Task{
		TaskID:    otherID,
		TaskType:  "chat_message",
		Workspace: "ws1",
		Status:    tasks.TaskStatusRunning,
		StartedAt: &started,
		Metadata: map[string]interface{}{
			"subject_participant": true,
		},
		Authority: tasks.TaskAuthorityInfo{
			SubjectType: string(models.PrincipalUser),
			SubjectID:   "someone-else@example.com",
		},
	}
	if err := s.taskStore.CreateTask(ctx, otherTask); err != nil {
		t.Fatalf("CreateTask(other): %v", err)
	}

	stream := &mockStream{}
	client := newTaskTestClient(stream, subjectUserIdentity(subjectID, "tab1"))
	client.deliveryCh = make(chan *pb.DownstreamMessage, 8)

	s.backfillRunningSubjectTaskMessages(ctx, client)

	msgTopic := models.MustTaskMessageTopic("ws1", taskID)
	if !router.hasExclusiveTopic(msgTopic) {
		t.Fatalf("expected exclusive subscription to %q after backfill", msgTopic)
	}
	if !client.HasSubscription(taskMsgSubKey(taskID)) {
		t.Errorf("expected client subscription under %q", taskMsgSubKey(taskID))
	}

	otherTopic := models.MustTaskMessageTopic("ws1", otherID)
	if router.hasExclusiveTopic(otherTopic) {
		t.Errorf("did not expect subscription to another subject's task lane %q", otherTopic)
	}
}

// (d) A MessageEnvelope delivered through createMessageHandler on the msg topic
// arrives as DownstreamMessage_Msg.
func TestTaskMessageLane_DeliversEnvelopeAsDownstreamMsg(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	subject := subjectUserIdentity("dev@example.com", "tab1")
	stream := &mockStream{}
	client := newTaskTestClient(stream, subject)
	client.deliveryCh = make(chan *pb.DownstreamMessage, 8)

	envelope := &pb.MessageEnvelope{
		Source:      models.MustAgentTopic("ws1", "worker", "alice"),
		Payload:     []byte("hello chat"),
		MessageType: pb.MessageType_CHAT,
		TimestampMs: time.Now().UnixMilli(),
	}
	b, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal MessageEnvelope: %v", err)
	}

	s.createMessageHandler(client)(b)

	select {
	case got := <-client.deliveryCh:
		incoming := got.GetMsg()
		if incoming == nil {
			t.Fatalf("expected DownstreamMessage_Msg, got %T", got.Payload)
		}
		if string(incoming.Payload) != "hello chat" {
			t.Errorf("payload = %q, want %q", string(incoming.Payload), "hello chat")
		}
		if incoming.MessageType != pb.MessageType_CHAT {
			t.Errorf("message type = %v, want CHAT", incoming.MessageType)
		}
		if incoming.SourceTopic != envelope.Source {
			t.Errorf("source topic = %q, want %q", incoming.SourceTopic, envelope.Source)
		}
	default:
		t.Fatal("expected a delivered DownstreamMessage on deliveryCh")
	}
}
