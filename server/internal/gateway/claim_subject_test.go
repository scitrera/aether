package gateway

// Tests for the task-driven chat lifecycle feature (Phase 1, Aether side):
//   - TaskOperation_CLAIM transitions an assigned/pending task to running and
//     fires a status notification.
//   - notifyTaskStatusChange emits an ADDITIONAL ProgressUpdate to the task's
//     OBO subject (pg::us::<subject>, Kind=TASK) when the task opted into
//     subject participation.
//   - authorizeTaskOp authorizes the OBO subject (and denies non-subjects).
//
// These reuse the native-sqlite gateway harness from task_test.go
// (newTaskTestServerWithSQLiteStore) so the real store + auth + notify paths
// are exercised end-to-end.

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
	"google.golang.org/protobuf/proto"
)

// subjectUserIdentity returns a window-specific User identity in ws1. The
// stored task subject id matches userID (the window specifier differs, as it
// would for a real connected browser tab).
func subjectUserIdentity(userID, window string) models.Identity {
	return models.Identity{
		Type:      models.PrincipalUser,
		Workspace: "ws1",
		ID:        userID,
		Specifier: window,
	}
}

// collectPublished snapshots the mock router's published messages.
func collectPublished(t *testing.T, s *GatewayServer) []publishedMsg {
	t.Helper()
	router, ok := s.router.(*mockMessageRouter)
	if !ok {
		t.Fatalf("server router is not *mockMessageRouter")
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	return append([]publishedMsg(nil), router.publishedMessages...)
}

// TestTaskOpClaim_AssignedTaskTransitionsToRunning verifies the assignee can
// CLAIM an assigned task, moving it to running, and that a TASK-kind status
// notification is published.
func TestTaskOpClaim_AssignedTaskTransitionsToRunning(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()
	ctx := context.Background()

	task := &tasks.Task{
		TaskID:        "task-claim-assigned",
		TaskType:      "chat_message",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusPending,
		ParentAgentID: aliceTopic, // creator, so authorized
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := s.taskStore.AssignTask(ctx, task.TaskID, aliceTopic); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}

	stream := &mockStream{}
	client := newTaskTestClient(stream, alice)

	op := &pb.TaskOperation{
		Op:        pb.TaskOperation_CLAIM,
		TaskId:    task.TaskID,
		RequestId: "claim-1",
	}
	s.handleTaskOp(ctx, client, op)

	stream.mu.Lock()
	resp := stream.sent[0].GetTaskOp()
	stream.mu.Unlock()

	if resp == nil {
		t.Fatal("expected TaskOperationResponse")
	}
	if !resp.Success {
		t.Fatalf("CLAIM: expected Success=true, got error=%q", resp.Error)
	}
	if resp.Task == nil || resp.Task.Status != pb.TaskStatus_TASK_STATUS_RUNNING {
		t.Errorf("CLAIM: expected returned task status RUNNING, got %v", resp.Task.GetStatus())
	}

	// Verify the stored task transitioned to running.
	updated, err := s.taskStore.GetTask(ctx, task.TaskID)
	if err != nil || updated == nil {
		t.Fatalf("GetTask after claim: %v", err)
	}
	if updated.Status != tasks.TaskStatusRunning {
		t.Errorf("stored status = %q, want running", updated.Status)
	}

	// A status notification should have been published (parent recipient).
	published := collectPublished(t, s)
	foundRunning := false
	for _, m := range published {
		var u pb.ProgressUpdate
		if proto.Unmarshal(m.payload, &u) != nil {
			continue
		}
		if u.TaskId == task.TaskID && u.State == "running" && u.Kind == pb.ProgressKind_PROGRESS_KIND_TASK {
			foundRunning = true
		}
	}
	if !foundRunning {
		t.Errorf("expected a running TASK-kind status notification; published=%d", len(published))
	}
}

// TestTaskOpClaim_Idempotent verifies re-claiming a running task succeeds
// (idempotent for multi-tab scenarios).
func TestTaskOpClaim_Idempotent(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	alice := callerIdentity("worker", "alice")
	aliceTopic := alice.ToTopic()
	ctx := context.Background()

	task := &tasks.Task{
		TaskID:        "task-claim-idem",
		TaskType:      "chat_message",
		Workspace:     "ws1",
		Status:        tasks.TaskStatusPending,
		ParentAgentID: aliceTopic,
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := s.taskStore.AssignTask(ctx, task.TaskID, aliceTopic); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	// First claim.
	if err := s.taskStore.ClaimTask(ctx, task.TaskID); err != nil {
		t.Fatalf("first ClaimTask: %v", err)
	}
	// Second claim must be a no-op success.
	if err := s.taskStore.ClaimTask(ctx, task.TaskID); err != nil {
		t.Errorf("idempotent ClaimTask: expected nil error, got %v", err)
	}
}

// TestSubjectNotify_PublishesToUserProgressTopic verifies that a task marked
// with subject_participant and a User OBO subject publishes an ADDITIONAL
// ProgressUpdate to pg::us::<subject> with Kind=TASK and the carried metadata
// (thread_id / message_id / task_type / status) on a lifecycle transition.
func TestSubjectNotify_PublishesToUserProgressTopic(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()
	subjectID := "dev@example.com"

	task := &tasks.Task{
		TaskID:    "task-subject-notify",
		TaskType:  "chat_message",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
		Metadata: map[string]interface{}{
			"subject_participant": true,
			"thread_id":           "thread-1",
			"message_id":          "msg-1",
		},
		Authority: tasks.TaskAuthorityInfo{
			SubjectType: string(models.PrincipalUser),
			SubjectID:   subjectID,
		},
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Trigger a transition notification directly (mirrors the post-op path).
	s.notifyTaskStatusChangeFromTaskID(ctx, task.TaskID, "running", "")

	published := collectPublished(t, s)
	wantTopic := "pg::us::" + subjectID
	var subjectUpdate *pb.ProgressUpdate
	for _, m := range published {
		if m.topic != wantTopic {
			continue
		}
		var u pb.ProgressUpdate
		if proto.Unmarshal(m.payload, &u) != nil {
			continue
		}
		subjectUpdate = &u
	}
	if subjectUpdate == nil {
		t.Fatalf("expected a subject ProgressUpdate on %q; published topics=%v", wantTopic, topicsOf(published))
	}
	if subjectUpdate.Kind != pb.ProgressKind_PROGRESS_KIND_TASK {
		t.Errorf("subject update Kind = %v, want TASK", subjectUpdate.Kind)
	}
	if subjectUpdate.State != "running" {
		t.Errorf("subject update State = %q, want running", subjectUpdate.State)
	}
	wantRecipient := "us::" + subjectID
	if subjectUpdate.Recipient != wantRecipient {
		t.Errorf("subject update Recipient = %q, want %q (bare user)", subjectUpdate.Recipient, wantRecipient)
	}
	if subjectUpdate.Metadata["thread_id"] != "thread-1" {
		t.Errorf("subject metadata thread_id = %q, want thread-1", subjectUpdate.Metadata["thread_id"])
	}
	if subjectUpdate.Metadata["message_id"] != "msg-1" {
		t.Errorf("subject metadata message_id = %q, want msg-1", subjectUpdate.Metadata["message_id"])
	}
	if subjectUpdate.Metadata["task_type"] != "chat_message" {
		t.Errorf("subject metadata task_type = %q, want chat_message", subjectUpdate.Metadata["task_type"])
	}
	if subjectUpdate.Metadata["status"] != "running" {
		t.Errorf("subject metadata status = %q, want running", subjectUpdate.Metadata["status"])
	}
}

// TestSubjectNotify_SkippedWhenNotParticipant verifies that a task WITHOUT the
// subject_participant flag does not publish to the per-user topic.
func TestSubjectNotify_SkippedWhenNotParticipant(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()
	subjectID := "dev@example.com"

	task := &tasks.Task{
		TaskID:    "task-no-subject-notify",
		TaskType:  "chat_message",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
		// subject_participant intentionally absent
		Authority: tasks.TaskAuthorityInfo{
			SubjectType: string(models.PrincipalUser),
			SubjectID:   subjectID,
		},
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	s.notifyTaskStatusChangeFromTaskID(ctx, task.TaskID, "running", "")

	for _, m := range collectPublished(t, s) {
		if m.topic == "pg::us::"+subjectID {
			t.Errorf("did not expect a subject notification when subject_participant is unset")
		}
	}
}

// TestTaskOpAuth_SubjectMatch verifies the OBO subject (the end user) is
// authorized to operate on its own task, matched by identity ID+type rather
// than topic string (the connected user topic carries a window specifier the
// stored subject id does not).
func TestTaskOpAuth_SubjectMatch(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()
	subjectID := "dev@example.com"

	task := &tasks.Task{
		TaskID:    "task-subject-auth",
		TaskType:  "chat_message",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
		Authority: tasks.TaskAuthorityInfo{
			SubjectType: string(models.PrincipalUser),
			SubjectID:   subjectID,
		},
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Caller is the subject user, connected from a specific window/tab.
	subjectClient := newTaskTestClient(&mockStream{}, subjectUserIdentity(subjectID, "win-1"))

	if !s.authorizeTaskOp(ctx, subjectClient, task) {
		t.Errorf("expected OBO subject to be authorized to operate on its own task")
	}
}

// TestTaskOpAuth_NonSubjectUserDenied verifies a User who is NOT the task's OBO
// subject is not authorized via the subject path. ACL is nil, so to isolate the
// subject check we put the caller in a different workspace (which short-circuits
// to deny before any fail-open workspace match).
func TestTaskOpAuth_NonSubjectUserDenied(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()

	task := &tasks.Task{
		TaskID:    "task-nonsubject-auth",
		TaskType:  "chat_message",
		Workspace: "ws1",
		Status:    tasks.TaskStatusPending,
		Authority: tasks.TaskAuthorityInfo{
			SubjectType: string(models.PrincipalUser),
			SubjectID:   "owner@example.com",
		},
	}
	if err := s.taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// A different user in a different workspace must be denied.
	other := models.Identity{
		Type:      models.PrincipalUser,
		Workspace: "ws2",
		ID:        "intruder@example.com",
		Specifier: "win-9",
	}
	otherClient := newTaskTestClient(&mockStream{}, other)

	if s.authorizeTaskOp(ctx, otherClient, task) {
		t.Errorf("expected non-subject user to be denied")
	}
}

func topicsOf(msgs []publishedMsg) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.topic)
	}
	return out
}
