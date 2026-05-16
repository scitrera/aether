package orchestration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/tasks"
)

// TestHibernationE2E_PauseWakeAssignsWithCheckpoint exercises the full Phase 3
// hibernation flow at the orchestration layer without a live gRPC session:
//
//  1. Create an orchestrated task, assign it, start it (running).
//  2. Pause it to HIBERNATED with a WaitSpec carrying a HibernationDescriptor.
//  3. Verify status flips to HIBERNATED and the WaitSpec round-trips correctly.
//  4. Call WakeHibernatedTask (simulating the task_waker scheduled-wake path).
//  5. Verify the task is now PENDING and WaitSpec is cleared.
//  6. Verify the reserved metadata keys are populated with checkpoint/session values.
//  7. Verify a row exists in orchestrated_task_queue with the correct profile
//     and implementation (orchestrator pickup precondition).
//  8. Simulate the orchestrator-pickup handoff: verify the metadata keys that
//     applyHibernationHandoffToAssignment (in package gateway) reads are
//     correctly set so the delivery sites can populate TaskAssignment.{CheckpointKey,
//     ResumeSessionId}. The helper is tested in isolation in
//     gateway/hibernation_handoff_test.go; here we confirm the orchestration
//     layer produces the expected metadata shape that the helper depends on.
//
// Note on gateway bypass: the PAUSE step calls taskService.PauseTask directly,
// bypassing validateHibernationPrecondition (which requires a CheckpointManager
// and is covered by its own gateway unit test from Stage A). This test focuses
// on orchestration-layer semantics: pause → wake → pending → queue → metadata.
//
// Note on applyHibernationHandoffToAssignment: that function lives in package
// gateway (unexported) and is exercised by its own unit tests in
// gateway/hibernation_handoff_test.go. The e2e test instead verifies that
// WakeHibernatedTask produces the exact metadata map shape (reserved keys +
// correct string values) that applyHibernationHandoffToAssignment expects,
// giving full integration coverage of the orchestration half of the contract.
func TestHibernationE2E_PauseWakeAssignsWithCheckpoint(t *testing.T) {
	store, cleanup := newWakerStore(t)
	defer cleanup()
	ctx := context.Background()

	// -------------------------------------------------------------------------
	// Step 1: Create an orchestrated task and bring it to running state.
	// -------------------------------------------------------------------------
	taskID := fmt.Sprintf("e2e-hib-%s", uuid.New().String())
	task := &tasks.Task{
		TaskID:               taskID,
		TaskType:             "agent_startup",
		Workspace:            "_test",
		AssignmentMode:       tasks.AssignmentModePool,
		TaskCategory:         tasks.TaskCategoryOrchestrated,
		TargetImplementation: "e2e-worker",
		LaunchParams: map[string]interface{}{
			"profile": "kubernetes",
		},
	}
	if err := store.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := store.AssignTask(ctx, taskID, "worker-e2e"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	if err := store.StartTask(ctx, taskID); err != nil {
		t.Fatalf("StartTask: %v", err)
	}

	got, err := store.GetTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetTask (after start): %v", err)
	}
	if got.Status != tasks.TaskStatusRunning {
		t.Fatalf("precondition: expected running, got %q", got.Status)
	}

	// -------------------------------------------------------------------------
	// Step 2: Pause to HIBERNATED via the orchestration service (bypassing the
	// gateway-side validateHibernationPrecondition which needs a CheckpointManager).
	// -------------------------------------------------------------------------
	const ckptKey = "ckpt-e2e-123"
	const resumeSessID = "sess-e2e-456"

	hib := &tasks.HibernationDescriptor{
		CheckpointKey:    ckptKey,
		ResumeSessionID:  resumeSessID,
		EscalationPolicy: "retry",
	}
	// Schedule wake in the past so the waker fires immediately.
	wakeAt := time.Now().Add(-1 * time.Second).UnixMilli()
	spec := &tasks.WaitSpec{
		Reason:              tasks.WaitReasonHibernation,
		Hibernation:         hib,
		ScheduledWakeUnixMs: wakeAt,
	}

	svc := newWakerService(store)
	if err := svc.PauseTask(ctx, taskID, tasks.TaskStatusHibernated, spec); err != nil {
		t.Fatalf("PauseTask -> HIBERNATED: %v", err)
	}

	// -------------------------------------------------------------------------
	// Step 3: Verify status is HIBERNATED and WaitSpec round-trips.
	// -------------------------------------------------------------------------
	got, err = store.GetTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetTask (after pause): %v", err)
	}
	if got.Status != tasks.TaskStatusHibernated {
		t.Errorf("after pause: status got %q want %q", got.Status, tasks.TaskStatusHibernated)
	}
	if got.WaitSpec == nil {
		t.Fatal("after pause: WaitSpec is nil, expected populated")
	}
	if got.WaitSpec.Reason != tasks.WaitReasonHibernation {
		t.Errorf("WaitSpec.Reason got %q want %q", got.WaitSpec.Reason, tasks.WaitReasonHibernation)
	}
	if got.WaitSpec.Hibernation == nil {
		t.Fatal("WaitSpec.Hibernation is nil after pause")
	}
	if got.WaitSpec.Hibernation.CheckpointKey != ckptKey {
		t.Errorf("WaitSpec.Hibernation.CheckpointKey = %q want %q", got.WaitSpec.Hibernation.CheckpointKey, ckptKey)
	}
	if got.WaitSpec.Hibernation.ResumeSessionID != resumeSessID {
		t.Errorf("WaitSpec.Hibernation.ResumeSessionID = %q want %q", got.WaitSpec.Hibernation.ResumeSessionID, resumeSessID)
	}

	// -------------------------------------------------------------------------
	// Step 4: Call WakeHibernatedTask — simulates task_waker scheduled-wake path.
	// -------------------------------------------------------------------------
	if err := svc.WakeHibernatedTask(ctx, taskID); err != nil {
		t.Fatalf("WakeHibernatedTask: %v", err)
	}

	// -------------------------------------------------------------------------
	// Step 5: Verify the task is now PENDING and WaitSpec is cleared.
	// -------------------------------------------------------------------------
	got, err = store.GetTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetTask (after wake): %v", err)
	}
	if got.Status != tasks.TaskStatusPending {
		t.Errorf("after wake: status got %q want %q", got.Status, tasks.TaskStatusPending)
	}
	if got.WaitSpec != nil {
		t.Errorf("WaitSpec should be cleared after wake; got %+v", got.WaitSpec)
	}

	// -------------------------------------------------------------------------
	// Step 6: Verify the reserved metadata keys are populated.
	//
	// These are the exact keys that applyHibernationHandoffToAssignment (in
	// package gateway) reads to populate TaskAssignment.{CheckpointKey,
	// ResumeSessionId}. Confirming them here closes the orchestration half of
	// the handoff contract.
	// -------------------------------------------------------------------------
	if v, _ := got.Metadata[tasks.MetadataKeyHibernationCheckpointKey].(string); v != ckptKey {
		t.Errorf("metadata[%q] = %q want %q",
			tasks.MetadataKeyHibernationCheckpointKey, v, ckptKey)
	}
	if v, _ := got.Metadata[tasks.MetadataKeyHibernationResumeSessionID].(string); v != resumeSessID {
		t.Errorf("metadata[%q] = %q want %q",
			tasks.MetadataKeyHibernationResumeSessionID, v, resumeSessID)
	}

	// -------------------------------------------------------------------------
	// Step 7: Verify a row exists in orchestrated_task_queue (orchestrator
	// pickup precondition — the orchestrator polls this table to spawn a fresh
	// worker).
	// -------------------------------------------------------------------------
	entries, err := store.PollPendingQueueEntries(ctx, 10)
	if err != nil {
		t.Fatalf("PollPendingQueueEntries: %v", err)
	}
	var foundEntry bool
	var entryProfile, entryImpl string
	for _, e := range entries {
		if e.TaskID == taskID {
			foundEntry = true
			entryProfile = e.Profile
			entryImpl = e.TargetImplementation
			break
		}
	}
	if !foundEntry {
		t.Fatalf("expected orchestrated_task_queue entry for %s; got %d entries total", taskID, len(entries))
	}
	if entryProfile != "kubernetes" {
		t.Errorf("queue entry profile = %q want %q", entryProfile, "kubernetes")
	}
	if entryImpl != "e2e-worker" {
		t.Errorf("queue entry impl = %q want %q", entryImpl, "e2e-worker")
	}

	// -------------------------------------------------------------------------
	// Step 8: Verify the metadata map shape satisfies the contract that
	// applyHibernationHandoffToAssignment depends on: both reserved keys must
	// be present as non-empty strings. The function itself is tested in
	// gateway/hibernation_handoff_test.go; here we confirm the orchestration
	// layer produces the expected input for it.
	// -------------------------------------------------------------------------
	ckptVal, ckptOK := got.Metadata[tasks.MetadataKeyHibernationCheckpointKey].(string)
	if !ckptOK || ckptVal == "" {
		t.Errorf("handoff contract: metadata[%q] must be a non-empty string; got %v (type %T)",
			tasks.MetadataKeyHibernationCheckpointKey,
			got.Metadata[tasks.MetadataKeyHibernationCheckpointKey],
			got.Metadata[tasks.MetadataKeyHibernationCheckpointKey])
	}
	sessVal, sessOK := got.Metadata[tasks.MetadataKeyHibernationResumeSessionID].(string)
	if !sessOK || sessVal == "" {
		t.Errorf("handoff contract: metadata[%q] must be a non-empty string; got %v (type %T)",
			tasks.MetadataKeyHibernationResumeSessionID,
			got.Metadata[tasks.MetadataKeyHibernationResumeSessionID],
			got.Metadata[tasks.MetadataKeyHibernationResumeSessionID])
	}
}
