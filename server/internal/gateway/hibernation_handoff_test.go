// Phase 3 Stage B: hibernation handoff plumbing through the orchestration
// delivery surface. The waker populates Task.Metadata with reserved keys
// (_hibernation_checkpoint_key / _hibernation_resume_session_id) on the wake
// path; DeliverQueuedTasks / DeliverPoolTasks / deliverPoolTaskToWorker copy
// those keys onto pb.TaskAssignment.{CheckpointKey, ResumeSessionId} via
// applyHibernationHandoffToAssignment so the rehydrating worker can LOAD the
// prior checkpoint without an extra RPC.
package gateway

import (
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/tasks"
)

// TestApplyHibernationHandoffToAssignment_Populates verifies that the helper
// reads the reserved metadata keys and writes them onto the assignment's
// dedicated CheckpointKey / ResumeSessionId fields. This is the contract the
// fresh worker depends on to resume a hibernated task.
func TestApplyHibernationHandoffToAssignment_Populates(t *testing.T) {
	assignment := &pb.TaskAssignment{}
	metadata := map[string]interface{}{
		tasks.MetadataKeyHibernationCheckpointKey:   "ckpt-abc",
		tasks.MetadataKeyHibernationResumeSessionID: "sess-xyz",
		"other_key": "preserved",
	}
	applyHibernationHandoffToAssignment(assignment, metadata)

	if assignment.CheckpointKey != "ckpt-abc" {
		t.Errorf("CheckpointKey = %q, want %q", assignment.CheckpointKey, "ckpt-abc")
	}
	if assignment.ResumeSessionId != "sess-xyz" {
		t.Errorf("ResumeSessionId = %q, want %q", assignment.ResumeSessionId, "sess-xyz")
	}
}

// TestApplyHibernationHandoffToAssignment_NoOpWhenAbsent verifies that fresh
// (non-wake) task assignments — i.e. tasks whose metadata never contained the
// reserved hibernation keys — are not mutated. This keeps the helper safe to
// call unconditionally from the delivery sites.
func TestApplyHibernationHandoffToAssignment_NoOpWhenAbsent(t *testing.T) {
	assignment := &pb.TaskAssignment{
		TaskId:   "task-fresh",
		TaskType: "regular",
	}
	metadata := map[string]interface{}{
		"some_other_key": "value",
	}
	applyHibernationHandoffToAssignment(assignment, metadata)

	if assignment.CheckpointKey != "" {
		t.Errorf("expected empty CheckpointKey on non-wake assignment, got %q", assignment.CheckpointKey)
	}
	if assignment.ResumeSessionId != "" {
		t.Errorf("expected empty ResumeSessionId on non-wake assignment, got %q", assignment.ResumeSessionId)
	}
}

// TestApplyHibernationHandoffToAssignment_NilMetadata: defensive — nil
// metadata is the freshly-created-task default, the helper must not panic.
func TestApplyHibernationHandoffToAssignment_NilMetadata(t *testing.T) {
	assignment := &pb.TaskAssignment{}
	applyHibernationHandoffToAssignment(assignment, nil)
	if assignment.CheckpointKey != "" || assignment.ResumeSessionId != "" {
		t.Errorf("expected empty fields for nil metadata, got CheckpointKey=%q ResumeSessionId=%q",
			assignment.CheckpointKey, assignment.ResumeSessionId)
	}
}

// TestApplyHibernationHandoffToAssignment_NilAssignment: defensive — a nil
// assignment must not panic. The helper is called unconditionally at the
// delivery sites; a defensive guard avoids a nil-deref hazard if a future
// caller passes a freshly-zeroed pointer.
func TestApplyHibernationHandoffToAssignment_NilAssignment(t *testing.T) {
	metadata := map[string]interface{}{
		tasks.MetadataKeyHibernationCheckpointKey: "ckpt-1",
	}
	// Must not panic.
	applyHibernationHandoffToAssignment(nil, metadata)
}

// TestApplyHibernationHandoffToAssignment_EmptyStringsSkipped: a metadata key
// present but with an empty-string value is treated as "no handoff". Mirrors
// the WakeHibernatedTask behavior (only non-empty descriptor fields are
// copied into metadata in the first place).
func TestApplyHibernationHandoffToAssignment_EmptyStringsSkipped(t *testing.T) {
	assignment := &pb.TaskAssignment{}
	metadata := map[string]interface{}{
		tasks.MetadataKeyHibernationCheckpointKey:   "",
		tasks.MetadataKeyHibernationResumeSessionID: "",
	}
	applyHibernationHandoffToAssignment(assignment, metadata)
	if assignment.CheckpointKey != "" || assignment.ResumeSessionId != "" {
		t.Errorf("empty-string metadata should not populate assignment fields, got CheckpointKey=%q ResumeSessionId=%q",
			assignment.CheckpointKey, assignment.ResumeSessionId)
	}
}

// TestApplyHibernationHandoffToAssignment_NonStringValuesIgnored: the
// metadata map is map[string]interface{}; a non-string value under a
// reserved key (e.g. accidental int) must not panic and must not populate
// the assignment.
func TestApplyHibernationHandoffToAssignment_NonStringValuesIgnored(t *testing.T) {
	assignment := &pb.TaskAssignment{}
	metadata := map[string]interface{}{
		tasks.MetadataKeyHibernationCheckpointKey:   42,
		tasks.MetadataKeyHibernationResumeSessionID: true,
	}
	applyHibernationHandoffToAssignment(assignment, metadata)
	if assignment.CheckpointKey != "" || assignment.ResumeSessionId != "" {
		t.Errorf("non-string metadata should be ignored, got CheckpointKey=%q ResumeSessionId=%q",
			assignment.CheckpointKey, assignment.ResumeSessionId)
	}
}
