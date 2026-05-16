package gateway

// Phase 3 Stage A: HIBERNATE precondition coverage.
//
// validateHibernationPrecondition enforces that a task transitioning to
// HIBERNATED must already have a checkpoint saved under the descriptor's key
// (scoped to the task's assignee identity). These tests exercise the unit
// directly with a stubbed CheckpointManager rather than going end-to-end
// through routing.go — Stage C will add E2E coverage when the SDK lands.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

func TestValidateHibernationPrecondition_RejectsNilHibernation(t *testing.T) {
	s := &GatewayServer{checkpoints: newMockCheckpointManager()}
	task := &tasks.Task{TaskID: "t-1", AssignedTo: "ag::ws1::worker::v1"}
	spec := &tasks.WaitSpec{Reason: tasks.WaitReasonHibernation}

	err := s.validateHibernationPrecondition(context.Background(), task, spec)
	if err == nil {
		t.Fatal("expected error for nil Hibernation descriptor, got nil")
	}
	if !strings.Contains(err.Error(), "checkpoint_key") {
		t.Errorf("expected error to mention checkpoint_key, got %q", err.Error())
	}
}

func TestValidateHibernationPrecondition_RejectsEmptyCheckpointKey(t *testing.T) {
	s := &GatewayServer{checkpoints: newMockCheckpointManager()}
	task := &tasks.Task{TaskID: "t-1", AssignedTo: "ag::ws1::worker::v1"}
	spec := &tasks.WaitSpec{
		Reason:      tasks.WaitReasonHibernation,
		Hibernation: &tasks.HibernationDescriptor{CheckpointKey: ""},
	}

	err := s.validateHibernationPrecondition(context.Background(), task, spec)
	if err == nil {
		t.Fatal("expected error for empty CheckpointKey, got nil")
	}
}

func TestValidateHibernationPrecondition_RejectsMissingCheckpoint(t *testing.T) {
	cp := newMockCheckpointManager()
	s := &GatewayServer{checkpoints: cp}
	task := &tasks.Task{TaskID: "t-1", AssignedTo: "ag::ws1::worker::v1"}
	spec := &tasks.WaitSpec{
		Reason:      tasks.WaitReasonHibernation,
		Hibernation: &tasks.HibernationDescriptor{CheckpointKey: "missing-key"},
	}

	err := s.validateHibernationPrecondition(context.Background(), task, spec)
	if err == nil {
		t.Fatal("expected error when checkpoint not present, got nil")
	}
	if !strings.Contains(err.Error(), "missing-key") {
		t.Errorf("expected error to mention the missing key, got %q", err.Error())
	}
}

func TestValidateHibernationPrecondition_AcceptsExistingCheckpoint(t *testing.T) {
	cp := newMockCheckpointManager()
	// Seed a checkpoint under the assignee identity + key.
	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "worker",
		Specifier:      "v1",
	}
	if err := cp.Save(context.Background(), identity, "ck-1", []byte("state"), time.Hour); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s := &GatewayServer{checkpoints: cp}
	task := &tasks.Task{TaskID: "t-1", AssignedTo: identity.String()}
	spec := &tasks.WaitSpec{
		Reason:      tasks.WaitReasonHibernation,
		Hibernation: &tasks.HibernationDescriptor{CheckpointKey: "ck-1"},
	}

	err := s.validateHibernationPrecondition(context.Background(), task, spec)
	if err != nil {
		t.Errorf("expected nil error with seeded checkpoint, got %v", err)
	}
}

func TestValidateHibernationPrecondition_NilCheckpointManager_Permissive(t *testing.T) {
	// Documented behavior: when no checkpoint manager is wired, the precondition
	// degrades gracefully rather than blocking hibernation. Stage B will add
	// E2E coverage; Stage A only needs to ensure the data model flows.
	s := &GatewayServer{checkpoints: nil}
	task := &tasks.Task{TaskID: "t-1", AssignedTo: "ag::ws1::worker::v1"}
	spec := &tasks.WaitSpec{
		Reason:      tasks.WaitReasonHibernation,
		Hibernation: &tasks.HibernationDescriptor{CheckpointKey: "ck-1"},
	}

	if err := s.validateHibernationPrecondition(context.Background(), task, spec); err != nil {
		t.Errorf("expected nil error with nil checkpoint manager, got %v", err)
	}
}

func TestValidateHibernationPrecondition_RejectsUnassignedTask(t *testing.T) {
	// A task with no assignee has no identity to scope the checkpoint to.
	// Reject explicitly rather than silently accept.
	s := &GatewayServer{checkpoints: newMockCheckpointManager()}
	task := &tasks.Task{TaskID: "t-1", AssignedTo: ""}
	spec := &tasks.WaitSpec{
		Reason:      tasks.WaitReasonHibernation,
		Hibernation: &tasks.HibernationDescriptor{CheckpointKey: "ck-1"},
	}

	err := s.validateHibernationPrecondition(context.Background(), task, spec)
	if err == nil {
		t.Fatal("expected error for unassigned task, got nil")
	}
	if !strings.Contains(err.Error(), "assignee") {
		t.Errorf("expected error to mention assignee, got %q", err.Error())
	}
}
