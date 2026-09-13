package gateway

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/orchestration"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
)

func TestQueuedTaskConsumerPreservesServiceOptOut(t *testing.T) {
	service := models.Identity{Type: models.PrincipalService}
	enabled := &pb.InitConnection{ClientType: &pb.InitConnection_Service{Service: &pb.ServiceIdentity{}}}
	disabled := &pb.InitConnection{ClientType: &pb.InitConnection_Service{Service: &pb.ServiceIdentity{NoPoolConsumer: true}}}
	if !consumesQueuedTasks(service, enabled) || consumesQueuedTasks(service, disabled) || consumesQueuedTasks(service, nil) {
		t.Fatal("service queue consumption must require registration without opt-out")
	}
	if !consumesQueuedTasks(models.Identity{Type: models.PrincipalAgent}, nil) || consumesQueuedTasks(models.Identity{Type: models.PrincipalUser}, enabled) {
		t.Fatal("agent eligibility and user exclusion changed")
	}
}

func TestConnectingServiceDrainsPendingPoolAcrossWorkspacesOnce(t *testing.T) {
	s, cleanup := newTaskTestServerWithSQLiteStore(t)
	defer cleanup()
	s.orchestration = &OrchestrationServices{TaskService: orchestration.NewTaskAssignmentService(s.taskStore, nil, nil, nil, nil)}
	ctx := context.Background()
	for _, workspace := range []string{"alpha", "beta"} {
		task := &tasks.Task{TaskID: "queued-" + workspace, TaskType: "document-review", Workspace: workspace,
			Status: tasks.TaskStatusPending, QueuedForStartup: true, AssignmentMode: tasks.AssignmentModePool, TargetImplementation: "customer-worker", Payload: []byte(workspace)}
		if err := s.taskStore.CreateTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}
	identity := models.Identity{Type: models.PrincipalService, Implementation: "customer-worker", Specifier: "first"}
	stream := &mockStream{}
	client := newTaskTestClient(stream, identity)
	if err := s.deliverQueuedTasksToAgent(ctx, identity, client); err != nil {
		t.Fatal(err)
	}
	if len(stream.sent) != 2 {
		t.Fatalf("got %d assignments, want both queued workspaces", len(stream.sent))
	}
	for _, message := range stream.sent {
		assignment := message.GetTaskAssignment()
		if assignment == nil || assignment.Workspace == "" || string(assignment.Payload) != assignment.Workspace {
			t.Fatalf("assignment lost its workspace: %v", assignment)
		}
		stored, err := s.taskStore.GetTask(ctx, assignment.TaskId)
		if err != nil || stored.AssignedTo != identity.String() {
			t.Fatalf("claim not persisted: %v %v", stored, err)
		}
	}
	other := models.Identity{Type: models.PrincipalService, Implementation: "customer-worker", Specifier: "second"}
	second := &mockStream{}
	if err := s.deliverQueuedTasksToAgent(ctx, other, newTaskTestClient(second, other)); err != nil {
		t.Fatal(err)
	}
	if len(second.sent) != 0 {
		t.Fatal("already claimed work delivered again")
	}
}
