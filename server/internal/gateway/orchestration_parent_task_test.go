package gateway

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
)

func createParentTask(t *testing.T, s *GatewayServer, parent *tasks.Task) {
	t.Helper()
	status := parent.Status
	assignee := parent.AssignedTo
	parent.Status = tasks.TaskStatusPending
	parent.AssignedTo = ""
	if err := s.taskStore.CreateTask(context.Background(), parent); err != nil {
		t.Fatalf("CreateTask(parent): %v", err)
	}
	if assignee != "" {
		if err := s.taskStore.AssignTask(context.Background(), parent.TaskID, assignee); err != nil {
			t.Fatalf("AssignTask(parent): %v", err)
		}
	}
	if status == tasks.TaskStatusRunning || tasks.IsTerminal(status) {
		if err := s.taskStore.StartTask(context.Background(), parent.TaskID); err != nil {
			t.Fatalf("StartTask(parent): %v", err)
		}
	}
	if status == tasks.TaskStatusCompleted {
		if err := s.taskStore.CompleteTask(context.Background(), parent.TaskID); err != nil {
			t.Fatalf("CompleteTask(parent): %v", err)
		}
	}
}

func createTaskResponse(stream *mockStream) *pb.CreateTaskResponse {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	for _, message := range stream.sent {
		if response := message.GetCreateTask(); response != nil {
			return response
		}
	}
	return nil
}

func TestHandleCreateTaskExplicitParentUsesValidatedAssignee(t *testing.T) {
	s, _, cleanup := newIdemTestServer(t)
	defer cleanup()

	worker := callerIdentity("worker", "alice")
	createParentTask(t, s, &tasks.Task{
		TaskID:         "parent-1",
		TaskType:       "chat_message",
		Workspace:      "ws1",
		Status:         tasks.TaskStatusRunning,
		AssignmentMode: tasks.AssignmentModeTargeted,
		AssignedTo:     worker.String(),
		CorrelationID:  "conversation-1",
		RootTaskID:     "root-1",
	})

	stream := &mockStream{}
	client := newTaskTestClient(stream, worker)
	// A long-lived worker may retain a different startup-task association.
	// Explicit parentage is request-scoped and must still select parent-1.
	client.AssociatedTaskID = "startup-task"
	request := &pb.CreateTaskRequest{
		TaskType:       "child",
		Workspace:      "ws1",
		AssignmentMode: pb.TaskAssignmentMode_SELF_ASSIGN,
		ParentTaskId:   "parent-1",
		RequestId:      "create-child",
	}
	if err := s.handleCreateTask(context.Background(), client, worker, request); err != nil {
		t.Fatalf("handleCreateTask: %v", err)
	}
	response := createTaskResponse(stream)
	if response == nil || !response.Success || response.TaskId == "" {
		t.Fatalf("CreateTaskResponse = %+v", response)
	}
	child, err := s.taskStore.GetTask(context.Background(), response.TaskId)
	if err != nil {
		t.Fatalf("GetTask(child): %v", err)
	}
	if child.ParentTaskID != "parent-1" {
		t.Fatalf("ParentTaskID = %q", child.ParentTaskID)
	}
	if child.RootTaskID != "root-1" || child.CorrelationID != "conversation-1" {
		t.Fatalf("inherited coordination = root:%q correlation:%q", child.RootTaskID, child.CorrelationID)
	}
}

func TestHandleCreateTaskExplicitParentFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		caller    models.Identity
		parent    *tasks.Task
		workspace string
		parentID  string
	}{
		{
			name:   "missing",
			caller: callerIdentity("worker", "alice"),
			parent: nil, workspace: "ws1", parentID: "missing-parent",
		},
		{
			name:      "different assignee",
			caller:    callerIdentity("worker", "alice"),
			parent:    &tasks.Task{TaskID: "parent-other", TaskType: "chat", Workspace: "ws1", Status: tasks.TaskStatusRunning, AssignmentMode: tasks.AssignmentModeTargeted, AssignedTo: callerIdentity("worker", "bob").String()},
			workspace: "ws1", parentID: "parent-other",
		},
		{
			name:      "terminal",
			caller:    callerIdentity("worker", "alice"),
			parent:    &tasks.Task{TaskID: "parent-done", TaskType: "chat", Workspace: "ws1", Status: tasks.TaskStatusCompleted, AssignmentMode: tasks.AssignmentModeTargeted, AssignedTo: callerIdentity("worker", "alice").String()},
			workspace: "ws1", parentID: "parent-done",
		},
		{
			name:      "cross workspace",
			caller:    callerIdentity("worker", "alice"),
			parent:    &tasks.Task{TaskID: "parent-ws2", TaskType: "chat", Workspace: "ws2", Status: tasks.TaskStatusRunning, AssignmentMode: tasks.AssignmentModeTargeted, AssignedTo: callerIdentity("worker", "alice").String()},
			workspace: "ws1", parentID: "parent-ws2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, _, cleanup := newIdemTestServer(t)
			defer cleanup()
			if test.parent != nil {
				createParentTask(t, s, test.parent)
			}
			stream := &mockStream{}
			client := newTaskTestClient(stream, test.caller)
			request := &pb.CreateTaskRequest{
				TaskType:       "child",
				Workspace:      test.workspace,
				AssignmentMode: pb.TaskAssignmentMode_SELF_ASSIGN,
				ParentTaskId:   test.parentID,
				RequestId:      "denied-child",
			}
			if err := s.handleCreateTask(context.Background(), client, test.caller, request); err != nil {
				t.Fatalf("handleCreateTask: %v", err)
			}
			response := createTaskResponse(stream)
			if response == nil || response.Success || response.ErrorCode != "ERR_PERMISSION_DENIED" || response.ErrorMessage != createTaskParentDenied {
				t.Fatalf("CreateTaskResponse = %+v", response)
			}
			wantTasks := 0
			if test.parent != nil && test.parent.Workspace == test.workspace {
				wantTasks = 1
			}
			if got := countWorkspaceTasks(t, s, test.workspace); got != wantTasks {
				t.Fatalf("workspace tasks after denied create = %d, want %d", got, wantTasks)
			}
		})
	}
}
