// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/orchestration"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
)

func TestPoolRetryReachesWorker(t *testing.T) {
	for _, admin := range []bool{false, true} {
		for _, online := range []bool{false, true} {
			for _, terminal := range []tasks.TaskStatus{tasks.TaskStatusFailed, tasks.TaskStatusCancelled} {
				t.Run(fmt.Sprintf("admin=%v/online=%v/%s", admin, online, terminal), func(t *testing.T) {
					s, cleanup := newTaskTestServerWithSQLiteStore(t)
					defer cleanup()
					ctx := context.Background()
					creator := defaultAgentIdentity()
					task := &tasks.Task{TaskID: "retry-pool", TaskType: "document-review", Workspace: creator.Workspace,
						Status: terminal, AssignmentMode: tasks.AssignmentModePool, TargetImplementation: "customer-worker",
						ParentAgentID: creator.String(), Payload: []byte("original payload"), Metadata: map[string]interface{}{"concern": "review"}}
					if err := s.taskStore.CreateTask(ctx, task); err != nil {
						t.Fatal(err)
					}
					worker := models.Identity{Type: models.PrincipalService, Implementation: "customer-worker", Specifier: "first"}
					stream := &mockStream{}
					client := newTaskTestClient(stream, worker)
					s.implementationIndex = make(map[string][]*ClientSession)
					if online {
						s.addToImplIndex(worker, client)
					}
					if admin {
						provider := &GatewayStateProvider{gateway: s, taskStore: s.taskStore}
						if err := provider.RetryTask(ctx, task.TaskID); err != nil {
							t.Fatal(err)
						}
					} else {
						callerStream := &mockStream{}
						s.handleTaskOp(ctx, newTaskTestClient(callerStream, creator), &pb.TaskOperation{
							Op: pb.TaskOperation_RETRY, TaskId: task.TaskID, RequestId: "retry-request"})
						response := callerStream.sent[0].GetTaskOp()
						if response == nil || !response.Success || response.RequestId != "retry-request" {
							t.Fatalf("retry failed: %v", response)
						}
					}
					if !online {
						pending, err := s.taskStore.GetPendingPoolTasks(ctx, "customer-worker", "")
						if err != nil || len(pending) != 1 {
							t.Fatalf("retry invisible to reconnect: %v %v", pending, err)
						}
						s.orchestration = &OrchestrationServices{TaskService: orchestration.NewTaskAssignmentService(s.taskStore, nil, nil, nil, nil)}
						if err := s.deliverQueuedTasksToAgent(ctx, worker, client); err != nil {
							t.Fatal(err)
						}
					}
					if len(stream.sent) != 1 {
						t.Fatalf("want exactly one assignment, got %d", len(stream.sent))
					}
					assignment := stream.sent[0].GetTaskAssignment()
					if assignment == nil || assignment.TaskId != task.TaskID || assignment.Workspace != task.Workspace || string(assignment.Payload) != string(task.Payload) {
						t.Fatalf("assignment changed task scope or input: %v", assignment)
					}
					if err := s.retryTask(ctx, task.TaskID); err == nil {
						t.Fatal("second retry of assigned task succeeded")
					}
					if len(stream.sent) != 1 {
						t.Fatal("task delivered twice")
					}
				})
			}
		}
	}
}
