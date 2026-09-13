// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"errors"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
	"google.golang.org/protobuf/proto"
)

func TestRouteTaskInputPreservesSubjectAndExactAssignee(t *testing.T) {
	router := newMockMessageRouter()
	server := newRoutingTestServer(router)
	server.taskStore = &fakeGetTaskStore{task: taskInputFixture()}
	stream := &mockStream{}
	sender := models.Identity{Type: models.PrincipalUser, ID: "alice", Workspace: "project", Specifier: "window"}
	payload := []byte{0, 255, 10, 42}
	server.routeMessage(context.Background(), newRoutingTestClient(sender, stream), &pb.SendMessage{
		TargetTopic: "tk::project::task-1::input", MessageType: pb.MessageType_OPAQUE, Payload: payload,
	})
	if stream.sentCount() != 0 || len(router.publishedMessages) != 1 {
		t.Fatalf("expected one authorized publish and no error, got %d publishes, %d errors", len(router.publishedMessages), stream.sentCount())
	}
	published := router.publishedMessages[0]
	if published.topic != "ag::_sandbox::worker::one" {
		t.Fatalf("wrong recipient %s", published.topic)
	}
	var envelope pb.MessageEnvelope
	if err := proto.Unmarshal(published.payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Source != sender.ToTopic() || envelope.Workspace != "project" ||
		string(envelope.Payload) != string(payload) || envelope.OnBehalfSubject != nil {
		t.Fatalf("task input changed subject, workspace or payload")
	}
}

func taskInputFixture() *tasks.Task {
	return &tasks.Task{
		TaskID: "task-1", Workspace: "project", Status: tasks.TaskStatusRunning,
		AssignedTo: "ag::_sandbox::worker::one",
		Authority:  tasks.TaskAuthorityInfo{SubjectType: string(models.PrincipalUser), SubjectID: "alice"},
		Metadata:   map[string]interface{}{"subject_participant": true, "turn_tool_host_id": "us::alice::window"},
	}
}

func TestTaskInputRefusesOtherSubjectsWindowsAndStaleTasks(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*tasks.Task, *models.Identity, *pb.SendMessage, *fakeGetTaskStore)
	}{
		{"other subject", func(_ *tasks.Task, s *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) { s.ID = "bob" }},
		{"other window", func(_ *tasks.Task, s *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) { s.Specifier = "other" }},
		{"other workspace", func(_ *tasks.Task, s *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) { s.Workspace = "other" }},
		{"service", func(_ *tasks.Task, s *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) {
			s.Type = models.PrincipalService
		}},
		{"wrong task id", func(t *tasks.Task, _ *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) { t.TaskID = "other" }},
		{"wrong stored workspace", func(t *tasks.Task, _ *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) { t.Workspace = "other" }},
		{"completed", func(t *tasks.Task, _ *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) {
			t.Status = tasks.TaskStatusCompleted
		}},
		{"no participant", func(t *tasks.Task, _ *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) {
			delete(t.Metadata, "subject_participant")
		}},
		{"wildcard worker", func(t *tasks.Task, _ *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) {
			t.AssignedTo = "ag::_sandbox::worker::*"
		}},
		{"service worker", func(t *tasks.Task, _ *models.Identity, _ *pb.SendMessage, _ *fakeGetTaskStore) {
			t.AssignedTo = "sv::worker::one"
		}},
		{"wrong app workspace", func(_ *tasks.Task, _ *models.Identity, m *pb.SendMessage, _ *fakeGetTaskStore) {
			m.AppWorkspace = "other"
		}},
		{"delegation", func(_ *tasks.Task, _ *models.Identity, m *pb.SendMessage, _ *fakeGetTaskStore) {
			m.Authorization = &pb.AuthorizationContext{AuthorityMode: "on_behalf_of"}
		}},
		{"checked access", func(_ *tasks.Task, _ *models.Identity, m *pb.SendMessage, _ *fakeGetTaskStore) {
			m.CheckedAccess = &pb.ResourceAccessRequest{}
		}},
		{"continuation", func(_ *tasks.Task, _ *models.Identity, m *pb.SendMessage, _ *fakeGetTaskStore) {
			m.AuthorityContinuation = &pb.AuthorityContinuationRequest{}
		}},
		{"chat type", func(_ *tasks.Task, _ *models.Identity, m *pb.SendMessage, _ *fakeGetTaskStore) {
			m.MessageType = pb.MessageType_CHAT
		}},
		{"wildcard task", func(_ *tasks.Task, _ *models.Identity, m *pb.SendMessage, _ *fakeGetTaskStore) {
			m.TargetTopic = "tk::project::*::input"
		}},
		{"extra segment", func(_ *tasks.Task, _ *models.Identity, m *pb.SendMessage, _ *fakeGetTaskStore) {
			m.TargetTopic += "::other"
		}},
		{"store error", func(_ *tasks.Task, _ *models.Identity, _ *pb.SendMessage, s *fakeGetTaskStore) {
			s.err = errors.New("unavailable")
		}},
		{"missing task", func(_ *tasks.Task, _ *models.Identity, _ *pb.SendMessage, s *fakeGetTaskStore) { s.task = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := taskInputFixture()
			store := &fakeGetTaskStore{task: task}
			sender := models.Identity{Type: models.PrincipalUser, ID: "alice", Workspace: "project", Specifier: "window"}
			msg := &pb.SendMessage{TargetTopic: "tk::project::task-1::input", MessageType: pb.MessageType_OPAQUE}
			tc.mutate(task, &sender, msg, store)
			server := &GatewayServer{taskStore: store}
			target, matched, err := server.resolveTaskInput(context.Background(), sender, msg)
			if !matched || err == nil || target != "" {
				t.Fatal("invalid task input was not refused")
			}
		})
	}
}
