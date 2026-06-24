package gateway

import (
	"context"
	"testing"

	"github.com/scitrera/aether/internal/acl"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// fakeGetTaskStore is a minimal taskstore.Store whose only useful method is
// GetTask. It embeds the interface so the remaining methods exist for type
// satisfaction but panic if a test path unexpectedly calls them — keeping the
// fake honest without implementing the whole large interface.
type fakeGetTaskStore struct {
	taskstore.Store
	task *tasks.Task
	err  error
}

func (f *fakeGetTaskStore) GetTask(_ context.Context, _ string) (*tasks.Task, error) {
	return f.task, f.err
}

// TestParseTaskMsgTopic exercises the pure topic-shape parser: only the exact
// tk::{ws}::{task}::msg form is a task-msg topic; the events lane and plain
// workspace topics are not.
func TestParseTaskMsgTopic(t *testing.T) {
	cases := []struct {
		name      string
		topic     string
		wantWS    string
		wantTask  string
		wantIsMsg bool
	}{
		{"msg lane", "tk::ws1::task-abc::msg", "ws1", "task-abc", true},
		{"events lane not matched", "tk::ws1::task-abc::events", "", "", false},
		{"plain workspace topic", "ag::ws1::impl::spec", "", "", false},
		{"too few segments", "tk::ws1::task-abc", "", "", false},
		{"too many segments", "tk::ws1::task-abc::msg::extra", "", "", false},
		{"wrong prefix", "xx::ws1::task-abc::msg", "", "", false},
		{"empty workspace", "tk::::task-abc::msg", "", "", false},
		{"empty task id", "tk::ws1::::msg", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws, task, isMsg := parseTaskMsgTopic(tc.topic)
			if ws != tc.wantWS || task != tc.wantTask || isMsg != tc.wantIsMsg {
				t.Fatalf("parseTaskMsgTopic(%q) = (%q,%q,%v), want (%q,%q,%v)",
					tc.topic, ws, task, isMsg, tc.wantWS, tc.wantTask, tc.wantIsMsg)
			}
		})
	}
}

// TestTaskPartyAuthorized mirrors authorizeTaskOp's party predicate: assignee,
// creator, OBO subject — plus the workspace tenancy guard.
func TestTaskPartyAuthorized(t *testing.T) {
	svc := models.Identity{Type: models.PrincipalService, Implementation: "metrics", Specifier: "bridge"}
	svcTopic := svc.ToTopic()

	cases := []struct {
		name   string
		sender models.Identity
		task   *tasks.Task
		want   bool
	}{
		{
			name:   "service assignee match",
			sender: svc,
			task:   &tasks.Task{Workspace: "default", AssignedTo: svcTopic},
			want:   true,
		},
		{
			name:   "service creator match",
			sender: svc,
			task:   &tasks.Task{Workspace: "default", ParentAgentID: svcTopic},
			want:   true,
		},
		{
			name:   "obo subject match",
			sender: models.Identity{Type: models.PrincipalUser, ID: "alice", Specifier: "win-7"},
			task: &tasks.Task{
				Workspace: "default",
				Authority: tasks.TaskAuthorityInfo{
					SubjectType: string(models.PrincipalUser),
					SubjectID:   "alice",
				},
			},
			want: true,
		},
		{
			name:   "non-party service",
			sender: models.Identity{Type: models.PrincipalService, Implementation: "other", Specifier: "x"},
			task:   &tasks.Task{Workspace: "default", AssignedTo: svcTopic},
			want:   false,
		},
		{
			name:   "tenancy guard: assignee in different workspace",
			sender: models.Identity{Type: models.PrincipalAgent, Workspace: "wsA", Implementation: "w", Specifier: "1"},
			task: &tasks.Task{
				Workspace: "wsB",
				AssignedTo: models.Identity{
					Type: models.PrincipalAgent, Workspace: "wsA", Implementation: "w", Specifier: "1",
				}.ToTopic(),
			},
			want: false,
		},
		{
			name:   "nil task",
			sender: svc,
			task:   nil,
			want:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := taskPartyAuthorized(tc.sender, tc.task); got != tc.want {
				t.Fatalf("taskPartyAuthorized() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTaskMsgSenderIsTaskParty exercises the store-backed helper end to end.
func TestTaskMsgSenderIsTaskParty(t *testing.T) {
	ctx := context.Background()
	svc := models.Identity{Type: models.PrincipalService, Implementation: "metrics", Specifier: "bridge"}
	svcTopic := svc.ToTopic()
	msgTopic := "tk::default::task-1::msg"

	t.Run("service assignee authorized to task msg lane", func(t *testing.T) {
		gw := &GatewayServer{taskStore: &fakeGetTaskStore{
			task: &tasks.Task{Workspace: "default", AssignedTo: svcTopic},
		}}
		ok, isMsg := gw.taskMsgSenderIsTaskParty(ctx, svc, msgTopic)
		if !isMsg || !ok {
			t.Fatalf("got (authorized=%v, isTaskMsg=%v); want (true, true)", ok, isMsg)
		}
	})

	t.Run("non-party service not authorized via this path", func(t *testing.T) {
		other := models.Identity{Type: models.PrincipalService, Implementation: "other", Specifier: "x"}
		gw := &GatewayServer{taskStore: &fakeGetTaskStore{
			task: &tasks.Task{Workspace: "default", AssignedTo: svcTopic},
		}}
		ok, isMsg := gw.taskMsgSenderIsTaskParty(ctx, other, msgTopic)
		if !isMsg || ok {
			t.Fatalf("got (authorized=%v, isTaskMsg=%v); want (false, true)", ok, isMsg)
		}
	})

	t.Run("cross-workspace assignee blocked by tenancy guard", func(t *testing.T) {
		agentWsA := models.Identity{Type: models.PrincipalAgent, Workspace: "wsA", Implementation: "w", Specifier: "1"}
		gw := &GatewayServer{taskStore: &fakeGetTaskStore{
			task: &tasks.Task{Workspace: "wsB", AssignedTo: agentWsA.ToTopic()},
		}}
		ok, isMsg := gw.taskMsgSenderIsTaskParty(ctx, agentWsA, "tk::wsB::task-1::msg")
		if !isMsg || ok {
			t.Fatalf("got (authorized=%v, isTaskMsg=%v); want (false, true)", ok, isMsg)
		}
	})

	t.Run("non-task topic no-ops", func(t *testing.T) {
		gw := &GatewayServer{taskStore: &fakeGetTaskStore{
			task: &tasks.Task{Workspace: "default", AssignedTo: svcTopic},
		}}
		ok, isMsg := gw.taskMsgSenderIsTaskParty(ctx, svc, "ag::default::impl::spec")
		if isMsg || ok {
			t.Fatalf("got (authorized=%v, isTaskMsg=%v); want (false, false)", ok, isMsg)
		}
	})

	t.Run("events lane no-ops", func(t *testing.T) {
		gw := &GatewayServer{taskStore: &fakeGetTaskStore{
			task: &tasks.Task{Workspace: "default", AssignedTo: svcTopic},
		}}
		ok, isMsg := gw.taskMsgSenderIsTaskParty(ctx, svc, "tk::default::task-1::events")
		if isMsg || ok {
			t.Fatalf("got (authorized=%v, isTaskMsg=%v); want (false, false)", ok, isMsg)
		}
	})

	t.Run("nil task store fails closed but flags task-msg shape", func(t *testing.T) {
		gw := &GatewayServer{}
		ok, isMsg := gw.taskMsgSenderIsTaskParty(ctx, svc, msgTopic)
		if !isMsg || ok {
			t.Fatalf("got (authorized=%v, isTaskMsg=%v); want (false, true)", ok, isMsg)
		}
	})
}

// TestCheckMessageSendTaskPartyGrant confirms the early-allow path in
// checkMessageSend returns AccessReadWrite for a task party (the service
// assignee that the plain workspace fallback would otherwise deny).
func TestCheckMessageSendTaskPartyGrant(t *testing.T) {
	ctx := context.Background()
	svc := models.Identity{Type: models.PrincipalService, Implementation: "metrics", Specifier: "bridge"}

	gw := &GatewayServer{
		acl: &acl.Service{}, // non-nil so the early ACL guard is passed
		taskStore: &fakeGetTaskStore{
			task: &tasks.Task{Workspace: "default", AssignedTo: svc.ToTopic()},
		},
	}
	level, err := gw.checkMessageSend(ctx, svc, "tk::default::task-1::msg")
	if err != nil {
		t.Fatalf("checkMessageSend() error = %v; want nil", err)
	}
	if level != acl.AccessReadWrite {
		t.Fatalf("checkMessageSend() level = %d; want %d (AccessReadWrite)", level, acl.AccessReadWrite)
	}
}
