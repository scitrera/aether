package gateway

import (
	"testing"

	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// TestTaskToProto_AuthorityFields verifies that the first-class authority
// lineage fields on tasks.Task are mapped into the proto TaskInfo by
// taskToProto. Guards against silent regressions in the Track B surfacing.
func TestTaskToProto_AuthorityFields(t *testing.T) {
	task := &tasks.Task{
		TaskID:        "task-xyz",
		TaskType:      "worker",
		Status:        tasks.TaskStatusRunning,
		Workspace:     "ws",
		ParentAgentID: "ag::ws::creator::inst-1",
		ParentTaskID:  "parent-task-1",
		Authority: tasks.TaskAuthorityInfo{
			Mode:                   "on_behalf_of",
			SubjectType:            "user",
			SubjectID:              "alice",
			RootSubjectType:        "user",
			RootSubjectID:          "alice",
			AuthorityGrantID:       "grant-current",
			RootAuthorityGrantID:   "grant-root",
			ParentAuthorityGrantID: "grant-parent",
		},
	}

	info := taskToProto(task)

	if info.AuthorityMode != "on_behalf_of" {
		t.Errorf("AuthorityMode = %q, want %q", info.AuthorityMode, "on_behalf_of")
	}
	if info.SubjectType != "user" {
		t.Errorf("SubjectType = %q, want %q", info.SubjectType, "user")
	}
	if info.SubjectId != "alice" {
		t.Errorf("SubjectId = %q, want %q", info.SubjectId, "alice")
	}
	if info.RootSubjectType != "user" {
		t.Errorf("RootSubjectType = %q, want %q", info.RootSubjectType, "user")
	}
	if info.RootSubjectId != "alice" {
		t.Errorf("RootSubjectId = %q, want %q", info.RootSubjectId, "alice")
	}
	if info.AuthorityGrantId != "grant-current" {
		t.Errorf("AuthorityGrantId = %q, want %q", info.AuthorityGrantId, "grant-current")
	}
	if info.RootAuthorityGrantId != "grant-root" {
		t.Errorf("RootAuthorityGrantId = %q, want %q", info.RootAuthorityGrantId, "grant-root")
	}
	if info.ParentAuthorityGrantId != "grant-parent" {
		t.Errorf("ParentAuthorityGrantId = %q, want %q", info.ParentAuthorityGrantId, "grant-parent")
	}
	if info.CreatorActorId != "ag::ws::creator::inst-1" {
		t.Errorf("CreatorActorId = %q, want %q", info.CreatorActorId, "ag::ws::creator::inst-1")
	}
	if info.ParentTaskId != "parent-task-1" {
		t.Errorf("ParentTaskId = %q, want %q", info.ParentTaskId, "parent-task-1")
	}
}

// TestTaskToProto_DirectTask verifies that a task without on-behalf-of context
// maps to empty authority fields rather than stale/garbage values.
func TestTaskToProto_DirectTask(t *testing.T) {
	task := &tasks.Task{
		TaskID:    "task-direct",
		TaskType:  "worker",
		Status:    tasks.TaskStatusRunning,
		Workspace: "ws",
	}

	info := taskToProto(task)
	if info.AuthorityMode != "" || info.SubjectId != "" || info.AuthorityGrantId != "" || info.ParentTaskId != "" {
		t.Errorf("direct task should have empty authority fields, got mode=%q subject=%q grant=%q parent_task=%q",
			info.AuthorityMode, info.SubjectId, info.AuthorityGrantId, info.ParentTaskId)
	}
}

// TestTaskToAdminInfo_AuthorityFields verifies the admin REST TaskInfo mapper
// surfaces the same authority lineage fields.
func TestTaskToAdminInfo_AuthorityFields(t *testing.T) {
	task := &tasks.Task{
		TaskID:        "task-admin",
		TaskType:      "worker",
		Status:        tasks.TaskStatusRunning,
		Workspace:     "ws",
		ParentAgentID: "ag::ws::creator::inst-1",
		ParentTaskID:  "parent-task-1",
		Authority: tasks.TaskAuthorityInfo{
			Mode:                   "on_behalf_of",
			SubjectType:            "user",
			SubjectID:              "alice",
			RootSubjectID:          "alice",
			AuthorityGrantID:       "grant-current",
			RootAuthorityGrantID:   "grant-root",
			ParentAuthorityGrantID: "grant-parent",
		},
	}

	info := taskToAdminInfo(task)
	if info.AuthorityMode != "on_behalf_of" {
		t.Errorf("AuthorityMode = %q, want on_behalf_of", info.AuthorityMode)
	}
	if info.SubjectID != "alice" {
		t.Errorf("SubjectID = %q, want alice", info.SubjectID)
	}
	if info.AuthorityGrantID != "grant-current" {
		t.Errorf("AuthorityGrantID = %q, want grant-current", info.AuthorityGrantID)
	}
	if info.RootAuthorityGrantID != "grant-root" {
		t.Errorf("RootAuthorityGrantID = %q, want grant-root", info.RootAuthorityGrantID)
	}
	if info.ParentAuthorityGrantID != "grant-parent" {
		t.Errorf("ParentAuthorityGrantID = %q, want grant-parent", info.ParentAuthorityGrantID)
	}
	if info.CreatorActorID != "ag::ws::creator::inst-1" {
		t.Errorf("CreatorActorID = %q, want ag.ws.creator.inst-1", info.CreatorActorID)
	}
	if info.ParentTaskID != "parent-task-1" {
		t.Errorf("ParentTaskID = %q, want parent-task-1", info.ParentTaskID)
	}
}

// TestLoadCallerTaskAuthority_NilGuards verifies the helper returns (nil, nil)
// without hitting downstream services when the caller is not bound to a task.
// Exhaustive derivation behavior is covered by the DB-backed integration test
// TestNestedCreateTaskDerivesAuthority.
func TestLoadCallerTaskAuthority_NilGuards(t *testing.T) {
	s := &GatewayServer{}

	got, err := s.loadCallerTaskAuthority(t.Context(), nil, models.Identity{})
	if err != nil || got != nil {
		t.Fatalf("nil client should return (nil, nil), got (%v, %v)", got, err)
	}

	client := &ClientSession{AssociatedTaskID: ""}
	got, err = s.loadCallerTaskAuthority(t.Context(), client, models.Identity{})
	if err != nil || got != nil {
		t.Fatalf("empty AssociatedTaskID should return (nil, nil), got (%v, %v)", got, err)
	}

	// taskStore/acl nil → guard must bail out before any call.
	clientWithTask := &ClientSession{AssociatedTaskID: "task-x"}
	got, err = s.loadCallerTaskAuthority(t.Context(), clientWithTask, models.Identity{})
	if err != nil || got != nil {
		t.Fatalf("nil acl/taskStore should return (nil, nil), got (%v, %v)", got, err)
	}
}
