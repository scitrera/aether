package orchestration

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/state"
	regpg "github.com/scitrera/aether/internal/storage/registry/postgres"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

func TestOrchestratedTaskPayload(t *testing.T) {
	// Test that orchestrated task payload contains required fields

	payload := &OrchestratedTaskPayload{
		TaskID:               "task-123",
		TargetImplementation: "python-worker",
		Workspace:            "production",
		LaunchParams: map[string]interface{}{
			"profile": "kubernetes",
			"image":   "worker:v1",
		},
	}

	if payload.TaskID == "" {
		t.Error("TaskID should not be empty")
	}

	if payload.TargetImplementation == "" {
		t.Error("TargetImplementation should not be empty")
	}

	if payload.LaunchParams["profile"] == nil {
		t.Error("LaunchParams should contain profile")
	}
}

// TestMergeLaunchParams tests the real registry.MergeLaunchParams function used in
// createOrchestratedStartupTask to merge defaults with per-request overrides.
func TestMergeLaunchParams(t *testing.T) {
	tests := []struct {
		name      string
		defaults  map[string]interface{}
		overrides map[string]interface{}
		want      map[string]interface{}
	}{
		{
			name: "override takes precedence over default",
			defaults: map[string]interface{}{
				"profile": "kubernetes",
				"cpu":     "500m",
				"memory":  "1Gi",
			},
			overrides: map[string]interface{}{
				"cpu": "2000m",
			},
			want: map[string]interface{}{
				"profile": "kubernetes",
				"cpu":     "2000m",
				"memory":  "1Gi",
			},
		},
		{
			name: "nil overrides returns copy of defaults",
			defaults: map[string]interface{}{
				"profile": "docker",
				"image":   "worker:latest",
			},
			overrides: nil,
			want: map[string]interface{}{
				"profile": "docker",
				"image":   "worker:latest",
			},
		},
		{
			name:      "nil defaults with overrides returns overrides",
			defaults:  nil,
			overrides: map[string]interface{}{"profile": "vm"},
			want:      map[string]interface{}{"profile": "vm"},
		},
		{
			name: "override adds new key not in defaults",
			defaults: map[string]interface{}{
				"profile": "kubernetes",
			},
			overrides: map[string]interface{}{
				"extra_flag": "true",
			},
			want: map[string]interface{}{
				"profile":    "kubernetes",
				"extra_flag": "true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := registry.MergeLaunchParams(tt.defaults, tt.overrides)
			for k, wantVal := range tt.want {
				gotVal, ok := got[k]
				if !ok {
					t.Errorf("key %q missing from merged result", k)
					continue
				}
				if gotVal != wantVal {
					t.Errorf("key %q: got %v, want %v", k, gotVal, wantVal)
				}
			}
			if len(got) != len(tt.want) {
				t.Errorf("merged map has %d keys, want %d", len(got), len(tt.want))
			}
		})
	}
}

type stubAuthorityGrantService struct {
	revoked []string
}

func (s *stubAuthorityGrantService) RevokeAuthorityGrant(_ context.Context, grantID string) error {
	s.revoked = append(s.revoked, grantID)
	return nil
}

func TestTaskAssignmentServiceCancelTaskRevokesAuthorityGrant(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	service := NewTaskAssignmentService(testDB.DB, taskStore, nil, nil, nil, nil)
	grantService := &stubAuthorityGrantService{}
	service.SetAuthorityGrantService(grantService)

	// Cancel must revoke the task's OWN authority grant only, never the
	// lineage root. For OBO-derived tasks the root is the caller's session
	// grant (e.g. a user's exchanged authority); revoking it would
	// invalidate every other concurrent operation under that session.
	task := &tasks.Task{
		TaskID:    uuid.New().String(),
		TaskType:  "unit-test",
		Workspace: "test-workspace",
		Metadata: map[string]interface{}{
			"authority_grant_id":      "grant-task-1",
			"root_authority_grant_id": "grant-root-user-1",
		},
	}
	if err := taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	if err := service.CancelTask(ctx, task.TaskID); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}

	if len(grantService.revoked) != 1 || grantService.revoked[0] != "grant-task-1" {
		t.Fatalf("revoked grants = %v, want [grant-task-1]", grantService.revoked)
	}
}

// TestTaskAssignmentServiceCancelTaskDoesNotRevokeOnlyRoot verifies that a
// task whose metadata records ONLY a root grant id (no per-task child grant)
// produces no revoke call — the root may belong to a parent context that
// outlives this task and must not be touched here.
func TestTaskAssignmentServiceCancelTaskDoesNotRevokeOnlyRoot(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	service := NewTaskAssignmentService(testDB.DB, taskStore, nil, nil, nil, nil)
	grantService := &stubAuthorityGrantService{}
	service.SetAuthorityGrantService(grantService)

	task := &tasks.Task{
		TaskID:    uuid.New().String(),
		TaskType:  "unit-test",
		Workspace: "test-workspace",
		Metadata: map[string]interface{}{
			"root_authority_grant_id": "grant-root-only",
		},
	}
	if err := taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	if err := service.CancelTask(ctx, task.TaskID); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}

	if len(grantService.revoked) != 0 {
		t.Fatalf("revoked grants = %v, want none (root must not be revoked)", grantService.revoked)
	}
}

// newTestTaskService builds a TaskAssignmentService wired against a real
// test DB, a registered agent implementation, and a miniredis-backed
// SessionRegistry. Callers get back the live service, the implementation
// name (for constructing target identities), and a cleanup that truncates
// the DB and stops miniredis.
func newTestTaskService(t *testing.T) (*TaskAssignmentService, string, string) {
	t.Helper()

	testDB, dbCleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		t.Skip("test database unavailable")
	}
	// dbCleanup truncates tables and releases the advisory lock. Run it before
	// closing the connection so the truncate statements succeed.
	t.Cleanup(func() {
		dbCleanup()
		testDB.Close()
	})

	ctx := context.Background()
	implementation := "fixaa-test-worker"
	// regpg.New bundles AgentRegistry + OrchestratorProfileManager into the
	// internal/storage/registry.Store interface that NewTaskAssignmentService
	// now expects. Passing nil for the profile state store is fine here — the
	// test doesn't exercise SelectOrchestrator.
	registryStore := regpg.New(testDB.DB, nil)
	if err := registryStore.Register(ctx, &registry.AgentRegistration{
		Implementation: implementation,
		LaunchParams: map[string]interface{}{
			"profile": "docker",
		},
	}); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sessionRegistry := state.NewSessionRegistryFromClient(redisClient)

	taskStore := taskpg.New(testDB.DB)
	service := NewTaskAssignmentService(testDB.DB, taskStore, registryStore, sessionRegistry, nil, nil)

	workspace := "fixaa-test-ws"
	return service, implementation, workspace
}

// TestCreateTaskRequest_TargetedPopulatesAuthority exercises the offline-
// agent path in handleTargeted: the queued regular task and the companion
// orchestrated startup task must both carry Authority.SubjectType="user" and
// Authority.SubjectID=<sender.ID> derived from CreateTaskRequest.SubjectIdentity.
func TestCreateTaskRequest_TargetedPopulatesAuthority(t *testing.T) {
	service, implementation, workspace := newTestTaskService(t)
	ctx := context.Background()

	targetAgentID := models.MustAgentTopic(workspace, implementation, "default")
	subject := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice@example.com",
		Specifier: "wnd-1",
	}

	req := &CreateTaskRequest{
		TaskType:       "message_delivery",
		Workspace:      workspace,
		AssignmentMode: "targeted",
		TargetAgentID:  targetAgentID,
		Metadata:       map[string]interface{}{"test": "value"},
		CreatorIdentity: models.Identity{
			Type: models.PrincipalAgent,
			ID:   "ag::_system::gateway::test",
		},
		SubjectIdentity: subject,
	}

	resp, err := service.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	if resp == nil || resp.TaskID == "" {
		t.Fatalf("CreateTask() returned empty response: %+v", resp)
	}

	stored, err := service.taskStore.GetTask(ctx, resp.TaskID)
	if err != nil {
		t.Fatalf("GetTask(%q) error = %v", resp.TaskID, err)
	}
	if got := stored.Authority.SubjectType; got != "user" {
		t.Errorf("Authority.SubjectType = %q, want %q", got, "user")
	}
	if got := stored.Authority.SubjectID; got != "alice@example.com" {
		t.Errorf("Authority.SubjectID = %q, want %q", got, "alice@example.com")
	}
	if got := stored.Authority.RootSubjectType; got != "user" {
		t.Errorf("Authority.RootSubjectType = %q, want %q", got, "user")
	}
	if got := stored.Authority.RootSubjectID; got != "alice@example.com" {
		t.Errorf("Authority.RootSubjectID = %q, want %q", got, "alice@example.com")
	}

	// The companion agent_startup task should also carry the same subject
	// lineage so buildTaskContext can populate task_context["user"] when the
	// orchestrated agent connects.
	hasActive, startupTaskID, err := service.taskStore.HasActiveStartupTask(ctx, implementation, workspace, "default")
	if err != nil {
		t.Fatalf("HasActiveStartupTask() error = %v", err)
	}
	if !hasActive {
		t.Fatalf("expected an active agent_startup task for offline agent")
	}
	startup, err := service.taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(startup %q) error = %v", startupTaskID, err)
	}
	if got := startup.Authority.SubjectType; got != "user" {
		t.Errorf("startup Authority.SubjectType = %q, want %q", got, "user")
	}
	if got := startup.Authority.SubjectID; got != "alice@example.com" {
		t.Errorf("startup Authority.SubjectID = %q, want %q", got, "alice@example.com")
	}
	if got := startup.Authority.RootSubjectType; got != "user" {
		t.Errorf("startup Authority.RootSubjectType = %q, want %q", got, "user")
	}
	if got := startup.Authority.RootSubjectID; got != "alice@example.com" {
		t.Errorf("startup Authority.RootSubjectID = %q, want %q", got, "alice@example.com")
	}
}

// TestCreateOrchestratedStartupTaskPopulatesAuthority covers the direct path
// where createOrchestratedStartupTask is called with a non-empty subject
// identity (e.g. from an admin-triggered startup on behalf of a user).
func TestCreateOrchestratedStartupTaskPopulatesAuthority(t *testing.T) {
	service, implementation, workspace := newTestTaskService(t)
	ctx := context.Background()

	targetIdentity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: implementation,
		Specifier:      "default",
	}
	subject := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "carol@example.com",
		Specifier: "wnd-9",
	}

	startupTaskID, err := service.createOrchestratedStartupTask(ctx, targetIdentity, workspace, nil, subject, nil)
	if err != nil {
		t.Fatalf("createOrchestratedStartupTask() error = %v", err)
	}

	stored, err := service.taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(%q) error = %v", startupTaskID, err)
	}
	if got := stored.Authority.SubjectType; got != "user" {
		t.Errorf("Authority.SubjectType = %q, want %q", got, "user")
	}
	if got := stored.Authority.SubjectID; got != "carol@example.com" {
		t.Errorf("Authority.SubjectID = %q, want %q", got, "carol@example.com")
	}
	if got := stored.Authority.RootSubjectType; got != "user" {
		t.Errorf("Authority.RootSubjectType = %q, want %q", got, "user")
	}
	if got := stored.Authority.RootSubjectID; got != "carol@example.com" {
		t.Errorf("Authority.RootSubjectID = %q, want %q", got, "carol@example.com")
	}
}

// TestCreateOrchestratedStartupTask_PropagatesTriggerTimestampMetadata is the
// regression guard for the first-message-lost bug. When the triggering
// request carries “trigger_timestamp_ms“ in its metadata (stamped by
// routing.go's “triggerOrchestration“ so a cold-started agent can replay
// the message that woke it up), that key MUST land on the spawned
// “agent_startup“ task's Metadata. The agent-side
// “lookupTriggerTimestampMs“ reads from the startup task because that's
// what “client.AssociatedTaskID“ binds to on connect.
//
// Prior bug: the startup task's Metadata was nil — timestamp was stored on
// the sibling “message_delivery“ row only — so first-time subscribes fell
// back to “.Next()“ and lost the message. See
// /home/drew/.claude/plans/first-message-lost-aether-fix.md.
func TestCreateOrchestratedStartupTask_PropagatesTriggerTimestampMetadata(t *testing.T) {
	service, implementation, workspace := newTestTaskService(t)
	ctx := context.Background()

	targetIdentity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: implementation,
		Specifier:      "dave@example.com",
	}

	// The triggering metadata is what routing.go builds:
	//   metadata["trigger"]              = "message_routing"
	//   metadata["target_topic"]         = <topic>
	//   metadata["trigger_timestamp_ms"] = "<unix-millis>"
	// Only ``trigger_timestamp_ms`` should propagate — we explicitly want
	// unrelated fields NOT to leak onto orchestration-state rows.
	triggeringMetadata := map[string]interface{}{
		"trigger":              "message_routing",
		"target_topic":         "ag::_apps::cowork::dave@example.com",
		"trigger_timestamp_ms": "1714000000000",
	}

	startupTaskID, err := service.createOrchestratedStartupTask(
		ctx, targetIdentity, workspace, nil, models.Identity{}, triggeringMetadata,
	)
	if err != nil {
		t.Fatalf("createOrchestratedStartupTask() error = %v", err)
	}

	stored, err := service.taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(%q) error = %v", startupTaskID, err)
	}
	if stored.Metadata == nil {
		t.Fatalf("stored.Metadata is nil — trigger_timestamp_ms was not propagated")
	}
	got, ok := stored.Metadata["trigger_timestamp_ms"]
	if !ok {
		t.Fatalf("stored.Metadata[\"trigger_timestamp_ms\"] missing; metadata=%v", stored.Metadata)
	}
	if gotStr, _ := got.(string); gotStr != "1714000000000" {
		t.Errorf("stored.Metadata[\"trigger_timestamp_ms\"] = %v, want \"1714000000000\"", got)
	}
	// Ensure unrelated keys did NOT leak — narrow whitelist only.
	if _, leaked := stored.Metadata["trigger"]; leaked {
		t.Errorf("unrelated key 'trigger' leaked into startup task metadata")
	}
	if _, leaked := stored.Metadata["target_topic"]; leaked {
		t.Errorf("unrelated key 'target_topic' leaked into startup task metadata")
	}
}

// TestCreateOrchestratedStartupTask_NilMetadataRemainsNil asserts the
// legacy no-metadata path still produces a task with nil Metadata, so
// existing callers that pass nil for “triggeringMetadata“ are not
// suddenly introducing an empty map.
func TestCreateOrchestratedStartupTask_NilMetadataRemainsNil(t *testing.T) {
	service, implementation, workspace := newTestTaskService(t)
	ctx := context.Background()

	targetIdentity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: implementation,
		Specifier:      "eve@example.com",
	}

	startupTaskID, err := service.createOrchestratedStartupTask(
		ctx, targetIdentity, workspace, nil, models.Identity{}, nil,
	)
	if err != nil {
		t.Fatalf("createOrchestratedStartupTask() error = %v", err)
	}
	stored, err := service.taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(%q) error = %v", startupTaskID, err)
	}
	if len(stored.Metadata) != 0 {
		t.Errorf("expected empty metadata for nil triggeringMetadata, got %v", stored.Metadata)
	}
}

// TestCreateOrchestratedStartupTask_PerUserSpecifierCoexist asserts that two
// startup tasks for the same (implementation, workspace) but different
// specifiers both become active simultaneously without colliding on the
// HasActiveStartupTask dedup. This is the load-bearing invariant for per-user
// singleton agents (e.g. CoworkAgent with workspace="_apps",
// specifier=<user_id>), where every user in a tenant shares the home workspace.
func TestCreateOrchestratedStartupTask_PerUserSpecifierCoexist(t *testing.T) {
	service, implementation, workspace := newTestTaskService(t)
	ctx := context.Background()

	identityA := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: implementation,
		Specifier:      "alice@example.com",
	}
	identityB := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: implementation,
		Specifier:      "bob@example.com",
	}

	taskA, err := service.createOrchestratedStartupTask(ctx, identityA, workspace, nil, models.Identity{}, nil)
	if err != nil {
		t.Fatalf("createOrchestratedStartupTask(alice) error = %v", err)
	}
	taskB, err := service.createOrchestratedStartupTask(ctx, identityB, workspace, nil, models.Identity{}, nil)
	if err != nil {
		t.Fatalf("createOrchestratedStartupTask(bob) error = %v", err)
	}
	if taskA == taskB || taskA == "" || taskB == "" {
		t.Fatalf("expected distinct non-empty startup task ids; got A=%q B=%q", taskA, taskB)
	}

	// Both tasks must be discoverable under their own specifier...
	hasA, foundA, err := service.taskStore.HasActiveStartupTask(ctx, implementation, workspace, "alice@example.com")
	if err != nil || !hasA || foundA != taskA {
		t.Fatalf("alice lookup: has=%v id=%q err=%v; want has=true id=%q", hasA, foundA, err, taskA)
	}
	hasB, foundB, err := service.taskStore.HasActiveStartupTask(ctx, implementation, workspace, "bob@example.com")
	if err != nil || !hasB || foundB != taskB {
		t.Fatalf("bob lookup: has=%v id=%q err=%v; want has=true id=%q", hasB, foundB, err, taskB)
	}

	// ...and a lookup for an unrelated specifier must not return either row.
	hasOther, foundOther, err := service.taskStore.HasActiveStartupTask(ctx, implementation, workspace, "carol@example.com")
	if err != nil {
		t.Fatalf("carol lookup error: %v", err)
	}
	if hasOther || foundOther != "" {
		t.Fatalf("carol lookup should be empty; got has=%v id=%q", hasOther, foundOther)
	}

	// A second create for alice must fail (same (impl, ws, specifier) tuple).
	if _, err := service.createOrchestratedStartupTask(ctx, identityA, workspace, nil, models.Identity{}, nil); err == nil {
		t.Fatalf("expected duplicate startup for alice to fail, got nil error")
	}

	// Verify the stored rows carry TargetSpecifier.
	storedA, err := service.taskStore.GetTask(ctx, taskA)
	if err != nil {
		t.Fatalf("GetTask(alice) error = %v", err)
	}
	if storedA.TargetSpecifier != "alice@example.com" {
		t.Errorf("alice TargetSpecifier = %q, want %q", storedA.TargetSpecifier, "alice@example.com")
	}
}

// TestCreateTaskRequest_TargetedEmptySubject verifies that zero-value
// SubjectIdentity preserves the previous behavior: no Authority fields are
// populated and existing internal/service-initiated callers continue to work.
func TestCreateTaskRequest_TargetedEmptySubject(t *testing.T) {
	service, implementation, workspace := newTestTaskService(t)
	ctx := context.Background()

	targetAgentID := models.MustAgentTopic(workspace, implementation, "default")
	req := &CreateTaskRequest{
		TaskType:       "message_delivery",
		Workspace:      workspace,
		AssignmentMode: "targeted",
		TargetAgentID:  targetAgentID,
		CreatorIdentity: models.Identity{
			Type: models.PrincipalAgent,
			ID:   "ag::_system::gateway::test",
		},
		// No SubjectIdentity — existing behavior must be preserved.
	}

	resp, err := service.CreateTask(ctx, req)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	stored, err := service.taskStore.GetTask(ctx, resp.TaskID)
	if err != nil {
		t.Fatalf("GetTask(%q) error = %v", resp.TaskID, err)
	}
	if got := stored.Authority.SubjectType; got != "" {
		t.Errorf("Authority.SubjectType = %q, want empty", got)
	}
	if got := stored.Authority.SubjectID; got != "" {
		t.Errorf("Authority.SubjectID = %q, want empty", got)
	}
	if got := stored.Authority.RootSubjectType; got != "" {
		t.Errorf("Authority.RootSubjectType = %q, want empty", got)
	}
	if got := stored.Authority.RootSubjectID; got != "" {
		t.Errorf("Authority.RootSubjectID = %q, want empty", got)
	}
}

// =============================================================================
// Queue-row retirement tests (Fix B2)
// =============================================================================

// stubDispatcher implements queueRetirementDispatcher and records calls for assertions.
type stubDispatcher struct {
	completedByTaskID []string
	failedByTaskID    []struct{ taskID, errorMsg string }
}

func (s *stubDispatcher) CompleteTaskByTaskID(_ context.Context, taskID string) error {
	s.completedByTaskID = append(s.completedByTaskID, taskID)
	return nil
}

func (s *stubDispatcher) FailTaskByTaskID(_ context.Context, taskID, errorMsg string) error {
	s.failedByTaskID = append(s.failedByTaskID, struct{ taskID, errorMsg string }{taskID, errorMsg})
	return nil
}

// TestStartTaskWithAgent_RetiresQueueRow verifies that StartTaskWithAgent retires
// the corresponding orchestrated_task_queue row via the dispatcher.
func TestStartTaskWithAgent_RetiresQueueRow(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	service := NewTaskAssignmentService(testDB.DB, taskStore, nil, nil, nil, nil)
	stub := &stubDispatcher{}
	service.SetOrchestratorDispatcher(stub)

	taskID := uuid.New().String()
	_, err := testDB.DB.ExecContext(ctx, `
		INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
		VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
	`, taskID)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}

	queueID := uuid.New().String()
	_, err = testDB.DB.ExecContext(ctx, `
		INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
		VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 0, 3)
	`, queueID, taskID)
	if err != nil {
		t.Fatalf("insert queue row: %v", err)
	}

	if err := service.StartTaskWithAgent(ctx, taskID, "ag::test-workspace::test-impl::default"); err != nil {
		t.Fatalf("StartTaskWithAgent() error = %v", err)
	}

	if len(stub.completedByTaskID) != 1 || stub.completedByTaskID[0] != taskID {
		t.Errorf("CompleteTaskByTaskID calls = %v, want [%s]", stub.completedByTaskID, taskID)
	}
	if len(stub.failedByTaskID) != 0 {
		t.Errorf("unexpected FailTaskByTaskID calls: %v", stub.failedByTaskID)
	}
}

// TestCompleteTask_RetiresQueueRow verifies that CompleteTask retires the queue row.
func TestCompleteTask_RetiresQueueRow(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	service := NewTaskAssignmentService(testDB.DB, taskStore, nil, nil, nil, nil)
	stub := &stubDispatcher{}
	service.SetOrchestratorDispatcher(stub)

	taskID := uuid.New().String()
	_, err := testDB.DB.ExecContext(ctx, `
		INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
		VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'running')
	`, taskID)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}

	queueID := uuid.New().String()
	_, err = testDB.DB.ExecContext(ctx, `
		INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
		VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 0, 3)
	`, queueID, taskID)
	if err != nil {
		t.Fatalf("insert queue row: %v", err)
	}

	if err := service.CompleteTask(ctx, taskID); err != nil {
		t.Fatalf("CompleteTask() error = %v", err)
	}

	if len(stub.completedByTaskID) != 1 || stub.completedByTaskID[0] != taskID {
		t.Errorf("CompleteTaskByTaskID calls = %v, want [%s]", stub.completedByTaskID, taskID)
	}
}

// TestFailTask_RetiresQueueRow verifies that FailTask retires the queue row as failed.
func TestFailTask_RetiresQueueRow(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	service := NewTaskAssignmentService(testDB.DB, taskStore, nil, nil, nil, nil)
	stub := &stubDispatcher{}
	service.SetOrchestratorDispatcher(stub)

	taskID := uuid.New().String()
	_, err := testDB.DB.ExecContext(ctx, `
		INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
		VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'running')
	`, taskID)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}

	queueID := uuid.New().String()
	_, err = testDB.DB.ExecContext(ctx, `
		INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
		VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 0, 3)
	`, queueID, taskID)
	if err != nil {
		t.Fatalf("insert queue row: %v", err)
	}

	const reason = "agent crashed"
	if err := service.FailTask(ctx, taskID, reason); err != nil {
		t.Fatalf("FailTask() error = %v", err)
	}

	if len(stub.failedByTaskID) != 1 {
		t.Fatalf("FailTaskByTaskID calls = %v, want 1 call", stub.failedByTaskID)
	}
	if stub.failedByTaskID[0].taskID != taskID {
		t.Errorf("FailTaskByTaskID taskID = %q, want %q", stub.failedByTaskID[0].taskID, taskID)
	}
	if stub.failedByTaskID[0].errorMsg != reason {
		t.Errorf("FailTaskByTaskID errorMsg = %q, want %q", stub.failedByTaskID[0].errorMsg, reason)
	}
}

// TestCancelTask_RetiresQueueRow verifies that CancelTask retires the queue row via FailTaskByTaskID.
func TestCancelTask_RetiresQueueRow(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	service := NewTaskAssignmentService(testDB.DB, taskStore, nil, nil, nil, nil)
	stub := &stubDispatcher{}
	service.SetOrchestratorDispatcher(stub)

	taskID := uuid.New().String()
	_, err := testDB.DB.ExecContext(ctx, `
		INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
		VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
	`, taskID)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}

	queueID := uuid.New().String()
	_, err = testDB.DB.ExecContext(ctx, `
		INSERT INTO orchestrated_task_queue (queue_id, task_id, target_implementation, workspace, profile, status, retry_count, max_retries)
		VALUES ($1, $2, 'test-impl', 'test-workspace', 'kubernetes', 'claimed', 0, 3)
	`, queueID, taskID)
	if err != nil {
		t.Fatalf("insert queue row: %v", err)
	}

	if err := service.CancelTask(ctx, taskID); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}

	if len(stub.failedByTaskID) != 1 {
		t.Fatalf("FailTaskByTaskID calls = %v, want 1 call", stub.failedByTaskID)
	}
	if stub.failedByTaskID[0].taskID != taskID {
		t.Errorf("FailTaskByTaskID taskID = %q, want %q", stub.failedByTaskID[0].taskID, taskID)
	}
	if stub.failedByTaskID[0].errorMsg != "task cancelled" {
		t.Errorf("FailTaskByTaskID errorMsg = %q, want %q", stub.failedByTaskID[0].errorMsg, "task cancelled")
	}
}

// TestStartTaskWithAgent_NoDispatcherIsSafe verifies that not calling
// SetOrchestratorDispatcher results in no panic and the task still transitions.
func TestStartTaskWithAgent_NoDispatcherIsSafe(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)
	// Intentionally no SetOrchestratorDispatcher call.
	service := NewTaskAssignmentService(testDB.DB, taskStore, nil, nil, nil, nil)

	taskID := uuid.New().String()
	_, err := testDB.DB.ExecContext(ctx, `
		INSERT INTO tasks (task_id, task_type, workspace, implementation, status)
		VALUES ($1, 'agent_startup', 'test-workspace', 'test-impl', 'assigned')
	`, taskID)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}

	if err := service.StartTaskWithAgent(ctx, taskID, "ag::test-workspace::test-impl::default"); err != nil {
		t.Fatalf("StartTaskWithAgent() without dispatcher error = %v", err)
	}

	// Verify task actually transitioned to running in the DB.
	stored, err := taskStore.GetTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if stored.Status != "running" {
		t.Errorf("task status = %q, want %q", stored.Status, "running")
	}
}
