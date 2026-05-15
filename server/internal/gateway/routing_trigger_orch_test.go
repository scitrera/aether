package gateway

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/state"
	regpg "github.com/scitrera/aether/internal/storage/registry/postgres"
	tasks "github.com/scitrera/aether/internal/storage/tasks"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// TestTriggerOrchestrationPropagatesSenderToStartupTask is the gateway-side
// integration coverage for Fix AA: it confirms that when routeMessage detects
// an offline target and invokes triggerOrchestration with a user sender, the
// resulting agent_startup task persisted by TaskAssignmentService carries
// Authority.SubjectType="user" and Authority.SubjectID=<sender.ID>. Without
// this, buildTaskContext cannot populate task_context["user"] and
// send_to_user downstream routes to us.None.<window>.
func TestTriggerOrchestrationPropagatesSenderToStartupTask(t *testing.T) {
	testDB, dbCleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		t.Skip("test database unavailable")
	}
	t.Cleanup(func() {
		dbCleanup()
		testDB.Close()
	})

	ctx := context.Background()
	implementation := "fixaa-gateway-worker"
	workspace := "fixaa-gateway-ws"

	// Stage 1 storage-interfaces refactor: NewTaskAssignmentService and
	// OrchestrationServices.Registry both take the bundled
	// internal/storage/registry.Store. Passing nil for the profile state store
	// is fine — these tests don't exercise SelectOrchestrator.
	agentRegistry := regpg.New(testDB.DB, nil)
	if err := agentRegistry.Register(ctx, &registry.AgentRegistration{
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
	taskService := orchestration.NewTaskAssignmentService(
		taskStore,
		agentRegistry,
		sessionRegistry,
		nil,
		nil,
	)

	server := &GatewayServer{
		gatewayID: "test-fixaa",
		taskStore: taskStore,
		orchestration: &OrchestrationServices{
			Registry:    agentRegistry,
			TaskService: taskService,
		},
	}

	sender := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "dave@example.com",
		Specifier: "wnd-abc",
	}
	targetTopic := models.MustAgentTopic(workspace, implementation, "default")

	server.triggerOrchestration(ctx, sender, targetTopic, 0, "")

	hasActive, startupTaskID, err := taskStore.HasActiveStartupTask(ctx, implementation, workspace, "default")
	if err != nil {
		t.Fatalf("HasActiveStartupTask() error = %v", err)
	}
	if !hasActive {
		t.Fatalf("expected triggerOrchestration to create an active agent_startup task")
	}

	startup, err := taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(%q) error = %v", startupTaskID, err)
	}
	if got := startup.Authority.SubjectType; got != "user" {
		t.Errorf("startup Authority.SubjectType = %q, want %q", got, "user")
	}
	if got := startup.Authority.SubjectID; got != "dave@example.com" {
		t.Errorf("startup Authority.SubjectID = %q, want %q", got, "dave@example.com")
	}
	if got := startup.Authority.RootSubjectType; got != "user" {
		t.Errorf("startup Authority.RootSubjectType = %q, want %q", got, "user")
	}
	if got := startup.Authority.RootSubjectID; got != "dave@example.com" {
		t.Errorf("startup Authority.RootSubjectID = %q, want %q", got, "dave@example.com")
	}
}

// TestTriggerOrchestrationMintsGrantForStartupTask is the gateway-side coverage
// for Fix AAA (updated after the message_delivery-task dropoff): when
// routeMessage's offline branch invokes triggerOrchestration with a user
// sender, the spawned agent_startup task must carry a root authority grant
// whose Subject is the user-sender. Without this, ACL checks for the spawned
// agent calling send_to_user deny with "Fallback policy for agent_workspace:
// READ". A previous revision of this test also asserted a companion
// message_delivery task was created + granted — that task is now skipped
// entirely because it carried no payload and had no agent-side handler, so
// the test additionally asserts its absence as a regression guard.
func TestTriggerOrchestrationMintsGrantForStartupTask(t *testing.T) {
	testDB, dbCleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		t.Skip("test database unavailable")
	}
	t.Cleanup(func() {
		dbCleanup()
		testDB.Close()
	})

	ctx := context.Background()
	implementation := "fixaaa-gateway-worker"
	workspace := "fixaaa-gateway-ws"

	// Stage 1 storage-interfaces refactor: NewTaskAssignmentService and
	// OrchestrationServices.Registry both take the bundled
	// internal/storage/registry.Store. Passing nil for the profile state store
	// is fine — these tests don't exercise SelectOrchestrator.
	agentRegistry := regpg.New(testDB.DB, nil)
	if err := agentRegistry.Register(ctx, &registry.AgentRegistration{
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
	aclSvc := acl.NewService(testDB.DB, "gateway-test-fixaaa-routing")
	taskService := orchestration.NewTaskAssignmentService(
		taskStore,
		agentRegistry,
		sessionRegistry,
		nil,
		nil,
	)

	server := &GatewayServer{
		gatewayID: "test-fixaaa",
		taskStore: taskStore,
		acl:       aclSvc,
		orchestration: &OrchestrationServices{
			Registry:    agentRegistry,
			TaskService: taskService,
		},
	}

	sender := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "eve@example.com",
		Specifier: "wnd-grant",
	}
	targetTopic := models.MustAgentTopic(workspace, implementation, "default")

	server.triggerOrchestration(ctx, sender, targetTopic, 0, "")

	// Verify the agent_startup task exists and was granted.
	hasActive, startupTaskID, err := taskStore.HasActiveStartupTask(ctx, implementation, workspace, "default")
	if err != nil {
		t.Fatalf("HasActiveStartupTask() error = %v", err)
	}
	if !hasActive {
		t.Fatalf("expected triggerOrchestration to create an active agent_startup task")
	}

	startup, err := taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(startup=%q) error = %v", startupTaskID, err)
	}
	if startup.Authority.AuthorityGrantID == "" {
		t.Errorf("agent_startup task Authority.AuthorityGrantID is empty; grant was not minted")
	}
	if startup.Authority.SubjectType != "user" || startup.Authority.SubjectID != sender.ID {
		t.Errorf("agent_startup task subject = (%q, %q), want (user, %q)", startup.Authority.SubjectType, startup.Authority.SubjectID, sender.ID)
	}
	// The delegate must be the target agent (not a task-anchor) so that
	// loadCallerMessageAuthority can match it when the agent sends send_to_user.
	targetIdentity, parseErr := models.ParseIdentity(targetTopic)
	if parseErr != nil {
		t.Fatalf("ParseIdentity(%q) error = %v", targetTopic, parseErr)
	}
	if startup.Authority.DelegateType != "agent" {
		t.Errorf("agent_startup grant DelegateType = %q, want %q", startup.Authority.DelegateType, "agent")
	}
	if startup.Authority.DelegateID != targetIdentity.CanonicalPrincipalID() {
		t.Errorf("agent_startup grant DelegateID = %q, want %q", startup.Authority.DelegateID, targetIdentity.CanonicalPrincipalID())
	}

	// Regression guard: triggerOrchestration must NOT create a message_delivery
	// task anymore. That task was metadata-only (no payload — user's actual
	// chat message is published to the RabbitMQ stream at routeMessage L340),
	// had no agent-side handler, and just consumed a redundant row + grant.
	// Dropping it halved the tasks per offline-agent trigger.
	delivery, err := taskStore.ListTasks(ctx, &tasks.TaskFilter{
		Workspace:     workspace,
		TaskType:      "message_delivery",
		TargetAgentID: targetTopic,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("ListTasks(message_delivery) error = %v", err)
	}
	if len(delivery) != 0 {
		t.Errorf("triggerOrchestration created %d message_delivery task(s); want 0 (task was removed as redundant)", len(delivery))
	}
}

// TestTriggerOrchestration_PropagatesTriggerTimestampThroughGrantMint asserts
// that the trigger_timestamp_ms metadata stamped by triggerOrchestration
// survives the subsequent mintTaskGrantForSender → UpdateTaskAuthority write.
// Prior to the fix, mintTaskGrantForSender passed an empty base map to
// applyTaskAuthorityGrantToMetadata and UpdateTaskAuthority wrote the
// resulting (authority-only) map straight to the task's metadata column —
// silently clobbering the trigger_timestamp_ms that createOrchestratedStartupTask
// had just placed. The downstream effect: the agent's cold-start subscription
// couldn't look up the trigger timestamp via lookupTriggerTimestampMs, fell
// through to .Next(), and lost the message that woke the agent up (the
// first-message-lost bug).
func TestTriggerOrchestration_PropagatesTriggerTimestampThroughGrantMint(t *testing.T) {
	testDB, dbCleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		t.Skip("test database unavailable")
	}
	t.Cleanup(func() {
		dbCleanup()
		testDB.Close()
	})

	ctx := context.Background()
	implementation := "trigger-ts-preservation-worker"
	workspace := "trigger-ts-preservation-ws"

	// Stage 1 storage-interfaces refactor: NewTaskAssignmentService and
	// OrchestrationServices.Registry both take the bundled
	// internal/storage/registry.Store. Passing nil for the profile state store
	// is fine — these tests don't exercise SelectOrchestrator.
	agentRegistry := regpg.New(testDB.DB, nil)
	if err := agentRegistry.Register(ctx, &registry.AgentRegistration{
		Implementation: implementation,
		LaunchParams:   map[string]interface{}{"profile": "docker"},
	}); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sessionRegistry := state.NewSessionRegistryFromClient(redisClient)

	taskStore := taskpg.New(testDB.DB)
	aclSvc := acl.NewService(testDB.DB, "gateway-test-trigger-ts")
	taskService := orchestration.NewTaskAssignmentService(
		taskStore, agentRegistry, sessionRegistry, nil, nil,
	)

	server := &GatewayServer{
		gatewayID: "test-triggerts",
		taskStore: taskStore,
		acl:       aclSvc,
		orchestration: &OrchestrationServices{
			Registry:    agentRegistry,
			TaskService: taskService,
		},
	}

	sender := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "dave@example.com",
		Specifier: "wnd-ts",
	}
	targetTopic := models.MustAgentTopic(workspace, implementation, "default")

	// Use a non-zero trigger timestamp — this is what we expect to survive.
	const expectedTriggerTs int64 = 1714000000000
	server.triggerOrchestration(ctx, sender, targetTopic, expectedTriggerTs, "")

	hasActive, startupTaskID, err := taskStore.HasActiveStartupTask(ctx, implementation, workspace, "default")
	if err != nil {
		t.Fatalf("HasActiveStartupTask() error = %v", err)
	}
	if !hasActive {
		t.Fatalf("expected triggerOrchestration to create an active agent_startup task")
	}

	startup, err := taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(startup=%q) error = %v", startupTaskID, err)
	}

	// Grant mint happened AFTER task creation; ensure it didn't overwrite metadata.
	if startup.Metadata == nil {
		t.Fatalf("startup task Metadata is nil after grant mint")
	}
	got, ok := startup.Metadata["trigger_timestamp_ms"]
	if !ok {
		t.Fatalf("startup task Metadata missing trigger_timestamp_ms after grant mint; got keys=%v", keysOf(startup.Metadata))
	}
	if gotStr, _ := got.(string); gotStr != "1714000000000" {
		t.Errorf("startup task trigger_timestamp_ms = %v, want \"1714000000000\"", got)
	}
	// Sanity: authority fields are also present (grant mint did run).
	if _, ok := startup.Metadata["authority_grant_id"]; !ok {
		t.Errorf("startup task Metadata missing authority_grant_id (grant mint didn't happen?)")
	}
}

// TestTriggerOrchestration_IncludesAppWorkspaceInGrantScope verifies that when
// triggerOrchestration is called with a non-empty senderAppWorkspace, the
// minted agent_startup task authority grant's WorkspaceScope contains BOTH
// the task's workspace (e.g. "_apps") AND the user's active app workspace
// (e.g. "default"). Without this, spawned agents are denied from creating
// resources (sandbox leases, KV writes) in the user's workspace.
func TestTriggerOrchestration_IncludesAppWorkspaceInGrantScope(t *testing.T) {
	testDB, dbCleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		t.Skip("test database unavailable")
	}
	t.Cleanup(func() {
		dbCleanup()
		testDB.Close()
	})

	ctx := context.Background()
	implementation := "app-workspace-scope-worker"
	workspace := "_apps"
	appWorkspace := "default"

	// Stage 1 storage-interfaces refactor: NewTaskAssignmentService and
	// OrchestrationServices.Registry both take the bundled
	// internal/storage/registry.Store. Passing nil for the profile state store
	// is fine — these tests don't exercise SelectOrchestrator.
	agentRegistry := regpg.New(testDB.DB, nil)
	if err := agentRegistry.Register(ctx, &registry.AgentRegistration{
		Implementation: implementation,
		LaunchParams:   map[string]interface{}{"profile": "docker"},
	}); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sessionRegistry := state.NewSessionRegistryFromClient(redisClient)

	taskStore := taskpg.New(testDB.DB)
	aclSvc := acl.NewService(testDB.DB, "gateway-test-appws-scope")
	taskService := orchestration.NewTaskAssignmentService(
		taskStore, agentRegistry, sessionRegistry, nil, nil,
	)

	server := &GatewayServer{
		gatewayID: "test-appws",
		taskStore: taskStore,
		acl:       aclSvc,
		orchestration: &OrchestrationServices{
			Registry:    agentRegistry,
			TaskService: taskService,
		},
	}

	sender := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "alice@example.com",
		Specifier: "wnd-appws",
	}
	targetTopic := models.MustAgentTopic(workspace, implementation, "alice@example.com")

	server.triggerOrchestration(ctx, sender, targetTopic, 0, appWorkspace)

	hasActive, startupTaskID, err := taskStore.HasActiveStartupTask(ctx, implementation, workspace, "alice@example.com")
	if err != nil {
		t.Fatalf("HasActiveStartupTask() error = %v", err)
	}
	if !hasActive {
		t.Fatalf("expected triggerOrchestration to create an active agent_startup task")
	}

	startup, err := taskStore.GetTask(ctx, startupTaskID)
	if err != nil {
		t.Fatalf("GetTask(startup=%q) error = %v", startupTaskID, err)
	}
	if startup.Authority.AuthorityGrantID == "" {
		t.Fatalf("agent_startup task Authority.AuthorityGrantID is empty; grant was not minted")
	}

	// Fetch the minted grant and check its WorkspaceScope contains both workspaces.
	grant, err := aclSvc.GetAuthorityGrant(ctx, startup.Authority.AuthorityGrantID)
	if err != nil {
		t.Fatalf("GetAuthorityGrant(%q) error = %v", startup.Authority.AuthorityGrantID, err)
	}

	scopeSet := make(map[string]bool, len(grant.WorkspaceScope))
	for _, w := range grant.WorkspaceScope {
		scopeSet[w] = true
	}
	if !scopeSet[workspace] {
		t.Errorf("grant WorkspaceScope %v missing task workspace %q", grant.WorkspaceScope, workspace)
	}
	if !scopeSet[appWorkspace] {
		t.Errorf("grant WorkspaceScope %v missing app_workspace %q", grant.WorkspaceScope, appWorkspace)
	}
}

// keysOf returns the string keys of a generic map for use in test failure
// messages.
func keysOf(m map[string]interface{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
