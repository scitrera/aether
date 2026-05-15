package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/acl"
	tasks "github.com/scitrera/aether/internal/storage/tasks"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// TestNestedCreateTaskDerivesAuthority covers the Phase 2 auto-derivation case:
// an agent currently delivering a task under a task-bound authority grant
// should be able to call CreateTask without an explicit AuthorizationContext
// and the new task's grant should be derived from the parent grant with
// consistent lineage. Skips without Postgres dev infra.
func TestNestedCreateTaskDerivesAuthority(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test")
	taskStore := taskpg.New(testDB.DB)

	// Create a pre-existing parent task that will own the root grant.
	parentTaskID := uuid.New().String()
	parentTask := &tasks.Task{
		TaskID:         parentTaskID,
		TaskType:       "parent-work",
		Workspace:      "nested-ws",
		AssignmentMode: tasks.AssignmentModeTargeted,
		TaskCategory:   tasks.TaskCategoryRegular,
		Status:         tasks.TaskStatusRunning,
	}
	if err := taskStore.CreateTask(ctx, parentTask); err != nil {
		t.Fatalf("CreateTask(parent) error = %v", err)
	}

	// Worker (delegate) currently executing parentTask.
	worker := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "nested-ws",
		Implementation: "worker",
		Specifier:      "inst-1",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	issuer := models.Identity{Type: models.PrincipalService, Implementation: "gateway", Specifier: "test"}

	// Use UTC explicitly: the DB column is TIMESTAMP WITHOUT TIME ZONE, and
	// lib/pq's conversion of local-zoned time.Time values via this column
	// strips zone metadata on the way out, which makes locally-zoned
	// fixtures read back as expired on non-UTC hosts.
	expires := time.Now().UTC().Add(30 * time.Minute)
	renewable := time.Now().UTC().Add(4 * time.Hour)

	// Mint a task-bound parent grant for worker delivering parentTask.
	parentGrant, err := aclSvc.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:        subject,
		Delegate:       worker,
		IssuedBy:       issuer,
		MayDelegate:    true,
		RemainingHops:  3,
		MaxAccessLevel: acl.AccessReadWrite,
		AudienceType:   acl.AuthorityAudienceAgent,
		AudienceID:     worker.CanonicalPrincipalID(),
		ExpiresAt:      expires,
		RenewableUntil: renewable,
		Reason:         "test-parent",
	})
	if err != nil {
		t.Fatalf("CreateAuthorityGrant(parent) error = %v", err)
	}

	// Persist grant onto the parent task so loadCallerTaskAuthority can find it.
	if err := taskStore.UpdateTaskAuthority(ctx, parentTaskID, tasks.TaskAuthorityInfo{
		Mode:                 "on_behalf_of",
		SubjectType:          parentGrant.SubjectType,
		SubjectID:            parentGrant.SubjectID,
		RootSubjectType:      parentGrant.RootSubjectType,
		RootSubjectID:        parentGrant.RootSubjectID,
		AuthorityGrantID:     parentGrant.GrantID,
		RootAuthorityGrantID: parentGrant.RootGrantID,
		AudienceType:         parentGrant.AudienceType,
		AudienceID:           parentGrant.AudienceID,
		DelegateType:         parentGrant.DelegateType,
		DelegateID:           parentGrant.DelegateID,
	}, nil); err != nil {
		t.Fatalf("UpdateTaskAuthority(parent) error = %v", err)
	}

	gw := &GatewayServer{
		acl:       aclSvc,
		taskStore: taskStore,
	}

	client := &ClientSession{
		SessionUUID:      uuid.New(),
		Identity:         worker,
		AssociatedTaskID: parentTaskID,
	}

	inherited, err := gw.loadCallerTaskAuthority(ctx, client, worker)
	if err != nil {
		t.Fatalf("loadCallerTaskAuthority() error = %v", err)
	}
	if inherited == nil {
		t.Fatalf("loadCallerTaskAuthority() = nil; expected inherited authority from parent task grant")
	}
	if inherited.Grant.GrantID != parentGrant.GrantID {
		t.Errorf("inherited grant id = %q, want %q", inherited.Grant.GrantID, parentGrant.GrantID)
	}
	if inherited.Subject.CanonicalPrincipalID() != "alice" {
		t.Errorf("inherited subject = %q, want alice", inherited.Subject.CanonicalPrincipalID())
	}

	// Now derive a child grant the way establishTaskAuthorityGrant would for a
	// nested task whose assignee is the same worker — asserting lineage fields.
	nestedTaskID := uuid.New().String()
	childGrant, err := gw.createTaskAuthorityGrant(
		ctx,
		inherited,
		worker,
		worker,
		acl.AuthorityAudienceAgent,
		worker.CanonicalPrincipalID(),
		nestedTaskID,
		"child-work",
		"targeted",
		false,
	)
	if err != nil {
		t.Fatalf("createTaskAuthorityGrant(nested) error = %v", err)
	}
	if childGrant.ParentGrantID == nil || *childGrant.ParentGrantID != parentGrant.GrantID {
		t.Errorf("child grant parent = %v, want %q", childGrant.ParentGrantID, parentGrant.GrantID)
	}
	wantRoot := parentGrant.RootGrantID
	if wantRoot == "" {
		wantRoot = parentGrant.GrantID
	}
	if childGrant.RootGrantID != wantRoot {
		t.Errorf("child grant root = %q, want %q", childGrant.RootGrantID, wantRoot)
	}
	if childGrant.RemainingHops != parentGrant.RemainingHops-1 {
		t.Errorf("child grant hops = %d, want %d", childGrant.RemainingHops, parentGrant.RemainingHops-1)
	}
	if childGrant.SubjectID != parentGrant.SubjectID {
		t.Errorf("child subject = %q, want %q", childGrant.SubjectID, parentGrant.SubjectID)
	}
}

// TestMintTaskGrantForSender_CreatesRootGrantAndUpdatesTask covers Fix AAA:
// when triggerOrchestration creates a task on behalf of a user-sender with no
// upstream OBO grant to derive from, mintTaskGrantForSender must produce a
// root-level authority grant (Subject = sender, ParentGrantID = NULL,
// RootGrantID == GrantID) AND persist the grant onto the task row's first-class
// authority columns via UpdateTaskAuthority.
func TestMintTaskGrantForSender_CreatesRootGrantAndUpdatesTask(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-fixaaa")
	taskStore := taskpg.New(testDB.DB)

	// Pre-create a task for the grant to attach to.
	taskID := uuid.New().String()
	workspace := "fixaaa-ws"
	task := &tasks.Task{
		TaskID:         taskID,
		TaskType:       "message_delivery",
		Workspace:      workspace,
		AssignmentMode: tasks.AssignmentModeTargeted,
		TaskCategory:   tasks.TaskCategoryRegular,
		Status:         tasks.TaskStatusPending,
	}
	if err := taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	gw := &GatewayServer{
		acl:       aclSvc,
		taskStore: taskStore,
		gatewayID: "test-fixaaa",
	}

	sender := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "carol@example.com",
		Specifier: "wnd-xyz",
	}
	delegate := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: "fixaaa-worker",
		Specifier:      "default",
	}

	grant, err := gw.mintTaskGrantForSender(ctx, sender, taskID, workspace, delegate, "test:fixaaa")
	if err != nil {
		t.Fatalf("mintTaskGrantForSender() error = %v", err)
	}

	// Grant must be a ROOT grant (no parent, root == self).
	if grant.ParentGrantID != nil {
		t.Errorf("grant.ParentGrantID = %v, want nil (root grant)", grant.ParentGrantID)
	}
	if grant.RootGrantID != grant.GrantID {
		t.Errorf("grant.RootGrantID = %q, want %q (root grant)", grant.RootGrantID, grant.GrantID)
	}
	if grant.SubjectID != sender.ID {
		t.Errorf("grant.SubjectID = %q, want %q", grant.SubjectID, sender.ID)
	}
	if grant.SubjectType != acl.PrincipalTypeUser {
		t.Errorf("grant.SubjectType = %q, want %q", grant.SubjectType, acl.PrincipalTypeUser)
	}
	if grant.RootSubjectID != sender.ID {
		t.Errorf("grant.RootSubjectID = %q, want %q", grant.RootSubjectID, sender.ID)
	}

	// Task row must have been updated with the grant lineage.
	persisted, err := taskStore.GetTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetTask(%q) error = %v", taskID, err)
	}
	if persisted.Authority.AuthorityGrantID != grant.GrantID {
		t.Errorf("task Authority.AuthorityGrantID = %q, want %q", persisted.Authority.AuthorityGrantID, grant.GrantID)
	}
	if persisted.Authority.RootAuthorityGrantID != grant.GrantID {
		t.Errorf("task Authority.RootAuthorityGrantID = %q, want %q", persisted.Authority.RootAuthorityGrantID, grant.GrantID)
	}
	if persisted.Authority.SubjectType != "user" {
		t.Errorf("task Authority.SubjectType = %q, want %q", persisted.Authority.SubjectType, "user")
	}
	if persisted.Authority.SubjectID != sender.ID {
		t.Errorf("task Authority.SubjectID = %q, want %q", persisted.Authority.SubjectID, sender.ID)
	}
}

// ---------------------------------------------------------------------------
// loadCallerMessageAuthority tests
// ---------------------------------------------------------------------------

// TestLoadCallerMessageAuthority_ReturnsAuthority verifies that an agent with
// an AssociatedTaskID pointing to a task with a valid grant gets a
// ResolvedAuthority back with the correct Subject, Actor, and Grant.
// The grant has MayDelegate=false / RemainingHops=0 to confirm that
// loadCallerMessageAuthority — unlike loadCallerTaskAuthority — succeeds even
// without delegation capability.
func TestLoadCallerMessageAuthority_ReturnsAuthority(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-msg-auth")
	taskStore := taskpg.New(testDB.DB)

	taskID := uuid.New().String()
	workspace := "msg-auth-ws"
	task := &tasks.Task{
		TaskID:         taskID,
		TaskType:       "message_delivery",
		Workspace:      workspace,
		AssignmentMode: tasks.AssignmentModeTargeted,
		TaskCategory:   tasks.TaskCategoryRegular,
		Status:         tasks.TaskStatusRunning,
	}
	if err := taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	agent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: "pool-worker",
		Specifier:      "inst-msg",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "bob@example.com"}
	issuer := models.Identity{Type: models.PrincipalService, Implementation: "gateway", Specifier: "test"}

	expires := time.Now().UTC().Add(30 * time.Minute)
	renewable := time.Now().UTC().Add(4 * time.Hour)

	// MayDelegate=false, RemainingHops=0 — grant can be USED but not further delegated.
	grant, err := aclSvc.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:        subject,
		Delegate:       agent,
		IssuedBy:       issuer,
		MayDelegate:    false,
		RemainingHops:  0,
		MaxAccessLevel: acl.AccessReadWrite,
		AudienceType:   acl.AuthorityAudienceAgent,
		AudienceID:     agent.CanonicalPrincipalID(),
		ExpiresAt:      expires,
		RenewableUntil: renewable,
		Reason:         "test-msg-authority",
	})
	if err != nil {
		t.Fatalf("CreateAuthorityGrant() error = %v", err)
	}

	if err := taskStore.UpdateTaskAuthority(ctx, taskID, tasks.TaskAuthorityInfo{
		Mode:                 "on_behalf_of",
		SubjectType:          grant.SubjectType,
		SubjectID:            grant.SubjectID,
		RootSubjectType:      grant.RootSubjectType,
		RootSubjectID:        grant.RootSubjectID,
		AuthorityGrantID:     grant.GrantID,
		RootAuthorityGrantID: grant.RootGrantID,
		AudienceType:         grant.AudienceType,
		AudienceID:           grant.AudienceID,
		DelegateType:         grant.DelegateType,
		DelegateID:           grant.DelegateID,
	}, nil); err != nil {
		t.Fatalf("UpdateTaskAuthority() error = %v", err)
	}

	gw := &GatewayServer{acl: aclSvc, taskStore: taskStore}
	client := &ClientSession{
		SessionUUID:      uuid.New(),
		Identity:         agent,
		AssociatedTaskID: taskID,
	}

	got, err := gw.loadCallerMessageAuthority(ctx, client, agent)
	if err != nil {
		t.Fatalf("loadCallerMessageAuthority() error = %v", err)
	}
	if got == nil {
		t.Fatal("loadCallerMessageAuthority() = nil; want ResolvedAuthority")
	}
	if got.Grant.GrantID != grant.GrantID {
		t.Errorf("Grant.GrantID = %q, want %q", got.Grant.GrantID, grant.GrantID)
	}
	if got.Subject.CanonicalPrincipalID() != subject.CanonicalPrincipalID() {
		t.Errorf("Subject = %q, want %q", got.Subject.CanonicalPrincipalID(), subject.CanonicalPrincipalID())
	}
	if got.Actor.CanonicalPrincipalID() != agent.CanonicalPrincipalID() {
		t.Errorf("Actor = %q, want %q", got.Actor.CanonicalPrincipalID(), agent.CanonicalPrincipalID())
	}

	// Contrast: loadCallerTaskAuthority must reject the same grant (no delegation capability).
	rejected, err := gw.loadCallerTaskAuthority(ctx, client, agent)
	if err != nil {
		t.Fatalf("loadCallerTaskAuthority() error = %v", err)
	}
	if rejected != nil {
		t.Errorf("loadCallerTaskAuthority() = non-nil; want nil for non-delegable grant")
	}
}

// TestLoadCallerMessageAuthority_NoTask verifies that (nil, nil) is returned
// when the client has no AssociatedTaskID set.
func TestLoadCallerMessageAuthority_NoTask(t *testing.T) {
	ctx := context.Background()
	gw := &GatewayServer{}

	agent := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1", Implementation: "w", Specifier: "s"}
	client := &ClientSession{SessionUUID: uuid.New(), Identity: agent, AssociatedTaskID: ""}

	got, err := gw.loadCallerMessageAuthority(ctx, client, agent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got non-nil ResolvedAuthority; want nil")
	}
}

// TestLoadCallerMessageAuthority_NoGrant verifies that (nil, nil) is returned
// when the session-bound task exists but has no authority grant attached.
func TestLoadCallerMessageAuthority_NoGrant(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	taskStore := taskpg.New(testDB.DB)

	taskID := uuid.New().String()
	task := &tasks.Task{
		TaskID:         taskID,
		TaskType:       "no-grant",
		Workspace:      "ws-ng",
		AssignmentMode: tasks.AssignmentModeTargeted,
		TaskCategory:   tasks.TaskCategoryRegular,
		Status:         tasks.TaskStatusRunning,
	}
	if err := taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	aclSvc := acl.NewService(testDB.DB, "gateway-test-no-grant")
	gw := &GatewayServer{acl: aclSvc, taskStore: taskStore}
	agent := models.Identity{Type: models.PrincipalAgent, Workspace: "ws-ng", Implementation: "w", Specifier: "s"}
	client := &ClientSession{SessionUUID: uuid.New(), Identity: agent, AssociatedTaskID: taskID}

	got, err := gw.loadCallerMessageAuthority(ctx, client, agent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got non-nil ResolvedAuthority; want nil (no grant on task)")
	}
}

// TestLoadCallerMessageAuthority_GrantUnusableByActor verifies that (nil, nil)
// is returned when the grant's audience does not match the caller agent.
func TestLoadCallerMessageAuthority_GrantUnusableByActor(t *testing.T) {
	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return
	}
	defer testDB.Close()
	defer cleanup()

	ctx := context.Background()
	aclSvc := acl.NewService(testDB.DB, "gateway-test-wrong-actor")
	taskStore := taskpg.New(testDB.DB)

	taskID := uuid.New().String()
	workspace := "wrong-actor-ws"
	task := &tasks.Task{
		TaskID:         taskID,
		TaskType:       "message_delivery",
		Workspace:      workspace,
		AssignmentMode: tasks.AssignmentModeTargeted,
		TaskCategory:   tasks.TaskCategoryRegular,
		Status:         tasks.TaskStatusRunning,
	}
	if err := taskStore.CreateTask(ctx, task); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	// The grant's audience is agent "authorized-worker", but we'll query with "other-worker".
	authorizedAgent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: "authorized-worker",
		Specifier:      "inst-a",
	}
	otherAgent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: "other-worker",
		Specifier:      "inst-b",
	}
	subject := models.Identity{Type: models.PrincipalUser, ID: "carol@example.com"}
	issuer := models.Identity{Type: models.PrincipalService, Implementation: "gateway", Specifier: "test"}

	expires := time.Now().UTC().Add(30 * time.Minute)
	renewable := time.Now().UTC().Add(4 * time.Hour)

	grant, err := aclSvc.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:        subject,
		Delegate:       authorizedAgent,
		IssuedBy:       issuer,
		MayDelegate:    true,
		RemainingHops:  1,
		MaxAccessLevel: acl.AccessReadWrite,
		AudienceType:   acl.AuthorityAudienceAgent,
		AudienceID:     authorizedAgent.CanonicalPrincipalID(),
		ExpiresAt:      expires,
		RenewableUntil: renewable,
		Reason:         "test-wrong-actor",
	})
	if err != nil {
		t.Fatalf("CreateAuthorityGrant() error = %v", err)
	}

	if err := taskStore.UpdateTaskAuthority(ctx, taskID, tasks.TaskAuthorityInfo{
		Mode:                 "on_behalf_of",
		SubjectType:          grant.SubjectType,
		SubjectID:            grant.SubjectID,
		RootSubjectType:      grant.RootSubjectType,
		RootSubjectID:        grant.RootSubjectID,
		AuthorityGrantID:     grant.GrantID,
		RootAuthorityGrantID: grant.RootGrantID,
		AudienceType:         grant.AudienceType,
		AudienceID:           grant.AudienceID,
		DelegateType:         grant.DelegateType,
		DelegateID:           grant.DelegateID,
	}, nil); err != nil {
		t.Fatalf("UpdateTaskAuthority() error = %v", err)
	}

	gw := &GatewayServer{acl: aclSvc, taskStore: taskStore}
	// otherAgent is the caller — grant audience is authorizedAgent, so it must be rejected.
	client := &ClientSession{
		SessionUUID:      uuid.New(),
		Identity:         otherAgent,
		AssociatedTaskID: taskID,
	}

	got, err := gw.loadCallerMessageAuthority(ctx, client, otherAgent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got non-nil ResolvedAuthority; want nil (grant audience mismatch)")
	}
}
