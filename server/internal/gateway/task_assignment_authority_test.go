package gateway

import (
	"context"
	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/internal/orchestration"
	taskstore "github.com/scitrera/aether/server/internal/storage/tasks"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
	"testing"
	"time"
)

func TestTaskAuthorityAssignmentRequiresDirectUserApproval(t *testing.T) {
	user := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	good := func() *pb.CreateTaskRequest {
		return &pb.CreateTaskRequest{AuthorityAssignment: &pb.TaskAuthorityAssignment{ExpiresInSeconds: 28800, MaxAccessLevel: 20}}
	}
	if err := validateTaskAuthorityAssignment(user, good()); err != nil {
		t.Fatal(err)
	}
	service := models.Identity{Type: models.PrincipalService, Implementation: "bridge", Specifier: "one"}
	if err := validateTaskAuthorityAssignment(service, good()); err == nil {
		t.Fatal("service self-authorized")
	}
	for _, mutate := range []func(*pb.CreateTaskRequest){
		func(r *pb.CreateTaskRequest) { r.Authorization = &pb.AuthorizationContext{} },
		func(r *pb.CreateTaskRequest) { r.ParentTaskId = "other-task" },
		func(r *pb.CreateTaskRequest) { r.AuthorityAssignment.ExpiresInSeconds = 0 },
		func(r *pb.CreateTaskRequest) { r.AuthorityAssignment.ExpiresInSeconds = 86401 },
		func(r *pb.CreateTaskRequest) { r.AuthorityAssignment.MaxAccessLevel = 40 },
	} {
		req := good()
		mutate(req)
		if err := validateTaskAuthorityAssignment(user, req); err == nil {
			t.Fatal("invalid assignment accepted", req)
		}
	}
}

func TestTaskLifetimeSurvivesDisconnectButCannotEscapeToService(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	ctx := context.Background()
	user := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	worker := models.Identity{Type: models.PrincipalService, Implementation: "worker", Specifier: "one"}
	expiry := time.Now().UTC().Add(time.Hour)
	root, err := store.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject: user, Delegate: worker, IssuedBy: user, MayDelegate: true, RemainingHops: 3,
		WorkspaceScope: []string{"project-a"}, MaxAccessLevel: 20,
		AudienceType: acl.AuthorityAudienceService, AudienceID: worker.CanonicalPrincipalID(),
		ExpiresAt: expiry, RenewableUntil: expiry, Metadata: map[string]interface{}{acl.AuthorityTaskLifetimeKey: "review-task"},
	})
	if err != nil {
		t.Fatal(err)
	}
	live := true
	audience := acl.GrantAudienceContext{SessionActive: func(uuid.UUID) bool { return false }, TaskActive: func(id string) bool { return live && id == "review-task" }}
	request := acl.RequestAuthorityContext{Subject: user, GrantID: root.GrantID}
	authority, err := store.ResolveAuthority(ctx, worker, request, audience)
	if err != nil {
		t.Fatalf("browser disconnect invalidated task: %v", err)
	}
	forwarded, err := gw.deriveMessageAuthorityContinuation(ctx, authority, "sv::bridge::one", &pb.AuthorityContinuationRequest{
		ScopeMode: pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT, RemainingHops: 2, ExpiresInSeconds: 900,
	}, nil, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	bridge := models.Identity{Type: models.PrincipalService, Implementation: "bridge", Specifier: "one"}
	child, err := store.GetAuthorityGrant(ctx, forwarded.Authorization.GrantId)
	if err != nil {
		t.Fatal(err)
	}
	if child.Metadata[acl.AuthorityTaskLifetimeKey] != "review-task" || child.RemainingHops != 2 || child.ExpiresAt.After(time.Now().Add(901*time.Second)) {
		t.Fatal("lost task lifetime or attenuation", child)
	}
	grandchild, err := store.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject: user, Delegate: bridge, IssuedBy: worker, ParentGrantID: &child.GrantID,
		WorkspaceScope: []string{"project-a"}, MaxAccessLevel: 20, AudienceType: acl.AuthorityAudienceService,
		AudienceID: bridge.CanonicalPrincipalID(), ExpiresAt: child.ExpiresAt, RenewableUntil: child.ExpiresAt,
		Metadata: map[string]interface{}{acl.AuthorityTaskLifetimeKey: "attacker-task"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if grandchild.Metadata[acl.AuthorityTaskLifetimeKey] != "review-task" {
		t.Fatal("child replaced authorized task")
	}
	request.GrantID = grandchild.GrantID
	if _, err = store.ResolveAuthority(ctx, bridge, request, audience); err != nil {
		t.Fatal(err)
	}
	live = false
	if _, err = store.ResolveAuthority(ctx, bridge, request, audience); err == nil {
		t.Fatal("terminal task authority survived")
	}
	live = true
	if _, err = store.ResolveAuthority(ctx, bridge, request, acl.GrantAudienceContext{}); err == nil {
		t.Fatal("missing task store failed open")
	}
	if err = store.RevokeAuthorityGrant(ctx, root.GrantID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ResolveAuthority(ctx, bridge, request, audience); err == nil {
		t.Fatal("revoked root authority survived")
	}
}

func TestContinuationDelegationIsBounded(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	authority, _, _ := createContinuationParent(t, store, 3)
	for _, req := range []*pb.AuthorityContinuationRequest{
		{ScopeMode: pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT, RemainingHops: 3},
		{ScopeMode: pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT, ExpiresInSeconds: 901},
	} {
		if _, err := gw.deriveMessageAuthorityContinuation(context.Background(), authority, "sv::bridge::one", req, nil, uuid.Nil); err == nil {
			t.Fatal("unbounded continuation accepted")
		}
	}
}

type authorityCaptureStore struct {
	taskstore.Store
	authority tasks.TaskAuthorityInfo
}

func (s *authorityCaptureStore) UpdateTaskAuthority(ctx context.Context, id string, a tasks.TaskAuthorityInfo, m map[string]interface{}) error {
	s.authority = a
	return nil
}

func TestUserAssignmentCreatesIndependentPoolGrant(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	capture := &authorityCaptureStore{}
	gw.taskStore = capture
	user := models.Identity{Type: models.PrincipalUser, ID: "alice", Specifier: "browser"}
	metadata, err := gw.establishTaskAuthorityGrant(context.Background(), "review-task", &orchestration.CreateTaskRequest{
		CreatorIdentity: user, Workspace: "project-a", TaskType: "review", AssignmentMode: "pool", RequiredDownstreamAuthorityHops: 3,
	}, nil, nil, &pb.TaskAuthorityAssignment{ExpiresInSeconds: 28800, MaxAccessLevel: 20})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := store.GetAuthorityGrant(context.Background(), metadata["authority_grant_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if grant.ParentGrantID != nil || grant.SubjectID != "alice" || grant.AudienceType != "task" || grant.AudienceID != "review-task" || grant.RemainingHops != 4 || grant.Metadata[acl.AuthorityTaskLifetimeKey] != "review-task" || len(grant.WorkspaceScope) != 1 || grant.WorkspaceScope[0] != "project-a" || !grant.ExpiresAt.Equal(grant.RenewableUntil) {
		t.Fatal("incorrect task root", grant)
	}
	if capture.authority.AuthorityGrantID != grant.GrantID {
		t.Fatal("grant not attached to task")
	}
}
