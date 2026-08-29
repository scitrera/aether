package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/pkg/models"
)

func TestWorkflowScheduleAccessRequiresCanonicalExactIdentity(t *testing.T) {
	data := []byte(`{"id":"sched/a","workspace":"workspace a"}`)
	access, scheduleOp, err := workflowScheduleAccessForOperation(&pb.WorkflowOperation{
		Op: pb.WorkflowOperation_UPSERT_SCHEDULE, Id: "sched/a", Workspace: "workspace a", Data: data,
	})
	if err != nil {
		t.Fatalf("workflowScheduleAccessForOperation: %v", err)
	}
	if !scheduleOp || !access.manage || access.workspace != "workspace a" ||
		access.resourceID != "workspaces/workspace%20a/schedules/sched%2Fa" {
		t.Fatalf("unexpected schedule access: %+v schedule=%v", access, scheduleOp)
	}

	for name, op := range map[string]*pb.WorkflowOperation{
		"wildcard workspace": {Op: pb.WorkflowOperation_LIST_SCHEDULES, Workspace: "*"},
		"missing id":         {Op: pb.WorkflowOperation_DELETE_SCHEDULE, Workspace: "workspace a"},
		"json id mismatch": {
			Op: pb.WorkflowOperation_CREATE_SCHEDULE, Id: "other", Workspace: "workspace a", Data: data,
		},
		"json workspace mismatch": {
			Op: pb.WorkflowOperation_CREATE_SCHEDULE, Id: "sched/a", Workspace: "other", Data: data,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, isSchedule, err := workflowScheduleAccessForOperation(op); !isSchedule || err == nil {
				t.Fatalf("got isSchedule=%v err=%v, want schedule validation error", isSchedule, err)
			}
		})
	}
}

func TestPrepareWorkflowOperationReplacesForgedRequestContext(t *testing.T) {
	client := &ClientSession{
		Identity:    models.Identity{Type: models.PrincipalUser, ID: "user-a"},
		SessionUUID: uuid.New(),
	}
	incoming := &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_CREATE_RULE,
		RequestContext: &pb.WorkflowRequestContext{
			ScheduleAuthorization: &pb.AuthorizationContext{GrantId: "forged"},
			RootGrantId:           "forged-root", PolicyDigest: "forged-digest",
		},
	}
	forwarded, grant, err := (&GatewayServer{}).prepareWorkflowOperation(context.Background(), client, incoming)
	if err != nil {
		t.Fatalf("prepareWorkflowOperation: %v", err)
	}
	if grant != nil || forwarded.GetRequestContext() == nil {
		t.Fatalf("grant=%v context=%v", grant, forwarded.GetRequestContext())
	}
	trusted := forwarded.GetRequestContext()
	if trusted.GetScheduleAuthorization() != nil || trusted.GetRootGrantId() != "" || trusted.GetPolicyDigest() != "" ||
		trusted.GetActor().GetPrincipalId() != "user-a" || trusted.GetActorSessionId() != client.SessionUUID.String() {
		t.Fatalf("forged context survived: %+v", trusted)
	}
	if incoming.GetRequestContext().GetScheduleAuthorization().GetGrantId() != "forged" {
		t.Fatal("prepareWorkflowOperation mutated the caller's protobuf")
	}
}

func TestWorkflowAuthorityPolicyDigestIsDeterministicAndVersionSensitive(t *testing.T) {
	scope := &pb.WorkflowScheduleAuthorityScope{
		WorkspaceScope: []string{"ws-a"}, OperationScope: []string{"task_create"},
		MaxAccessLevel: 20, PolicyVersion: 1,
	}
	first, err := workflowAuthorityPolicyDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	second, err := workflowAuthorityPolicyDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || first != second {
		t.Fatalf("digest is not deterministic: first=%q second=%q", first, second)
	}
	scope.PolicyVersion = 2
	changed, err := workflowAuthorityPolicyDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatalf("policy version did not affect digest: %q", first)
	}
}

func TestWorkflowScheduleGrantRemainingHopsAccountsForPoolSelection(t *testing.T) {
	scope := &pb.WorkflowScheduleAuthorityScope{RequiredTaskAuthorityHops: 1}
	if got := workflowScheduleGrantRemainingHops(workflowScheduleAccess{targeted: true}, scope); got != 2 {
		t.Fatalf("targeted schedule remaining hops = %d, want 2", got)
	}
	if got := workflowScheduleGrantRemainingHops(workflowScheduleAccess{targeted: false}, scope); got != 3 {
		t.Fatalf("pool schedule remaining hops = %d, want 3", got)
	}
}

func TestIntersectWorkflowScheduleSourceLifetime(t *testing.T) {
	now := time.Now().UTC()
	parent := &acl.AuthorityGrant{
		ExpiresAt:      now.Add(time.Hour),
		RenewableUntil: now.Add(2 * time.Hour),
	}
	request := acl.CreateAuthorityGrantRequest{
		ExpiresAt:      now.Add(24 * time.Hour),
		RenewableUntil: now.Add(48 * time.Hour),
	}

	intersectWorkflowScheduleSourceLifetime(&request, parent)
	if !request.ExpiresAt.Equal(parent.ExpiresAt) || !request.RenewableUntil.Equal(parent.RenewableUntil) {
		t.Fatalf("lifetime was not intersected with source: expires=%v renewable=%v", request.ExpiresAt, request.RenewableUntil)
	}

	shorter := acl.CreateAuthorityGrantRequest{
		ExpiresAt:      now.Add(5 * time.Minute),
		RenewableUntil: now.Add(10 * time.Minute),
	}
	intersectWorkflowScheduleSourceLifetime(&shorter, parent)
	if !shorter.ExpiresAt.Equal(now.Add(5*time.Minute)) || !shorter.RenewableUntil.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("shorter requested lifetime changed: expires=%v renewable=%v", shorter.ExpiresAt, shorter.RenewableUntil)
	}
}
