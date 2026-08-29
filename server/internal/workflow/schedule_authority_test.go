package workflow

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

func TestScheduleAuthorityFromOperationFailsClosed(t *testing.T) {
	action, _ := json.Marshal(ActionDef{
		Type: "create_task", TaskType: "scheduled", RequireTaskAuthority: true,
		RequiredDownstreamAuthorityHops: 1,
	})
	schedule := &Schedule{ID: "sched-a", Workspace: "ws-a", Action: action}

	if _, err := scheduleAuthorityFromOperation(&pb.WorkflowOperation{}, schedule); err == nil ||
		!strings.Contains(err.Error(), "requires task authority") {
		t.Fatalf("missing authority error = %v", err)
	}

	op := &pb.WorkflowOperation{
		ScheduleAuthorityScope: &pb.WorkflowScheduleAuthorityScope{RequiredTaskAuthorityHops: 1, PolicyVersion: 1},
		RequestContext: &pb.WorkflowRequestContext{
			ScheduleAuthorization: &pb.AuthorizationContext{
				AuthorityMode: "on_behalf_of",
				Subject:       &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "user-a"},
				GrantId:       "private-schedule-grant",
			},
			RootGrantId: "root-a", SourceGrantId: "source-a",
			ExpiresAtMs: time.Now().Add(time.Hour).UnixMilli(), PolicyDigest: "sha256:test", PolicyVersion: 1,
			LifetimeMode: pb.WorkflowAuthorityLifetimeMode_WORKFLOW_AUTHORITY_LIFETIME_DURABLE,
		},
	}
	authority, err := scheduleAuthorityFromOperation(op, schedule)
	if err != nil {
		t.Fatalf("scheduleAuthorityFromOperation: %v", err)
	}
	if authority.Authorization.GetGrantId() != "private-schedule-grant" || authority.PolicyVersion != 1 ||
		authority.RootGrantID != "root-a" || authority.SourceGrantID != "source-a" {
		t.Fatalf("authority = %+v", authority)
	}

	op.RequestContext.PolicyVersion = 2
	if _, err := scheduleAuthorityFromOperation(op, schedule); err == nil || !strings.Contains(err.Error(), "policy version") {
		t.Fatalf("unknown policy version error = %v", err)
	}
}

func TestScheduleAuthorityNeverMarshalsWithPublicSchedule(t *testing.T) {
	schedule := Schedule{
		ID: "sched-a", Workspace: "ws-a",
		Authority: &ScheduleAuthority{
			Authorization: &pb.AuthorizationContext{GrantId: "secret-grant"},
			PolicyDigest:  "secret-digest", PolicyVersion: 1,
		},
	}
	data, err := json.Marshal(schedule)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-grant") || strings.Contains(string(data), "secret-digest") {
		t.Fatalf("private authority leaked: %s", data)
	}
}
