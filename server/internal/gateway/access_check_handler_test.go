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

type fakeRuntimeAccessChecker struct {
	directCalls    int
	authorityCalls int
	decision       *acl.ACLDecision
	err            error
}

func (f *fakeRuntimeAccessChecker) CheckAccess(context.Context, models.Identity, string, string, string, string, uuid.UUID, int) (*acl.ACLDecision, error) {
	f.directCalls++
	return f.decision, f.err
}

func (f *fakeRuntimeAccessChecker) CheckAccessWithAuthority(context.Context, models.Identity, *acl.ResolvedAuthority, string, string, string, string, uuid.UUID, int) (*acl.ACLDecision, error) {
	f.authorityCalls++
	return f.decision, f.err
}

func validAccessRequest() *pb.ResourceAccessRequest {
	return &pb.ResourceAccessRequest{
		ResourceType:        models.ResourceTypeToolCatalogEntry,
		ResourceId:          "provider-1/tool-1",
		Operation:           "invoke",
		Workspace:           "workspace-1",
		RequiredAccessLevel: int32(acl.AccessReadWrite),
		CorrelationId:       "call-1",
	}
}

func TestEvaluateResourceAccessDirect(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	checker := &fakeRuntimeAccessChecker{decision: &acl.ACLDecision{
		Allowed:              true,
		Decision:             acl.DecisionAllow,
		EffectiveAccessLevel: acl.AccessManage,
	}}
	actor := models.Identity{Type: models.PrincipalAgent, Workspace: "workspace-1", Implementation: "harness", Specifier: "one"}

	receipt, err := evaluateResourceAccess(context.Background(), checker, actor, nil, uuid.New(), validAccessRequest(), "sv::tools::one", now)
	if err != nil {
		t.Fatalf("evaluateResourceAccess() error = %v", err)
	}
	if !receipt.GetAllowed() || receipt.GetAuthorityMode() != "direct" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if receipt.GetDeliveryTarget() != "sv::tools::one" {
		t.Fatalf("delivery_target = %q", receipt.GetDeliveryTarget())
	}
	if got, want := receipt.GetExpiresAtMs(), now.Add(defaultReceiptTTL).UnixMilli(); got != want {
		t.Fatalf("expires_at_ms = %d, want %d", got, want)
	}
	if checker.directCalls != 1 || checker.authorityCalls != 0 {
		t.Fatalf("checker calls direct=%d authority=%d", checker.directCalls, checker.authorityCalls)
	}
}

func TestEvaluateResourceAccessOBOClampsExpiryAndDoesNotFallback(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	grantExpiry := now.Add(7 * time.Second)
	checker := &fakeRuntimeAccessChecker{decision: &acl.ACLDecision{
		Allowed:              false,
		Decision:             acl.DecisionDeny,
		EffectiveAccessLevel: acl.AccessRead,
	}}
	actor := models.Identity{Type: models.PrincipalService, Implementation: "harness", Specifier: "one"}
	subject := models.Identity{Type: models.PrincipalUser, ID: "user-1", Specifier: "window-1"}
	authority := &acl.ResolvedAuthority{
		Actor:   actor,
		Subject: subject,
		Grant: &acl.AuthorityGrant{
			GrantID:         "grant-1",
			RootGrantID:     "root-1",
			RootSubjectType: "user",
			RootSubjectID:   "user-1",
			ExpiresAt:       grantExpiry,
		},
	}

	receipt, err := evaluateResourceAccess(context.Background(), checker, actor, authority, uuid.New(), validAccessRequest(), "", now)
	if err != nil {
		t.Fatalf("evaluateResourceAccess() error = %v", err)
	}
	if receipt.GetAllowed() || receipt.GetDenialCode() != "access_denied" {
		t.Fatalf("unexpected denial receipt: %+v", receipt)
	}
	if receipt.GetAuthorityMode() != "on_behalf_of" || receipt.GetGrantId() != "grant-1" || receipt.GetRootGrantId() != "root-1" {
		t.Fatalf("missing authority lineage: %+v", receipt)
	}
	if receipt.GetExpiresAtMs() != grantExpiry.UnixMilli() {
		t.Fatalf("expires_at_ms = %d, want grant expiry %d", receipt.GetExpiresAtMs(), grantExpiry.UnixMilli())
	}
	if checker.directCalls != 0 || checker.authorityCalls != 1 {
		t.Fatalf("OBO check fell back: direct=%d authority=%d", checker.directCalls, checker.authorityCalls)
	}
}

func TestValidateResourceAccessRequestRejectsNonCanonicalAndZeroAccess(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*pb.ResourceAccessRequest)
	}{
		{"surrounding whitespace", func(req *pb.ResourceAccessRequest) { req.ResourceId = " tool " }},
		{"missing correlation", func(req *pb.ResourceAccessRequest) { req.CorrelationId = "" }},
		{"zero access", func(req *pb.ResourceAccessRequest) { req.RequiredAccessLevel = 0 }},
		{"unknown access", func(req *pb.ResourceAccessRequest) { req.RequiredAccessLevel = 15 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validAccessRequest()
			tt.mutate(req)
			if err := validateResourceAccessRequest(req); err == nil {
				t.Fatal("validateResourceAccessRequest() error = nil")
			}
		})
	}
}

func TestReceiptTTLAllowsLongerViewBind(t *testing.T) {
	req := validAccessRequest()
	req.ResourceType = models.ResourceTypeWorkspaceExecutionView
	req.Operation = "bind"
	if got := receiptTTL(req); got != viewBindReceiptTTL {
		t.Fatalf("receiptTTL() = %s, want %s", got, viewBindReceiptTTL)
	}
}
