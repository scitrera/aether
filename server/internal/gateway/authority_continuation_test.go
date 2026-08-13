package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	aclsqlite "github.com/scitrera/aether/server/internal/storage/acl/sqlite"
	"github.com/scitrera/aether/server/pkg/models"
	_ "modernc.org/sqlite"
)

func newAuthorityContinuationHarness(t *testing.T) (*GatewayServer, *aclsqlite.Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "authority-continuation.db")
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", dbPath))
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	store, err := aclsqlite.New(db, nil, nil, "continuation-test")
	if err != nil {
		_ = db.Close()
		t.Fatalf("aclsqlite.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
		_ = db.Close()
	})
	return &GatewayServer{acl: store, gatewayID: "continuation-test"}, store
}

func createContinuationParent(t *testing.T, store *aclsqlite.Store, remainingHops int) (*acl.ResolvedAuthority, models.Identity, models.Identity) {
	t.Helper()
	ctx := context.Background()
	subject := models.Identity{Type: models.PrincipalUser, ID: "alice@example.com"}
	actor := models.Identity{Type: models.PrincipalAgent, Workspace: "project-a", Implementation: "sahara", Specifier: "worker-1"}
	grant, err := store.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:        subject,
		Delegate:       actor,
		IssuedBy:       subject,
		MayDelegate:    remainingHops > 0,
		RemainingHops:  remainingHops,
		WorkspaceScope: []string{"project-a"},
		ResourceScope:  map[string][]string{"tool": {"workspace.*"}},
		OperationScope: []string{"query", "describe", "invoke"},
		MaxAccessLevel: acl.AccessReadWrite,
		AudienceType:   acl.AuthorityAudienceAgent,
		AudienceID:     actor.CanonicalPrincipalID(),
		ExpiresAt:      time.Now().UTC().Add(30 * time.Minute),
		RenewableUntil: time.Now().UTC().Add(2 * time.Hour),
		Reason:         "continuation-test-parent",
	})
	if err != nil {
		t.Fatalf("CreateAuthorityGrant(parent): %v", err)
	}
	resolved, err := store.ResolveAuthority(ctx, actor, acl.RequestAuthorityContext{
		Mode: "on_behalf_of", Subject: subject, GrantID: grant.GrantID,
	}, acl.GrantAudienceContext{Actor: actor})
	if err != nil {
		t.Fatalf("ResolveAuthority(parent): %v", err)
	}
	return resolved, actor, subject
}

func inheritedServiceContinuation() *pb.AuthorityContinuationRequest {
	return &pb.AuthorityContinuationRequest{
		ScopeMode: pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT,
	}
}

func attenuatedContinuation(bindingID, workspace string) *pb.AuthorityContinuationRequest {
	return &pb.AuthorityContinuationRequest{
		ScopeMode: pb.AuthorityContinuationRequest_SCOPE_MODE_ATTENUATE,
		BindingId: bindingID,
		Scope: &pb.AuthorityContinuationScope{
			WorkspaceScope: []string{workspace},
			ResourceScope: []*pb.ACLAuthorityGrantResourceScopeEntry{
				{ResourceType: "tool", Patterns: []string{"workspace.*"}},
			},
			OperationScope: []string{"query"},
			MaxAccessLevel: int32(acl.AccessRead),
		},
	}
}

func allowedContinuationReceipt(bindingID, workspace string) *pb.AccessDecisionReceipt {
	return &pb.AccessDecisionReceipt{
		DecisionId: "decision-" + bindingID,
		Allowed:    true,
		Request: &pb.ResourceAccessRequest{
			ResourceType: "tool-catalog/entry", ResourceId: "provider/tool",
			Operation: "tool.invoke.read", Workspace: workspace,
			RequiredAccessLevel: int32(acl.AccessRead), CorrelationId: bindingID,
		},
	}
}

func TestDeriveMessageAuthorityContinuation_BindsLeafAndReuses(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	authority, _, subject := createContinuationParent(t, store, 1)
	ctx := context.Background()
	target := "sv::tool-catalog::catalog-7"

	forwarded, err := gw.deriveMessageAuthorityContinuation(
		ctx, authority, target, inheritedServiceContinuation(), nil, uuid.New(),
	)
	if err != nil {
		t.Fatalf("deriveMessageAuthorityContinuation: %v", err)
	}
	if forwarded.GetDeliveryTarget() != target {
		t.Fatalf("delivery target = %q, want %q", forwarded.GetDeliveryTarget(), target)
	}
	if forwarded.GetAuthorization().GetGrantId() == "" || forwarded.GetAuthorization().GetAuthorityMode() != "on_behalf_of" {
		t.Fatalf("invalid forwarded authorization: %+v", forwarded.GetAuthorization())
	}
	if forwarded.GetAuthorization().GetSubject().GetPrincipalId() != subject.CanonicalPrincipalID() {
		t.Fatalf("forwarded subject = %q, want %q", forwarded.GetAuthorization().GetSubject().GetPrincipalId(), subject.CanonicalPrincipalID())
	}

	child, err := store.GetAuthorityGrant(ctx, forwarded.GetAuthorization().GetGrantId())
	if err != nil {
		t.Fatalf("GetAuthorityGrant(child): %v", err)
	}
	if child.ParentGrantID == nil || *child.ParentGrantID != authority.Grant.GrantID {
		t.Fatalf("child parent = %v, want %q", child.ParentGrantID, authority.Grant.GrantID)
	}
	if child.MayDelegate || child.RemainingHops != 0 {
		t.Fatalf("child must be a non-delegable leaf: may_delegate=%v remaining_hops=%d", child.MayDelegate, child.RemainingHops)
	}
	if child.AudienceType != acl.AuthorityAudienceService || child.AudienceID != target {
		t.Fatalf("child audience = %s/%s, want service/%s", child.AudienceType, child.AudienceID, target)
	}
	if child.ExpiresAt.After(time.Now().UTC().Add(messageAuthorityContinuationTTL + 5*time.Second)) {
		t.Fatalf("child expiry %s exceeds continuation TTL", child.ExpiresAt)
	}
	if child.MaxAccessLevel != authority.Grant.MaxAccessLevel ||
		!resourceScopesEqual(child.ResourceScope, authority.Grant.ResourceScope) {
		t.Fatalf("child scope did not preserve parent attenuation")
	}

	service, parseErr := models.ParseIdentity(target)
	if parseErr != nil {
		t.Fatalf("ParseIdentity(target): %v", parseErr)
	}
	resolved, err := store.ResolveAuthority(ctx, service, acl.RequestAuthorityContext{
		Mode:    "on_behalf_of",
		Subject: subject,
		GrantID: child.GrantID,
	}, acl.GrantAudienceContext{Actor: service})
	if err != nil || resolved == nil {
		t.Fatalf("service ResolveAuthority(child) = %+v, %v", resolved, err)
	}

	reused, err := gw.deriveMessageAuthorityContinuation(
		ctx, authority, target, inheritedServiceContinuation(), nil, uuid.New(),
	)
	if err != nil {
		t.Fatalf("deriveMessageAuthorityContinuation(reuse): %v", err)
	}
	if reused.GetAuthorization().GetGrantId() != child.GrantID {
		t.Fatalf("reused grant = %q, want %q", reused.GetAuthorization().GetGrantId(), child.GrantID)
	}
}

func TestDeriveMessageAuthorityContinuation_BindsAttenuatedAgentPerInvocation(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	authority, _, subject := createContinuationParent(t, store, 1)
	ctx := context.Background()
	target := "ag::project-a::tool-host::one"
	bindingID := "tool-call-1"

	forwarded, err := gw.deriveMessageAuthorityContinuation(
		ctx, authority, target, attenuatedContinuation(bindingID, "project-a"),
		allowedContinuationReceipt(bindingID, "project-a"), uuid.New(),
	)
	if err != nil {
		t.Fatalf("derive agent continuation: %v", err)
	}
	if forwarded.GetDeliveryTarget() != target || forwarded.GetBindingId() != bindingID {
		t.Fatalf("forwarded binding = target:%q binding:%q", forwarded.GetDeliveryTarget(), forwarded.GetBindingId())
	}
	if forwarded.GetScope().GetMaxAccessLevel() != int32(acl.AccessRead) ||
		!slices.Equal(forwarded.GetScope().GetOperationScope(), []string{"query"}) {
		t.Fatalf("forwarded scope = %+v", forwarded.GetScope())
	}

	child, err := store.GetAuthorityGrant(ctx, forwarded.GetAuthorization().GetGrantId())
	if err != nil {
		t.Fatalf("GetAuthorityGrant(agent child): %v", err)
	}
	if child.AudienceType != acl.AuthorityAudienceAgent || child.AudienceID != target {
		t.Fatalf("child audience = %s/%s, want agent/%s", child.AudienceType, child.AudienceID, target)
	}
	if child.MayDelegate || child.RemainingHops != 0 || child.MaxAccessLevel != acl.AccessRead {
		t.Fatalf("unsafe agent child: %+v", child)
	}
	agent, parseErr := models.ParseIdentity(target)
	if parseErr != nil {
		t.Fatalf("ParseIdentity(agent): %v", parseErr)
	}
	resolved, err := store.ResolveAuthority(ctx, agent, acl.RequestAuthorityContext{
		Mode: "on_behalf_of", Subject: subject, GrantID: child.GrantID,
	}, acl.GrantAudienceContext{Actor: agent})
	if err != nil || resolved == nil {
		t.Fatalf("agent ResolveAuthority(child) = %+v, %v", resolved, err)
	}

	second, err := gw.deriveMessageAuthorityContinuation(
		ctx, authority, target, attenuatedContinuation(bindingID, "project-a"),
		allowedContinuationReceipt(bindingID, "project-a"), uuid.New(),
	)
	if err != nil {
		t.Fatalf("derive second agent continuation: %v", err)
	}
	if second.GetAuthorization().GetGrantId() == child.GrantID {
		t.Fatal("agent continuation was reused across invocations")
	}
}

func TestDeriveMessageAuthorityContinuation_RejectsUnsafeInputsAndCascadesRevocation(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	authority, _, _ := createContinuationParent(t, store, 1)
	ctx := context.Background()

	if _, err := gw.deriveMessageAuthorityContinuation(ctx, nil, "sv::tool-catalog::one", inheritedServiceContinuation(), nil, uuid.Nil); err == nil {
		t.Fatal("expected missing authority to fail")
	}
	if _, err := gw.deriveMessageAuthorityContinuation(ctx, authority, "sv::tool-catalog", inheritedServiceContinuation(), nil, uuid.Nil); err == nil {
		t.Fatal("expected wildcard service target to fail")
	}
	if _, err := gw.deriveMessageAuthorityContinuation(ctx, authority, "ag::project-a::worker::one", inheritedServiceContinuation(), nil, uuid.Nil); err == nil {
		t.Fatal("expected inherited agent target to fail")
	}

	forwarded, err := gw.deriveMessageAuthorityContinuation(ctx, authority, "sv::tool-catalog::one", inheritedServiceContinuation(), nil, uuid.Nil)
	if err != nil {
		t.Fatalf("deriveMessageAuthorityContinuation: %v", err)
	}
	if err := store.RevokeAuthorityGrant(ctx, authority.Grant.GrantID); err != nil {
		t.Fatalf("RevokeAuthorityGrant(parent): %v", err)
	}
	child, err := store.GetAuthorityGrant(ctx, forwarded.GetAuthorization().GetGrantId())
	if err != nil {
		t.Fatalf("GetAuthorityGrant(child): %v", err)
	}
	if err := child.ValidateActiveAt(time.Now()); !errors.Is(err, acl.ErrAuthorityGrantRevoked) {
		t.Fatalf("child active after parent revocation: %v", err)
	}

	noHop, _, _ := createContinuationParent(t, store, 0)
	if _, err := gw.deriveMessageAuthorityContinuation(ctx, noHop, "sv::tool-catalog::two", inheritedServiceContinuation(), nil, uuid.Nil); !errors.Is(err, acl.ErrAuthorityGrantDelegationDenied) {
		t.Fatalf("no-hop error = %v, want delegation denied", err)
	}
}

func TestDeriveMessageAuthorityContinuation_RejectsUnboundOrEscalatedAgentScope(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	authority, _, _ := createContinuationParent(t, store, 1)
	ctx := context.Background()
	target := "ag::project-a::tool-host::one"
	bindingID := "tool-call-1"

	request := attenuatedContinuation(bindingID, "project-a")
	if _, err := gw.deriveMessageAuthorityContinuation(ctx, authority, target, request, nil, uuid.Nil); err == nil {
		t.Fatal("expected missing checked receipt to fail")
	}
	wrongCorrelation := allowedContinuationReceipt("another-call", "project-a")
	if _, err := gw.deriveMessageAuthorityContinuation(ctx, authority, target, request, wrongCorrelation, uuid.Nil); err == nil {
		t.Fatal("expected mismatched correlation to fail")
	}
	wrongWorkspace := allowedContinuationReceipt(bindingID, "project-b")
	if _, err := gw.deriveMessageAuthorityContinuation(ctx, authority, target, request, wrongWorkspace, uuid.Nil); err == nil {
		t.Fatal("expected mismatched workspace to fail")
	}
	escalated := attenuatedContinuation(bindingID, "project-a")
	escalated.Scope.MaxAccessLevel = int32(acl.AccessManage)
	if _, err := gw.deriveMessageAuthorityContinuation(ctx, authority, target, escalated, allowedContinuationReceipt(bindingID, "project-a"), uuid.Nil); !errors.Is(err, acl.ErrAuthorityGrantScopeEscalation) {
		t.Fatalf("scope escalation error = %v, want scope escalation", err)
	}
}

func TestCreateTaskAuthorityGrant_RequiresDeclaredDownstreamBudget(t *testing.T) {
	gw, store := newAuthorityContinuationHarness(t)
	ctx := context.Background()
	worker := models.Identity{Type: models.PrincipalAgent, Workspace: "project-a", Implementation: "sahara", Specifier: "worker-2"}

	oneHop, actor, _ := createContinuationParent(t, store, 1)
	if _, err := gw.createTaskAuthorityGrant(
		ctx, oneHop, actor, worker,
		acl.AuthorityAudienceAgent, worker.CanonicalPrincipalID(),
		"task-one", "chat", "targeted", 1,
	); err == nil {
		t.Fatal("expected task grant to reject a parent that cannot leave the requested downstream hop")
	}

	twoHops, actor, _ := createContinuationParent(t, store, 2)
	grant, err := gw.createTaskAuthorityGrant(
		ctx, twoHops, actor, worker,
		acl.AuthorityAudienceAgent, worker.CanonicalPrincipalID(),
		"task-two", "chat", "targeted", 1,
	)
	if err != nil {
		t.Fatalf("createTaskAuthorityGrant with sufficient budget: %v", err)
	}
	if !grant.MayDelegate || grant.RemainingHops != 1 {
		t.Fatalf("task grant budget = may_delegate:%v remaining:%d, want true/1", grant.MayDelegate, grant.RemainingHops)
	}
}
