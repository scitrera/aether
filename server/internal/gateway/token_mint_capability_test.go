package gateway

// Tests for the scoped user-token mint capability enforced in handleTokenOp's
// CREATE branch:
//
//   - non-user token mint (agent/service/task) still works under the existing
//     admin/tokens gate (no extra capability required).
//   - user token mint by a non-system principal with NO ACL is denied (the new
//     capability/mint_user_tokens + admin/* gate fails closed) and never
//     reaches the admin provider.
//   - user token mint by a system principal (orchestrator) is allowed via the
//     admin/* umbrella that isAllowedAdminOpQuiet grants system principals.
//   - user token mint with an empty created_by is rejected (created_by is the
//     impersonated user id and must not silently default to "admin").

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/pkg/models"
)

// fakeTokenProvider embeds admin.StateProvider (so the large interface is
// satisfied) and records CreateToken calls. Any non-overridden method panics
// if the code under test calls it, surfacing unexpected paths.
type fakeTokenProvider struct {
	admin.StateProvider
	createCalls []*admin.CreateTokenRequest
}

func (f *fakeTokenProvider) CreateToken(_ context.Context, req *admin.CreateTokenRequest) (*admin.CreateTokenResult, error) {
	f.createCalls = append(f.createCalls, req)
	return &admin.CreateTokenResult{
		PlaintextToken: "plaintext",
		Token: &admin.TokenInfo{
			ID:            "tok-new",
			Name:          req.Name,
			PrincipalType: req.PrincipalType,
			CreatedBy:     req.CreatedBy,
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
		},
	}, nil
}

func newTokenMintServer(provider admin.StateProvider) *GatewayServer {
	s := newAdminTestServer()
	s.adminProvider = provider
	return s
}

func createTokenOp(principalType, createdBy string) *pb.TokenOperation {
	return &pb.TokenOperation{
		Op:        pb.TokenOperation_CREATE,
		RequestId: "req-1",
		CreateRequest: &pb.TokenCreateRequest{
			Name:          "test-token",
			PrincipalType: principalType,
			CreatedBy:     createdBy,
		},
	}
}

func TestHandleTokenOp_NonUserTokenMint_Succeeds(t *testing.T) {
	provider := &fakeTokenProvider{}
	s := newTokenMintServer(provider)
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalOrchestrator)
	caller := client.Identity

	s.handleTokenOp(context.Background(), client, caller, createTokenOp("Agent", "orchestrator"))

	if len(provider.createCalls) != 1 {
		t.Fatalf("expected CreateToken to be called once for agent token, got %d calls", len(provider.createCalls))
	}
	if provider.createCalls[0].PrincipalType != "Agent" {
		t.Errorf("expected PrincipalType=Agent, got %q", provider.createCalls[0].PrincipalType)
	}
}

func TestHandleTokenOp_UserTokenMint_NoCapability_Denied(t *testing.T) {
	provider := &fakeTokenProvider{}
	s := newTokenMintServer(provider) // s.acl is nil → capability + admin/* both fail
	stream := &mockStream{}
	// Non-system principal (Agent) so isAllowedAdminOpQuiet(...,"*") returns false.
	client := newAdminTestClient(stream, models.PrincipalAgent)
	caller := client.Identity

	s.handleTokenOp(context.Background(), client, caller, createTokenOp("User", "drew"))

	if len(provider.createCalls) != 0 {
		t.Fatalf("expected user-token mint to be DENIED before reaching provider, got %d CreateToken calls", len(provider.createCalls))
	}
	if stream.sentCount() == 0 {
		t.Fatal("expected an error response to be sent for denied user-token mint")
	}
	stream.mu.Lock()
	tokenResp := stream.sent[0].GetToken()
	stream.mu.Unlock()
	if tokenResp == nil || tokenResp.Success {
		t.Fatalf("expected failed TokenResponse, got %v", tokenResp)
	}
	if tokenResp.Error == "" {
		t.Error("expected non-empty error message on denial")
	}
}

func TestHandleTokenOp_UserTokenMint_SystemPrincipal_Allowed(t *testing.T) {
	provider := &fakeTokenProvider{}
	s := newTokenMintServer(provider)
	stream := &mockStream{}
	// Orchestrator is a system principal: isAllowedAdminOpQuiet(...,"*") → true,
	// so the user-token gate passes even with nil ACL.
	client := newAdminTestClient(stream, models.PrincipalOrchestrator)
	caller := client.Identity

	s.handleTokenOp(context.Background(), client, caller, createTokenOp("User", "drew"))

	if len(provider.createCalls) != 1 {
		t.Fatalf("expected user-token mint to succeed for system principal, got %d CreateToken calls", len(provider.createCalls))
	}
	if provider.createCalls[0].CreatedBy != "drew" {
		t.Errorf("expected CreatedBy=drew recorded server-side, got %q", provider.createCalls[0].CreatedBy)
	}
}

func TestHandleTokenOp_UserTokenMint_EmptyCreatedBy_Rejected(t *testing.T) {
	provider := &fakeTokenProvider{}
	s := newTokenMintServer(provider)
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalOrchestrator)
	caller := client.Identity

	// System principal passes the capability gate, but an empty created_by must
	// still be rejected: it is the impersonated user id.
	s.handleTokenOp(context.Background(), client, caller, createTokenOp("User", ""))

	if len(provider.createCalls) != 0 {
		t.Fatalf("expected user-token mint with empty created_by to be rejected, got %d CreateToken calls", len(provider.createCalls))
	}
	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for empty created_by")
	}
}
