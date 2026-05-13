package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// resolveFirstPendingToken drains the request queue and resolves the first
// pending token request with the given response.
func resolveFirstPendingToken(client *BaseClient, resp *TokenResponse) {
	time.Sleep(10 * time.Millisecond)
	<-client.RequestQueue()
	client.pendingTokenRequests.Range(func(key, val any) bool {
		ch := val.(chan *TokenResponse)
		client.pendingTokenRequests.Delete(key)
		ch <- resp
		return false
	})
}

// =============================================================================
// TokenOps Tests
// =============================================================================

func TestTokenOps_List(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingToken(client, &TokenResponse{Success: true, TotalCount: 3})

	ctx := context.Background()
	resp, err := client.Tokens().List(ctx)
	if err != nil {
		t.Errorf("TokenOps.List() error = %v", err)
	}
	if resp == nil {
		t.Fatal("List() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if resp.TotalCount != 3 {
		t.Errorf("TotalCount = %d, want 3", resp.TotalCount)
	}
}

func TestTokenOps_List_QueuesCorrectOp(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.Tokens().SendOp(&pb.TokenOperation{
		Op: pb.TokenOperation_LIST,
	})
	if err != nil {
		t.Errorf("TokenOps.SendOp() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		tokenOp := msg.GetTokenOp()
		if tokenOp == nil {
			t.Fatal("Expected TokenOperation in message")
		}
		if tokenOp.Op != pb.TokenOperation_LIST {
			t.Errorf("Op = %v, want LIST", tokenOp.Op)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestTokenOps_Get(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		tokenOp := msg.GetTokenOp()
		if tokenOp == nil || tokenOp.Op != pb.TokenOperation_GET {
			return
		}
		client.pendingTokenRequests.Range(func(key, val any) bool {
			ch := val.(chan *TokenResponse)
			client.pendingTokenRequests.Delete(key)
			ch <- &TokenResponse{
				Success: true,
				Token:   &TokenInfo{ID: tokenOp.TokenId, Name: "test-token"},
			}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.Tokens().Get(ctx, "tok-123")
	if err != nil {
		t.Errorf("TokenOps.Get() error = %v", err)
	}
	if resp == nil || resp.Token == nil {
		t.Fatal("Get() response/token should not be nil")
	}
	if resp.Token.ID != "tok-123" {
		t.Errorf("Token.ID = %q, want %q", resp.Token.ID, "tok-123")
	}
}

func TestTokenOps_Create(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		tokenOp := msg.GetTokenOp()
		if tokenOp == nil || tokenOp.Op != pb.TokenOperation_CREATE {
			return
		}
		client.pendingTokenRequests.Range(func(key, val any) bool {
			ch := val.(chan *TokenResponse)
			client.pendingTokenRequests.Delete(key)
			ch <- &TokenResponse{
				Success:        true,
				PlaintextToken: "tok-plaintext-abc",
				CreatedToken:   &TokenInfo{ID: "tok-new", Name: tokenOp.CreateRequest.GetName()},
			}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.Tokens().Create(ctx, "my-token", "agent", []string{"*"}, []string{"connect"}, 24, "admin")
	if err != nil {
		t.Errorf("TokenOps.Create() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Create() response should be successful")
	}
	if resp.PlaintextToken != "tok-plaintext-abc" {
		t.Errorf("PlaintextToken = %q, want %q", resp.PlaintextToken, "tok-plaintext-abc")
	}
	if resp.CreatedToken == nil || resp.CreatedToken.Name != "my-token" {
		t.Error("Create() should return the created token info")
	}
}

func TestTokenOps_Delete(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		tokenOp := msg.GetTokenOp()
		if tokenOp == nil || tokenOp.Op != pb.TokenOperation_DELETE {
			return
		}
		if tokenOp.TokenId != "tok-del" {
			return
		}
		client.pendingTokenRequests.Range(func(key, val any) bool {
			ch := val.(chan *TokenResponse)
			client.pendingTokenRequests.Delete(key)
			ch <- &TokenResponse{Success: true, Message: "token deleted"}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.Tokens().Delete(ctx, "tok-del")
	if err != nil {
		t.Errorf("TokenOps.Delete() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Delete() response should be successful")
	}
}

func TestTokenOps_Revoke(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		tokenOp := msg.GetTokenOp()
		if tokenOp == nil || tokenOp.Op != pb.TokenOperation_REVOKE {
			return
		}
		if tokenOp.TokenId != "tok-rev" {
			return
		}
		client.pendingTokenRequests.Range(func(key, val any) bool {
			ch := val.(chan *TokenResponse)
			client.pendingTokenRequests.Delete(key)
			ch <- &TokenResponse{Success: true, Message: "token revoked"}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.Tokens().Revoke(ctx, "tok-rev")
	if err != nil {
		t.Errorf("TokenOps.Revoke() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Revoke() response should be successful")
	}
}

// =============================================================================
// Dispatch Test
// =============================================================================

func TestBaseClient_DispatchResponse_TokenResponse(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	var received *TokenResponse
	client.OnTokenResponse(func(ctx context.Context, resp *TokenResponse) error {
		received = resp
		return nil
	})

	ctx := context.Background()
	protoResp := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Token{
			Token: &pb.TokenResponse{
				Success:    true,
				TotalCount: 2,
				Tokens: []*pb.TokenInfo{
					{Id: "tok-1", Name: "first"},
					{Id: "tok-2", Name: "second"},
				},
			},
		},
	}

	if err := client.dispatchResponse(ctx, protoResp); err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}
	if received == nil {
		t.Fatal("OnTokenResponse handler was not called")
	}
	if !received.Success {
		t.Error("Response should be successful")
	}
	if received.TotalCount != 2 {
		t.Errorf("TotalCount = %d, want 2", received.TotalCount)
	}
	if len(received.Tokens) != 2 {
		t.Errorf("len(Tokens) = %d, want 2", len(received.Tokens))
	}
	if received.Tokens[0].ID != "tok-1" {
		t.Errorf("Tokens[0].ID = %q, want %q", received.Tokens[0].ID, "tok-1")
	}
}
