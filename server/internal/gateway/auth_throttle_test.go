package gateway

// Tests for the per-identity auth-failure throttle in authenticateCredentials.
//
// The throttle is defense-in-depth against stale/zombie clients that reconnect
// in a tight loop presenting a token the gateway no longer knows: after
// authFailThreshold failures within authFailWindow, the identity is fast-
// rejected for authFailCooldown WITHOUT re-calling ValidateToken. A successful
// validation clears the bookkeeping; cooldown expiry allows re-validation.

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/state"
	"github.com/scitrera/aether/server/pkg/models"
)

// throttleMockTokenStore is a state.TokenStore whose ValidateToken behavior is
// programmable and call-counted, so tests can assert when (and whether) the
// gateway actually re-validates.
type throttleMockTokenStore struct {
	validateCalls int
	// validateFunc, if set, produces the result for each ValidateToken call.
	validateFunc func() (*state.TaskAuthToken, error)
}

func (m *throttleMockTokenStore) GenerateToken(_ context.Context, taskID, targetIdentity, workspace, orchestratorID string) (*state.TaskAuthToken, error) {
	return nil, errors.New("not implemented")
}

func (m *throttleMockTokenStore) ValidateToken(_ context.Context, _ string) (*state.TaskAuthToken, error) {
	m.validateCalls++
	if m.validateFunc != nil {
		return m.validateFunc()
	}
	return nil, errors.New("token not found")
}

func (m *throttleMockTokenStore) RevokeToken(_ context.Context, _ string) error { return nil }

func (m *throttleMockTokenStore) RevokeTokensForTask(_ context.Context, _ string) error { return nil }

func (m *throttleMockTokenStore) ListTokensForTask(_ context.Context, _ string) ([]*state.TaskAuthToken, error) {
	return nil, nil
}

// throttleTestInit builds an InitConnection carrying a token credential for the
// given agent identity coordinates.
func throttleTestInit(token string) *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "prod",
				Implementation: "classifier",
				Specifier:      "v2",
			},
		},
		Credentials: map[string]string{"token": token},
	}
}

func throttleTestIdentity() models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "prod",
		Implementation: "classifier",
		Specifier:      "v2",
	}
}

// TestAuthThrottle_EngagesAfterThresholdFailures verifies that authFailThreshold
// consecutive failures engage the cooldown and that a subsequent attempt is
// fast-rejected WITHOUT calling ValidateToken again.
func TestAuthThrottle_EngagesAfterThresholdFailures(t *testing.T) {
	mock := &throttleMockTokenStore{} // always fails: "token not found"
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)
	h.tokenStore = mock

	ctx := context.Background()
	identity := throttleTestIdentity()
	init := throttleTestInit("stale-token")

	// First authFailThreshold attempts each call ValidateToken and fail.
	for i := 0; i < authFailThreshold; i++ {
		_, _, err := h.authenticateCredentials(ctx, init, identity, false, false)
		if err == nil {
			t.Fatalf("attempt %d: expected error from failed validation, got nil", i+1)
		}
	}
	if mock.validateCalls != authFailThreshold {
		t.Fatalf("expected %d ValidateToken calls during threshold, got %d", authFailThreshold, mock.validateCalls)
	}

	// The next attempt must be throttled: no additional ValidateToken call.
	_, _, err := h.authenticateCredentials(ctx, init, identity, false, false)
	if err == nil {
		t.Fatal("expected throttled attempt to return an error")
	}
	if mock.validateCalls != authFailThreshold {
		t.Errorf("throttled attempt should NOT call ValidateToken: got %d calls, want %d", mock.validateCalls, authFailThreshold)
	}
}

// TestAuthThrottle_SuccessBeforeThresholdResetsCounter verifies that a
// successful validation before the threshold clears the failure bookkeeping so
// later failures start counting from zero again.
func TestAuthThrottle_SuccessBeforeThresholdResetsCounter(t *testing.T) {
	identity := throttleTestIdentity()
	mock := &throttleMockTokenStore{}
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)
	h.tokenStore = mock

	ctx := context.Background()
	init := throttleTestInit("the-token")

	// A few failures, but fewer than the threshold.
	failsBeforeSuccess := authFailThreshold - 2
	for i := 0; i < failsBeforeSuccess; i++ {
		if _, _, err := h.authenticateCredentials(ctx, init, identity, false, false); err == nil {
			t.Fatalf("pre-success attempt %d: expected failure", i+1)
		}
	}

	// One success: token validated and matches the connecting identity.
	mock.validateFunc = func() (*state.TaskAuthToken, error) {
		return &state.TaskAuthToken{TaskID: "task-1", TargetIdentity: identity.String()}, nil
	}
	if _, _, err := h.authenticateCredentials(ctx, init, identity, false, false); err != nil {
		t.Fatalf("expected successful validation, got %v", err)
	}

	// Verify the throttle record was cleared.
	h.authFailMu.Lock()
	_, present := h.authFailRecords[identity.String()]
	h.authFailMu.Unlock()
	if present {
		t.Error("expected throttle record cleared after successful validation")
	}

	// Now fail again: it should take a full authFailThreshold failures to
	// re-engage, proving the counter reset. Validate that the (threshold-1)th
	// failure is still re-validated (not throttled).
	mock.validateFunc = nil // back to always-fail
	callsBefore := mock.validateCalls
	for i := 0; i < authFailThreshold-1; i++ {
		if _, _, err := h.authenticateCredentials(ctx, init, identity, false, false); err == nil {
			t.Fatalf("post-reset attempt %d: expected failure", i+1)
		}
	}
	if got := mock.validateCalls - callsBefore; got != authFailThreshold-1 {
		t.Errorf("expected %d re-validations after reset (not throttled early), got %d", authFailThreshold-1, got)
	}
}

// TestAuthThrottle_CooldownExpiryAllowsRevalidation verifies that once the
// cooldown elapses the throttle releases and ValidateToken is called again.
func TestAuthThrottle_CooldownExpiryAllowsRevalidation(t *testing.T) {
	identity := throttleTestIdentity()
	mock := &throttleMockTokenStore{}
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)
	h.tokenStore = mock

	ctx := context.Background()
	init := throttleTestInit("stale-token")

	// Drive past the threshold to engage cooldown.
	for i := 0; i < authFailThreshold; i++ {
		if _, _, err := h.authenticateCredentials(ctx, init, identity, false, false); err == nil {
			t.Fatalf("attempt %d: expected failure", i+1)
		}
	}
	if mock.validateCalls != authFailThreshold {
		t.Fatalf("expected %d ValidateToken calls, got %d", authFailThreshold, mock.validateCalls)
	}

	// Confirm currently throttled (no new validation).
	if _, _, err := h.authenticateCredentials(ctx, init, identity, false, false); err == nil {
		t.Fatal("expected throttled attempt to error")
	}
	if mock.validateCalls != authFailThreshold {
		t.Fatalf("expected still %d ValidateToken calls during cooldown, got %d", authFailThreshold, mock.validateCalls)
	}

	// Simulate cooldown expiry by backdating the record's cooldownUntil.
	h.authFailMu.Lock()
	rec := h.authFailRecords[identity.String()]
	rec.cooldownUntil = time.Now().Add(-time.Second)
	h.authFailMu.Unlock()

	// Re-validation should now occur (the throttle released).
	if _, _, err := h.authenticateCredentials(ctx, init, identity, false, false); err == nil {
		t.Fatal("expected validation error after cooldown expiry")
	}
	if mock.validateCalls != authFailThreshold+1 {
		t.Errorf("expected re-validation after cooldown expiry: got %d ValidateToken calls, want %d", mock.validateCalls, authFailThreshold+1)
	}
}

// TestAuthThrottle_HelpersBookkeeping exercises the throttle helpers directly to
// pin down window-reset and threshold semantics independent of the gRPC path.
func TestAuthThrottle_HelpersBookkeeping(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)
	key := "ag::prod::classifier::v2"
	base := time.Now()

	// Failures 1..threshold-1 should not engage; threshold-th engages once.
	for i := 1; i < authFailThreshold; i++ {
		if engaged := h.recordAuthFailure(key, base); engaged {
			t.Fatalf("failure %d should not engage cooldown", i)
		}
		if h.throttled(key, base) {
			t.Fatalf("should not be throttled after %d failures", i)
		}
	}
	if engaged := h.recordAuthFailure(key, base); !engaged {
		t.Fatalf("failure %d should engage cooldown", authFailThreshold)
	}
	if !h.throttled(key, base) {
		t.Fatal("should be throttled once cooldown engaged")
	}

	// A failure spaced beyond the window resets the counter (no engage).
	afterWindow := base.Add(authFailCooldown + authFailWindow + time.Second)
	if h.throttled(key, afterWindow) {
		t.Fatal("should no longer be throttled after cooldown elapses")
	}
	if engaged := h.recordAuthFailure(key, afterWindow); engaged {
		t.Fatal("first failure in a fresh window should not engage")
	}

	// clearAuthFailure drops the record entirely.
	h.clearAuthFailure(key)
	h.authFailMu.Lock()
	_, present := h.authFailRecords[key]
	h.authFailMu.Unlock()
	if present {
		t.Error("expected record removed after clearAuthFailure")
	}
}
