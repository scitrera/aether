package state

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/pkg/models"
)

// newTestSessionRegistry starts an in-process miniredis and returns a
// SessionRegistry backed by it. miniredis cleanup is handled via t.Cleanup.
func newTestSessionRegistry(t *testing.T) (*SessionRegistry, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewSessionRegistryFromClient(client), mr
}

func TestRedisGetSessionGateway_ReturnsStoredGatewayID(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestSessionRegistry(t)

	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws",
		Implementation: "impl",
		Specifier:      "spec",
	}
	const sessionID = "sess-1"
	const gatewayID = "test-gateway-1"

	// Acquire lock and register session — mirrors the gateway connect flow.
	acquired, err := reg.AcquireLock(ctx, identity, sessionID)
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	if !acquired {
		t.Fatal("expected lock acquisition to succeed")
	}
	if err := reg.RegisterSession(ctx, identity, sessionID, gatewayID); err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}

	got, err := reg.GetSessionGateway(ctx, identity)
	if err != nil {
		t.Fatalf("GetSessionGateway: %v", err)
	}
	if got != gatewayID {
		t.Errorf("GetSessionGateway = %q, want %q", got, gatewayID)
	}
}

func TestRedisGetSessionGateway_OfflinePrincipalReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestSessionRegistry(t)

	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws",
		Implementation: "impl",
		Specifier:      "offline",
	}

	got, err := reg.GetSessionGateway(ctx, identity)
	if err != nil {
		t.Fatalf("GetSessionGateway: %v", err)
	}
	if got != "" {
		t.Errorf("GetSessionGateway = %q, want empty string when principal offline", got)
	}
}

func TestRedisGetSessionGateway_LockWithoutSessionReturnsEmpty(t *testing.T) {
	// Edge case: lock present but the session HASH was wiped (e.g. expired
	// independently or a legacy entry). Should return "" without error.
	ctx := context.Background()
	reg, _ := newTestSessionRegistry(t)

	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws",
		Implementation: "impl",
		Specifier:      "stale",
	}
	if _, err := reg.AcquireLock(ctx, identity, "sess-stale"); err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}

	got, err := reg.GetSessionGateway(ctx, identity)
	if err != nil {
		t.Fatalf("GetSessionGateway: %v", err)
	}
	if got != "" {
		t.Errorf("GetSessionGateway = %q, want empty string when session HASH missing", got)
	}
}

func TestBadgerGetSessionGateway_ReturnsStoredGatewayID(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()
	const sessionID = "sess-bg-1"
	const gatewayID = "test-gateway-1"

	if _, _, _, err := reg.AcquireOrResumeLock(ctx, id, sessionID, "", 0); err != nil {
		t.Fatalf("AcquireOrResumeLock: %v", err)
	}
	if err := reg.RegisterSession(ctx, id, sessionID, gatewayID); err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}

	got, err := reg.GetSessionGateway(ctx, id)
	if err != nil {
		t.Fatalf("GetSessionGateway: %v", err)
	}
	if got != gatewayID {
		t.Errorf("GetSessionGateway = %q, want %q", got, gatewayID)
	}
}

func TestBadgerGetSessionGateway_OfflinePrincipalReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	got, err := reg.GetSessionGateway(ctx, id)
	if err != nil {
		t.Fatalf("GetSessionGateway: %v", err)
	}
	if got != "" {
		t.Errorf("GetSessionGateway = %q, want empty string when principal offline", got)
	}
}
