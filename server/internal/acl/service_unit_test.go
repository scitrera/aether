package acl

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

// --- Minimal sql/driver mock for applyFallback tests ---

// stubRows implements driver.Rows for a single int column.
type stubRows struct {
	cols    []string
	values  [][]driver.Value
	pos     int
	scanErr error
}

func (r *stubRows) Columns() []string { return r.cols }
func (r *stubRows) Close() error      { return nil }
func (r *stubRows) Next(dest []driver.Value) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.pos >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.pos])
	r.pos++
	return nil
}

// stubConn implements driver.Conn with a configurable QueryRow response.
type stubConn struct {
	queryRowFunc func(query string, args []driver.NamedValue) (driver.Rows, error)
}

func (c *stubConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("not implemented")
}
func (c *stubConn) Close() error              { return nil }
func (c *stubConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not implemented") }
func (c *stubConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.queryRowFunc(query, args)
}

// stubDriver creates an in-process *sql.DB whose query behaviour is controlled
// by the supplied queryRowFunc.
type stubDriver struct {
	queryRowFunc func(query string, args []driver.NamedValue) (driver.Rows, error)
}

func (d *stubDriver) Open(_ string) (driver.Conn, error) {
	return &stubConn{queryRowFunc: d.queryRowFunc}, nil
}

func newStubDB(t *testing.T, queryRowFunc func(string, []driver.NamedValue) (driver.Rows, error)) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("stub-%s", t.Name())
	sql.Register(name, &stubDriver{queryRowFunc: queryRowFunc})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("failed to open stub db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// --- applyFallback unit tests ---

func TestApplyFallback_NoRowsReturnsDeny(t *testing.T) {
	db := newStubDB(t, func(q string, args []driver.NamedValue) (driver.Rows, error) {
		return &stubRows{cols: []string{"fallback_access_level"}, values: nil}, nil
	})

	svc := &Service{db: db}
	principal := models.Identity{Type: models.PrincipalUser, ID: "bob"}

	decision, err := svc.applyFallback(context.Background(), principal, ResourceTypeWorkspace, AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.Allowed {
		t.Error("expected denied when no fallback row exists")
	}
	if !decision.FallbackApplied {
		t.Error("expected FallbackApplied=true")
	}
	if decision.Decision != DecisionDeny {
		t.Errorf("Decision = %q, want DENY", decision.Decision)
	}
}

func TestApplyFallback_FallbackAllows(t *testing.T) {
	db := newStubDB(t, func(q string, args []driver.NamedValue) (driver.Rows, error) {
		return &stubRows{
			cols:   []string{"fallback_access_level"},
			values: [][]driver.Value{{int64(AccessReadWrite)}},
		}, nil
	})

	svc := &Service{db: db}
	principal := models.Identity{Type: models.PrincipalUser, ID: "carol"}

	decision, err := svc.applyFallback(context.Background(), principal, ResourceTypeWorkspace, AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !decision.Allowed {
		t.Error("expected allowed when fallback level >= required level")
	}
	if !decision.FallbackApplied {
		t.Error("expected FallbackApplied=true")
	}
	if decision.Decision != DecisionAllow {
		t.Errorf("Decision = %q, want ALLOW", decision.Decision)
	}
}

func TestApplyFallback_FallbackDeniesInsufficientLevel(t *testing.T) {
	db := newStubDB(t, func(q string, args []driver.NamedValue) (driver.Rows, error) {
		return &stubRows{
			cols:   []string{"fallback_access_level"},
			values: [][]driver.Value{{int64(AccessRead)}},
		}, nil
	})

	svc := &Service{db: db}
	principal := models.Identity{Type: models.PrincipalUser, ID: "dave"}

	decision, err := svc.applyFallback(context.Background(), principal, ResourceTypeWorkspace, AccessManage)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.Allowed {
		t.Error("expected denied when fallback level < required level")
	}
	if decision.EffectiveAccessLevel != AccessRead {
		t.Errorf("EffectiveAccessLevel = %d, want %d", decision.EffectiveAccessLevel, AccessRead)
	}
}

func TestApplyFallback_DBErrorPropagated(t *testing.T) {
	db := newStubDB(t, func(q string, args []driver.NamedValue) (driver.Rows, error) {
		return nil, fmt.Errorf("connection refused")
	})

	svc := &Service{db: db}
	principal := models.Identity{Type: models.PrincipalUser, ID: "eve"}

	_, err := svc.applyFallback(context.Background(), principal, ResourceTypeWorkspace, AccessRead)
	if err == nil {
		t.Error("expected error when DB returns error")
	}
}

// --- CheckAccess with nil enforcer (fail-closed path) ---

func TestCheckAccess_NilEnforcerDeniesAccess(t *testing.T) {
	db := newStubDB(t, func(q string, args []driver.NamedValue) (driver.Rows, error) {
		return &stubRows{cols: []string{"fallback_access_level"}, values: nil}, nil
	})

	svc := &Service{
		db:        db,
		enforcer:  nil, // nil enforcer — must deny all access (fail-closed)
		audit:     NewAuditLogger(db, "test-gw"),
		gatewayID: "test-gw",
	}
	defer svc.Close()

	alice := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	decision, err := svc.CheckAccess(context.Background(), alice, ResourceTypeWorkspace, "prod", "connect", "prod", uuid.New(), AccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision == nil {
		t.Fatal("expected non-nil decision")
	}
	if decision.Decision != DecisionDeny {
		t.Errorf("Decision = %q, want DENY", decision.Decision)
	}
	if decision.Allowed {
		t.Error("expected Allowed=false with nil enforcer (fail-closed)")
	}
}

// --- PrincipalTypeForModel ---

func TestPrincipalTypeForModel(t *testing.T) {
	tests := []struct {
		pt   models.PrincipalType
		want string
	}{
		{models.PrincipalUser, PrincipalTypeUser},
		{models.PrincipalAgent, PrincipalTypeAgent},
		{models.PrincipalTask, PrincipalTypeTask},
		{models.PrincipalWorkflowEngine, PrincipalTypeWorkflowEngine},
		{models.PrincipalMetricsBridge, PrincipalTypeMetricsBridge},
		{models.PrincipalOrchestrator, PrincipalTypeOrchestrator},
		{models.PrincipalBridge, PrincipalTypeBridge},
		{models.PrincipalService, PrincipalTypeService},
		{models.PrincipalType("Unknown"), "unknown"},
	}

	for _, tt := range tests {
		got := PrincipalTypeForModel(tt.pt)
		if got != tt.want {
			t.Errorf("PrincipalTypeForModel(%v) = %q, want %q", tt.pt, got, tt.want)
		}
	}
}
