package workflow_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	wfstore "github.com/scitrera/aether/server/internal/storage/workflow"
	wfsqlite "github.com/scitrera/aether/server/internal/storage/workflow/sqlite"
)

// newJoinTestStore spins up a fresh native-SQLite store backed by a temp-dir
// file, mirroring sqliteNativeFactory in conformance_test.go.
func newJoinTestStore(t *testing.T) wfstore.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "workflow_joins.db")
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite (native): %v", err)
	}
	store, err := wfsqlite.New(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("wfsqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return store
}

func containsJoin(joins []wfstore.Join, id int64) bool {
	for _, j := range joins {
		if j.ID == id {
			return true
		}
	}
	return false
}

// TestJoins_EnsureAndGet covers EnsureJoin (insert + idempotent re-ensure)
// and GetJoin (present + absent → nil,nil).
func TestJoins_EnsureAndGet(t *testing.T) {
	ctx := context.Background()
	store := newJoinTestStore(t)

	tag := uniqueName(t, "join")
	deadline := time.Now().Add(time.Hour)
	j := &wfstore.Join{
		JoinName:       "jn-" + tag,
		Workspace:      "ws-" + tag,
		CorrelationKey: "ck-" + tag,
		Mode:           wfstore.JoinModeCount,
		ArrivedCount:   0,
		Status:         wfstore.JoinStatusOpen,
		DeadlineAt:     &deadline,
	}

	got, err := store.EnsureJoin(ctx, j)
	if err != nil {
		t.Fatalf("EnsureJoin: %v", err)
	}
	if got == nil || got.ID == 0 {
		t.Fatalf("EnsureJoin did not return a live row with an ID: %+v", got)
	}
	if got.Mode != wfstore.JoinModeCount || got.Status != wfstore.JoinStatusOpen {
		t.Fatalf("EnsureJoin field mismatch: %+v", got)
	}
	if got.DeadlineAt == nil {
		t.Fatalf("EnsureJoin did not persist deadline_at")
	}
	firstID := got.ID

	// Idempotent re-ensure with different field values must NOT create a
	// second row and must NOT mutate the existing one.
	dup := &wfstore.Join{
		JoinName:       "jn-" + tag,
		Workspace:      "ws-" + tag,
		CorrelationKey: "ck-" + tag,
		Mode:           wfstore.JoinModeSet, // different, should be ignored
		ArrivedCount:   99,                  // different, should be ignored
		Status:         wfstore.JoinStatusFired,
	}
	again, err := store.EnsureJoin(ctx, dup)
	if err != nil {
		t.Fatalf("EnsureJoin (re-ensure): %v", err)
	}
	if again.ID != firstID {
		t.Fatalf("re-ensure produced a new row id: got %d want %d", again.ID, firstID)
	}
	if again.Mode != wfstore.JoinModeCount || again.ArrivedCount != 0 || again.Status != wfstore.JoinStatusOpen {
		t.Fatalf("re-ensure mutated the existing row: %+v", again)
	}

	// GetJoin present.
	present, err := store.GetJoin(ctx, "jn-"+tag, "ws-"+tag, "ck-"+tag)
	if err != nil {
		t.Fatalf("GetJoin (present): %v", err)
	}
	if present == nil || present.ID != firstID {
		t.Fatalf("GetJoin (present) returned %+v", present)
	}

	// GetJoin absent → (nil, nil).
	absent, err := store.GetJoin(ctx, "jn-missing", "ws-missing", "ck-missing")
	if err != nil {
		t.Fatalf("GetJoin (absent): %v", err)
	}
	if absent != nil {
		t.Fatalf("GetJoin (absent) expected nil, got %+v", absent)
	}
}

// TestJoins_Mutations covers UpdateJoinArrived, SetJoinExpected, SetJoinDirty,
// and MarkJoinTerminal.
func TestJoins_Mutations(t *testing.T) {
	ctx := context.Background()
	store := newJoinTestStore(t)

	tag := uniqueName(t, "join")
	j := &wfstore.Join{
		JoinName:       "jn-" + tag,
		Workspace:      "ws-" + tag,
		CorrelationKey: "ck-" + tag,
		Mode:           wfstore.JoinModeCount,
		Status:         wfstore.JoinStatusOpen,
	}
	row, err := store.EnsureJoin(ctx, j)
	if err != nil {
		t.Fatalf("EnsureJoin: %v", err)
	}

	if err := store.UpdateJoinArrived(ctx, row.ID, 3); err != nil {
		t.Fatalf("UpdateJoinArrived: %v", err)
	}
	if err := store.SetJoinExpected(ctx, row.ID, 5); err != nil {
		t.Fatalf("SetJoinExpected: %v", err)
	}
	if err := store.SetJoinDirty(ctx, row.ID, true); err != nil {
		t.Fatalf("SetJoinDirty: %v", err)
	}

	got, err := store.GetJoin(ctx, "jn-"+tag, "ws-"+tag, "ck-"+tag)
	if err != nil {
		t.Fatalf("GetJoin: %v", err)
	}
	if got.ArrivedCount != 3 {
		t.Fatalf("ArrivedCount: got %d want 3", got.ArrivedCount)
	}
	if got.ExpectedCount == nil || *got.ExpectedCount != 5 {
		t.Fatalf("ExpectedCount: got %v want 5", got.ExpectedCount)
	}
	if !got.Dirty {
		t.Fatalf("Dirty: got false want true")
	}

	linger := time.Now().Add(2 * time.Hour)
	if err := store.MarkJoinTerminal(ctx, row.ID, wfstore.JoinStatusFired, linger); err != nil {
		t.Fatalf("MarkJoinTerminal: %v", err)
	}
	terminal, err := store.GetJoin(ctx, "jn-"+tag, "ws-"+tag, "ck-"+tag)
	if err != nil {
		t.Fatalf("GetJoin (terminal): %v", err)
	}
	if terminal.Status != wfstore.JoinStatusFired {
		t.Fatalf("Status: got %q want %q", terminal.Status, wfstore.JoinStatusFired)
	}
	if terminal.LingerUntil == nil {
		t.Fatalf("LingerUntil was not persisted")
	}
}

// TestJoins_DueDeadlines verifies that an open row with a past deadline is
// returned, while a future-deadline row and a fired (terminal) row are not.
func TestJoins_DueDeadlines(t *testing.T) {
	ctx := context.Background()
	store := newJoinTestStore(t)

	tag := uniqueName(t, "join")
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)

	// Open + past deadline → should be returned.
	dueRow, err := store.EnsureJoin(ctx, &wfstore.Join{
		JoinName: "jn-due-" + tag, Workspace: "ws-" + tag, CorrelationKey: "ck-due-" + tag,
		Mode: wfstore.JoinModeCount, Status: wfstore.JoinStatusOpen, DeadlineAt: &past,
	})
	if err != nil {
		t.Fatalf("EnsureJoin (due): %v", err)
	}

	// Open + future deadline → should NOT be returned.
	futureRow, err := store.EnsureJoin(ctx, &wfstore.Join{
		JoinName: "jn-future-" + tag, Workspace: "ws-" + tag, CorrelationKey: "ck-future-" + tag,
		Mode: wfstore.JoinModeCount, Status: wfstore.JoinStatusOpen, DeadlineAt: &future,
	})
	if err != nil {
		t.Fatalf("EnsureJoin (future): %v", err)
	}

	// Fired + past deadline → should NOT be returned (not open).
	firedRow, err := store.EnsureJoin(ctx, &wfstore.Join{
		JoinName: "jn-fired-" + tag, Workspace: "ws-" + tag, CorrelationKey: "ck-fired-" + tag,
		Mode: wfstore.JoinModeCount, Status: wfstore.JoinStatusFired, DeadlineAt: &past,
	})
	if err != nil {
		t.Fatalf("EnsureJoin (fired): %v", err)
	}

	due, err := store.GetDueJoinDeadlines(ctx, time.Now())
	if err != nil {
		t.Fatalf("GetDueJoinDeadlines: %v", err)
	}
	if !containsJoin(due, dueRow.ID) {
		t.Fatalf("GetDueJoinDeadlines did not include the past-deadline open row %d", dueRow.ID)
	}
	if containsJoin(due, futureRow.ID) {
		t.Fatalf("GetDueJoinDeadlines unexpectedly included the future-deadline row %d", futureRow.ID)
	}
	if containsJoin(due, firedRow.ID) {
		t.Fatalf("GetDueJoinDeadlines unexpectedly included the fired (terminal) row %d", firedRow.ID)
	}
}

// TestJoins_List covers ListJoins for a specific workspace and the all
// ("" / "*") case.
func TestJoins_List(t *testing.T) {
	ctx := context.Background()
	store := newJoinTestStore(t)

	tag := uniqueName(t, "join")
	ws := "ws-" + tag
	a, err := store.EnsureJoin(ctx, &wfstore.Join{
		JoinName: "jn-a-" + tag, Workspace: ws, CorrelationKey: "ck-a-" + tag,
		Mode: wfstore.JoinModeCount, Status: wfstore.JoinStatusOpen,
	})
	if err != nil {
		t.Fatalf("EnsureJoin (a): %v", err)
	}
	other, err := store.EnsureJoin(ctx, &wfstore.Join{
		JoinName: "jn-b-" + tag, Workspace: "other-" + tag, CorrelationKey: "ck-b-" + tag,
		Mode: wfstore.JoinModeCount, Status: wfstore.JoinStatusOpen,
	})
	if err != nil {
		t.Fatalf("EnsureJoin (other): %v", err)
	}

	scoped, err := store.ListJoins(ctx, ws)
	if err != nil {
		t.Fatalf("ListJoins (scoped): %v", err)
	}
	if !containsJoin(scoped, a.ID) {
		t.Fatalf("ListJoins(%q) did not include row %d", ws, a.ID)
	}
	if containsJoin(scoped, other.ID) {
		t.Fatalf("ListJoins(%q) unexpectedly included other-workspace row %d", ws, other.ID)
	}

	all, err := store.ListJoins(ctx, "")
	if err != nil {
		t.Fatalf("ListJoins (all): %v", err)
	}
	if !containsJoin(all, a.ID) || !containsJoin(all, other.ID) {
		t.Fatalf("ListJoins(\"\") did not include both rows %d and %d", a.ID, other.ID)
	}
}
