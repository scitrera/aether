// Package workflow_test contains the cross-backend conformance suite for
// workflow.Store. The same test cases run against every implementation —
// postgres today, sqlite-native once Stage 2 lands. Drift between
// implementations gets caught here.
//
// Per `.slop/20260514_storage_interfaces_stage0.md` §8, the suite is
// table-driven with one subtest per backend. The postgres subtest skips
// when DATABASE_URL / dev infra is unavailable; the sqlite subtest is
// always runnable since it spins up a temp-dir SQLite file (via the
// sqlite_compat driver, since the Stage 1 impl still emits
// postgres-flavored SQL).
package workflow_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	wfstore "github.com/scitrera/aether/internal/storage/workflow"
	wfpg "github.com/scitrera/aether/internal/storage/workflow/postgres"
	wfsqlite "github.com/scitrera/aether/internal/storage/workflow/sqlite"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/internal/workflow"
	wfmigrations "github.com/scitrera/aether/internal/workflow/migrations"
	wfsqlitemigrations "github.com/scitrera/aether/internal/workflow/migrations/sqlite"

	_ "github.com/scitrera/aether/pkg/dbcompat" // registers "sqlite_compat" driver
)

// storeFactory builds a Store and returns a cleanup func. The factory may
// call t.Skip if its prerequisites are unmet (e.g. postgres dev infra not
// running) — the harness honors that and reports the subtest as skipped.
// supportsOutOfOrderPlaceholders was historically false for the sqlite
// backend because pkg/dbcompat's rewriter rewrote `$N` → bare `?` and
// lost postgres's positional reordering for SQL like
// `... status = $2, error_message = $3 WHERE execution_id = $1`. The
// 2026-05-14 fix changed the rewriter to emit SQLite's `?N` numbered
// binding syntax, which preserves identity-of-N across reuses and
// out-of-order references. Both subtests gated on this flag now pass on
// sqlite. The flag remains so that a future Stage 3 sqlite impl that uses
// bare `sqlite` driver (no dbcompat rewriter at all) can advertise its
// own placeholder support story without re-discovering this history.
type storeFactory func(t *testing.T) (store wfstore.Store, supportsOutOfOrderPlaceholders bool, cleanup func())

func TestStoreConformance(t *testing.T) {
	backends := []struct {
		name    string
		factory storeFactory
	}{
		{name: "postgres", factory: postgresFactory},
		{name: "sqlite", factory: sqliteFactory},
		{name: "sqlite_native", factory: sqliteNativeFactory},
	}

	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			t.Run("Rules_round_trip", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runRulesRoundTrip(t, store)
			})
			t.Run("Executions_status_transitions", func(t *testing.T) {
				store, supportsOOP, cleanup := b.factory(t)
				defer cleanup()
				if !supportsOOP {
					// Legacy UpdateExecutionStatus uses out-of-order $N
					// placeholders. See storeFactory doc above. Re-enables in
					// Stage 2.
					t.Skip("out-of-order $N placeholders not supported by pkg/dbcompat rewriter (Stage 2 gap)")
				}
				runExecutionsStatusTransitions(t, store)
			})
			t.Run("Schedules_round_trip", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runSchedulesRoundTrip(t, store)
			})
			t.Run("StateMachines_round_trip", func(t *testing.T) {
				store, supportsOOP, cleanup := b.factory(t)
				defer cleanup()
				if !supportsOOP {
					// Legacy UpdateStateMachineInstance uses out-of-order $N
					// placeholders. See storeFactory doc above. Re-enables in
					// Stage 2.
					t.Skip("out-of-order $N placeholders not supported by pkg/dbcompat rewriter (Stage 2 gap)")
				}
				runStateMachinesRoundTrip(t, store)
			})
		})
	}
}

// =============================================================================
// Test cases
// =============================================================================

// runRulesRoundTrip exercises CreateRule → GetRule → GetMatchingRules → DeleteRule.
func runRulesRoundTrip(t *testing.T, store wfstore.Store) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueName(t, "rule")
	r := &wfstore.Rule{
		RuleName:            tag,
		SourceAgent:         "agent-" + tag,
		SourceEvent:         "evt-" + tag,
		TriggerCondition:    "",
		TransformationStyle: "template-yaml",
		DestinationTemplate: "{}",
		Workspace:           "ws-" + tag,
		Priority:            5,
		Active:              true,
	}
	if err := store.CreateRule(ctx, r); err != nil {
		t.Fatalf("CreateRule: %v", err)
	}
	if r.ID == 0 {
		t.Fatalf("CreateRule did not populate ID")
	}

	got, err := store.GetRule(ctx, r.ID)
	if err != nil {
		t.Fatalf("GetRule: %v", err)
	}
	if got == nil {
		t.Fatalf("GetRule returned nil for id %d", r.ID)
	}
	if got.RuleName != tag {
		t.Fatalf("GetRule.RuleName: got %q want %q", got.RuleName, tag)
	}

	matches, err := store.GetMatchingRules(ctx, "agent-"+tag, "evt-"+tag, "ws-"+tag)
	if err != nil {
		t.Fatalf("GetMatchingRules: %v", err)
	}
	if !containsRule(matches, r.ID) {
		t.Fatalf("GetMatchingRules did not include id %d (got %d matches)", r.ID, len(matches))
	}

	// Non-matching source — must NOT include our row.
	misses, err := store.GetMatchingRules(ctx, "wrong-agent", "evt-"+tag, "ws-"+tag)
	if err != nil {
		t.Fatalf("GetMatchingRules (miss): %v", err)
	}
	if containsRule(misses, r.ID) {
		t.Fatalf("GetMatchingRules with wrong agent unexpectedly included id %d", r.ID)
	}

	if err := store.DeleteRule(ctx, r.ID); err != nil {
		t.Fatalf("DeleteRule: %v", err)
	}
	gone, err := store.GetRule(ctx, r.ID)
	if err != nil {
		t.Fatalf("GetRule after delete: %v", err)
	}
	if gone != nil {
		t.Fatalf("GetRule after delete returned non-nil row")
	}
}

// runExecutionsStatusTransitions exercises CreateExecution →
// GetExecution → UpdateExecutionStatus → GetExecution → GetRunningExecutions.
func runExecutionsStatusTransitions(t *testing.T, store wfstore.Store) {
	t.Helper()
	ctx := context.Background()

	execID := uniqueName(t, "exec")
	e := &wfstore.WorkflowExecution{
		ExecutionID:     execID,
		WorkflowID:      "wf-" + execID,
		WorkflowVersion: 1,
		Workspace:       "ws-" + execID,
		Status:          wfstore.ExecStatusRunning,
		TriggerData:     json.RawMessage(`{"hint":"conformance"}`),
		Metadata:        json.RawMessage(`{}`),
	}
	if err := store.CreateExecution(ctx, e); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if e.StartedAt.IsZero() {
		t.Fatalf("CreateExecution did not populate StartedAt")
	}

	got, err := store.GetExecution(ctx, execID)
	if err != nil {
		t.Fatalf("GetExecution: %v", err)
	}
	if got == nil || got.Status != wfstore.ExecStatusRunning {
		t.Fatalf("GetExecution.Status: got %+v want running", got)
	}

	running, err := store.GetRunningExecutions(ctx)
	if err != nil {
		t.Fatalf("GetRunningExecutions: %v", err)
	}
	if !containsExec(running, execID) {
		t.Fatalf("GetRunningExecutions did not include %s", execID)
	}

	if err := store.UpdateExecutionStatus(ctx, execID, wfstore.ExecStatusCompleted, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus: %v", err)
	}

	got, err = store.GetExecution(ctx, execID)
	if err != nil {
		t.Fatalf("GetExecution (post-update): %v", err)
	}
	if got == nil || got.Status != wfstore.ExecStatusCompleted {
		t.Fatalf("GetExecution.Status after update: got %+v want completed", got)
	}
	if got.CompletedAt == nil {
		t.Fatalf("GetExecution.CompletedAt was not stamped on terminal status")
	}

	stillRunning, err := store.GetRunningExecutions(ctx)
	if err != nil {
		t.Fatalf("GetRunningExecutions (post-update): %v", err)
	}
	if containsExec(stillRunning, execID) {
		t.Fatalf("GetRunningExecutions still includes %s after completion", execID)
	}
}

// runSchedulesRoundTrip exercises CreateSchedule → GetSchedule →
// ListSchedules → DeleteSchedule.
func runSchedulesRoundTrip(t *testing.T, store wfstore.Store) {
	t.Helper()
	ctx := context.Background()

	id := uniqueName(t, "sched")
	next := time.Now().Add(time.Hour)
	sc := &wfstore.Schedule{
		ID:            id,
		Name:          "name-" + id,
		Workspace:     "ws-" + id,
		ScheduleType:  wfstore.ScheduleTypeInterval,
		ScheduleExpr:  "5m",
		Action:        json.RawMessage(`{"hint":"conformance"}`),
		Enabled:       true,
		NextFireAt:    &next,
		MissPolicy:    "skip",
		MaxConcurrent: 1,
	}
	if err := store.CreateSchedule(ctx, sc); err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}
	if sc.CreatedAt.IsZero() {
		t.Fatalf("CreateSchedule did not populate CreatedAt")
	}

	got, err := store.GetSchedule(ctx, id)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got == nil || got.Name != "name-"+id {
		t.Fatalf("GetSchedule.Name: got %+v want name-%s", got, id)
	}

	listed, err := store.ListSchedules(ctx, "ws-"+id)
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if !containsSchedule(listed, id) {
		t.Fatalf("ListSchedules did not include %s", id)
	}

	if err := store.DeleteSchedule(ctx, id); err != nil {
		t.Fatalf("DeleteSchedule: %v", err)
	}
	gone, err := store.GetSchedule(ctx, id)
	if err != nil {
		t.Fatalf("GetSchedule after delete: %v", err)
	}
	if gone != nil {
		t.Fatalf("GetSchedule after delete returned non-nil row")
	}
}

// runStateMachinesRoundTrip exercises CreateStateMachine +
// CreateStateMachineInstance → GetStateMachineInstance →
// UpdateStateMachineInstance.
func runStateMachinesRoundTrip(t *testing.T, store wfstore.Store) {
	t.Helper()
	ctx := context.Background()

	machineID := uniqueName(t, "sm")
	sm := &wfstore.StateMachineDef{
		ID:         machineID,
		Workspace:  "ws-" + machineID,
		Definition: json.RawMessage(`{"states":{"start":{},"done":{}}}`),
		Active:     true,
	}
	if err := store.CreateStateMachine(ctx, sm); err != nil {
		t.Fatalf("CreateStateMachine: %v", err)
	}

	instanceID := uniqueName(t, "inst")
	inst := &wfstore.StateMachineInstance{
		InstanceID:   instanceID,
		MachineID:    machineID,
		Workspace:    "ws-" + machineID,
		CurrentState: "start",
		Data:         json.RawMessage(`{}`),
	}
	if err := store.CreateStateMachineInstance(ctx, inst); err != nil {
		t.Fatalf("CreateStateMachineInstance: %v", err)
	}

	got, err := store.GetStateMachineInstance(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetStateMachineInstance: %v", err)
	}
	if got == nil || got.CurrentState != "start" {
		t.Fatalf("GetStateMachineInstance.CurrentState: got %+v want start", got)
	}

	if err := store.UpdateStateMachineInstance(ctx, instanceID, "done", nil, true); err != nil {
		t.Fatalf("UpdateStateMachineInstance: %v", err)
	}

	got, err = store.GetStateMachineInstance(ctx, instanceID)
	if err != nil {
		t.Fatalf("GetStateMachineInstance (post-update): %v", err)
	}
	if got == nil || got.CurrentState != "done" {
		t.Fatalf("GetStateMachineInstance.CurrentState after transition: got %+v want done", got)
	}
	if got.CompletedAt == nil {
		t.Fatalf("GetStateMachineInstance.CompletedAt was not stamped on terminal transition")
	}
}

// =============================================================================
// Backend factories
// =============================================================================

// postgresFactory connects to the dev postgres instance via testutil and
// also applies the workflow domain's own migration set (testutil's
// SetupTestDB only covers gateway migrations, not workflow's). Skips when
// the dev infra isn't reachable.
func postgresFactory(t *testing.T) (wfstore.Store, bool, func()) {
	t.Helper()
	testDB, cleanupDB := testutil.SetupTestDB(t)
	if testDB == nil {
		// SetupTestDB calls t.Skip on its own when infra is unavailable;
		// if we reach here with nil, just bail.
		return nil, false, func() {}
	}

	ctx := context.Background()
	if err := wfmigrations.Run(ctx, testDB.DB); err != nil {
		cleanupDB()
		t.Fatalf("apply workflow postgres migrations: %v", err)
	}

	// Clear any rows left from prior runs so list-shaped assertions
	// (GetRunningExecutions, ListSchedules) are predictable. testutil's
	// TruncateTestTables doesn't know about workflow tables.
	truncateWorkflowTables(t, testDB.DB, true)

	store := wfpg.New(testDB.DB, false)

	cleanup := func() {
		truncateWorkflowTables(t, testDB.DB, true)
		cleanupDB()
	}
	return store, true, cleanup
}

// sqliteFactory opens a fresh temp-dir SQLite database via the
// sqlite_compat driver (so dbcompat handles the postgres-flavored SQL
// the legacy store still emits in Stage 1), runs the workflow sqlite
// migration set, and constructs a postgres-impl workflow.Store on top of
// that handle with isSQLite=true.
func sqliteFactory(t *testing.T) (wfstore.Store, bool, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "workflow.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite_compat", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite_compat: %v", err)
	}
	// Single-writer pool to match aetherlite's workflow.db semantics and
	// avoid SQLITE_BUSY in WAL mode (§14.3).
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := wfsqlitemigrations.Run(ctx, db); err != nil {
		_ = db.Close()
		t.Fatalf("apply workflow sqlite migrations: %v", err)
	}

	store := wfpg.New(db, true)

	cleanup := func() {
		_ = db.Close()
	}
	// supportsOutOfOrderPlaceholders=true — see storeFactory doc.
	return store, true, cleanup
}

// sqliteNativeFactory opens a fresh temp-dir SQLite database using the
// native sqlite impl (internal/storage/workflow/sqlite) with the bare
// "sqlite" driver — NO dbcompat, NO postgres-flavored SQL. This is the
// Stage 2 implementation under test: it owns all its own SQL, does inline
// timestamp parsing, and uses native SQLite idioms throughout.
func sqliteNativeFactory(t *testing.T) (wfstore.Store, bool, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "workflow_native.db")
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

	cleanup := func() {
		_ = db.Close()
	}
	// supportsOutOfOrderPlaceholders=true — the native impl writes its own
	// SQL with bare ? placeholders in the correct positional order. No
	// dbcompat rewriter involved.
	return store, true, cleanup
}

// truncateWorkflowTables wipes every workflow_* table so each subtest sees
// a clean slate. Postgres path uses TRUNCATE ... CASCADE; we never call
// this on sqlite because the sqlite factory uses a per-subtest temp file.
func truncateWorkflowTables(t *testing.T, db *sql.DB, postgres bool) {
	t.Helper()
	if !postgres {
		return
	}
	tables := []string{
		"workflow_step_states",
		"workflow_executions",
		"workflow_state_machine_instances",
		"workflow_state_machines",
		"workflow_schedules",
		"workflow_definitions",
		"workflow_rules",
	}
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table)); err != nil {
			// Table might not exist on this DB (e.g. partial migration
			// set). Logging is enough — the test asserts on specific rows
			// it inserts, not on absolute counts.
			t.Logf("note: could not truncate %s: %v", table, err)
		}
	}
}

// =============================================================================
// Helpers
// =============================================================================

// uniqueName returns a per-test-per-call identifier that won't collide
// with rows other tests inserted into the shared dev database.
func uniqueName(t *testing.T, hint string) string {
	t.Helper()
	return fmt.Sprintf("conformance-%s-%s-%d", hint, sanitizeTestName(t.Name()), time.Now().UnixNano())
}

// sanitizeTestName strips characters from t.Name() that aren't safe for
// embedding in SQL identifiers/values without quoting noise.
func sanitizeTestName(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

func containsRule(rules []wfstore.Rule, id int) bool {
	for _, r := range rules {
		if r.ID == id {
			return true
		}
	}
	return false
}

func containsExec(execs []wfstore.WorkflowExecution, id string) bool {
	for _, e := range execs {
		if e.ExecutionID == id {
			return true
		}
	}
	return false
}

func containsSchedule(schedules []wfstore.Schedule, id string) bool {
	for _, s := range schedules {
		if s.ID == id {
			return true
		}
	}
	return false
}

// =============================================================================
// Stage 2.5 integration test — NewServerWithStore injection
// =============================================================================

// TestNewServerWithStore_NativeInjection verifies that the native sqlite
// store can be injected into the workflow engine via NewServerWithStore.
// This is the Stage 2.5 plumbing that unblocks the Wave 3 cut-over: the
// caller constructs a wfsqlite.Store on a bare "sqlite" driver handle
// and injects it, bypassing dbcompat entirely.
//
// The test lives here (in internal/storage/workflow/) rather than in
// internal/workflow/ because importing wfsqlite from internal/workflow/
// would create an import cycle (internal/storage/workflow/sqlite →
// internal/storage/workflow → internal/workflow via types.go).
func TestNewServerWithStore_NativeInjection(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workflow_native_inject.db")
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	defer db.Close()

	store, err := wfsqlite.New(db)
	if err != nil {
		t.Fatalf("wfsqlite.New: %v", err)
	}

	cfg := &workflow.Config{
		Mode: workflow.ModeLite,
		SQLite: workflow.SQLiteConfig{
			Path: dbPath,
		},
	}

	srv, err := workflow.NewServerWithStore(cfg, store)
	if err != nil {
		t.Fatalf("NewServerWithStore: %v", err)
	}
	if srv == nil {
		t.Fatal("NewServerWithStore returned nil Server")
	}

	// Verify the store works through a round-trip — create a rule via
	// the injected store, then read it back.
	ctx := context.Background()
	tag := uniqueName(t, "inject")
	r := &wfstore.Rule{
		RuleName:            tag,
		SourceAgent:         "agent-" + tag,
		SourceEvent:         "evt-" + tag,
		TriggerCondition:    "",
		TransformationStyle: "template-yaml",
		DestinationTemplate: "{}",
		Workspace:           "ws-" + tag,
		Priority:            1,
		Active:              true,
	}
	if err := store.CreateRule(ctx, r); err != nil {
		t.Fatalf("CreateRule via injected store: %v", err)
	}
	if r.ID == 0 {
		t.Fatalf("CreateRule did not populate ID")
	}

	got, err := store.GetRule(ctx, r.ID)
	if err != nil {
		t.Fatalf("GetRule: %v", err)
	}
	if got == nil || got.RuleName != tag {
		t.Fatalf("GetRule round-trip failed: got %+v want RuleName=%q", got, tag)
	}
}

// TestNewServerWithStore_LegacyInjection verifies the injection path
// also works with the legacy postgres-wrapped store (sqlite_compat
// driver). This ensures backward compatibility for callers that
// construct the store externally but still use the dbcompat translation
// path during mixed-mode operation.
func TestNewServerWithStore_LegacyInjection(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workflow_legacy_inject.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite_compat", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite_compat: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := wfsqlitemigrations.Run(ctx, db); err != nil {
		t.Fatalf("apply workflow sqlite migrations: %v", err)
	}

	store := wfpg.New(db, true)

	cfg := &workflow.Config{
		Mode: workflow.ModeLite,
		SQLite: workflow.SQLiteConfig{
			Path: dbPath,
		},
	}

	srv, err := workflow.NewServerWithStore(cfg, store)
	if err != nil {
		t.Fatalf("NewServerWithStore: %v", err)
	}
	if srv == nil {
		t.Fatal("NewServerWithStore returned nil Server")
	}

	// Round-trip a rule to verify the legacy store works through injection.
	tag := uniqueName(t, "legacy-inject")
	r := &wfstore.Rule{
		RuleName:            tag,
		SourceAgent:         "agent-" + tag,
		SourceEvent:         "evt-" + tag,
		TriggerCondition:    "",
		TransformationStyle: "template-yaml",
		DestinationTemplate: "{}",
		Workspace:           "ws-" + tag,
		Priority:            1,
		Active:              true,
	}
	if err := store.CreateRule(ctx, r); err != nil {
		t.Fatalf("CreateRule via legacy injected store: %v", err)
	}
	if r.ID == 0 {
		t.Fatalf("CreateRule did not populate ID")
	}

	got, err := store.GetRule(ctx, r.ID)
	if err != nil {
		t.Fatalf("GetRule: %v", err)
	}
	if got == nil || got.RuleName != tag {
		t.Fatalf("GetRule round-trip failed: got %+v want RuleName=%q", got, tag)
	}
}
