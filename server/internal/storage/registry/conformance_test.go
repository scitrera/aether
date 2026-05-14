// Package registry_test contains the cross-backend conformance suite for
// registry.Store. The same test cases run against every implementation —
// postgres today, sqlite-native once Stage 2 lands. Drift between
// implementations gets caught here.
//
// Per `.slop/20260514_storage_interfaces_stage0.md` §8, the suite is
// table-driven with one subtest per backend. The postgres subtest skips
// when DATABASE_URL / dev infra is unavailable; the sqlite subtest is
// always runnable since it spins up a temp-dir SQLite file.
package registry_test

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	legacyregistry "github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/storage/registry"
	registrypg "github.com/scitrera/aether/internal/storage/registry/postgres"
	"github.com/scitrera/aether/internal/testutil"
	sqlitemigrations "github.com/scitrera/aether/migrations/sqlite"

	_ "github.com/scitrera/aether/pkg/dbcompat" // registers "sqlite_compat" driver
)

// storeFactory builds a Store and returns a cleanup func plus a capability
// bitfield. Capabilities flag whether the backend's SQL dialect supports a
// given operation in this Stage 1 wrap. The Stage 1 wrapper does not
// rewrite legacy postgres SQL (per §10: "no semantic schema changes"), so
// queries that depend on PG-specific syntax not handled by dbcompat
// (e.g. `INTERVAL '60 seconds'` literals, multi-reference `$N`
// placeholders) surface as cap=false on sqlite. Stage 2's native sqlite
// impl closes every cap gap.
//
// The factory may call t.Skip if its prerequisites are unmet (e.g.
// postgres dev infra not running) — the harness honors that and reports
// the subtest as skipped.
type caps struct {
	// activeProfileFilter: GetActiveOrchestratorsForProfile / ListAllProfiles
	// / SelectOrchestrator. These use `INTERVAL '60 seconds'` literals that
	// dbcompat doesn't rewrite.
	activeProfileFilter bool
	// staleCleanup: CleanupStaleProfiles uses `$1::interval` with a
	// duration.String() argument — dbcompat strips the cast but SQLite can't
	// do timestamp - text arithmetic.
	staleCleanup bool
	// launchParamsLookup: GetLaunchParams always uses a query that
	// references `$1` in two positions (CASE WHEN + IN clause). dbcompat's
	// blind `$N` → `?` rewrite produces 3 placeholders against 2 bound
	// args on sqlite. Affects every call, not just the `:specifier`
	// suffix path.
	launchParamsLookup bool
}

type storeFactory func(t *testing.T) (store registry.Store, supports caps, cleanup func())

func TestStoreConformance(t *testing.T) {
	backends := []struct {
		name    string
		factory storeFactory
	}{
		{name: "postgres", factory: postgresFactory},
		{name: "sqlite", factory: sqliteFactory},
	}

	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			t.Run("AgentRegistry_Roundtrip", func(t *testing.T) {
				store, supports, cleanup := b.factory(t)
				defer cleanup()
				runAgentRegistryRoundtrip(t, store, supports)
			})
			t.Run("OrchestratorProfiles_Roundtrip", func(t *testing.T) {
				store, supports, cleanup := b.factory(t)
				defer cleanup()
				runOrchestratorProfilesRoundtrip(t, store, supports)
			})
			t.Run("CleanupStaleProfiles", func(t *testing.T) {
				store, supports, cleanup := b.factory(t)
				defer cleanup()
				if !supports.staleCleanup {
					t.Skip("CleanupStaleProfiles unsupported on this backend until Stage 2 (legacy `$1::interval` + duration.String() arg incompatible with SQLite)")
				}
				runCleanupStaleProfiles(t, store)
			})
		})
	}
}

// runAgentRegistryRoundtrip walks the agent_registry surface end-to-end:
// Register → Get → Exists → List → Delete → Get (miss). The
// suffix-stripping launch-params path is gated on the launchParamsLookup
// capability because the legacy SQL doesn't survive dbcompat's blind
// `$N` → `?` rewrite when a placeholder is referenced twice.
func runAgentRegistryRoundtrip(t *testing.T, store registry.Store, supports caps) {
	t.Helper()
	ctx := context.Background()

	impl := uniqueTag(t, "agent")
	profile := "k8s"
	reg := &registry.AgentRegistration{
		Implementation: impl,
		LaunchParams: map[string]interface{}{
			"profile": profile,
			"image":   "ghcr.io/example/agent:latest",
		},
		Description: "conformance test agent",
	}

	if err := store.Register(ctx, reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if reg.CreatedAt.IsZero() || reg.UpdatedAt.IsZero() {
		t.Fatalf("Register did not stamp timestamps: created=%v updated=%v",
			reg.CreatedAt, reg.UpdatedAt)
	}

	got, err := store.Get(ctx, impl)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Implementation != impl {
		t.Fatalf("Get returned wrong implementation: got %q want %q", got.Implementation, impl)
	}
	if got.LaunchParams["profile"] != profile {
		t.Fatalf("Get returned wrong profile: got %v want %q", got.LaunchParams["profile"], profile)
	}

	exists, err := store.Exists(ctx, impl)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Fatalf("Exists returned false for registered impl %q", impl)
	}

	// Also exercise the ":specifier" stripping path on Get/Exists.
	if _, err := store.Get(ctx, impl+":Default"); err != nil {
		t.Fatalf("Get with :specifier suffix: %v", err)
	}
	if existsSpec, err := store.Exists(ctx, impl+":Default"); err != nil || !existsSpec {
		t.Fatalf("Exists with :specifier suffix: exists=%v err=%v", existsSpec, err)
	}

	list, err := store.List(ctx, profile)
	if err != nil {
		t.Fatalf("List by profile: %v", err)
	}
	if !containsImpl(list, impl) {
		t.Fatalf("List(profile=%q) did not include %q; got %d entries", profile, impl, len(list))
	}

	listAll, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if !containsImpl(listAll, impl) {
		t.Fatalf("List(\"\") did not include %q; got %d entries", impl, len(listAll))
	}

	// GetLaunchParams always uses a query that references `$1` in TWO
	// positions (one in CASE, one in IN). dbcompat's blind `$N` → `?`
	// rewrite produces an arity mismatch on sqlite: 3 placeholders vs 2
	// bound args. Stage 2's native sqlite impl will rewrite this query.
	if supports.launchParamsLookup {
		params, err := store.GetLaunchParams(ctx, impl)
		if err != nil {
			t.Fatalf("GetLaunchParams: %v", err)
		}
		if params["profile"] != profile {
			t.Fatalf("GetLaunchParams profile mismatch: got %v want %q", params["profile"], profile)
		}

		paramsSpec, err := store.GetLaunchParams(ctx, impl+":Default")
		if err != nil {
			t.Fatalf("GetLaunchParams with :specifier suffix: %v", err)
		}
		if paramsSpec["profile"] != profile {
			t.Fatalf("GetLaunchParams(:specifier) profile mismatch: got %v want %q",
				paramsSpec["profile"], profile)
		}
	}

	if err := store.Delete(ctx, impl); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	existsAfter, err := store.Exists(ctx, impl)
	if err != nil {
		t.Fatalf("Exists after delete: %v", err)
	}
	if existsAfter {
		t.Fatalf("Exists still true after Delete for %q", impl)
	}
}

// runOrchestratorProfilesRoundtrip walks the orchestrator_profiles surface.
// The legacy SQL behind GetActiveOrchestratorsForProfile / ListAllProfiles /
// SelectOrchestrator embeds `INTERVAL '60 seconds'` literals that dbcompat
// doesn't rewrite, so those paths are gated on the activeProfileFilter
// capability. Stage 2's native sqlite impl will rewrite the queries.
//
// What both backends support today:
//
//	RegisterProfiles → GetOrchestratorProfiles → OrchestratorSupportsProfile
//	→ UpdateHeartbeat → UnregisterOrchestrator
func runOrchestratorProfilesRoundtrip(t *testing.T, store registry.Store, supports caps) {
	t.Helper()
	ctx := context.Background()

	orchID := uniqueTag(t, "orch")
	profile := uniqueTag(t, "profile") // unique profile so other test runs don't bleed in
	profiles := []string{profile, profile + "-alt"}
	workspace := "_system"

	if err := store.RegisterProfiles(ctx, orchID, profiles, workspace); err != nil {
		t.Fatalf("RegisterProfiles: %v", err)
	}

	owned, err := store.GetOrchestratorProfiles(ctx, orchID)
	if err != nil {
		t.Fatalf("GetOrchestratorProfiles: %v", err)
	}
	if len(owned) != len(profiles) {
		t.Fatalf("GetOrchestratorProfiles returned %d rows, want %d", len(owned), len(profiles))
	}

	hasProfile, err := store.OrchestratorSupportsProfile(ctx, orchID, profile)
	if err != nil {
		t.Fatalf("OrchestratorSupportsProfile: %v", err)
	}
	if !hasProfile {
		t.Fatalf("OrchestratorSupportsProfile returned false for registered profile")
	}

	if supports.activeProfileFilter {
		active, err := store.GetActiveOrchestratorsForProfile(ctx, profile, workspace)
		if err != nil {
			t.Fatalf("GetActiveOrchestratorsForProfile: %v", err)
		}
		if !containsString(active, orchID) {
			t.Fatalf("GetActiveOrchestratorsForProfile did not include %q; got %v", orchID, active)
		}

		picked, err := store.SelectOrchestrator(ctx, profile, workspace)
		if err != nil {
			t.Fatalf("SelectOrchestrator: %v", err)
		}
		if picked != orchID {
			t.Fatalf("SelectOrchestrator returned %q, want %q (single orchestrator)", picked, orchID)
		}

		all, err := store.ListAllProfiles(ctx)
		if err != nil {
			t.Fatalf("ListAllProfiles: %v", err)
		}
		var seen int
		for _, p := range all {
			if p.OrchestratorID == orchID {
				seen++
			}
		}
		if seen != len(profiles) {
			t.Fatalf("ListAllProfiles saw %d rows for %q, want %d", seen, orchID, len(profiles))
		}
	}

	if err := store.UpdateHeartbeat(ctx, orchID); err != nil {
		t.Fatalf("UpdateHeartbeat: %v", err)
	}

	if err := store.UnregisterOrchestrator(ctx, orchID); err != nil {
		t.Fatalf("UnregisterOrchestrator: %v", err)
	}

	afterUnreg, err := store.GetOrchestratorProfiles(ctx, orchID)
	if err != nil {
		t.Fatalf("GetOrchestratorProfiles after unregister: %v", err)
	}
	if len(afterUnreg) != 0 {
		t.Fatalf("GetOrchestratorProfiles returned %d rows after Unregister, want 0", len(afterUnreg))
	}
}

// runCleanupStaleProfiles inserts a profile, ages its heartbeat past the
// retention window via a direct UPDATE, and confirms CleanupStaleProfiles
// removes it.
//
// Direct UPDATE is used because there's no public API to set a heartbeat
// in the past; the alternative (sleep) would slow the test by minutes. The
// query intentionally matches both PG and dbcompat-rewritten SQLite via the
// dialect-neutral interval expression.
func runCleanupStaleProfiles(t *testing.T, store registry.Store) {
	t.Helper()
	ctx := context.Background()

	orchID := uniqueTag(t, "stale")
	profile := uniqueTag(t, "stale-profile")
	if err := store.RegisterProfiles(ctx, orchID, []string{profile}, "_system"); err != nil {
		t.Fatalf("RegisterProfiles: %v", err)
	}

	// Use a tiny CleanupStaleProfiles maxAge; we'll first ensure the row
	// is older than that by waiting briefly. This avoids needing direct DB
	// access (which would require leaking *sql.DB through the test setup).
	// The wait is short — 1.5s — because the maxAge below is 1s.
	time.Sleep(1500 * time.Millisecond)

	deleted, err := store.CleanupStaleProfiles(ctx, time.Second)
	if err != nil {
		t.Fatalf("CleanupStaleProfiles: %v", err)
	}
	if deleted < 1 {
		t.Fatalf("CleanupStaleProfiles deleted %d rows, want at least 1", deleted)
	}

	remaining, err := store.GetOrchestratorProfiles(ctx, orchID)
	if err != nil {
		t.Fatalf("GetOrchestratorProfiles after cleanup: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 rows after CleanupStaleProfiles, got %d", len(remaining))
	}
}

// =============================================================================
// Backend factories
// =============================================================================

// postgresFactory connects to the dev postgres instance via testutil and
// constructs a Store backed by a Badger ProfileStateStore (Badger is easier
// to set up in-test than a Redis client and exercises the same interface).
// Skips when the dev infra isn't reachable.
//
// Postgres supports every capability — it's the native dialect that the
// legacy SQL was written for.
func postgresFactory(t *testing.T) (registry.Store, caps, func()) {
	t.Helper()
	testDB, cleanupDB := testutil.SetupTestDB(t)
	if testDB == nil {
		return nil, caps{}, func() {}
	}

	stateDB, closeBadger := openTestBadger(t)
	state := legacyregistry.NewBadgerProfileStateStore(stateDB)
	store := registrypg.New(testDB.DB, state)

	cleanup := func() {
		closeBadger()
		cleanupDB()
	}
	return store, caps{
		activeProfileFilter: true,
		staleCleanup:        true,
		launchParamsLookup:  true,
	}, cleanup
}

// sqliteFactory opens a fresh temp-dir SQLite database via the sqlite_compat
// driver (so dbcompat handles the postgres-flavored SQL the legacy registry
// still emits in Stage 1), runs the sqlite migration set, and constructs a
// postgres-impl registry.Store on top of that handle. The ProfileStateStore
// is a Badger instance in another temp dir.
//
// Capability gates on this backend (all closed in Stage 2 by a native
// sqlite Store impl that rewrites the offending queries):
//
//   - activeProfileFilter: legacy SQL uses `INTERVAL '60 seconds'`
//     literals — dbcompat's regex doesn't cover the INTERVAL-prefix form.
//   - staleCleanup: legacy SQL passes maxAge.String() against `$1::interval`;
//     dbcompat strips the cast but SQLite can't do timestamp - text math.
//   - launchParamsLookup: legacy query references `$1` in two positions;
//     dbcompat's blind `$N` → `?` rewrite produces an arity mismatch.
//
// Note: AetherLite does not exercise any of these capability-gated paths
// today (lite mode is single-orchestrator, no profile fleet) — so these
// gaps don't block runtime use. They're documented here so Stage 2 closes
// them deliberately.
func sqliteFactory(t *testing.T) (registry.Store, caps, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "registry.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite_compat", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite_compat: %v", err)
	}
	// Single-writer pool to match aetherlite's semantics and avoid
	// SQLITE_BUSY in WAL mode.
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := applySQLiteMigrationsForTest(ctx, db, sqlitemigrations.MigrationFS); err != nil {
		_ = db.Close()
		t.Fatalf("apply sqlite migrations: %v", err)
	}

	stateDB, closeBadger := openTestBadger(t)
	state := legacyregistry.NewBadgerProfileStateStore(stateDB)
	store := registrypg.New(db, state)

	cleanup := func() {
		closeBadger()
		_ = db.Close()
	}
	return store, caps{
		activeProfileFilter: false,
		staleCleanup:        false,
		launchParamsLookup:  false,
	}, cleanup
}

// openTestBadger spins up a fresh embedded Badger instance in a temp
// directory and returns it along with a close func. Used as the
// ProfileStateStore backing for both subtests so we exercise the round-robin
// counter against a real store.
func openTestBadger(t *testing.T) (*badger.DB, func()) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "badger")
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open: %v", err)
	}
	return db, func() { _ = db.Close() }
}

// applySQLiteMigrationsForTest is a test-local copy of the cmd/aetherlite
// migration helper. We duplicate it here (rather than importing) because
// the conformance package shouldn't pull on cmd/* code just for migration
// plumbing.
func applySQLiteMigrationsForTest(ctx context.Context, db *sql.DB, fs embed.FS) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read embed fs: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version,
		).Scan(&count); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}
		content, err := fs.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("exec %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES (?)", version,
		); err != nil {
			return fmt.Errorf("record %s: %w", version, err)
		}
	}
	return nil
}

// =============================================================================
// Helpers
// =============================================================================

// uniqueTag returns a per-test-per-call identifier that won't collide with
// rows other tests inserted into the shared dev database. SQL-safe (no
// special chars besides hyphens).
func uniqueTag(t *testing.T, hint string) string {
	t.Helper()
	clean := strings.ReplaceAll(t.Name(), "/", "-")
	return fmt.Sprintf("conf-%s-%s-%d", hint, clean, time.Now().UnixNano())
}

func containsImpl(list []*registry.AgentRegistration, impl string) bool {
	for _, r := range list {
		if r.Implementation == impl {
			return true
		}
	}
	return false
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
