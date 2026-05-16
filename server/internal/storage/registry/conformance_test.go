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
	stderrors "errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	legacyregistry "github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/storage/registry"
	registrypg "github.com/scitrera/aether/internal/storage/registry/postgres"
	registrysqlite "github.com/scitrera/aether/internal/storage/registry/sqlite"
	"github.com/scitrera/aether/internal/testutil"
	sqliteregistrymigrations "github.com/scitrera/aether/migrations/sqlite_registry"
	aethererrors "github.com/scitrera/aether/pkg/errors"
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
		{name: "sqlite_native", factory: sqliteNativeFactory},
	}

	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			t.Run("AgentRegistry_Roundtrip", func(t *testing.T) {
				store, supports, cleanup := b.factory(t)
				defer cleanup()
				runAgentRegistryRoundtrip(t, store, supports)
			})
			t.Run("AgentRegistry_Phase5_Roundtrip", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAgentRegistryPhase5Roundtrip(t, store)
			})
			t.Run("AgentRegistry_PrefixConflict", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAgentRegistryPrefixConflict(t, store)
			})
			t.Run("AgentRegistry_PrefixIndexLookup", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAgentRegistryPrefixIndexLookup(t, store)
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

// runAgentRegistryPhase5Roundtrip exercises the Phase 5 resource_schema,
// capabilities, and extensions columns end-to-end: Register with non-nil
// values → Get returns them → List returns them → re-Register with different
// values → Get reflects the update. Stage A only verifies that the data model
// round-trips through every backend; uniqueness enforcement on
// resource_type_prefix lives in Stage B and is not tested here.
func runAgentRegistryPhase5Roundtrip(t *testing.T, store registry.Store) {
	t.Helper()
	ctx := context.Background()

	impl := uniqueTag(t, "agent-phase5")
	profile := "k8s"
	resourceSchema := []registry.AgentResourceSchemaEntry{
		{
			ResourceTypePrefix: "chat/",
			PermissionVerbs:    []string{"read", "write"},
			ResourceIDSchema:   `{"type":"string","pattern":"^[a-z0-9-]+$"}`,
		},
		{
			ResourceTypePrefix: "docmgmt/document",
			PermissionVerbs:    []string{"read", "write", "admin"},
		},
	}
	capabilities := map[string]bool{
		"streaming":            true,
		"hibernation_aware":    false,
		"extensions_supported": true,
	}
	extensions := []string{
		"https://example.com/ext/a2a/streaming",
		"https://example.com/ext/a2a/auth",
	}

	reg := &registry.AgentRegistration{
		Implementation: impl,
		LaunchParams: map[string]interface{}{
			"profile": profile,
			"image":   "ghcr.io/example/agent:latest",
		},
		Description:    "phase5 round-trip agent",
		ResourceSchema: resourceSchema,
		Capabilities:   capabilities,
		Extensions:     extensions,
	}

	if err := store.Register(ctx, reg); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := store.Get(ctx, impl)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertPhase5Fields(t, "Get", got, resourceSchema, capabilities, extensions)

	listAll, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	var found *registry.AgentRegistration
	for _, r := range listAll {
		if r.Implementation == impl {
			found = r
			break
		}
	}
	if found == nil {
		t.Fatalf("List did not return %q", impl)
	}
	assertPhase5Fields(t, "List", found, resourceSchema, capabilities, extensions)

	// Update: drop one resource family, flip a capability, replace extensions.
	updatedSchema := []registry.AgentResourceSchemaEntry{
		{
			ResourceTypePrefix: "workflow/run",
			PermissionVerbs:    []string{"read"},
		},
	}
	updatedCaps := map[string]bool{"streaming": false}
	updatedExts := []string{"https://example.com/ext/a2a/v2"}

	reg2 := &registry.AgentRegistration{
		Implementation: impl,
		LaunchParams: map[string]interface{}{
			"profile": profile,
			"image":   "ghcr.io/example/agent:next",
		},
		Description:    "phase5 round-trip agent (updated)",
		ResourceSchema: updatedSchema,
		Capabilities:   updatedCaps,
		Extensions:     updatedExts,
	}
	if err := store.Register(ctx, reg2); err != nil {
		t.Fatalf("Register (update): %v", err)
	}

	got2, err := store.Get(ctx, impl)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	assertPhase5Fields(t, "GetAfterUpdate", got2, updatedSchema, updatedCaps, updatedExts)

	// Empty/nil values on a subsequent update should clear the columns back
	// to NULL — exercising the encodeNullableJSON / encodePhase5JSON path.
	reg3 := &registry.AgentRegistration{
		Implementation: impl,
		LaunchParams: map[string]interface{}{
			"profile": profile,
		},
		Description: "phase5 cleared",
	}
	if err := store.Register(ctx, reg3); err != nil {
		t.Fatalf("Register (clear): %v", err)
	}
	got3, err := store.Get(ctx, impl)
	if err != nil {
		t.Fatalf("Get after clear: %v", err)
	}
	if len(got3.ResourceSchema) != 0 {
		t.Fatalf("expected nil ResourceSchema after clear, got %+v", got3.ResourceSchema)
	}
	if len(got3.Capabilities) != 0 {
		t.Fatalf("expected nil Capabilities after clear, got %+v", got3.Capabilities)
	}
	if len(got3.Extensions) != 0 {
		t.Fatalf("expected nil Extensions after clear, got %+v", got3.Extensions)
	}

	if err := store.Delete(ctx, impl); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// runAgentRegistryPrefixConflict verifies Phase 5 Stage B uniqueness
// enforcement: two registrations may not claim the same resource_type_prefix
// simultaneously, but releasing a prefix (via an update that drops it) allows
// another registration to claim it.
func runAgentRegistryPrefixConflict(t *testing.T, store registry.Store) {
	t.Helper()
	ctx := context.Background()

	implA := uniqueTag(t, "prefconfA")
	implB := uniqueTag(t, "prefconfB")
	prefix := uniqueTag(t, "prefix") + "/"
	altPrefix := uniqueTag(t, "altprefix") + "/"

	// Register A claiming "prefix/"
	regA := &registry.AgentRegistration{
		Implementation: implA,
		LaunchParams:   map[string]interface{}{"profile": "k8s"},
		ResourceSchema: []registry.AgentResourceSchemaEntry{
			{ResourceTypePrefix: prefix, PermissionVerbs: []string{"read"}},
		},
	}
	if err := store.Register(ctx, regA); err != nil {
		t.Fatalf("Register A: %v", err)
	}
	defer func() { _ = store.Delete(ctx, implA) }()

	// Register B claiming the same prefix → expect ResourceTypePrefixConflictError.
	regB := &registry.AgentRegistration{
		Implementation: implB,
		LaunchParams:   map[string]interface{}{"profile": "k8s"},
		ResourceSchema: []registry.AgentResourceSchemaEntry{
			{ResourceTypePrefix: prefix, PermissionVerbs: []string{"read"}},
		},
	}
	err := store.Register(ctx, regB)
	if err == nil {
		_ = store.Delete(ctx, implB)
		t.Fatalf("Register B: expected ResourceTypePrefixConflictError, got nil")
	}
	var conflict *aethererrors.ResourceTypePrefixConflictError
	if !stderrors.As(err, &conflict) {
		t.Fatalf("Register B: expected ResourceTypePrefixConflictError, got %T: %v", err, err)
	}
	if conflict.Prefix != prefix {
		t.Fatalf("conflict.Prefix = %q, want %q", conflict.Prefix, prefix)
	}
	if conflict.Existing != implA {
		t.Fatalf("conflict.Existing = %q, want %q", conflict.Existing, implA)
	}

	// Register B with a different (non-colliding) prefix → expect success.
	regB.ResourceSchema = []registry.AgentResourceSchemaEntry{
		{ResourceTypePrefix: altPrefix, PermissionVerbs: []string{"write"}},
	}
	if err := store.Register(ctx, regB); err != nil {
		t.Fatalf("Register B with altPrefix: %v", err)
	}
	defer func() { _ = store.Delete(ctx, implB) }()

	// Update A to drop "prefix/" — releases it for future claims.
	regA.ResourceSchema = nil
	if err := store.Register(ctx, regA); err != nil {
		t.Fatalf("Register A (drop prefix): %v", err)
	}

	// Now B can extend its schema to include the previously contested prefix.
	regB.ResourceSchema = []registry.AgentResourceSchemaEntry{
		{ResourceTypePrefix: altPrefix},
		{ResourceTypePrefix: prefix},
	}
	if err := store.Register(ctx, regB); err != nil {
		t.Fatalf("Register B claiming released prefix: %v", err)
	}

	// Self-conflict (same prefix declared twice in one registration) must be
	// rejected at input validation, regardless of the live table.
	regSelf := &registry.AgentRegistration{
		Implementation: uniqueTag(t, "self"),
		LaunchParams:   map[string]interface{}{"profile": "k8s"},
		ResourceSchema: []registry.AgentResourceSchemaEntry{
			{ResourceTypePrefix: "dup/"},
			{ResourceTypePrefix: "dup/"},
		},
	}
	if err := store.Register(ctx, regSelf); err == nil {
		_ = store.Delete(ctx, regSelf.Implementation)
		t.Fatalf("Register self-conflict: expected error, got nil")
	} else if !stderrors.As(err, &conflict) {
		t.Fatalf("self-conflict error %v: want ResourceTypePrefixConflictError, got %T", err, err)
	}
}

// runAgentRegistryPrefixIndexLookup registers two agents with non-overlapping
// resource_type_prefix declarations and asserts that registry.PrefixIndex
// resolves resource types under each prefix to the right owning agent. The
// uniqueness check is exercised implicitly: both Register calls must succeed.
func runAgentRegistryPrefixIndexLookup(t *testing.T, store registry.Store) {
	t.Helper()
	ctx := context.Background()

	implA := uniqueTag(t, "idxA")
	implB := uniqueTag(t, "idxB")
	prefixA := uniqueTag(t, "famA") + "/"
	prefixB := uniqueTag(t, "famB") + "/"

	regA := &registry.AgentRegistration{
		Implementation: implA,
		LaunchParams:   map[string]interface{}{"profile": "k8s"},
		ResourceSchema: []registry.AgentResourceSchemaEntry{
			{ResourceTypePrefix: prefixA},
		},
	}
	regB := &registry.AgentRegistration{
		Implementation: implB,
		LaunchParams:   map[string]interface{}{"profile": "k8s"},
		ResourceSchema: []registry.AgentResourceSchemaEntry{
			{ResourceTypePrefix: prefixB},
		},
	}
	if err := store.Register(ctx, regA); err != nil {
		t.Fatalf("Register A: %v", err)
	}
	defer func() { _ = store.Delete(ctx, implA) }()
	if err := store.Register(ctx, regB); err != nil {
		t.Fatalf("Register B: %v", err)
	}
	defer func() { _ = store.Delete(ctx, implB) }()

	// Build a PrefixIndex from the live registry state and verify Lookup
	// routes resource types under each prefix to the right owner.
	all, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	idx := legacyregistry.NewPrefixIndex()
	idx.Rebuild(all)

	if impl, _, ok := idx.Lookup(prefixA + "thing/xyz"); !ok || impl != implA {
		t.Fatalf("Lookup(%q): got impl=%q ok=%v, want %q true", prefixA+"thing/xyz", impl, ok, implA)
	}
	if impl, _, ok := idx.Lookup(prefixB + "thing/xyz"); !ok || impl != implB {
		t.Fatalf("Lookup(%q): got impl=%q ok=%v, want %q true", prefixB+"thing/xyz", impl, ok, implB)
	}
	if _, _, ok := idx.Lookup("nonexistent-resource/abc"); ok {
		t.Fatalf("Lookup unknown resource: ok=true, want false")
	}

	// Delete A and verify the index releases its prefix when rebuilt.
	if err := store.Delete(ctx, implA); err != nil {
		t.Fatalf("Delete A: %v", err)
	}
	all2, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	idx.Rebuild(all2)
	if _, _, ok := idx.Lookup(prefixA + "thing/xyz"); ok {
		t.Fatalf("Lookup(%q) after delete: ok=true, want false", prefixA+"thing/xyz")
	}
	if impl, _, ok := idx.Lookup(prefixB + "thing/xyz"); !ok || impl != implB {
		t.Fatalf("Lookup(%q) after A delete: got %q ok=%v, want %q true", prefixB+"thing/xyz", impl, ok, implB)
	}
}

// assertPhase5Fields compares the Phase 5 fields on a retrieved registration
// against expected values. Resource schema is compared entry-by-entry by
// prefix because the storage layer doesn't guarantee slice order (sqlite +
// postgres both preserve insertion order today, but the API contract is
// "set of entries").
func assertPhase5Fields(t *testing.T, label string, got *registry.AgentRegistration,
	wantSchema []registry.AgentResourceSchemaEntry, wantCaps map[string]bool, wantExts []string,
) {
	t.Helper()
	if len(got.ResourceSchema) != len(wantSchema) {
		t.Fatalf("%s: ResourceSchema length got %d want %d", label, len(got.ResourceSchema), len(wantSchema))
	}
	gotByPrefix := make(map[string]registry.AgentResourceSchemaEntry, len(got.ResourceSchema))
	for _, e := range got.ResourceSchema {
		gotByPrefix[e.ResourceTypePrefix] = e
	}
	for _, want := range wantSchema {
		g, ok := gotByPrefix[want.ResourceTypePrefix]
		if !ok {
			t.Fatalf("%s: ResourceSchema missing prefix %q", label, want.ResourceTypePrefix)
		}
		if g.ResourceIDSchema != want.ResourceIDSchema {
			t.Fatalf("%s: ResourceIDSchema for %q got %q want %q",
				label, want.ResourceTypePrefix, g.ResourceIDSchema, want.ResourceIDSchema)
		}
		if !stringsEqual(g.PermissionVerbs, want.PermissionVerbs) {
			t.Fatalf("%s: PermissionVerbs for %q got %v want %v",
				label, want.ResourceTypePrefix, g.PermissionVerbs, want.PermissionVerbs)
		}
	}
	if len(got.Capabilities) != len(wantCaps) {
		t.Fatalf("%s: Capabilities length got %d want %d", label, len(got.Capabilities), len(wantCaps))
	}
	for k, v := range wantCaps {
		if gv, ok := got.Capabilities[k]; !ok || gv != v {
			t.Fatalf("%s: Capabilities[%q] got %v ok=%v want %v", label, k, gv, ok, v)
		}
	}
	if !stringsEqual(got.Extensions, wantExts) {
		t.Fatalf("%s: Extensions got %v want %v", label, got.Extensions, wantExts)
	}
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

// sqliteNativeFactory builds a registry.Store using the Stage 2 native-sqlite
// implementation (internal/storage/registry/sqlite). It uses the bare "sqlite"
// driver (modernc.org/sqlite) with its own per-domain migration tree — no
// dbcompat, no postgres SQL rewriting. This is the target state for AetherLite.
//
// All capabilities are supported because the native sqlite impl rewrites
// every query that the dbcompat-wrapped postgres path couldn't handle:
//
//   - activeProfileFilter: staleness cutoff computed in Go, compared as TEXT
//     (no INTERVAL literal needed).
//   - staleCleanup: same — Go-computed cutoff, simple WHERE clause.
//   - launchParamsLookup: uses three separate ? placeholders (no repeated-$N
//     problem).
func sqliteNativeFactory(t *testing.T) (registry.Store, caps, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "registry_native.db")
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}

	stateDB, closeBadger := openTestBadger(t)
	state := legacyregistry.NewBadgerProfileStateStore(stateDB)

	store, err := registrysqlite.New(db, state, sqliteregistrymigrations.MigrationFS)
	if err != nil {
		closeBadger()
		_ = db.Close()
		t.Fatalf("registrysqlite.New: %v", err)
	}

	cleanup := func() {
		closeBadger()
		_ = db.Close()
	}
	return store, caps{
		activeProfileFilter: true,
		staleCleanup:        true,
		launchParamsLookup:  true,
	}, cleanup
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
