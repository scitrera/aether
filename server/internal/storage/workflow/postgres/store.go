// Package postgres provides the postgres-backed implementation of
// workflow.Store. It is the Stage 1 wrapper that exposes the existing
// internal/workflow.Store logic behind the new storage interface without
// any behavior change.
//
// The legacy *internal/workflow.Store already implements every method on
// workflow.Store one-for-one (same signatures, same returns), so this
// package contains nothing but a type alias and a thin constructor — no
// wrapping struct, no method forwarding, no behavioral drift risk.
//
// Stage 1 quirk — the isSQLite bool: the legacy constructor takes a
// dialect-flag bool that the impl uses for inline branching (notably
// `FOR UPDATE SKIP LOCKED` on GetDueSchedules, which postgres supports
// and sqlite does not). Stage 1 preserves the flag on New per
// `.slop/20260513_native-storage-interfaces.md` §9 and the Stage 0
// decisions doc — postgres callers pass `isSQLite=false`, lite callers
// pass `isSQLite=true`, and dbcompat continues to translate the
// postgres-flavored SQL the impl emits for sqlite. Stage 2 splits this
// package and `internal/storage/workflow/sqlite` into siblings that each
// drop the flag (sqlite uses native idioms instead of dbcompat).
//
// Stage 2 of the storage-interfaces plan (see
// `.slop/20260513_native-storage-interfaces.md` §3) introduces a sibling
// package `internal/storage/workflow/sqlite` with a native-sqlite Store
// that satisfies the same workflow.Store interface. At that point
// AetherLite stops going through dbcompat for the workflow domain. The
// interface stays unchanged across that transition.
package postgres

import (
	"database/sql"

	"github.com/scitrera/aether/internal/storage/workflow"
	legacy "github.com/scitrera/aether/internal/workflow"
)

// Store is the postgres-backed workflow store. It is a direct type alias
// for the legacy *internal/workflow.Store; no wrapping is needed because
// the legacy type's method set already matches workflow.Store
// byte-for-byte. The alias keeps the new construction site (postgres.New)
// idiomatic while avoiding any indirection cost.
type Store = legacy.Store

// New constructs a postgres-backed workflow Store on top of the given
// *sql.DB. The isSQLite flag controls inline dialect branching inside the
// legacy impl — see the package doc for the Stage 1 rationale.
//
// Callers retain ownership of db; the Store has no Close method (it does
// not own background goroutines).
func New(db *sql.DB, isSQLite bool) *Store {
	return legacy.NewStore(db, isSQLite)
}

// Compile-time conformance assert. This is the load-bearing check that
// workflow.Store and *Store agree on the full method set. If a method is
// added to workflow.Store or its signature changes, the build breaks
// here.
var _ workflow.Store = (*Store)(nil)
