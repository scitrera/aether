// Package postgres provides the postgres-backed implementation of
// tasks.Store. It is the Stage 1 wrapper that exposes the existing
// *pkg/tasks.TaskStore logic behind the new storage interface without any
// behavior change.
//
// The legacy *pkg/tasks.TaskStore already implements every method on
// tasks.Store one-for-one (same signatures, same returns), so this package
// contains nothing but a type alias and a thin constructor — no wrapping
// struct, no method forwarding, no behavioral drift risk.
//
// Stage 2 of the storage-interfaces plan (see
// `.slop/20260513_native-storage-interfaces.md` §3) introduces a sibling
// package `internal/storage/tasks/sqlite` with a native-sqlite Store that
// satisfies the same tasks.Store interface. At that point AetherLite stops
// going through dbcompat for task reads/writes. The interface stays
// unchanged across that transition (modulo the StoreTx swap noted in
// `tasks.Store.RecordAuditEventTx`).
package postgres

import (
	"database/sql"

	"github.com/scitrera/aether/internal/storage/tasks"
	legacy "github.com/scitrera/aether/pkg/tasks"
)

// Store is the postgres-backed task store. It is a direct type alias for the
// legacy *pkg/tasks.TaskStore; no wrapping is needed because the legacy
// type's method set already matches tasks.Store byte-for-byte. The alias
// keeps the new construction site (postgres.New) idiomatic while avoiding
// any indirection cost.
type Store = legacy.TaskStore

// New constructs a postgres-backed task Store on top of the given *sql.DB.
// Callers retain ownership of db; the store does not own connection-pool
// lifetime.
func New(db *sql.DB) *Store {
	return legacy.NewTaskStore(db)
}

// Compile-time conformance assert. This is the load-bearing check that
// tasks.Store and *Store agree on the full method set. If a method is added
// to tasks.Store or its signature changes, the build breaks here.
var _ tasks.Store = (*Store)(nil)
