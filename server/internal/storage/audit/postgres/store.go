// Package postgres provides the postgres-backed implementation of
// audit.Store. It is the Stage 1 wrapper that exposes the existing
// internal/audit.AuditLogger logic behind the new storage interface
// without any behavior change.
//
// The legacy *internal/audit.AuditLogger already implements every method
// on audit.Store one-for-one (same signatures, same returns), so this
// package contains nothing but a type alias and a thin constructor — no
// wrapping struct, no method forwarding, no behavioral drift risk.
//
// Stage 2 of the storage-interfaces plan (see
// `.slop/20260513_native-storage-interfaces.md` §3) introduces a sibling
// package `internal/storage/audit/sqlite` with a native-sqlite Store that
// satisfies the same audit.Store interface. At that point AetherLite stops
// going through dbcompat for audit reads/writes. The interface stays
// unchanged across that transition.
package postgres

import (
	"database/sql"

	legacy "github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/storage/audit"
)

// Store is the postgres-backed audit store. It is a direct type alias for
// the legacy *internal/audit.AuditLogger; no wrapping is needed because the
// legacy type's method set already matches audit.Store byte-for-byte. The
// alias keeps the new construction site (postgres.New) idiomatic while
// avoiding any indirection cost.
type Store = legacy.AuditLogger

// New constructs a postgres-backed audit Store on top of the given
// *sql.DB. The gatewayID is stamped on every event emitted by this logger
// (comprehensive_audit_log.gateway_id). If config is nil, DefaultConfig()
// is used.
//
// Callers retain ownership of db; Store.Close() does NOT close the
// underlying connection pool — it only stops the async writer goroutine.
func New(db *sql.DB, gatewayID string, config *audit.Config) *Store {
	return legacy.NewAuditLogger(db, gatewayID, config)
}

// Compile-time conformance asserts. These are load-bearing checks that
// *Store agrees on the full method set with audit.Store, and on the
// narrow write-only subset with audit.EventSink (used by the ACL layer).
var (
	_ audit.Store     = (*Store)(nil) // full interface
	_ audit.EventSink = (*Store)(nil) // narrow write-only interface used by ACL layer
)
