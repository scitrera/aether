// Package postgres provides the postgres-backed implementation of
// acl.Store. It is the Stage 1 wrapper that exposes the existing
// internal/acl.Service logic behind the new storage interface without any
// behavior change.
//
// The legacy *internal/acl.Service already implements every method on
// acl.Store one-for-one (same signatures, same returns), so this package
// contains nothing but a type alias and three thin constructors mirroring
// the legacy NewService / NewServiceWithAuditDB / NewServiceWithSharedAudit
// surface — no wrapping struct, no method forwarding, no behavioral drift
// risk.
//
// Stage 2 of the storage-interfaces plan (see
// `.slop/20260513_native-storage-interfaces.md` §3) introduces a sibling
// package `internal/storage/acl/sqlite` with a native-sqlite Store that
// satisfies the same acl.Store interface. At that point AetherLite stops
// going through dbcompat for ACL reads/writes. The interface stays
// unchanged across that transition.
package postgres

import (
	"database/sql"

	legacy "github.com/scitrera/aether/internal/acl"
	legacyaudit "github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/storage/acl"
)

// Store is the postgres-backed ACL store. It is a direct type alias for
// the legacy *internal/acl.Service; no wrapping is needed because the
// legacy type's method set already matches acl.Store byte-for-byte. The
// alias keeps the new construction site (postgres.New*) idiomatic while
// avoiding any indirection cost.
//
// Note on naming: the legacy package calls this type *Service* because it
// owns more than persistence (Casbin enforcer, fallback cache, audit
// adapter). The new convention (per
// `.slop/20260514_storage_interfaces_stage0.md` §2) uniformly names every
// domain's surface a *Store*; this alias bridges the two.
type Store = legacy.Service

// New constructs a postgres-backed ACL Store whose ACL rules and audit log
// both live in the same database (postgres path, or single-file
// aetherlite). The gatewayID is stamped on every ACL-decision audit row
// emitted by this store.
//
// COMPAT PATH: this constructor builds its own *legacyaudit.AuditLogger
// writer goroutine. Two writers (gateway audit + ACL audit) targeting the
// same comprehensive_audit_log table re-introduce the WAL writer-vs-writer
// contention this refactor exists to eliminate. Production
// gateway/aetherlite paths should call NewWithSharedAudit so a single
// writer drains both producers; this wrapper exists only for utility
// tooling (init-secrets, authproxy) where the audit batcher is short-lived
// or contention is not a concern.
//
// Callers retain ownership of db; Store.Close() does NOT close the
// underlying connection pool — it only stops the writer goroutine (when
// this constructor owns it) and tears down adapter references.
func New(db *sql.DB, gatewayID string) *Store {
	return legacy.NewService(db, gatewayID)
}

// NewWithAuditDB constructs an ACL Store backed by `db` for ACL state
// (acl_rules, acl_fallback_policies, acl_authority_grants) and by
// `auditDB` for the comprehensive_audit_log writer. Used in deployments
// where the ACL state and audit log live in different physical files
// (e.g. AetherLite's aether.db + audit.db split) but the caller does not
// pre-construct an audit writer.
//
// COMPAT PATH: like New, this constructor builds its own audit writer
// goroutine. See the New() doc-comment for the contention caveat.
func NewWithAuditDB(db, auditDB *sql.DB, gatewayID string) *Store {
	return legacy.NewServiceWithAuditDB(db, auditDB, gatewayID)
}

// NewWithSharedAudit constructs an ACL Store that funnels audit writes
// through `sharedAudit` (owned by the caller — typically the gateway
// constructed it at startup). `db` carries ACL rule state; `auditDB` is
// the read-side handle for ACL audit queries and must point at the same
// physical file as `sharedAudit` (audit.db in lite, aether.db in
// postgres). Pass the same *sql.DB for db/auditDB in single-file
// deployments.
//
// This is the contention-free path: only one batched writer goroutine
// (the shared one) ever touches comprehensive_audit_log. Production
// gateway/aetherlite use this constructor.
//
// The sharedAudit parameter is typed as audit.EventSink (the narrow
// write-only interface) rather than a concrete type. Both the legacy
// *audit.AuditLogger and the native-sqlite *auditsqlite.Store satisfy
// EventSink, so Wave 3 can cut over audit to the native impl by
// changing only the construction site in cmd/aetherlite — this
// constructor does not need to change.
func NewWithSharedAudit(db *sql.DB, sharedAudit legacyaudit.EventSink, auditDB *sql.DB, gatewayID string) *Store {
	return legacy.NewServiceWithSharedAudit(db, sharedAudit, auditDB, gatewayID)
}

// Compile-time conformance assert. This is the load-bearing check that
// acl.Store and *Store agree on the full method set. If a method is
// added to acl.Store or its signature changes, the build breaks here.
var _ acl.Store = (*Store)(nil)
