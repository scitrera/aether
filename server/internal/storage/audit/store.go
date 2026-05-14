// Package audit defines the storage interface for the comprehensive audit log
// subsystem (comprehensive_audit_log table + related views and helpers).
//
// Stage 1 consumers (callers that depend on this interface today):
//   - cmd/gateway/main.go       — constructs the postgres-backed impl
//   - cmd/aetherlite/main.go    — constructs the postgres-backed impl behind
//     the sqlite_compat translation layer (until
//     Stage 2 introduces a native sqlite sibling)
//   - internal/gateway/server.go — holds the *audit.Store handle, threads it
//     through to ACL adapter, admin handlers, and
//     the message/connection plumbing
//
// The interface intentionally mirrors the legacy *internal/audit.AuditLogger
// method set one-for-one. This is the mechanical-extraction phase of the
// storage refactor described in
// `.slop/20260513_native-storage-interfaces.md` §2/§3/§13: postgres impl is
// byte-for-byte the same logic, just re-homed behind an interface so a
// future sqlite-native sibling (Stage 2) can drop in.
package audit

import (
	"context"
)

// Store is the audit-log surface consumed by the gateway. It records audit
// events (async via LogEvent or synchronously via LogEventSync), serves the
// admin query path (QueryAuditLog), and exposes retention-cleanup hooks.
//
// Nil-tolerance policy (§14.1 of the storage-interfaces plan): callers MUST
// pass a non-nil implementation. Audit logging is a load-bearing security
// surface — silent nil-deref hazards (the chat-message SIGSEGV pattern that
// inspired this refactor) and silent typed-nil-via-failed-assertion hazards
// (the cleanup-leader-election degradation pattern) are both unacceptable
// here. There is no defensible "opt-out" mode for audit: a deployment that
// wants to disable audit writes does so via Config.Enabled=false on a real
// impl, which short-circuits inside the impl while keeping the contract
// non-nil and the method calls cheap. No NoOp impl is provided in this
// domain.
//
// Lifecycle: Close() flushes any in-flight async batch and stops the writer
// goroutine. Calling Close more than once is safe (the underlying base
// logger uses sync.Once).
type Store interface {
	// LogEvent enqueues an audit event for async batched write. Returns
	// immediately. Drops the event if the channel buffer is full (the
	// performance safety valve documented in BaseLogger.Enqueue).
	//
	// If audit is disabled (Config.Enabled=false) or the event type is not
	// in Config.EnabledEventTypes, this is a no-op.
	LogEvent(ctx context.Context, event *Event)

	// LogEventSync writes a single audit event synchronously. Use only for
	// events that must be persisted before the caller proceeds (e.g. tests,
	// migrations, security-critical events that must not be dropped).
	//
	// Returns ErrEventNotEnabled if the event type is disabled. Returns nil
	// if Config.Enabled=false (matches LogEvent's no-op behavior).
	LogEventSync(ctx context.Context, event *Event) error

	// Close stops the async writer, flushing any remaining batched events.
	// Safe to call multiple times; subsequent calls are no-ops.
	Close() error

	// QueryAuditLog retrieves audit events matching the filter, ordered by
	// timestamp DESC. Supports pagination via Limit/Offset.
	QueryAuditLog(ctx context.Context, filter EventFilter) ([]*Event, error)

	// CleanupOldLogs deletes audit log entries older than retentionDays and
	// returns the number of rows removed. Implementations may differ in
	// how they execute the delete (postgres uses a stored function;
	// sqlite-native, in Stage 2, will do a parameterized DELETE).
	CleanupOldLogs(ctx context.Context, retentionDays int) (int64, error)

	// GetConfig returns the active audit configuration. Callers MUST NOT
	// mutate the returned pointer; treat as read-only.
	GetConfig() *Config
}
