package audit

import "context"

// EventSink is the narrow interface that consumers needing only the
// fire-and-forget audit-write path depend on. It exposes exactly one
// method — LogEvent — which is the async, non-blocking enqueue that
// both the legacy *AuditLogger and the native-sqlite *auditsqlite.Store
// implement with identical semantics (enqueue to a batched writer,
// drop if the channel buffer is full).
//
// This interface exists to decouple the ACL layer from the concrete
// *AuditLogger type. Before this interface, the ACL→audit adapter
// (internal/acl.AuditLogger) took *audit.AuditLogger directly, which
// coupled ACL construction to the legacy concrete and blocked Wave 3's
// audit cut-over to the native-sqlite Store without also rewriting ACL
// constructors. With EventSink, ACL accepts any audit writer; the
// concrete is chosen at the construction site (cmd/gateway or
// cmd/aetherlite) and threaded through opaquely.
//
// Semantic contract:
//   - LogEvent MUST be safe to call from any goroutine.
//   - LogEvent MUST return immediately (non-blocking). If the impl's
//     internal buffer is full, the event is silently dropped (the
//     performance safety valve documented in BaseLogger.Enqueue).
//   - If audit is disabled (Config.Enabled=false) or the event type is
//     not in Config.EnabledEventTypes, LogEvent is a no-op.
//   - Callers MUST NOT assume persistence — LogEvent is best-effort.
//     Use LogEventSync (on the full audit.Store interface) when
//     guaranteed persistence is required.
//
// Why not audit.Store? ACL only calls LogEvent. Accepting the full
// Store interface (6 methods) would over-couple ACL to query/cleanup
// capabilities it never uses. Narrow interfaces are idiomatic Go (§15.2
// truth-in-naming: name it after what the consumer needs, not after the
// provider's full surface).
type EventSink interface {
	// LogEvent enqueues an audit event for async batched write.
	// See the type-level doc for the full semantic contract.
	LogEvent(ctx context.Context, event *AuditEvent)
}

// Compile-time conformance: *AuditLogger (legacy concrete) satisfies EventSink.
var _ EventSink = (*AuditLogger)(nil)
