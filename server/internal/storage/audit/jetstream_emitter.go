// JetStreamAuditEmitter is a Store decorator that publishes audit events to a
// JetStream stream in addition to whatever the inner store does (typically
// SQLite or Postgres). Both writes happen on the same hot-path call; failures
// on the JetStream side are logged best-effort and never returned to the caller.
//
// Stream schema: the "audit" JetStream stream captures all subjects matching
// "audit.>" (MaxAge 7 days, file storage). Subject per event is derived from
// the aether topic "audit::{workspace}::{event_type}" via
// natscodec.ToNATSSubject, which produces "audit.{ws-esc}.{event_type-esc}".
//
// Workspace fallback: events whose Workspace field is empty land on the
// "_system" workspace token so every subject is well-formed.
//
// Event-type fallback: events whose EventType field is empty land on the
// "unknown" token.
//
// The emitter satisfies the Store interface and is a transparent decorator:
// QueryAuditLog, CleanupOldLogs, GetConfig, and Close all delegate to the inner.

package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/router/natscodec"
)

// auditStreamName is the JetStream stream name for audit events.
const auditStreamName = "audit"

// auditStreamSubjectFilter is the wildcard that the "audit" stream captures.
const auditStreamSubjectFilter = "audit.>"

// auditStreamMaxAge is the default retention window for audit events in JetStream.
const auditStreamMaxAge = 7 * 24 * time.Hour

// auditWorkspaceFallback is used when AuditEvent.Workspace is empty.
const auditWorkspaceFallback = "_system"

// auditEventTypeFallback is used when AuditEvent.EventType is empty.
const auditEventTypeFallback = "unknown"

// JSAuditLogger is the minimal logger interface for JetStreamAuditEmitter
// diagnostic output. Only JetStream-side failures are routed here; these are
// best-effort and are never returned to callers. Pass nil to silence all
// diagnostic output.
type JSAuditLogger interface {
	Warnf(format string, args ...any)
}

// JetStreamAuditEmitter wraps an inner Store, publishing each audit event to
// JetStream in addition to the inner write. It satisfies the full Store
// interface and is a transparent decorator.
//
// Thread-safety: the emitter holds no mutable state of its own. The inner
// Store and the JetStream client are independently goroutine-safe per their
// own contracts.
type JetStreamAuditEmitter struct {
	inner Store
	js    jetstream.JetStream
	log   JSAuditLogger
}

// Compile-time interface check.
var _ Store = (*JetStreamAuditEmitter)(nil)

// NewJetStreamAuditEmitter constructs the decorator. The "audit" JetStream
// stream is created (or updated) idempotently on construction with a 7-day
// MaxAge and file storage. The stream creation uses the supplied ctx; after
// the constructor returns, no background goroutines are held by the emitter.
//
// Parameters:
//   - ctx      — used only for the idempotent stream-create call.
//   - inner    — the underlying Store (SQLite or Postgres); must not be nil.
//   - js       — JetStream context; must not be nil.
//   - replicas — JetStream replica count. Values < 1 are clamped to 1.
//   - log      — optional diagnostic logger for JetStream-side failures;
//     nil is tolerated (failures are silently dropped).
func NewJetStreamAuditEmitter(
	ctx context.Context,
	inner Store,
	js jetstream.JetStream,
	replicas int,
	log JSAuditLogger,
) (*JetStreamAuditEmitter, error) {
	if inner == nil {
		return nil, fmt.Errorf("jetstream_audit_emitter: inner store is required")
	}
	if js == nil {
		return nil, fmt.Errorf("jetstream_audit_emitter: js is required")
	}
	if replicas < 1 {
		replicas = 1
	}

	// Idempotently create (or update) the "audit" stream.
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      auditStreamName,
		Subjects:  []string{auditStreamSubjectFilter},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    auditStreamMaxAge,
		Storage:   jetstream.FileStorage,
		Replicas:  replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream_audit_emitter: create/update stream %q: %w", auditStreamName, err)
	}

	return &JetStreamAuditEmitter{
		inner: inner,
		js:    js,
		log:   log,
	}, nil
}

// auditSubject builds the NATS subject for an audit event.
// Subject schema: audit.{ws-esc}.{event_type-esc}
func auditSubject(workspace, eventType string) string {
	if workspace == "" {
		workspace = auditWorkspaceFallback
	}
	if eventType == "" {
		eventType = auditEventTypeFallback
	}
	// Construct an aether-style topic with "::" separators, then translate to
	// a NATS subject (natscodec replaces "::" with "." and escapes tokens).
	aetherTopic := fmt.Sprintf("audit::%s::%s", workspace, eventType)
	return natscodec.ToNATSSubject(aetherTopic)
}

// publishEvent marshals the event to JSON and publishes it to JetStream.
// Failures are logged best-effort and never returned to the caller.
func (e *JetStreamAuditEmitter) publishEvent(ctx context.Context, event *Event) {
	subject := auditSubject(event.Workspace, event.EventType)

	payload, err := json.Marshal(event)
	if err != nil {
		if e.log != nil {
			e.log.Warnf("jetstream_audit_emitter: marshal event (type=%s workspace=%s): %v",
				event.EventType, event.Workspace, err)
		}
		return
	}

	if _, err := e.js.Publish(ctx, subject, payload); err != nil {
		if e.log != nil {
			e.log.Warnf("jetstream_audit_emitter: publish to %q: %v", subject, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Store interface implementation
// ---------------------------------------------------------------------------

// LogEvent enqueues the event in the inner store (async, non-blocking) and
// then publishes it to JetStream in a goroutine. JetStream publish failures
// are logged best-effort and never returned.
func (e *JetStreamAuditEmitter) LogEvent(ctx context.Context, event *Event) {
	// Inner write first (fast, non-blocking enqueue).
	e.inner.LogEvent(ctx, event)

	// Best-effort JetStream publish. Use a detached background context so
	// caller cancellation does not abort the publish unnecessarily.
	go e.publishEvent(context.Background(), event)
}

// LogEventSync writes the event synchronously to the inner store (blocking)
// and then publishes it to JetStream on the caller's context. Inner write
// errors ARE returned; JetStream publish failures are logged best-effort only.
func (e *JetStreamAuditEmitter) LogEventSync(ctx context.Context, event *Event) error {
	// Authoritative synchronous write.
	if err := e.inner.LogEventSync(ctx, event); err != nil {
		return err
	}

	// Best-effort JetStream publish on the caller's context.
	e.publishEvent(ctx, event)
	return nil
}

// Close flushes the inner store and stops its writer goroutine.
func (e *JetStreamAuditEmitter) Close() error {
	return e.inner.Close()
}

// QueryAuditLog delegates to the inner store.
func (e *JetStreamAuditEmitter) QueryAuditLog(ctx context.Context, filter EventFilter) ([]*Event, error) {
	return e.inner.QueryAuditLog(ctx, filter)
}

// CleanupOldLogs delegates to the inner store.
func (e *JetStreamAuditEmitter) CleanupOldLogs(ctx context.Context, retentionDays int) (int64, error) {
	return e.inner.CleanupOldLogs(ctx, retentionDays)
}

// GetConfig delegates to the inner store.
func (e *JetStreamAuditEmitter) GetConfig() *Config {
	return e.inner.GetConfig()
}
