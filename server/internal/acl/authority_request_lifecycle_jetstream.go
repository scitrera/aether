// Phase 2 Stage B (cluster mode): JetStream-backed authority-request lifecycle.
//
// This file layers JetStream KV state + a per-workspace event stream on top
// of the existing lifecycle service (authority_request_lifecycle.go).
// Approvers subscribe to per-workspace events in real-time instead of polling
// the SQLite/Postgres authority-request table.
//
// Layout per lifecycle transition (Submit / Approve / Deny / Cancel /
// SweepExpired):
//  1. The inner service performs the SQLite/Postgres write (preserves the
//     audit-log row + full-aether mode behavior). On failure the wrapper
//     short-circuits — no KV write, no event emission, consistency is
//     preserved.
//  2. The wrapper updates the corresponding KV bucket entry
//     (aether_authority_requests, keyed by request_id). CAS is enforced via
//     jetstream.KeyValue.Update(key, value, revision); on a stale-revision
//     conflict ErrAuthorityRequestConcurrentModification is returned and the
//     caller is expected to retry (re-read inner + retry the KV step). The
//     inner row remains authoritative — KV is best-effort cache + event-trigger.
//  3. The wrapper publishes a lifecycle event to JetStream subject
//     authreq.{ws-escaped}.events for approvers to subscribe to. Subjects are
//     produced via natscodec.ToNATSSubject so workspace tokens with NATS-
//     unsafe characters round-trip cleanly. Empty workspaces use the literal
//     "_" sentinel (matches encodeSegment in kv/jetstream_store.go).
//
// Reads (the hot path through CheckAccess / authority_context evaluation)
// stay on the inner service in this task; KV is write-only here. A later
// optimization can add a KV read-through cache without modifying CheckAccess.
//
// The wrapper preserves the inner service's method shapes exactly so it can
// be a drop-in replacement at the gateway-wiring choke point. The
// constructor takes the inner service as the Lifecycle interface so tests
// can substitute fakes when bootstrapping a full *acl.Service is heavy.

package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/router/natscodec"
	"github.com/scitrera/aether/pkg/models"
)

// Authority-request JetStream resource names. Centralized so callers (and
// the cluster-mode wiring in cmd/aetherlite/main.go) can reference the same
// constants when provisioning streams / KV buckets outside of this wrapper.
const (
	// AuthorityRequestsKVBucket is the NATS KV bucket name for authority-
	// request lifecycle state. Each key is a request_id; each value is a
	// JSON-encoded AuthorityRequest snapshot.
	AuthorityRequestsKVBucket = "aether_authority_requests"

	// AuthorityGrantsKVBucket is the NATS KV bucket name for authority-grant
	// state (mirror of acl_authority_grants, keyed by grant_id).
	AuthorityGrantsKVBucket = "aether_authority_grants"

	// AuthorityRequestsStream is the JetStream stream name for per-workspace
	// lifecycle events. Subjects: "authreq.>"; retention=Limits; MaxAge=7d.
	AuthorityRequestsStream = "authreq"

	// authorityRequestSubjectFilter is the JetStream subject filter for the
	// authreq stream — covers all per-workspace event subjects.
	authorityRequestSubjectFilter = "authreq.>"

	// authorityRequestStreamMaxAge bounds the per-workspace event retention.
	// Seven days matches the agentic-fabric protocol guide Phase 2 spec for
	// approver re-subscription windows.
	authorityRequestStreamMaxAge = 7 * 24 * time.Hour

	// authorityRequestCASMaxAttempts caps the retry loop for KV CAS races
	// inside a single lifecycle call (e.g. retrying after a Create-vs-Update
	// race when two operations race on the initial Submit). Cross-caller
	// races (two Approves on the same request) surface as
	// ErrAuthorityRequestConcurrentModification — caller is responsible.
	authorityRequestCASMaxAttempts = 4
)

// AuthorityRequestEventType enumerates the lifecycle transitions emitted on
// the per-workspace authreq.* subject. Values are stable JSON strings; the
// proto enum in api/proto/aether.proto (AuthorityRequestEvent.EventType) uses
// matching semantics but the wire encoding here is JSON so consumers do not
// pull in the proto descriptor at parse time.
type AuthorityRequestEventType string

const (
	AuthorityRequestEventTypeCreated   AuthorityRequestEventType = "created"
	AuthorityRequestEventTypeApproved  AuthorityRequestEventType = "approved"
	AuthorityRequestEventTypeDenied    AuthorityRequestEventType = "denied"
	AuthorityRequestEventTypeExpired   AuthorityRequestEventType = "expired"
	AuthorityRequestEventTypeCancelled AuthorityRequestEventType = "cancelled"
)

// AuthorityRequestLifecycleEvent is the JSON payload published to
// authreq.{ws}.events on each lifecycle transition. Fields are minimal so
// the wire format stays small; the full AuthorityRequest is embedded under
// `request` for consumers that need scope/timing details without a follow-up
// KV read.
type AuthorityRequestLifecycleEvent struct {
	EventType      AuthorityRequestEventType `json:"event_type"`
	RequestID      string                    `json:"request_id"`
	Workspace      string                    `json:"workspace,omitempty"`
	StatusFrom     AuthorityRequestStatus    `json:"status_from,omitempty"`
	StatusTo       AuthorityRequestStatus    `json:"status_to"`
	TimestampMs    int64                     `json:"timestamp_ms"`
	ActorPrincipal string                    `json:"actor_principal,omitempty"` // "<type>:<id>" or "" for system actors
	GrantID        string                    `json:"grant_id,omitempty"`        // populated on Approved
	Request        *AuthorityRequest         `json:"request,omitempty"`
}

// ErrAuthorityRequestConcurrentModification is returned by the wrapper when
// a CAS update on the KV bucket loses a race against a concurrent writer.
// The caller is expected to retry (re-read the inner state and retry the
// KV-side step). Distinct from ErrAuthorityRequestAlreadyResolved, which is
// surfaced by the inner service when the storage-layer transition is
// rejected for a row already in a terminal state.
var ErrAuthorityRequestConcurrentModification = errors.New("authority request concurrently modified")

// Lifecycle is the narrow interface the JetStream wrapper consumes. *acl.Service
// satisfies it via the existing methods on authority_request_lifecycle.go and
// authority_requests.go. The interface lives here (not in a sibling file) so
// callers wiring the cluster-mode service only have to import one package.
//
// Wrapper-relevant calls only: Get / Submit / Approve / Deny / Cancel-open /
// SweepExpired. The hot-path read methods (CheckAccess, ListAuthorityRequests)
// stay direct on *acl.Service and are not part of this surface.
type Lifecycle interface {
	GetAuthorityRequest(ctx context.Context, requestID string) (*AuthorityRequest, error)
	SubmitAuthorityRequest(ctx context.Context, req *AuthorityRequest) (*AuthorityRequest, error)
	ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *ApproveDecision) (*AuthorityRequest, error)
	DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*AuthorityRequest, error)
	CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*AuthorityRequest, error)
	SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*AuthorityRequest, error)
}

// Compile-time assertion: *Service satisfies Lifecycle.
var _ Lifecycle = (*Service)(nil)

// Logger is the minimal logger interface used by the wrapper. The global
// internal/logging.Logger satisfies it; tests pass a zerolog adapter or nil.
// nil is tolerated: failures inside the JetStream side-effect path are
// best-effort and never surface back to the caller (the inner service has
// already written the authoritative row).
type JSLogger interface {
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// JetStreamAuthorityLifecycle wraps an existing Lifecycle service, adding a
// per-workspace JetStream event stream and a KV mirror for fast watch-based
// approver subscription. The wrapper satisfies the Lifecycle interface
// itself so it can be a drop-in replacement at the gateway-wiring site.
//
// Thread-safety: the wrapper holds no mutable state of its own; the
// JetStream client + KV bucket handle are independently goroutine-safe.
type JetStreamAuthorityLifecycle struct {
	inner    Lifecycle
	js       jetstream.JetStream
	kv       jetstream.KeyValue
	replicas int
	log      JSLogger
}

// Compile-time assertion: the wrapper satisfies Lifecycle.
var _ Lifecycle = (*JetStreamAuthorityLifecycle)(nil)

// NewJetStreamAuthorityLifecycle constructs the wrapper. Both the KV bucket
// (aether_authority_requests) and the per-workspace event stream (authreq)
// are created idempotently on construction so callers can rely on them
// being present after the constructor returns.
//
// replicas is the JetStream replica count for both the KV bucket and the
// event stream. Pass 1 for single-node / A1 topologies; pass the cluster
// peer count (clamped to 3) for B/C topologies. Values <1 are clamped to 1.
//
// logger is optional (nil-tolerant). When non-nil it receives warnings for
// best-effort failures in the JetStream side-effects (KV write, event
// publish) that do NOT roll the inner write back.
func NewJetStreamAuthorityLifecycle(
	ctx context.Context,
	inner Lifecycle,
	js jetstream.JetStream,
	replicas int,
	logger JSLogger,
) (*JetStreamAuthorityLifecycle, error) {
	if inner == nil {
		return nil, errors.New("jetstream authority lifecycle: inner is required")
	}
	if js == nil {
		return nil, errors.New("jetstream authority lifecycle: js is required")
	}
	if replicas < 1 {
		replicas = 1
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   AuthorityRequestsKVBucket,
		History:  1,
		Replicas: replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream authority lifecycle: open KV bucket %s: %w", AuthorityRequestsKVBucket, err)
	}

	// Grants bucket is provisioned here as well so callers wiring the
	// cluster service in cmd/aetherlite get both buckets idempotently.
	// The wrapper itself does not write to the grants bucket — that lives
	// in a sibling implementation tied to CreateAuthorityGrant — but the
	// idempotent create here keeps the bootstrap responsibilities co-located.
	if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   AuthorityGrantsKVBucket,
		History:  1,
		Replicas: replicas,
	}); err != nil {
		return nil, fmt.Errorf("jetstream authority lifecycle: open KV bucket %s: %w", AuthorityGrantsKVBucket, err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      AuthorityRequestsStream,
		Subjects:  []string{authorityRequestSubjectFilter},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    authorityRequestStreamMaxAge,
		Storage:   jetstream.FileStorage,
		Replicas:  replicas,
	}); err != nil {
		return nil, fmt.Errorf("jetstream authority lifecycle: ensure stream %s: %w", AuthorityRequestsStream, err)
	}

	return &JetStreamAuthorityLifecycle{
		inner:    inner,
		js:       js,
		kv:       kv,
		replicas: replicas,
		log:      logger,
	}, nil
}

// GetAuthorityRequest passes through to the inner service. Reads do not
// touch the KV mirror in this task (CheckAccess hot path optimization is
// future work — see file header).
func (w *JetStreamAuthorityLifecycle) GetAuthorityRequest(ctx context.Context, requestID string) (*AuthorityRequest, error) {
	return w.inner.GetAuthorityRequest(ctx, requestID)
}

// SubmitAuthorityRequest is the wrapper for the SQL-side Submit. Order:
//  1. Inner write (SQLite/Postgres). On failure: return, no side effects.
//  2. KV Create (CAS on revision 0 — first write only). On revision conflict
//     (another writer raced with the same request_id, e.g. a deterministic
//     UUID collision or a retry path) we surface
//     ErrAuthorityRequestConcurrentModification so the caller can choose to
//     retry. The inner row remains authoritative; the divergence self-heals
//     on the next lifecycle action.
//  3. Event publish (best-effort: log + continue on failure).
func (w *JetStreamAuthorityLifecycle) SubmitAuthorityRequest(ctx context.Context, req *AuthorityRequest) (*AuthorityRequest, error) {
	persisted, err := w.inner.SubmitAuthorityRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := w.kvCreate(ctx, persisted); err != nil {
		// Inner row exists; KV diverged. Surface to caller so they can retry
		// or accept and continue with degraded watch behavior.
		return persisted, err
	}

	w.publishEvent(ctx, &AuthorityRequestLifecycleEvent{
		EventType:      AuthorityRequestEventTypeCreated,
		RequestID:      persisted.RequestID,
		Workspace:      firstWorkspace(persisted),
		StatusFrom:     "",
		StatusTo:       persisted.Status,
		TimestampMs:    nowMs(),
		ActorPrincipal: principalString(persisted.RequestingActor),
		Request:        persisted,
	})

	return persisted, nil
}

// ApproveAuthorityRequest wraps the inner approval. Order:
//  1. Inner approve (mints grant + flips row to APPROVED).
//  2. KV Update with CAS — looks up current revision via Get to retain the
//     existing key/revision lineage. On a concurrent-write race against
//     another lifecycle call (e.g. Approve vs. Deny vs. Cancel arriving at
//     once) the CAS fails and we return
//     ErrAuthorityRequestConcurrentModification. Idempotency:
//     the inner service's ResolveAuthorityRequest already guards via
//     ErrAuthorityRequestAlreadyResolved on a second approve attempt.
//  3. Event publish.
func (w *JetStreamAuthorityLifecycle) ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *ApproveDecision) (*AuthorityRequest, error) {
	// Snapshot the prior status for the event (best-effort: if the read
	// fails we proceed with empty status_from rather than failing the call).
	statusFrom := w.snapshotStatus(ctx, requestID)

	resolved, err := w.inner.ApproveAuthorityRequest(ctx, requestID, approverIdentity, decision)
	if err != nil {
		return nil, err
	}

	if err := w.kvUpdate(ctx, resolved); err != nil {
		return resolved, err
	}

	w.publishEvent(ctx, &AuthorityRequestLifecycleEvent{
		EventType:      AuthorityRequestEventTypeApproved,
		RequestID:      resolved.RequestID,
		Workspace:      firstWorkspace(resolved),
		StatusFrom:     statusFrom,
		StatusTo:       resolved.Status,
		TimestampMs:    nowMs(),
		ActorPrincipal: principalString(approverIdentity),
		GrantID:        resolved.GrantedGrantID,
		Request:        resolved,
	})

	return resolved, nil
}

// DenyAuthorityRequest wraps the inner deny. Same ordering as Approve; no
// grant_id since denial does not mint a grant.
func (w *JetStreamAuthorityLifecycle) DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*AuthorityRequest, error) {
	statusFrom := w.snapshotStatus(ctx, requestID)

	resolved, err := w.inner.DenyAuthorityRequest(ctx, requestID, approverIdentity, reason)
	if err != nil {
		return nil, err
	}

	if err := w.kvUpdate(ctx, resolved); err != nil {
		return resolved, err
	}

	w.publishEvent(ctx, &AuthorityRequestLifecycleEvent{
		EventType:      AuthorityRequestEventTypeDenied,
		RequestID:      resolved.RequestID,
		Workspace:      firstWorkspace(resolved),
		StatusFrom:     statusFrom,
		StatusTo:       resolved.Status,
		TimestampMs:    nowMs(),
		ActorPrincipal: principalString(approverIdentity),
		Request:        resolved,
	})

	return resolved, nil
}

// CancelOpenAuthorityRequest wraps the inner cancel.
func (w *JetStreamAuthorityLifecycle) CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*AuthorityRequest, error) {
	statusFrom := w.snapshotStatus(ctx, requestID)

	resolved, err := w.inner.CancelOpenAuthorityRequest(ctx, requestID, reason)
	if err != nil {
		return nil, err
	}

	if err := w.kvUpdate(ctx, resolved); err != nil {
		return resolved, err
	}

	w.publishEvent(ctx, &AuthorityRequestLifecycleEvent{
		EventType:      AuthorityRequestEventTypeCancelled,
		RequestID:      resolved.RequestID,
		Workspace:      firstWorkspace(resolved),
		StatusFrom:     statusFrom,
		StatusTo:       resolved.Status,
		TimestampMs:    nowMs(),
		ActorPrincipal: principalString(resolved.RequestingActor),
		Request:        resolved,
	})

	return resolved, nil
}

// SweepExpiredAuthorityRequests wraps the inner sweep. Each expired row gets
// its own KV update + event emit. CAS conflicts on individual rows are
// logged best-effort; the sweep continues so a single contended row does not
// stall the batch. This mirrors the inner service's audit-event semantics
// (per-row best-effort emit, never short-circuit the loop).
func (w *JetStreamAuthorityLifecycle) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*AuthorityRequest, error) {
	expired, err := w.inner.SweepExpiredAuthorityRequests(ctx, now, limit)
	if err != nil {
		return nil, err
	}

	for _, r := range expired {
		if err := w.kvUpdate(ctx, r); err != nil {
			w.warnf("kv update failed during sweep for %s: %v", r.RequestID, err)
			// continue: per-row best-effort; do not abort the sweep
		}
		w.publishEvent(ctx, &AuthorityRequestLifecycleEvent{
			EventType:      AuthorityRequestEventTypeExpired,
			RequestID:      r.RequestID,
			Workspace:      firstWorkspace(r),
			StatusFrom:     AuthorityRequestStatusPending,
			StatusTo:       r.Status,
			TimestampMs:    nowMs(),
			ActorPrincipal: "", // system actor
			Request:        r,
		})
	}

	return expired, nil
}

// ----------------------------------------------------------------------
// JetStream side-effect helpers
// ----------------------------------------------------------------------

// kvCreate writes the initial KV entry for a request via the Create path
// (atomic first-write only — Create fails if the key already exists).
// Wraps the revision-conflict error in ErrAuthorityRequestConcurrentModification.
func (w *JetStreamAuthorityLifecycle) kvCreate(ctx context.Context, req *AuthorityRequest) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("jetstream authority lifecycle: marshal request %s: %w", req.RequestID, err)
	}

	if _, err := w.kv.Create(ctx, req.RequestID, payload); err != nil {
		if isJSKeyExists(err) {
			// Race: another writer claimed this request_id first. Try a
			// single CAS-Update fallback (handles the case where the
			// inner storage layer retried after our previous successful
			// Create — idempotency preserved at the cost of one extra
			// round-trip).
			if updErr := w.kvUpdate(ctx, req); updErr == nil {
				return nil
			}
			return fmt.Errorf("%w: kv create %s: %v", ErrAuthorityRequestConcurrentModification, req.RequestID, err)
		}
		return fmt.Errorf("jetstream authority lifecycle: kv create %s: %w", req.RequestID, err)
	}
	return nil
}

// kvUpdate reads the current revision and writes the new payload via CAS.
// On revision conflict ErrAuthorityRequestConcurrentModification is returned
// so the caller can decide whether to retry.
//
// The single retry inside this loop handles the rare race where the prior
// revision was deleted between our Get and Update — falls back to Create
// for that specific case.
func (w *JetStreamAuthorityLifecycle) kvUpdate(ctx context.Context, req *AuthorityRequest) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("jetstream authority lifecycle: marshal request %s: %w", req.RequestID, err)
	}

	var lastErr error
	for attempt := 0; attempt < authorityRequestCASMaxAttempts; attempt++ {
		entry, getErr := w.kv.Get(ctx, req.RequestID)
		if getErr != nil {
			if errors.Is(getErr, jetstream.ErrKeyNotFound) {
				// No entry yet (e.g. sweep hit a row that never landed in KV
				// because Submit failed midway). Create instead.
				if _, cErr := w.kv.Create(ctx, req.RequestID, payload); cErr == nil {
					return nil
				} else if isJSKeyExists(cErr) {
					// Another writer beat us to Create; retry the Update path.
					lastErr = cErr
					continue
				} else {
					return fmt.Errorf("jetstream authority lifecycle: kv create-after-missing %s: %w", req.RequestID, cErr)
				}
			}
			return fmt.Errorf("jetstream authority lifecycle: kv get %s: %w", req.RequestID, getErr)
		}

		if _, updErr := w.kv.Update(ctx, req.RequestID, payload, entry.Revision()); updErr == nil {
			return nil
		} else if isJSRevisionConflict(updErr) {
			lastErr = updErr
			continue
		} else {
			return fmt.Errorf("jetstream authority lifecycle: kv update %s: %w", req.RequestID, updErr)
		}
	}

	return fmt.Errorf("%w: kv update %s exhausted retries: %v", ErrAuthorityRequestConcurrentModification, req.RequestID, lastErr)
}

// snapshotStatus reads the current status from the inner service before a
// transition so the emitted event can record status_from. Failures are
// silent (returns empty) — events are best-effort observability, not
// authoritative state.
func (w *JetStreamAuthorityLifecycle) snapshotStatus(ctx context.Context, requestID string) AuthorityRequestStatus {
	cur, err := w.inner.GetAuthorityRequest(ctx, requestID)
	if err != nil || cur == nil {
		return ""
	}
	return cur.Status
}

// publishEvent JSON-encodes the lifecycle event and publishes it to the
// per-workspace subject. Publish failures are logged but never surfaced to
// the caller — the inner write is already durable and approvers can recover
// state via a KV read on reconnect.
func (w *JetStreamAuthorityLifecycle) publishEvent(ctx context.Context, evt *AuthorityRequestLifecycleEvent) {
	payload, err := json.Marshal(evt)
	if err != nil {
		w.warnf("marshal lifecycle event for %s: %v", evt.RequestID, err)
		return
	}
	subject := WorkspaceEventSubject(evt.Workspace)
	if _, err := w.js.Publish(ctx, subject, payload); err != nil {
		w.warnf("publish lifecycle event for %s on %s: %v", evt.RequestID, subject, err)
	}
}

// WorkspaceEventSubject returns the NATS subject for per-workspace lifecycle
// events. Exported so approver-side subscribers (gateway handler, SDK) can
// compute the same subject without re-deriving the codec rules.
//
// Aether topic: "authreq::{workspace}::events" → NATS subject via
// natscodec.ToNATSSubject. Empty workspace uses "_" sentinel (matches
// kv/jetstream_store.go's encodeSegment for empty segments) so the subject
// still has three tokens and matches the "authreq.>" filter.
func WorkspaceEventSubject(workspace string) string {
	ws := workspace
	if strings.TrimSpace(ws) == "" {
		ws = "_"
	}
	return natscodec.ToNATSSubject("authreq::" + ws + "::events")
}

// ----------------------------------------------------------------------
// Small utilities
// ----------------------------------------------------------------------

func (w *JetStreamAuthorityLifecycle) warnf(format string, args ...any) {
	if w.log != nil {
		w.log.Warnf(format, args...)
	}
}

func firstWorkspace(req *AuthorityRequest) string {
	if req == nil || len(req.WorkspaceScope) == 0 {
		return ""
	}
	return req.WorkspaceScope[0]
}

func principalString(id models.Identity) string {
	ref := id.PrincipalRef()
	if ref.IsZero() {
		return ""
	}
	return PrincipalTypeForModel(ref.Type) + ":" + ref.ID
}

func nowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// isJSKeyExists detects the "key already exists" race from KV.Create.
func isJSKeyExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	if jsErr, ok := err.(jetstream.JetStreamError); ok {
		if apiErr := jsErr.APIError(); apiErr != nil {
			return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "key exists") || strings.Contains(msg, "wrong last sequence")
}

// isJSRevisionConflict detects the "wrong last sequence" race from KV.Update.
func isJSRevisionConflict(err error) bool {
	if err == nil {
		return false
	}
	if jsErr, ok := err.(jetstream.JetStreamError); ok {
		if apiErr := jsErr.APIError(); apiErr != nil {
			return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "wrong last sequence") || strings.Contains(msg, "key exists")
}
