// Package state contains the JetStream-backed implementation of
// gateway.SessionManager. This implementation is the cluster-aware twin of
// BadgerSessionRegistry (single-node) and SessionRegistry (Redis). It uses
// NATS JetStream Key/Value buckets for cross-gateway identity locks, session
// registry entries, and proxy tunnel/request pins.
//
// Design notes
//
//   - NATS KV (as of nats.go v1.52) supports bucket-level MaxAge (TTL) and
//     per-entry TTL via KeyTTL on Create, but per-entry TTL cannot be reset
//     by Update (Update strips TTL). That makes Update unusable for refresh
//     of TTL'd locks. We therefore enforce expiry in the application layer
//     by encoding expires_unix_ms inside the JSON value, and rely on the
//     bucket-level MaxAge as a coarse GC safety net (set to several times
//     LockTTL so it never expires a healthy entry).
//   - Atomic acquire-or-resume uses kv.Create for fresh acquisition (returns
//     ErrKeyExists if held) and a bounded CAS loop of Get + Update(revision)
//     for resume and force-takeover paths.
//   - Tunnel and request pins use simple Put with bucket MaxAge — pin
//     refresh re-writes the entry with the same value, which is also the
//     pattern used by the Redis impl's SET-then-EXPIRE.
package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// JetStream KV bucket names. These mirror the categories used by the Redis
// implementation but live in dedicated buckets so each can carry an
// appropriate MaxAge / replica count.
const (
	jsBucketLocks        = "aether_locks"
	jsBucketSessions     = "aether_sessions"
	jsBucketPinsTunnel   = "aether_pins_tunnel"
	jsBucketPinsRequest  = "aether_pins_request"
	jsBucketMaxAgeFactor = 10 // bucket MaxAge = LockTTL * factor (safety net)
	jsMaxCASAttempts     = 8
	jsServiceTypeTag     = "sv"
)

// JetStreamSession implements gateway.SessionManager using NATS JetStream KV
// buckets.
type JetStreamSession struct {
	locks    jetstream.KeyValue
	sessions jetstream.KeyValue
	pinsT    jetstream.KeyValue
	pinsR    jetstream.KeyValue
	replicas int
}

// JetStreamSessionConfig drives bucket creation. Replicas is the desired
// replica count for each bucket; 0 selects the JetStream default (1).
type JetStreamSessionConfig struct {
	Replicas int
}

// lockValue is the JSON payload stored under each identity lock key.
// expiresUnixMs is the logical expiry — readers MUST treat the entry as
// absent when the current wall clock is past this value.
//
// The Client* and Initial*/Reconnection* fields are session-lifetime
// metadata added per the InitConnection versioning spec; they are
// preserved across resume_session_id takeovers so admin observers see
// the original connect time and accumulated reconnect count.
type lockValue struct {
	GatewayID      string `json:"gateway_id"`
	SessionID      string `json:"session_id"`
	AcquiredUnixMs int64  `json:"acquired_unix_ms"`
	ExpiresUnixMs  int64  `json:"expires_unix_ms"`

	ClientVersion           string         `json:"client_version,omitempty"`
	ClientSDK               string         `json:"client_sdk,omitempty"`
	ClientBuildInfo         *BuildInfoMeta `json:"client_build_info,omitempty"`
	InitialConnectionUnixMs int64          `json:"initial_connection_unix_ms,omitempty"`
	ReconnectionCount       int32          `json:"reconnection_count,omitempty"`
}

// sessionValue is the JSON payload stored under each session_id key.
type sessionValue struct {
	Identity         string `json:"identity"`
	GatewayID        string `json:"gateway_id"`
	RegisteredUnixMs int64  `json:"registered_unix_ms"`
}

// pinValue is the JSON payload stored under each tunnel/request pin.
type pinValue struct {
	Value         string `json:"value"`
	ExpiresUnixMs int64  `json:"expires_unix_ms"`
}

// NewJetStreamSession opens (creating if needed) the four KV buckets used by
// the session manager. The bucket MaxAge is set to several multiples of
// LockTTL so the bucket itself never garbage-collects a healthy refresh
// cadence; logical expiry is enforced in-process by reading the encoded
// expires_unix_ms field on each access.
func NewJetStreamSession(ctx context.Context, js jetstream.JetStream, cfg JetStreamSessionConfig) (*JetStreamSession, error) {
	if js == nil {
		return nil, errors.New("jetstream session: nil JetStream context")
	}
	replicas := cfg.Replicas
	if replicas < 1 {
		replicas = 1
	}

	// Bucket-level MaxAge serves as a coarse safety net for orphaned entries.
	// Tunnel/request pin buckets use shorter MaxAge to bound long-lived
	// orphans on those keyspaces.
	lockMaxAge := time.Duration(jsBucketMaxAgeFactor) * LockTTL
	sessionMaxAge := lockMaxAge
	tunnelMaxAge := 30 * time.Minute
	requestMaxAge := 10 * time.Minute

	locks, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      jsBucketLocks,
		Description: "Aether identity locks (cluster-wide).",
		Replicas:    replicas,
		TTL:         lockMaxAge,
		History:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream session: open locks bucket: %w", err)
	}
	sessions, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      jsBucketSessions,
		Description: "Aether session metadata (session_id → identity + gateway).",
		Replicas:    replicas,
		TTL:         sessionMaxAge,
		History:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream session: open sessions bucket: %w", err)
	}
	pinsT, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      jsBucketPinsTunnel,
		Description: "Aether tunnel stickiness pins.",
		Replicas:    replicas,
		TTL:         tunnelMaxAge,
		History:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream session: open tunnel pin bucket: %w", err)
	}
	pinsR, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      jsBucketPinsRequest,
		Description: "Aether HTTP request stickiness pins.",
		Replicas:    replicas,
		TTL:         requestMaxAge,
		History:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream session: open request pin bucket: %w", err)
	}

	return &JetStreamSession{
		locks:    locks,
		sessions: sessions,
		pinsT:    pinsT,
		pinsR:    pinsR,
		replicas: replicas,
	}, nil
}

// ---- key encoding ----
//
// NATS KV keys must match the regex `^[-/_=.a-zA-Z0-9]+$`. Identity strings
// use "::" as the segment separator, which contains the colon character
// (invalid). Tunnel/request IDs may contain arbitrary characters. We
// hex-escape any byte that falls outside the safe set as `_HH` (a leading
// underscore followed by two hex digits). This mirrors the scheme used by
// `internal/kv/jetstream_store.go`'s encodeSegment but simplified since we
// do not embed multiple segments. The encoding is round-trippable via
// decodeKVKey.

func encodeKVKey(s string) string {
	if s == "" {
		return "_00"
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_':
			b.WriteString("_5F")
		case c >= '0' && c <= '9',
			c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c == '-' || c == '/' || c == '=' || c == '.':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
			b.WriteByte(hexNibble(c >> 4))
			b.WriteByte(hexNibble(c & 0x0F))
		}
	}
	return b.String()
}

func decodeKVKey(s string) string {
	if s == "_00" {
		return ""
	}
	if !strings.Contains(s, "_") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		c := s[i]
		if c == '_' && i+2 < len(s) {
			h1, ok1 := hexVal(s[i+1])
			h2, ok2 := hexVal(s[i+2])
			if ok1 && ok2 {
				b.WriteByte((h1 << 4) | h2)
				i += 3
				continue
			}
		}
		b.WriteByte(c)
		i++
	}
	return b.String()
}

func hexNibble(v byte) byte {
	if v < 10 {
		return '0' + v
	}
	return 'A' + v - 10
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

func nowUnixMs() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }

func newLockValue(gatewayID, sessionID string) lockValue {
	now := nowUnixMs()
	return lockValue{
		GatewayID:      gatewayID,
		SessionID:      sessionID,
		AcquiredUnixMs: now,
		ExpiresUnixMs:  now + LockTTL.Milliseconds(),
	}
}

// applyConnectMeta copies the per-connect client metadata fields from meta
// onto lv. Lifetime fields (InitialConnectionUnixMs, ReconnectionCount)
// are computed by the caller per the resume/fresh/force semantics — this
// helper only fans out the per-version metadata.
func (lv *lockValue) applyConnectMeta(meta ConnectMeta) {
	lv.ClientVersion = meta.ClientVersion
	lv.ClientSDK = meta.ClientSDK
	lv.ClientBuildInfo = meta.ClientBuildInfo
}

func encodeLockValue(lv lockValue) ([]byte, error) { return json.Marshal(lv) }

func decodeLockValue(raw []byte) (lockValue, error) {
	var lv lockValue
	if err := json.Unmarshal(raw, &lv); err != nil {
		return lockValue{}, fmt.Errorf("decode lock value: %w", err)
	}
	return lv, nil
}

// isLockExpired returns true when the stored expires_unix_ms is in the past.
func isLockExpired(lv lockValue) bool { return lv.ExpiresUnixMs <= nowUnixMs() }

// AcquireOrResumeLock attempts to acquire the identity lock under the
// semantics specified by SessionManager.
//
// Semantics:
//   - Lock absent (or logically expired) → Create with new value (acquired=true).
//   - Existing lock with matching resumeSessionID → CAS-update with new
//     session_id (resumed=true, acquired=true).
//   - Existing lock with remaining TTL < forceTakeoverThresholdMs → CAS-update
//     (forced=true, acquired=true).
//   - Otherwise → acquired=false (no error).
//
// The gateway_id used for the new value is derived from the principal's
// session_id rather than passed in: callers wire gatewayID via
// RegisterSession. The lock value's gateway_id is set to sessionID for
// compatibility with downstream readers that key off the lock's session_id.
// (Phase-7 cross-gateway forwarding queries RegisterSession's gateway_id via
// GetSessionGateway.)
func (s *JetStreamSession) AcquireOrResumeLock(
	ctx context.Context,
	identity models.Identity,
	sessionID, resumeSessionID string,
	forceTakeoverThresholdMs int64,
	meta ConnectMeta,
) (ConnectResult, error) {
	key := encodeKVKey(identity.String())

	for attempt := 0; attempt < jsMaxCASAttempts; attempt++ {
		entry, getErr := s.locks.Get(ctx, key)
		if getErr != nil && !errors.Is(getErr, jetstream.ErrKeyNotFound) {
			return ConnectResult{}, fmt.Errorf("jetstream AcquireOrResumeLock: get: %w", getErr)
		}

		// Path 1: lock absent — try fresh Create.
		if getErr != nil { // ErrKeyNotFound
			lv := newLockValue(sessionID, sessionID)
			lv.applyConnectMeta(meta)
			lv.InitialConnectionUnixMs = lv.AcquiredUnixMs
			lv.ReconnectionCount = 0
			encoded, encErr := encodeLockValue(lv)
			if encErr != nil {
				return ConnectResult{}, encErr
			}
			if _, createErr := s.locks.Create(ctx, key, encoded); createErr != nil {
				if isRevisionConflictJS(createErr) {
					// Another writer beat us — retry.
					continue
				}
				return ConnectResult{}, fmt.Errorf("jetstream AcquireOrResumeLock: create: %w", createErr)
			}
			return ConnectResult{
				Acquired:                true,
				InitialConnectionUnixMs: lv.InitialConnectionUnixMs,
				ReconnectionCount:       lv.ReconnectionCount,
			}, nil
		}

		// Existing entry — decode and assess.
		current, decErr := decodeLockValue(entry.Value())
		if decErr != nil {
			// Corrupt entry: take ownership via CAS overwrite.
			lv := newLockValue(sessionID, sessionID)
			lv.applyConnectMeta(meta)
			lv.InitialConnectionUnixMs = lv.AcquiredUnixMs
			lv.ReconnectionCount = 0
			encoded, encErr := encodeLockValue(lv)
			if encErr != nil {
				return ConnectResult{}, encErr
			}
			if _, updErr := s.locks.Update(ctx, key, encoded, entry.Revision()); updErr != nil {
				if isRevisionConflictJS(updErr) {
					continue
				}
				return ConnectResult{}, fmt.Errorf("jetstream AcquireOrResumeLock: cas overwrite corrupt: %w", updErr)
			}
			logging.Logger.Warn().
				Str("identity", identity.String()).
				Msg("jetstream session: corrupt lock entry — took ownership via CAS overwrite")
			return ConnectResult{
				Acquired:                true,
				Forced:                  true,
				InitialConnectionUnixMs: lv.InitialConnectionUnixMs,
				ReconnectionCount:       lv.ReconnectionCount,
			}, nil
		}

		// Logically expired → treat as absent. Take ownership via CAS update
		// (Update needs the current revision; we have it from Get).
		if isLockExpired(current) {
			lv := newLockValue(sessionID, sessionID)
			lv.applyConnectMeta(meta)
			lv.InitialConnectionUnixMs = lv.AcquiredUnixMs
			lv.ReconnectionCount = 0
			encoded, encErr := encodeLockValue(lv)
			if encErr != nil {
				return ConnectResult{}, encErr
			}
			if _, updErr := s.locks.Update(ctx, key, encoded, entry.Revision()); updErr != nil {
				if isRevisionConflictJS(updErr) {
					continue
				}
				return ConnectResult{}, fmt.Errorf("jetstream AcquireOrResumeLock: cas expired: %w", updErr)
			}
			return ConnectResult{
				Acquired:                true,
				InitialConnectionUnixMs: lv.InitialConnectionUnixMs,
				ReconnectionCount:       lv.ReconnectionCount,
			}, nil
		}

		// Path 2: resume — caller advertises a prior session ID matching the holder.
		if resumeSessionID != "" && current.SessionID == resumeSessionID {
			lv := newLockValue(sessionID, sessionID)
			lv.applyConnectMeta(meta)
			// Preserve original connect time; fall back to the new acquire
			// time if the predecessor predates the lifetime fields (legacy
			// JSON without InitialConnectionUnixMs).
			if current.InitialConnectionUnixMs > 0 {
				lv.InitialConnectionUnixMs = current.InitialConnectionUnixMs
			} else {
				lv.InitialConnectionUnixMs = lv.AcquiredUnixMs
			}
			lv.ReconnectionCount = current.ReconnectionCount + 1
			encoded, encErr := encodeLockValue(lv)
			if encErr != nil {
				return ConnectResult{}, encErr
			}
			if _, updErr := s.locks.Update(ctx, key, encoded, entry.Revision()); updErr != nil {
				if isRevisionConflictJS(updErr) {
					continue
				}
				return ConnectResult{}, fmt.Errorf("jetstream AcquireOrResumeLock: cas resume: %w", updErr)
			}
			return ConnectResult{
				Acquired:                true,
				Resumed:                 true,
				InitialConnectionUnixMs: lv.InitialConnectionUnixMs,
				ReconnectionCount:       lv.ReconnectionCount,
			}, nil
		}

		// Path 3: force takeover — holder TTL has decayed below threshold.
		if forceTakeoverThresholdMs > 0 {
			remainingMs := current.ExpiresUnixMs - nowUnixMs()
			if remainingMs > 0 && remainingMs < forceTakeoverThresholdMs {
				lv := newLockValue(sessionID, sessionID)
				lv.applyConnectMeta(meta)
				// Force-takeover semantics: prior holder was dead, so we
				// treat this as a fresh connect (counter resets).
				lv.InitialConnectionUnixMs = lv.AcquiredUnixMs
				lv.ReconnectionCount = 0
				encoded, encErr := encodeLockValue(lv)
				if encErr != nil {
					return ConnectResult{}, encErr
				}
				if _, updErr := s.locks.Update(ctx, key, encoded, entry.Revision()); updErr != nil {
					if isRevisionConflictJS(updErr) {
						continue
					}
					return ConnectResult{}, fmt.Errorf("jetstream AcquireOrResumeLock: cas force: %w", updErr)
				}
				logging.Logger.Warn().
					Str("identity", identity.String()).
					Int64("remaining_ms", remainingMs).
					Msg("jetstream session: forced lock takeover — previous holder missed refresh cycles")
				return ConnectResult{
					Acquired:                true,
					Forced:                  true,
					InitialConnectionUnixMs: lv.InitialConnectionUnixMs,
					ReconnectionCount:       lv.ReconnectionCount,
				}, nil
			}
		}

		// Lock is healthy and held by a different session — reject.
		return ConnectResult{}, nil
	}
	return ConnectResult{}, fmt.Errorf("jetstream AcquireOrResumeLock: too much contention after %d attempts", jsMaxCASAttempts)
}

// ReleaseLock releases the identity lock when the caller still owns it
// (sessionID matches the stored session_id). CAS-based: uses Delete with the
// LastRevision option so a stale caller cannot drop a takeover.
func (s *JetStreamSession) ReleaseLock(ctx context.Context, identity models.Identity, sessionID string) error {
	key := encodeKVKey(identity.String())

	for attempt := 0; attempt < jsMaxCASAttempts; attempt++ {
		entry, err := s.locks.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return nil // already released
			}
			return fmt.Errorf("jetstream ReleaseLock: get: %w", err)
		}
		current, decErr := decodeLockValue(entry.Value())
		if decErr != nil {
			// Corrupt entry — best-effort delete with revision.
			if delErr := s.locks.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); delErr != nil {
				if isRevisionConflictJS(delErr) {
					continue
				}
				return fmt.Errorf("jetstream ReleaseLock: delete corrupt: %w", delErr)
			}
			return nil
		}
		if current.SessionID != sessionID {
			// Not our lock — no-op (matches Badger/Redis semantics).
			return nil
		}
		if delErr := s.locks.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); delErr != nil {
			if isRevisionConflictJS(delErr) {
				continue
			}
			return fmt.Errorf("jetstream ReleaseLock: delete: %w", delErr)
		}
		return nil
	}
	return fmt.Errorf("jetstream ReleaseLock: too much contention after %d attempts", jsMaxCASAttempts)
}

// RefreshLock extends the logical TTL of the identity lock by re-writing the
// value with a new expires_unix_ms. Returns false (no error) when the lock
// is no longer held by us (revision mismatch, key gone, or session mismatch).
func (s *JetStreamSession) RefreshLock(ctx context.Context, identity models.Identity, sessionID string) (bool, error) {
	key := encodeKVKey(identity.String())

	for attempt := 0; attempt < jsMaxCASAttempts; attempt++ {
		entry, err := s.locks.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return false, nil
			}
			return false, fmt.Errorf("jetstream RefreshLock: get: %w", err)
		}
		current, decErr := decodeLockValue(entry.Value())
		if decErr != nil {
			return false, fmt.Errorf("jetstream RefreshLock: decode: %w", decErr)
		}
		if current.SessionID != sessionID {
			return false, nil
		}
		current.ExpiresUnixMs = nowUnixMs() + LockTTL.Milliseconds()
		encoded, encErr := encodeLockValue(current)
		if encErr != nil {
			return false, encErr
		}
		if _, updErr := s.locks.Update(ctx, key, encoded, entry.Revision()); updErr != nil {
			if isRevisionConflictJS(updErr) {
				continue
			}
			return false, fmt.Errorf("jetstream RefreshLock: update: %w", updErr)
		}
		return true, nil
	}
	return false, fmt.Errorf("jetstream RefreshLock: too much contention after %d attempts", jsMaxCASAttempts)
}

// RefreshLockAndSession refreshes both the lock TTL and the session
// metadata. Refreshes are not atomic across buckets in NATS KV; we refresh
// the lock first and only refresh the session if the lock refresh
// succeeded.
func (s *JetStreamSession) RefreshLockAndSession(ctx context.Context, identity models.Identity, sessionID string) (bool, error) {
	ok, err := s.RefreshLock(ctx, identity, sessionID)
	if err != nil || !ok {
		return ok, err
	}
	// Best-effort: refreshing the session entry just re-writes the same JSON
	// to reset the bucket-level TTL clock. We deliberately swallow
	// ErrKeyNotFound — a missing session entry should not cause the caller
	// to drop the (now refreshed) lock.
	if refreshErr := s.RefreshSession(ctx, sessionID); refreshErr != nil && !errors.Is(refreshErr, jetstream.ErrKeyNotFound) {
		logging.Logger.Warn().
			Err(refreshErr).
			Str("session_id", sessionID).
			Msg("jetstream session: lock refreshed but session refresh failed")
	}
	return true, nil
}

// RegisterSession stores session metadata keyed by sessionID.
func (s *JetStreamSession) RegisterSession(ctx context.Context, identity models.Identity, sessionID, gatewayID string) error {
	key := encodeKVKey(sessionID)
	sv := sessionValue{
		Identity:         identity.String(),
		GatewayID:        gatewayID,
		RegisteredUnixMs: nowUnixMs(),
	}
	encoded, err := json.Marshal(sv)
	if err != nil {
		return fmt.Errorf("jetstream RegisterSession: encode: %w", err)
	}
	if _, putErr := s.sessions.Put(ctx, key, encoded); putErr != nil {
		return fmt.Errorf("jetstream RegisterSession: put: %w", putErr)
	}
	return nil
}

// GetSessionIdentity resolves the principal identity stored for a session.
func (s *JetStreamSession) GetSessionIdentity(ctx context.Context, sessionID string) (models.Identity, error) {
	key := encodeKVKey(sessionID)
	entry, err := s.sessions.Get(ctx, key)
	if err != nil {
		return models.Identity{}, fmt.Errorf("jetstream GetSessionIdentity: %w", err)
	}
	var sv sessionValue
	if jsonErr := json.Unmarshal(entry.Value(), &sv); jsonErr != nil {
		return models.Identity{}, fmt.Errorf("jetstream GetSessionIdentity: decode: %w", jsonErr)
	}
	return parseStoredSessionIdentity(sv.Identity)
}

// GetSessionGateway returns the gateway_id of the gateway hosting the given
// principal's connection. Returns "" with nil error when the principal is
// offline (lock absent or expired).
func (s *JetStreamSession) GetSessionGateway(ctx context.Context, identity models.Identity) (string, error) {
	lockKey := encodeKVKey(identity.String())
	lockEntry, err := s.locks.Get(ctx, lockKey)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("jetstream GetSessionGateway: get lock: %w", err)
	}
	lv, decErr := decodeLockValue(lockEntry.Value())
	if decErr != nil {
		return "", fmt.Errorf("jetstream GetSessionGateway: decode lock: %w", decErr)
	}
	if isLockExpired(lv) {
		return "", nil
	}
	// Look up session metadata for the gateway_id.
	sessKey := encodeKVKey(lv.SessionID)
	sessEntry, err := s.sessions.Get(ctx, sessKey)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("jetstream GetSessionGateway: get session: %w", err)
	}
	var sv sessionValue
	if jsonErr := json.Unmarshal(sessEntry.Value(), &sv); jsonErr != nil {
		return "", fmt.Errorf("jetstream GetSessionGateway: decode session: %w", jsonErr)
	}
	return sv.GatewayID, nil
}

// UnregisterSession removes session metadata for the given sessionID.
func (s *JetStreamSession) UnregisterSession(ctx context.Context, sessionID string) error {
	key := encodeKVKey(sessionID)
	if err := s.sessions.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("jetstream UnregisterSession: %w", err)
	}
	return nil
}

// RefreshSession re-writes the session metadata entry to reset the bucket
// MaxAge clock.
func (s *JetStreamSession) RefreshSession(ctx context.Context, sessionID string) error {
	key := encodeKVKey(sessionID)
	entry, err := s.sessions.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("jetstream RefreshSession: get: %w", err)
	}
	// Re-Put the same value to reset MaxAge.
	if _, putErr := s.sessions.Put(ctx, key, entry.Value()); putErr != nil {
		return fmt.Errorf("jetstream RefreshSession: put: %w", putErr)
	}
	return nil
}

// IsActive returns true when an identity has a live (non-expired) lock.
func (s *JetStreamSession) IsActive(ctx context.Context, identity string) (bool, error) {
	key := encodeKVKey(identity)
	entry, err := s.locks.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("jetstream IsActive: %w", err)
	}
	lv, decErr := decodeLockValue(entry.Value())
	if decErr != nil {
		return false, nil
	}
	return !isLockExpired(lv), nil
}

// IsOnline mirrors BadgerSessionRegistry.IsOnline for parity with non-context
// call sites in TaskAssignmentService.
func (s *JetStreamSession) IsOnline(identity models.Identity) bool {
	active, err := s.IsActive(context.Background(), identity.String())
	if err != nil {
		return false
	}
	return active
}

// FindHealthyServiceInstances iterates the lock bucket and returns the
// identity strings of `sv::{impl}::*` holders whose stored expiry leaves at
// least minRemaining of TTL. minRemaining ≤ 0 disables the TTL filter.
func (s *JetStreamSession) FindHealthyServiceInstances(ctx context.Context, impl string, minRemaining time.Duration) ([]string, error) {
	// We must iterate all keys because NATS KV key matching uses subject
	// wildcards (`.` separator), not arbitrary prefix matching. Identity
	// strings encode `::` as `_3A_3A` under encodeKVKey, so server-side
	// filtering on the encoded prefix would work but produces brittle
	// behaviour if the encoding ever changes. The lock keyspace is small
	// (one entry per connected principal) so a full scan is acceptable.
	all, err := s.locks.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("jetstream FindHealthyServiceInstances: list keys: %w", err)
	}
	wantPrefix := jsServiceTypeTag + models.IdentitySep + impl + models.IdentitySep
	minRemainingMs := minRemaining.Milliseconds()

	out := make([]string, 0, len(all))
	for _, encKey := range all {
		identityStr := decodeKVKey(encKey)
		if !strings.HasPrefix(identityStr, wantPrefix) {
			continue
		}
		entry, err := s.locks.Get(ctx, encKey)
		if err != nil {
			continue
		}
		lv, decErr := decodeLockValue(entry.Value())
		if decErr != nil {
			continue
		}
		if isLockExpired(lv) {
			continue
		}
		if minRemainingMs > 0 {
			remaining := lv.ExpiresUnixMs - nowUnixMs()
			if remaining < minRemainingMs {
				continue
			}
		}
		out = append(out, identityStr)
	}
	return out, nil
}

// ---- tunnel pins ----

const (
	defaultTunnelPinTTL  = 5 * time.Minute
	defaultRequestPinTTL = 60 * time.Second
)

func encodePinValue(value string, ttl time.Duration) ([]byte, error) {
	pv := pinValue{
		Value:         value,
		ExpiresUnixMs: nowUnixMs() + ttl.Milliseconds(),
	}
	return json.Marshal(pv)
}

func decodePinValue(raw []byte) (pinValue, error) {
	var pv pinValue
	if err := json.Unmarshal(raw, &pv); err != nil {
		return pinValue{}, err
	}
	return pv, nil
}

// SetTunnelPin records a tunnel pin. Replaces any existing pin (mirrors the
// Redis impl, which uses SET not SETNX).
func (s *JetStreamSession) SetTunnelPin(ctx context.Context, tunnelID, serviceIdentity string, ttl time.Duration) error {
	if tunnelID == "" || serviceIdentity == "" {
		return fmt.Errorf("tunnelID and serviceIdentity must be non-empty")
	}
	if ttl <= 0 {
		ttl = defaultTunnelPinTTL
	}
	encoded, err := encodePinValue(serviceIdentity, ttl)
	if err != nil {
		return fmt.Errorf("jetstream SetTunnelPin: encode: %w", err)
	}
	if _, err := s.pinsT.Put(ctx, encodeKVKey(tunnelID), encoded); err != nil {
		return fmt.Errorf("jetstream SetTunnelPin: put: %w", err)
	}
	return nil
}

// GetTunnelPin returns the bound service identity, or "" when absent or
// logically expired.
func (s *JetStreamSession) GetTunnelPin(ctx context.Context, tunnelID string) (string, error) {
	if tunnelID == "" {
		return "", nil
	}
	entry, err := s.pinsT.Get(ctx, encodeKVKey(tunnelID))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("jetstream GetTunnelPin: %w", err)
	}
	pv, decErr := decodePinValue(entry.Value())
	if decErr != nil {
		return "", nil
	}
	if pv.ExpiresUnixMs <= nowUnixMs() {
		return "", nil
	}
	return pv.Value, nil
}

// RefreshTunnelPin extends the TTL of an existing pin by re-writing its
// expiry. No-op when the pin is absent.
func (s *JetStreamSession) RefreshTunnelPin(ctx context.Context, tunnelID string, ttl time.Duration) error {
	if tunnelID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultTunnelPinTTL
	}
	entry, err := s.pinsT.Get(ctx, encodeKVKey(tunnelID))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("jetstream RefreshTunnelPin: get: %w", err)
	}
	pv, decErr := decodePinValue(entry.Value())
	if decErr != nil {
		return nil
	}
	encoded, err := encodePinValue(pv.Value, ttl)
	if err != nil {
		return fmt.Errorf("jetstream RefreshTunnelPin: encode: %w", err)
	}
	if _, err := s.pinsT.Put(ctx, encodeKVKey(tunnelID), encoded); err != nil {
		return fmt.Errorf("jetstream RefreshTunnelPin: put: %w", err)
	}
	return nil
}

// DeleteTunnelPin removes a tunnel pin. Idempotent.
func (s *JetStreamSession) DeleteTunnelPin(ctx context.Context, tunnelID string) error {
	if tunnelID == "" {
		return nil
	}
	if err := s.pinsT.Delete(ctx, encodeKVKey(tunnelID)); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("jetstream DeleteTunnelPin: %w", err)
	}
	return nil
}

// ---- request pins ----

// SetRequestPin records a request pin.
func (s *JetStreamSession) SetRequestPin(ctx context.Context, requestID, pinValueStr string, ttl time.Duration) error {
	if requestID == "" || pinValueStr == "" {
		return fmt.Errorf("requestID and pinValue must be non-empty")
	}
	if ttl <= 0 {
		ttl = defaultRequestPinTTL
	}
	encoded, err := encodePinValue(pinValueStr, ttl)
	if err != nil {
		return fmt.Errorf("jetstream SetRequestPin: encode: %w", err)
	}
	if _, err := s.pinsR.Put(ctx, encodeKVKey(requestID), encoded); err != nil {
		return fmt.Errorf("jetstream SetRequestPin: put: %w", err)
	}
	return nil
}

// GetRequestPin returns the bound pin value, or "" when absent or logically
// expired.
func (s *JetStreamSession) GetRequestPin(ctx context.Context, requestID string) (string, error) {
	if requestID == "" {
		return "", nil
	}
	entry, err := s.pinsR.Get(ctx, encodeKVKey(requestID))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("jetstream GetRequestPin: %w", err)
	}
	pv, decErr := decodePinValue(entry.Value())
	if decErr != nil {
		return "", nil
	}
	if pv.ExpiresUnixMs <= nowUnixMs() {
		return "", nil
	}
	return pv.Value, nil
}

// RefreshRequestPin extends an existing request pin's TTL.
func (s *JetStreamSession) RefreshRequestPin(ctx context.Context, requestID string, ttl time.Duration) error {
	if requestID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultRequestPinTTL
	}
	entry, err := s.pinsR.Get(ctx, encodeKVKey(requestID))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("jetstream RefreshRequestPin: get: %w", err)
	}
	pv, decErr := decodePinValue(entry.Value())
	if decErr != nil {
		return nil
	}
	encoded, err := encodePinValue(pv.Value, ttl)
	if err != nil {
		return fmt.Errorf("jetstream RefreshRequestPin: encode: %w", err)
	}
	if _, err := s.pinsR.Put(ctx, encodeKVKey(requestID), encoded); err != nil {
		return fmt.Errorf("jetstream RefreshRequestPin: put: %w", err)
	}
	return nil
}

// DeleteRequestPin removes a request pin. Idempotent.
func (s *JetStreamSession) DeleteRequestPin(ctx context.Context, requestID string) error {
	if requestID == "" {
		return nil
	}
	if err := s.pinsR.Delete(ctx, encodeKVKey(requestID)); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("jetstream DeleteRequestPin: %w", err)
	}
	return nil
}

// isRevisionConflictJS returns true for errors that indicate a concurrent
// writer beat us to the revision (CAS retry is safe).
func isRevisionConflictJS(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			return true
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "wrong last sequence") || strings.Contains(msg, "key exists")
}
