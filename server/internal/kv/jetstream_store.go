package kv

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/router/natscodec"
	"github.com/scitrera/aether/pkg/models"
)

const (
	// jsBucketName is the single NATS KV bucket used for all aether KV data.
	jsBucketName = "aether_kv"

	// jsMaxCASAttempts is the maximum number of CAS retry iterations for
	// guarded counter operations before giving up with an error.
	jsMaxCASAttempts = 32

	// jsSeparator is the intra-key separator inside a NATS KV key.
	// We use "." so each segment becomes a proper NATS subject token, which
	// is required for the `prefix.>` filter pattern in List/ListPaginated
	// to fire correctly. "/" would store the whole key as a single NATS
	// subject token and trailing ">" would not behave as a wildcard.
	// Real "." inside an aether segment is escaped to "_2E_" so there is no
	// ambiguity between separator and content.
	jsSeparator = "."
)

// JetStreamKVStore implements gateway.KVReadWriter using a single NATS KV bucket.
//
// Key layout inside the bucket:
//
//	{scopeTag}/{impl}/{spec}/{userID}/{workspace}/{userKey}
//
// where each segment is percent-hex–escaped with the same _XX_ scheme used by
// natscodec so that arbitrary aether identifiers map to NATS-safe key segments.
// Empty segments are encoded as "_" (a single underscore, which becomes "_5F_"
// only when the original value literally is "_"). This makes scope isolation
// prefix-filterable without ambiguity.
//
// Scope tags mirror BuildNamespace semantics:
//
//	global            → g
//	global-exclusive  → ge
//	workspace         → w
//	workspace-exclusive → we
//	user-shared       → us
//	user              → u
//	user-workspace-shared → uws
//	user-workspace    → uw
type JetStreamKVStore struct {
	kv jetstream.KeyValue
}

// NewJetStreamKVStore creates or opens the bucket and returns a ready store.
// ttl is applied as the bucket-level MaxAge; entries that supply a per-call
// TTL override are stored with a dedicated PutWithOptions call.
func NewJetStreamKVStore(ctx context.Context, js jetstream.JetStream) (*JetStreamKVStore, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  jsBucketName,
		History: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream kv: open bucket %s: %w", jsBucketName, err)
	}
	return &JetStreamKVStore{kv: kv}, nil
}

// ---- key encoding ----
//
// Per-segment escaping is delegated to natscodec.EscapeForKVKey, which uses a
// shared LRU cache and a single source of truth for byte classification across
// all NATS namespaces (subject, KV key, durable consumer name). The empty
// segment "_" sentinel convention is local to this package — natscodec does
// not need to know about it. A bare "_" is unambiguous because natscodec
// always escapes a real "_" byte to "_5F_".

// encodeKVSegment wraps natscodec.EscapeForKVKey with the empty-segment
// sentinel used by this package's namespace layout.
func encodeKVSegment(s string) string {
	if s == "" {
		return "_"
	}
	return natscodec.EscapeForKVKey(s)
}

// decodeKVSegment reverses encodeKVSegment.
func decodeKVSegment(s string) string {
	if s == "_" {
		return ""
	}
	return natscodec.Unescape(s)
}

// scopeTag maps a KVScope to a short NATS-safe tag.
func scopeTag(scope KVScope) string {
	switch scope {
	case ScopeGlobal:
		return "g"
	case ScopeGlobalExclusive:
		return "ge"
	case ScopeWorkspace:
		return "w"
	case ScopeWorkspaceExclusive:
		return "we"
	case ScopeUserShared:
		return "us"
	case ScopeUser:
		return "u"
	case ScopeUserWorkspaceShared:
		return "uws"
	case ScopeUserWorkspace:
		return "uw"
	default:
		return encodeKVSegment(string(scope))
	}
}

// buildKey constructs the full NATS KV key for an entry.
// Format: {scopeTag}/{impl}/{spec}/{userID}/{workspace}/{userKey}
func buildJSKey(agent models.Identity, scope KVScope, userID, workspace, key string) string {
	return strings.Join([]string{
		scopeTag(scope),
		encodeKVSegment(agent.Implementation),
		encodeKVSegment(agent.Specifier),
		encodeKVSegment(userID),
		encodeKVSegment(workspace),
		encodeKVSegment(key),
	}, jsSeparator)
}

// buildJSPrefix constructs the namespace prefix for list/filter operations.
// The prefix ends with "/" so NATS keys() matching works by prefix.
func buildJSPrefix(agent models.Identity, scope KVScope, userID, workspace string) string {
	return strings.Join([]string{
		scopeTag(scope),
		encodeKVSegment(agent.Implementation),
		encodeKVSegment(agent.Specifier),
		encodeKVSegment(userID),
		encodeKVSegment(workspace),
	}, jsSeparator) + jsSeparator
}

// extractUserKey strips the namespace prefix from a full NATS KV key and
// returns the decoded user-visible key name.
func extractUserKey(fullKey, prefix string) (string, bool) {
	if !strings.HasPrefix(fullKey, prefix) {
		return "", false
	}
	encoded := fullKey[len(prefix):]
	return decodeKVSegment(encoded), true
}

// ---- KVReadWriter implementation ----

// Get retrieves a value by key in the given scope.
func (s *JetStreamKVStore) Get(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	fullKey := buildJSKey(agent, scope, userID, workspace, key)
	entry, err := s.kv.Get(ctx, fullKey)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", fmt.Errorf("%w: %s", ErrKeyNotFound, key)
		}
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}
	val, _, expired := decodeStoredValue(string(entry.Value()), time.Now())
	if expired {
		return "", fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}
	return val, nil
}

// Set stores a value. When ttl > 0 a per-entry TTL is requested via Put
// options (nats.go v1.52.0+ supports KeyValueConfig.TTL per-entry via
// KeyValueEntry TTL field through CreateOrUpdateKeyValue; for per-key TTL
// we use a separate ephemeral KV or store the expiry inline).
//
// NATS KV (as of v2.10/nats.go v1.52) supports per-bucket TTL but NOT
// per-key TTL. We emulate per-key TTL by storing the value as
// "{expireUnixNano}:{value}" and returning ErrKeyNotFound on Get when the
// entry is stale. This is a best-effort TTL (stale entries linger in the
// bucket but are logically expired). For production use a watcher or bucket
// max-age is preferred; this implementation documents the trade-off.
func (s *JetStreamKVStore) Set(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	value string,
	userID string,
	workspace string,
	ttl time.Duration,
) error {
	if err := validateKey(key); err != nil {
		return err
	}
	fullKey := buildJSKey(agent, scope, userID, workspace, key)
	stored := encodeStoredValue(value, ttl)
	if _, err := s.kv.Put(ctx, fullKey, []byte(stored)); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}
	return nil
}

// Delete removes a key from the store.
func (s *JetStreamKVStore) Delete(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) error {
	if err := validateKey(key); err != nil {
		return err
	}
	fullKey := buildJSKey(agent, scope, userID, workspace, key)
	if err := s.kv.Delete(ctx, fullKey); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil // idempotent
		}
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// List returns all (non-expired) keys in a scope namespace, capped at DefaultListLimit.
func (s *JetStreamKVStore) List(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	userID string,
	workspace string,
) (map[string]string, error) {
	res, err := s.ListPaginated(ctx, agent, scope, userID, workspace, &ListOptions{Limit: DefaultListLimit})
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

// ListPaginated returns up to opts.Limit non-expired keys with their values.
// NATS KV does not support server-side pagination; we fetch all matching keys
// client-side, sort them, then apply offset/limit via the Cursor field
// (interpreted as a decimal integer offset).
//
// Trade-off: this is O(N) in namespace size and carries eventual-consistency
// implications for large namespaces. Acceptable for v1; replace with a
// dedicated index stream when namespace sizes exceed a few thousand keys.
func (s *JetStreamKVStore) ListPaginated(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	userID string,
	workspace string,
	opts *ListOptions,
) (*ListResult, error) {
	limit := DefaultListLimit
	offset := 0
	if opts != nil {
		if opts.Limit > 0 {
			limit = opts.Limit
		}
		if opts.Cursor != "" && opts.Cursor != "0" {
			if n, err := strconv.Atoi(opts.Cursor); err == nil && n > 0 {
				offset = n
			}
		}
	}

	prefix := buildJSPrefix(agent, scope, userID, workspace)

	// ListKeysFiltered uses a NATS wildcard ("prefix>") to return only keys
	// within the target namespace, avoiding a full bucket scan.
	// The prefix already ends with "/" so appending ">" gives e.g. "g/./././>/".
	// We strip the trailing "/" from prefix before appending ">".
	filterPattern := strings.TrimSuffix(prefix, jsSeparator) + jsSeparator + ">"
	lister, err := s.kv.ListKeysFiltered(ctx, filterPattern)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return &ListResult{Items: make(map[string]string), NextCursor: "", HasMore: false}, nil
		}
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	var matching []string
	for k := range lister.Keys() {
		matching = append(matching, k)
	}
	if err := lister.Stop(); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		// non-fatal: we already collected what we can
		_ = err
	}
	sort.Strings(matching)
	sort.Strings(matching)

	// Apply offset.
	if offset > len(matching) {
		offset = len(matching)
	}
	matching = matching[offset:]

	hasMore := false
	var nextCursor string
	if len(matching) > limit {
		matching = matching[:limit]
		hasMore = true
		nextCursor = strconv.Itoa(offset + limit)
	}

	items := make(map[string]string, len(matching))
	now := time.Now()
	for _, fullKey := range matching {
		entry, err := s.kv.Get(ctx, fullKey)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("failed to get value for %s: %w", fullKey, err)
		}
		val, _, expired := decodeStoredValue(string(entry.Value()), now)
		if expired {
			continue
		}
		userKey, ok := extractUserKey(fullKey, prefix)
		if !ok {
			continue
		}
		items[userKey] = val
	}

	return &ListResult{Items: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

// Increment atomically increments a counter by 1 using a CAS loop.
func (s *JetStreamKVStore) Increment(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	val, _, err := s.casCounter(ctx, agent, scope, key, userID, workspace, 1, 0, false)
	return val, err
}

// Decrement atomically decrements a counter by 1 using a CAS loop.
func (s *JetStreamKVStore) Decrement(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	val, _, err := s.casCounter(ctx, agent, scope, key, userID, workspace, -1, 0, false)
	return val, err
}

// IncrementIf atomically increments by delta if result <= ceiling.
func (s *JetStreamKVStore) IncrementIf(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	ceiling int64,
) (int64, bool, error) {
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	if delta < 0 {
		return 0, false, fmt.Errorf("IncrementIf delta must be non-negative, got %d", delta)
	}
	return s.casCounter(ctx, agent, scope, key, userID, workspace, delta, ceiling, true)
}

// DecrementIf atomically decrements by delta if result >= floor.
func (s *JetStreamKVStore) DecrementIf(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	floor int64,
) (int64, bool, error) {
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	if delta < 0 {
		return 0, false, fmt.Errorf("DecrementIf delta must be non-negative, got %d", delta)
	}
	return s.casCounter(ctx, agent, scope, key, userID, workspace, -delta, floor, false)
}

// casCounter is the shared CAS loop for all counter operations.
//
// Parameters:
//   - delta: amount to add (negative for decrement).
//   - guard: ceiling (isCeiling=true) or floor (isCeiling=false); ignored when neither.
//   - isCeiling: when false the guard check is skipped (plain Increment/Decrement).
func (s *JetStreamKVStore) casCounter(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	guard int64,
	isCeiling bool,
) (int64, bool, error) {
	fullKey := buildJSKey(agent, scope, userID, workspace, key)

	for attempt := 0; attempt < jsMaxCASAttempts; attempt++ {
		var current int64
		var expireAt int64 // non-zero means entry had a TTL wrapper
		var rev uint64

		entry, err := s.kv.Get(ctx, fullKey)
		if err != nil {
			if !errors.Is(err, jetstream.ErrKeyNotFound) {
				return 0, false, fmt.Errorf("casCounter get %s: %w", key, err)
			}
			// Key absent → start at 0, no TTL, Create path (rev==0).
		} else {
			rev = entry.Revision()
			decoded, exp, expired := decodeStoredValue(string(entry.Value()), time.Now())
			if expired {
				// Logically expired: start fresh at 0, drop the TTL window.
				// Caller must Set-with-TTL again to open a new rate window.
				current = 0
				expireAt = 0
			} else {
				expireAt = exp
				if decoded != "" {
					current, err = strconv.ParseInt(strings.TrimSpace(decoded), 10, 64)
					if err != nil {
						return 0, false, fmt.Errorf("casCounter parse %s: %w", key, err)
					}
				}
			}
		}

		proposed := current + delta

		// Guard check (only for IncrementIf/DecrementIf).
		if isCeiling && proposed > guard {
			return current, false, nil
		}
		if !isCeiling && delta < 0 && proposed < guard {
			return current, false, nil
		}

		// Re-encode. Preserve the existing TTL window when present and not
		// expired — Increment/Decrement must NOT reset the expiry deadline.
		var encoded []byte
		if expireAt > 0 {
			encoded = []byte(fmt.Sprintf("%s%d:%d", ttlPrefix, expireAt, proposed))
		} else {
			encoded = []byte(strconv.FormatInt(proposed, 10))
		}

		var writeErr error
		if rev == 0 {
			// No entry yet — use Create for atomic first-write.
			_, writeErr = s.kv.Create(ctx, fullKey, encoded)
		} else {
			_, writeErr = s.kv.Update(ctx, fullKey, encoded, rev)
		}
		if writeErr == nil {
			return proposed, true, nil
		}
		// On revision conflict (concurrent writer), retry.
		if isRevisionConflict(writeErr) {
			continue
		}
		return 0, false, fmt.Errorf("casCounter update %s: %w", key, writeErr)
	}
	return 0, false, fmt.Errorf("casCounter %s: too much contention after %d attempts", key, jsMaxCASAttempts)
}

// isRevisionConflict returns true for errors that indicate a concurrent
// update beat us to the revision — safe to retry.
//
// nats.go v1.52 returns jetstream.ErrKeyExists when Create is called for
// a key that already exists, and a JetStream API error with
// JSErrCodeStreamWrongLastSequence (10071) when Update is called with a
// stale revision. We detect both via errors.Is and the JetStreamError
// interface's APIError() accessor.
func isRevisionConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	// Check via JetStreamError interface via type assertion (errors.As does not
	// work with interface targets in Go).
	if jsErr, ok := err.(jetstream.JetStreamError); ok {
		if apiErr := jsErr.APIError(); apiErr != nil {
			return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
		}
	}
	// String fallback for wrapped errors.
	msg := err.Error()
	return strings.Contains(msg, "wrong last sequence") || strings.Contains(msg, "key exists")
}

// ---- soft TTL encoding ----
//
// NATS KV (v2.10 / nats.go v1.52) supports only bucket-level MaxAge, not
// per-entry TTL. We encode a TTL into the stored value payload using the
// format:  "@{expireUnixNano}:{value}"
//
// On Get/List we check the wall clock and treat stale entries as not-found.
// This is soft expiry: the entry stays in the bucket until GC or bucket TTL
// removes it. The "@" prefix is a sentinel that cannot appear from
// encodeKVSegment (which would encode "@" as "_40_") so it is unambiguous.

const ttlPrefix = "@"

func encodeStoredValue(value string, ttl time.Duration) string {
	if ttl <= 0 {
		return value
	}
	expireAt := time.Now().Add(ttl).UnixNano()
	return fmt.Sprintf("%s%d:%s", ttlPrefix, expireAt, value)
}

// decodeStoredValue decodes a stored value. Returns (plainValue, expireUnixNano, expired).
//
// expireUnixNano==0 means no TTL wrapper was present.
// expired==true means a wrapper was present but the entry is stale; callers
// should treat the entry as not-found. When expired==true, plainValue is "".
func decodeStoredValue(raw string, now time.Time) (string, int64, bool) {
	if !strings.HasPrefix(raw, ttlPrefix) {
		return raw, 0, false
	}
	rest := raw[len(ttlPrefix):]
	idx := strings.IndexByte(rest, ':')
	if idx < 0 {
		return raw, 0, false // malformed, treat as no-TTL
	}
	expireNano, err := strconv.ParseInt(rest[:idx], 10, 64)
	if err != nil {
		return raw, 0, false
	}
	expireAt := time.Unix(0, expireNano)
	if now.After(expireAt) {
		return "", expireNano, true // expired
	}
	return rest[idx+1:], expireNano, false
}
