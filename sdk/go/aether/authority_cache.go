// Package aether authority grant cache for the Go SDK.
//
// This file provides AuthorityGrantCache, a per-actor cache that keeps
// recently-issued runtime authority grants warm and proactively invalidates
// them when the gateway pushes AuthorityGrantRevocation events on the
// downstream stream.
//
// Behaviour mirrors the Python AuthorityGrantCache (see
// sdk/python-client/scitrera_aether_client/authority_cache.py):
//
//   - Grants are keyed by (sourceSessionID, audienceType, audienceID).
//   - Cached entries are served while not past expires_at - softRenewSkew,
//     and the gateway has not pushed an AuthorityGrantRevocation for the
//     grant_id OR root_grant_id.
//   - Server-supplied cache_hint_ttl_seconds is honoured as an upper bound.
//   - In-flight fetches per cache key are de-duplicated via per-key Mutex.
//   - DeriveForTask uses the idempotent DERIVE_FOR_TARGET op so repeated
//     calls return the same grant rather than minting new ones.

package aether

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultSoftRenewSkew is the default time before expires_at at which a
// cached grant is considered stale and will be re-exchanged. Mirrors the
// Python default and matches the connection-lock heartbeat cadence.
const DefaultSoftRenewSkew = 30 * time.Second

// authorityCacheKey identifies a cache slot. Empty strings are valid for
// any field (an exchange with no audience).
type authorityCacheKey struct {
	sourceSessionID string
	audienceType    string
	audienceID      string
}

// authorityCacheEntry holds a cached grant plus the metadata needed to
// decide when to refresh it.
type authorityCacheEntry struct {
	grant            *pb.ACLAuthorityGrantInfo
	expiresAtUnix    int64 // proto units; 0 if grant has no expiry
	cacheHintTtlS    int32 // 0 if server didn't supply a hint
	cachedAtUnixNano int64 // local wall-clock (nanos) when stored
}

// AuthorityGrantCacheOption configures a new AuthorityGrantCache.
type AuthorityGrantCacheOption func(*AuthorityGrantCache)

// WithSoftRenewSkew sets the soft-renew skew. Cached grants are considered
// stale this long before their server-side expiry. Must be non-negative.
func WithSoftRenewSkew(skew time.Duration) AuthorityGrantCacheOption {
	return func(c *AuthorityGrantCache) {
		if skew < 0 {
			skew = 0
		}
		c.softRenewSkew = skew
	}
}

// WithOpTimeout overrides the per-op timeout passed to the underlying
// AuthorityGrantOps when the cache issues exchange/derive/revoke calls.
func WithOpTimeout(timeout time.Duration) AuthorityGrantCacheOption {
	return func(c *AuthorityGrantCache) {
		if timeout > 0 {
			c.opTimeout = timeout
		}
	}
}

// WithClock overrides the cache's wall-clock source. Useful for tests.
func WithClock(clock func() time.Time) AuthorityGrantCacheOption {
	return func(c *AuthorityGrantCache) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// WithExpiresAtUnitMillis tells the cache to interpret
// ACLAuthorityGrantInfo.expires_at as unix-MILLISECONDS rather than
// unix-seconds. Match the projection your gateway emits (see proto
// comments). Default is unix-SECONDS.
func WithExpiresAtUnitMillis(millis bool) AuthorityGrantCacheOption {
	return func(c *AuthorityGrantCache) {
		c.expiresInMillis = millis
	}
}

// AuthorityGrantCache caches runtime authority grants and listens for
// revocation pushes from the gateway. Concurrency-safe.
type AuthorityGrantCache struct {
	ops *AuthorityGrantOps

	softRenewSkew   time.Duration
	opTimeout       time.Duration
	clock           func() time.Time
	expiresInMillis bool

	mu      sync.RWMutex
	entries map[authorityCacheKey]*authorityCacheEntry

	// grant_id -> set of cache keys for fast revocation lookup.
	grantIDIndex map[string]map[authorityCacheKey]struct{}
	// root_grant_id -> set of cache keys (cascade invalidation).
	rootGrantIDIndex map[string]map[authorityCacheKey]struct{}

	// Per-key fetch locks to single-flight concurrent get-or-exchange calls.
	fetchMu     sync.Mutex
	fetchLocks  map[authorityCacheKey]*sync.Mutex
	closedFlag  bool
	parent      interface{ removeAuthorityCache(*AuthorityGrantCache) }
	logRevokeFn func(grantID string, err error)
}

// NewAuthorityGrantCache constructs a cache wired to the supplied
// AuthorityGrantOps. Most callers should use BaseClient.MakeAuthorityCache
// instead so the client routes AuthorityGrantRevocation events to it.
func NewAuthorityGrantCache(ops *AuthorityGrantOps, opts ...AuthorityGrantCacheOption) *AuthorityGrantCache {
	cache := &AuthorityGrantCache{
		ops:              ops,
		softRenewSkew:    DefaultSoftRenewSkew,
		opTimeout:        DefaultAuthorityGrantTimeout,
		clock:            time.Now,
		entries:          make(map[authorityCacheKey]*authorityCacheEntry),
		grantIDIndex:     make(map[string]map[authorityCacheKey]struct{}),
		rootGrantIDIndex: make(map[string]map[authorityCacheKey]struct{}),
		fetchLocks:       make(map[authorityCacheKey]*sync.Mutex),
		logRevokeFn: func(grantID string, err error) {
			log.Printf("aether.AuthorityGrantCache: revoke %s failed: %v", grantID, err)
		},
	}
	for _, opt := range opts {
		opt(cache)
	}
	return cache
}

// =============================================================================
// Public API
// =============================================================================

// GetOrExchange returns a cached grant for the (source_session_id,
// audience_type, audience_id) triplet, or exchanges a fresh one when the
// cache is missing/stale. Callers MUST keep the rest of the request shape
// stable for a given cache key.
//
// Returns (nil, nil) when the gateway responds with success=false or no
// grant — callers should fall back to direct invocation or surface the
// underlying response.
func (c *AuthorityGrantCache) GetOrExchange(ctx context.Context, sourceSessionID, audienceType, audienceID string, opts ExchangeOpts) (*pb.ACLAuthorityGrantInfo, error) {
	key := authorityCacheKey{
		sourceSessionID: sourceSessionID,
		audienceType:    audienceType,
		audienceID:      audienceID,
	}

	if cached := c.getUnexpired(key); cached != nil {
		return cached, nil
	}

	lock := c.inflightLock(key)
	lock.Lock()
	defer lock.Unlock()

	if cached := c.getUnexpired(key); cached != nil {
		return cached, nil
	}

	if opts.AudienceType == "" {
		opts.AudienceType = audienceType
	}
	if opts.AudienceID == "" {
		opts.AudienceID = audienceID
	}
	if opts.Timeout == 0 {
		opts.Timeout = c.opTimeout
	}

	resp, err := c.ops.Exchange(ctx, sourceSessionID, opts)
	if err != nil {
		return nil, err
	}
	grant := extractGrant(resp)
	if grant == nil {
		return nil, nil
	}
	c.store(key, grant, resp)
	return grant, nil
}

// DeriveForTask idempotently derives a child grant scoped to a target task
// (target_type defaults to "task"). Uses the DERIVE_FOR_TARGET op so the
// gateway returns an existing visible grant when one is already in place
// — making this safe to call repeatedly.
func (c *AuthorityGrantCache) DeriveForTask(ctx context.Context, parentGrantID, taskID string, opts DeriveForTargetOpts) (*pb.ACLAuthorityGrantInfo, error) {
	return c.DeriveForTarget(ctx, parentGrantID, "task", taskID, opts)
}

// DeriveForTarget is the general form of DeriveForTask supporting any
// target principal type (task, agent, user, ...).
func (c *AuthorityGrantCache) DeriveForTarget(ctx context.Context, parentGrantID, targetType, targetID string, opts DeriveForTargetOpts) (*pb.ACLAuthorityGrantInfo, error) {
	key := authorityCacheKey{
		sourceSessionID: "derive::" + parentGrantID + "::" + targetType + "::" + targetID,
		audienceType:    opts.AudienceType,
		audienceID:      opts.AudienceID,
	}

	if cached := c.getUnexpired(key); cached != nil {
		return cached, nil
	}

	lock := c.inflightLock(key)
	lock.Lock()
	defer lock.Unlock()

	if cached := c.getUnexpired(key); cached != nil {
		return cached, nil
	}

	if opts.Timeout == 0 {
		opts.Timeout = c.opTimeout
	}

	resp, err := c.ops.DeriveForTarget(ctx, parentGrantID, targetType, targetID, opts)
	if err != nil {
		return nil, err
	}
	grant := extractGrant(resp)
	if grant == nil {
		return nil, nil
	}
	c.store(key, grant, resp)
	return grant, nil
}

// Invalidate drops every cache entry whose grant ID or root grant ID
// matches the supplied ID. Returns the number of entries dropped.
func (c *AuthorityGrantCache) Invalidate(grantIDOrRoot string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.invalidateLocked(grantIDOrRoot)
}

// invalidateLocked drops entries for a grant id or root grant id. Must be
// called with c.mu held for write.
func (c *AuthorityGrantCache) invalidateLocked(grantIDOrRoot string) int {
	dropped := 0
	for _, idx := range []map[string]map[authorityCacheKey]struct{}{c.grantIDIndex, c.rootGrantIDIndex} {
		keys := idx[grantIDOrRoot]
		for key := range keys {
			entry, ok := c.entries[key]
			if !ok {
				continue
			}
			delete(c.entries, key)
			c.unindexLocked(key, entry.grant)
			dropped++
		}
		delete(idx, grantIDOrRoot)
	}
	return dropped
}

// RevokeAll best-effort revokes every cached grant on the gateway, then
// clears the cache. Per-grant errors are logged but not returned.
func (c *AuthorityGrantCache) RevokeAll(ctx context.Context) error {
	c.mu.Lock()
	entries := make([]*authorityCacheEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		entries = append(entries, entry)
	}
	c.entries = make(map[authorityCacheKey]*authorityCacheEntry)
	c.grantIDIndex = make(map[string]map[authorityCacheKey]struct{})
	c.rootGrantIDIndex = make(map[string]map[authorityCacheKey]struct{})
	c.mu.Unlock()

	for _, entry := range entries {
		grantID := entry.grant.GetGrantId()
		if grantID == "" {
			continue
		}
		if _, err := c.ops.Revoke(ctx, grantID); err != nil {
			c.logRevokeFn(grantID, err)
		}
	}
	return nil
}

// HandleRevocationEvent invalidates any cache entries matching the event's
// grant_id and/or root_grant_id. Wired into BaseClient by
// MakeAuthorityCache; tests may also call this directly.
func (c *AuthorityGrantCache) HandleRevocationEvent(evt *pb.AuthorityGrantRevocation) int {
	if evt == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	dropped := 0
	if grantID := evt.GetGrantId(); grantID != "" {
		dropped += c.invalidateLocked(grantID)
	}
	if rootID := evt.GetRootGrantId(); rootID != "" && rootID != evt.GetGrantId() {
		dropped += c.invalidateLocked(rootID)
	}
	return dropped
}

// Stats returns cache observability counters.
type AuthorityGrantCacheStats struct {
	Size                int
	GrantIDsIndexed     int
	RootGrantIDsIndexed int
}

// Stats returns a snapshot of cache observability counters.
func (c *AuthorityGrantCache) Stats() AuthorityGrantCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return AuthorityGrantCacheStats{
		Size:                len(c.entries),
		GrantIDsIndexed:     len(c.grantIDIndex),
		RootGrantIDsIndexed: len(c.rootGrantIDIndex),
	}
}

// =============================================================================
// High-level convenience helpers (Phase 4)
// =============================================================================

// IsValid reports whether grantID is currently cached and fresh (not
// revoked, not past its soft-renew deadline). Cache-only — never
// round-trips to the gateway. Stale or revoked entries observed by this
// call are evicted as a side-effect.
func (c *AuthorityGrantCache) IsValid(grantID string) bool {
	if grantID == "" {
		return false
	}
	c.mu.RLock()
	keys := make([]authorityCacheKey, 0, len(c.grantIDIndex[grantID]))
	for key := range c.grantIDIndex[grantID] {
		keys = append(keys, key)
	}
	c.mu.RUnlock()
	for _, key := range keys {
		if grant := c.getUnexpired(key); grant != nil {
			return true
		}
	}
	return false
}

// ListActive returns every cached grant that is currently fresh,
// de-duplicated by grant_id. Stale/revoked entries observed during the
// snapshot are evicted as a side-effect (same path as GetOrExchange).
func (c *AuthorityGrantCache) ListActive() []*pb.ACLAuthorityGrantInfo {
	c.mu.RLock()
	keys := make([]authorityCacheKey, 0, len(c.entries))
	for key := range c.entries {
		keys = append(keys, key)
	}
	c.mu.RUnlock()
	seen := make(map[string]struct{}, len(keys))
	out := make([]*pb.ACLAuthorityGrantInfo, 0, len(keys))
	for _, key := range keys {
		grant := c.getUnexpired(key)
		if grant == nil {
			continue
		}
		id := grant.GetGrantId()
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, grant)
	}
	return out
}

// RevokeLocal drops grantID from the cache without calling the gateway.
// Alias of Invalidate with a name that telegraphs the local-only
// semantics; the matching server-side revoke is AuthorityGrantOps.Revoke
// or the admin client's RevokeAuthorityGrant. Returns the number of
// entries dropped.
func (c *AuthorityGrantCache) RevokeLocal(grantID string) int {
	return c.Invalidate(grantID)
}

// Refresh force-drops a cached grant and re-exchanges it.
//
// Returns (nil, nil) when the cache doesn't know grantID, or when the
// underlying exchange fails. Derived entries (those produced via
// DeriveForTask / DeriveForTarget) cannot be refreshed this way — they
// require their original parent/target identity — and return (nil, nil)
// after being dropped from the cache; callers should re-derive
// explicitly.
func (c *AuthorityGrantCache) Refresh(ctx context.Context, grantID string, opts ExchangeOpts) (*pb.ACLAuthorityGrantInfo, error) {
	if grantID == "" {
		return nil, nil
	}
	c.mu.RLock()
	var key authorityCacheKey
	var found bool
	for k := range c.grantIDIndex[grantID] {
		key = k
		found = true
		break
	}
	c.mu.RUnlock()
	if !found {
		return nil, nil
	}
	c.Invalidate(grantID)
	// Derived entries use a synthetic sourceSessionID prefix that
	// Exchange cannot reproduce; signal the caller to re-derive.
	if len(key.sourceSessionID) >= 8 && key.sourceSessionID[:8] == "derive::" {
		return nil, nil
	}
	return c.GetOrExchange(ctx, key.sourceSessionID, key.audienceType, key.audienceID, opts)
}

// Close de-registers this cache from its parent client (if any) so it no
// longer receives revocation events. Safe to call multiple times.
func (c *AuthorityGrantCache) Close() {
	c.mu.Lock()
	if c.closedFlag {
		c.mu.Unlock()
		return
	}
	c.closedFlag = true
	parent := c.parent
	c.mu.Unlock()
	if parent != nil {
		parent.removeAuthorityCache(c)
	}
}

// =============================================================================
// Internals
// =============================================================================

func (c *AuthorityGrantCache) getUnexpired(key authorityCacheKey) *pb.ACLAuthorityGrantInfo {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil
	}
	if entry.grant.GetRevoked() {
		c.mu.Lock()
		// Double-check after re-locking in case another goroutine refreshed.
		if cur, stillThere := c.entries[key]; stillThere && cur == entry {
			delete(c.entries, key)
			c.unindexLocked(key, entry.grant)
		}
		c.mu.Unlock()
		return nil
	}
	now := c.clock()
	deadline := c.effectiveDeadline(entry)
	if !deadline.IsZero() && !now.Before(deadline) {
		c.mu.Lock()
		if cur, stillThere := c.entries[key]; stillThere && cur == entry {
			delete(c.entries, key)
			c.unindexLocked(key, entry.grant)
		}
		c.mu.Unlock()
		return nil
	}
	return entry.grant
}

func (c *AuthorityGrantCache) effectiveDeadline(entry *authorityCacheEntry) time.Time {
	var deadline time.Time
	if entry.expiresAtUnix > 0 {
		var expiresAt time.Time
		if c.expiresInMillis {
			expiresAt = time.Unix(0, entry.expiresAtUnix*int64(time.Millisecond))
		} else {
			expiresAt = time.Unix(entry.expiresAtUnix, 0)
		}
		soft := expiresAt.Add(-c.softRenewSkew)
		deadline = soft
	}
	if entry.cacheHintTtlS > 0 {
		hintDeadline := time.Unix(0, entry.cachedAtUnixNano).Add(time.Duration(entry.cacheHintTtlS) * time.Second)
		if deadline.IsZero() || hintDeadline.Before(deadline) {
			deadline = hintDeadline
		}
	}
	return deadline
}

func (c *AuthorityGrantCache) store(key authorityCacheKey, grant *pb.ACLAuthorityGrantInfo, resp *pb.AuthorityGrantResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if prior, ok := c.entries[key]; ok {
		c.unindexLocked(key, prior.grant)
	}

	var hint int32
	if resp != nil {
		hint = resp.GetCacheHintTtlSeconds()
	}
	entry := &authorityCacheEntry{
		grant:            grant,
		expiresAtUnix:    grant.GetExpiresAt(),
		cacheHintTtlS:    hint,
		cachedAtUnixNano: c.clock().UnixNano(),
	}
	c.entries[key] = entry
	c.indexLocked(key, grant)
}

func (c *AuthorityGrantCache) indexLocked(key authorityCacheKey, grant *pb.ACLAuthorityGrantInfo) {
	if id := grant.GetGrantId(); id != "" {
		set, ok := c.grantIDIndex[id]
		if !ok {
			set = make(map[authorityCacheKey]struct{})
			c.grantIDIndex[id] = set
		}
		set[key] = struct{}{}
	}
	if rootID := grant.GetRootGrantId(); rootID != "" && rootID != grant.GetGrantId() {
		set, ok := c.rootGrantIDIndex[rootID]
		if !ok {
			set = make(map[authorityCacheKey]struct{})
			c.rootGrantIDIndex[rootID] = set
		}
		set[key] = struct{}{}
	}
}

func (c *AuthorityGrantCache) unindexLocked(key authorityCacheKey, grant *pb.ACLAuthorityGrantInfo) {
	if id := grant.GetGrantId(); id != "" {
		if set, ok := c.grantIDIndex[id]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(c.grantIDIndex, id)
			}
		}
	}
	if rootID := grant.GetRootGrantId(); rootID != "" {
		if set, ok := c.rootGrantIDIndex[rootID]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(c.rootGrantIDIndex, rootID)
			}
		}
	}
}

func (c *AuthorityGrantCache) inflightLock(key authorityCacheKey) *sync.Mutex {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	lock, ok := c.fetchLocks[key]
	if !ok {
		lock = &sync.Mutex{}
		c.fetchLocks[key] = lock
	}
	return lock
}

// extractGrant returns the response's grant when success=true and grant_id
// is non-empty, otherwise nil. Mirrors the Python helper.
func extractGrant(resp *pb.AuthorityGrantResponse) *pb.ACLAuthorityGrantInfo {
	if resp == nil || !resp.GetSuccess() {
		return nil
	}
	grant := resp.GetGrant()
	if grant == nil || grant.GetGrantId() == "" {
		return nil
	}
	return grant
}
