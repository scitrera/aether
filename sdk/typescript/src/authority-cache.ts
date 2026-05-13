/**
 * Authority grant cache for the Aether TypeScript client.
 *
 * Mirrors the Python `AuthorityGrantCache` (see
 * `sdk/python-client/scitrera_aether_client/authority_cache.py`):
 *
 * - Caches grants per `(sourceSessionId, audienceType, audienceId)`.
 * - Soft-renews at `expiresAt - softRenewSkewMs` (configurable).
 * - Honors server `cacheHintTtlSeconds` from `AuthorityGrantResponse` as
 *   an upper bound on the cached lifetime.
 * - Listens for `AuthorityGrantRevocation` push events and invalidates
 *   matching entries by `grantId` OR `rootGrantId` (cascade).
 * - `deriveForTask(...)` uses the idempotent `DERIVE_FOR_TARGET` op so
 *   repeated calls return the same grant rather than minting new ones.
 *
 * Concurrency: per-key in-flight Promise dedupe collapses concurrent
 * `getOrExchange` / `deriveForTask` calls so only one network round-trip
 * happens per cache key at a time.
 *
 * @module authority-cache
 */

import type { AetherClient } from "./client.js";
import type { AuthorityGrantInfo, AuthorityGrantResponse, AuthorityGrantRevocation } from "./types.js";

/**
 * Configuration for {@link AuthorityGrantCache}.
 */
export interface AuthorityGrantCacheOptions {
  /**
   * Soft-renew skew, in milliseconds. Re-exchange a grant this long
   * before its server-side `expiresAt`. Default: 30000 (30 s) to match
   * the connection-lock heartbeat cadence.
   */
  softRenewSkewMs?: number;

  /**
   * Default per-op timeout (ms) passed through to the underlying client
   * when the cache issues exchange / derive / revoke calls. Default:
   * 10000.
   */
  opTimeoutMs?: number;

  /**
   * Wall-clock provider, returning unix-ms. Pluggable for tests.
   * Default: `Date.now`.
   */
  clock?: () => number;

  /**
   * When true the cache treats `AuthorityGrantInfo.expiresAt` as
   * unix-MILLISECONDS rather than unix-seconds. The proto comment on
   * `AuthorityGrantInfo` says "Unix seconds" but the runtime
   * `ACLAuthorityGrantInfo` uses milliseconds in practice — pick the
   * unit your gateway emits. Default: `false` (seconds).
   */
  expiresAtInMillis?: boolean;
}

/** Subset of {@link AetherClient} the cache depends on. */
export interface AuthorityCacheClient {
  exchangeAuthorityGrant(opts: Record<string, unknown>): Promise<AuthorityGrantResponse>;
  deriveAuthorityGrantForTarget(opts: Record<string, unknown>): Promise<AuthorityGrantResponse>;
  revokeAuthorityGrant(grantId: string, timeout?: number): Promise<AuthorityGrantResponse>;
  _removeAuthorityCache?(cache: AuthorityGrantCache): void;
}

/** Composite cache key. */
type CacheKey = string;

interface CacheEntry {
  grant: AuthorityGrantInfo;
  /** Absolute deadline (unix-ms) at which this entry is considered stale. */
  effectiveDeadlineMs: number;
  /** Absolute server-side expires_at, in proto units (see {@link AuthorityGrantCacheOptions.expiresAtInMillis}). */
  rawExpiresAt: number;
  cacheHintTtlS: number;
  fetchedAtMs: number;
}

/**
 * Per-actor cache of runtime authority grants. See module docstring for
 * the behavioural contract.
 */
export class AuthorityGrantCache {
  private readonly _client: AuthorityCacheClient;
  private readonly _softRenewSkewMs: number;
  private readonly _opTimeoutMs: number;
  private readonly _clock: () => number;
  private readonly _expiresAtInMillis: boolean;

  private readonly _entries = new Map<CacheKey, CacheEntry>();
  /** grantId -> set of cache keys, for fast revocation lookup. */
  private readonly _grantIdIndex = new Map<string, Set<CacheKey>>();
  /** rootGrantId -> set of cache keys, for cascade invalidation. */
  private readonly _rootGrantIdIndex = new Map<string, Set<CacheKey>>();
  /** In-flight per-key fetch promises, for single-flight de-dupe. */
  private readonly _inflight = new Map<CacheKey, Promise<AuthorityGrantInfo | null>>();

  private _closed = false;

  constructor(client: AuthorityCacheClient, opts: AuthorityGrantCacheOptions = {}) {
    if (opts.softRenewSkewMs !== undefined && opts.softRenewSkewMs < 0) {
      throw new Error("softRenewSkewMs must be non-negative");
    }
    this._client = client;
    this._softRenewSkewMs = opts.softRenewSkewMs ?? 30_000;
    this._opTimeoutMs = opts.opTimeoutMs ?? 10_000;
    this._clock = opts.clock ?? (() => Date.now());
    this._expiresAtInMillis = opts.expiresAtInMillis ?? false;
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Return a cached grant for the (sourceSessionId, audienceType,
   * audienceId) triplet, or exchange a fresh one. Callers MUST keep the
   * rest of the request shape stable for a given key.
   *
   * Returns `null` when the gateway response is missing, has
   * `success === false`, or carries no grant — callers should fall back
   * to direct invocation or surface the error.
   */
  async getOrExchange(opts: {
    sourceSessionId: string;
    audienceType?: string;
    audienceId?: string;
    workspaceScope?: string[];
    resourceScope?: { resourceType: string; patterns: string[] }[];
    operationScope?: string[];
    maxAccessLevel?: number;
    validWhileAudienceActive?: boolean;
    expiresAt?: number;
    renewableUntil?: number;
    mayDelegate?: boolean;
    remainingHops?: number;
    reason?: string;
    metadata?: Record<string, string>;
    timeout?: number;
  }): Promise<AuthorityGrantInfo | null> {
    const audienceType = opts.audienceType ?? "";
    const audienceId = opts.audienceId ?? "";
    const key = makeKey(opts.sourceSessionId, audienceType, audienceId);

    const cached = this._getUnexpired(key);
    if (cached !== null) {
      return cached;
    }

    return this._withInflight(key, async () => {
      const cachedAfterLock = this._getUnexpired(key);
      if (cachedAfterLock !== null) {
        return cachedAfterLock;
      }
      const response = await this._client.exchangeAuthorityGrant({
        sourceSessionId: opts.sourceSessionId,
        audienceType,
        audienceId,
        workspaceScope: opts.workspaceScope,
        resourceScope: opts.resourceScope,
        operationScope: opts.operationScope,
        maxAccessLevel: opts.maxAccessLevel,
        validWhileAudienceActive: opts.validWhileAudienceActive,
        expiresAt: opts.expiresAt,
        renewableUntil: opts.renewableUntil,
        mayDelegate: opts.mayDelegate,
        remainingHops: opts.remainingHops,
        reason: opts.reason,
        metadata: opts.metadata,
        timeout: opts.timeout ?? this._opTimeoutMs,
      });
      const grant = extractGrant(response);
      if (!grant) {
        return null;
      }
      this._store(key, grant, response);
      return grant;
    });
  }

  /**
   * Idempotent derive for a target task: returns an existing visible
   * grant matching (parentGrantId, taskId, audience) when one is in
   * place, otherwise mints a new one via `DERIVE_FOR_TARGET`. Safe to
   * call repeatedly without leaking grants.
   */
  async deriveForTask(opts: {
    parentGrantId: string;
    taskId: string;
    audienceType?: string;
    audienceId?: string;
    operationScope?: string[];
    maxAccessLevel?: number;
    expiresAt?: number;
    renewableUntil?: number;
    mayDelegate?: boolean;
    remainingHops?: number;
    reason?: string;
    timeout?: number;
  }): Promise<AuthorityGrantInfo | null> {
    return this.deriveForTarget({
      parentGrantId: opts.parentGrantId,
      targetType: "task",
      targetId: opts.taskId,
      audienceType: opts.audienceType,
      audienceId: opts.audienceId,
      operationScope: opts.operationScope,
      maxAccessLevel: opts.maxAccessLevel,
      expiresAt: opts.expiresAt,
      renewableUntil: opts.renewableUntil,
      mayDelegate: opts.mayDelegate,
      remainingHops: opts.remainingHops,
      reason: opts.reason,
      timeout: opts.timeout,
    });
  }

  /** General form of {@link deriveForTask} for arbitrary target principals. */
  async deriveForTarget(opts: {
    parentGrantId: string;
    targetType: string;
    targetId: string;
    audienceType?: string;
    audienceId?: string;
    operationScope?: string[];
    maxAccessLevel?: number;
    expiresAt?: number;
    renewableUntil?: number;
    mayDelegate?: boolean;
    remainingHops?: number;
    reason?: string;
    timeout?: number;
  }): Promise<AuthorityGrantInfo | null> {
    const audienceType = opts.audienceType ?? "";
    const audienceId = opts.audienceId ?? "";
    const key = makeKey(
      `derive::${opts.parentGrantId}::${opts.targetType}::${opts.targetId}`,
      audienceType,
      audienceId,
    );

    const cached = this._getUnexpired(key);
    if (cached !== null) {
      return cached;
    }

    return this._withInflight(key, async () => {
      const cachedAfterLock = this._getUnexpired(key);
      if (cachedAfterLock !== null) {
        return cachedAfterLock;
      }
      const response = await this._client.deriveAuthorityGrantForTarget({
        parentGrantId: opts.parentGrantId,
        targetType: opts.targetType,
        targetId: opts.targetId,
        audienceType,
        audienceId,
        operationScope: opts.operationScope,
        maxAccessLevel: opts.maxAccessLevel,
        expiresAt: opts.expiresAt,
        renewableUntil: opts.renewableUntil,
        mayDelegate: opts.mayDelegate,
        remainingHops: opts.remainingHops,
        reason: opts.reason,
        timeout: opts.timeout ?? this._opTimeoutMs,
      });
      const grant = extractGrant(response);
      if (!grant) {
        return null;
      }
      this._store(key, grant, response);
      return grant;
    });
  }

  /**
   * Drop every cache entry whose grant ID OR root grant ID matches.
   * Returns the number of entries dropped. Safe to call from any
   * context.
   */
  invalidate(grantIdOrRoot: string): number {
    let dropped = 0;
    for (const idx of [this._grantIdIndex, this._rootGrantIdIndex]) {
      const keys = idx.get(grantIdOrRoot);
      if (!keys) {
        continue;
      }
      // Snapshot the keys because _drop mutates the index.
      for (const key of [...keys]) {
        if (this._drop(key)) {
          dropped++;
        }
      }
      idx.delete(grantIdOrRoot);
    }
    return dropped;
  }

  /**
   * Best-effort revoke every cached grant on the gateway, then clear.
   * Per-grant errors are caught and logged via `console.warn`.
   */
  async revokeAll(): Promise<void> {
    const entries = [...this._entries.values()];
    this._entries.clear();
    this._grantIdIndex.clear();
    this._rootGrantIdIndex.clear();

    await Promise.all(
      entries.map(async (entry) => {
        const grantId = entry.grant.grantId;
        if (!grantId) {
          return;
        }
        try {
          await this._client.revokeAuthorityGrant(grantId, this._opTimeoutMs);
        } catch (err) {
          console.warn(`AuthorityGrantCache.revokeAll: revoke ${grantId} failed`, err);
        }
      }),
    );
  }

  /**
   * Invalidate cache entries matching a server-pushed revocation event.
   * Returns the number of entries dropped. Wired into the client
   * dispatch loop by {@link AetherClient.makeAuthorityCache}; tests may
   * also call this directly.
   */
  handleRevocationEvent(evt: AuthorityGrantRevocation): number {
    let dropped = 0;
    if (evt.grantId) {
      dropped += this.invalidate(evt.grantId);
    }
    if (evt.rootGrantId && evt.rootGrantId !== evt.grantId) {
      dropped += this.invalidate(evt.rootGrantId);
    }
    return dropped;
  }

  /** Cache stats for observability tests. */
  stats(): { size: number; grantIdsIndexed: number; rootGrantIdsIndexed: number } {
    return {
      size: this._entries.size,
      grantIdsIndexed: this._grantIdIndex.size,
      rootGrantIdsIndexed: this._rootGrantIdIndex.size,
    };
  }

  // ===========================================================================
  // High-level convenience helpers (Phase 4)
  // ===========================================================================

  /**
   * Report whether `grantId` is currently cached and fresh (not revoked,
   * not past its soft-renew deadline). Cache-only — never round-trips to
   * the gateway. Stale/revoked entries observed here are evicted as a
   * side-effect.
   */
  isValid(grantId: string): boolean {
    if (!grantId) {
      return false;
    }
    const keys = this._grantIdIndex.get(grantId);
    if (!keys || keys.size === 0) {
      return false;
    }
    // Snapshot — `_getUnexpired` mutates the index when it evicts.
    for (const key of [...keys]) {
      if (this._getUnexpired(key) !== null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return a snapshot of every cached grant that is currently fresh,
   * de-duplicated by `grantId`. Stale/revoked entries observed during
   * the snapshot are evicted as a side-effect.
   */
  listActive(): AuthorityGrantInfo[] {
    const out: AuthorityGrantInfo[] = [];
    const seen = new Set<string>();
    // Snapshot keys first so eviction during iteration is safe.
    for (const key of [...this._entries.keys()]) {
      const grant = this._getUnexpired(key);
      if (!grant || !grant.grantId || seen.has(grant.grantId)) {
        continue;
      }
      seen.add(grant.grantId);
      out.push(grant);
    }
    return out;
  }

  /**
   * Drop `grantId` from the cache without calling the gateway. Alias of
   * {@link invalidate} with a name that telegraphs the local-only
   * semantics; the matching server-side revoke is
   * {@link AetherClient.revokeAuthorityGrant}. Returns the number of
   * entries dropped.
   */
  revokeLocal(grantId: string): number {
    return this.invalidate(grantId);
  }

  /**
   * Force-drop a cached grant and re-exchange it.
   *
   * Returns `null` when the cache does not know `grantId`, the matching
   * entry was originally derived (those cannot be refreshed via
   * exchange — re-derive explicitly), or the underlying exchange fails.
   */
  async refresh(
    grantId: string,
    opts: {
      workspaceScope?: string[];
      resourceScope?: { resourceType: string; patterns: string[] }[];
      operationScope?: string[];
      maxAccessLevel?: number;
      validWhileAudienceActive?: boolean;
      expiresAt?: number;
      renewableUntil?: number;
      mayDelegate?: boolean;
      remainingHops?: number;
      reason?: string;
      metadata?: Record<string, string>;
      timeout?: number;
    } = {},
  ): Promise<AuthorityGrantInfo | null> {
    if (!grantId) {
      return null;
    }
    const keys = this._grantIdIndex.get(grantId);
    if (!keys || keys.size === 0) {
      return null;
    }
    // Pick the first key — all keys for a given grantId share the same
    // grant payload.
    const [firstKey] = keys;
    const [sourceSessionId, audienceType, audienceId] = firstKey.split("|||");
    this.invalidate(grantId);
    if (sourceSessionId.startsWith("derive::")) {
      return null;
    }
    return this.getOrExchange({
      sourceSessionId,
      audienceType,
      audienceId,
      ...opts,
    });
  }

  /**
   * Deregister this cache from its parent client so it stops receiving
   * AuthorityGrantRevocation events. Idempotent.
   */
  close(): void {
    if (this._closed) {
      return;
    }
    this._closed = true;
    this._entries.clear();
    this._grantIdIndex.clear();
    this._rootGrantIdIndex.clear();
    this._client._removeAuthorityCache?.(this);
  }

  // ===========================================================================
  // Internals
  // ===========================================================================

  private _getUnexpired(key: CacheKey): AuthorityGrantInfo | null {
    const entry = this._entries.get(key);
    if (!entry) {
      return null;
    }
    if (entry.grant.revoked) {
      this._drop(key);
      return null;
    }
    const now = this._clock();
    if (entry.effectiveDeadlineMs > 0 && now >= entry.effectiveDeadlineMs) {
      this._drop(key);
      return null;
    }
    return entry.grant;
  }

  private _store(
    key: CacheKey,
    grant: AuthorityGrantInfo,
    response: AuthorityGrantResponse,
  ): void {
    // Drop any prior entry under this key first.
    const prior = this._entries.get(key);
    if (prior) {
      this._unindex(key, prior.grant);
    }

    const fetchedAt = this._clock();
    const cacheHintTtlS = response.cacheHintTtlSeconds ?? 0;
    const rawExpiresAt = grant.expiresAt ?? 0;
    const deadlines: number[] = [];
    if (rawExpiresAt > 0) {
      const expiresAtMs = this._expiresAtInMillis ? rawExpiresAt : rawExpiresAt * 1000;
      deadlines.push(expiresAtMs - this._softRenewSkewMs);
    }
    if (cacheHintTtlS > 0) {
      deadlines.push(fetchedAt + cacheHintTtlS * 1000);
    }
    const effectiveDeadlineMs = deadlines.length === 0 ? 0 : Math.min(...deadlines);

    this._entries.set(key, {
      grant,
      effectiveDeadlineMs,
      rawExpiresAt,
      cacheHintTtlS,
      fetchedAtMs: fetchedAt,
    });
    this._index(key, grant);
  }

  private _drop(key: CacheKey): boolean {
    const entry = this._entries.get(key);
    if (!entry) {
      return false;
    }
    this._entries.delete(key);
    this._unindex(key, entry.grant);
    return true;
  }

  private _index(key: CacheKey, grant: AuthorityGrantInfo): void {
    if (grant.grantId) {
      let set = this._grantIdIndex.get(grant.grantId);
      if (!set) {
        set = new Set<CacheKey>();
        this._grantIdIndex.set(grant.grantId, set);
      }
      set.add(key);
    }
    if (grant.rootGrantId && grant.rootGrantId !== grant.grantId) {
      let set = this._rootGrantIdIndex.get(grant.rootGrantId);
      if (!set) {
        set = new Set<CacheKey>();
        this._rootGrantIdIndex.set(grant.rootGrantId, set);
      }
      set.add(key);
    }
  }

  private _unindex(key: CacheKey, grant: AuthorityGrantInfo): void {
    if (grant.grantId) {
      const set = this._grantIdIndex.get(grant.grantId);
      if (set) {
        set.delete(key);
        if (set.size === 0) {
          this._grantIdIndex.delete(grant.grantId);
        }
      }
    }
    if (grant.rootGrantId) {
      const set = this._rootGrantIdIndex.get(grant.rootGrantId);
      if (set) {
        set.delete(key);
        if (set.size === 0) {
          this._rootGrantIdIndex.delete(grant.rootGrantId);
        }
      }
    }
  }

  private _withInflight(
    key: CacheKey,
    fetcher: () => Promise<AuthorityGrantInfo | null>,
  ): Promise<AuthorityGrantInfo | null> {
    const existing = this._inflight.get(key);
    if (existing) {
      return existing;
    }
    const promise = fetcher().finally(() => {
      this._inflight.delete(key);
    });
    this._inflight.set(key, promise);
    return promise;
  }
}

function makeKey(sourceSessionId: string, audienceType: string, audienceId: string): CacheKey {
  // Triple-pipe is a delimiter that is illegal in any valid principal id /
  // audience identifier today. Mirror the Python tuple-key encoding.
  return `${sourceSessionId}|||${audienceType}|||${audienceId}`;
}

function extractGrant(response: AuthorityGrantResponse | undefined | null): AuthorityGrantInfo | null {
  if (!response || !response.success) {
    return null;
  }
  const grant = response.grant;
  if (!grant || !grant.grantId) {
    return null;
  }
  return grant;
}
