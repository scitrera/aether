import { describe, it, expect, vi, beforeEach } from "vitest";
import { AuthorityGrantCache, type AuthorityCacheClient } from "../authority-cache.js";
import type {
  AuthorityGrantInfo,
  AuthorityGrantResponse,
  AuthorityGrantRevocation,
} from "../types.js";

// =============================================================================
// Test helpers
// =============================================================================

function makeGrant(
  overrides: Partial<AuthorityGrantInfo> & Pick<AuthorityGrantInfo, "grantId">,
): AuthorityGrantInfo {
  return {
    grantId: overrides.grantId,
    rootGrantId: overrides.rootGrantId ?? overrides.grantId,
    parentGrantId: "",
    mayDelegate: false,
    remainingHops: 0,
    workspaceScope: [],
    resourceScope: [],
    operationScope: [],
    maxAccessLevel: 0,
    accessLevelName: "",
    audienceType: "",
    audienceId: "",
    validWhileAudienceActive: false,
    expiresAt: 0,
    renewableUntil: 0,
    renewedAt: 0,
    revoked: false,
    revokedAt: 0,
    reason: "",
    metadata: {},
    createdAt: 0,
    ...overrides,
  };
}

interface FakeClientState {
  exchangeCalls: Record<string, unknown>[];
  deriveCalls: Record<string, unknown>[];
  revokeCalls: string[];
  exchangeQueue: AuthorityGrantResponse[];
  deriveQueue: AuthorityGrantResponse[];
  removeCalls: number;
}

function makeFakeClient(state: FakeClientState): AuthorityCacheClient {
  return {
    async exchangeAuthorityGrant(opts) {
      state.exchangeCalls.push(opts);
      const next = state.exchangeQueue.shift();
      return (
        next ?? {
          success: false,
          error: "no canned response",
          message: "",
          requestId: "",
        }
      );
    },
    async deriveAuthorityGrantForTarget(opts) {
      state.deriveCalls.push(opts);
      const next = state.deriveQueue.shift();
      return (
        next ?? {
          success: false,
          error: "no canned response",
          message: "",
          requestId: "",
        }
      );
    },
    async revokeAuthorityGrant(grantId, _timeout) {
      state.revokeCalls.push(grantId);
      return { success: true, error: "", message: "", requestId: "" };
    },
    _removeAuthorityCache() {
      state.removeCalls++;
    },
  };
}

function newState(): FakeClientState {
  return {
    exchangeCalls: [],
    deriveCalls: [],
    revokeCalls: [],
    exchangeQueue: [],
    deriveQueue: [],
    removeCalls: 0,
  };
}

// =============================================================================
// Tests
// =============================================================================

describe("AuthorityGrantCache", () => {
  let now = 1_700_000_000_000;
  const clock = () => now;

  beforeEach(() => {
    now = 1_700_000_000_000;
  });

  it("serves cache hit without re-exchanging", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-1",
        // expiresAt in seconds (default), 1 hour from "now".
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });

    const cache = new AuthorityGrantCache(client, { clock });
    const g1 = await cache.getOrExchange({
      sourceSessionId: "sess-1",
      audienceType: "service",
      audienceId: "memorylayer",
    });
    expect(g1?.grantId).toBe("g-1");

    const g2 = await cache.getOrExchange({
      sourceSessionId: "sess-1",
      audienceType: "service",
      audienceId: "memorylayer",
    });
    expect(g2).toBe(g1);
    expect(state.exchangeCalls.length).toBe(1);
  });

  it("soft-renews before server-side expiry", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    const expiresAtFirst = Math.floor(now / 1000) + 60; // 60 s in future
    const expiresAtSecond = Math.floor(now / 1000) + 3600;
    state.exchangeQueue.push(
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({ grantId: "g-old", expiresAt: expiresAtFirst }),
      },
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({ grantId: "g-new", expiresAt: expiresAtSecond }),
      },
    );

    const cache = new AuthorityGrantCache(client, {
      clock,
      // Soft-renew 45 s before expiry; advancing 31 s puts us inside that window.
      softRenewSkewMs: 45_000,
    });

    const g1 = await cache.getOrExchange({ sourceSessionId: "sess" });
    expect(g1?.grantId).toBe("g-old");

    now += 31_000;

    const g2 = await cache.getOrExchange({ sourceSessionId: "sess" });
    expect(g2?.grantId).toBe("g-new");
    expect(state.exchangeCalls.length).toBe(2);
  });

  it("invalidates cache entries by grantId on revocation event", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push(
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({
          grantId: "g-1",
          rootGrantId: "g-root",
          expiresAt: Math.floor(now / 1000) + 3600,
        }),
      },
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({
          grantId: "g-2",
          rootGrantId: "g-root",
          expiresAt: Math.floor(now / 1000) + 3600,
        }),
      },
    );

    const cache = new AuthorityGrantCache(client, { clock });
    await cache.getOrExchange({ sourceSessionId: "sess" });

    const dropped = cache.handleRevocationEvent({
      grantId: "g-1",
      rootGrantId: "",
      reason: "",
      revokedAt: 0,
      cascade: false,
    } satisfies AuthorityGrantRevocation);
    expect(dropped).toBe(1);

    // Re-fetch should issue a new exchange.
    const g = await cache.getOrExchange({ sourceSessionId: "sess" });
    expect(g?.grantId).toBe("g-2");
    expect(state.exchangeCalls.length).toBe(2);
  });

  it("cascade-invalidates by rootGrantId", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-1",
        rootGrantId: "g-root",
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });
    state.deriveQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-2",
        rootGrantId: "g-root",
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });

    const cache = new AuthorityGrantCache(client, { clock });
    await cache.getOrExchange({ sourceSessionId: "sess" });
    await cache.deriveForTask({ parentGrantId: "g-1", taskId: "tsk-1" });

    expect(cache.stats().size).toBe(2);

    const dropped = cache.handleRevocationEvent({
      grantId: "g-root",
      rootGrantId: "g-root",
      reason: "",
      revokedAt: 0,
      cascade: true,
    });
    expect(dropped).toBe(2);
    expect(cache.stats().size).toBe(0);
  });

  it("deriveForTask is idempotent (cache serves repeat calls)", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.deriveQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-derived",
        rootGrantId: "g-root",
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });

    const cache = new AuthorityGrantCache(client, { clock });
    const g1 = await cache.deriveForTask({
      parentGrantId: "g-parent",
      taskId: "tsk-1",
      audienceType: "service",
      audienceId: "memorylayer",
    });
    const g2 = await cache.deriveForTask({
      parentGrantId: "g-parent",
      taskId: "tsk-1",
      audienceType: "service",
      audienceId: "memorylayer",
    });
    expect(g1).toBe(g2);
    expect(state.deriveCalls.length).toBe(1);
  });

  it("revokeAll calls revoke for each cached grant and clears state", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-a",
        rootGrantId: "g-a",
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });
    state.deriveQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-b",
        rootGrantId: "g-a",
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });

    const cache = new AuthorityGrantCache(client, { clock });
    await cache.getOrExchange({ sourceSessionId: "sess-a", audienceType: "svc", audienceId: "x" });
    await cache.deriveForTask({ parentGrantId: "g-a", taskId: "tsk-1" });

    await cache.revokeAll();
    expect(state.revokeCalls.sort()).toEqual(["g-a", "g-b"]);
    expect(cache.stats().size).toBe(0);
  });

  it("honors server-supplied cacheHintTtlSeconds as upper bound", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push(
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        // expiresAt is far in the future, but cacheHintTtlSeconds is short.
        cacheHintTtlSeconds: 5,
        grant: makeGrant({
          grantId: "g-hint",
          expiresAt: Math.floor(now / 1000) + 3600,
        }),
      },
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({
          grantId: "g-hint-2",
          expiresAt: Math.floor(now / 1000) + 3600,
        }),
      },
    );

    const cache = new AuthorityGrantCache(client, { clock, softRenewSkewMs: 0 });
    const g1 = await cache.getOrExchange({ sourceSessionId: "sess" });
    expect(g1?.grantId).toBe("g-hint");

    // Advance past the cache hint TTL.
    now += 6_000;

    const g2 = await cache.getOrExchange({ sourceSessionId: "sess" });
    expect(g2?.grantId).toBe("g-hint-2");
    expect(state.exchangeCalls.length).toBe(2);
  });

  it("close deregisters the cache from the parent client", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    const cache = new AuthorityGrantCache(client, { clock });
    cache.close();
    expect(state.removeCalls).toBe(1);
    cache.close(); // double-close is a no-op
    expect(state.removeCalls).toBe(1);
  });

  it("returns null when the gateway responds with success=false", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push({
      success: false,
      error: "denied",
      message: "",
      requestId: "",
    });
    const cache = new AuthorityGrantCache(client, { clock });
    const g = await cache.getOrExchange({ sourceSessionId: "sess" });
    expect(g).toBeNull();
  });

  // ===========================================================================
  // High-level helpers (Phase 4)
  // ===========================================================================

  it("isValid returns true only for cached, fresh grants", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-1",
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });

    const cache = new AuthorityGrantCache(client, { clock });
    expect(cache.isValid("g-1")).toBe(false);
    await cache.getOrExchange({ sourceSessionId: "sess", audienceType: "task", audienceId: "t1" });
    expect(cache.isValid("g-1")).toBe(true);
    expect(cache.isValid("")).toBe(false);
    expect(cache.isValid("g-unknown")).toBe(false);
  });

  it("listActive returns a de-duplicated snapshot of fresh grants", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push(
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({ grantId: "g-1", expiresAt: Math.floor(now / 1000) + 3600 }),
      },
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        // cacheHintTtlSeconds: 5 -> expires at now+5_000ms.
        cacheHintTtlSeconds: 5,
        grant: makeGrant({ grantId: "g-2", expiresAt: Math.floor(now / 1000) + 3600 }),
      },
    );

    const cache = new AuthorityGrantCache(client, { clock, softRenewSkewMs: 0 });
    await cache.getOrExchange({ sourceSessionId: "s1", audienceType: "task", audienceId: "t1" });
    await cache.getOrExchange({ sourceSessionId: "s2", audienceType: "task", audienceId: "t2" });
    const active = cache.listActive();
    expect(active.map((g) => g.grantId).sort()).toEqual(["g-1", "g-2"]);

    // Advance past g-2's cache-hint TTL — listActive must evict it.
    now += 6_000;
    const active2 = cache.listActive();
    expect(active2.map((g) => g.grantId)).toEqual(["g-1"]);
  });

  it("revokeLocal drops cache entry without invoking server-side revoke", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({ grantId: "g-1", expiresAt: Math.floor(now / 1000) + 3600 }),
    });

    const cache = new AuthorityGrantCache(client, { clock });
    await cache.getOrExchange({ sourceSessionId: "sess", audienceType: "task", audienceId: "t1" });
    const dropped = cache.revokeLocal("g-1");
    expect(dropped).toBe(1);
    expect(cache.stats().size).toBe(0);
    expect(state.revokeCalls).toEqual([]);
  });

  it("refresh force-drops the cached entry and re-exchanges", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.exchangeQueue.push(
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({ grantId: "g-old", expiresAt: Math.floor(now / 1000) + 3600 }),
      },
      {
        success: true,
        error: "",
        message: "",
        requestId: "",
        grant: makeGrant({ grantId: "g-new", expiresAt: Math.floor(now / 1000) + 3600 }),
      },
    );

    const cache = new AuthorityGrantCache(client, { clock });
    const first = await cache.getOrExchange({ sourceSessionId: "sess", audienceType: "task", audienceId: "t1" });
    expect(first?.grantId).toBe("g-old");

    const refreshed = await cache.refresh("g-old");
    expect(refreshed?.grantId).toBe("g-new");
    expect(cache.isValid("g-old")).toBe(false);
    expect(cache.isValid("g-new")).toBe(true);
    expect(state.exchangeCalls.length).toBe(2);
  });

  it("refresh returns null for unknown grant ids and does not call exchange", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    const cache = new AuthorityGrantCache(client, { clock });
    expect(await cache.refresh("")).toBeNull();
    expect(await cache.refresh("never-cached")).toBeNull();
    expect(state.exchangeCalls.length).toBe(0);
  });

  it("refresh on a derived entry drops it and returns null", async () => {
    const state = newState();
    const client = makeFakeClient(state);
    state.deriveQueue.push({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({
        grantId: "g-derived",
        rootGrantId: "g-root",
        expiresAt: Math.floor(now / 1000) + 3600,
      }),
    });

    const cache = new AuthorityGrantCache(client, { clock });
    await cache.deriveForTask({ parentGrantId: "g-parent", taskId: "tsk-1" });
    const out = await cache.refresh("g-derived");
    expect(out).toBeNull();
    expect(cache.isValid("g-derived")).toBe(false);
    expect(state.exchangeCalls.length).toBe(0);
  });

  it("collapses concurrent getOrExchange into a single network call", async () => {
    const state = newState();
    let resolveExchange: ((value: AuthorityGrantResponse) => void) | undefined;
    const client: AuthorityCacheClient = {
      async exchangeAuthorityGrant(opts) {
        state.exchangeCalls.push(opts);
        return new Promise((resolve) => {
          resolveExchange = resolve;
        });
      },
      async deriveAuthorityGrantForTarget() {
        return { success: false, error: "", message: "", requestId: "" };
      },
      async revokeAuthorityGrant() {
        return { success: true, error: "", message: "", requestId: "" };
      },
    };
    const cache = new AuthorityGrantCache(client, { clock });
    const p1 = cache.getOrExchange({ sourceSessionId: "sess" });
    const p2 = cache.getOrExchange({ sourceSessionId: "sess" });
    // Both promises share the same in-flight fetch.
    expect(state.exchangeCalls.length).toBe(1);
    resolveExchange?.({
      success: true,
      error: "",
      message: "",
      requestId: "",
      grant: makeGrant({ grantId: "g-only", expiresAt: Math.floor(now / 1000) + 3600 }),
    });
    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1?.grantId).toBe("g-only");
    expect(r2?.grantId).toBe("g-only");
    expect(state.exchangeCalls.length).toBe(1);
  });
});
