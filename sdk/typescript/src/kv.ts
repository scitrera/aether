/**
 * KV (key-value) store operations for the Aether TypeScript SDK.
 *
 * This module provides the KVClient class for interacting with the
 * hierarchical KV store through the Aether gateway connection.
 *
 * KV operations support four scopes:
 * - Global: Accessible to all entities across all workspaces
 * - Workspace: Accessible within a specific workspace
 * - User: Accessible to a specific user across all workspaces
 * - UserWorkspace: Accessible to a specific user within a specific workspace
 *
 * @module kv
 */

import type { AetherClient } from "./client.js";
import type { KVGetOptions, KVPutOptions, KVDeleteOptions, KVListOptions, KVIncrementOptions, KVDecrementOptions, KVIncrementIfOptions, KVDecrementIfOptions, KVResponse } from "./types.js";
import { KVScope } from "./types.js";
import { TimeoutError } from "./errors.js";

/** Default timeout for synchronous KV operations (5 seconds). */
const DEFAULT_KV_TIMEOUT = 5000;

/**
 * KVClient provides KV store operations over an Aether connection.
 *
 * Access this through `client.kv()` rather than constructing directly.
 *
 * Supports both async (fire-and-forget with callback) and sync (Promise-based)
 * operation modes.
 *
 * @example
 * ```typescript
 * const kv = client.kv();
 *
 * // Async put (fire-and-forget)
 * kv.put({ key: "my-key", value: encode("my-value"), scope: KVScope.Global });
 *
 * // Sync get (returns a Promise)
 * const response = await kv.getSync({ key: "my-key", scope: KVScope.Global });
 * if (response.success) {
 *   console.log("Value:", response.value);
 * }
 * ```
 */
export class KVClient {
  private _client: AetherClient;

  /** @internal */
  constructor(client: AetherClient) {
    this._client = client;
  }

  // ===========================================================================
  // Async Operations (fire-and-forget, responses via onKVResponse callback)
  // ===========================================================================

  /**
   * Retrieves a value from the KV store (async).
   *
   * The response is delivered via the onKVResponse handler callback.
   * For synchronous operation, use {@link getSync}.
   *
   * @param opts - Get operation options
   */
  get(opts: KVGetOptions): void {
    this._client.sendKVOperation({
      op: "GET",
      scope: opts.scope ?? KVScope.Global,
      key: opts.key,
      userId: opts.userId,
      workspace: opts.workspace,
    });
  }

  /**
   * Stores a value in the KV store (async).
   *
   * The response is delivered via the onKVResponse handler callback.
   * For synchronous operation, use {@link putSync}.
   *
   * @param opts - Put operation options
   */
  put(opts: KVPutOptions): void {
    this._client.sendKVOperation({
      op: "PUT",
      scope: opts.scope ?? KVScope.Global,
      key: opts.key,
      value: opts.value,
      userId: opts.userId,
      workspace: opts.workspace,
      ttl: opts.ttl,
    });
  }

  /**
   * Removes a key from the KV store (async).
   *
   * The response is delivered via the onKVResponse handler callback.
   * For synchronous operation, use {@link deleteSync}.
   *
   * @param opts - Delete operation options
   */
  delete(opts: KVDeleteOptions): void {
    this._client.sendKVOperation({
      op: "DELETE",
      scope: opts.scope ?? KVScope.Global,
      key: opts.key,
      userId: opts.userId,
      workspace: opts.workspace,
    });
  }

  /**
   * Lists keys from the KV store (async).
   *
   * The response is delivered via the onKVResponse handler callback.
   * For synchronous operation, use {@link listSync}.
   *
   * @param opts - List operation options
   */
  list(opts?: KVListOptions): void {
    this._client.sendKVOperation({
      op: "LIST",
      scope: opts?.scope ?? KVScope.Global,
      key: opts?.keyPrefix,
      userId: opts?.userId,
      workspace: opts?.workspace,
    });
  }

  // ===========================================================================
  // Synchronous Operations (Promise-based with timeout)
  // ===========================================================================

  /**
   * Retrieves a value from the KV store and waits for the response.
   *
   * @param opts - Get operation options (includes optional timeout)
   * @returns Promise resolving to the KV response
   * @throws {@link TimeoutError} if the operation times out
   */
  async getSync(opts: KVGetOptions): Promise<KVResponse> {
    const timeout = opts.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "GET",
        scope: opts.scope ?? KVScope.Global,
        key: opts.key,
        userId: opts.userId,
        workspace: opts.workspace,
        requestId,
      });
    });
  }

  /**
   * Stores a value in the KV store and waits for the response.
   *
   * @param opts - Put operation options (includes optional timeout)
   * @returns Promise resolving to the KV response
   * @throws {@link TimeoutError} if the operation times out
   */
  async putSync(opts: KVPutOptions): Promise<KVResponse> {
    const timeout = opts.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "PUT",
        scope: opts.scope ?? KVScope.Global,
        key: opts.key,
        value: opts.value,
        userId: opts.userId,
        workspace: opts.workspace,
        ttl: opts.ttl,
        requestId,
      });
    });
  }

  /**
   * Removes a key from the KV store and waits for the response.
   *
   * @param opts - Delete operation options (includes optional timeout)
   * @returns Promise resolving to the KV response
   * @throws {@link TimeoutError} if the operation times out
   */
  async deleteSync(opts: KVDeleteOptions): Promise<KVResponse> {
    const timeout = opts.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "DELETE",
        scope: opts.scope ?? KVScope.Global,
        key: opts.key,
        userId: opts.userId,
        workspace: opts.workspace,
        requestId,
      });
    });
  }

  /**
   * Lists keys from the KV store and waits for the response.
   *
   * @param opts - List operation options (includes optional timeout)
   * @returns Promise resolving to the KV response
   * @throws {@link TimeoutError} if the operation times out
   */
  async listSync(opts?: KVListOptions): Promise<KVResponse> {
    const timeout = opts?.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "LIST",
        scope: opts?.scope ?? KVScope.Global,
        key: opts?.keyPrefix,
        userId: opts?.userId,
        workspace: opts?.workspace,
        requestId,
      });
    });
  }

  /**
   * Increments a counter in the KV store (async).
   *
   * The response is delivered via the onKVResponse handler callback.
   * For synchronous operation, use {@link incrementSync}.
   *
   * @param opts - Increment operation options
   */
  increment(opts: KVIncrementOptions): void {
    this._client.sendKVOperation({
      op: "INCREMENT",
      scope: opts.scope ?? KVScope.Global,
      key: opts.key,
      userId: opts.userId,
      workspace: opts.workspace,
    });
  }

  /**
   * Decrements a counter in the KV store (async).
   *
   * The response is delivered via the onKVResponse handler callback.
   * For synchronous operation, use {@link decrementSync}.
   *
   * @param opts - Decrement operation options
   */
  decrement(opts: KVDecrementOptions): void {
    this._client.sendKVOperation({
      op: "DECREMENT",
      scope: opts.scope ?? KVScope.Global,
      key: opts.key,
      userId: opts.userId,
      workspace: opts.workspace,
    });
  }

  /**
   * Increments a counter in the KV store and waits for the response.
   *
   * @param opts - Increment operation options (includes optional timeout)
   * @returns Promise resolving to the KV response with counterValue set
   * @throws {@link TimeoutError} if the operation times out
   */
  async incrementSync(opts: KVIncrementOptions): Promise<KVResponse> {
    const timeout = opts.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "INCREMENT",
        scope: opts.scope ?? KVScope.Global,
        key: opts.key,
        userId: opts.userId,
        workspace: opts.workspace,
        requestId,
      });
    });
  }

  /**
   * Decrements a counter in the KV store and waits for the response.
   *
   * @param opts - Decrement operation options (includes optional timeout)
   * @returns Promise resolving to the KV response with counterValue set
   * @throws {@link TimeoutError} if the operation times out
   */
  async decrementSync(opts: KVDecrementOptions): Promise<KVResponse> {
    const timeout = opts.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "DECREMENT",
        scope: opts.scope ?? KVScope.Global,
        key: opts.key,
        userId: opts.userId,
        workspace: opts.workspace,
        requestId,
      });
    });
  }

  /**
   * Increments a counter only if it is strictly below `ceiling` (async).
   *
   * The response (with `applied` and `counterValue`) is delivered via the onKVResponse callback.
   * For synchronous operation, use {@link incrementIfSync}.
   *
   * @param opts - IncrementIf operation options
   */
  incrementIf(opts: KVIncrementIfOptions): void {
    this._client.sendKVOperation({
      op: "INCREMENT_IF",
      scope: opts.scope ?? KVScope.Global,
      key: opts.key,
      userId: opts.userId,
      workspace: opts.workspace,
      guardValue: BigInt(opts.ceiling),
      deltaValue: opts.delta != null ? BigInt(opts.delta) : undefined,
    });
  }

  /**
   * Decrements a counter only if it is strictly above `floor` (async).
   *
   * The response (with `applied` and `counterValue`) is delivered via the onKVResponse callback.
   * For synchronous operation, use {@link decrementIfSync}.
   *
   * @param opts - DecrementIf operation options
   */
  decrementIf(opts: KVDecrementIfOptions): void {
    this._client.sendKVOperation({
      op: "DECREMENT_IF",
      scope: opts.scope ?? KVScope.Global,
      key: opts.key,
      userId: opts.userId,
      workspace: opts.workspace,
      guardValue: BigInt(opts.floor),
      deltaValue: opts.delta != null ? BigInt(opts.delta) : undefined,
    });
  }

  /**
   * Increments a counter only if it is strictly below `ceiling`, and waits for the response.
   *
   * @param opts - IncrementIf operation options (includes optional timeout)
   * @returns Promise resolving to the KV response with `counterValue` and `applied` set
   * @throws {@link TimeoutError} if the operation times out
   */
  async incrementIfSync(opts: KVIncrementIfOptions): Promise<KVResponse> {
    const timeout = opts.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "INCREMENT_IF",
        scope: opts.scope ?? KVScope.Global,
        key: opts.key,
        userId: opts.userId,
        workspace: opts.workspace,
        guardValue: BigInt(opts.ceiling),
        deltaValue: opts.delta != null ? BigInt(opts.delta) : undefined,
        requestId,
      });
    });
  }

  /**
   * Decrements a counter only if it is strictly above `floor`, and waits for the response.
   *
   * @param opts - DecrementIf operation options (includes optional timeout)
   * @returns Promise resolving to the KV response with `counterValue` and `applied` set
   * @throws {@link TimeoutError} if the operation times out
   */
  async decrementIfSync(opts: KVDecrementIfOptions): Promise<KVResponse> {
    const timeout = opts.timeout ?? DEFAULT_KV_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendKVOperation({
        op: "DECREMENT_IF",
        scope: opts.scope ?? KVScope.Global,
        key: opts.key,
        userId: opts.userId,
        workspace: opts.workspace,
        guardValue: BigInt(opts.floor),
        deltaValue: opts.delta != null ? BigInt(opts.delta) : undefined,
        requestId,
      });
    });
  }

  // ===========================================================================
  // Convenience Methods
  // ===========================================================================

  /**
   * Retrieves a value from the global scope (async).
   *
   * @param key - The key to retrieve
   */
  getGlobal(key: string): void {
    this.get({ key, scope: KVScope.Global });
  }

  /**
   * Stores a value in the global scope (async).
   *
   * @param key - The key to store
   * @param value - The value to store
   */
  putGlobal(key: string, value: Uint8Array): void {
    this.put({ key, value, scope: KVScope.Global });
  }

  /**
   * Removes a key from the global scope (async).
   *
   * @param key - The key to delete
   */
  deleteGlobal(key: string): void {
    this.delete({ key, scope: KVScope.Global });
  }

  /**
   * Lists keys from the global scope (async).
   *
   * @param keyPrefix - Prefix to filter keys (empty for all)
   */
  listGlobal(keyPrefix = ""): void {
    this.list({ keyPrefix, scope: KVScope.Global });
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Waits for a correlated KV response with timeout.
   * @internal
   */
  private _waitForResponse(
    requestId: string,
    timeout: number,
    sendFn: () => void,
  ): Promise<KVResponse> {
    return new Promise<KVResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._client.removePendingRequest(requestId);
        reject(new TimeoutError("KV operation timed out", timeout / 1000));
      }, timeout);

      this._client.registerPendingKVRequest(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      sendFn();
    });
  }
}
