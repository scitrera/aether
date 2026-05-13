/**
 * Checkpoint operations for the Aether TypeScript SDK.
 *
 * Checkpoints allow agents/tasks to persist arbitrary state that survives
 * restarts. This is separate from message offset tracking (handled
 * automatically by RabbitMQ Streams).
 *
 * @module checkpoint
 */

import type { AetherClient } from "./client.js";
import type { CheckpointResponse } from "./types.js";
import { TimeoutError } from "./errors.js";

/** Default timeout for synchronous checkpoint operations (5 seconds). */
const DEFAULT_CHECKPOINT_TIMEOUT = 5000;

/**
 * Options for a checkpoint save operation.
 */
export interface CheckpointSaveOptions {
  /** The checkpoint data to save. */
  data: Uint8Array;
  /** Checkpoint key. Allows multiple named checkpoints per identity. Default: "default". */
  key?: string;
  /** TTL in seconds (-1 = server default, 0 = no expiration). */
  ttl?: number;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Options for a checkpoint load operation.
 */
export interface CheckpointLoadOptions {
  /** Checkpoint key to load. Default: "default". */
  key?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Options for a checkpoint delete operation.
 */
export interface CheckpointDeleteOptions {
  /** Checkpoint key to delete. Default: "default". */
  key?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Options for a checkpoint list operation.
 */
export interface CheckpointListOptions {
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * CheckpointClient provides checkpoint operations over an Aether connection.
 *
 * Access this through `client.checkpoint()` rather than constructing directly.
 *
 * Supports both async (fire-and-forget with callback) and sync (Promise-based)
 * operation modes.
 *
 * @example
 * ```typescript
 * const cp = client.checkpoint();
 *
 * // Save state
 * const encoder = new TextEncoder();
 * await cp.saveSync({ data: encoder.encode(JSON.stringify(myState)) });
 *
 * // Load state
 * const response = await cp.loadSync({});
 * if (response.success && response.data.length > 0) {
 *   const state = JSON.parse(new TextDecoder().decode(response.data));
 * }
 *
 * // List checkpoints
 * const listResponse = await cp.listSync({});
 * console.log("Checkpoint keys:", listResponse.keys);
 * ```
 */
export class CheckpointClient {
  private _client: AetherClient;

  /** @internal */
  constructor(client: AetherClient) {
    this._client = client;
  }

  // ===========================================================================
  // Async Operations (fire-and-forget, responses via onCheckpointResponse)
  // ===========================================================================

  /**
   * Saves checkpoint data (async).
   * The response is delivered via the onCheckpointResponse handler.
   */
  save(opts: CheckpointSaveOptions): void {
    this._client.sendCheckpointOperation({
      op: "SAVE",
      key: opts.key ?? "",
      data: opts.data,
      ttl: opts.ttl ?? -1,
    });
  }

  /**
   * Loads checkpoint data (async).
   * The response is delivered via the onCheckpointResponse handler.
   */
  load(opts?: CheckpointLoadOptions): void {
    this._client.sendCheckpointOperation({
      op: "LOAD",
      key: opts?.key ?? "",
    });
  }

  /**
   * Deletes a checkpoint (async).
   * The response is delivered via the onCheckpointResponse handler.
   */
  delete(opts?: CheckpointDeleteOptions): void {
    this._client.sendCheckpointOperation({
      op: "DELETE",
      key: opts?.key ?? "",
    });
  }

  /**
   * Lists checkpoint keys (async).
   * The response is delivered via the onCheckpointResponse handler.
   */
  list(): void {
    this._client.sendCheckpointOperation({
      op: "LIST",
    });
  }

  // ===========================================================================
  // Synchronous Operations (Promise-based with timeout)
  // ===========================================================================

  /**
   * Saves checkpoint data and waits for the response.
   *
   * @throws {@link TimeoutError} if the operation times out
   */
  async saveSync(opts: CheckpointSaveOptions): Promise<CheckpointResponse> {
    const timeout = opts.timeout ?? DEFAULT_CHECKPOINT_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendCheckpointOperation({
        op: "SAVE",
        key: opts.key ?? "",
        data: opts.data,
        ttl: opts.ttl ?? -1,
        requestId,
      });
    });
  }

  /**
   * Loads checkpoint data and waits for the response.
   *
   * @throws {@link TimeoutError} if the operation times out
   */
  async loadSync(opts?: CheckpointLoadOptions): Promise<CheckpointResponse> {
    const timeout = opts?.timeout ?? DEFAULT_CHECKPOINT_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendCheckpointOperation({
        op: "LOAD",
        key: opts?.key ?? "",
        requestId,
      });
    });
  }

  /**
   * Deletes a checkpoint and waits for the response.
   *
   * @throws {@link TimeoutError} if the operation times out
   */
  async deleteSync(opts?: CheckpointDeleteOptions): Promise<CheckpointResponse> {
    const timeout = opts?.timeout ?? DEFAULT_CHECKPOINT_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendCheckpointOperation({
        op: "DELETE",
        key: opts?.key ?? "",
        requestId,
      });
    });
  }

  /**
   * Lists checkpoint keys and waits for the response.
   *
   * @throws {@link TimeoutError} if the operation times out
   */
  async listSync(opts?: CheckpointListOptions): Promise<CheckpointResponse> {
    const timeout = opts?.timeout ?? DEFAULT_CHECKPOINT_TIMEOUT;
    const requestId = this._client.nextRequestId();

    return this._waitForResponse(requestId, timeout, () => {
      this._client.sendCheckpointOperation({
        op: "LIST",
        requestId,
      });
    });
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private _waitForResponse(
    requestId: string,
    timeout: number,
    sendFn: () => void,
  ): Promise<CheckpointResponse> {
    return new Promise<CheckpointResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._client.removePendingRequest(requestId);
        reject(new TimeoutError("Checkpoint operation timed out", timeout / 1000));
      }, timeout);

      this._client.registerPendingCheckpointRequest(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      sendFn();
    });
  }
}
