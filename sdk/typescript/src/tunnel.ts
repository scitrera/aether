/**
 * Tunnel (tunnelDial) support for the Aether TypeScript SDK.
 *
 * Provides `tunnelDial()` on AetherClient for opening a bidirectional byte-stream
 * tunnel through the Aether gRPC connection to a remote service.
 *
 * Uses the Web Streams API (ReadableStream / WritableStream) so callers work
 * in both Node ≥18 and browser environments.
 *
 * @module tunnel
 */

import type { AetherClient } from "./client.js";
import { ConnectionError } from "./errors.js";

// =============================================================================
// Constants
// =============================================================================

const TUNNEL_CHUNK_SIZE = 256 * 1024; // 256 KiB per TunnelData frame
const INBOUND_ACK_THRESHOLD = 256 * 1024; // send TunnelAck after consuming this many bytes
const DEFAULT_INITIAL_CREDITS = 16;

// =============================================================================
// Types
// =============================================================================

/** Wire protocol for the tunnel. */
export type TunnelProtocol = "tcp" | "udp" | "ws" | "websocket";

/** Options for tunnelDial(). */
export interface TunnelDialOptions {
  /** Hint passed to the remote sidecar (e.g. "host:port"). */
  remoteHint?: string;
  /** Idle timeout in ms (server-side enforcement). */
  idleTimeoutMs?: number;
  /** Byte quota (server-side enforcement). */
  maxBytes?: number;
  /** Arbitrary metadata forwarded in TunnelOpen (e.g. WS sub-protocols). */
  metadata?: Record<string, string>;
  /** Reserved for v2 reconnect/resume; ignored in v1. */
  sessionToken?: string;
  /** Initial outbound credit window (default: 16). */
  initialCredits?: number;
  /**
   * Pin the tunnel to a named terminator backend. The backend's allow-list
   * still applies — explicit naming selects which backend's ACL is consulted,
   * not whether the tunnel is allowed.
   */
  backend?: string;
}

/** Reason string from a remote TunnelClose frame. */
export type TunnelCloseReason = "NORMAL" | "PEER_RESET" | "IDLE_TIMEOUT" | "QUOTA" | "ERROR" | string;

/** Error thrown when the remote side sends a TunnelClose frame. */
export class TunnelClosedError extends Error {
  readonly reason: TunnelCloseReason;
  readonly detail: string;
  constructor(reason: TunnelCloseReason, detail: string) {
    super(`Aether tunnel closed: ${reason}: ${detail}`);
    this.name = "TunnelClosedError";
    this.reason = reason;
    this.detail = detail;
  }
}

/**
 * Duplex tunnel object returned by tunnelDial().
 *
 * - `readable`: incoming bytes from the remote end.
 * - `writable`: outgoing bytes to the remote end.
 * - `close()`: send a NORMAL TunnelClose and clean up.
 */
export interface TunnelStream {
  /** Readable stream of inbound Uint8Array chunks from the remote end. */
  readonly readable: ReadableStream<Uint8Array>;
  /** Writable stream; write Uint8Array chunks to send to the remote end. */
  readonly writable: WritableStream<Uint8Array>;
  /** Sends TunnelClose{NORMAL} and tears down both streams. */
  close(): void;
  /** Resolves when the tunnel is fully closed (either side). */
  readonly closed: Promise<void>;
}

// =============================================================================
// Internal per-tunnel state (registered in _pendingTunnels on the client)
// =============================================================================

/** @internal */
export interface TunnelInflight {
  /** Push inbound data (from downstream TunnelData) into the readable side. */
  pushData(data: Uint8Array, fin: boolean): void;
  /** Replenish outbound credits (from downstream TunnelAck). */
  addCredits(n: number): void;
  /** Signal that the remote closed the tunnel. */
  closeWithError(err: TunnelClosedError): void;
}

// =============================================================================
// tunnelDial — main entry point
// =============================================================================

/**
 * Opens a byte-stream tunnel through the Aether connection.
 *
 * @param client   - Connected AetherClient instance.
 * @param target   - Target topic (e.g. "sv::proxy::default").
 * @param protocol - Wire protocol: "tcp", "udp", "ws", or "websocket".
 * @param options  - Optional dial parameters.
 * @returns A TunnelStream with readable/writable Web Streams and a close() method.
 * @throws {@link ConnectionError} if not connected.
 */
export function tunnelDial(
  client: AetherClient,
  target: string,
  protocol: TunnelProtocol,
  options: TunnelDialOptions = {},
): TunnelStream {
  if (!(client as unknown as { _connected: boolean })._connected) {
    throw new ConnectionError("Not connected to gateway");
  }
  if (!target) {
    throw new ConnectionError("tunnelDial: target topic is required");
  }

  const tunnelId = (client as unknown as { nextRequestId(): string }).nextRequestId();
  const initialCredits = options.initialCredits ?? DEFAULT_INITIAL_CREDITS;

  // ---- outbound credit tracking ----
  let outCredits = initialCredits;
  // Waiters blocked on credits: each entry is a resolve fn for one credit slot.
  const creditWaiters: Array<() => void> = [];

  function consumeCredit(): Promise<void> {
    if (outCredits > 0) {
      outCredits--;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => { creditWaiters.push(resolve); });
  }

  function addCredits(n: number): void {
    outCredits += n;
    // Wake blocked writers one credit at a time.
    while (outCredits > 0 && creditWaiters.length > 0) {
      outCredits--;
      const waiter = creditWaiters.shift()!;
      waiter();
    }
  }

  // ---- inbound flow control ----
  let inboundBytesConsumed = 0;

  // ---- closed state ----
  let closed = false;
  let closedError: TunnelClosedError | undefined;
  let closedResolve!: () => void;
  const closedPromise = new Promise<void>((resolve) => { closedResolve = resolve; });

  // ---- ReadableStream for inbound bytes ----
  let readableController!: ReadableStreamDefaultController<Uint8Array>;
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      readableController = controller;
    },
    cancel() {
      doClose("NORMAL");
    },
  });

  // ---- WritableStream for outbound bytes ----
  const writable = new WritableStream<Uint8Array>({
    async write(chunk) {
      if (closed) {
        throw closedError ?? new TunnelClosedError("NORMAL", "tunnel closed");
      }
      let offset = 0;
      while (offset < chunk.length) {
        await consumeCredit();
        if (closed) {
          throw closedError ?? new TunnelClosedError("NORMAL", "tunnel closed");
        }
        const end = Math.min(offset + TUNNEL_CHUNK_SIZE, chunk.length);
        const slice = chunk.slice(offset, end);
        offset = end;
        (client as unknown as { _sendUpstream(msg: Record<string, unknown>): void })._sendUpstream({
          tunnelData: {
            tunnelId,
            data: slice,
            fin: false,
          },
        });
      }
    },
    close() {
      // WritableStream closed by caller — send FIN frame then tear down.
      (client as unknown as { _sendUpstream(msg: Record<string, unknown>): void })._sendUpstream({
        tunnelData: {
          tunnelId,
          data: new Uint8Array(0),
          fin: true,
        },
      });
      doClose("NORMAL");
    },
    abort(_reason) {
      doClose("NORMAL");
    },
  });

  // ---- inflight registration ----
  const inflight: TunnelInflight = {
    pushData(data: Uint8Array, fin: boolean) {
      if (closed) return;
      if (data.length > 0) {
        readableController.enqueue(data);
        // Track consumed bytes and send TunnelAck when threshold crossed.
        inboundBytesConsumed += data.length;
        if (inboundBytesConsumed >= INBOUND_ACK_THRESHOLD) {
          const credits = Math.floor(inboundBytesConsumed / INBOUND_ACK_THRESHOLD) * 16;
          inboundBytesConsumed = inboundBytesConsumed % INBOUND_ACK_THRESHOLD;
          (client as unknown as { _sendUpstream(msg: Record<string, unknown>): void })._sendUpstream({
            tunnelAck: {
              tunnelId,
              credits,
            },
          });
        }
      }
      if (fin) {
        readableController.close();
        doClose("NORMAL");
      }
    },
    addCredits,
    closeWithError(err: TunnelClosedError) {
      if (closed) return;
      closedError = err;
      try { readableController.error(err); } catch { /* already closed */ }
      // Wake all pending writers with an error.
      closed = true;
      while (creditWaiters.length > 0) {
        const waiter = creditWaiters.shift()!;
        waiter(); // they will see `closed === true` and throw
      }
      const pendingMap = (client as unknown as { _pendingTunnels: Map<string, TunnelInflight> })._pendingTunnels;
      pendingMap.delete(tunnelId);
      closedResolve();
    },
  };

  const pendingMap = (client as unknown as { _pendingTunnels: Map<string, TunnelInflight> })._pendingTunnels;
  pendingMap.set(tunnelId, inflight);

  // ---- send TunnelOpen ----
  const openMsg: Record<string, unknown> = {
    tunnelId,
    targetTopic: target,
    protocol: (protocol === "ws" || protocol === "websocket") ? "WEBSOCKET" : protocol.toUpperCase(),
    remoteHint: options.remoteHint ?? "",
    metadata: options.metadata ?? {},
    sessionToken: options.sessionToken ?? "",
    backendName: options.backend ?? "",
  };
  if (options.idleTimeoutMs != null) {
    openMsg["idleTimeoutMs"] = options.idleTimeoutMs;
  }
  if (options.maxBytes != null) {
    openMsg["maxBytes"] = options.maxBytes;
  }
  (client as unknown as { _sendUpstream(msg: Record<string, unknown>): void })._sendUpstream({
    tunnelOpen: openMsg,
  });

  // ---- close helper ----
  function doClose(_reason: TunnelCloseReason): void {
    if (closed) return;
    closed = true;
    // Wake any blocked writers.
    while (creditWaiters.length > 0) {
      const waiter = creditWaiters.shift()!;
      waiter();
    }
    pendingMap.delete(tunnelId);
    closedResolve();
  }

  return {
    readable,
    writable,
    closed: closedPromise,
    close() {
      if (closed) return;
      (client as unknown as { _sendUpstream(msg: Record<string, unknown>): void })._sendUpstream({
        tunnelClose: {
          tunnelId,
          reason: "NORMAL",
          detail: "",
        },
      });
      doClose("NORMAL");
    },
  };
}
