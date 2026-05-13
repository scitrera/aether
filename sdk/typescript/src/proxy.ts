/**
 * Proxy HTTP support for the Aether TypeScript SDK.
 *
 * Provides `proxyHttp()` on AetherClient for tunneling HTTP requests through
 * the Aether gRPC stream, and `AetherFetchTransport` as a drop-in replacement
 * for the Web Fetch API transport layer.
 *
 * Bodies larger than CHUNK_THRESHOLD (256 KB) are split into
 * ProxyHttpBodyChunk frames and reassembled on the response side.
 *
 * @module proxy
 */

import type { AetherClient } from "./client.js";
import { ConnectionError, TimeoutError } from "./errors.js";

// =============================================================================
// Constants
// =============================================================================

const CHUNK_THRESHOLD = 256 * 1024; // 256 KB

// =============================================================================
// Types
// =============================================================================

/** Transport-layer error kinds from the gateway. */
export enum ProxyErrorKind {
  Unknown = 0,
  DialFailed = 1,
  Timeout = 2,
  UpstreamReset = 3,
  AclDenied = 4,
  SidecarUnavailable = 5,
  PayloadTooLarge = 6,
  DecodeFailed = 7,
}

/** Transport-layer error from the gateway (not an HTTP error). */
export interface ProxyError {
  readonly kind: ProxyErrorKind;
  readonly message: string;
}

/** Structured response from a proxied HTTP request. */
export interface ProxyHttpResponse {
  readonly requestId: string;
  readonly statusCode: number;
  readonly headers: Record<string, string>;
  readonly body: Uint8Array;
  readonly bodyChunked: boolean;
  readonly error?: ProxyError;
}

/** Options for proxyHttp(). */
export interface ProxyHttpOptions {
  /** Request headers to send. */
  headers?: Record<string, string>;
  /** Request body (raw bytes). */
  body?: Uint8Array;
  /** Timeout in milliseconds (default: 30 000). */
  timeoutMs?: number;
  /** Whether to follow HTTP redirects (default: true). */
  followRedirects?: boolean;
  /**
   * Pin the request to a named terminator backend. The backend's allow-list
   * still applies — explicit naming selects which backend's ACL is consulted,
   * not whether the request is allowed.
   */
  backend?: string;
  /**
   * Opt into unbounded response streaming (SSE / log tails / model token
   * streams). When true, ``timeoutMs`` becomes the time-to-first-byte
   * deadline only; subsequent body bytes are governed by
   * ``streamIdleTimeoutMs``. The response body is delivered as a real
   * ``ReadableStream<Uint8Array>`` so consumers can iterate chunks as they
   * arrive without buffering the whole response.
   */
  streamResponse?: boolean;
  /**
   * Idle deadline (ms) between body bytes when ``streamResponse=true``.
   * Default 30 000 (30s) when unset. Exceeding closes the stream with
   * ``ProxyError{TIMEOUT}``.
   */
  streamIdleTimeoutMs?: number;
  /**
   * Maximum total response body bytes when ``streamResponse=true``. 0 (the
   * default) means "use the per-backend cap". Exceeding closes the stream
   * with ``ProxyError{PAYLOAD_TOO_LARGE}``.
   */
  maxResponseBodyBytes?: number;
}

/** Error thrown when the proxy transport layer fails. */
export class ProxyHttpError extends Error {
  readonly proxyError: ProxyError;
  constructor(err: ProxyError) {
    super(`Proxy transport error [${ProxyErrorKind[err.kind]}]: ${err.message}`);
    this.name = "ProxyHttpError";
    this.proxyError = err;
  }
}

// =============================================================================
// proxyHttp — callable as client.proxyHttp(...)
// =============================================================================

/**
 * Sends an HTTP request through the Aether gRPC stream to a service principal
 * and returns a fetch-compatible `Response` object.
 *
 * @param client - Connected AetherClient instance
 * @param target - Target topic, e.g. `"sv::memorylayer::default"` or wildcard `"sv::memorylayer"`
 * @param method - HTTP method, e.g. `"GET"`, `"POST"`
 * @param path   - Path including query string, e.g. `"/v1/memories/abc"`
 * @param opts   - Optional headers, body, timeout, followRedirects
 * @returns A `Response`-compatible object (standard Web API Response)
 * @throws {@link ProxyHttpError} on transport-layer failure
 * @throws {@link TimeoutError} if the request times out
 * @throws {@link ConnectionError} if not connected
 */
export async function proxyHttp(
  client: AetherClient,
  target: string,
  method: string,
  path: string,
  opts: ProxyHttpOptions = {},
): Promise<Response> {
  if (!(client as unknown as { _connected: boolean })._connected) {
    throw new ConnectionError("Not connected to gateway");
  }

  const requestId = (client as { nextRequestId(): string }).nextRequestId();
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const body = opts.body ?? new Uint8Array(0);
  const headers = opts.headers ?? {};
  const followRedirects = opts.followRedirects ?? true;
  const backendName = opts.backend ?? "";
  const streamResponse = opts.streamResponse ?? false;
  const streamIdleTimeoutMs = opts.streamIdleTimeoutMs ?? 0;
  const maxResponseBodyBytes = opts.maxResponseBodyBytes ?? 0;
  const bodyChunked = body.length > CHUNK_THRESHOLD;

  // For streaming responses, we wire a ReadableStream<Uint8Array> whose
  // controller is fed by the client's chunk dispatcher. The promise resolves
  // as soon as the header (TTFB) lands so the caller gets a real Response
  // back without waiting for fin.
  let streamingController: ReadableStreamDefaultController<Uint8Array> | null = null;
  const streamSlotMap = (client as {
    _pendingProxyHttpStreams: Map<
      string,
      { controller: ReadableStreamDefaultController<Uint8Array>; headerResolved: boolean }
    >;
  })._pendingProxyHttpStreams;
  const readable = streamResponse
    ? new ReadableStream<Uint8Array>({
        start(controller) {
          streamingController = controller;
          streamSlotMap.set(requestId, { controller, headerResolved: false });
        },
        cancel() {
          streamSlotMap.delete(requestId);
        },
      })
    : null;

  return new Promise<Response>((resolve, reject) => {
    const timer = setTimeout(() => {
      (client as { _pendingProxyHttpRequests: Map<string, unknown> })._pendingProxyHttpRequests.delete(requestId);
      (client as { _pendingProxyHttpChunks: Map<string, unknown> })._pendingProxyHttpChunks.delete(requestId);
      (client as { _pendingProxyHttpChunks: Map<string, unknown> })._pendingProxyHttpChunks.delete(requestId + ":shell");
      if (streamResponse) {
        streamSlotMap.delete(requestId);
        if (streamingController) {
          try { streamingController.error(new TimeoutError(`proxyHttp timed out after ${timeoutMs}ms`)); } catch { /* already closed */ }
        }
      }
      reject(new TimeoutError(`proxyHttp timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    const pendingMap = (client as { _pendingProxyHttpRequests: Map<string, (r: ProxyHttpResponse) => void> })._pendingProxyHttpRequests;
    pendingMap.set(requestId, (resp: ProxyHttpResponse) => {
      clearTimeout(timer);
      if (resp.error && resp.error.kind !== ProxyErrorKind.Unknown) {
        if (streamResponse && streamingController) {
          try { streamingController.error(new ProxyHttpError(resp.error)); } catch { /* */ }
        }
        reject(new ProxyHttpError(resp.error));
        return;
      }
      // Streaming responses surface the ReadableStream<Uint8Array> body so
      // chunks arrive incrementally; bounded responses keep the legacy
      // buffered shape.
      if (streamResponse && readable) {
        const responseHeaders = new Headers(resp.headers);
        resolve(new Response(readable, {
          status: resp.statusCode,
          headers: responseHeaders,
        }));
        return;
      }
      const responseHeaders = new Headers(resp.headers);
      resolve(new Response(resp.body.length > 0 ? resp.body : null, {
        status: resp.statusCode,
        headers: responseHeaders,
      }));
    });

    // Send the request envelope
    const upstream = client as unknown as { _sendUpstream(msg: Record<string, unknown>): void };

    if (!bodyChunked) {
      upstream._sendUpstream({
        proxyHttpRequest: {
          requestId,
          targetTopic: target,
          method,
          path,
          headers,
          body,
          bodyChunked: false,
          timeoutMs,
          followRedirects,
          backendName,
          streamResponseIndefinitely: streamResponse,
          streamIdleTimeoutMs,
          maxResponseBodyBytes,
        },
      });
    } else {
      // Send header envelope first with bodyChunked=true, empty body
      upstream._sendUpstream({
        proxyHttpRequest: {
          requestId,
          targetTopic: target,
          method,
          path,
          headers,
          body: new Uint8Array(0),
          bodyChunked: true,
          timeoutMs,
          followRedirects,
          backendName,
          streamResponseIndefinitely: streamResponse,
          streamIdleTimeoutMs,
          maxResponseBodyBytes,
        },
      });
      // Send body as chunks
      let seq = 0;
      let offset = 0;
      while (offset < body.length) {
        const end = Math.min(offset + CHUNK_THRESHOLD, body.length);
        const chunk = body.slice(offset, end);
        const fin = end >= body.length;
        upstream._sendUpstream({
          proxyHttpBodyChunk: {
            requestId,
            isRequest: true,
            seq,
            data: chunk,
            fin,
          },
        });
        seq++;
        offset = end;
      }
    }
  });
}

// =============================================================================
// AetherFetchTransport — drop-in for the Web Fetch API
// =============================================================================

/**
 * A drop-in transport for the Web Fetch API that routes requests through
 * an Aether gRPC connection to a service principal.
 *
 * @example
 * ```typescript
 * const transport = new AetherFetchTransport(agentClient, "sv::memorylayer::default");
 * const response = await transport.fetch("/v1/memories/abc");
 * ```
 */
export class AetherFetchTransport {
  private readonly _client: AetherClient;
  private readonly _target: string;
  private readonly _defaultTimeoutMs: number;
  private readonly _defaultBackend: string;

  /**
   * @param client  - Connected AetherClient
   * @param target  - Default target topic (e.g. `"sv::memorylayer::default"`)
   * @param timeoutMs - Default request timeout in ms (default: 30 000)
   * @param backend - Optional default terminator backend name applied to
   *   every request. The backend's allow-list still applies — explicit
   *   naming selects which backend's ACL is consulted.
   */
  constructor(client: AetherClient, target: string, timeoutMs = 30_000, backend = "") {
    this._client = client;
    this._target = target;
    this._defaultTimeoutMs = timeoutMs;
    this._defaultBackend = backend;
  }

  /**
   * Fetch-compatible method. Accepts a URL string or Request object plus
   * optional RequestInit overrides.
   *
   * The URL hostname/protocol is ignored — requests are routed to the
   * configured target topic. Only the pathname + search are used as the path.
   *
   * @returns Web API `Response`
   */
  async fetch(input: string | URL | Request, init?: RequestInit): Promise<Response> {
    let method = "GET";
    let path = "/";
    let reqHeaders: Record<string, string> = {};
    let reqBody: Uint8Array | undefined;

    if (typeof input === "string" || input instanceof URL) {
      const url = typeof input === "string" ? new URL(input, "http://placeholder") : input;
      path = url.pathname + url.search;
      method = (init?.method ?? "GET").toUpperCase();
    } else {
      // Request object
      const url = new URL(input.url, "http://placeholder");
      path = url.pathname + url.search;
      method = input.method.toUpperCase();
      // Copy headers from Request object
      input.headers.forEach((value, key) => { reqHeaders[key] = value; });
    }

    // Merge init headers
    if (init?.headers) {
      if (init.headers instanceof Headers) {
        init.headers.forEach((v, k) => { reqHeaders[k] = v; });
      } else if (Array.isArray(init.headers)) {
        for (const [k, v] of init.headers) { reqHeaders[k] = v; }
      } else {
        Object.assign(reqHeaders, init.headers);
      }
    }

    // Resolve body
    const rawBody = init?.body ?? (input instanceof Request ? input.body : null);
    if (rawBody != null) {
      if (rawBody instanceof Uint8Array) {
        reqBody = rawBody;
      } else if (rawBody instanceof ArrayBuffer) {
        reqBody = new Uint8Array(rawBody);
      } else if (typeof rawBody === "string") {
        reqBody = new TextEncoder().encode(rawBody);
      } else if (rawBody instanceof ReadableStream) {
        // Drain the stream
        const reader = rawBody.getReader();
        const parts: Uint8Array[] = [];
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value) parts.push(value instanceof Uint8Array ? value : new Uint8Array(value as ArrayBuffer));
        }
        const total = parts.reduce((s, p) => s + p.length, 0);
        reqBody = new Uint8Array(total);
        let off = 0;
        for (const p of parts) { reqBody.set(p, off); off += p.length; }
      }
    }

    return proxyHttp(this._client, this._target, method, path, {
      headers: reqHeaders,
      body: reqBody,
      timeoutMs: this._defaultTimeoutMs,
      backend: this._defaultBackend || undefined,
    });
  }
}
