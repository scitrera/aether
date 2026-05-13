import { describe, it, expect } from "vitest";
import { proxyHttp, ProxyErrorKind } from "../proxy.js";
import type { AetherClient } from "../client.js";

// =============================================================================
// Stub helpers — minimal AetherClient surface for streaming
// =============================================================================

function makeClient(): AetherClient & {
  _pendingProxyHttpRequests: Map<string, (r: unknown) => void>;
  _pendingProxyHttpChunks: Map<string, unknown>;
  _pendingProxyHttpStreams: Map<
    string,
    { controller: ReadableStreamDefaultController<Uint8Array>; headerResolved: boolean }
  >;
  _sentMessages: Record<string, unknown>[];
} {
  const _pendingProxyHttpRequests = new Map<string, (r: unknown) => void>();
  const _pendingProxyHttpChunks = new Map<string, unknown>();
  const _pendingProxyHttpStreams = new Map<
    string,
    { controller: ReadableStreamDefaultController<Uint8Array>; headerResolved: boolean }
  >();
  const _sentMessages: Record<string, unknown>[] = [];
  let _idCounter = 0;

  return {
    _connected: true,
    _pendingProxyHttpRequests,
    _pendingProxyHttpChunks,
    _pendingProxyHttpStreams,
    _sentMessages,
    nextRequestId() {
      _idCounter++;
      return `req-stream-${_idCounter}`;
    },
    _sendUpstream(msg: Record<string, unknown>) {
      _sentMessages.push(msg);
    },
  } as unknown as ReturnType<typeof makeClient>;
}

/** Drives the streaming dispatch: lands the header, then enqueues N chunks. */
function deliverStreamingHeader(
  client: ReturnType<typeof makeClient>,
  requestId: string,
  status: number,
  headers: Record<string, string>,
  error?: { kind: number; message: string },
) {
  // Resolve the proxy request promise (binds the ReadableStream into the Response).
  const pending = client._pendingProxyHttpRequests.get(requestId);
  const slot = client._pendingProxyHttpStreams.get(requestId);
  if (slot) {
    slot.headerResolved = true;
  }
  if (pending) {
    client._pendingProxyHttpRequests.delete(requestId);
    pending({
      requestId,
      statusCode: status,
      headers,
      body: new Uint8Array(0),
      bodyChunked: true,
      error,
    });
  }
}

function deliverStreamingChunk(
  client: ReturnType<typeof makeClient>,
  requestId: string,
  data: Uint8Array,
  fin: boolean,
) {
  const slot = client._pendingProxyHttpStreams.get(requestId);
  if (!slot) return;
  if (data.length > 0) {
    slot.controller.enqueue(data);
  }
  if (fin) {
    slot.controller.close();
    client._pendingProxyHttpStreams.delete(requestId);
  }
}

function deliverStreamingError(
  client: ReturnType<typeof makeClient>,
  requestId: string,
  message: string,
) {
  const slot = client._pendingProxyHttpStreams.get(requestId);
  if (!slot) return;
  slot.controller.error(new Error(message));
  client._pendingProxyHttpStreams.delete(requestId);
}

// =============================================================================
// Tests
// =============================================================================

describe("proxyHttp streaming", () => {
  it("emits the streamResponseIndefinitely flag on the upstream envelope", async () => {
    const client = makeClient();
    void proxyHttp(client, "sv::svc::stream", "GET", "/events", {
      streamResponse: true,
      streamIdleTimeoutMs: 15_000,
      maxResponseBodyBytes: 1024,
    });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    expect(msg.proxyHttpRequest["streamResponseIndefinitely"]).toBe(true);
    expect(msg.proxyHttpRequest["streamIdleTimeoutMs"]).toBe(15_000);
    expect(msg.proxyHttpRequest["maxResponseBodyBytes"]).toBe(1024);
  });

  it("returns a Response whose body is a real ReadableStream<Uint8Array>", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::stream", "GET", "/events", {
      streamResponse: true,
    });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;

    deliverStreamingHeader(client, requestId, 200, { "content-type": "text/event-stream" });

    const response = await promise;
    expect(response.status).toBe(200);
    expect(response.body).toBeInstanceOf(ReadableStream);

    // Push three chunks then fin.
    deliverStreamingChunk(client, requestId, new TextEncoder().encode("event-0\n"), false);
    deliverStreamingChunk(client, requestId, new TextEncoder().encode("event-1\n"), false);
    deliverStreamingChunk(client, requestId, new TextEncoder().encode("event-2\n"), true);

    const reader = (response.body as ReadableStream<Uint8Array>).getReader();
    const decoder = new TextDecoder();
    let assembled = "";
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      assembled += decoder.decode(value);
    }
    expect(assembled).toBe("event-0\nevent-1\nevent-2\n");
  });

  it("propagates a mid-stream error to the body reader", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::stream", "GET", "/big", {
      streamResponse: true,
    });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;

    deliverStreamingHeader(client, requestId, 200, {});
    const response = await promise;

    // Push the partial chunk first; reader reads it; THEN inject the error.
    deliverStreamingChunk(client, requestId, new TextEncoder().encode("partial"), false);
    const reader = (response.body as ReadableStream<Uint8Array>).getReader();
    const first = await reader.read();
    expect(first.value).toEqual(new TextEncoder().encode("partial"));

    deliverStreamingError(client, requestId, "exceeded cap");
    await expect(reader.read()).rejects.toThrow("exceeded cap");
  });

  it("rejects when the header lands carrying a TTFB ProxyError", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::stream", "GET", "/dead", {
      streamResponse: true,
    });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;

    deliverStreamingHeader(client, requestId, 0, {}, {
      kind: ProxyErrorKind.Timeout,
      message: "TTFB timeout",
    });

    await expect(promise).rejects.toThrow("Timeout");
  });
});
