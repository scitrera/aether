import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  proxyHttp,
  AetherFetchTransport,
  ProxyErrorKind,
  ProxyHttpError,
} from "../proxy.js";
import type { AetherClient } from "../client.js";
import { ConnectionError, TimeoutError } from "../errors.js";

// =============================================================================
// Helpers — minimal AetherClient stub
// =============================================================================

interface PendingMap<T> extends Map<string, T> {}

let reqCounter = 0;

function makeClient(opts: { connected?: boolean } = {}): AetherClient & {
  _pendingProxyHttpRequests: PendingMap<(r: unknown) => void>;
  _pendingProxyHttpChunks: Map<string, unknown>;
  _sentMessages: Record<string, unknown>[];
} {
  const _pendingProxyHttpRequests: PendingMap<(r: unknown) => void> = new Map();
  const _pendingProxyHttpChunks: Map<string, unknown> = new Map();
  const _sentMessages: Record<string, unknown>[] = [];
  let _idCounter = 0;

  const client = {
    _connected: opts.connected ?? true,
    _pendingProxyHttpRequests,
    _pendingProxyHttpChunks,
    _sentMessages,
    nextRequestId() {
      reqCounter++;
      _idCounter++;
      return `req-test-${_idCounter}`;
    },
    _sendUpstream(msg: Record<string, unknown>) {
      _sentMessages.push(msg);
    },
  } as unknown as AetherClient & {
    _pendingProxyHttpRequests: PendingMap<(r: unknown) => void>;
    _pendingProxyHttpChunks: Map<string, unknown>;
    _sentMessages: Record<string, unknown>[];
  };

  return client;
}

/** Simulate gateway delivering a non-chunked ProxyHttpResponse. */
function deliverResponse(
  client: ReturnType<typeof makeClient>,
  requestId: string,
  statusCode: number,
  headers: Record<string, string>,
  body: Uint8Array,
  error?: { kind: number; message: string },
) {
  const pending = client._pendingProxyHttpRequests.get(requestId);
  if (pending) {
    client._pendingProxyHttpRequests.delete(requestId);
    pending({ requestId, statusCode, headers, body, bodyChunked: false, error });
  }
}

/** Simulate gateway delivering a chunked response. */
function deliverChunkedResponse(
  client: ReturnType<typeof makeClient>,
  requestId: string,
  statusCode: number,
  headers: Record<string, string>,
  chunks: Uint8Array[],
) {
  // First the shell arrives (bodyChunked=true, empty body)
  client._pendingProxyHttpChunks.set(requestId, []);
  client._pendingProxyHttpChunks.set(requestId + ":shell", {
    requestId,
    statusCode,
    headers,
    body: new Uint8Array(0),
    bodyChunked: true,
  });

  // Then each chunk
  for (let i = 0; i < chunks.length; i++) {
    const fin = i === chunks.length - 1;
    const chunkList = client._pendingProxyHttpChunks.get(requestId) as Uint8Array[];
    chunkList.push(chunks[i]);

    if (fin) {
      const totalLen = chunkList.reduce((s, c) => s + c.length, 0);
      const body = new Uint8Array(totalLen);
      let offset = 0;
      for (const c of chunkList) { body.set(c, offset); offset += c.length; }

      client._pendingProxyHttpChunks.delete(requestId);
      client._pendingProxyHttpChunks.delete(requestId + ":shell");

      const pending = client._pendingProxyHttpRequests.get(requestId);
      if (pending) {
        client._pendingProxyHttpRequests.delete(requestId);
        pending({ requestId, statusCode, headers, body, bodyChunked: true });
      }
    }
  }
}

// =============================================================================
// proxyHttp() — basic request/response
// =============================================================================

describe("proxyHttp", () => {
  it("throws ConnectionError when client is not connected", async () => {
    const client = makeClient({ connected: false });
    await expect(proxyHttp(client, "sv::svc::inst", "GET", "/foo")).rejects.toThrow(ConnectionError);
  });

  it("sends a ProxyHttpRequest upstream and resolves on response", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::inst", "GET", "/api/v1/items");

    // Grab the requestId from the sent message
    expect(client._sentMessages).toHaveLength(1);
    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    expect(requestId).toBeTruthy();
    expect(msg.proxyHttpRequest["method"]).toBe("GET");
    expect(msg.proxyHttpRequest["path"]).toBe("/api/v1/items");
    expect(msg.proxyHttpRequest["targetTopic"]).toBe("sv::svc::inst");
    expect(msg.proxyHttpRequest["bodyChunked"]).toBe(false);

    // Deliver response
    deliverResponse(client, requestId, 200, { "content-type": "application/json" }, new TextEncoder().encode('{"ok":true}'));

    const response = await promise;
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("application/json");
    const text = await response.text();
    expect(text).toBe('{"ok":true}');
  });

  it("sends correct headers and body", async () => {
    const client = makeClient();
    const body = new TextEncoder().encode("hello");
    const promise = proxyHttp(client, "sv::svc::inst", "POST", "/data", {
      headers: { "x-custom": "value" },
      body,
    });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    expect(msg.proxyHttpRequest["method"]).toBe("POST");
    expect((msg.proxyHttpRequest["headers"] as Record<string, string>)["x-custom"]).toBe("value");
    expect(msg.proxyHttpRequest["bodyChunked"]).toBe(false);

    deliverResponse(client, requestId, 201, {}, new Uint8Array(0));
    const response = await promise;
    expect(response.status).toBe(201);
  });

  it("rejects with ProxyHttpError on transport-layer error", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::inst", "GET", "/");

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;

    deliverResponse(client, requestId, 0, {}, new Uint8Array(0), {
      kind: ProxyErrorKind.SidecarUnavailable,
      message: "no instances connected",
    });

    await expect(promise).rejects.toThrow(ProxyHttpError);
    await expect(promise).rejects.toThrow("SidecarUnavailable");
  });

  it("rejects with TimeoutError when no response arrives", async () => {
    vi.useFakeTimers();
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::inst", "GET", "/slow", { timeoutMs: 1000 });
    vi.advanceTimersByTime(1001);
    await expect(promise).rejects.toThrow(TimeoutError);
    vi.useRealTimers();
  });

  it("supports wildcard sv::{impl} target form", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::memorylayer", "GET", "/ping");
    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    expect(msg.proxyHttpRequest["targetTopic"]).toBe("sv::memorylayer");
    deliverResponse(client, requestId, 204, {}, new Uint8Array(0));
    const response = await promise;
    expect(response.status).toBe(204);
  });
});

// =============================================================================
// proxyHttp() — chunked body
// =============================================================================

describe("proxyHttp chunking", () => {
  it("splits request body >256KB into ProxyHttpBodyChunk frames", async () => {
    const client = makeClient();
    const bigBody = new Uint8Array(300 * 1024); // 300 KB
    bigBody.fill(0xab);

    const promise = proxyHttp(client, "sv::svc::inst", "POST", "/upload", { body: bigBody });

    // First message: header envelope with bodyChunked=true
    const headerMsg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    expect(headerMsg.proxyHttpRequest["bodyChunked"]).toBe(true);

    // Subsequent messages: body chunks
    const chunkMsgs = client._sentMessages.slice(1) as Array<{ proxyHttpBodyChunk: Record<string, unknown> }>;
    expect(chunkMsgs.length).toBeGreaterThan(1);
    expect(chunkMsgs[chunkMsgs.length - 1].proxyHttpBodyChunk["fin"]).toBe(true);

    // Total data in chunks should equal original body
    const allChunkData = chunkMsgs.map(m => m.proxyHttpBodyChunk["data"] as Uint8Array);
    const totalLen = allChunkData.reduce((s, c) => s + c.length, 0);
    expect(totalLen).toBe(bigBody.length);

    const requestId = headerMsg.proxyHttpRequest["requestId"] as string;
    deliverResponse(client, requestId, 200, {}, new Uint8Array(0));
    await promise;
  });

  it("reassembles chunked response body correctly", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::inst", "GET", "/large");

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;

    const part1 = new Uint8Array([1, 2, 3]);
    const part2 = new Uint8Array([4, 5, 6]);
    const part3 = new Uint8Array([7, 8, 9]);

    deliverChunkedResponse(client, requestId, 200, {}, [part1, part2, part3]);

    const response = await promise;
    expect(response.status).toBe(200);
    const buf = await response.arrayBuffer();
    expect(new Uint8Array(buf)).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9]));
  });

  it("handles large random-size body reassembly", async () => {
    const client = makeClient();
    const promise = proxyHttp(client, "sv::svc::inst", "GET", "/rand");
    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;

    // Simulate 5 random-size chunks
    const expected: number[] = [];
    const chunkArrays: Uint8Array[] = [];
    for (let i = 0; i < 5; i++) {
      const size = 10 + i * 7;
      const chunk = new Uint8Array(size);
      chunk.fill(i + 1);
      chunkArrays.push(chunk);
      for (let j = 0; j < size; j++) expected.push(i + 1);
    }

    deliverChunkedResponse(client, requestId, 200, { "x-count": "5" }, chunkArrays);

    const response = await promise;
    const buf = await response.arrayBuffer();
    expect(new Uint8Array(buf)).toEqual(new Uint8Array(expected));
  });
});

// =============================================================================
// AetherFetchTransport
// =============================================================================

describe("AetherFetchTransport", () => {
  it("routes fetch() through proxyHttp with correct method and path", async () => {
    const client = makeClient();
    const transport = new AetherFetchTransport(client, "sv::svc::inst");

    const promise = transport.fetch("http://ignored.host/api/items?q=1");

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    expect(msg.proxyHttpRequest["method"]).toBe("GET");
    expect(msg.proxyHttpRequest["path"]).toBe("/api/items?q=1");
    expect(msg.proxyHttpRequest["targetTopic"]).toBe("sv::svc::inst");

    deliverResponse(client, requestId, 200, {}, new Uint8Array(0));
    const response = await promise;
    expect(response.status).toBe(200);
  });

  it("passes method and body from RequestInit", async () => {
    const client = makeClient();
    const transport = new AetherFetchTransport(client, "sv::svc::inst");
    const bodyStr = JSON.stringify({ key: "val" });
    const promise = transport.fetch("/create", { method: "POST", body: bodyStr });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    expect(msg.proxyHttpRequest["method"]).toBe("POST");

    deliverResponse(client, requestId, 201, {}, new Uint8Array(0));
    await promise;
  });

  it("merges headers from RequestInit", async () => {
    const client = makeClient();
    const transport = new AetherFetchTransport(client, "sv::svc::inst");
    const promise = transport.fetch("/data", {
      headers: { authorization: "Bearer tok", "x-custom": "abc" },
    });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    const hdrs = msg.proxyHttpRequest["headers"] as Record<string, string>;
    expect(hdrs["authorization"]).toBe("Bearer tok");
    expect(hdrs["x-custom"]).toBe("abc");

    deliverResponse(client, requestId, 200, {}, new Uint8Array(0));
    await promise;
  });

  it("accepts a Request object", async () => {
    const client = makeClient();
    const transport = new AetherFetchTransport(client, "sv::svc::inst");
    const req = new Request("http://ignored/path/to/resource", {
      method: "DELETE",
      headers: { "x-req-header": "present" },
    });
    const promise = transport.fetch(req);

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    expect(msg.proxyHttpRequest["method"]).toBe("DELETE");
    expect(msg.proxyHttpRequest["path"]).toBe("/path/to/resource");
    const hdrs = msg.proxyHttpRequest["headers"] as Record<string, string>;
    expect(hdrs["x-req-header"]).toBe("present");

    deliverResponse(client, requestId, 200, {}, new Uint8Array(0));
    await promise;
  });

  it("accepts Headers object in RequestInit", async () => {
    const client = makeClient();
    const transport = new AetherFetchTransport(client, "sv::svc::inst");
    const headers = new Headers({ "x-h": "val" });
    const promise = transport.fetch("/x", { headers });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    const hdrs = msg.proxyHttpRequest["headers"] as Record<string, string>;
    expect(hdrs["x-h"]).toBe("val");

    deliverResponse(client, requestId, 200, {}, new Uint8Array(0));
    await promise;
  });

  it("propagates ProxyHttpError from transport layer", async () => {
    const client = makeClient();
    const transport = new AetherFetchTransport(client, "sv::svc::inst");
    const promise = transport.fetch("/gone");

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    const requestId = msg.proxyHttpRequest["requestId"] as string;
    deliverResponse(client, requestId, 0, {}, new Uint8Array(0), {
      kind: ProxyErrorKind.DialFailed,
      message: "connection refused",
    });

    await expect(promise).rejects.toThrow(ProxyHttpError);
  });
});

// =============================================================================
// backend option / AetherFetchTransport default backend
// =============================================================================

describe("proxyHttp backend option", () => {
  it("emits backendName on the upstream envelope when supplied", async () => {
    const client = makeClient();
    void proxyHttp(client, "sv::svc::inst", "GET", "/v1/ping", { backend: "admin" });

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    expect(msg.proxyHttpRequest["backendName"]).toBe("admin");
  });

  it("emits empty backendName when option is omitted", async () => {
    const client = makeClient();
    void proxyHttp(client, "sv::svc::inst", "GET", "/v1/ping");

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    expect(msg.proxyHttpRequest["backendName"]).toBe("");
  });

  it("AetherFetchTransport ctor default backend threads through", async () => {
    const client = makeClient();
    const transport = new AetherFetchTransport(client, "sv::svc::default", 30_000, "primary");
    void transport.fetch("/probe");

    const msg = client._sentMessages[0] as { proxyHttpRequest: Record<string, unknown> };
    expect(msg.proxyHttpRequest["backendName"]).toBe("primary");
  });
});
