import { describe, it, expect, beforeEach } from "vitest";
import { tunnelDial, TunnelClosedError } from "../tunnel.js";
import type { TunnelInflight } from "../tunnel.js";
import type { AetherClient } from "../client.js";
import { ConnectionError } from "../errors.js";

// =============================================================================
// Helpers — minimal AetherClient stub
// =============================================================================

function makeClient(opts: { connected?: boolean } = {}): AetherClient & {
  _pendingTunnels: Map<string, TunnelInflight>;
  _sentMessages: Record<string, unknown>[];
} {
  const _pendingTunnels = new Map<string, TunnelInflight>();
  const _sentMessages: Record<string, unknown>[] = [];
  let _idCounter = 0;

  const client = {
    _connected: opts.connected ?? true,
    _pendingTunnels,
    _sentMessages,
    nextRequestId() {
      _idCounter++;
      return `tun-test-${_idCounter}`;
    },
    _sendUpstream(msg: Record<string, unknown>) {
      _sentMessages.push(msg);
    },
  } as unknown as AetherClient & {
    _pendingTunnels: Map<string, TunnelInflight>;
    _sentMessages: Record<string, unknown>[];
  };

  return client;
}

/** Read all available chunks from a ReadableStream (non-blocking — drains what's queued). */
async function drainReadable(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  // Read with a short-circuit: collect chunks until the stream closes or cancels.
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) parts.push(value);
  }
  reader.releaseLock();
  const total = parts.reduce((s, p) => s + p.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) { out.set(p, off); off += p.length; }
  return out;
}

// =============================================================================
// Basic connectivity
// =============================================================================

describe("tunnelDial", () => {
  it("throws ConnectionError when client is not connected", () => {
    const client = makeClient({ connected: false });
    expect(() => tunnelDial(client, "sv::proxy::default", "tcp")).toThrow(ConnectionError);
  });

  it("sends TunnelOpen upstream on dial", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "tcp");

    expect(client._sentMessages).toHaveLength(1);
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect(msg.tunnelOpen).toBeDefined();
    expect(msg.tunnelOpen["targetTopic"]).toBe("sv::proxy::default");
    expect(msg.tunnelOpen["protocol"]).toBe("TCP");
    expect(typeof msg.tunnelOpen["tunnelId"]).toBe("string");
  });

  it("registers inflight in _pendingTunnels", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "tcp");

    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const tunnelId = msg.tunnelOpen["tunnelId"] as string;
    expect(client._pendingTunnels.has(tunnelId)).toBe(true);
  });

  it("maps 'ws' protocol to WEBSOCKET", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "ws");
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect(msg.tunnelOpen["protocol"]).toBe("WEBSOCKET");
  });

  it("maps 'websocket' protocol to WEBSOCKET", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "websocket");
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect(msg.tunnelOpen["protocol"]).toBe("WEBSOCKET");
  });

  it("maps 'udp' protocol to UDP", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "udp");
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect(msg.tunnelOpen["protocol"]).toBe("UDP");
  });

  it("forwards metadata in TunnelOpen", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "ws", {
      metadata: { "ws_framing": "tagged" },
    });
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect((msg.tunnelOpen["metadata"] as Record<string, string>)["ws_framing"]).toBe("tagged");
  });

  it("forwards remoteHint and idleTimeoutMs", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "tcp", {
      remoteHint: "localhost:8080",
      idleTimeoutMs: 5000,
    });
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect(msg.tunnelOpen["remoteHint"]).toBe("localhost:8080");
    expect(msg.tunnelOpen["idleTimeoutMs"]).toBe(5000);
  });
});

// =============================================================================
// Echo round-trip via fake transport
// =============================================================================

describe("tunnel echo round-trip", () => {
  it("delivers inbound data to readable stream", async () => {
    const client = makeClient();
    const tunnel = tunnelDial(client, "sv::proxy::default", "tcp", { initialCredits: 16 });

    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const tunnelId = msg.tunnelOpen["tunnelId"] as string;
    const inflight = client._pendingTunnels.get(tunnelId)!;

    const payload = new Uint8Array([1, 2, 3, 4, 5]);

    // Push inbound data then FIN to close the readable.
    inflight.pushData(payload, false);
    inflight.pushData(new Uint8Array(0), true);

    const received = await drainReadable(tunnel.readable);
    expect(received).toEqual(payload);
  });

  it("sends TunnelData upstream when writing to writable", async () => {
    const client = makeClient();
    const tunnel = tunnelDial(client, "sv::proxy::default", "tcp", { initialCredits: 16 });

    const writer = tunnel.writable.getWriter();
    await writer.write(new Uint8Array([10, 20, 30]));
    writer.releaseLock();

    // Should have sent TunnelOpen + at least one TunnelData
    const dataMessages = client._sentMessages.filter(m => "tunnelData" in m);
    expect(dataMessages.length).toBeGreaterThanOrEqual(1);
    const first = (dataMessages[0] as { tunnelData: Record<string, unknown> }).tunnelData;
    expect(first["tunnelId"]).toBe((client._sentMessages[0] as { tunnelOpen: Record<string, unknown> }).tunnelOpen["tunnelId"]);
    expect(first["data"]).toBeInstanceOf(Uint8Array);
    expect(Array.from(first["data"] as Uint8Array)).toEqual([10, 20, 30]);
  });
});

// =============================================================================
// Half-close
// =============================================================================

describe("tunnel half-close", () => {
  it("sends TunnelClose{NORMAL} when close() is called", () => {
    const client = makeClient();
    const tunnel = tunnelDial(client, "sv::proxy::default", "tcp");

    tunnel.close();

    const closeMessages = client._sentMessages.filter(m => "tunnelClose" in m);
    expect(closeMessages).toHaveLength(1);
    const closeMsg = (closeMessages[0] as { tunnelClose: Record<string, unknown> }).tunnelClose;
    expect(closeMsg["reason"]).toBe("NORMAL");
  });

  it("removes tunnel from _pendingTunnels after close()", () => {
    const client = makeClient();
    const tunnel = tunnelDial(client, "sv::proxy::default", "tcp");
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const tunnelId = msg.tunnelOpen["tunnelId"] as string;

    tunnel.close();
    expect(client._pendingTunnels.has(tunnelId)).toBe(false);
  });
});

// =============================================================================
// Two concurrent tunnels are independent
// =============================================================================

describe("concurrent tunnels", () => {
  it("two tunnels receive data independently", async () => {
    const client = makeClient();
    const t1 = tunnelDial(client, "sv::proxy::default", "tcp", { initialCredits: 16 });
    const t2 = tunnelDial(client, "sv::proxy::default", "tcp", { initialCredits: 16 });

    const msg1 = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const msg2 = client._sentMessages[1] as { tunnelOpen: Record<string, unknown> };
    const id1 = msg1.tunnelOpen["tunnelId"] as string;
    const id2 = msg2.tunnelOpen["tunnelId"] as string;
    expect(id1).not.toBe(id2);

    const inf1 = client._pendingTunnels.get(id1)!;
    const inf2 = client._pendingTunnels.get(id2)!;

    inf1.pushData(new Uint8Array([0xAA]), false);
    inf1.pushData(new Uint8Array(0), true);

    inf2.pushData(new Uint8Array([0xBB]), false);
    inf2.pushData(new Uint8Array(0), true);

    const [r1, r2] = await Promise.all([
      drainReadable(t1.readable),
      drainReadable(t2.readable),
    ]);

    expect(r1).toEqual(new Uint8Array([0xAA]));
    expect(r2).toEqual(new Uint8Array([0xBB]));
  });
});

// =============================================================================
// TunnelClose maps to typed errors
// =============================================================================

describe("TunnelClose error mapping", () => {
  it("errors readable when TunnelClose is received", async () => {
    const client = makeClient();
    const tunnel = tunnelDial(client, "sv::proxy::default", "tcp");
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const tunnelId = msg.tunnelOpen["tunnelId"] as string;
    const inflight = client._pendingTunnels.get(tunnelId)!;

    inflight.closeWithError(new TunnelClosedError("PEER_RESET", "connection reset by peer"));

    const reader = tunnel.readable.getReader();
    await expect(reader.read()).rejects.toThrow(TunnelClosedError);
    await expect(reader.read()).rejects.toThrow("PEER_RESET");
  });

  it("TunnelClosedError carries reason and detail", () => {
    const err = new TunnelClosedError("IDLE_TIMEOUT", "idle for 30s");
    expect(err.reason).toBe("IDLE_TIMEOUT");
    expect(err.detail).toBe("idle for 30s");
    expect(err.name).toBe("TunnelClosedError");
  });

  it("removes tunnel from _pendingTunnels on closeWithError", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "tcp");
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const tunnelId = msg.tunnelOpen["tunnelId"] as string;
    const inflight = client._pendingTunnels.get(tunnelId)!;

    inflight.closeWithError(new TunnelClosedError("ERROR", "oops"));
    expect(client._pendingTunnels.has(tunnelId)).toBe(false);
  });
});

// =============================================================================
// Bidirectional flow control
// =============================================================================

describe("flow control", () => {
  it("writer blocks when initial credits exhausted and resumes on TunnelAck", async () => {
    const client = makeClient();
    // Start with 1 credit so we can easily exhaust it.
    const tunnel = tunnelDial(client, "sv::proxy::default", "tcp", { initialCredits: 1 });
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const tunnelId = msg.tunnelOpen["tunnelId"] as string;
    const inflight = client._pendingTunnels.get(tunnelId)!;

    const writer = tunnel.writable.getWriter();

    // First write consumes the 1 credit — should complete immediately.
    await writer.write(new Uint8Array([0x01]));

    // Second write: no credits left — should block.
    let secondWriteDone = false;
    const secondWrite = writer.write(new Uint8Array([0x02])).then(() => {
      secondWriteDone = true;
    });

    // Give the event loop a turn — secondWrite should still be blocked.
    await Promise.resolve();
    await Promise.resolve();
    expect(secondWriteDone).toBe(false);

    // Grant a credit via TunnelAck — second write should unblock.
    inflight.addCredits(1);
    await secondWrite;
    expect(secondWriteDone).toBe(true);

    writer.releaseLock();
  });

  it("sends TunnelAck upstream after consuming enough inbound bytes", () => {
    const client = makeClient();
    tunnelDial(client, "sv::proxy::default", "tcp", { initialCredits: 16 });
    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    const tunnelId = msg.tunnelOpen["tunnelId"] as string;
    const inflight = client._pendingTunnels.get(tunnelId)!;

    // Push 256 KiB + 1 byte to trigger an ACK.
    const bigChunk = new Uint8Array(256 * 1024 + 1);
    inflight.pushData(bigChunk, false);

    const ackMessages = client._sentMessages.filter(m => "tunnelAck" in m);
    expect(ackMessages.length).toBeGreaterThanOrEqual(1);
    const ack = (ackMessages[0] as { tunnelAck: Record<string, unknown> }).tunnelAck;
    expect(ack["tunnelId"]).toBe(tunnelId);
    expect(Number(ack["credits"])).toBeGreaterThan(0);
  });
});

// =============================================================================
// backend option
// =============================================================================

describe("tunnelDial backend option", () => {
  it("emits backendName on the TunnelOpen envelope when supplied", () => {
    const client = makeClient();
    tunnelDial(client, "sv::svc::default", "tcp", { backend: "tcp-b" });

    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect(msg.tunnelOpen["backendName"]).toBe("tcp-b");
  });

  it("emits empty backendName when option is omitted", () => {
    const client = makeClient();
    tunnelDial(client, "sv::svc::default", "tcp");

    const msg = client._sentMessages[0] as { tunnelOpen: Record<string, unknown> };
    expect(msg.tunnelOpen["backendName"]).toBe("");
  });
});
