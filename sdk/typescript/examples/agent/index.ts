/**
 * Aether TypeScript SDK — Agent client example.
 *
 * Demonstrates:
 *   - Agent client construction and graceful connect
 *   - Handler registration (messages, errors, lifecycle)
 *   - Sending messages, events, and metrics
 *   - KV operations across scopes
 *   - Task creation
 *   - Graceful shutdown on SIGINT/SIGTERM
 *
 * Run:  npx tsx examples/agent/index.ts
 *
 * Environment:
 *   AETHER_SERVER     gateway address (default: localhost:50051)
 *   AETHER_WORKSPACE  workspace to join (default: default)
 *   AETHER_API_KEY    API key credential (optional)
 *   AETHER_TENANT     tenant id (optional)
 */
import {
  AgentClient,
  KVScope,
  newMetric,
  withAPIKey,
  withTenant,
  type ConnectionAck,
  type IncomingMessage,
} from "../../src/index.js";

const SERVER = process.env.AETHER_SERVER ?? "localhost:50051";
const WORKSPACE = process.env.AETHER_WORKSPACE ?? "default";
const IMPL = "ts-demo";
const SPEC = "agent-01";

async function main(): Promise<void> {
  const credentials = {
    ...(process.env.AETHER_API_KEY ? withAPIKey(process.env.AETHER_API_KEY) : {}),
    ...(process.env.AETHER_TENANT ? withTenant(process.env.AETHER_TENANT) : {}),
  };

  const agent = new AgentClient({
    address: SERVER,
    workspace: WORKSPACE,
    implementation: IMPL,
    specifier: SPEC,
    credentials,
    reconnect: true,
    reconnectDelay: 1000,
    maxReconnectDelay: 30000,
  });

  agent.onMessage((msg: IncomingMessage) => {
    console.log(`[agent] msg from ${msg.sourceTopic}: ${new TextDecoder().decode(msg.payload)}`);
  });
  agent.onError((err) => console.error(`[agent] error: ${err.code} ${err.message}`));
  agent.onConnect((ack: ConnectionAck) =>
    console.log(`[agent] connected sessionId=${ack.sessionId} resumed=${ack.resumed}`),
  );
  agent.onDisconnect((reason) => console.log(`[agent] disconnected: ${reason}`));

  const shutdown = async (signal: string) => {
    console.log(`\n[agent] ${signal} received, shutting down...`);
    await agent.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  console.log(`[agent] connecting to ${SERVER} as ${IMPL}.${SPEC} in ${WORKSPACE}...`);
  await agent.connect();

  agent.sendToAgent(WORKSPACE, IMPL, SPEC, new TextEncoder().encode("Hello from TS agent!"));

  await agent.kv().put({
    key: "demo/setting",
    value: new TextEncoder().encode("global-value"),
    scope: KVScope.Global,
  });
  const got = await agent.kv().getSync({ key: "demo/setting", scope: KVScope.Global });
  console.log(`[agent] KV get → success=${got.success} value=${got.value}`);

  agent.sendEvent(new TextEncoder().encode(JSON.stringify({ event: "agent_started", at: Date.now() })));
  agent.sendMetric(newMetric().add("messages_processed", "", 1).tag("agent", IMPL).build());

  agent.createTask({ taskType: "data-processing", workspace: WORKSPACE });

  console.log("[agent] demo operations complete; waiting for messages (Ctrl+C to exit)");
}

main().catch((err) => {
  console.error("[agent] fatal:", err);
  process.exit(1);
});
