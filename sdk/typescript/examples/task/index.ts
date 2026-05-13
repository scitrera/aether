/**
 * Aether TypeScript SDK — Task client example.
 *
 * Demonstrates:
 *   - Unique task (with specifier) and non-unique task (worker pool) modes
 *   - Sending messages to agents, users, other tasks
 *   - Progress reporting
 *   - Workspace switching
 *   - Graceful shutdown
 *
 * Run:
 *   npx tsx examples/task/index.ts                  # unique
 *   AETHER_TASK_UNIQUE=false npx tsx examples/task/index.ts   # non-unique pool worker
 */
import {
  TaskClient,
  newMetric,
  withAPIKey,
  withTenant,
  type IncomingMessage,
} from "../../src/index.js";

const SERVER = process.env.AETHER_SERVER ?? "localhost:50051";
const WORKSPACE = process.env.AETHER_WORKSPACE ?? "default";
const IMPL = "ts-demo-task";
const UNIQUE = process.env.AETHER_TASK_UNIQUE !== "false";
const SPEC = UNIQUE ? "task-01" : undefined;

async function main(): Promise<void> {
  const credentials = {
    ...(process.env.AETHER_API_KEY ? withAPIKey(process.env.AETHER_API_KEY) : {}),
    ...(process.env.AETHER_TENANT ? withTenant(process.env.AETHER_TENANT) : {}),
  };

  const task = new TaskClient({
    address: SERVER,
    workspace: WORKSPACE,
    implementation: IMPL,
    ...(UNIQUE ? { uniqueSpecifier: SPEC! } : {}),
    credentials,
    reconnect: true,
  });

  task.onMessage((msg: IncomingMessage) => {
    console.log(`[task] msg from ${msg.sourceTopic}: ${new TextDecoder().decode(msg.payload)}`);
  });
  task.onError((err) => console.error(`[task] error: ${err.code} ${err.message}`));
  task.onConnect((ack) => console.log(`[task] connected sessionId=${ack.sessionId}`));

  const shutdown = async (signal: string) => {
    console.log(`\n[task] ${signal} received, shutting down...`);
    await task.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  console.log(`[task] connecting to ${SERVER} as ${task.topic} (unique=${task.isUnique})...`);
  await task.connect();

  task.sendToAgent(WORKSPACE, "ts-demo", "agent-01", new TextEncoder().encode("hello agent from task"));
  task.sendToUser("user-123", "tab-1", new TextEncoder().encode("task completed notification"));

  const work = JSON.stringify({ job_id: "job-123", priority: "high" });
  task.sendToTask(WORKSPACE, "worker-pool", "", new TextEncoder().encode(work));

  task.sendEvent(new TextEncoder().encode(JSON.stringify({ event: "task_progress", progress: 50 })));
  task.sendMetric(newMetric().add("items_processed", "", 100).tag("task", IMPL).build());

  console.log(`[task] current workspace: ${task.workspace}`);

  console.log("[task] demo operations complete; waiting (Ctrl+C to exit)");
}

main().catch((err) => {
  console.error("[task] fatal:", err);
  process.exit(1);
});
