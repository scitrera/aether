/**
 * Aether TypeScript SDK — Orchestrator client example.
 *
 * Demonstrates:
 *   - Orchestrator client construction with supported profiles
 *   - TaskAssignment handler — the orchestrator's primary callback
 *   - Sending status messages back to the targeted agent/task
 *   - Graceful shutdown
 *
 * Orchestrators receive TaskAssignment messages when the gateway needs to start
 * an agent or task whose identity is currently offline. The orchestrator is
 * expected to launch the corresponding compute resource (container, pod, VM)
 * and report progress back via status messages.
 *
 * Run:  npx tsx examples/orchestrator/index.ts
 */
import {
  OrchestratorClient,
  withAPIKey,
  withTenant,
  type TaskAssignment,
} from "../../src/index.js";

const SERVER = process.env.AETHER_SERVER ?? "localhost:50051";
const IMPL = process.env.AETHER_IMPL ?? "ts-demo-orchestrator";
const SPEC = process.env.AETHER_SPEC ?? "orch-01";
const PROFILES = (process.env.AETHER_PROFILES ?? "docker,kubernetes").split(",").map((p) => p.trim());

async function main(): Promise<void> {
  const credentials = {
    ...(process.env.AETHER_API_KEY ? withAPIKey(process.env.AETHER_API_KEY) : {}),
    ...(process.env.AETHER_TENANT ? withTenant(process.env.AETHER_TENANT) : {}),
  };

  const orch = new OrchestratorClient({
    address: SERVER,
    implementation: IMPL,
    specifier: SPEC,
    supportedProfiles: PROFILES,
    credentials,
    reconnect: true,
  });

  orch.onTaskAssignment(async (task: TaskAssignment) => {
    console.log(
      `\n[orchestrator] *** TaskAssignment ***\n` +
        `  taskId=${task.taskId} profile=${task.profile} target=${task.targetImplementation}\n` +
        `  workspace=${task.workspace} specifier=${task.specifier}\n` +
        `  launchParams=${JSON.stringify(task.launchParams)}`,
    );

    switch (task.profile) {
      case "docker":
        console.log("[orchestrator] would launch Docker container...");
        break;
      case "kubernetes":
        console.log("[orchestrator] would create Kubernetes pod...");
        break;
      default:
        console.log(`[orchestrator] unsupported profile: ${task.profile}`);
        return;
    }

    if (task.workspace && task.targetImplementation && task.specifier) {
      const status = JSON.stringify({
        status: "launching",
        taskId: task.taskId,
        message: "orchestrator starting your instance",
      });
      orch.sendStatusToAgent(
        task.workspace,
        task.targetImplementation,
        task.specifier,
        new TextEncoder().encode(status),
      );
    }
  });

  orch.onError((err) => console.error(`[orchestrator] error: ${err.code} ${err.message}`));
  orch.onConnect((ack) => console.log(`[orchestrator] connected sessionId=${ack.sessionId}`));
  orch.onDisconnect((reason) => console.log(`[orchestrator] disconnected: ${reason}`));

  const shutdown = async (signal: string) => {
    console.log(`\n[orchestrator] ${signal} received, shutting down...`);
    await orch.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  console.log(`[orchestrator] connecting to ${SERVER} as ${IMPL}.${SPEC} with profiles [${PROFILES.join(", ")}]...`);
  await orch.connect();

  console.log("[orchestrator] ready; waiting for TaskAssignment messages (Ctrl+C to exit)");
}

main().catch((err) => {
  console.error("[orchestrator] fatal:", err);
  process.exit(1);
});
