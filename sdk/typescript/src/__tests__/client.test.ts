import { describe, it, expect } from "vitest";
import {
  // Types and enums
  PrincipalType,
  MessageType,
  KVScope,
  TaskAssignmentMode,
  SignalType,
  // Topic helpers
  agentTopic,
  globalAgentsTopic,
  uniqueTaskTopic,
  taskTopic,
  taskBroadcastTopic,
  userTopic,
  userWorkspaceTopic,
  globalUsersTopic,
  eventTopic,
  eventWildcardTopic,
  metricTopic,
  metricWildcardTopic,
  TOPIC_PREFIX_AGENT,
  TOPIC_PREFIX_UNIQUE_TASK,
  TOPIC_PREFIX_TASK,
  TOPIC_PREFIX_TASK_BROADCAST,
  TOPIC_PREFIX_USER,
  TOPIC_PREFIX_USER_WORKSPACE,
  TOPIC_PREFIX_GLOBAL_AGENTS,
  TOPIC_PREFIX_GLOBAL_USERS,
  TOPIC_PREFIX_EVENT,
  TOPIC_PREFIX_METRIC,
  TOPIC_PREFIX_PROGRESS,
  progressTopic,
  // Errors
  AetherError,
  ConnectionError,
  ConnectionClosedError,
  ReconnectionError,
  AuthenticationError,
  PermissionDeniedError,
  DuplicateIdentityError,
  TimeoutError,
  InvalidArgumentError,
  NotFoundError,
  UnimplementedError,
  MessageError,
  KVOperationError,
  CheckpointError,
  isRecoverable,
  isConnectionError,
  isTimeoutError,
  // Credential helpers
  withAPIKey,
  withToken,
  withTaskToken,
  withTenant,
  // Client classes
  AetherClient,
  AgentClient,
  TaskClient,
  UserClient,
  OrchestratorClient,
  WorkflowEngineClient,
  MetricsBridgeClient,
  CheckpointClient,
  KVClient,
  // New response types
  type WorkspaceResponse,
  type AgentResponse,
  type ACLResponse,
  type WorkflowResponse,
  type KVIncrementOptions,
  type KVDecrementOptions,
  type WorkspaceResponseHandler,
  type AgentResponseHandler,
  type ACLResponseHandler,
  type AuthorityGrantResponseHandler,
  type WorkflowResponseHandler,
  type AuditSubmitResponse,
  type AuditSubmitResponseHandler,
} from "../index.js";

// =============================================================================
// Enum Tests
// =============================================================================

describe("PrincipalType", () => {
  it("has all expected values", () => {
    expect(PrincipalType.Agent).toBe("agent");
    expect(PrincipalType.UniqueTask).toBe("unique_task");
    expect(PrincipalType.NonUniqueTask).toBe("non_unique_task");
    expect(PrincipalType.User).toBe("user");
    expect(PrincipalType.WorkflowEngine).toBe("workflow_engine");
    expect(PrincipalType.MetricsBridge).toBe("metrics_bridge");
    expect(PrincipalType.Orchestrator).toBe("orchestrator");
  });
});

describe("MessageType", () => {
  it("has all expected values", () => {
    expect(MessageType.Unspecified).toBe(0);
    expect(MessageType.Chat).toBe(1);
    expect(MessageType.Control).toBe(2);
    expect(MessageType.ToolCall).toBe(3);
    expect(MessageType.Event).toBe(4);
    expect(MessageType.Metric).toBe(5);
  });
});

describe("KVScope", () => {
  it("has all expected values", () => {
    expect(KVScope.Unspecified).toBe(0);
    expect(KVScope.Global).toBe(1);
    expect(KVScope.Workspace).toBe(2);
    expect(KVScope.User).toBe(3);
    expect(KVScope.UserWorkspace).toBe(4);
  });
});

describe("TaskAssignmentMode", () => {
  it("has all expected values", () => {
    expect(TaskAssignmentMode.SelfAssign).toBe(0);
    expect(TaskAssignmentMode.Targeted).toBe(1);
    expect(TaskAssignmentMode.Pool).toBe(2);
  });
});

describe("SignalType", () => {
  it("has expected values", () => {
    expect(SignalType.ForceDisconnect).toBe(0);
  });
});

// =============================================================================
// Topic Tests
// =============================================================================

describe("Topic Construction", () => {
  describe("topic prefixes", () => {
    it("has correct prefix constants", () => {
      expect(TOPIC_PREFIX_AGENT).toBe("ag");
      expect(TOPIC_PREFIX_UNIQUE_TASK).toBe("tu");
      expect(TOPIC_PREFIX_TASK).toBe("ta");
      expect(TOPIC_PREFIX_TASK_BROADCAST).toBe("tb");
      expect(TOPIC_PREFIX_USER).toBe("us");
      expect(TOPIC_PREFIX_USER_WORKSPACE).toBe("uw");
      expect(TOPIC_PREFIX_GLOBAL_AGENTS).toBe("ga");
      expect(TOPIC_PREFIX_GLOBAL_USERS).toBe("gu");
      expect(TOPIC_PREFIX_EVENT).toBe("event");
      expect(TOPIC_PREFIX_METRIC).toBe("metric");
      expect(TOPIC_PREFIX_PROGRESS).toBe("pg");
    });
  });

  describe("agentTopic", () => {
    it("creates correct agent topic", () => {
      expect(agentTopic("prod", "data-processor", "instance-1")).toBe(
        "ag::prod::data-processor::instance-1",
      );
    });
  });

  describe("globalAgentsTopic", () => {
    it("creates correct global agents topic", () => {
      expect(globalAgentsTopic("prod")).toBe("ga::prod");
    });
  });

  describe("uniqueTaskTopic", () => {
    it("creates correct unique task topic", () => {
      expect(uniqueTaskTopic("prod", "report-gen", "daily")).toBe(
        "tu::prod::report-gen::daily",
      );
    });
  });

  describe("taskTopic", () => {
    it("creates correct non-unique task topic", () => {
      expect(taskTopic("prod", "worker", "abc123")).toBe(
        "ta::prod::worker::abc123",
      );
    });
  });

  describe("taskBroadcastTopic", () => {
    it("creates correct task broadcast topic", () => {
      expect(taskBroadcastTopic("prod", "worker")).toBe("tb::prod::worker");
    });
  });

  describe("userTopic", () => {
    it("creates correct user topic", () => {
      expect(userTopic("alice", "tab-1")).toBe("us::alice::tab-1");
    });
  });

  describe("userWorkspaceTopic", () => {
    it("creates correct user workspace topic", () => {
      expect(userWorkspaceTopic("alice", "prod")).toBe("uw::alice::prod");
    });
  });

  describe("globalUsersTopic", () => {
    it("creates correct global users topic", () => {
      expect(globalUsersTopic("prod")).toBe("gu::prod");
    });
  });

  describe("eventTopic", () => {
    it("creates correct event topic", () => {
      expect(eventTopic("task.completed")).toBe("event::task.completed");
    });
  });

  describe("eventWildcardTopic", () => {
    it("returns correct wildcard", () => {
      expect(eventWildcardTopic()).toBe("event::*");
    });
  });

  describe("metricTopic", () => {
    it("creates correct metric topic", () => {
      expect(metricTopic("performance")).toBe("metric::performance");
    });
  });

  describe("metricWildcardTopic", () => {
    it("returns correct wildcard", () => {
      expect(metricWildcardTopic()).toBe("metric::*");
    });
  });

  describe("progressTopic", () => {
    it("creates correct progress topic", () => {
      expect(progressTopic("prod")).toBe("pg::prod");
    });
  });
});

// =============================================================================
// Error Tests
// =============================================================================

describe("Errors", () => {
  describe("AetherError", () => {
    it("creates with message", () => {
      const err = new AetherError("test error");
      expect(err.message).toBe("test error");
      expect(err.code).toBe("");
      expect(err.details).toBe("");
      expect(err).toBeInstanceOf(Error);
    });

    it("creates with code and details", () => {
      const err = new AetherError("test", "CODE", "details");
      expect(err.message).toBe("[CODE] test (details)");
      expect(err.code).toBe("CODE");
      expect(err.details).toBe("details");
    });

    it("preserves cause", () => {
      const cause = new Error("original");
      const err = new AetherError("wrapped", "", "", cause);
      expect(err.cause).toBe(cause);
    });
  });

  describe("ConnectionError", () => {
    it("creates with default message", () => {
      const err = new ConnectionError();
      expect(err.message).toBe("Failed to connect to Aether gateway");
      expect(err.name).toBe("ConnectionError");
      expect(err).toBeInstanceOf(AetherError);
    });
  });

  describe("ConnectionClosedError", () => {
    it("creates with reason", () => {
      const err = new ConnectionClosedError("server restart");
      expect(err.reason).toBe("server restart");
      expect(err.name).toBe("ConnectionClosedError");
      expect(err).toBeInstanceOf(AetherError);
    });
  });

  describe("ReconnectionError", () => {
    it("creates with attempts", () => {
      const err = new ReconnectionError(5);
      expect(err.attempts).toBe(5);
      expect(err.name).toBe("ReconnectionError");
    });
  });

  describe("AuthenticationError", () => {
    it("has UNAUTHENTICATED code", () => {
      const err = new AuthenticationError();
      expect(err.code).toBe("UNAUTHENTICATED");
      expect(err.name).toBe("AuthenticationError");
    });
  });

  describe("PermissionDeniedError", () => {
    it("has PERMISSION_DENIED code", () => {
      const err = new PermissionDeniedError();
      expect(err.code).toBe("PERMISSION_DENIED");
    });
  });

  describe("DuplicateIdentityError", () => {
    it("has ALREADY_EXISTS code", () => {
      const err = new DuplicateIdentityError("ag::prod::worker::1");
      expect(err.code).toBe("ALREADY_EXISTS");
      expect(err.identity).toBe("ag::prod::worker::1");
    });
  });

  describe("TimeoutError", () => {
    it("has DEADLINE_EXCEEDED code", () => {
      const err = new TimeoutError("connect", 5);
      expect(err.code).toBe("DEADLINE_EXCEEDED");
      expect(err.operation).toBe("connect");
      expect(err.timeoutSeconds).toBe(5);
    });
  });

  describe("InvalidArgumentError", () => {
    it("has INVALID_ARGUMENT code", () => {
      const err = new InvalidArgumentError("bad value", "field");
      expect(err.code).toBe("INVALID_ARGUMENT");
      expect(err.argument).toBe("field");
    });
  });

  describe("NotFoundError", () => {
    it("creates with resource", () => {
      const err = new NotFoundError("workspace");
      expect(err.message).toContain("workspace not found");
      expect(err.resource).toBe("workspace");
    });
  });

  describe("UnimplementedError", () => {
    it("creates with operation", () => {
      const err = new UnimplementedError("list");
      expect(err.message).toContain("list");
      expect(err.operation).toBe("list");
    });
  });

  describe("MessageError", () => {
    it("creates with default message", () => {
      const err = new MessageError();
      expect(err.message).toBe("Message error");
    });
  });

  describe("KVOperationError", () => {
    it("creates with operation and key", () => {
      const err = new KVOperationError("get", "my-key");
      expect(err.message).toContain("KV get operation failed");
      expect(err.message).toContain("my-key");
      expect(err.operation).toBe("get");
      expect(err.key).toBe("my-key");
    });
  });

  describe("CheckpointError", () => {
    it("creates with operation and key", () => {
      const err = new CheckpointError("save", "state");
      expect(err.message).toContain("Checkpoint save operation failed");
      expect(err.message).toContain("state");
    });
  });
});

// =============================================================================
// Error Classification Tests
// =============================================================================

describe("Error Classification", () => {
  describe("isRecoverable", () => {
    it("returns false for auth errors", () => {
      expect(isRecoverable(new AuthenticationError())).toBe(false);
    });

    it("returns false for permission errors", () => {
      expect(isRecoverable(new PermissionDeniedError())).toBe(false);
    });

    it("returns false for duplicate identity", () => {
      expect(isRecoverable(new DuplicateIdentityError())).toBe(false);
    });

    it("returns false for invalid argument", () => {
      expect(isRecoverable(new InvalidArgumentError())).toBe(false);
    });

    it("returns false for not found", () => {
      expect(isRecoverable(new NotFoundError())).toBe(false);
    });

    it("returns false for unimplemented", () => {
      expect(isRecoverable(new UnimplementedError())).toBe(false);
    });

    it("returns true for connection errors", () => {
      expect(isRecoverable(new ConnectionError())).toBe(true);
    });

    it("returns true for timeout errors", () => {
      expect(isRecoverable(new TimeoutError())).toBe(true);
    });

    it("returns true for generic errors", () => {
      expect(isRecoverable(new Error("something"))).toBe(true);
    });
  });

  describe("isConnectionError", () => {
    it("returns true for ConnectionError", () => {
      expect(isConnectionError(new ConnectionError())).toBe(true);
    });

    it("returns true for ConnectionClosedError", () => {
      expect(isConnectionError(new ConnectionClosedError())).toBe(true);
    });

    it("returns true for ReconnectionError", () => {
      expect(isConnectionError(new ReconnectionError(5))).toBe(true);
    });

    it("returns false for other errors", () => {
      expect(isConnectionError(new AuthenticationError())).toBe(false);
    });
  });

  describe("isTimeoutError", () => {
    it("returns true for TimeoutError", () => {
      expect(isTimeoutError(new TimeoutError())).toBe(true);
    });

    it("returns false for other errors", () => {
      expect(isTimeoutError(new ConnectionError())).toBe(false);
    });
  });
});

// =============================================================================
// Credential Helper Tests
// =============================================================================

describe("Credential Helpers", () => {
  it("withAPIKey creates correct credentials", () => {
    const creds = withAPIKey("test-key");
    expect(creds["x-api-key"]).toBe("test-key");
  });

  it("withToken creates correct credentials", () => {
    const creds = withToken("jwt-token");
    expect(creds["authorization"]).toBe("Bearer jwt-token");
  });

  it("withTaskToken creates correct credentials", () => {
    const creds = withTaskToken("task-token");
    expect(creds["token"]).toBe("task-token");
  });

  it("withTenant creates correct credentials", () => {
    const creds = withTenant("tenant-1");
    expect(creds["x-tenant-id"]).toBe("tenant-1");
  });
});

// =============================================================================
// Client Construction Tests
// =============================================================================

describe("AetherClient", () => {
  it("throws InvalidArgumentError when address is missing", () => {
    expect(() => new AetherClient({ address: "" })).toThrow(InvalidArgumentError);
  });

  it("creates with valid address", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(client.connected).toBe(false);
    expect(client.sessionId).toBe("");
  });

  it("supports default reconnection options", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(client).toBeDefined();
  });

  it("supports custom reconnection options", () => {
    const client = new AetherClient({
      address: "localhost:50051",
      reconnect: true,
      reconnectDelay: 500,
      maxReconnectDelay: 10000,
    });
    expect(client).toBeDefined();
  });
});

describe("AgentClient", () => {
  it("throws InvalidArgumentError when workspace is missing", () => {
    expect(
      () =>
        new AgentClient({
          address: "localhost:50051",
          workspace: "",
          implementation: "test",
          specifier: "1",
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("throws InvalidArgumentError when implementation is missing", () => {
    expect(
      () =>
        new AgentClient({
          address: "localhost:50051",
          workspace: "prod",
          implementation: "",
          specifier: "1",
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("throws InvalidArgumentError when specifier is missing", () => {
    expect(
      () =>
        new AgentClient({
          address: "localhost:50051",
          workspace: "prod",
          implementation: "test",
          specifier: "",
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("creates with valid options", () => {
    const agent = new AgentClient({
      address: "localhost:50051",
      workspace: "prod",
      implementation: "data-processor",
      specifier: "instance-1",
    });
    expect(agent.workspace).toBe("prod");
    expect(agent.implementation).toBe("data-processor");
    expect(agent.specifier).toBe("instance-1");
    expect(agent.topic).toBe("ag::prod::data-processor::instance-1");
  });
});

describe("UserClient", () => {
  it("throws InvalidArgumentError when userId is missing", () => {
    expect(
      () =>
        new UserClient({
          address: "localhost:50051",
          userId: "",
          windowId: "tab-1",
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("throws InvalidArgumentError when windowId is missing", () => {
    expect(
      () =>
        new UserClient({
          address: "localhost:50051",
          userId: "alice",
          windowId: "",
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("creates with valid options", () => {
    const user = new UserClient({
      address: "localhost:50051",
      userId: "alice",
      windowId: "tab-1",
    });
    expect(user.userId).toBe("alice");
    expect(user.windowId).toBe("tab-1");
    expect(user.topic).toBe("us::alice::tab-1");
    expect(user.workspace).toBe("");
    expect(user.workspaceTopic).toBe("");
  });

  it("supports workspace association", () => {
    const user = new UserClient({
      address: "localhost:50051",
      userId: "alice",
      windowId: "tab-1",
      workspace: "prod",
    });
    expect(user.workspace).toBe("prod");
    expect(user.workspaceTopic).toBe("uw::alice::prod");

    user.setWorkspace("staging");
    expect(user.workspace).toBe("staging");
    expect(user.workspaceTopic).toBe("uw::alice::staging");
  });
});

// =============================================================================
// TaskClient Tests
// =============================================================================

describe("TaskClient", () => {
  it("throws InvalidArgumentError when workspace is missing", () => {
    expect(
      () =>
        new TaskClient({
          address: "localhost:50051",
          workspace: "",
          implementation: "worker",
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("throws InvalidArgumentError when implementation is missing", () => {
    expect(
      () =>
        new TaskClient({
          address: "localhost:50051",
          workspace: "prod",
          implementation: "",
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("creates unique task with specifier", () => {
    const task = new TaskClient({
      address: "localhost:50051",
      workspace: "prod",
      implementation: "report-gen",
      uniqueSpecifier: "daily-report",
    });
    expect(task.workspace).toBe("prod");
    expect(task.implementation).toBe("report-gen");
    expect(task.uniqueSpecifier).toBe("daily-report");
    expect(task.isUnique).toBe(true);
    expect(task.topic).toBe("tu::prod::report-gen::daily-report");
  });

  it("creates non-unique task without specifier", () => {
    const task = new TaskClient({
      address: "localhost:50051",
      workspace: "prod",
      implementation: "worker",
    });
    expect(task.uniqueSpecifier).toBe("");
    expect(task.isUnique).toBe(false);
    expect(task.topic).toBe("tb::prod::worker");
  });
});

// =============================================================================
// OrchestratorClient Tests
// =============================================================================

describe("OrchestratorClient", () => {
  it("throws InvalidArgumentError when implementation is missing", () => {
    expect(
      () =>
        new OrchestratorClient({
          address: "localhost:50051",
          implementation: "",
          supportedProfiles: ["kubernetes"],
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("throws InvalidArgumentError when supportedProfiles is empty", () => {
    expect(
      () =>
        new OrchestratorClient({
          address: "localhost:50051",
          implementation: "k8s-orch",
          supportedProfiles: [],
        }),
    ).toThrow(InvalidArgumentError);
  });

  it("creates with valid options", () => {
    const orch = new OrchestratorClient({
      address: "localhost:50051",
      implementation: "k8s-orchestrator",
      supportedProfiles: ["kubernetes", "docker"],
      specifier: "orch-1",
    });
    expect(orch.implementation).toBe("k8s-orchestrator");
    expect(orch.specifier).toBe("orch-1");
    expect(orch.supportedProfiles).toEqual(["kubernetes", "docker"]);
  });

  it("auto-generates specifier if not provided", () => {
    const orch = new OrchestratorClient({
      address: "localhost:50051",
      implementation: "k8s-orchestrator",
      supportedProfiles: ["kubernetes"],
    });
    expect(orch.specifier).toBeTruthy();
    expect(orch.specifier.length).toBe(8);
  });
});

// =============================================================================
// WorkflowEngineClient Tests
// =============================================================================

describe("WorkflowEngineClient", () => {
  it("creates with valid options", () => {
    const engine = new WorkflowEngineClient({
      address: "localhost:50051",
    });
    expect(engine).toBeDefined();
    expect(engine.connected).toBe(false);
  });
});

// =============================================================================
// MetricsBridgeClient Tests
// =============================================================================

describe("MetricsBridgeClient", () => {
  it("creates with valid options", () => {
    const bridge = new MetricsBridgeClient({
      address: "localhost:50051",
    });
    expect(bridge).toBeDefined();
    expect(bridge.connected).toBe(false);
  });
});

// =============================================================================
// CheckpointClient Tests
// =============================================================================

describe("CheckpointClient", () => {
  it("is accessible from AetherClient", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const cp = client.checkpoint();
    expect(cp).toBeInstanceOf(CheckpointClient);
  });

  it("returns the same instance on repeated calls", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const cp1 = client.checkpoint();
    const cp2 = client.checkpoint();
    expect(cp1).toBe(cp2);
  });
});

// =============================================================================
// KVClient Tests
// =============================================================================

describe("KVClient", () => {
  it("is accessible from AetherClient", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const kv = client.kv();
    expect(kv).toBeInstanceOf(KVClient);
  });

  it("returns the same instance on repeated calls", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const kv1 = client.kv();
    const kv2 = client.kv();
    expect(kv1).toBe(kv2);
  });
});

describe("KVClient increment/decrement methods", () => {
  it("has increment method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const kv = client.kv();
    expect(typeof kv.increment).toBe("function");
  });

  it("has incrementSync method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const kv = client.kv();
    expect(typeof kv.incrementSync).toBe("function");
  });

  it("has decrement method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const kv = client.kv();
    expect(typeof kv.decrement).toBe("function");
  });

  it("has decrementSync method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const kv = client.kv();
    expect(typeof kv.decrementSync).toBe("function");
  });
});

// =============================================================================
// New Type Exports Tests
// =============================================================================

describe("New type exports", () => {
  it("WorkspaceResponse type is usable", () => {
    const response: WorkspaceResponse = {
      success: true,
      error: "",
      message: "ok",
      totalCount: 0,
    };
    expect(response.success).toBe(true);
  });

  it("AgentResponse type is usable", () => {
    const response: AgentResponse = {
      success: true,
      error: "",
      message: "ok",
      totalCount: 1,
    };
    expect(response.totalCount).toBe(1);
  });

  it("ACLResponse type is usable", () => {
    const response: ACLResponse = {
      success: false,
      error: "denied",
      message: "",
    };
    expect(response.error).toBe("denied");
  });

  it("WorkflowResponse type is usable", () => {
    const response: WorkflowResponse = {
      success: true,
      error: "",
      message: "triggered",
      totalCount: 0,
    };
    expect(response.message).toBe("triggered");
  });

  it("KVIncrementOptions type is usable", () => {
    const opts: KVIncrementOptions = { key: "my-counter", scope: KVScope.Global };
    expect(opts.key).toBe("my-counter");
  });

  it("KVDecrementOptions type is usable", () => {
    const opts: KVDecrementOptions = { key: "my-counter", scope: KVScope.Workspace };
    expect(opts.scope).toBe(KVScope.Workspace);
  });
});

// =============================================================================
// AetherClient New Methods Tests
// =============================================================================

describe("AetherClient new methods", () => {
  it("has completeTask method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.completeTask).toBe("function");
  });

  it("has failTask method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.failTask).toBe("function");
  });

  it("has sendWorkspaceOperation method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.sendWorkspaceOperation).toBe("function");
  });

  it("has sendAgentOperation method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.sendAgentOperation).toBe("function");
  });

  it("has sendACLOperation method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.sendACLOperation).toBe("function");
  });

  it("has sendAuthorityGrantOperation method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.sendAuthorityGrantOperation).toBe("function");
  });

  it("has sendWorkflowOperation method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.sendWorkflowOperation).toBe("function");
  });

  it("has onWorkspaceResponse method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.onWorkspaceResponse).toBe("function");
  });

  it("has onAgentResponse method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.onAgentResponse).toBe("function");
  });

  it("has onACLResponse method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.onACLResponse).toBe("function");
  });

  it("has onAuthorityGrantResponse method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.onAuthorityGrantResponse).toBe("function");
  });

  it("has onWorkflowResponse method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.onWorkflowResponse).toBe("function");
  });
});

// =============================================================================
// Handler Registration Tests
// =============================================================================

describe("handler registration", () => {
  it("registers workspace response handler without error", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const handler: WorkspaceResponseHandler = () => {};
    client.onWorkspaceResponse(handler);
    // No error thrown = success
  });

  it("registers agent response handler without error", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const handler: AgentResponseHandler = () => {};
    client.onAgentResponse(handler);
  });

  it("registers ACL response handler without error", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const handler: ACLResponseHandler = () => {};
    client.onACLResponse(handler);
  });

  it("registers authority grant response handler without error", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const handler: AuthorityGrantResponseHandler = () => {};
    client.onAuthorityGrantResponse(handler);
  });

  it("registers workflow response handler without error", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const handler: WorkflowResponseHandler = () => {};
    client.onWorkflowResponse(handler);
  });

  it("allows replacing handler with a new one", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    const handler1: WorkspaceResponseHandler = () => {};
    const handler2: WorkspaceResponseHandler = () => {};
    client.onWorkspaceResponse(handler1);
    client.onWorkspaceResponse(handler2);
    // No error thrown = success
  });
});

// =============================================================================
// Token Operations Tests
// =============================================================================

describe("Token Operations", () => {
  it("has sendTokenOperation method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.sendTokenOperation).toBe("function");
  });
  it("has onTokenResponse method", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    expect(typeof client.onTokenResponse).toBe("function");
  });
  it("registers token response handler", () => {
    const client = new AetherClient({ address: "localhost:50051" });
    client.onTokenResponse(() => {});
  });
});

// =============================================================================
// switchWorkspace Tests
// =============================================================================

describe("AgentClient.switchWorkspace", () => {
  it("updates workspace property on success", () => {
    const agent = new AgentClient({
      address: "localhost:50051",
      workspace: "prod",
      implementation: "worker",
      specifier: "1",
    });
    expect(agent.workspace).toBe("prod");
    // switchWorkspace is fire-and-forget (void); it updates the local property
    // and enqueues the upstream proto message. We verify the local state change
    // here; wire-level behavior requires an integration test.
    // Note: calling when not connected will throw ConnectionError from
    // _sendUpstream, so we monkeypatch the stream to avoid that.
    (agent as unknown as { _stream: { write: () => void } })._stream = { write: () => {} };
    agent.switchWorkspace("staging");
    expect(agent.workspace).toBe("staging");
  });

  it("throws InvalidArgumentError when workspace is empty", () => {
    const agent = new AgentClient({
      address: "localhost:50051",
      workspace: "prod",
      implementation: "worker",
      specifier: "1",
    });
    expect(() => agent.switchWorkspace("")).toThrow(InvalidArgumentError);
  });
});

describe("TaskClient.switchWorkspace", () => {
  it("updates workspace property on success", () => {
    const task = new TaskClient({
      address: "localhost:50051",
      workspace: "prod",
      implementation: "report-gen",
      uniqueSpecifier: "daily",
    });
    expect(task.workspace).toBe("prod");
    (task as unknown as { _stream: { write: () => void } })._stream = { write: () => {} };
    task.switchWorkspace("staging");
    expect(task.workspace).toBe("staging");
  });

  it("throws InvalidArgumentError when workspace is empty", () => {
    const task = new TaskClient({
      address: "localhost:50051",
      workspace: "prod",
      implementation: "worker",
    });
    expect(() => task.switchWorkspace("")).toThrow(InvalidArgumentError);
  });
});

describe("UserClient.switchWorkspace", () => {
  it("updates workspace property on success", () => {
    const user = new UserClient({
      address: "localhost:50051",
      userId: "alice",
      windowId: "tab-1",
      workspace: "prod",
    });
    expect(user.workspace).toBe("prod");
    (user as unknown as { _stream: { write: () => void } })._stream = { write: () => {} };
    user.switchWorkspace("staging");
    expect(user.workspace).toBe("staging");
  });

  it("throws InvalidArgumentError when workspace is empty", () => {
    const user = new UserClient({
      address: "localhost:50051",
      userId: "alice",
      windowId: "tab-1",
    });
    expect(() => user.switchWorkspace("")).toThrow(InvalidArgumentError);
  });
});

describe("AetherClient.submitAuditEvent", () => {
  it("sends upstream submitAuditEvent and resolves on response", async () => {
    const client = new AetherClient({ address: "localhost:50051" });
    let captured: any = null;
    (client as any)._stream = { write: (msg: any) => { captured = msg; } };
    const promise = client.submitAuditEvent({
      eventType: "message",
      operation: "test_op",
      metadata: { key: "v" },
    });
    expect(captured).not.toBeNull();
    expect(captured.submitAuditEvent).toBeDefined();
    const requestId = captured.submitAuditEvent.clientRequestId;
    expect(typeof requestId).toBe("string");
    expect(requestId.length).toBeGreaterThan(0);
    expect(captured.submitAuditEvent.eventType).toBe("message");

    // Manually invoke the pending callback to simulate the downstream response.
    const cb = (client as any)._pendingAuditSubmitRequests.get(requestId);
    expect(cb).toBeDefined();
    cb({ clientRequestId: requestId, success: true, errorCode: "", errorMessage: "" });
    const result = await promise;
    expect(result.success).toBe(true);
  });

  it("resolves with a server-error response (not reject)", async () => {
    const client = new AetherClient({ address: "localhost:50051" });
    let captured: any = null;
    (client as any)._stream = { write: (msg: any) => { captured = msg; } };
    const promise = client.submitAuditEvent({ eventType: "message" });
    const requestId = captured.submitAuditEvent.clientRequestId;
    const cb = (client as any)._pendingAuditSubmitRequests.get(requestId);
    cb({ clientRequestId: requestId, success: false, errorCode: "ERR_AUDIT_TYPE_FORBIDDEN", errorMessage: "event type not permitted" });
    const result = await promise;
    expect(result.success).toBe(false);
    expect(result.errorCode).toBe("ERR_AUDIT_TYPE_FORBIDDEN");
  });

  it("rejects on timeout", async () => {
    const client = new AetherClient({ address: "localhost:50051" });
    (client as any)._stream = { write: () => {} };
    await expect(client.submitAuditEvent({ eventType: "message" }, 50)).rejects.toThrow(/timed out/);
  });
});
