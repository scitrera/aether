#!/usr/bin/env python3
"""
Aether Client Example

This example demonstrates the full feature set of the Aether Python client,
including:
- Agent, Task, User, Orchestrator, WorkflowEngine, and MetricsBridge clients
- Message sending with different message types
- KV operations with scope support (global, workspace, user, user-workspace)
- Task creation with assignment modes (self-assign, targeted, pool)
- Task assignment callbacks
- Config snapshot handling
- Context manager support (auto-cleanup on exit)
- Custom exception handling
- TLS/mTLS configuration
- MultiprocessOrchestrator for subprocess management
"""

import json
import time
from typing import Optional

from scitrera_aether_client import (
    # Client classes
    AgentClient,
    TaskClient,
    UserClient,
    OrchestratorClient,
    WorkflowEngineClient,
    MetricsBridgeClient,
    # Orchestrator classes
    MultiprocessOrchestrator,
    # Message type constants
    CHAT,
    CONTROL,
    EVENT,
    METRIC,
    # Task assignment mode constants
    SELF_ASSIGN,
    TARGETED,
    # Custom exceptions
    AetherError,
    ConnectionError,
    AuthenticationError,
    DuplicateIdentityError,
    ReconnectionError,
    TimeoutError,
    # Type definitions (for documentation)
    ConnectionConfig,
    TLSConfig,
    # Metric builder
    new_metric,
)


def demo_agent_client():
    """Demonstrate the AgentClient with all features."""
    print("\n" + "=" * 60)
    print("Agent Client Demo")
    print("=" * 60)

    client = AgentClient(
        workspace="default",
        implementation="python-demo",
        specifier="agent-01"
    )

    # Set up callbacks
    def on_message(msg):
        print(f"[Agent] Message from {msg.source_topic}: {msg.payload.decode()}")

    def on_config(config):
        print(f"[Agent] Config snapshot received:")
        print(f"  Workspace KV: {dict(config.kv)}")
        print(f"  Global KV: {dict(config.global_kv)}")

    def on_task_assignment(assignment):
        print(f"[Agent] Task assigned:")
        print(f"  Task ID: {assignment.task_id}")
        print(f"  Task Type: {assignment.task_type}")
        print(f"  Assigned To: {assignment.assigned_to}")
        print(f"  Metadata: {dict(assignment.metadata)}")

    def on_kv_response(kv):
        print(f"[Agent] KV Response: success={kv.success}")
        if kv.value:
            print(f"  Value: {kv.value}")
        if kv.keys:
            print(f"  Keys: {list(kv.keys)}")

    def on_error(error):
        print(f"[Agent] Error: {error.code} - {error.message}")

    client.on_message = on_message
    client.on_config = on_config
    client.on_task_assignment = on_task_assignment
    client.on_kv_response = on_kv_response
    client.on_error = on_error

    try:
        print("Connecting to gateway...")
        client.connect("localhost:50051")
        print("Connected!")

        time.sleep(1)

        # Demo: Send a message to ourselves
        print("\n--- Sending message to self ---")
        client.send_message_to_agent(
            workspace="default",
            implementation="python-demo",
            specifier="agent-01",
            payload=b"Hello from Python agent!"
        )

        time.sleep(0.5)

        # Demo: KV operations with different scopes
        print("\n--- KV Operations ---")

        # Global scope
        print("Storing value in global scope...")
        client.kv_put(
            key="demo/setting",
            value=b"global-value",
            scope="global"
        )

        time.sleep(0.3)

        # Workspace scope
        print("Storing value in workspace scope...")
        client.kv_put(
            key="demo/workspace-setting",
            value=b"workspace-value",
            scope="workspace",
            workspace="default"
        )

        time.sleep(0.3)

        # Retrieve a value
        print("Getting value from global scope...")
        client.kv_get(key="demo/setting", scope="global")

        time.sleep(0.5)

        # Demo: Create a self-assigned task
        print("\n--- Task Creation (Self-Assign) ---")
        client.create_task(
            task_type="data-processing",
            workspace="default",
            assignment_mode=SELF_ASSIGN,
            metadata={"source": "python-demo", "priority": "normal"}
        )

        time.sleep(0.5)

        # Demo: Send events and metrics
        print("\n--- Events and Metrics ---")
        event_payload = json.dumps({
            "event_type": "agent_started",
            "agent_id": "python-demo.agent-01",
            "timestamp": time.time()
        }).encode()
        client.send_event(event_payload)
        print("Sent event to workflow engine")

        metric = new_metric().trace("demo-trace-1").add("messages_processed", "counter", 42.0).tag("agent", "python-demo").build()
        client.send_metric(metric)
        print("Sent metric to metrics bridge")

        time.sleep(1)

        print("\nWaiting for messages (Ctrl+C to exit)...")
        while True:
            time.sleep(1)

    except AuthenticationError as e:
        print(f"[Agent] Authentication failed: {e}")
    except DuplicateIdentityError as e:
        print(f"[Agent] Identity already in use: {e}")
    except ReconnectionError as e:
        print(f"[Agent] Could not reconnect after {e.attempts} attempts")
    except ConnectionError as e:
        print(f"[Agent] Connection failed: {e}")
    except AetherError as e:
        print(f"[Agent] Aether error: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        client.close()
        print("Disconnected.")


def demo_agent_context_manager():
    """Demonstrate using AgentClient as a context manager (new feature)."""
    print("\n" + "=" * 60)
    print("Agent Client (Context Manager) Demo")
    print("=" * 60)

    # Using context manager for automatic cleanup
    with AgentClient(
        workspace="default",
        implementation="python-demo",
        specifier="context-agent"
    ) as client:
        client.on_message = lambda msg: print(f"[Agent] {msg.payload.decode()}")

        try:
            client.connect("localhost:50051")
            print("Connected via context manager!")

            client.send_message_to_agent(
                workspace="default",
                implementation="python-demo",
                specifier="context-agent",
                payload=b"Hello via context manager!"
            )

            time.sleep(1)
            print("Exiting context manager (auto-close)...")

        except AetherError as e:
            print(f"[Agent] Error: {e}")

    print("Connection closed automatically.")


def demo_tls_configuration():
    """Demonstrate TLS/mTLS configuration (new feature)."""
    print("\n" + "=" * 60)
    print("TLS Configuration Demo")
    print("=" * 60)

    # Example 1: Basic TLS (server verification only)
    print("\n--- Basic TLS Configuration ---")
    print("Creating client with TLS enabled...")

    # Note: These paths are examples - replace with actual certificate paths
    _tls_client = AgentClient(
        workspace="default",
        implementation="python-demo",
        specifier="tls-agent",
        # Enable TLS
        tls_enabled=True,
        # Root CA certificate (verify server identity)
        tls_root_cert_path="/path/to/ca.crt",  # or use tls_root_cert=bytes
    )
    print("  TLS client created (server verification)")

    # Example 2: Mutual TLS (mTLS) with client certificates
    print("\n--- mTLS Configuration ---")
    print("Creating client with mTLS enabled...")

    _mtls_client = AgentClient(
        workspace="default",
        implementation="python-demo",
        specifier="mtls-agent",
        # Enable TLS
        tls_enabled=True,
        # Root CA certificate
        tls_root_cert_path="/path/to/ca.crt",
        # Client certificate and key for mTLS
        tls_client_cert_path="/path/to/client.crt",
        tls_client_key_path="/path/to/client.key",
    )
    print("  mTLS client created (mutual authentication)")

    # Example 3: Using in-memory certificates
    print("\n--- In-Memory Certificates ---")
    print("Certificates can also be provided as bytes:")
    print("  tls_root_cert=b'-----BEGIN CERTIFICATE-----...'")
    print("  tls_client_cert=b'-----BEGIN CERTIFICATE-----...'")
    print("  tls_client_key=b'-----BEGIN PRIVATE KEY-----...'")

    print("\nNote: This demo doesn't actually connect (no real certs).")


def demo_orchestrator_client():
    """Demonstrate the OrchestratorClient."""
    print("\n" + "=" * 60)
    print("Orchestrator Client Demo")
    print("=" * 60)

    client = OrchestratorClient(
        implementation="demo-orchestrator",
        supported_profiles=["docker", "kubernetes"]
    )

    def on_message(msg):
        print(f"[Orchestrator] Received from {msg.source_topic}:")
        try:
            data = json.loads(msg.payload.decode())
            print(f"  Startup request: {data}")
        except Exception:
            print(f"  Raw: {msg.payload.decode()}")

    def on_task_assignment(assignment):
        print(f"[Orchestrator] Task assignment received:")
        print(f"  Task ID: {assignment.task_id}")
        print(f"  Task Type: {assignment.task_type}")
        # Orchestrator would now launch the required compute resources

    client.on_message = on_message
    client.on_task_assignment = on_task_assignment

    try:
        print("Connecting as orchestrator...")
        client.connect("localhost:50051")
        print("Orchestrator connected! Listening for startup requests...")

        while True:
            time.sleep(1)
    except AetherError as e:
        print(f"[Orchestrator] Error: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        client.close()
        print("Orchestrator disconnected.")


def demo_multiprocess_orchestrator():
    """Demonstrate the MultiprocessOrchestrator (new feature)."""
    print("\n" + "=" * 60)
    print("MultiprocessOrchestrator Demo")
    print("=" * 60)

    class _DemoOrchestrator(MultiprocessOrchestrator):
        """Example orchestrator that spawns Python scripts."""

        def get_implementation(self) -> str:
            return "demo-multiprocess"

        def get_supported_profiles(self) -> list:
            return ["python-script", "python-module"]

        def handle_assignment(self, assignment) -> None:
            """Handle task assignment by spawning a subprocess."""
            task_type = assignment.task_type
            metadata = dict(assignment.metadata)

            print(f"[Orchestrator] Handling assignment: {task_type}")

            if metadata.get("profile") == "python-script":
                # Spawn a Python script
                script_path = metadata.get("script", "agent.py")
                self.spawn_subprocess(
                    script_path=script_path,
                    task_id=assignment.task_id,
                    args=["--task-id", assignment.task_id],
                    env={"AETHER_WORKSPACE": assignment.workspace}
                )
            elif metadata.get("profile") == "python-module":
                # Run a Python module
                module_name = metadata.get("module", "my_agent")
                self.spawn_module(
                    module_name=module_name,
                    task_id=assignment.task_id,
                    args=["--task-id", assignment.task_id]
                )
            else:
                print(f"[Orchestrator] Unknown profile: {metadata.get('profile')}")

        def on_connect(self) -> None:
            """Called when connected to gateway."""
            print("[Orchestrator] Connected and ready for assignments!")

        def on_disconnect(self, reason: str) -> None:
            """Called when disconnected."""
            print(f"[Orchestrator] Disconnected: {reason}")

    print("\nMultiprocessOrchestrator features:")
    print("  - Extends BaseOrchestrator for easy subprocess management")
    print("  - spawn_subprocess() - Launch Python scripts")
    print("  - spawn_module() - Run Python modules via 'python -m'")
    print("  - Automatic process tracking and cleanup")
    print("  - Output capture with dedicated reader threads")
    print("  - Graceful termination (SIGTERM -> SIGKILL fallback)")
    print("\nSee the _DemoOrchestrator class above for implementation example.")


def demo_workflow_engine():
    """Demonstrate the WorkflowEngineClient."""
    print("\n" + "=" * 60)
    print("Workflow Engine Client Demo")
    print("=" * 60)

    # Using context manager for automatic cleanup
    with WorkflowEngineClient() as client:
        def on_message(msg):
            print(f"[WorkflowEngine] Event from {msg.source_topic}:")
            try:
                event = json.loads(msg.payload.decode())
                print(f"  Event type: {event.get('event_type')}")
                print(f"  Data: {event}")
            except Exception:
                print(f"  Raw: {msg.payload.decode()}")

        client.on_message = on_message

        try:
            print("Connecting as workflow engine...")
            client.connect("localhost:50051")
            print("Workflow engine connected! Listening for events...")

            # Demo: Send a command to an agent
            time.sleep(1)
            print("\n--- Sending command to agent ---")
            command = json.dumps({
                "command": "start_processing",
                "params": {"batch_size": 100}
            }).encode()
            client.send_command_to_agent(
                workspace="default",
                implementation="python-demo",
                specifier="agent-01",
                payload=command
            )
            print("Command sent!")

            while True:
                time.sleep(1)
        except AetherError as e:
            print(f"[WorkflowEngine] Error: {e}")
        except KeyboardInterrupt:
            pass

    print("Workflow engine disconnected.")


def demo_metrics_bridge():
    """Demonstrate the MetricsBridgeClient."""
    print("\n" + "=" * 60)
    print("Metrics Bridge Client Demo")
    print("=" * 60)

    # Using context manager for automatic cleanup
    with MetricsBridgeClient() as client:
        def on_message(msg):
            print(f"[MetricsBridge] Metric from {msg.source_topic}:")
            try:
                metric = json.loads(msg.payload.decode())
                print(f"  Name: {metric.get('metric_name')}")
                print(f"  Value: {metric.get('value')}")
                print(f"  Tags: {metric.get('tags')}")
            except Exception:
                print(f"  Raw: {msg.payload.decode()}")

        client.on_message = on_message

        try:
            print("Connecting as metrics bridge...")
            client.connect("localhost:50051")
            print("Metrics bridge connected! Listening for metrics...")

            while True:
                time.sleep(1)
        except AetherError as e:
            print(f"[MetricsBridge] Error: {e}")
        except KeyboardInterrupt:
            pass

    print("Metrics bridge disconnected.")


def demo_targeted_task():
    """Demonstrate creating a targeted task (Phase 6 feature)."""
    print("\n" + "=" * 60)
    print("Targeted Task Creation Demo")
    print("=" * 60)

    # Using context manager for automatic cleanup
    with AgentClient(
        workspace="default",
        implementation="task-creator",
        specifier="01"
    ) as client:
        client.on_message = lambda msg: print(f"[TaskCreator] Message: {msg.payload.decode()}")

        try:
            print("Connecting...")
            client.connect("localhost:50051")

            time.sleep(1)

            # Create a targeted task that will be assigned to a specific agent
            # If the target agent is offline, this will trigger orchestration
            print("\n--- Creating targeted task ---")
            client.create_task(
                task_type="specialized-processing",
                workspace="default",
                assignment_mode=TARGETED,
                target_agent_id="ag::default::worker::specialist-01",
                launch_param_overrides={
                    "memory": "4G",
                    "gpu": "true"
                },
                metadata={
                    "priority": "high",
                    "deadline": "2024-12-31T23:59:59Z"
                }
            )
            print("Targeted task created! If agent is offline, orchestrator will spin it up.")

            time.sleep(2)
        except AetherError as e:
            print(f"[TaskCreator] Error: {e}")

    print("Done.")


def demo_error_handling():
    """Demonstrate the custom exception hierarchy (new feature)."""
    print("\n" + "=" * 60)
    print("Custom Exception Handling Demo")
    print("=" * 60)

    print("\nAether provides a structured exception hierarchy:")
    print("  AetherError (base)")
    print("  ├── ConnectionError")
    print("  │   ├── ConnectionClosedError")
    print("  │   └── ReconnectionError")
    print("  ├── AuthenticationError")
    print("  ├── PermissionDeniedError")
    print("  ├── DuplicateIdentityError")
    print("  ├── TimeoutError")
    print("  ├── InvalidArgumentError")
    print("  ├── NotFoundError")
    print("  ├── NotImplementedError")
    print("  ├── MessageError")
    print("  ├── KVOperationError")
    print("  └── CheckpointError")

    print("\nExample error handling pattern:")
    print("""
    from scitrera_aether_client import (
        AgentClient,
        AetherError,
        ConnectionError,
        AuthenticationError,
        DuplicateIdentityError,
        ReconnectionError,
    )

    client = AgentClient("ws", "impl", "spec")
    try:
        client.connect("localhost:50051")
        # ... do work ...
    except AuthenticationError as e:
        print(f"Auth failed: {e}")
    except DuplicateIdentityError as e:
        print(f"Identity {e.identity} already connected")
    except ReconnectionError as e:
        print(f"Failed to reconnect after {e.attempts} attempts")
    except ConnectionError as e:
        print(f"Connection failed: {e}")
    except AetherError as e:
        print(f"Other Aether error: {e}")
    finally:
        client.close()
    """)


def main():
    """Main entry point - run the agent demo by default."""
    import sys

    demos = {
        "agent": demo_agent_client,
        "context": demo_agent_context_manager,
        "tls": demo_tls_configuration,
        "orchestrator": demo_orchestrator_client,
        "multiprocess": demo_multiprocess_orchestrator,
        "workflow": demo_workflow_engine,
        "metrics": demo_metrics_bridge,
        "targeted": demo_targeted_task,
        "errors": demo_error_handling,
    }

    if len(sys.argv) > 1:
        demo_name = sys.argv[1].lower()
        if demo_name in demos:
            demos[demo_name]()
        else:
            print(f"Unknown demo: {demo_name}")
            print(f"Available demos: {', '.join(demos.keys())}")
            sys.exit(1)
    else:
        print("Aether Client Demo")
        print("==================")
        print("\nUsage: python example.py [demo]")
        print("\nAvailable demos:")
        print("  agent       - Agent client with messaging, KV, and task creation")
        print("  context     - Agent client using context manager (auto-cleanup)")
        print("  tls         - TLS/mTLS configuration options")
        print("  orchestrator - Orchestrator client for managing agent lifecycle")
        print("  multiprocess - MultiprocessOrchestrator for subprocess management")
        print("  workflow    - Workflow engine for processing events")
        print("  metrics     - Metrics bridge for collecting telemetry")
        print("  targeted    - Targeted task creation with orchestration")
        print("  errors      - Custom exception hierarchy demonstration")
        print("\nRunning default 'agent' demo...\n")
        demo_agent_client()


if __name__ == "__main__":
    main()
