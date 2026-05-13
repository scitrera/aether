#!/usr/bin/env python3
"""
Aether Async Client Example

This example demonstrates the async variant of the Aether Python client,
including:
- Async Agent, Task, User, Orchestrator, WorkflowEngine, and MetricsBridge clients
- Async message sending with different message types
- Async KV operations with scope support
- Task creation with assignment modes
- Async context manager support
- Custom exception handling
- TLS/mTLS configuration
- Connection and disconnection callbacks
"""

import asyncio
import json
import time as time_module

from scitrera_aether_client import (
    # Async client classes
    AsyncAgentClient,
    AsyncTaskClient,
    AsyncUserClient,
    AsyncOrchestratorClient,
    AsyncWorkflowEngineClient,
    AsyncMetricsBridgeClient,
    # Message type constants
    CHAT,
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


async def demo_agent_client():
    """Demonstrate the AsyncAgentClient with all features."""
    print("\n" + "=" * 60)
    print("Async Agent Client Demo")
    print("=" * 60)

    client = AsyncAgentClient(
        workspace="default",
        implementation="python-async-demo",
        specifier="agent-01"
    )

    # Callbacks can be sync or async functions
    async def on_message(msg):
        print(f"[Agent] Message from {msg.source_topic}: {msg.payload.decode()}")

    def on_config(config):
        # Sync callback also works
        print(f"[Agent] Config snapshot received:")
        print(f"  Workspace KV: {dict(config.kv)}")
        print(f"  Global KV: {dict(config.global_kv)}")

    async def on_task_assignment(assignment):
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

    async def on_connect():
        print("[Agent] Connected callback triggered!")

    async def on_disconnect(reason):
        print(f"[Agent] Disconnected: {reason}")

    def on_error(error):
        print(f"[Agent] Error: {error.code} - {error.message}")

    client.on_message = on_message
    client.on_config = on_config
    client.on_task_assignment = on_task_assignment
    client.on_kv_response = on_kv_response
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_error = on_error

    try:
        print("Connecting to gateway...")
        await client.connect("localhost:50051")
        print("Connected!")

        await asyncio.sleep(1)

        # Demo: Send a message to ourselves
        print("\n--- Sending message to self ---")
        await client.send_message_to_agent(
            workspace="default",
            implementation="python-async-demo",
            specifier="agent-01",
            payload=b"Hello from async Python agent!"
        )

        await asyncio.sleep(0.5)

        # Demo: KV operations with different scopes
        print("\n--- KV Operations ---")

        # Global scope
        print("Storing value in global scope...")
        await client.kv_put_nowait(
            key="async-demo/setting",
            value=b"global-value",
            scope="global"
        )

        await asyncio.sleep(0.3)

        # Workspace scope
        print("Storing value in workspace scope...")
        await client.kv_put_nowait(
            key="async-demo/workspace-setting",
            value=b"workspace-value",
            scope="workspace",
            workspace="default"
        )

        await asyncio.sleep(0.3)

        # Retrieve a value synchronously (with await)
        print("Getting value from global scope (sync wait)...")
        response = await client.kv_get(key="async-demo/setting", scope="global", timeout=5.0)
        if response:
            print(f"  Got value: {response.value}")
        else:
            print("  Timeout waiting for KV response")

        await asyncio.sleep(0.5)

        # Demo: Create a self-assigned task
        print("\n--- Task Creation (Self-Assign) ---")
        await client.create_task(
            task_type="async-data-processing",
            workspace="default",
            assignment_mode=SELF_ASSIGN,
            metadata={"source": "python-async-demo", "priority": "normal"}
        )

        await asyncio.sleep(0.5)

        # Demo: Send events and metrics
        print("\n--- Events and Metrics ---")
        event_payload = json.dumps({
            "event_type": "async_agent_started",
            "agent_id": "python-async-demo.agent-01",
            "timestamp": time_module.time()
        }).encode()
        await client.send_event(event_payload)
        print("Sent event to workflow engine")

        metric = new_metric().trace("async-demo-trace-1").add("async_messages_processed", "counter", 42.0).tag("agent", "python-async-demo").build()
        await client.send_metric(metric)
        print("Sent metric to metrics bridge")

        await asyncio.sleep(1)

        print("\nWaiting for messages (Ctrl+C to exit)...")
        await client.wait_until_disconnected()

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
    except asyncio.CancelledError:
        pass
    finally:
        await client.close()
        print("Disconnected.")


async def demo_agent_context_manager():
    """Demonstrate using AsyncAgentClient as a context manager."""
    print("\n" + "=" * 60)
    print("Async Agent Client (Context Manager) Demo")
    print("=" * 60)

    async with AsyncAgentClient(
        workspace="default",
        implementation="python-async-demo",
        specifier="context-agent"
    ) as client:
        client.on_message = lambda msg: print(f"[Agent] {msg.payload.decode()}")

        try:
            await client.connect("localhost:50051")
            print("Connected via context manager!")

            await client.send_message_to_agent(
                workspace="default",
                implementation="python-async-demo",
                specifier="context-agent",
                payload=b"Hello via context manager!"
            )

            await asyncio.sleep(1)
            print("Exiting context manager (auto-close)...")

        except AetherError as e:
            print(f"[Agent] Error: {e}")

    print("Connection closed automatically.")


async def demo_tls_configuration():
    """Demonstrate TLS/mTLS configuration (new feature)."""
    print("\n" + "=" * 60)
    print("Async TLS Configuration Demo")
    print("=" * 60)

    # Example 1: Basic TLS (server verification only)
    print("\n--- Basic TLS Configuration ---")
    print("Creating async client with TLS enabled...")

    # Note: These paths are examples - replace with actual certificate paths
    _tls_client = AsyncAgentClient(
        workspace="default",
        implementation="python-async-demo",
        specifier="tls-agent",
        # Enable TLS
        tls_enabled=True,
        # Root CA certificate (verify server identity)
        tls_root_cert_path="/path/to/ca.crt",  # or use tls_root_cert=bytes
    )
    print("  TLS client created (server verification)")

    # Example 2: Mutual TLS (mTLS) with client certificates
    print("\n--- mTLS Configuration ---")
    print("Creating async client with mTLS enabled...")

    _mtls_client = AsyncAgentClient(
        workspace="default",
        implementation="python-async-demo",
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


async def demo_orchestrator_client():
    """Demonstrate the AsyncOrchestratorClient."""
    print("\n" + "=" * 60)
    print("Async Orchestrator Client Demo")
    print("=" * 60)

    client = AsyncOrchestratorClient(
        implementation="async-demo-orchestrator",
        supported_profiles=["docker", "kubernetes"]
    )

    async def on_message(msg):
        print(f"[Orchestrator] Received from {msg.source_topic}:")
        try:
            data = json.loads(msg.payload.decode())
            print(f"  Startup request: {data}")
        except Exception:
            print(f"  Raw: {msg.payload.decode()}")

    async def on_task_assignment(assignment):
        print(f"[Orchestrator] Task assignment received:")
        print(f"  Task ID: {assignment.task_id}")
        print(f"  Task Type: {assignment.task_type}")

    client.on_message = on_message
    client.on_task_assignment = on_task_assignment

    try:
        print("Connecting as async orchestrator...")
        await client.connect("localhost:50051")
        print("Orchestrator connected! Listening for startup requests...")

        await client.wait_until_disconnected()
    except AetherError as e:
        print(f"[Orchestrator] Error: {e}")
    except asyncio.CancelledError:
        pass
    finally:
        await client.close()
        print("Orchestrator disconnected.")


async def demo_workflow_engine():
    """Demonstrate the AsyncWorkflowEngineClient."""
    print("\n" + "=" * 60)
    print("Async Workflow Engine Client Demo")
    print("=" * 60)

    # Using async context manager for automatic cleanup
    async with AsyncWorkflowEngineClient() as client:
        async def on_message(msg):
            print(f"[WorkflowEngine] Event from {msg.source_topic}:")
            try:
                event = json.loads(msg.payload.decode())
                print(f"  Event type: {event.get('event_type')}")
                print(f"  Data: {event}")
            except Exception:
                print(f"  Raw: {msg.payload.decode()}")

        client.on_message = on_message

        try:
            print("Connecting as async workflow engine...")
            await client.connect("localhost:50051")
            print("Workflow engine connected! Listening for events...")

            await asyncio.sleep(1)
            print("\n--- Sending command to agent ---")
            command = json.dumps({
                "command": "start_async_processing",
                "params": {"batch_size": 100}
            }).encode()
            await client.send_command_to_agent(
                workspace="default",
                implementation="python-async-demo",
                specifier="agent-01",
                payload=command
            )
            print("Command sent!")

            await client.wait_until_disconnected()
        except AetherError as e:
            print(f"[WorkflowEngine] Error: {e}")
        except asyncio.CancelledError:
            # Ignore cancellation to allow graceful shutdown of the workflow engine demo.
            pass

    print("Workflow engine disconnected.")


async def demo_metrics_bridge():
    """Demonstrate the AsyncMetricsBridgeClient."""
    print("\n" + "=" * 60)
    print("Async Metrics Bridge Client Demo")
    print("=" * 60)

    # Using async context manager for automatic cleanup
    async with AsyncMetricsBridgeClient() as client:
        async def on_message(msg):
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
            print("Connecting as async metrics bridge...")
            await client.connect("localhost:50051")
            print("Metrics bridge connected! Listening for metrics...")

            await client.wait_until_disconnected()
        except AetherError as e:
            print(f"[MetricsBridge] Error: {e}")
        except asyncio.CancelledError:
            # Ignore cancellation to allow graceful shutdown of the metrics bridge demo.
            pass

    print("Metrics bridge disconnected.")


async def demo_targeted_task():
    """Demonstrate creating a targeted task."""
    print("\n" + "=" * 60)
    print("Async Targeted Task Creation Demo")
    print("=" * 60)

    # Using async context manager for automatic cleanup
    async with AsyncAgentClient(
        workspace="default",
        implementation="async-task-creator",
        specifier="01"
    ) as client:
        client.on_message = lambda msg: print(f"[TaskCreator] Message: {msg.payload.decode()}")

        try:
            print("Connecting...")
            await client.connect("localhost:50051")

            await asyncio.sleep(1)

            print("\n--- Creating targeted task ---")
            await client.create_task(
                task_type="async-specialized-processing",
                workspace="default",
                assignment_mode=TARGETED,
                target_agent_id="ag::default::async-worker::specialist-01",
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

            await asyncio.sleep(2)
        except AetherError as e:
            print(f"[TaskCreator] Error: {e}")

    print("Done.")


async def demo_concurrent_clients():
    """Demonstrate running multiple async clients concurrently."""
    print("\n" + "=" * 60)
    print("Concurrent Async Clients Demo")
    print("=" * 60)

    # Create multiple clients
    agent1 = AsyncAgentClient("default", "concurrent-demo", "agent-01")
    agent2 = AsyncAgentClient("default", "concurrent-demo", "agent-02")

    agent1.on_message = lambda msg: print(f"[Agent-01] {msg.payload.decode()}")
    agent2.on_message = lambda msg: print(f"[Agent-02] {msg.payload.decode()}")

    try:
        # Connect both concurrently
        print("Connecting both agents concurrently...")
        await asyncio.gather(
            agent1.connect("localhost:50051"),
            agent2.connect("localhost:50051")
        )
        print("Both agents connected!")

        await asyncio.sleep(0.5)

        # Send messages between them
        print("\n--- Cross-agent messaging ---")
        await agent1.send_message_to_agent("default", "concurrent-demo", "agent-02",
                                           b"Hello from agent-01!")
        await agent2.send_message_to_agent("default", "concurrent-demo", "agent-01",
                                           b"Hello from agent-02!")

        await asyncio.sleep(1)

    except AetherError as e:
        print(f"Error: {e}")
    finally:
        # Close both
        await asyncio.gather(
            agent1.close(),
            agent2.close()
        )
        print("Both agents disconnected.")


async def demo_error_handling():
    """Demonstrate the custom exception hierarchy with async clients."""
    print("\n" + "=" * 60)
    print("Async Custom Exception Handling Demo")
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

    print("\nExample async error handling pattern:")
    print("""
    from scitrera_aether_client import (
        AsyncAgentClient,
        AetherError,
        ConnectionError,
        AuthenticationError,
        DuplicateIdentityError,
        ReconnectionError,
    )

    async def run_agent():
        async with AsyncAgentClient("ws", "impl", "spec") as client:
            try:
                await client.connect("localhost:50051")
                # ... do async work ...
                await client.wait_until_disconnected()
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
            except asyncio.CancelledError:
                print("Task cancelled")
    """)


async def main():
    """Main entry point - run the agent demo by default."""
    import sys

    demos = {
        "agent": demo_agent_client,
        "context": demo_agent_context_manager,
        "tls": demo_tls_configuration,
        "orchestrator": demo_orchestrator_client,
        "workflow": demo_workflow_engine,
        "metrics": demo_metrics_bridge,
        "targeted": demo_targeted_task,
        "concurrent": demo_concurrent_clients,
        "errors": demo_error_handling,
    }

    if len(sys.argv) > 1:
        demo_name = sys.argv[1].lower()
        if demo_name in demos:
            await demos[demo_name]()
        else:
            print(f"Unknown demo: {demo_name}")
            print(f"Available demos: {', '.join(demos.keys())}")
            sys.exit(1)
    else:
        print("Aether Async Client Demo")
        print("========================")
        print("\nUsage: python example_async.py [demo]")
        print("\nAvailable demos:")
        print("  agent       - Async agent client with messaging, KV, and task creation")
        print("  context     - Agent client using async context manager")
        print("  tls         - TLS/mTLS configuration options")
        print("  orchestrator - Async orchestrator for managing agent lifecycle")
        print("  workflow    - Async workflow engine for processing events")
        print("  metrics     - Async metrics bridge for collecting telemetry")
        print("  targeted    - Async targeted task creation with orchestration")
        print("  concurrent  - Multiple async clients running concurrently")
        print("  errors      - Custom exception hierarchy demonstration")
        print("\nRunning default 'agent' demo...\n")
        await demo_agent_client()


if __name__ == "__main__":
    asyncio.run(main())
