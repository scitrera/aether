// Package aether provides a Go SDK for connecting to the Aether distributed
// control plane. It wraps the gRPC client with higher-level abstractions for
// agent connection, message handling, and KV operations.
//
// # Overview
//
// Aether is a distributed control plane for routing structured messages,
// tracking tasks, and managing connection lifecycles. This SDK provides
// idiomatic Go clients for agents, tasks, users, and other principal types.
//
// # Principal Types
//
// The SDK provides specialized client types for each principal:
//
//   - [AgentClient]: For persistent agents with workspace/impl/spec identity
//   - [TaskClient]: For unique or non-unique tasks with automatic broadcast subscription
//   - [UserClient]: For user connections with window-based identity
//   - [OrchestratorClient]: For compute orchestration and task assignment
//   - [WorkflowEngineClient]: For event processing
//   - [MetricsBridgeClient]: For telemetry collection
//
// # Connection Management
//
// All clients support automatic reconnection with exponential backoff,
// context-based cancellation, and graceful shutdown:
//
//	client, err := aether.NewAgentClient(aether.AgentOptions{
//	    ClientOptions: aether.ClientOptions{
//	        ServerAddr: "localhost:50051",
//	        Connection: aether.ConnectionOptions{AutoReconnect: true},
//	    },
//	    Workspace:      "production",
//	    Implementation: "my-agent",
//	    Specifier:      "instance-1",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err := client.Connect(context.Background()); err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
// # Message Handling
//
// Register handlers to process incoming messages:
//
//	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
//	    fmt.Printf("Received: %s\n", msg.Payload)
//	    return nil
//	})
//
// # KV Store
//
// Access the hierarchical key-value store (async put, sync get):
//
//	kv := client.KV()
//	err = kv.Put("my-key", []byte("my-value"), aether.KVScopeGlobal, "", "", 3600)
//
//	resp, err := kv.GetSync(ctx, aether.KVGetOptions{
//	    Key:   "my-key",
//	    Scope: aether.KVScopeGlobal,
//	})
//
// # Key Architectural Principle
//
// The connection itself IS the distributed lock AND the heartbeat. When the
// gRPC stream closes, the identity lock is immediately released. No separate
// heartbeat API exists.
package aether
