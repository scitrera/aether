// Package storage_test contains a cross-layer smoke test that verifies the
// JetStream-backed implementations of the four core storage-adjacent interfaces
// (SessionManager, MessageRouter, KVReadWriter, CheckpointManager) can be
// constructed and exercise their basic contract against a single embedded NATS
// server.
//
// # Why this file lives here
//
// The five domain-specific conformance suites (acl, audit, registry, tasks,
// workflow) test SQL-backed stores — Postgres and SQLite — against a shared
// "Store" contract. None of those domains have a JetStream-backed Store
// implementation; JetStream is the cross-gateway *sync* layer, not a
// replacement for per-node SQLite durability. Therefore NO jetstreamFactory
// subtest is added to those individual suites.
//
// The JetStream implementations live in sibling packages:
//
//   - internal/state.JetStreamSession    → gateway.SessionManager
//   - internal/router.JetStreamRouter    → gateway.MessageRouter
//   - internal/kv.JetStreamKVStore       → gateway.KVReadWriter
//   - internal/checkpoint.JetStreamCheckpointStore → gateway.CheckpointManager
//
// This smoke test exercises all four against a single ephemeral embedded NATS
// server (port -1 / OS-assigned) and proves the JetStream stack is wired
// correctly at the storage-layer scope.
//
// Skipped under -short to keep CI fast.
package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/checkpoint"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/router"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/pkg/models"
)

// startSmokeJetStream boots an in-process NATS server with JetStream enabled
// on an OS-assigned port (Port: -1) and returns a connected JetStream context.
// Cleanup is registered via t.Cleanup so the server is shut down after the
// test regardless of pass/fail.
func startSmokeJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	opts := &natsserver.Options{
		Host:               "127.0.0.1",
		Port:               -1, // ephemeral port
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 64 * 1024 * 1024,
		JetStreamMaxStore:  256 * 1024 * 1024,
		NoSigs:             true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server new: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats server did not become ready within 10s")
	}
	conn, err := natsgo.Connect("", natsgo.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		t.Fatalf("jetstream new: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Drain()
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return js
}

// TestJetStreamStackSmoke verifies that the four JetStream-backed
// implementations can be constructed and pass basic round-trip assertions
// against a single embedded NATS server. It is the canonical proof that the
// JetStream storage layer is correctly wired at the storage-package scope.
//
// This test is skipped under -short.
func TestJetStreamStackSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream smoke test in -short mode")
	}

	ctx := context.Background()
	js := startSmokeJetStream(t)

	t.Run("SessionManager_AcquireRelease", func(t *testing.T) {
		sess, err := state.NewJetStreamSession(ctx, js, state.JetStreamSessionConfig{Replicas: 1})
		if err != nil {
			t.Fatalf("NewJetStreamSession: %v", err)
		}

		id := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "smoke",
			Implementation: "test-agent",
			Specifier:      fmt.Sprintf("smoke-%d", time.Now().UnixNano()),
		}

		r, err := sess.AcquireOrResumeLock(ctx, id, "sess-smoke-1", "", 0, state.ConnectMeta{})
		acquired, resumed, forced := r.Acquired, r.Resumed, r.Forced
		if err != nil {
			t.Fatalf("AcquireOrResumeLock: %v", err)
		}
		if !acquired || resumed || forced {
			t.Fatalf("expected acquired=true resumed=false forced=false, got (%v,%v,%v)", acquired, resumed, forced)
		}

		active, err := sess.IsActive(ctx, id.String())
		if err != nil {
			t.Fatalf("IsActive: %v", err)
		}
		if !active {
			t.Fatal("expected IsActive=true after acquire")
		}

		if err := sess.ReleaseLock(ctx, id, "sess-smoke-1"); err != nil {
			t.Fatalf("ReleaseLock: %v", err)
		}

		active, err = sess.IsActive(ctx, id.String())
		if err != nil {
			t.Fatalf("IsActive post-release: %v", err)
		}
		if active {
			t.Fatal("expected IsActive=false after release")
		}
	})

	t.Run("MessageRouter_PublishSubscribe", func(t *testing.T) {
		r, err := router.NewJetStreamRouter(js, 1, nil)
		if err != nil {
			t.Fatalf("NewJetStreamRouter: %v", err)
		}

		// Aether topics use "::" as the segment separator. The router encodes
		// them via natscodec before mapping to a JetStream stream. An ag topic
		// must have the form "ag::{workspace}::{impl}::{spec}" so natscodec
		// produces "ag.{workspace}.{impl}.{spec}" which matches the "ag.>"
		// subject filter on the "ag" JetStream stream.
		topic := fmt.Sprintf("ag::smoke::test-router::smoke-%d", time.Now().UnixNano())
		received := make(chan []byte, 1)

		unsub, err := r.Subscribe(topic, func(payload []byte) {
			select {
			case received <- payload:
			default:
			}
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		defer unsub()

		want := []byte("smoke-payload")
		if err := r.Publish(ctx, topic, want); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		select {
		case got := <-received:
			if string(got) != string(want) {
				t.Fatalf("received %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for published message")
		}
	})

	t.Run("KVStore_SetGet", func(t *testing.T) {
		kvStore, err := kv.NewJetStreamKVStore(ctx, js)
		if err != nil {
			t.Fatalf("NewJetStreamKVStore: %v", err)
		}

		agent := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "smoke",
			Implementation: "kv-agent",
			Specifier:      "default",
		}

		key := fmt.Sprintf("smoke-key-%d", time.Now().UnixNano())
		value := "smoke-value"

		if err := kvStore.Set(ctx, agent, kv.ScopeGlobal, key, value, "", "", 0); err != nil {
			t.Fatalf("Set: %v", err)
		}

		got, err := kvStore.Get(ctx, agent, kv.ScopeGlobal, key, "", "")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got != value {
			t.Fatalf("Get returned %q, want %q", got, value)
		}

		if err := kvStore.Delete(ctx, agent, kv.ScopeGlobal, key, "", ""); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		_, err = kvStore.Get(ctx, agent, kv.ScopeGlobal, key, "", "")
		if err == nil {
			t.Fatal("Get after Delete: expected error, got nil")
		}
	})

	t.Run("CheckpointStore_SaveLoad", func(t *testing.T) {
		cpStore, err := checkpoint.NewJetStreamCheckpointStore(ctx, js)
		if err != nil {
			t.Fatalf("NewJetStreamCheckpointStore: %v", err)
		}

		id := models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "smoke",
			Implementation: "cp-agent",
			Specifier:      fmt.Sprintf("smoke-%d", time.Now().UnixNano()),
		}

		key := "smoke-checkpoint"
		payload := []byte(`{"step":"one","hint":"smoke"}`)

		if err := cpStore.Save(ctx, id, key, payload, 10*time.Minute); err != nil {
			t.Fatalf("Save: %v", err)
		}

		cp, err := cpStore.Load(ctx, id, key)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if string(cp.Data) != string(payload) {
			t.Fatalf("Load returned %q, want %q", cp.Data, payload)
		}

		if err := cpStore.Delete(ctx, id, key); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		// Load returns nil, nil when the checkpoint does not exist (not an error).
		missing, err := cpStore.Load(ctx, id, key)
		if err != nil {
			t.Fatalf("Load after Delete: unexpected error: %v", err)
		}
		if missing != nil {
			t.Fatalf("Load after Delete: expected nil checkpoint, got %+v", missing)
		}
	})
}
