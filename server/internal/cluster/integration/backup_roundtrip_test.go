package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/cluster/backup"
)

// testLogger drains log lines into t.Log so failed integration tests surface
// ordering.
type testLogger struct{ t *testing.T }

func (l testLogger) Infof(format string, args ...any)  { l.t.Logf("INFO "+format, args...) }
func (l testLogger) Warnf(format string, args ...any)  { l.t.Logf("WARN "+format, args...) }
func (l testLogger) Errorf(format string, args ...any) { l.t.Logf("ERR  "+format, args...) }

// TestClusterIntegration_BackupRoundtrip_PerDomain exercises the full
// backup→wipe→restore cycle for each of the policy domain kinds (stream + KV)
// against the 3-node embedded cluster using LocalFileStorage as the object
// store. This is the cluster-scope generalisation of
// TestRestoreFromS3_RoundTrip / TestRestoreFromS3_KV_RoundTrip in
// internal/cluster/backup/backup_test.go — the difference is the underlying
// JetStream is now replicated across three nodes so we exercise the
// snapshot/restore code paths against a real cluster, not a single embedded
// server.
//
// For each domain:
//  1. Seed it with known data.
//  2. Force a snapshot via BackupCoordinator.tickOnce().
//  3. Wipe the stream / KV bucket cluster-wide (DeleteStream / DeleteKeyValue).
//  4. Call RestoreFromS3 (which reads from LocalFileStorage) on the cluster.
//  5. Assert the data matches the original seed.
func TestClusterIntegration_BackupRoundtrip_PerDomain(t *testing.T) {
	c := setupCluster3(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use node-0 as the leader/operator for the backup coordinator. The
	// coordinator's leader-election guarantees only one of the cluster's
	// coordinators runs at a time; for this test we instantiate one
	// coordinator and drive it directly.
	js := c.Node(0).JetStream()

	// --- Phase: seed both domains with known data ---

	// Stream domain: create a stream and publish known subject/body pairs.
	const streamName = "orders_cluster"
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName + ".>"},
		Replicas: 3,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	wantStream := map[string]string{
		streamName + ".created": "alpha",
		streamName + ".updated": "beta",
		streamName + ".deleted": "gamma",
	}
	for sub, body := range wantStream {
		if _, err := js.Publish(ctx, sub, []byte(body)); err != nil {
			t.Fatalf("publish %s: %v", sub, err)
		}
	}

	// KV domain: create a KV bucket and put known keys.
	const kvBucket = "settings_cluster"
	kvHandle, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   kvBucket,
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("create kv: %v", err)
	}
	wantKV := map[string]string{
		"feature_a": "on",
		"feature_b": "off",
		"theme":     "midnight",
	}
	for k, v := range wantKV {
		if _, err := kvHandle.Put(ctx, k, []byte(v)); err != nil {
			t.Fatalf("kv put %s: %v", k, err)
		}
	}

	// --- Phase: configure the backup coordinator with LocalFileStorage ---

	storageRoot := t.TempDir()
	storage, err := backup.NewLocalFileStorage(storageRoot)
	if err != nil {
		t.Fatalf("local storage: %v", err)
	}
	policies := []backup.BackupPolicy{
		{
			Domain:       streamName,
			Kind:         backup.DomainKindStream,
			MinInterval:  50 * time.Millisecond,
			S3Prefix:     "cluster-rt",
			ReplicaCount: 3,
		},
		{
			Domain:       kvBucket,
			Kind:         backup.DomainKindKV,
			MinInterval:  50 * time.Millisecond,
			S3Prefix:     "cluster-rt",
			ReplicaCount: 3,
		},
	}
	coord, err := backup.NewBackupCoordinator(js, storage, policies, "node-a-coord", testLogger{t}, backup.WithLeaderTTL(2*time.Second))
	if err != nil {
		t.Fatalf("new coord: %v", err)
	}

	// Run the coordinator in a goroutine. It will run periodic ticks (cadence
	// derived from MinInterval=50ms above) and we wait until both domains have
	// produced at least one .bin upload before cancelling. The coordinator's
	// Run() returns ctx.Err() on cancellation, which is expected here.
	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = coord.Run(runCtx)
	}()
	// Belt-and-suspenders: ensure the goroutine winds down even on a
	// fatal-path return below.
	t.Cleanup(func() {
		runCancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		streamObjs, _ := storage.List(ctx, fmt.Sprintf("cluster-rt/%s", streamName))
		kvObjs, _ := storage.List(ctx, fmt.Sprintf("cluster-rt/%s", kvBucket))
		if countBinObjects(streamObjs) > 0 && countBinObjects(kvObjs) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	runCancel()
	<-runDone

	// suppress unused warning when Run is the only consumer of coord
	_ = coord

	streamObjs, _ := storage.List(ctx, fmt.Sprintf("cluster-rt/%s", streamName))
	kvObjs, _ := storage.List(ctx, fmt.Sprintf("cluster-rt/%s", kvBucket))
	if countBinObjects(streamObjs) == 0 {
		t.Fatalf("stream domain: no .bin objects produced in %s", storageRoot)
	}
	if countBinObjects(kvObjs) == 0 {
		t.Fatalf("kv domain: no .bin objects produced in %s", storageRoot)
	}

	// --- Phase: wipe the cluster-side state for both domains ---

	if err := js.DeleteStream(ctx, streamName); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	if err := js.DeleteKeyValue(ctx, kvBucket); err != nil {
		t.Fatalf("delete kv: %v", err)
	}

	// --- Phase: restore both domains from LocalFileStorage ---

	if err := backup.RestoreFromS3(ctx, js, storage, policies[0], testLogger{t}); err != nil {
		t.Fatalf("restore stream: %v", err)
	}
	if err := backup.RestoreFromS3(ctx, js, storage, policies[1], testLogger{t}); err != nil {
		t.Fatalf("restore kv: %v", err)
	}

	// --- Phase: assert data integrity ---

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("get stream post-restore: %v", err)
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("ordered consumer post-restore: %v", err)
	}

	gotStream := make(map[string]string)
	streamDeadline := time.Now().Add(5 * time.Second)
	for len(gotStream) < len(wantStream) && time.Now().Before(streamDeadline) {
		msg, err := cons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			continue
		}
		gotStream[msg.Subject()] = string(msg.Data())
	}
	if len(gotStream) != len(wantStream) {
		t.Fatalf("stream restore: expected %d msgs, got %d: %+v", len(wantStream), len(gotStream), gotStream)
	}
	for sub, body := range wantStream {
		if gotStream[sub] != body {
			t.Errorf("stream subject %s: want %q, got %q", sub, body, gotStream[sub])
		}
	}

	kvAfter, err := js.KeyValue(ctx, kvBucket)
	if err != nil {
		t.Fatalf("get kv post-restore: %v", err)
	}
	for k, v := range wantKV {
		entry, err := kvAfter.Get(ctx, k)
		if err != nil {
			t.Errorf("kv get %s post-restore: %v", k, err)
			continue
		}
		if string(entry.Value()) != v {
			t.Errorf("kv key %s: want %q, got %q", k, v, string(entry.Value()))
		}
	}
}

// countBinObjects counts ObjectInfo entries whose key ends in ".bin".
// Repeats backup_test.go's countSuffix helper but inlined here so the
// integration package does not have to import test-only symbols.
func countBinObjects(objs []backup.ObjectInfo) int {
	n := 0
	for _, o := range objs {
		if len(o.Key) >= 4 && o.Key[len(o.Key)-4:] == ".bin" {
			n++
		}
	}
	return n
}
