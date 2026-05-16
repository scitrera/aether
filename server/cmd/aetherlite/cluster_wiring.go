// cluster_wiring.go — helpers used by main.go when AETHERLITE_CLUSTER_MODE=true.
//
// This file contains:
//   - The cluster-mode environment variable parser (clusterEnv).
//   - A zerolog-to-cluster/nats.Logger + cluster/backup.Logger adapter.
//   - defaultBackupPolicies(), which returns the tiered backup table from the plan.
//   - parseHAMode() and chooseDispatcher() for declaring config-derived choices.
//
// Keeping these helpers out of main.go keeps the cluster-mode branch in
// main.go skimmable and bisectable.

package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/cluster/backup"
	clusternats "github.com/scitrera/aether/internal/cluster/nats"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/registry"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
)

// clusterEnv aggregates every AETHERLITE_* env var that gates cluster-mode
// behavior. Parsed once at startup so main.go can log effective values in one
// place and avoid re-reading env in deeper helpers.
type clusterEnv struct {
	Enabled         bool
	Peers           []string
	HAMode          clusternats.HAMode
	HAModeRaw       string
	NATSClientPort  int
	NATSClusterPort int
	S3Bucket        string
	S3Prefix        string
	S3Region        string
	S3Endpoint      string
	S3AccessKey     string
	S3SecretKey     string
	S3ForcePath     bool
	RestoreFromS3   bool
	Dispatcher      string // "polling" or "jetstream"
}

// readClusterEnv pulls every AETHERLITE_* cluster-mode env var into a
// clusterEnv struct. Defaults match the plan's "single-node, embedded NATS,
// local backups" topology so an operator can flip AETHERLITE_CLUSTER_MODE=true
// and get a working dev/test setup without any further configuration.
func readClusterEnv() clusterEnv {
	enabled := config.EnvBool("AETHERLITE_CLUSTER_MODE", false)
	peersRaw := config.EnvStr("AETHERLITE_CLUSTER_PEERS", "")
	var peers []string
	if peersRaw != "" {
		for _, p := range strings.Split(peersRaw, ",") {
			if t := strings.TrimSpace(p); t != "" {
				peers = append(peers, t)
			}
		}
	}
	haModeRaw := config.EnvStr("AETHERLITE_HA_MODE", "auto")
	haMode := parseHAMode(haModeRaw)

	dispatcher := strings.ToLower(strings.TrimSpace(config.EnvStr("AETHER_DISPATCHER", "")))
	if dispatcher == "" {
		// In cluster mode, default to jetstream dispatcher. Otherwise polling.
		if enabled {
			dispatcher = "jetstream"
		} else {
			dispatcher = "polling"
		}
	}

	return clusterEnv{
		Enabled:         enabled,
		Peers:           peers,
		HAMode:          haMode,
		HAModeRaw:       haModeRaw,
		NATSClientPort:  config.EnvInt("AETHERLITE_NATS_CLIENT_PORT", 0),
		NATSClusterPort: config.EnvInt("AETHERLITE_NATS_CLUSTER_PORT", 6222),
		S3Bucket:        config.EnvStr("AETHERLITE_S3_BUCKET", ""),
		S3Prefix:        config.EnvStr("AETHERLITE_S3_PREFIX", "aetherlite/"),
		S3Region:        config.EnvStr("AETHERLITE_S3_REGION", "us-east-1"),
		S3Endpoint:      config.EnvStr("AETHERLITE_S3_ENDPOINT", ""),
		S3AccessKey:     config.EnvStr("AETHERLITE_S3_ACCESS_KEY", ""),
		S3SecretKey:     config.EnvStr("AETHERLITE_S3_SECRET_KEY", ""),
		S3ForcePath:     config.EnvBool("AETHERLITE_S3_FORCE_PATH_STYLE", false),
		RestoreFromS3:   config.EnvBool("AETHERLITE_RESTORE_FROM_S3", false),
		Dispatcher:      dispatcher,
	}
}

// parseHAMode maps the AETHERLITE_HA_MODE string into the cluster/nats enum.
// Unknown values fall back to HAModeAuto with a warning logged by the caller.
func parseHAMode(s string) clusternats.HAMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "async":
		return clusternats.HAModeAsync
	case "sync":
		return clusternats.HAModeSync
	case "", "auto":
		return clusternats.HAModeAuto
	default:
		return clusternats.HAModeAuto
	}
}

// topologyLabel returns the human-readable topology string for startup logs.
// Mirrors the topology vocabulary used in the plan §"Deployment Topologies".
func topologyLabel(peers int, mode clusternats.HAMode) string {
	switch {
	case peers == 0:
		return "A (single-node, R=1)"
	case peers == 1 && mode == clusternats.HAModeAsync:
		return "B (dual-node, async source/mirror)"
	case peers == 1 && mode == clusternats.HAModeSync:
		return "B (dual-node, sync quorum)"
	case peers == 1:
		return "B (dual-node, auto -> R=2)"
	default:
		return "C (3+ node, R=3)"
	}
}

// zerologClusterLogger adapts the global logging.Logger to the
// cluster/nats.Logger and cluster/backup.Logger interfaces (same shape: Infof,
// Warnf, Errorf). A single adapter type satisfies both — Go interface
// satisfaction is structural.
type zerologClusterLogger struct{}

func (zerologClusterLogger) Infof(format string, args ...any) {
	logging.Logger.Info().Msgf(format, args...)
}
func (zerologClusterLogger) Warnf(format string, args ...any) {
	logging.Logger.Warn().Msgf(format, args...)
}
func (zerologClusterLogger) Errorf(format string, args ...any) {
	logging.Logger.Error().Msgf(format, args...)
}

// Compile-time assertions: zerologClusterLogger satisfies both targets.
var (
	_ clusternats.Logger = zerologClusterLogger{}
	_ backup.Logger      = zerologClusterLogger{}
)

// defaultBackupPolicies returns the tiered backup table from the plan
// (§"Critical Files" → backup policy matrix). Data-driven so adding or
// removing a domain is a one-line change. Tier intervals mirror the plan:
//   - "hot" identity-critical state → 30s (acl_rules, authority_grants/requests)
//   - control-plane metadata        → 1min (registry)
//   - bulk data                     → 5min (audit, kv, checkpoints)
//   - transient task notifications  → not backed up (MaxAge on the stream
//     handles retention).
func defaultBackupPolicies() []backup.BackupPolicy {
	return []backup.BackupPolicy{
		{
			Domain:      "aether_registry",
			Kind:        backup.DomainKindKV,
			KindStr:     "kv",
			MinInterval: 1 * time.Minute,
			S3Prefix:    "registry/",
		},
		{
			Domain:      "aether_acl_rules",
			Kind:        backup.DomainKindKV,
			KindStr:     "kv",
			MinInterval: 30 * time.Second,
			S3Prefix:    "acl/",
		},
		{
			Domain:      "aether_authority_grants",
			Kind:        backup.DomainKindKV,
			KindStr:     "kv",
			MinInterval: 30 * time.Second,
			S3Prefix:    "auth_grants/",
		},
		{
			Domain:      "aether_authority_requests",
			Kind:        backup.DomainKindKV,
			KindStr:     "kv",
			MinInterval: 30 * time.Second,
			S3Prefix:    "auth_requests/",
		},
		{
			Domain:      "audit",
			Kind:        backup.DomainKindStream,
			KindStr:     "stream",
			MinInterval: 5 * time.Minute,
			S3Prefix:    "audit/",
		},
		{
			Domain:      "aether_kv",
			Kind:        backup.DomainKindKV,
			KindStr:     "kv",
			MinInterval: 5 * time.Minute,
			S3Prefix:    "kv/",
		},
		{
			// Checkpoint store splits into KV (small) + Object (large) buckets +
			// an index KV. The index is the source of truth for "what's stored
			// where" so we back it up; large blobs (object store) are out of
			// scope for the periodic coordinator and would be handled by an
			// object-store-side replication mechanism in a real deployment.
			Domain:      "aether_checkpoints_idx",
			Kind:        backup.DomainKindKV,
			KindStr:     "kv",
			MinInterval: 5 * time.Minute,
			S3Prefix:    "checkpoints/",
		},
	}
}

// activateClusterPrefixIndex provisions the aether_registry KV bucket and
// starts the PrefixIndex JetStream watch in cluster mode. The returned
// PrefixIndex is the cluster-aware index that should be wired into the ACL
// service via SetPrefixIndex AFTER the state provider's own initialization
// (which constructs a non-watching index by default). On error the caller
// should treat the failure as fatal — cluster mode without cross-gateway
// agent registry sync is a misconfiguration, not a degraded state.
//
// Read side (Watch) is wired here; the matching write-side propagation runs
// inside registry.Register / Delete when the registry store has been given
// the KV bucket via KVSetter — main.go handles that plumbing using the kv
// returned from this function.
func activateClusterPrefixIndex(ctx context.Context, js jetstream.JetStream, replicas int, registryList []*registry.AgentRegistration) (*registry.PrefixIndex, jetstream.KeyValue, error) {
	kv, err := registry.CreateOrOpenRegistryBucket(ctx, js, replicas)
	if err != nil {
		return nil, nil, err
	}
	idx := registry.NewPrefixIndex()
	// Seed from the local DB FIRST so the index is non-empty even before the
	// watch's initial-values burst arrives. The watch will then merge / replace
	// entries as they come in from the KV bucket.
	if len(registryList) > 0 {
		idx.Rebuild(registryList)
	}
	if err := idx.StartJetStreamWatch(ctx, kv, slog.Default()); err != nil {
		return nil, nil, err
	}
	logging.Logger.Info().
		Str("bucket", "aether_registry").
		Int("seed_count", len(registryList)).
		Msg("PrefixIndex JetStream watch started (cluster mode)")
	return idx, kv, nil
}

// activateClusterAuthorityLifecycle idempotently provisions the JetStream
// resources (authreq stream + aether_authority_requests / aether_authority_grants
// KV buckets) needed by approver subscribers and the JetStreamTaskWaker, and
// returns a Store decorator that routes the 6 authority-request lifecycle
// methods through a JetStreamAuthorityLifecycle wrapper while delegating all
// other Store methods to the inner.
//
// The returned *aclstore.JetStreamAuthorityStore is the value the caller must
// thread into the gateway in place of the raw inner Store, so writes
// originated by the gateway's ACL field flow through the JetStream wrapper's
// CAS+event-publish path.
func activateClusterAuthorityLifecycle(_ context.Context, inner aclstore.Store, js jetstream.JetStream, replicas int) (*aclstore.JetStreamAuthorityStore, error) {
	// aclstore.JetStreamAuthorityStore embeds the inner Store and shadows the
	// 6 lifecycle methods with calls through an internal JetStream wrapper.
	// The wrapper's constructor (invoked inside the decorator constructor)
	// performs the idempotent stream/bucket creates we care about regardless
	// of whether the returned decorator is consumed downstream.
	wrapped, err := aclstore.NewJetStreamAuthorityStore(inner, js, replicas, zerologClusterLogger{})
	if err != nil {
		return nil, err
	}
	logging.Logger.Info().
		Str("stream", acl.AuthorityRequestsStream).
		Str("requests_bucket", acl.AuthorityRequestsKVBucket).
		Str("grants_bucket", acl.AuthorityGrantsKVBucket).
		Int("replicas", replicas).
		Msg("authority-request JetStream stream + KV buckets provisioned (cluster mode); Store decorator installed")
	return wrapped, nil
}

// startClusterTaskWaker spawns the JetStream-driven task waker alongside the
// existing scanner-based waker. Composition (not replacement) is the design
// contract: the JetStream path covers authority-resolved + inbound-INPUT
// wakes, the timer-based scanner continues to handle dependency reconciliation
// + scheduled-timer wakes + timeout-to-fail. State-machine transitions are
// idempotent, so a double-fire is a no-op.
//
// Returns a started waker (consumers active) and a stop func that blocks
// until the goroutines drain. ctx must outlive the desired waker lifetime;
// cancelling ctx is the supported teardown path.
func startClusterTaskWaker(ctx context.Context, js jetstream.JetStream, ts taskstore.Store, svc *orchestration.TaskAssignmentService) {
	w := orchestration.NewJetStreamTaskWaker(js, ts, svc, "")
	go w.Run(ctx)
	logging.Logger.Info().Msg("JetStream task waker started (cluster mode)")
}

// buildBackupStorage returns the StorageClient for the backup coordinator.
// When AETHERLITE_S3_BUCKET is set, returns an S3-backed client (works with
// AWS, MinIO, R2, etc. via AETHERLITE_S3_ENDPOINT + force-path-style).
// Otherwise returns a LocalFileStorage rooted at "{data_dir}/backups" — fine
// for single-node development and explicit on-disk cold backups.
func buildBackupStorage(ctx context.Context, cenv clusterEnv, dataDir string) (backup.StorageClient, error) {
	if cenv.S3Bucket == "" {
		root := filepath.Join(dataDir, "backups")
		s, err := backup.NewLocalFileStorage(root)
		if err != nil {
			return nil, err
		}
		logging.Logger.Info().Str("root", root).Msg("backup storage: using local filesystem")
		return s, nil
	}
	s3cfg := backup.S3Config{
		Bucket:          cenv.S3Bucket,
		Region:          cenv.S3Region,
		Endpoint:        cenv.S3Endpoint,
		AccessKeyID:     cenv.S3AccessKey,
		SecretAccessKey: cenv.S3SecretKey,
		ForcePathStyle:  cenv.S3ForcePath,
	}
	s, err := backup.NewS3StorageClient(ctx, s3cfg)
	if err != nil {
		return nil, err
	}
	logging.Logger.Info().
		Str("bucket", cenv.S3Bucket).
		Str("region", cenv.S3Region).
		Str("endpoint", cenv.S3Endpoint).
		Msg("backup storage: using S3-compatible client")
	return s, nil
}
