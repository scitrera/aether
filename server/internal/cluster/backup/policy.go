// Package backup provides a tiered hot/warm/cold backup model for the
// aetherlite cluster. A leader-elected coordinator periodically snapshots
// JetStream streams and KV buckets and uploads them, with a manifest sidecar,
// to an object store (S3 / MinIO / local filesystem for tests).
//
// This package only depends on the v2 NATS JetStream client and the AWS SDK
// v2 S3 client. It contains no references to the rest of the aether codebase
// beyond the embedded NATS package's JetStream type.
package backup

import (
	"errors"
	"fmt"
	"time"
)

// DomainKind describes whether a backup domain is a JetStream stream or a
// JetStream KV bucket. (The set of supported kinds is deliberately small so
// the coordinator can stay generic.)
type DomainKind int

const (
	// DomainKindStream snapshots a JetStream stream's messages.
	DomainKindStream DomainKind = iota
	// DomainKindKV snapshots a JetStream KV bucket.
	DomainKindKV
)

// String returns a stable string label for the DomainKind.
func (k DomainKind) String() string {
	switch k {
	case DomainKindStream:
		return "stream"
	case DomainKindKV:
		return "kv"
	default:
		return "unknown"
	}
}

// BackupPolicy describes a single domain that the coordinator should
// periodically snapshot. The set of policies is fixed at startup; future
// extensions might allow hot-reload but that is out of scope here.
type BackupPolicy struct {
	// Domain is the JetStream stream name or KV bucket name to snapshot.
	Domain string `yaml:"domain" json:"domain"`
	// Kind selects between stream and KV snapshot logic.
	Kind DomainKind `yaml:"-" json:"-"`
	// KindStr is the YAML-friendly representation ("stream" or "kv").
	KindStr string `yaml:"kind" json:"kind"`
	// MinInterval is the minimum time between successful backups for this
	// domain. The coordinator will not run a new snapshot until MinInterval
	// has elapsed since the previously recorded one.
	MinInterval time.Duration `yaml:"min_interval" json:"min_interval"`
	// MaxBatchAge is an advisory upper bound on the age of any uploaded
	// snapshot. Currently informational; the coordinator already pushes at
	// MinInterval. Reserved for future warm/cold tier scheduling.
	MaxBatchAge time.Duration `yaml:"max_batch_age" json:"max_batch_age"`
	// S3Prefix is the prefix (under the object store's bucket) used to store
	// snapshots and manifest sidecars for this domain. The final keys take
	// the form "{S3Prefix}/{Domain}/{snapshot_id}.bin" and
	// "{S3Prefix}/{Domain}/{snapshot_id}.manifest.json".
	S3Prefix string `yaml:"s3_prefix" json:"s3_prefix"`
	// ReplicaCount is forwarded to JetStream when (re-)creating streams or
	// KV buckets during a Restore operation. 0 means "use 1".
	ReplicaCount int `yaml:"replica_count" json:"replica_count"`
}

// Validate ensures the policy fields are usable.
func (p *BackupPolicy) Validate() error {
	if p.Domain == "" {
		return errors.New("backup policy: domain is required")
	}
	if p.MinInterval <= 0 {
		return fmt.Errorf("backup policy %q: min_interval must be > 0", p.Domain)
	}
	if p.S3Prefix == "" {
		return fmt.Errorf("backup policy %q: s3_prefix is required", p.Domain)
	}
	// Normalize Kind from KindStr when needed (YAML/JSON path).
	if p.KindStr != "" {
		switch p.KindStr {
		case "stream":
			p.Kind = DomainKindStream
		case "kv":
			p.Kind = DomainKindKV
		default:
			return fmt.Errorf("backup policy %q: unknown kind %q", p.Domain, p.KindStr)
		}
	}
	return nil
}
