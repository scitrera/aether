package backup

import (
	"encoding/json"
)

// Manifest is the sidecar JSON document written alongside every snapshot.
// It is intentionally small and human-readable so operators can audit
// snapshots out-of-band by reading just the manifest.
type Manifest struct {
	Domain          string `json:"domain"`
	SnapshotID      string `json:"snapshot_id"`
	RaftIndex       uint64 `json:"raft_index"`
	SizeBytes       int64  `json:"size_bytes"`
	CreatedAtUnixMs int64  `json:"created_at_unix_ms"`
	ChecksumSHA256  string `json:"checksum_sha256"`
	Kind            string `json:"kind"`
}

// MarshalJSON returns the manifest as indented JSON suitable for upload.
func (m *Manifest) MarshalJSON() ([]byte, error) {
	type alias Manifest
	return json.MarshalIndent((*alias)(m), "", "  ")
}
