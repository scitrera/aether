package backup

import (
	"encoding/json"
	"hash"
	"io"
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

// hashingCountingReader wraps an io.Reader so that:
//   - every byte read is also fed to a sha256.Hash
//   - a running byte count is maintained for the manifest's SizeBytes field
type hashingCountingReader struct {
	r io.Reader
	h hash.Hash
	n int64
}

func newHashingCountingReader(r io.Reader, h hash.Hash) *hashingCountingReader {
	return &hashingCountingReader{r: r, h: h}
}

func (r *hashingCountingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		// hash.Hash.Write never errors per the io.Writer contract for
		// hash implementations in the stdlib, so we ignore its return.
		_, _ = r.h.Write(p[:n])
		r.n += int64(n)
	}
	return n, err
}

// Count returns the total number of bytes observed by Read so far.
func (r *hashingCountingReader) Count() int64 { return r.n }
