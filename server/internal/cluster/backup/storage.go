package backup

import (
	"context"
	"io"
	"time"
)

// ObjectInfo is the minimal info the coordinator needs about an object in the
// backing store: key + modification time + size.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// StorageClient is the abstraction every backend (S3, local file, etc.) must
// implement. Sizes flow through Upload so that the S3 multipart uploader can
// choose part sizes appropriately; the local backend simply ignores the value.
type StorageClient interface {
	// Upload writes the bytes from reader (of size bytes, when known) to the
	// object identified by key. The meta map is stored as object metadata
	// (best effort; backends that don't support metadata may still accept
	// it as a side-channel manifest hint).
	Upload(ctx context.Context, key string, reader io.Reader, size int64, meta map[string]string) error

	// Download streams the object identified by key to writer.
	Download(ctx context.Context, key string, writer io.Writer) error

	// LatestKey returns the lexicographically latest data ("*.bin") object
	// under prefix, along with its stored metadata. Returns os.ErrNotExist
	// when nothing is found.
	LatestKey(ctx context.Context, prefix string) (key string, meta map[string]string, err error)

	// List returns every object under prefix.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}
