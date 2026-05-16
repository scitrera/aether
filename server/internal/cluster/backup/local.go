package backup

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// LocalFileStorage is a StorageClient that stores objects on the local
// filesystem under a root directory. It is intended for tests and for "cold"
// backups to a co-located disk (NFS mount, etc.). Object metadata is
// serialized as a sidecar JSON file under "<key>.meta.json".
type LocalFileStorage struct {
	root string
	mu   sync.Mutex
}

// Compile-time interface assertion.
var _ StorageClient = (*LocalFileStorage)(nil)

// NewLocalFileStorage returns a LocalFileStorage rooted at root. The root
// directory is created if it does not exist.
func NewLocalFileStorage(root string) (*LocalFileStorage, error) {
	if root == "" {
		return nil, errors.New("local storage: root is required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &LocalFileStorage{root: root}, nil
}

// Upload copies bytes from reader into "{root}/{key}" and writes meta to a
// sidecar JSON file.
func (l *LocalFileStorage) Upload(ctx context.Context, key string, reader io.Reader, size int64, meta map[string]string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	full := filepath.Join(l.root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	f, err := os.Create(full)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, reader); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if len(meta) > 0 {
		mf, err := os.Create(full + ".meta.json")
		if err != nil {
			return err
		}
		defer mf.Close()
		enc := json.NewEncoder(mf)
		enc.SetIndent("", "  ")
		if err := enc.Encode(meta); err != nil {
			return err
		}
	}
	return nil
}

// Download streams "{root}/{key}" to writer.
func (l *LocalFileStorage) Download(ctx context.Context, key string, writer io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	full := filepath.Join(l.root, key)
	f, err := os.Open(full)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(writer, f)
	return err
}

// LatestKey scans the prefix and returns the lexicographically latest
// "*.bin" object plus the metadata stored in its sidecar.
func (l *LocalFileStorage) LatestKey(ctx context.Context, prefix string) (string, map[string]string, error) {
	objs, err := l.List(ctx, prefix)
	if err != nil {
		return "", nil, err
	}
	// Filter to ".bin" data files, ignore manifests + meta sidecars.
	var bins []ObjectInfo
	for _, o := range objs {
		if strings.HasSuffix(o.Key, ".bin") {
			bins = append(bins, o)
		}
	}
	if len(bins) == 0 {
		return "", nil, os.ErrNotExist
	}
	sort.Slice(bins, func(i, j int) bool { return bins[i].Key < bins[j].Key })
	latest := bins[len(bins)-1]

	meta := map[string]string{}
	mfPath := filepath.Join(l.root, latest.Key+".meta.json")
	if data, err := os.ReadFile(mfPath); err == nil {
		_ = json.Unmarshal(data, &meta)
	}
	return latest.Key, meta, nil
}

// List walks the directory tree under "{root}/{prefix}" and returns every
// regular file's relative key.
func (l *LocalFileStorage) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base := filepath.Join(l.root, prefix)
	var out []ObjectInfo
	walkErr := filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Skip sidecar metadata files; they're an implementation detail of
		// the local backend.
		if strings.HasSuffix(path, ".meta.json") {
			return nil
		}
		rel, err := filepath.Rel(l.root, path)
		if err != nil {
			return err
		}
		out = append(out, ObjectInfo{
			Key:          filepath.ToSlash(rel),
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
		return nil
	})
	if walkErr != nil && !os.IsNotExist(walkErr) {
		return nil, walkErr
	}
	return out, nil
}
