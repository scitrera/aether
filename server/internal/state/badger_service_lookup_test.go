package state

import (
	"bytes"
	"context"
	"expvar"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	badgery "github.com/dgraph-io/badger/v4/y"
	"github.com/scitrera/aether/server/internal/lite"
)

func TestBadgerServiceLookupDoesNotReadValues(t *testing.T) {
	// The value-log counter is process-wide; keep this test sequential.
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueThreshold(1024))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	err = db.Update(func(txn *badger.Txn) error {
		for _, id := range []string{"sv::worker::a", "sv::worker::b", "sv::worker-extra::other"} {
			if err := txn.Set(lockKey(id), []byte("session")); err != nil {
				return err
			}
		}
		expired := badger.NewEntry(lockKey("sv::worker::expired"), []byte("session"))
		expired.ExpiresAt = uint64(time.Now().Add(-time.Hour).Unix())
		if err := txn.SetEntry(expired); err != nil {
			return err
		}
		// Adjacent values belong to a different namespace in the shared DB.
		// A key-only service lookup must never fetch them from the value log.
		payload := bytes.Repeat([]byte("x"), 64*1024)
		for i := 0; i < 64; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("%s%03d", lite.PrefixToken, i)), payload); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reads, ok := expvar.Get(badgery.BADGER_METRIC_PREFIX + "read_num_vlog").(*expvar.Int)
	if !ok {
		t.Fatal("Badger value-log read metric unavailable")
	}
	reg := NewBadgerSessionRegistry(db)
	for _, tc := range []struct {
		implementation string
		want           []string
	}{
		{"worker", []string{"sv::worker::a", "sv::worker::b"}},
		{"missing", nil},
	} {
		t.Run(tc.implementation, func(t *testing.T) {
			before := reads.Value()
			got, err := reg.FindHealthyServiceInstances(context.Background(), tc.implementation, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("instances = %v, want %v", got, tc.want)
			}
			if delta := reads.Value() - before; delta != 0 {
				t.Fatalf("key-only lookup read %d unrelated value-log entries", delta)
			}
		})
	}
}
