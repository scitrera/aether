package kv

import (
	"context"
	"expvar"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	badgery "github.com/dgraph-io/badger/v4/y"
	"github.com/scitrera/aether/server/pkg/models"
)

func TestBadgerListReadsOnlyRequestedValues(t *testing.T) {
	// Value-log counters are process-wide; do not run in parallel.
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueThreshold(1024))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s := NewBadgerKVStore(db)
	agent := models.Identity{Type: models.PrincipalAgent, Implementation: "test", Specifier: "worker"}
	value := strings.Repeat("x", 64*1024)
	for i := 0; i < 128; i++ {
		if err := s.Set(context.Background(), agent, ScopeGlobalExclusive, fmt.Sprintf("key-%03d", i), value, "", "", 0); err != nil {
			t.Fatal(err)
		}
	}
	reads := expvar.Get(badgery.BADGER_METRIC_PREFIX + "read_num_vlog").(*expvar.Int)
	before := reads.Value()
	page, err := s.ListPaginated(context.Background(), agent, ScopeGlobalExclusive, "", "", &ListOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items["key-000"] != value || !page.HasMore || page.NextCursor == "" {
		t.Fatal("incorrect first page")
	}
	if delta := reads.Value() - before; delta != 1 {
		t.Fatalf("one-item page read %d values; want exactly 1", delta)
	}
}
