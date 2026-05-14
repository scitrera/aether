package dbcompat

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"path/filepath"
	"testing"
	"time"
)

// TestIsTimestampColumn covers the column-name heuristic that drives the
// Rows wrapper.
func TestIsTimestampColumn(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		// canonical _at suffix
		{"created_at", true},
		{"updated_at", true},
		{"granted_at", true},
		{"expires_at", true},
		{"completed_at", true},
		{"disconnected_at", true},
		// prefix-only matches
		{"timestamp", true},
		{"last_heartbeat", true},
		{"connected_at", true},
		{"renewable_until", true},
		// non-timestamp columns
		{"id", false},
		{"task_id", false},
		{"workspace", false},
		{"metadata", false},
		{"status", false},
		{"access_level", false},
		// case-insensitive (sqlite column names round-trip with whatever case
		// they were declared in; the heuristic lowercases before matching)
		{"CREATED_AT", true},
		{"LAST_HEARTBEAT", true},
	}
	for _, c := range cases {
		if got := isTimestampColumn(c.name); got != c.want {
			t.Errorf("isTimestampColumn(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestParseTimestamp covers the format ladder used for SQLite text timestamps.
func TestParseTimestamp(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantOK  bool
		wantUTC string // RFC3339 representation for comparison
	}{
		{"sqlite default format", "2026-05-13 14:23:45", true, "2026-05-13T14:23:45Z"},
		{"sqlite with microseconds", "2026-05-13 14:23:45.123456", true, "2026-05-13T14:23:45.123456Z"},
		{"sqlite with nanoseconds", "2026-05-13 14:23:45.123456789", true, "2026-05-13T14:23:45.123456789Z"},
		{"rfc3339", "2026-05-13T14:23:45Z", true, "2026-05-13T14:23:45Z"},
		{"rfc3339 nano", "2026-05-13T14:23:45.123456789Z", true, "2026-05-13T14:23:45.123456789Z"},
		// go's time.Time.String() format — what modernc.org/sqlite stores when
		// the aether ACL service binds a time.Time parameter directly.
		{"go time.String() with monotonic", "2026-05-13 14:23:45.123456789 +0000 UTC m=+15.114547836", true, "2026-05-13T14:23:45.123456789Z"},
		{"go time.String() without monotonic", "2026-05-13 14:23:45.123456789 +0000 UTC", true, "2026-05-13T14:23:45.123456789Z"},
		{"empty string", "", false, ""},
		{"garbage", "not a timestamp", false, ""},
		{"partial date", "2026-05-13", false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseTimestamp(c.input)
			if ok != c.wantOK {
				t.Fatalf("parseTimestamp(%q) ok = %v, want %v", c.input, ok, c.wantOK)
			}
			if !ok {
				return
			}
			if g := got.UTC().Format(time.RFC3339Nano); g != c.wantUTC {
				t.Errorf("parseTimestamp(%q) = %s, want %s", c.input, g, c.wantUTC)
			}
		})
	}
}

// fakeRows is a minimal driver.Rows used to exercise compatRows without
// pulling in a live sqlite connection.
type fakeRows struct {
	cols   []string
	rows   [][]driver.Value
	pos    int
	closed bool
}

func (f *fakeRows) Columns() []string { return f.cols }
func (f *fakeRows) Close() error      { f.closed = true; return nil }
func (f *fakeRows) Next(dest []driver.Value) error {
	if f.pos >= len(f.rows) {
		return io.EOF
	}
	row := f.rows[f.pos]
	for i := range dest {
		dest[i] = row[i]
	}
	f.pos++
	return nil
}

// TestCompatRows_TimestampCoercion verifies that string values in
// timestamp-named columns are converted to time.Time at Next().
func TestCompatRows_TimestampCoercion(t *testing.T) {
	inner := &fakeRows{
		cols: []string{"id", "created_at", "name"},
		rows: [][]driver.Value{
			{int64(1), "2026-05-13 14:23:45", "alpha"},
		},
	}
	rows := wrapRows(inner)
	dest := make([]driver.Value, 3)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if dest[0].(int64) != 1 {
		t.Errorf("dest[0] = %v, want 1", dest[0])
	}
	tt, ok := dest[1].(time.Time)
	if !ok {
		t.Fatalf("dest[1] not coerced to time.Time, got %T (%v)", dest[1], dest[1])
	}
	if want := "2026-05-13T14:23:45Z"; tt.UTC().Format(time.RFC3339) != want {
		t.Errorf("dest[1] = %v, want %s", tt.UTC().Format(time.RFC3339), want)
	}
	if dest[2].(string) != "alpha" {
		t.Errorf("dest[2] = %v, want alpha (non-timestamp passthrough)", dest[2])
	}
}

// TestCompatRows_NullPassthrough verifies that NULL (nil) values in a
// timestamp column are not touched.
func TestCompatRows_NullPassthrough(t *testing.T) {
	inner := &fakeRows{
		cols: []string{"id", "completed_at"},
		rows: [][]driver.Value{
			{int64(1), nil},
		},
	}
	rows := wrapRows(inner)
	dest := make([]driver.Value, 2)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if dest[1] != nil {
		t.Errorf("dest[1] = %v, want nil", dest[1])
	}
}

// TestCompatRows_NonTimestampUnchanged verifies that string columns that
// aren't named like a timestamp are not coerced.
func TestCompatRows_NonTimestampUnchanged(t *testing.T) {
	inner := &fakeRows{
		cols: []string{"id", "status", "metadata"},
		rows: [][]driver.Value{
			{int64(1), "running", `{"foo":"bar"}`},
		},
	}
	rows := wrapRows(inner)
	dest := make([]driver.Value, 3)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if dest[1].(string) != "running" {
		t.Errorf("dest[1] = %v, want running", dest[1])
	}
	if dest[2].(string) != `{"foo":"bar"}` {
		t.Errorf("dest[2] = %v, want metadata json", dest[2])
	}
}

// TestCompatRows_MalformedTimestampPassthrough verifies that a non-parseable
// string in a timestamp-named column is left as a string so the stdlib can
// raise the original error.
func TestCompatRows_MalformedTimestampPassthrough(t *testing.T) {
	inner := &fakeRows{
		cols: []string{"created_at"},
		rows: [][]driver.Value{
			{"not a real timestamp"},
		},
	}
	rows := wrapRows(inner)
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if s, ok := dest[0].(string); !ok || s != "not a real timestamp" {
		t.Errorf("dest[0] = %v (%T), want original string passthrough", dest[0], dest[0])
	}
}

// TestEndToEnd_ScanIntoTime exercises the full sqlite_compat driver: open a
// real database, insert a row whose created_at uses CURRENT_TIMESTAMP, and
// scan it into *time.Time. Without the coercion wrapper this errors out
// with "unsupported Scan, storing driver.Value type string into type *time.Time".
func TestEndToEnd_ScanIntoTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := sql.Open("sqlite_compat", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		completed_at TEXT,
		label TEXT
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO t (id, label) VALUES (1, 'hello')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var (
		id          int64
		createdAt   time.Time
		completedAt sql.NullTime
		label       string
	)
	row := db.QueryRowContext(ctx, `SELECT id, created_at, completed_at, label FROM t WHERE id = 1`)
	if err := row.Scan(&id, &createdAt, &completedAt, &label); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if id != 1 || label != "hello" {
		t.Errorf("id=%d label=%q, want 1/hello", id, label)
	}
	if createdAt.IsZero() {
		t.Error("createdAt is zero — coercion did not run")
	}
	if completedAt.Valid {
		t.Errorf("completedAt expected NULL, got %v", completedAt.Time)
	}
}

// TestEndToEnd_PreparedStmtQuery exercises the prepared-statement path that
// database/sql uses for the prepared-statement cache (Stmt.QueryContext on
// the wrapped *sql.Stmt). The wrapper installs compatRows on this path too.
func TestEndToEnd_PreparedStmtQuery(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite_compat", filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE acl_rules (
		rule_id TEXT PRIMARY KEY,
		granted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO acl_rules (rule_id) VALUES ('r1')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	stmt, err := db.PrepareContext(ctx, `SELECT rule_id, granted_at FROM acl_rules WHERE rule_id = ?`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, "r1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("no rows")
	}
	var rid string
	var gAt time.Time
	if err := rows.Scan(&rid, &gAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if gAt.IsZero() {
		t.Error("granted_at coercion failed")
	}
}
