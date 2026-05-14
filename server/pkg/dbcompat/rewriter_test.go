package dbcompat

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestRewriteQuery_Placeholders(t *testing.T) {
	input := "SELECT * FROM foo WHERE a = $1 AND b = $2 AND c = $3"
	want := "SELECT * FROM foo WHERE a = ?1 AND b = ?2 AND c = ?3"
	if got := rewriteQuery(input); got != want {
		t.Errorf("placeholders: got %q, want %q", got, want)
	}
}

func TestRewriteQuery_Now(t *testing.T) {
	cases := []struct{ input, want string }{
		{"SELECT NOW()", "SELECT CURRENT_TIMESTAMP"},
		{"SELECT now()", "SELECT CURRENT_TIMESTAMP"},
		{"WHERE created_at < NOW()", "WHERE created_at < CURRENT_TIMESTAMP"},
	}
	for _, c := range cases {
		if got := rewriteQuery(c.input); got != c.want {
			t.Errorf("NOW(): got %q, want %q", got, c.want)
		}
	}
}

func TestRewriteQuery_TypeCasts(t *testing.T) {
	cases := []struct{ input, want string }{
		{"SELECT $1::text", "SELECT ?1"},
		{"SELECT $1::interval", "SELECT ?1"},
		{"SELECT $1::integer", "SELECT ?1"},
		{"($3 || ' seconds')::interval", "(?3 || ' seconds')"},
	}
	for _, c := range cases {
		if got := rewriteQuery(c.input); got != c.want {
			t.Errorf("cast: got %q, want %q", got, c.want)
		}
	}
}

func TestRewriteQuery_Combined(t *testing.T) {
	input := "WHERE created_at < NOW() AND id = $1::text"
	want := "WHERE created_at < CURRENT_TIMESTAMP AND id = ?1"
	if got := rewriteQuery(input); got != want {
		t.Errorf("combined: got %q, want %q", got, want)
	}
}

// TestRewriteQuery_PositionalReuse verifies that a $N used more than once in
// a query is rewritten to ?N (not bare ?), preserving positional-reuse semantics.
func TestRewriteQuery_PositionalReuse(t *testing.T) {
	input := "SELECT * FROM x WHERE id IN ($1, $2) AND owner = $1"
	want := "SELECT * FROM x WHERE id IN (?1, ?2) AND owner = ?1"
	if got := rewriteQuery(input); got != want {
		t.Errorf("positional reuse: got %q, want %q", got, want)
	}
}

// TestEndToEnd_PositionalReuse exercises the SQLite ?NNN binding path with a
// real sqlite_compat database. This is the regression case for queries like
// GetLaunchParams that reference the same $N placeholder more than once.
func TestEndToEnd_PositionalReuse(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite_compat", filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE items (name TEXT PRIMARY KEY, kind TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO items VALUES ('alpha', 'specific'), ('base', 'generic')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Mirrors the GetLaunchParams shape: $1 referenced twice, 2 args.
	// PostgreSQL: WHERE name IN ($1, $2) ORDER BY CASE WHEN name = $1 THEN 0 ELSE 1 END LIMIT 1
	// This should prefer the specific row ('alpha') over the generic one.
	wdb := Wrap(db, DialectSQLite)
	row := wdb.QueryRowContext(ctx, `
		SELECT kind FROM items
		WHERE name IN ($1, $2)
		ORDER BY CASE WHEN name = $1 THEN 0 ELSE 1 END
		LIMIT 1`,
		"alpha", "base",
	)
	var kind string
	if err := row.Scan(&kind); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if kind != "specific" {
		t.Errorf("got kind=%q, want %q — positional reuse of $1 broken", kind, "specific")
	}
}

// TestEndToEnd_GetLaunchParamsShape is a direct regression test mirroring the
// exact query from internal/registry/agent_registry.go GetLaunchParams.
func TestEndToEnd_GetLaunchParamsShape(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite_compat", filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE agent_registry (
		implementation TEXT PRIMARY KEY,
		launch_params TEXT
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_registry VALUES ('specific_impl', '{"foo":"bar"}'), ('base_impl', '{"baz":"qux"}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	wdb := Wrap(db, DialectSQLite)
	row := wdb.QueryRowContext(ctx, `
		SELECT launch_params FROM agent_registry
		WHERE implementation IN ($1, $2)
		ORDER BY CASE WHEN implementation = $1 THEN 0 ELSE 1 END
		LIMIT 1`,
		"specific_impl", "base_impl",
	)
	var params string
	if err := row.Scan(&params); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if params != `{"foo":"bar"}` {
		t.Errorf("got params=%q, want specific_impl row — $1 reuse broken", params)
	}
}

func TestRewriteQuery_PostgresPassthrough(t *testing.T) {
	// Postgres dialect should not rewrite anything
	db := Wrap(nil, DialectPostgres)
	input := "SELECT $1, NOW(), $2::text"
	if got := db.rewrite(input); got != input {
		t.Errorf("postgres passthrough: got %q, want %q (unchanged)", got, input)
	}
}

func TestWrapUnwrap(t *testing.T) {
	db := Wrap(nil, DialectSQLite)
	if db.Dialect != DialectSQLite {
		t.Errorf("Dialect: got %q, want %q", db.Dialect, DialectSQLite)
	}
	if db.Unwrap() != nil {
		t.Error("Unwrap: expected nil underlying db")
	}
}
