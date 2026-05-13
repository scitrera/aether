package dbcompat

import (
	"testing"
)

func TestRewriteQuery_Placeholders(t *testing.T) {
	input := "SELECT * FROM foo WHERE a = $1 AND b = $2 AND c = $3"
	want := "SELECT * FROM foo WHERE a = ? AND b = ? AND c = ?"
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
		{"SELECT $1::text", "SELECT ?"},
		{"SELECT $1::interval", "SELECT ?"},
		{"SELECT $1::integer", "SELECT ?"},
		{"($3 || ' seconds')::interval", "(? || ' seconds')"},
	}
	for _, c := range cases {
		if got := rewriteQuery(c.input); got != c.want {
			t.Errorf("cast: got %q, want %q", got, c.want)
		}
	}
}

func TestRewriteQuery_Combined(t *testing.T) {
	input := "WHERE created_at < NOW() AND id = $1::text"
	want := "WHERE created_at < CURRENT_TIMESTAMP AND id = ?"
	if got := rewriteQuery(input); got != want {
		t.Errorf("combined: got %q, want %q", got, want)
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
