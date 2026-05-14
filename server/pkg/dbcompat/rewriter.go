package dbcompat

import (
	"context"
	"database/sql"
	"regexp"
)

var (
	rePlaceholder = regexp.MustCompile(`\$\d+`)
	reNow         = regexp.MustCompile(`(?i)NOW\(\)`)
	// reCast matches PostgreSQL type casts like ::interval, ::text, ::text[], ::uuid.
	// Restricted to known cast targets and requires a word boundary after to avoid
	// matching inside string literals like '::buffer' or identifiers.
	reCast = regexp.MustCompile(`::(interval|text|integer|int|bigint|uuid|bytea|bool|boolean|timestamp|timestamptz|date|time|numeric|real|float)(\[\])?\b`)
)

// rewriteQuery converts PostgreSQL syntax to SQLite syntax.
// Note: regexes do not inspect string literals. If a query contains a literal
// like '::interval' in single-quoted text, it will be incorrectly rewritten.
// The restricted cast regex above reduces but does not eliminate this risk.
func rewriteQuery(query string) string {
	// $N -> ?N. Preserves PostgreSQL's positional-reuse semantics
	// (same N resolves to the same arg value) and matches SQLite's
	// ?NNN syntax: https://sqlite.org/lang_expr.html#varparam.
	query = rePlaceholder.ReplaceAllStringFunc(query, func(m string) string {
		return "?" + m[1:]
	})
	query = reNow.ReplaceAllString(query, "CURRENT_TIMESTAMP")
	query = reCast.ReplaceAllString(query, "")
	return query
}

// DB wraps *sql.DB with optional query rewriting for SQLite compatibility.
// When dialect is DialectPostgres, queries pass through unchanged.
// When dialect is DialectSQLite, PostgreSQL-specific syntax is rewritten.
type DB struct {
	*sql.DB
	Dialect Dialect
}

// Wrap creates a dbcompat.DB from an existing *sql.DB and dialect.
func Wrap(db *sql.DB, dialect Dialect) *DB {
	return &DB{DB: db, Dialect: dialect}
}

// Unwrap returns the underlying *sql.DB.
func (d *DB) Unwrap() *sql.DB {
	return d.DB
}

func (d *DB) rewrite(query string) string {
	if d.Dialect == DialectSQLite {
		return rewriteQuery(query)
	}
	return query
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.DB.ExecContext(ctx, d.rewrite(query), args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.DB.QueryContext(ctx, d.rewrite(query), args...)
}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.DB.QueryRowContext(ctx, d.rewrite(query), args...)
}

func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, dialect: d.Dialect}, nil
}

// Tx wraps *sql.Tx with optional query rewriting for SQLite compatibility.
type Tx struct {
	*sql.Tx
	dialect Dialect
}

func (t *Tx) rewrite(query string) string {
	if t.dialect == DialectSQLite {
		return rewriteQuery(query)
	}
	return query
}

func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.Tx.ExecContext(ctx, t.rewrite(query), args...)
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.Tx.QueryContext(ctx, t.rewrite(query), args...)
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.Tx.QueryRowContext(ctx, t.rewrite(query), args...)
}
