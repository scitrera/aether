// Package dbcompat provides database dialect abstraction for supporting
// multiple SQL backends (PostgreSQL, SQLite) in the Aether gateway.
package dbcompat

// Dialect identifies the SQL database backend in use.
type Dialect string

const (
	// DialectPostgres indicates a PostgreSQL database backend.
	DialectPostgres Dialect = "postgres"

	// DialectSQLite indicates a SQLite database backend (via modernc.org/sqlite).
	DialectSQLite Dialect = "sqlite"
)
