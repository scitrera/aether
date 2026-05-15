// Package sqlite_audit_native holds the native-SQLite migration set for the
// audit domain (Stage 2 of the storage-interfaces refactor). Unlike
// migrations/sqlite_audit/ which relies on dbcompat's postgres-to-sqlite
// translation, these migrations use pure SQLite idioms and are consumed
// directly by internal/storage/audit/sqlite.Store.
package sqlite_audit_native

import "embed"

//go:embed *.sql
var MigrationFS embed.FS
