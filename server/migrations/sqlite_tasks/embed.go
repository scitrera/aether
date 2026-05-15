// Package sqlite_tasks holds the native-SQLite migration set for the tasks
// domain (Stage 2 of the storage-interfaces refactor). Unlike the monolithic
// migrations/sqlite/ which relies on dbcompat's postgres-to-sqlite
// translation, these migrations use pure SQLite idioms and are consumed
// directly by internal/storage/tasks/sqlite.Store.
package sqlite_tasks

import "embed"

//go:embed *.sql
var MigrationFS embed.FS
