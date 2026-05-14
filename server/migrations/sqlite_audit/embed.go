// Package sqlite_audit holds the SQLite migration set for AetherLite's
// dedicated audit database (audit.db). In lite mode the high-volume
// async batch writer (comprehensive_audit_log) gets its own SQLite file
// so its WAL writer lock can't block the synchronous task-read hot path
// that runs against aether.db.
//
// The postgres path is unaffected — the full gateway keeps everything in
// one connection pool because postgres handles its own write concurrency.
package sqlite_audit

import "embed"

//go:embed *.sql
var MigrationFS embed.FS
