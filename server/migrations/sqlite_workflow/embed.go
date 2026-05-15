// Package sqlite_workflow holds the native SQLite migration set for the
// workflow domain. This is the Stage 2 per-domain migration tree — each
// domain owns its own schema file(s) and migration runner, following the
// precedent established by migrations/sqlite_audit/.
//
// The legacy dbcompat-translated migrations live in
// internal/workflow/migrations/sqlite/ and remain untouched — they
// continue to serve the Stage 1 (dbcompat-wrapped) code path until
// Stage 3 retires dbcompat from aetherlite entirely.
package sqlite_workflow

import "embed"

//go:embed *.sql
var MigrationFS embed.FS
