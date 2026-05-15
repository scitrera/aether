// Package sqlite_registry holds the SQLite migration set for AetherLite's
// dedicated registry database (registry.db). In lite mode the agent catalog
// and orchestrator profile fleet get their own SQLite file so each domain
// owns its WAL independently.
//
// The postgres path is unaffected -- the full gateway keeps everything in
// one connection pool because postgres handles its own write concurrency.
package sqlite_registry

import "embed"

//go:embed *.sql
var MigrationFS embed.FS
