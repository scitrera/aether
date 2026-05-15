// Package sqlite_acl holds the native SQLite migration set for the ACL
// domain's per-domain database file. Stage 2 of the storage-interfaces
// refactor: each domain owns its own migration tree and file handle.
//
// The postgres path is unaffected -- the full gateway keeps everything in
// one connection pool because postgres handles its own write concurrency.
package sqlite_acl

import "embed"

//go:embed *.sql
var MigrationFS embed.FS
