// Package sqlite_tokens holds the SQLite migration set for the API tokens
// database (tokens.db). In lite mode the token store gets its own SQLite
// file so its WAL writer lock doesn't contend with other domains.
//
// The postgres path is unaffected — the full gateway uses 007_api_tokens.sql
// against its shared PostgreSQL instance.
package sqlite_tokens

import "embed"

//go:embed *.sql
var MigrationFS embed.FS
