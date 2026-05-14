-- Migration 006: target_specifier for per-user singleton agent dedup.
-- SQLite counterpart of postgres migration 015_target_specifier.sql.
--
-- The target_specifier column and the matching idx_startup_lookup partial
-- index are already declared in 001_full_schema.sql, so this migration is
-- a no-op on fresh databases. It exists only to keep version numbering
-- aligned with the postgres side.

-- Intentionally empty.
