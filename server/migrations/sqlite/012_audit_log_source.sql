-- Migration 012: Add source column to comprehensive_audit_log.
-- SQLite counterpart of postgres migration 021_audit_log_source.sql.
--
-- The source column (default 'gateway') and idx_cal_source index are already
-- declared in 001_full_schema.sql, so this migration is a no-op on fresh
-- databases. Pre-001-consolidated aether-lite databases (if any exist) would
-- need a manual schema fix; this file does not attempt to retrofit them.

-- Intentionally empty.
