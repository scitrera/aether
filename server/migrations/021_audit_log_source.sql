-- Migration: Add source column to comprehensive_audit_log
--
-- The source column distinguishes audit rows the gateway emitted from its own
-- observation (default 'gateway') from rows submitted by a connected principal
-- via SubmitAuditEvent ('principal'). Forensics queries can filter on this
-- column to separate gateway-truth events from principal-claimed events.
--
-- Default 'gateway' preserves the existing meaning for all pre-existing rows.

ALTER TABLE comprehensive_audit_log
    ADD COLUMN source VARCHAR(20) NOT NULL DEFAULT 'gateway';

CREATE INDEX IF NOT EXISTS idx_comprehensive_audit_log_source
    ON comprehensive_audit_log (source);
