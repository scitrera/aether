-- Migration: Add time-based partitioning to comprehensive_audit_log
-- Uses PostgreSQL declarative partitioning by month on the timestamp column.
-- This enables efficient retention management (DROP partition vs DELETE) and
-- improves query performance for time-bounded queries.

-- Step 1: Rename the existing table to a temporary name
ALTER TABLE comprehensive_audit_log RENAME TO comprehensive_audit_log_old;

-- Step 2: Create the partitioned table with the same schema
CREATE TABLE comprehensive_audit_log
(
    audit_id       BIGSERIAL,
    timestamp      TIMESTAMP    NOT NULL DEFAULT NOW(),
    event_type     VARCHAR(50)  NOT NULL,
    actor_type     VARCHAR(50)  NOT NULL,
    actor_id       VARCHAR(255) NOT NULL,
    resource_type  VARCHAR(50),
    resource_id    VARCHAR(255),
    operation      VARCHAR(100) NOT NULL,
    workspace      VARCHAR(255),
    session_id     UUID,
    gateway_id     VARCHAR(100),
    success        BOOLEAN      NOT NULL DEFAULT TRUE,
    error_message  TEXT,
    metadata       JSONB,
    PRIMARY KEY (audit_id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Step 3: Create partitions for recent months and upcoming months
-- We create partitions covering the past 3 months through the next 2 months
CREATE TABLE comprehensive_audit_log_y2026m01 PARTITION OF comprehensive_audit_log
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE comprehensive_audit_log_y2026m02 PARTITION OF comprehensive_audit_log
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE comprehensive_audit_log_y2026m03 PARTITION OF comprehensive_audit_log
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE comprehensive_audit_log_y2026m04 PARTITION OF comprehensive_audit_log
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE comprehensive_audit_log_y2026m05 PARTITION OF comprehensive_audit_log
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE comprehensive_audit_log_y2026m06 PARTITION OF comprehensive_audit_log
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- Default partition for data outside defined ranges (safety net)
CREATE TABLE comprehensive_audit_log_default PARTITION OF comprehensive_audit_log DEFAULT;

-- Step 4: Copy existing data from the old table
INSERT INTO comprehensive_audit_log (
    audit_id, timestamp, event_type, actor_type, actor_id,
    resource_type, resource_id, operation, workspace,
    session_id, gateway_id, success, error_message, metadata
)
SELECT
    audit_id, timestamp, event_type, actor_type, actor_id,
    resource_type, resource_id, operation, workspace,
    session_id, gateway_id, success, error_message, metadata
FROM comprehensive_audit_log_old;

-- Step 5: Drop the old non-partitioned table (CASCADE drops dependent views)
DROP TABLE comprehensive_audit_log_old CASCADE;

-- Step 6: Recreate indexes on the partitioned table
-- PostgreSQL automatically creates indexes on each partition
CREATE INDEX IF NOT EXISTS idx_cal_timestamp ON comprehensive_audit_log (timestamp);
CREATE INDEX IF NOT EXISTS idx_cal_actor ON comprehensive_audit_log (actor_type, actor_id);
CREATE INDEX IF NOT EXISTS idx_cal_event_type ON comprehensive_audit_log (event_type);
CREATE INDEX IF NOT EXISTS idx_cal_resource ON comprehensive_audit_log (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_cal_operation ON comprehensive_audit_log (operation);
CREATE INDEX IF NOT EXISTS idx_cal_workspace ON comprehensive_audit_log (workspace);
CREATE INDEX IF NOT EXISTS idx_cal_session ON comprehensive_audit_log (session_id) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cal_gateway ON comprehensive_audit_log (gateway_id);
CREATE INDEX IF NOT EXISTS idx_cal_success ON comprehensive_audit_log (success) WHERE NOT success;

-- Step 7: Recreate the acl_audit_log view (dropped when old table was replaced)
CREATE OR REPLACE VIEW acl_audit_log AS
SELECT
    audit_id,
    timestamp,
    COALESCE(metadata->>'decision', CASE WHEN success THEN 'ALLOW' ELSE 'DENY' END) AS decision,
    (metadata->>'access_level')::integer AS access_level,
    actor_type AS principal_type,
    actor_id AS principal_id,
    resource_type,
    resource_id,
    operation,
    workspace,
    (metadata->>'delegation_chain_id')::uuid AS delegation_chain_id,
    (metadata->>'rule_id')::uuid AS rule_id,
    COALESCE((metadata->>'fallback_applied')::boolean, false) AS fallback_applied,
    gateway_id,
    session_id,
    metadata
FROM comprehensive_audit_log
WHERE event_type = 'authorization';

-- Step 8: Function to auto-create future monthly partitions
-- Run this periodically (e.g., monthly via cron or the gateway cleanup service)
CREATE OR REPLACE FUNCTION create_audit_partition_if_needed(target_date date)
RETURNS void AS $$
DECLARE
    partition_name text;
    start_date date;
    end_date date;
BEGIN
    start_date := date_trunc('month', target_date);
    end_date := start_date + interval '1 month';
    partition_name := 'comprehensive_audit_log_y' || to_char(start_date, 'YYYY') || 'm' || to_char(start_date, 'MM');

    -- Check if partition already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = partition_name
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF comprehensive_audit_log FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
        RAISE NOTICE 'Created audit partition: %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Step 9: Function to drop old partitions for retention management
-- More efficient than DELETE: instantly reclaims space
CREATE OR REPLACE FUNCTION drop_audit_partitions_before(cutoff_date date)
RETURNS integer AS $$
DECLARE
    partition_record record;
    dropped_count integer := 0;
BEGIN
    FOR partition_record IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON i.inhrelid = c.oid
        JOIN pg_class p ON i.inhparent = p.oid
        WHERE p.relname = 'comprehensive_audit_log'
          AND c.relname != 'comprehensive_audit_log_default'
          AND c.relname LIKE 'comprehensive_audit_log_y%'
    LOOP
        -- Extract the date from the partition name (e.g., y2026m01 -> 2026-01-01)
        DECLARE
            year_str text;
            month_str text;
            partition_date date;
        BEGIN
            year_str := substring(partition_record.relname from 'y(\d{4})');
            month_str := substring(partition_record.relname from 'm(\d{2})');
            IF year_str IS NOT NULL AND month_str IS NOT NULL THEN
                partition_date := (year_str || '-' || month_str || '-01')::date;
                IF partition_date < cutoff_date THEN
                    EXECUTE format('DROP TABLE %I', partition_record.relname);
                    dropped_count := dropped_count + 1;
                    RAISE NOTICE 'Dropped audit partition: %', partition_record.relname;
                END IF;
            END IF;
        END;
    END LOOP;
    RETURN dropped_count;
END;
$$ LANGUAGE plpgsql;

-- Step 10: Ensure partitions exist for the next 3 months
SELECT create_audit_partition_if_needed((NOW() + interval '1 month')::date);
SELECT create_audit_partition_if_needed((NOW() + interval '2 months')::date);
SELECT create_audit_partition_if_needed((NOW() + interval '3 months')::date);
