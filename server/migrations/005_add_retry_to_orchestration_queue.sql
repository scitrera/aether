-- Add retry tracking columns to orchestrated_task_queue
-- Implements retry limiting for failed orchestration tasks

-- =============================================================================
-- Add Retry Columns to Orchestrated Task Queue
-- =============================================================================

ALTER TABLE orchestrated_task_queue
    ADD COLUMN retry_count INT NOT NULL DEFAULT 0,
    ADD COLUMN max_retries INT NOT NULL DEFAULT 3;

-- Index for finding tasks ready for retry
CREATE INDEX idx_orch_queue_stale_claims
    ON orchestrated_task_queue(claimed_at) WHERE status = 'claimed';

-- Note: This index helps detect stuck/abandoned claims from crashed gateways
-- Tasks claimed but not completed within a reasonable timeout can be retried
