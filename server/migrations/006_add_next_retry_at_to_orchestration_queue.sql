-- Add next_retry_at timestamp for exponential backoff
-- Complements migration 005 retry tracking columns

-- =============================================================================
-- Add Next Retry Timestamp Column
-- =============================================================================

ALTER TABLE orchestrated_task_queue
    ADD COLUMN next_retry_at TIMESTAMP;

-- Index for finding tasks ready to retry after backoff period
CREATE INDEX idx_orch_queue_next_retry
    ON orchestrated_task_queue(next_retry_at) WHERE status = 'pending' AND next_retry_at IS NOT NULL;

-- Note: This column enables exponential backoff for retries
-- Tasks with next_retry_at set will only be polled when the timestamp is reached
