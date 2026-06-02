-- Fan-in/barrier/coalesce join instances. The authoritative arrival counter
-- lives in KV; this table is the durable observability + deadline-sweep
-- surface. One row per (join_name, workspace, correlation_key).
CREATE TABLE IF NOT EXISTS workflow_joins (
  id              BIGSERIAL PRIMARY KEY,
  join_name       TEXT NOT NULL,
  workspace       TEXT NOT NULL,
  correlation_key TEXT NOT NULL,
  mode            TEXT NOT NULL,
  expected_count  BIGINT,
  arrived_count   BIGINT NOT NULL DEFAULT 0,
  dirty           BOOLEAN NOT NULL DEFAULT FALSE,
  status          TEXT NOT NULL DEFAULT 'open',
  deadline_at     TIMESTAMPTZ,
  linger_until    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (join_name, workspace, correlation_key)
);
CREATE INDEX IF NOT EXISTS idx_workflow_joins_due ON workflow_joins (status, deadline_at);
