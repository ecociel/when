
CREATE TABLE IF NOT EXISTS task (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    partition_key TEXT,
    args BYTEA NOT NULL,
    due TIMESTAMPTZ NOT NULL,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retry_count SMALLINT NOT NULL DEFAULT 0,
    retry_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_state_paused_due
    ON task (due,paused);
