
CREATE TABLE IF NOT EXISTS task (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    partition_key TEXT,
    args BYTEA NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    due TIMESTAMPTZ NOT NULL,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    --external_key TEXT,
    --triggered    BOOLEAN NOT NULL,
    --publishing_at    TIMESTAMPTZ,
    --published_at     TIMESTAMPTZ,
    --publish_attempts INT NOT NULL,
    --last_publish_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_task_state_paused_due
    ON scheduled_tasks (state, paused, due);