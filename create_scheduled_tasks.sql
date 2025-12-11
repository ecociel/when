DROP TABLE IF EXISTS scheduled_tasks;

CREATE TYPE task_status AS ENUM ('pending', 'publishing');

CREATE TABLE IF NOT EXISTS scheduled_tasks (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    key TEXT,
    payload BYTEA NOT NULL,
    status task_status NOT NULL DEFAULT 'pending',
    run_at TIMESTAMPTZ NOT NULL,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    external_key TEXT,
    triggered    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_sched
    ON scheduled_tasks (status, paused, run_at);

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_external
    ON scheduled_tasks (external_key)
    WHERE external_key IS NOT NULL;