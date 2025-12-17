DROP TABLE IF EXISTS scheduled_tasks;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_state') THEN
            CREATE TYPE task_state AS ENUM (
                'pending',
                'publishing',
                'published'
                );
        END IF;
    END$$;

CREATE TABLE IF NOT EXISTS scheduled_tasks (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    key TEXT,
    payload BYTEA NOT NULL,
    state task_state NOT NULL DEFAULT 'pending',
    run_at TIMESTAMPTZ NOT NULL,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    external_key TEXT,
    triggered    BOOLEAN NOT NULL DEFAULT FALSE,
    publishing_at    TIMESTAMPTZ,
    published_at     TIMESTAMPTZ,
    publish_attempts INT NOT NULL DEFAULT 0,
    last_publish_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_due
    ON scheduled_tasks (state, paused, run_at);

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_external
    ON scheduled_tasks (external_key)
    WHERE external_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_published_at
    ON scheduled_tasks (published_at)
    WHERE state = 'published';

CREATE OR REPLACE FUNCTION scheduled_tasks_touch_updated_at()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_scheduled_tasks_updated_at ON scheduled_tasks;

CREATE TRIGGER trg_scheduled_tasks_updated_at
    BEFORE UPDATE ON scheduled_tasks
    FOR EACH ROW
EXECUTE FUNCTION scheduled_tasks_touch_updated_at();