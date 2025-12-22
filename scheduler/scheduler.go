package scheduler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ecociel/when/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler interface {
	Schedule(ctx context.Context, task domain.Task) error
}

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (r *Postgres) Schedule(ctx context.Context, task domain.Task) error {
	const q = `
        INSERT INTO scheduled_tasks
          (topic, key, payload, run_at, paused, external_key, triggered)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7)
`
	var payloadObj map[string]any
	if err := json.Unmarshal(task.Payload, &payloadObj); err != nil {
		return fmt.Errorf("serialize payload: %w", err)
	}
	_, err := r.pool.Exec(ctx, q, task.Topic, task.Key, task.Payload, task.RunAt, task.Paused, task.ExternalKey, task.Triggered)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	return nil
}
