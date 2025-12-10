package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ecociel/when/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool}
}

func (repo *PostgresRepo) Insert(ctx context.Context, t *domain.Task) (int64, error) {
	const q = `INSERT INTO scheduled_tasks (topic, key, payload, run_at, paused) VALUES ($1, $2, $3, $4, $5)
RETURNING id;
`
	var payloadObj map[string]any
	if err := json.Unmarshal(t.Payload, &payloadObj); err != nil {
		return 0, fmt.Errorf("serialize payload: %w", err)
	}
	var id int64
	err := repo.pool.QueryRow(ctx, q, t.Topic, t.Key, t.Payload, t.RunAt, t.Paused).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert task: %w", err)
	}
	return id, err
}

func (repo *PostgresRepo) Pause(ctx context.Context, id int64) error {
	//TODO implement me
	panic("implement me")
}

func (repo *PostgresRepo) UnPause(ctx context.Context, id int64) error {
	//TODO implement me
	panic("implement me")
}

func (repo *PostgresRepo) Reschedule(ctx context.Context, id int64, when time.Time) error {
	//TODO implement me
	panic("implement me")
}
