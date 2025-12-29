package scheduler

import (
	"context"
	"fmt"

	"github.com/ecociel/when/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{pool: pool}
}

func (s *Scheduler) Schedule(ctx context.Context, task domain.Task) (id int64, err error) {
	const q = `
        INSERT INTO task
          (name, partition_key, args, due)
        VALUES
          ($1, $2, $3, $4)
        RETURNING id
        `
	err = s.pool.QueryRow(ctx, q, task.Name, task.PartitionKey, task.Args, task.Due).Scan(&id)
	if err != nil {
		return id, fmt.Errorf("insert task: %w", err)
	}
	return id, nil
}
