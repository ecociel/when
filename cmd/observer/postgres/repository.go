package postgres

import (
	"context"
	"fmt"

	"github.com/ecociel/when/lib/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repo struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

func (repo *Repo) ClaimDueTasks(ctx context.Context, limit int) ([]domain.Task, error) {
	const q = `
    SELECT id, name, partition_key, args
    FROM task
    WHERE due < now() AND paused = FALSE ORDER BY due
    LIMIT $1
     `
	rows, err := repo.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("query claim due tasks: %w", err)
	}
	defer rows.Close()

	var tasks []domain.Task
	for rows.Next() {
		var task domain.Task
		if err := rows.Scan(&task.ID, &task.Name, &task.PartitionKey, &task.Args); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows claim due tasks: %w", err)
	}
	return tasks, nil
}

func (repo *Repo) Delete(ctx context.Context, id int64) error {
	const q = `
      DELETE FROM task WHERE id = $1`
	if _, err := repo.pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("delete for %d: %w", id, err)
	}
	return nil
}
