package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/ecociel/when/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool}
}

func (repo *PostgresRepo) ClaimDueTasks(ctx context.Context, now time.Time, limit int) ([]domain.Task, error) {
	const q = `
    SELECT id, name, partition_key, args
    FROM task
    WHERE state = 'pending' AND due < now() AND paused = FALSE ORDER BY due
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

//func (repo *PostgresRepo) SetTaskPublished(ctx context.Context, id int64) error {
//	const q = `UPDATE scheduled_tasks SET state = 'published' WHERE id = $1;`
//	_, err := repo.pool.Exec(ctx, q, id)
//	return fmt.Errorf("set task published for %d: %w", id, err)
//}

//func (repo *PostgresRepo) RevertTaskToPending(ctx context.Context, id int64, nextRunAt time.Time) error {
//	const q = `UPDATE scheduled_tasks SET status = 'pending', run_at = $2, updated_at = now() WHERE id = $1 AND status = 'publishing';`
//	_, err := repo.pool.Exec(ctx, q, id, nextRunAt)
//	if err != nil {
//		return err
//	}
//	return nil
//}

func (repo *PostgresRepo) MarkPublished(ctx context.Context, id int64) error {
	const q = `
      UPDATE task
      SET state = 'published', updated_at = now()
      WHERE id = $1`
	if _, err := repo.pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("mark published for %d: %w", id, err)
	}
	return nil
}

//func (repo *PostgresRepo) MarkPublishFailed(ctx context.Context, id int64, errMsg string, nextRunAt time.Time) error {
//	const q = `UPDATE scheduled_tasks SET state = 'pending', publish_attempts = publish_attempts + 1, last_publish_error = $2, run_at = $3, updated_at = now() WHERE id = $1 AND state = 'publishing';`
//	_, err := repo.pool.Exec(ctx, q, id, errMsg, nextRunAt)
//	return fmt.Errorf("mark pending after publish failure for %d: %w", id, err)
//}

//func (repo *PostgresRepo) ResetStuckPublishing(ctx context.Context, olderThan time.Duration) (int64, error) {
//	const q = `UPDATE scheduled_tasks SET state = 'pending', updated_at = now() WHERE state = 'publishing' AND publishing_at IS NOT NULL AND published_at < now() - ($1::interval);`
//	ct, err := repo.pool.Exec(ctx, q, olderThan.String())
//	if err != nil {
//		return 0, err
//	}
//	return ct.RowsAffected(), nil
//}
