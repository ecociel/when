package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ecociel/when/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool}
}

func (repo *PostgresRepo) StoreTask(ctx context.Context, t *domain.Task) (int64, error) {
	const q = `INSERT INTO scheduled_tasks (topic, key, payload, run_at, paused, external_key, triggered)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id;
`
	var payloadObj map[string]any
	if err := json.Unmarshal(t.Payload, &payloadObj); err != nil {
		return 0, fmt.Errorf("serialize payload: %w", err)
	}
	var id int64
	err := repo.pool.QueryRow(ctx, q, t.Topic, t.Key, t.Payload, t.RunAt, t.Paused, t.ExternalKey, t.Triggered).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert task: %w", err)
	}
	return id, err
}

//func (repo *PostgresRepo) Pause(ctx context.Context, id int64) error {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (repo *PostgresRepo) UnPause(ctx context.Context, id int64) error {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (repo *PostgresRepo) Reschedule(ctx context.Context, id int64, when time.Time) error {
//	//TODO implement me
//	panic("implement me")
//}

func (repo *PostgresRepo) ClaimDueTasks(ctx context.Context, now time.Time, limit int) ([]domain.Task, error) {
	const q = `WITH due AS(
SELECT id FROM scheduled_tasks WHERE state = 'pending' AND run_at < $1 AND paused = FALSE AND (external_key IS NULL OR triggered = TRUE) 
ORDER BY run_at FOR UPDATE SKIP LOCKED LIMIT $2)
UPDATE scheduled_tasks t SET state = 'publishing', publishing_at = now(), updated_at = now() FROM due WHERE t.id = due.id RETURNING t.id, t.topic, t.key, t.payload, t.run_at, t.paused, t.external_key, t.triggered, t.state, t.publish_attempts;`

	tx, err := repo.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, q, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []domain.Task
	for rows.Next() {
		var t domain.Task
		if err := rows.Scan(&t.ID, &t.Topic, &t.Key, &t.Payload, &t.RunAt, &t.Paused, &t.ExternalKey, &t.Triggered, &t.State, &t.PublishAttempts); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (repo *PostgresRepo) DeleteTaskByID(ctx context.Context, id int64) error {
	const q = `DELETE FROM scheduled_tasks WHERE id = $1;`
	_, err := repo.pool.Exec(ctx, q, id)
	return err
}

func (repo *PostgresRepo) RevertTaskToPending(ctx context.Context, id int64, nextRunAt time.Time) error {
	const q = `UPDATE scheduled_tasks SET status = 'pending', run_at = $2, updated_at = now() WHERE id = $1 AND status = 'publishing';`
	_, err := repo.pool.Exec(ctx, q, id, nextRunAt)
	if err != nil {
		return err
	}
	return nil
}

func (repo *PostgresRepo) MarkPublished(ctx context.Context, id int64) error {
	const q = `UPDATE scheduled_tasks SET state = 'published', published_at = now(), last_publish_error = NULL, updated_at = now() WHERE id = $1 AND state = 'publishing';`
	_, err := repo.pool.Exec(ctx, q, id)
	return err
}
func (repo *PostgresRepo) MarkPublishFailed(ctx context.Context, id int64, errMsg string, nextRunAt time.Time) error {
	const q = `UPDATE scheduled_tasks SET state = 'pending', publish_attempts = publish_attempts + 1, last_publish_error = $2, run_at = $3, updated_at = now() WHERE id = $1 AND state = 'publishing';`
	_, err := repo.pool.Exec(ctx, q, id, errMsg, nextRunAt)
	return err
}

func (repo *PostgresRepo) ResetStuckPublishing(ctx context.Context, olderThan time.Duration) (int64, error) {
	const q = `UPDATE scheduled_tasks SET state = 'pending', updated_at = now() WHERE state = 'publishing' AND publishing_at IS NOT NULL AND published_at < now() - ($1::interval);`
	ct, err := repo.pool.Exec(ctx, q, olderThan.String())
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}
