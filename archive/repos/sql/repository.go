package sql

import (
	"context"
	"fmt"

	"github.com/ecociel/when/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool}
}

func (repo *PostgresRepo) StoreTask(ctx context.Context, t *domain.Task) (int64, error) {
	const q = `INSERT INTO task (name, payload, due  )
VALUES ($1, $2, $3 )
RETURNING id;
`
	//var payloadObj map[string]any
	//if err := json.Unmarshal(t.Args, &payloadObj); err != nil {
	//	return 0, fmt.Errorf("serialize payload: %w", err)
	//}
	var id int64
	err := repo.pool.QueryRow(ctx, q, t.Name, t.Args, t.Due).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert task: %w", err)
	}
	return id, err
}

//func (repo *PostgresRepo) DeleteTaskByID(ctx context.Context, id int64) error {
//	const q = `DELETE FROM task WHERE id = $1;`
//	_, err := repo.pool.Exec(ctx, q, id)
//	return err
//}

//func (repo *PostgresRepo) RevertTaskToPending(ctx context.Context, id int64, nextRunAt time.Time) error {
//	const q = `UPDATE scheduled_tasks SET status = 'pending', run_at = $2, updated_at = now() WHERE id = $1 AND status = 'publishing';`
//	_, err := repo.pool.Exec(ctx, q, id, nextRunAt)
//	if err != nil {
//		return err
//	}
//	return nil
//}
