package sql

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func (repo *PostgresRepo) Insert(ctx context.Context, kind string, event any) error {
	//TODO implement me
	panic("implement me")
}

func (repo *PostgresRepo) Pause(ctx context.Context, kind string, event any) error {
	//TODO implement me
	panic("implement me")
}

func (repo *PostgresRepo) UnPause(ctx context.Context, kind string, event any) error {
	//TODO implement me
	panic("implement me")
}

func (repo *PostgresRepo) Reschedule(ctx context.Context, kind string, event any) error {
	//TODO implement me
	panic("implement me")
}

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool}
}
