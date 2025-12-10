package main

import (
	"context"
	"github.com/ecociel/when/gateway/kafka"
	"github.com/ecociel/when/repos/sql"
	"github.com/ecociel/when/uc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	kClient, err := kgo.NewClient(kgo.SeedBrokers(""))
	if err != nil {
		log.Fatal(err)
	}

	store := sql.NewPostgresRepo(pool)

	_ = kafka.NewPublisher(kClient, "")

	uc.MakeScheduleUseCase(store)

}
