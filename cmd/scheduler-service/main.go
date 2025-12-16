package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ecociel/when/gateway/kafka"
	"github.com/ecociel/when/repos/sql"
	"github.com/ecociel/when/runner"
	"github.com/ecociel/when/uc"
)

func main() {
	ctx := context.Background()

	pg, _ := pgxpool.New(ctx, "postgres://scheduler:scheduler@localhost:5432/postgres?sslmode=disable")
	store := sql.NewPostgresRepo(pg)

	kafkaClient, _ := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	publisher := kafka.NewPublisher(kafkaClient, "tasks.queue")

	processDue := uc.MakeProcessDueTasksUseCase(store, publisher)

	log.Println("Scheduler started...")

	run := runner.NewRunner(processDue, 100, 2*time.Second)

	run.Run(ctx)
}
