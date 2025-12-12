package main

import (
	"context"
	"fmt"
	"github.com/ecociel/when/domain"
	"github.com/ecociel/when/gateway/kafka"
	"github.com/ecociel/when/repos/sql"
	"github.com/ecociel/when/runner"
	"github.com/ecociel/when/uc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"time"
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://scheduler:scheduler@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Unable to connect to Postgres: %v", err)
	}

	fmt.Println("Connected to Postgres successfully!")

	kClient, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()

	store := sql.NewPostgresRepo(pool)

	publisher := kafka.NewPublisher(kClient, "")

	scheduleTask := uc.MakeScheduleUseCase(store)
	uc.MakePauseUseCase(store)
	uc.MakeUnPauseUseCase(store)
	uc.MakeRescheduleUseCase(store)
	process := uc.MakeProcessDueTasksUseCase(store, publisher)

	go runner.NewRunner(process, 100, 2*time.Second).Run(ctx)

	task := &domain.Task{
		Topic:   "email.send",
		Payload: []byte(`{"email":"user@example.com","subject":"Hello"}`),
		RunAt:   time.Now().Add(10 * time.Second),
	}

	taskId, err := scheduleTask(ctx, task)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("task ID: %d", taskId)
	select {}
}
