package main

import (
	"context"
	"github.com/ecociel/when/domain"
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
	defer kClient.Close()

	store := sql.NewPostgresRepo(pool)

	publisher := kafka.NewPublisher(kClient, "")

	scheduleTask := uc.MakeScheduleUseCase(store)
	uc.MakePauseUseCase(store)
	uc.MakeUnPauseUseCase(store)
	uc.MakeRescheduleUseCase(store)
	uc.MakeProcessDueTasksUseCase(store, publisher)

	task := &domain.Task{}

	taskId, err := scheduleTask(ctx, task)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("task ID: %d", taskId)

}
