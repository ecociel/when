package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ecociel/when/domain"
	"github.com/ecociel/when/repos/sql"
	"github.com/ecociel/when/uc"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TaskPayload struct {
	Action string `json:"action"`
	UserID int    `json:"userId"`
}

func main() {
	ctx := context.Background()

	// To schedule retries:
	pg, _ := pgxpool.New(ctx, "postgres://scheduler:scheduler@localhost:5432/postgres?sslmode=disable")
	store := sql.NewPostgresRepo(pg)
	scheduleTask := uc.MakeScheduleUseCase(store)

	kafkaConsumer, _ := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("task-workers"),
		kgo.ConsumeTopics("tasks.queue"),
	)

	for {
		fetches := kafkaConsumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Println("worker poll error:", errs)
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {

			var payload TaskPayload
			_ = json.Unmarshal(record.Value, &payload)

			log.Println("Worker received:", payload)

			// Action routing
			switch payload.Action {
			case "sync_user":
				err := runSyncUser(payload.UserID)
				if err != nil {
					log.Println("sync_user failed:", err)

					// Schedule retry
					retry := &domain.Task{
						Topic:   "tasks.queue",
						Payload: record.Value,
						RunAt:   time.Now().Add(2 * time.Minute),
					}
					_, _ = scheduleTask(ctx, retry)
				}
			default:
				log.Println("Unknown action:", payload.Action)
			}
		})
	}
}

func runSyncUser(id int) error {
	// pretend sometimes it fails
	if time.Now().Unix()%2 == 0 {
		return fmt.Errorf("random failure")
	}
	log.Println("synced user:", id)
	return nil
}
