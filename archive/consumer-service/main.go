package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/ecociel/when/archive/repos/sql"
	"github.com/ecociel/when/domain"
	"github.com/ecociel/when/uc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ExploreEvent struct {
	Type   string `json:"type"`
	UserID int    `json:"userId"`
}

func main() {
	ctx := context.Background()

	// --- scheduler wiring ---
	pg, _ := pgxpool.New(ctx, "postgres://scheduler:scheduler@localhost:5432/postgres?sslmode=disable")
	store := sql.NewPostgresRepo(pg)

	//kafkaProducer, _ := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	//publisher := kafka.NewPublisher(kafkaProducer, "tasks.queue")

	scheduleTask := uc.MakeScheduleUseCase(store)

	// events consumer
	kafkaConsumer, _ := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("explore-event-consumer"),
		kgo.ConsumeTopics("tasks.queue"),
	)

	for {
		fetches := kafkaConsumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Println("poll error:", errs)
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			var evt ExploreEvent
			_ = json.Unmarshal(record.Value, &evt)

			log.Println("received explore event:", evt)

			// decide if action should be scheduled
			if evt.Type == "USER_UPDATED" {
				// schedule a future task
				payload := []byte(`{"action":"sync_user","userId":` +
					json.Number(rune(evt.UserID)).String() + `}`)

				task := &domain.Task{
					Name: "tasks.queue",
					Args: payload,
					Due:  time.Now().Add(10 * time.Minute),
				}

				id, err := scheduleTask(ctx, task)
				if err != nil {
					log.Println("failed to schedule:", err)
				} else {
					log.Println("scheduled future task:", id)
				}
			}
		})
	}
}
