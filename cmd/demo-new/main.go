package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ecociel/when/domain"
	"github.com/ecociel/when/gateway/kafka"
	"github.com/ecociel/when/repos/sql"
	"github.com/ecociel/when/runner"
	"github.com/ecociel/when/uc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

type Payload struct {
	Action string `json:"action"`
	Seq    int    `json:"seq"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pgDSN := "postgres://scheduler:scheduler@localhost:5432/postgres?sslmode=disable"
	brokers := []string{"localhost:9092"}
	taskTopic := "tasks.queue"

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()

	kClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("demo-all-in-one"),
	)
	if err != nil {
		log.Fatalf("kafka connect: %v", err)
	}
	defer kClient.Close()

	store := sql.NewPostgresRepo(pool)
	publisher := kafka.NewPublisher(kClient, taskTopic)

	processDue := uc.MakeProcessDueTasksUseCase(store, publisher)
	scheduleTask := uc.MakeScheduleUseCase(store)

	go func() {
		log.Println("scheduler runner started")
		runner.NewRunner(processDue, 100, 500*time.Millisecond).Run(ctx)
	}()

	go func() {
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup("demo-workers"),
			kgo.ConsumeTopics(taskTopic),
			kgo.ConsumeTopics("email.send"),
		)
		if err != nil {
			log.Fatalf("worker kafka client: %v", err)
		}
		defer consumer.Close()

		log.Println("worker started")

		for ctx.Err() == nil {
			fetches := consumer.PollFetches(ctx)
			fetches.EachError(func(topic string, partition int32, err error) {
				log.Printf("worker fetch error topic=%s partition=%d: %v", topic, partition, err)
			})

			fetches.EachRecord(func(r *kgo.Record) {
				var p Payload
				if err := json.Unmarshal(r.Value, &p); err != nil {
					log.Printf("bad payload: %v", err)
					return
				}

				log.Printf("run action=%s seq=%d", p.Action, p.Seq)

				// Intentional failure for checking the behaviour of code
				if rand.Intn(2) == 0 {
					log.Printf("seq=%d failed → reschedule in 3s", p.Seq)

					retry := &domain.Task{
						Topic:   taskTopic,
						Payload: r.Value,
						RunAt:   time.Now().Add(3 * time.Second),
					}
					if _, err := scheduleTask(ctx, retry); err != nil {
						log.Printf("reschedule failed: %v", err)
					}
					return
				}

				log.Printf("seq=%d succeeded", p.Seq)
			})
		}
	}()

	go func() {
		ticker := time.NewTicker(4 * time.Second)
		defer ticker.Stop()

		seq := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++

				payload := []byte(fmt.Sprintf(`{"action":"demo-email-send","seq":%d}`, seq))
				task := &domain.Task{
					Topic:   "email.send",
					Payload: payload,
					RunAt:   time.Now().Add(5 * time.Second),
				}

				id, err := scheduleTask(ctx, task)
				if err != nil {
					log.Printf("schedule failed: %v", err)
					continue
				}
				log.Printf("scheduled id=%d seq=%d runAt=+5s", id, seq)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		seq := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++

				payload := []byte(fmt.Sprintf(`{"action":"demo-task-queue","seq":%d}`, seq))
				task := &domain.Task{
					Topic:   taskTopic,
					Payload: payload,
					RunAt:   time.Now().Add(5 * time.Second),
				}

				id, err := scheduleTask(ctx, task)
				if err != nil {
					log.Printf("schedule failed: %v", err)
					continue
				}
				log.Printf("scheduled id=%d seq=%d runAt=+5s", id, seq)
			}
		}
	}()
	<-ctx.Done()
	log.Println("shutting down")
}

// partitions need to be decided by id
