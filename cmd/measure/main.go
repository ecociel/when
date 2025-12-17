package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ecociel/when/gateway/kafka"
	"github.com/ecociel/when/repos/sql"
	"github.com/ecociel/when/runner"
	"github.com/ecociel/when/uc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	kClient, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()

	store := sql.NewPostgresRepo(pool)
	pub := kafka.NewPublisher(kClient, "")

	process := uc.MakeProcessDueTasksUseCase(store, pub, uc.CompletionMarkPublished)

	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
				//case <-t.C:
				//	n, err := reclaim(ctx, 2*time.Minute)
				//	if err != nil {
				//		log.Printf("reclaim error: %v", err)
				//		continue
				//	}
				//	if n > 0 {
				//		log.Printf("reclaimed %d stuck tasks", n)
				//	}
			}
		}
	}()

	log.Println("relay started")
	runner.NewRunner(process, 200, 1*time.Second).Run(ctx)
}
