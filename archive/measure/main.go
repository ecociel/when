package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ecociel/when/archive/metrics"
	"github.com/ecociel/when/archive/repos/sql"
	uc2 "github.com/ecociel/when/archive/uc"
	"github.com/ecociel/when/lib/observer"
	"github.com/ecociel/when/lib/observer/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ecociel/when/uc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, "postgres://scheduler:scheduler@localhost:5432/postgres?sslmode=disable")
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
	pub := kafka.New(kClient, "")

	reg := prometheus.NewRegistry()
	m := metrics.NewPromMetrics(reg)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		log.Printf("metrics listening on http://localhost%s/metrics", ":9090")
		if err := http.ListenAndServe(":9090", mux); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()

	process := uc2.MakeProcessDueTasksUseCase(store, pub, uc2.CompletionMarkPublished, m)

	// Optional: reclaim stuck publishing rows
	reclaim := uc.MakeReclaimStuckUseCase(store)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := reclaim(ctx, 2*time.Minute)
				if err != nil {
					log.Printf("reclaim error: %v", err)
					continue
				}
				if n > 0 {
					log.Printf("reclaimed %d stuck tasks", n)
					m.TasksReclaimed(int(n))
				}
			}
		}
	}()

	log.Println("relay started")
	observer.New(process, 200, 1*time.Second).Run(ctx)
}
