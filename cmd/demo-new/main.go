package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Payload struct {
	Action string `json:"action"`
	Seq    int    `json:"seq"`
}

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://scheduler:scheduler@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	seq := 0
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("demo inserter started")

	for range ticker.C {
		seq++

		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			log.Println("begin:", err)
			continue
		}

		payload, _ := json.Marshal(Payload{Action: "demo", Seq: seq})
		runAt := time.Now().Add(5 * time.Second)

		_, err = tx.Exec(ctx, `
			INSERT INTO scheduled_tasks (topic, payload, state, run_at, paused)
			VALUES ($1, $2, 'pending', $3, FALSE)
		`, "tasks.queue", payload, runAt)

		if err != nil {
			_ = tx.Rollback(ctx)
			log.Println("insert:", err)
			continue
		}

		if err := tx.Commit(ctx); err != nil {
			log.Println("commit:", err)
			continue
		}

		log.Printf("scheduled seq=%d runAt=+5s\n", seq)
	}
}
