package runner

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ecociel/when/lib/domain"
	"github.com/ecociel/when/lib/observer/kafka"
	"github.com/ecociel/when/lib/observer/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Runner struct {
	limit     int
	interval  time.Duration
	store     store
	publisher publisher
}

type publisher interface {
	PublishSync(ctx context.Context, task domain.Task) error
}

type store interface {
	ClaimDueTasks(ctx context.Context, limit int) ([]domain.Task, error)
	Delete(ctx context.Context, id uint64) error
}

func New(limit int, interval time.Duration, pool *pgxpool.Pool, client *kgo.Client, topic string) *Runner {
	return &Runner{
		limit:     limit,
		interval:  interval,
		store:     postgres.New(pool),
		publisher: kafka.New(client, topic),
	}
}

func (r *Runner) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.interval):
			if err := r.process(ctx); err != nil {
				log.Printf("scheduler process error: %v", err)
			}
		}
	}
}

func (r *Runner) process(ctx context.Context) error {
	tasks, err := r.store.ClaimDueTasks(ctx, r.limit)
	if err != nil {
		return fmt.Errorf("fetching due tasks: %w", err)
	}
	//m.TaskClaimed(len(tasks))
	log.Printf("claimed %d due tasks", len(tasks))

	for _, task := range tasks {
		if err := r.publisher.PublishSync(ctx, task); err != nil {
			log.Printf("publish failed for %d: %v", task.ID, err)
			continue
		}
		//m.PublishLatency(time.Since(start))
		//m.TaskPublished()
		if err := r.store.Delete(ctx, task.ID); err != nil {
			return fmt.Errorf("deletion failed for %d: %w", task.ID, err)
		}

	}
	return nil
}
