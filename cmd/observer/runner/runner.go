package runner

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ecociel/when/lib/domain"
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

func New(limit int, interval time.Duration, store store, publisher publisher) *Runner {
	return &Runner{
		limit:     limit,
		interval:  interval,
		store:     store,
		publisher: publisher,
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
		//start := time.Now()

		if err := r.publisher.PublishSync(ctx, task); err != nil {
			// implement reschedule delay on publish error with backoff
			//next := now.Add(backoff(t.PublishAttempts + 1))
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
