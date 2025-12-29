package runner

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ecociel/when/lib/domain"
	log2 "github.com/emicklei/go-restful/v3/log"
)

type PublishCompletionMode int

const (
	CompletionDelete PublishCompletionMode = iota
	CompletionMarkPublished
)

type Runner struct {
	limit     int
	interval  time.Duration
	store     store
	publisher publisher
	mode      PublishCompletionMode
}

type publisher interface {
	PublishSync(ctx context.Context, task domain.Task) error
}

type store interface {
	ClaimDueTasks(ctx context.Context, limit int) ([]domain.Task, error)
	MarkPublished(ctx context.Context, id int64) error
	Delete(ctx context.Context, id int64) error
}

func New(limit int, interval time.Duration, store store, publisher publisher, mode PublishCompletionMode) *Runner {
	return &Runner{
		limit:     limit,
		interval:  interval,
		store:     store,
		publisher: publisher,
		mode:      mode,
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
	log.Printf("ClaimDueTasks: %v", tasks)

	for _, task := range tasks {
		//start := time.Now()

		if err := r.publisher.PublishSync(ctx, task); err != nil {
			// implement reschedule delay on publish error with backoff
			//next := now.Add(backoff(t.PublishAttempts + 1))
			log2.Printf("publish failed for %d: %v", task.ID, err)
			continue
		}
		//m.PublishLatency(time.Since(start))
		//m.TaskPublished()
		switch r.mode {
		case CompletionDelete:
			err := r.store.Delete(ctx, task.ID)
			if err != nil {
				return fmt.Errorf("deletion failed for %d: %w", task.ID, err)
			}
		case CompletionMarkPublished:
			err := r.store.MarkPublished(ctx, task.ID)
			if err != nil {
				return fmt.Errorf("mark published failed for %d: %w", task.ID, err)
			}
		default:
			log.Fatalf("unknown completion mode %d", r.mode)
		}

	}
	return nil
}

// implement reschedule delay on publish error with backoff
//func backoff(attempt int) time.Duration {
//	switch attempt {
//	case 1:
//		return 5 * time.Second
//	case 2:
//		return 15 * time.Second
//	case 3:
//		return 1 * time.Minute
//	default:
//		return 5 * time.Minute
//	}
//}
