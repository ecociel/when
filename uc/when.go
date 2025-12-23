package uc

import (
	"context"
	"github.com/ecociel/when/domain"
	"github.com/ecociel/when/metrics"
	"github.com/emicklei/go-restful/v3/log"
	"strconv"
	"time"
)

type PublishCompletionMode int

const (
	CompletionDelete PublishCompletionMode = iota
	CompletionMarkPublished
)

// EventPublisher TODO
type EventPublisher interface {
	PublishSync(ctx context.Context, topic string, key []byte, value []byte, headers map[string][]byte) error
}

type DueTaskStore interface {
	ClaimDueTasks(ctx context.Context, now time.Time, limit int) ([]domain.Task, error)
	DeleteTaskByID(ctx context.Context, id int64) error
	RevertTaskToPending(ctx context.Context, id int64, nextRunAt time.Time) error

	MarkPublished(ctx context.Context, id int64) error

	MarkPublishFailed(ctx context.Context, id int64, errMsg string, nextRunAt time.Time) error
	ResetStuckPublishing(ctx context.Context, olderThan time.Duration) (int64, error)
}

type ProcessDueTasksUseCase = func(ctx context.Context, limit int) error
type ReclaimStuckUseCase = func(ctx context.Context, olderThan time.Duration) (int64, error)

func MakeProcessDueTasksUseCase(
	store DueTaskStore,
	publisher EventPublisher,
	mode PublishCompletionMode,
	m metrics.SchedulerMetrics,
) ProcessDueTasksUseCase {
	return func(ctx context.Context, limit int) error {
		now := time.Now()

		tasks, err := store.ClaimDueTasks(ctx, now, limit)
		if err != nil {
			return err
		}
		m.TaskClaimed(len(tasks))
		log.Printf("ClaimDueTasks: %v", tasks)

		for _, t := range tasks {
			start := time.Now()
			var key []byte
			if t.Key != nil {
				key = []byte(*t.Key)
			}

			headers := map[string][]byte{
				"scheduler_task_id": []byte(strconv.FormatInt(t.ID, 10)),
			}

			if err := publisher.PublishSync(ctx, t.Topic, key, t.Payload, headers); err != nil {
				m.TaskPublishFailed()
				next := now.Add(backoff(t.PublishAttempts + 1))
				err := store.MarkPublishFailed(ctx, t.ID, err.Error(), next)
				if err != nil {
					return err
				}
				log.Printf("reverting task %v: %v", t.ID, err)
				continue
			}
			m.PublishLatency(time.Since(start))
			m.TaskPublished()
			err := store.MarkPublished(ctx, t.ID)
			if err != nil {
				return err
			}

			if mode == CompletionDelete {
				err := store.DeleteTaskByID(ctx, t.ID)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func MakeReclaimStuckUseCase(store DueTaskStore) ReclaimStuckUseCase {
	return func(ctx context.Context, olderThan time.Duration) (int64, error) {
		return store.ResetStuckPublishing(ctx, olderThan)
	}
}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 5 * time.Second
	case 2:
		return 15 * time.Second
	case 3:
		return 1 * time.Minute
	default:
		return 5 * time.Minute
	}
}
