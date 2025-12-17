package uc

import (
	"context"
	"fmt"
	"github.com/ecociel/when/domain"
	"github.com/emicklei/go-restful/v3/log"
	"math"
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

	MarkPublishFailed(ctx context.Context, id int64) error

	// ResetStuckPublished TODO for may be crash recovery
	ResetStuckPublished(ctx context.Context, id int64) error
}

type ProcessDueTasksUseCase = func(ctx context.Context, limit int) error

func MakeProcessDueTasksUseCase(
	store DueTaskStore,
	publisher EventPublisher,
	mode PublishCompletionMode,
) ProcessDueTasksUseCase {
	return func(ctx context.Context, limit int) error {
		now := time.Now()

		tasks, err := store.ClaimDueTasks(ctx, now, limit)
		if err != nil {
			return err
		}
		log.Printf("ClaimDueTasks: %v", tasks)

		for _, t := range tasks {
			var key []byte
			if t.Key != nil {
				key = []byte(*t.Key)
			}

			headers := map[string][]byte{
				"scheduler_task_id": []byte(int64ToString(t.ID)),
			}

			if err := publisher.PublishSync(ctx, t.Topic, key, t.Payload, headers); err != nil {
				now.Add(backoff(t.PublishAttempts + 1))
				err := store.MarkPublishFailed(ctx, t.ID)
				if err != nil {
					return err
				}
				//_ = store.RevertTaskToPending(ctx, t.ID, now.Add(time.Minute))
				log.Printf("reverting task %v: %v", t.ID, err)
				continue
			}
			//TODO wht if delete fail?
			// what if worker dies in between?
			err := store.MarkPublished(ctx, t.ID)
			if err != nil {
				return err
			}

			if mode == CompletionMarkPublished {
				err := store.DeleteTaskByID(ctx, t.ID)
				if err != nil {
					return err
				}
			}
			//	err = store.DeleteTaskByID(ctx, t.ID)
			//	if err != nil {
			//		log.Printf("deleting task %v: %v", t.ID, err)
			//	}
		}
		return nil
	}
}

func backoff(attempt int) time.Duration {
	return time.Duration(math.Pow(2, float64(attempt))) * time.Second
}

func int64ToString(id int64) []byte {
	return []byte(fmt.Sprintf("%d", id))
}
