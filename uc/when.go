package uc

import (
	"context"
	"github.com/ecociel/when/domain"
	"github.com/emicklei/go-restful/v3/log"
	"time"
)

// EventPublisher TODO
type EventPublisher interface {
	PublishSync(ctx context.Context, topic string, key []byte, value []byte) error
}

type DueTaskStore interface {
	ClaimDueTasks(ctx context.Context, now time.Time, limit int) ([]domain.Task, error)
	DeleteTaskByID(ctx context.Context, id int64) error
	RevertTaskToPending(ctx context.Context, id int64, nextRunAt time.Time) error
}

type ProcessDueTasksUseCase = func(ctx context.Context, limit int) error

func MakeProcessDueTasksUseCase(
	store DueTaskStore,
	publisher EventPublisher,
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

			if err := publisher.PublishSync(ctx, t.Topic, key, t.Payload); err != nil {
				_ = store.RevertTaskToPending(ctx, t.ID, now.Add(time.Minute))
				log.Printf("reverting task %v: %v", t.ID, err)
				continue
			}
			//TODO wht if delete fail?
			// what if worker dies in between?
			err = store.DeleteTaskByID(ctx, t.ID)
			if err != nil {
				log.Printf("deleting task %v: %v", t.ID, err)
			}
		}
		return nil
	}
}
