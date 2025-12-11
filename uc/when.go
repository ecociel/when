package uc

import (
	"context"
	"github.com/ecociel/when/domain"
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

		for _, t := range tasks {
			var key []byte
			if t.Key != nil {
				key = []byte(*t.Key)
			}

			if err := publisher.PublishSync(ctx, t.Topic, key, t.Payload); err != nil {
				_ = store.RevertTaskToPending(ctx, t.ID, now.Add(time.Minute))
				continue
			}
			//TODO
			_ = store.DeleteTaskByID(ctx, t.ID)
		}
		return nil
	}
}
