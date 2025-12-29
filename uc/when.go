package uc

import (
	"context"
	"fmt"
	"time"

	"github.com/ecociel/when/domain"
	"github.com/emicklei/go-restful/v3/log"
)

type PublishCompletionMode int

const (
	CompletionDelete PublishCompletionMode = iota
	CompletionMarkPublished
)

// EventPublisher TODO
type EventPublisher interface {
	PublishSync(ctx context.Context, task domain.Task) error
}

type DueTaskStore interface {
	ClaimDueTasks(ctx context.Context, now time.Time, limit int) ([]domain.Task, error)
	//DeleteTaskByID(ctx context.Context, id int64) error
	//RevertTaskToPending(ctx context.Context, id int64, nextRunAt time.Time) error

	MarkPublished(ctx context.Context, id int64) error
	//
	//MarkPublishFailed(ctx context.Context, id int64, errMsg string, nextRunAt time.Time) error
	//ResetStuckPublishing(ctx context.Context, olderThan time.Duration) (int64, error)
}

type ProcessDueTasksUseCase = func(ctx context.Context, limit int) error

//type ReclaimStuckUseCase = func(ctx context.Context, olderThan time.Duration) (int64, error)

func MakeProcessDueTasksUseCase(
	store DueTaskStore,
	publisher EventPublisher,
	mode PublishCompletionMode,
//m metrics.SchedulerMetrics,
) ProcessDueTasksUseCase {
	return func(ctx context.Context, limit int) error {
		now := time.Now()

		tasks, err := store.ClaimDueTasks(ctx, now, limit)
		if err != nil {
			return fmt.Errorf("fetching due tasks: %w", err)
		}
		//m.TaskClaimed(len(tasks))
		log.Printf("ClaimDueTasks: %v", tasks)

		for _, task := range tasks {
			//start := time.Now()

			if err := publisher.PublishSync(ctx, task); err != nil {
				////m.TaskPublishFailed()
				//next := now.Add(backoff(t.PublishAttempts + 1))
				//err := store.MarkPublishFailed(ctx, t.ID, err.Error(), next)
				//if err != nil {
				//	return err
				//}
				//log.Printf("reverting task %v: %v", t.ID, err)
				log.Printf("publish failed for %d: %v", task.ID, err)
				continue
			}
			//m.PublishLatency(time.Since(start))
			//m.TaskPublished()
			err := store.MarkPublished(ctx, task.ID)
			if err != nil {
				return fmt.Errorf("mark published failed for %d: %w", task.ID, err)
			}

			//if mode == CompletionDelete {
			//	err := store.DeleteTaskByID(ctx, t.ID)
			//	if err != nil {
			//		return err
			//	}
			//}
		}
		return nil
	}
}

//func MakeReclaimStuckUseCase(store DueTaskStore) ReclaimStuckUseCase {
//	return func(ctx context.Context, olderThan time.Duration) (int64, error) {
//		return store.ResetStuckPublishing(ctx, olderThan)
//	}
//}

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
