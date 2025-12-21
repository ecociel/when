package uc

import (
	"context"
	"github.com/ecociel/when/domain"
)

//TODO Need rework

type Writer interface {
	StoreTask(ctx context.Context, t *domain.Task) (int64, error)
	//Pause(ctx context.Context, id int64) error
	//UnPause(ctx context.Context, id int64) error
	//Reschedule(ctx context.Context, id int64, when time.Time) error
}
type ScheduleUseCase = func(ctx context.Context, t *domain.Task) (int64, error)

//type PauseUseCase = func(ctx context.Context, id int64) error
//type UnPauseUseCase = func(ctx context.Context, id int64) error
//type RescheduleUseCase = func(ctx context.Context, id int64, when time.Time) error

func MakeScheduleUseCase(w Writer) ScheduleUseCase {
	return func(ctx context.Context, t *domain.Task) (int64, error) {
		return w.StoreTask(ctx, t)
	}
}

//func MakePauseUseCase(w Writer) PauseUseCase {
//	return func(ctx context.Context, id int64) error {
//		return w.Pause(ctx, id)
//	}
//}
//func MakeUnPauseUseCase(w Writer) UnPauseUseCase {
//	return func(ctx context.Context, id int64) error {
//		return w.UnPause(ctx, id)
//	}
//}
//func MakeRescheduleUseCase(w Writer) RescheduleUseCase {
//	return func(ctx context.Context, id int64, when time.Time) error {
//		return w.Reschedule(ctx, id, when)
//	}
//}
