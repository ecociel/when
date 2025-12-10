package uc

import "context"

//TODO Need rework

type Writer interface {
	Insert(ctx context.Context, kind string, event any) error
	Pause(ctx context.Context, kind string, event any) error
	UnPause(ctx context.Context, kind string, event any) error
	Reschedule(ctx context.Context, kind string, event any) error
}
type ScheduleUseCase = func(ctx context.Context, kind string, event any) error
type PauseUseCase = func(ctx context.Context, kind string, event any) error
type UnPauseUseCase = func(ctx context.Context, kind string, event any) error
type RescheduleUseCase = func(ctx context.Context, kind string, event any) error

func MakeScheduleUseCase(w Writer) ScheduleUseCase {
	return func(ctx context.Context, kind string, event any) error {
		return w.Insert(ctx, kind, event)
	}
}
func MakePauseUseCase(w Writer) PauseUseCase {
	return func(ctx context.Context, kind string, event any) error {
		return w.Pause(ctx, kind, event)
	}
}
func MakeUnPauseUseCase(w Writer) UnPauseUseCase {
	return func(ctx context.Context, kind string, event any) error {
		return w.UnPause(ctx, kind, event)
	}
}
func MakeRescheduleUseCase(w Writer) RescheduleUseCase {
	return func(ctx context.Context, kind string, event any) error {
		return w.Reschedule(ctx, kind, event)
	}
}
