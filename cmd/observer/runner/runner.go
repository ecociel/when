package runner

import (
	"context"
	"log"
	"time"

	"github.com/ecociel/when/uc"
)

type Runner struct {
	Process uc.ProcessDueTasksUseCase
	Limit   int
	Every   time.Duration
}

func NewRunner(process uc.ProcessDueTasksUseCase, limit int, interval time.Duration) *Runner {
	return &Runner{
		Process: process,
		Limit:   limit,
		Every:   interval,
	}
}

func (r *Runner) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.Every):
			if err := r.Process(ctx, r.Limit); err != nil {
				log.Printf("scheduler process error: %v", err)
			}
		}
	}
}
