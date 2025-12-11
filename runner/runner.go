package runner

import (
	"context"
	"github.com/ecociel/when/uc"
	"log"
	"time"
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
	ticker := time.NewTicker(r.Every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Process(ctx, r.Limit); err != nil {
				log.Printf("scheduler process error: %v", err)
			}
		}
	}
}
