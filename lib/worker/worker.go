package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"time"

	"github.com/ecociel/when/lib/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler func(id uint64, data []byte) error

// Worker subscribes to tas queue and executes corresponding actions
// Workers can be run in parallel.
type Worker struct {
	client   *kgo.Client
	pool     *pgxpool.Pool
	handlers map[string]Handler
}

func New(client *kgo.Client, pool *pgxpool.Pool) *Worker {
	return &Worker{client: client, pool: pool, handlers: make(map[string]Handler)}
}

func (w *Worker) RegisterHandler(name string, hdl Handler) {
	w.handlers[name] = hdl
}

func (w *Worker) Run(ctx context.Context) {

	for {
		fetches := w.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			log.Println("consuming client closed, returning")
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("fetch err topic %s partition %d: %v", t, p, err)
		})
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Println("poll error:", errs)
			continue
		}

		for record := range fetches.RecordsAll() {
			task := recToTask(record)
			hdl, ok := w.handlers[task.Name]
			if !ok {
				log.Printf("Unkown task: %q", task.Name)
				return
			}

			if err := hdl(task.ID, task.Args); err != nil {
				log.Printf("handle %s/%d: %v", task.Name, task.ID, err)
				setReschedule(&task, time.Now(), err)
				if err := w.ReSchedule(ctx, task); err != nil {
					log.Printf("reschedule %s/%d: %v", task.Name, task.ID, err)
					break
				}
			}
			if err := w.client.CommitRecords(ctx, record); err != nil {
				log.Printf("commit record for %s/%d: %v", task.Name, task.ID, err)
				break
			}

		}
	}

}

func setReschedule(task *domain.Task, now time.Time, err error) {
	task.RetryCount++
	task.RetryReason = err.Error()
	//task.Due = task.Due.Add(calculateBackoff(task.RetryCount, 30*time.Second, 1*time.Minute))
	task.Due = now.Add(calculateBackoff(task.RetryCount, 30*time.Second, 1*time.Minute))
}

func recToTask(rec *kgo.Record) (task domain.Task) {
	task.PartitionKey = string(rec.Key)
	task.Args = rec.Value
	for i := range rec.Headers {
		switch rec.Headers[i].Key {
		case domain.HeaderID:
			task.ID = binary.BigEndian.Uint64(rec.Headers[i].Value)
		case domain.HeaderName:
			task.Name = string(rec.Headers[i].Value)
		case domain.HeaderRetryCount:
			task.RetryCount = binary.BigEndian.Uint16(rec.Headers[i].Value)
		case domain.HeaderRetryReason:
			task.RetryReason = string(rec.Headers[i].Value)
		}
	}
	return
}

func (w *Worker) ReSchedule(ctx context.Context, task domain.Task) error {
	const q = `
        INSERT INTO task
          (id, name, partition_key, args, due, retry_count, retry_reason)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        `
	_, err := w.pool.Exec(ctx, q, task.ID, task.Name, task.PartitionKey, task.Args, task.Due, task.RetryCount, task.RetryReason)
	if err != nil {
		return fmt.Errorf("reschedeule insert task: %w", err)
	}
	log.Printf("Rescheduled PrintCount %s/%d: %s", task.Name, task.ID, string(task.Args))
	return nil
}

func calculateBackoff(retryCount uint16, baseDelay, maxDelay time.Duration) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}

	// Compute exponential factor: 2^retryCount
	expFactor := math.Pow(2, float64(retryCount))

	// Base exponential delay
	delay := time.Duration(float64(baseDelay) * expFactor)

	// Apply full jitter: random between 0 and delay (equivalent to +/-50% on average)
	// Alternative: for +/-50% jitter, use delay/2 + rand(0 to delay)
	jitter := time.Duration(rand.Float64() * float64(delay))
	delay = jitter

	// Cap the delay if maxDelay is specified
	if maxDelay > 0 && delay > maxDelay {
		delay = maxDelay
	}

	return delay
}
