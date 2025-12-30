package worker

import (
	"context"
	"log"
	"time"

	"github.com/ecociel/when/lib/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler func(id string, data []byte) error

// Worker subscribes to tas queue and executes corresponding actions
// Workers can be run in parallel.
type Worker struct {
	client   *kgo.Client
	handlers map[string]Handler
}

func New(client *kgo.Client) *Worker {
	return &Worker{client: client, handlers: make(map[string]Handler)}
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

		fetches.EachRecord(func(record *kgo.Record) {

			name := name(record.Headers)
			id := id(record.Headers)

			hdl, ok := w.handlers[name]
			if !ok {
				log.Printf("Unkown task: %q", name)
				return
			}

			if err := hdl(id, record.Value); err != nil {
				log.Printf("handle %s/%s: %v", name, id, err)
				// TODO what to do here? Re-schedule?
			}

			// TODO do anything here? delete task from table?
			//Delete or Done(ctx context.Context, id int64) error

		})
		if fetches.NumRecords() == 0 {
			log.Printf("no records, sleep 1s")
			time.Sleep(1000 * time.Millisecond)
		}
	}

}

func name(headers []kgo.RecordHeader) string {
	for i := range headers {
		if headers[i].Key == domain.HeaderTaskName {
			return string(headers[i].Value)
		}
	}
	return ""
}
func id(headers []kgo.RecordHeader) string {
	for i := range headers {
		if headers[i].Key == domain.HeaderTaskID {
			return string(headers[i].Value)
		}
	}
	return ""
}
