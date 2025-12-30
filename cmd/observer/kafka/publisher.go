package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ecociel/when/lib/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	client *kgo.Client
}

func New(client *kgo.Client) *Publisher {
	return &Publisher{client: client}
}

func (p *Publisher) PublishSync(ctx context.Context, task domain.Task) error {

	record := taskToRec(task)
	if err := p.client.ProduceSync(ctx, &record).FirstErr(); err != nil {
		return fmt.Errorf("publish event: %w\n", err)
	}

	return nil
}

func taskToRec(task domain.Task) (rec kgo.Record) {
	id := make([]byte, 8)
	binary.BigEndian.PutUint64(id, task.ID)
	var headers []kgo.RecordHeader
	headers = append(headers, kgo.RecordHeader{Key: domain.HeaderID, Value: id})
	headers = append(headers, kgo.RecordHeader{Key: domain.HeaderName, Value: []byte(task.Name)})
	if task.RetryCount > 0 {
		cnt := make([]byte, 2)
		binary.BigEndian.PutUint16(cnt, task.RetryCount)
		headers = append(headers, kgo.RecordHeader{Key: domain.HeaderRetryCount, Value: cnt})
		headers = append(headers, kgo.RecordHeader{Key: domain.HeaderRetryReason, Value: []byte(task.RetryReason)})
	}

	rec.Key = []byte(task.PartitionKey)
	rec.Value = task.Args
	rec.Headers = headers
	return
}
