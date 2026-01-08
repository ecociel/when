package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ecociel/when/lib/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer defines the interface for producing messages to Kafka
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

type Publisher struct {
	client Producer
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
	id := binary.BigEndian.AppendUint64(nil, task.ID)
	capacity := 2
	if task.RetryCount > 0 {
		capacity = 4
	}
	headers := make([]kgo.RecordHeader, 0, capacity)
	headers = append(headers, kgo.RecordHeader{Key: domain.HeaderID, Value: id})
	headers = append(headers, kgo.RecordHeader{Key: domain.HeaderName, Value: []byte(task.Name)})
	if task.RetryCount > 0 {
		cnt := binary.BigEndian.AppendUint16(nil, task.RetryCount)
		headers = append(headers, kgo.RecordHeader{Key: domain.HeaderRetryCount, Value: cnt})
		headers = append(headers, kgo.RecordHeader{Key: domain.HeaderRetryReason, Value: []byte(task.RetryReason)})
	}

	rec.Key = []byte(task.PartitionKey)
	rec.Value = task.Args
	rec.Headers = headers
	return
}
