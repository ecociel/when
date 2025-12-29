package kafka

import (
	"context"
	"fmt"
	"strconv"

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
	headers := []kgo.RecordHeader{
		{
			Key:   domain.HeaderTaskID,
			Value: []byte(strconv.FormatInt(task.ID, 10)),
		},
		{
			Key:   domain.HeaderTaskName,
			Value: []byte(task.Name),
		},
	}

	record := &kgo.Record{Key: []byte(task.PartitionKey), Value: task.Args, Headers: headers}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish event: %w\n", err)
	}

	return nil
}
