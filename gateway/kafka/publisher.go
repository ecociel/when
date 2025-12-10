package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Kind = "kind"

type Publisher struct {
	client *kgo.Client
	topic  string
}

func NewPublisher(client *kgo.Client, topic string) *Publisher {
	return &Publisher{client: client, topic: topic}
}

func (p *Publisher) Publish(ctx context.Context, kind string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialize event: %w\n", err)
	}
	headers := []kgo.RecordHeader{{Key: Kind, Value: []byte(kind)}}
	record := &kgo.Record{Topic: p.topic, Headers: headers, Value: data}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish event: %w\n", err)
	}

	return nil
}
