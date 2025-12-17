package kafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	client       *kgo.Client
	defaultTopic string
}

func NewPublisher(client *kgo.Client, topic string) *Publisher {
	return &Publisher{client: client, defaultTopic: topic}
}

func (p *Publisher) PublishSync(ctx context.Context, topic string, key []byte, value []byte, headers map[string][]byte) error {
	t := topic
	if t == "" {
		t = p.defaultTopic
	}
	record := &kgo.Record{Topic: t, Key: key, Value: value}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish event: %w\n", err)
	}

	return nil
}
