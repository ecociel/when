package kafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Kind = "kind"

type Publisher struct {
	client       *kgo.Client
	defaultTopic string
}

func NewPublisher(client *kgo.Client, topic string) *Publisher {
	return &Publisher{client: client, defaultTopic: topic}
}

func (p *Publisher) PublishSync(ctx context.Context, topic string, key []byte, value []byte) error {
	//data, err := json.Marshal(event)
	//if err != nil {
	//	return fmt.Errorf("serialize event: %w\n", err)
	//}
	t := topic
	if t == "" {
		t = p.defaultTopic
	}
	// headers := []kgo.RecordHeader{{Key: Kind, Value: key}}
	record := &kgo.Record{Topic: t, Key: key, Value: value}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish event: %w\n", err)
	}

	return nil
}
