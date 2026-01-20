package kafkaclient

import (
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func MustNew(hostPorts []string, group, topic string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(hostPorts...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
		//kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		log.Fatalf("create events client: %v", err)
	}
	return client, nil
}
