package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ecociel/when/lib/domain"
	"github.com/ecociel/when/lib/scheduler"
	"github.com/ecociel/when/lib/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelseyhightower/envconfig"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Payload struct {
	Action string `json:"action"`
	Seq    int    `json:"seq"`
}

type Config struct {
	DbConnectionUri     string   `required:"true" split_words:"true"`
	QueueHostPorts      []string `required:"true" split_words:"true"`
	EventsTopic         string   `required:"true" split_words:"true"`
	EventsConsumerGroup string   `required:"true" split_words:"true"`
}

type Counter struct {
	Count string `json:"count"`
}

func main() {
	var config Config
	envconfig.MustProcess("", &config)

	pool, err := pgxpool.New(context.Background(), config.DbConnectionUri)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	sched := scheduler.New(pool)

	printCountHdl := MakePrintCountHandler()

	go func() {

		for seq := 0; true; seq++ {
			select {
			case <-time.After(2 * time.Second):
				task := domain.Task{
					Name:         "PrintCount",
					PartitionKey: domain.PartitionKeyNone,
					Args:         []byte(fmt.Sprintf(`{"count":"%d"}`, seq)),
					Due:          time.Now().Add(5 * time.Second),
				}
				if _, err := sched.Schedule(context.Background(), task); err != nil {
					log.Fatal(err)
				}
				log.Println("scheduled")
			}
		}
	}()

	kClient, err := mustNewKafkaClient(config.QueueHostPorts, config.EventsConsumerGroup, config.EventsTopic)
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()

	wrk := worker.New(kClient)
	wrk.RegisterHandler("PrintCount", printCountHdl)
	wrk.Run(context.Background())
}

func mustNewKafkaClient(hostPorts []string, group, topic string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(hostPorts...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordRetries(1),
		kgo.RecordDeliveryTimeout(1*time.Second),
		kgo.DefaultProduceTopic(topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(1*time.Second),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("create events client: %v", err)
	}
	return client, nil
}

func MakePrintCountHandler() func(id string, data []byte) error {
	return func(id string, data []byte) error {
		var c Counter
		if err := json.Unmarshal(data, &c); err != nil {
			log.Printf("unmarshal counter of task PrintCount/%s: %v", id, err)
		}
		log.Printf("PrintCount %s: %s", id, c.Count)
		return nil
	}

}
