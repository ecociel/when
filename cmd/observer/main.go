package main

import (
	"context"
	"log"
	"time"

	"github.com/ecociel/when/cmd/observer/kafka"
	"github.com/ecociel/when/cmd/observer/postgres"
	"github.com/ecociel/when/cmd/observer/runner"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelseyhightower/envconfig"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	DbConnectionUri string   `required:"true" split_words:"true"`
	QueueHostPorts  []string `required:"true" split_words:"true"`
	EventsTopic     string   `required:"true" split_words:"true"`
}

func main() {
	ctx := context.Background()

	var config Config
	envconfig.MustProcess("", &config)

	pool, err := pgxpool.New(context.Background(), config.DbConnectionUri)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	store := postgres.New(pool)

	kClient, err := mustNewKafkaClient(config.QueueHostPorts, config.EventsTopic)
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()

	publisher := kafka.New(kClient)

	go runner.New(100, 10*time.Second, store, publisher).Run(ctx)

	select {}
}

func mustNewKafkaClient(hostPorts []string, topic string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(hostPorts...),
		kgo.AllowAutoTopicCreation(),
		//kgo.RecordRetries(1),
		//kgo.RecordDeliveryTimeout(1*time.Second),
		kgo.DefaultProduceTopic(topic),
		//kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("create events client: %v", err)
	}
	return client, nil
}
