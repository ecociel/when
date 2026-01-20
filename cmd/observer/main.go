package main

import (
	"context"
	"log"
	"time"

	"github.com/ecociel/when/lib/kafkaclient"
	"github.com/ecociel/when/lib/observer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	DbConnectionUri    string   `required:"true" split_words:"true"`
	QueueHostPorts     []string `required:"true" split_words:"true"`
	TasksTopic         string   `required:"true" split_words:"true"`
	TasksConsumerGroup string   `required:"true" split_words:"true"`
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

	kClient, err := kafkaclient.MustNew(config.QueueHostPorts, config.TasksConsumerGroup, config.TasksTopic)
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()

	go observer.New(100, 1*time.Second, pool, kClient).Run(ctx)

	select {}
}
