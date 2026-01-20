package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/ecociel/when/lib/domain"
	"github.com/ecociel/when/lib/kafkaclient"
	"github.com/ecociel/when/lib/scheduler"
	"github.com/ecociel/when/lib/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	DbConnectionUri    string   `required:"true" split_words:"true"`
	QueueHostPorts     []string `required:"true" split_words:"true"`
	TasksTopic         string   `required:"true" split_words:"true"`
	TasksConsumerGroup string   `required:"true" split_words:"true"`
}

type Counter struct {
	Count string `json:"count"`
	Ts    int64  `json:"ts"`
}

var check = make([]int64, 1000)

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

		for seq := 0; seq < 1000; seq++ {
			select {
			case <-time.After(200 * time.Millisecond):
				task := domain.Task{
					Name:         "PrintCount",
					PartitionKey: domain.PartitionKeyNone,
					Args:         []byte(fmt.Sprintf(`{"count":"%d","ts":%d}`, seq, time.Now().Unix())),
					Due:          time.Now().Add(5 * time.Second),
				}
				if _, err := sched.Schedule(context.Background(), task); err != nil {
					log.Fatal(err)
				}
				check[seq] = 0
			}
		}

		time.Sleep(10)

		for i := 0; i < 1000; i++ {
			log.Printf("%d %d", i, check[i])
		}
	}()

	kClient, err := kafkaclient.MustNew(config.QueueHostPorts, config.TasksConsumerGroup, config.TasksTopic)
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	wrk := worker.New(kClient, pool)
	wrk.RegisterHandler("PrintCount", printCountHdl)
	wrk.Run(ctx)
}

func MakePrintCountHandler() func(id uint64, data []byte) error {
	return func(id uint64, data []byte) error {
		var c Counter
		if err := json.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("unmarshal counter of task PrintCount/%d: %v", id, err)
		}
		x, _ := strconv.Atoi(c.Count)

		if x%100 == 0 {
			return fmt.Errorf("MODULO FAILED: %d", id)
		}
		log.Printf("Handled PrintCount %d: %s", id, c.Count)
		n, _ := strconv.ParseInt(c.Count, 10, 64)

		check[n] = time.Now().Unix() - c.Ts
		return nil
	}

}
