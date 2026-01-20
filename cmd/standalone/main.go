package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/ecociel/when/lib/domain"
	"github.com/ecociel/when/lib/kafkaclient"
	"github.com/ecociel/when/lib/observer"
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

type Payload struct {
	Seq string `json:"seq"`
	Ts  int64  `json:"ts"`
}

const Iterations = 1000

var latency struct {
	sync.RWMutex
	min int64
	max int64
}

func main() {
	var config Config
	envconfig.MustProcess("", &config)

	latency.min = math.MaxInt64
	latency.max = 0

	//ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	//defer cancel()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, config.DbConnectionUri)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	kClient, err := kafkaclient.MustNew(config.QueueHostPorts, config.TasksConsumerGroup, config.TasksTopic)
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()

	// Observer
	go observer.New(100, 1*time.Second, pool, kClient).Run(ctx)

	time.Sleep(5 * time.Second)
	// Worker
	printSeqHdl := MakeSayHelloHandler()
	wrk := worker.New(kClient, pool)
	wrk.RegisterHandler("SayHello", printSeqHdl)
	go wrk.Run(ctx)

	// Scheduler
	sched := scheduler.New(pool)
	go MakeSchedulingLoop(ctx, sched)()

	var nano float64 = 1_000_000_000
	for {
		latency.RLock()
		log.Printf("________________________________ min=%.3f max=%.3f", float64(latency.min)/nano, float64(latency.max)/nano)
		latency.RUnlock()
		time.Sleep(5 * time.Second)
	}
}

func MakeSchedulingLoop(ctx context.Context, scheduler *scheduler.Scheduler) func() {
	return func() {

		for seq := 0; seq < Iterations; seq++ {
			select {
			case <-time.After(10000 * time.Millisecond):
				task := domain.Task{
					Name:         "SayHello",
					PartitionKey: domain.PartitionKeyNone,
					Args:         []byte(fmt.Sprintf(`{"seq":"%d","ts":%d}`, seq, time.Now().UnixNano())),
					Due:          time.Now().Add(5 * time.Second),
				}
				if _, err := scheduler.Schedule(ctx, task); err != nil {
					log.Fatal(err)
				}
			}
		}

	}
}

func MakeSayHelloHandler() func(id uint64, data []byte) error {
	return func(id uint64, data []byte) error {
		var c Payload
		if err := json.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("unmarshal payload of task SayHello/%d: %v", id, err)
		}
		log.Printf("Handled SayHello %d: %s", id, c.Seq)

		l := time.Now().UnixNano() - c.Ts
		latency.Lock()
		latency.min = min(latency.min, l)
		latency.max = max(latency.max, l)
		latency.Unlock()

		return nil
	}

}
