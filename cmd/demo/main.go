package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ecociel/when/domain"
	"github.com/ecociel/when/scheduler"
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

	println("1")
	kClient, err := mustNewKafkaClient(config.QueueHostPorts, config.EventsConsumerGroup, config.EventsTopic)
	if err != nil {
		log.Fatal(err)
	}
	defer kClient.Close()
	println("2")
	ctx := context.Background()
	for {
		println("3")
		fetches := kClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			log.Println("consuming client closed, returning")
			return
		}
		println("3.5")
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("fetch err topic %s partition %d: %v", t, p, err)
		})
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Println("poll error:", errs)
			continue
		}
		println("4")

		fetches.EachRecord(func(record *kgo.Record) {

			name := name(record.Headers)
			id := id(record.Headers)
			switch name {
			case "PrintCount":
				if err = printCountHdl(id, record.Value); err != nil {
					log.Printf("hanlde PrintCount/%s: %v", id, err)
				}
			default:
				log.Printf("Unkown task: %q", name)
			}

			//// decide if action should be scheduled
			//if evt.Type == "USER_UPDATED" {
			//	// schedule a future task
			//	payload := []byte(`{"action":"sync_user","userId":` +
			//		json.Number(rune(evt.UserID)).String() + `}`)
			//
			//	task := &domain.Task{
			//		Name:   "tasks.queue",
			//		Args: payload,
			//		Due:   time.Now().Add(10 * time.Minute),
			//	}
			//
			//	id, err := scheduleTask(ctx, task)
			//	if err != nil {
			//		log.Println("failed to schedule:", err)
			//	} else {
			//		log.Println("scheduled future task:", id)
			//	}
			//}
		})
		if fetches.NumRecords() == 0 {
			log.Printf("no records, sleep 1s")
			time.Sleep(1000 * time.Millisecond)
		}
	}

	//select {}

}

func name(headers []kgo.RecordHeader) string {
	for i := range headers {
		if headers[i].Key == domain.HeaderTaskName {
			return string(headers[i].Value)
		}
	}
	return ""
}
func id(headers []kgo.RecordHeader) string {
	for i := range headers {
		if headers[i].Key == domain.HeaderTaskID {
			return string(headers[i].Value)
		}
	}
	return ""
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
