# when


A small Go library for scheduling asynchronous tasks using **Postgres** + **Kafka (franz-go)**.


## Install

```bash
go get github.com/you/when
```


### Create topic explicitly
```
docker exec -it scheduler-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <Topic-Name> --partitions 1 --replication-factor 1
```

### To see consumed messages from the topic
```
docker exec -it scheduler-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic <Topic-Name> \
  --from-beginning
```