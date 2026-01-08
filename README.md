# When - Distributed Task Scheduler

A reliable, PostgreSQL-backed task scheduler for Go that enables delayed execution of asynchronous tasks via Kafka.

## Overview

**When** is a distributed task scheduling system that allows you to schedule tasks for future execution. Tasks are stored in PostgreSQL with a specified due time, and an observer service periodically claims and publishes due tasks to Kafka for worker consumption.

### Key Features

- **Persistent Storage**: Tasks are stored in PostgreSQL, ensuring durability and reliability
- **Delayed Execution**: Schedule tasks to run at any future time
- **Kafka Integration**: Seamless integration with Kafka for scalable task distribution
- **Partition Keys**: Support for Kafka partition keys to ensure ordered processing when needed
- **Retry Support**: Built-in retry mechanism with retry count and reason tracking
- **At-Least-Once Delivery**: Tasks are only deleted after successful Kafka publishing
- **Graceful Degradation**: Continues processing other tasks if one fails to publish
- **Testable Architecture**: Fully tested with interface-based design

## Use Cases

- **Scheduled Notifications**: Send emails, SMS, or push notifications at specific times
- **Delayed Jobs**: Execute background jobs after a delay (e.g., "delete account after 30 days")
- **Periodic Tasks**: Schedule recurring operations (combined with rescheduling logic)
- **Workflow Orchestration**: Coordinate multi-step workflows with time-based transitions
- **Rate Limiting**: Distribute tasks over time to avoid overwhelming downstream services
- **Reminder Systems**: Trigger reminders at specific dates/times

## Architecture

```
┌──────────────┐         ┌─────────────┐         ┌──────────┐
│   Client     │────────>│  Scheduler  │────────>│PostgreSQL│
│ (Schedule)   │         │   (Insert)  │         │  (Store) │
└──────────────┘         └─────────────┘         └──────────┘
                                                       │
                                                       │ Poll
                                                       ▼
                         ┌─────────────┐         ┌──────────┐
┌──────────────┐<────────│  Publisher  │<────────│ Observer │
│    Kafka     │         │  (Publish)  │         │ (Runner) │
│   (Queue)    │         └─────────────┘         └──────────┘
└──────────────┘
       │
       │ Consume
       ▼
┌──────────────┐
│   Worker     │
│  (Execute)   │
└──────────────┘
```

### Components

1. **Scheduler** (`lib/scheduler`)
   - Accepts task scheduling requests
   - Inserts tasks into PostgreSQL with a `due` timestamp
   - Returns task ID for tracking

2. **Observer** (`cmd/observer`)
   - Runs continuously in the background
   - Periodically polls PostgreSQL for due tasks
   - Claims tasks using a configurable batch size (default: 100)
   - Publishes tasks to Kafka via the Publisher
   - Deletes successfully published tasks

3. **Publisher** (`cmd/observer/kafka`)
   - Converts tasks to Kafka records
   - Publishes records to the configured Kafka topic
   - Encodes task metadata in Kafka headers (ID, name, retry info)

4. **Worker** (`lib/worker`)
   - Consumes tasks from Kafka
   - Decodes task metadata from Kafka headers
   - Executes the task's business logic

## How It Works

### Task Lifecycle

1. **Schedule**: Client calls `Scheduler.Schedule()` with a task and due time
2. **Store**: Task is inserted into PostgreSQL `task` table with status "pending"
3. **Poll**: Observer wakes up periodically (configurable interval, default: 10s)
4. **Claim**: Observer queries for tasks where `due <= NOW()` (limit: 100)
5. **Publish**: Each task is published to Kafka with headers containing metadata
6. **Delete**: Successfully published tasks are deleted from the database
7. **Consume**: Worker picks up the task from Kafka and executes it

### Task Structure

```go
type Task struct {
    ID           uint64        // Unique task identifier
    Name         string        // Task name/type
    PartitionKey string        // Kafka partition key for ordering
    Args         []byte        // Task payload (JSON or any serialized data)
    Due          time.Time     // When the task should execute
    RetryCount   uint16        // Number of retry attempts
    RetryReason  string        // Reason for retry
}
```

### Database Schema

```sql
CREATE TABLE task (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    args          BYTEA,
    due           TIMESTAMP NOT NULL,
    retry_count   SMALLINT DEFAULT 0,
    retry_reason  TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_task_due ON task(due);
```

## Configuration

### Observer Configuration

The observer is configured via environment variables:

```bash
# Database connection string
DB_CONNECTION_URI="postgres://user:pass@localhost:5432/when_db"

# Kafka broker addresses (comma-separated)
QUEUE_HOST_PORTS="localhost:9092,localhost:9093"

# Kafka topic for task events
EVENTS_TOPIC="tasks"
```

### Runner Parameters

```go
runner.New(
    limit,     // Max tasks to claim per cycle (e.g., 100)
    interval,  // Polling interval (e.g., 10*time.Second)
    store,     // PostgreSQL store implementation
    publisher, // Kafka publisher implementation
)
```

## Components

### Scheduler

```go
import "github.com/ecociel/when/lib/scheduler"

pool := pgxpool.New(ctx, connectionString)
sched := scheduler.New(pool)

task := domain.Task{
    Name:         "send_email",
    PartitionKey: "user_123",
    Args:         []byte(`{"to":"user@example.com","subject":"Hello"}`),
    Due:          time.Now().Add(1 * time.Hour),
}

id, err := sched.Schedule(ctx, task)
```

### Observer

```go
import "github.com/ecociel/when/cmd/observer/runner"

runner := runner.New(
    100,                // Claim up to 100 tasks per cycle
    10*time.Second,     // Poll every 10 seconds
    store,              // PostgreSQL store
    publisher,          // Kafka publisher
)

go runner.Run(ctx)
```

### Worker

```go
import "github.com/ecociel/when/lib/worker"

worker := worker.New(kafkaClient, func(task domain.Task) error {
    // Execute your business logic here
    log.Printf("Executing task %s: %s", task.Name, string(task.Args))
    return nil
})

worker.Start(ctx)
```

## Build

Build Docker images for each component:

```bash
# Build observer
docker build -t when-observer --build-arg APP=observer .

# Build demo (scheduler + worker combined)
docker build -t when-demo --build-arg APP=demo .
```

## Run the Demo

Start the complete system using Docker Compose:

```bash
docker-compose up
```

This starts:
- PostgreSQL database
- Kafka (via RedPanda)
- Observer service
- Demo application (scheduler + worker)

### Visit the RedPanda Console UI

Point your browser to http://localhost:8000 to:
- View Kafka topics
- Monitor task messages
- Inspect task headers and payloads

## Testing

The project includes comprehensive test coverage:

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test ./cmd/observer/kafka -v
go test ./cmd/observer/runner -v
```

### Test Structure

- **Unit Tests**: All components have unit tests with mocked dependencies
- **Interface-Based Testing**: Publisher and Runner use interfaces for easy mocking
- **Integration Tests**: Demo app provides end-to-end testing

## Error Handling

### Publish Failures

If a task fails to publish to Kafka:
- The error is logged
- The task remains in the database
- Processing continues with the next task
- The task will be retried on the next polling cycle

### Delete Failures

If a task deletion fails:
- The error is returned immediately
- Processing stops to prevent message loss
- Manual intervention may be required

### Context Cancellation

All operations respect context cancellation for graceful shutdown:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

runner.Run(ctx) // Will stop when context is cancelled
```

## Design Decisions

### Why PostgreSQL?

- ACID guarantees ensure tasks are never lost
- Efficient indexing on `due` timestamp for fast queries
- Native support for transactions
- Battle-tested reliability and scalability

### Why Kafka?

- High throughput for task distribution
- Built-in partition support for ordered processing
- Horizontal scalability with consumer groups
- Durable message storage with configurable retention

### Why Claim-and-Delete?

Tasks are only deleted after successful Kafka publish to ensure **at-least-once delivery**. This prevents task loss in case of:
- Kafka connection failures
- Publisher crashes
- Network partitions

The trade-off is potential duplicate execution if deletion fails, which is acceptable for idempotent tasks.

## Performance Considerations

- **Batch Size**: Adjust the `limit` parameter (default: 100) based on task volume
- **Polling Interval**: Tune the interval (default: 10s) based on latency requirements
- **Database Indexing**: Ensure the `due` column is indexed for fast queries
- **Kafka Partitions**: Use partition keys to distribute load across Kafka partitions

## Project Structure

```
when/
├── cmd/
│   ├── observer/          # Observer service (main application)
│   │   ├── kafka/         # Kafka publisher implementation
│   │   ├── postgres/      # PostgreSQL store implementation
│   │   └── runner/        # Task polling and processing logic
│   └── demo/              # Demo application
├── lib/
│   ├── domain/            # Domain models (Task, headers)
│   ├── scheduler/         # Scheduler for inserting tasks
│   └── worker/            # Worker for consuming tasks
└── README.md
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass: `go test ./...`
- Code is properly formatted: `go fmt ./...`
- New features include tests
- Commits follow conventional commit messages

## License

[Add your license here]

## Future Enhancements

- [ ] Add exponential backoff for failed publishes
- [ ] Implement task prioritization
- [ ] Add metrics and observability (Prometheus)
- [ ] Support for task cancellation
- [ ] Dead letter queue for permanently failed tasks
- [ ] Web UI for task monitoring

