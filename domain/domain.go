package domain

import "time"

type TaskState string

const (
	StatusPending    TaskState = "pending"
	StatusPublishing TaskState = "publishing"
	StatePublished   TaskState = "published"
)

type Task struct {
	ID              int64
	Topic           string
	Key             *string
	Payload         []byte
	RunAt           time.Time
	Paused          bool
	ExternalKey     *string
	Triggered       bool
	State           TaskState
	PublishAttempts int
}
