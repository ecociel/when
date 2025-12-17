package domain

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusPublishing Status = "publishing"
	StatePublished   Status = "published"
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
	Status          Status
	PublishAttempts int
}
