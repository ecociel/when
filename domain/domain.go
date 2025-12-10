package domain

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusPublishing Status = "publishing"
)

type Task struct {
	ID      int64
	Topic   string
	Key     *string
	Payload []byte
	RunAt   time.Time
	Paused  bool
	Status  Status
}
