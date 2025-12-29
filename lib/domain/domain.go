package domain

import "time"

//type TaskState string

//const (
//	StatusPending    TaskState = "pending"
//	StatusPublishing TaskState = "publishing"
//	StatePublished   TaskState = "published"
//)

// PartitionKeyNone indicates that partition is not relevant
// because sort order of tasks is not important.
const PartitionKeyNone = "-"

const HeaderTaskID = "task_id"
const HeaderTaskName = "task_name"

type Task struct {
	ID           int64
	Name         string
	PartitionKey string
	Args         []byte
	Due          time.Time
	Paused       bool
	//ExternalKey     *string
	//Triggered       bool
	//State           TaskState
	//PublishAttempts int
}
