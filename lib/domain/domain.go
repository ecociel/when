package domain

import "time"

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
}
