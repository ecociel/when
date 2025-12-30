package domain

import "time"

// PartitionKeyNone indicates that partition is not relevant
// because sort order of tasks is not important.
const PartitionKeyNone = "-"

const HeaderID = "id"
const HeaderName = "name"
const HeaderRetryCount = "retry_count"
const HeaderRetryReason = "retry_reason"

type Task struct {
	ID           uint64
	Name         string
	PartitionKey string
	Args         []byte
	Due          time.Time
	RetryCount   uint16
	RetryReason  string
}
