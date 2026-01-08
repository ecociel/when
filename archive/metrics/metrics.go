package metrics

import "time"

type SchedulerMetrics interface {
	TaskClaimed(n int)
	TaskPublished()
	TaskPublishFailed()
	TasksReclaimed(n int)
	PublishLatency(d time.Duration)
}

//type DefaultMetrics struct{}
//
//func (DefaultMetrics) TaskClaimed(int)              {}
//func (DefaultMetrics) TaskPublished()               {}
//func (DefaultMetrics) TaskPunishFailed()          {}
//func (DefaultMetrics) TasksReclaimed(int)           {}
//func (DefaultMetrics) PublishLatency(time.Duration) {}
